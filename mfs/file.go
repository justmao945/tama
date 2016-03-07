package mfs

import (
	"fmt"
	"io"
	"sync"
)

// File is a high level interface which can be read or write at specified offset.
type File struct {
	v       *Volume  // root
	fd      uint32   // unique number on a volume
	blkSize int32    // logical block size, excluding header size
	blks    []*block // may contain nil, block idx: off / blkSize, off in block = off % blkSize
	l       sync.RWMutex
}

func (f *File) String() string {
	f.l.RLock()
	defer f.l.RUnlock()

	return fmt.Sprintf("F{v:%v, fd:%v, bs:%v, b:%v}", f.v.Name(), f.fd, f.blkSize, f.blks)
}

func newFile(v *Volume, fd uint32, blkSize int32) *File {
	return &File{v: v, fd: fd, blkSize: blkSize}
}

// Add a block to this file
func (f *File) add(b *block) {
	if b.fd != f.fd {
		panic("add block not owned by this file")
	}
	if int(b.idx) < len(f.blks) { // add a block in file hole
		if f.blks[b.idx] != nil {
			panic("already have block")
		}
		b.size = b.cap // resize the file hole
	} else if len(f.blks) > 0 { // add new block
		last := f.blks[len(f.blks)-1]
		last.size = last.cap // resize the previous last one block
	}

	for i := len(f.blks); i <= int(b.idx); i++ {
		f.blks = append(f.blks, nil) // FIXME: use map to replace array ?
	}
	f.blks[b.idx] = b
}

// may return nil block, which means a file hole
func (f *File) getReadBlock(off int64) (b *block, boff int32, err error) {
	idx := int(off / int64(f.blkSize))
	boff = int32(off % int64(f.blkSize))

	f.l.RLock()
	defer f.l.RUnlock()

	if idx < len(f.blks) {
		b = f.blks[idx]
	} else {
		err = io.EOF
	}
	return
}

// always return a non-nil block to write
func (f *File) getWriteBlock(off int64) (b *block, boff int32, err error) {
	idx := int(off / int64(f.blkSize))
	boff = int32(off % int64(f.blkSize))

	f.l.RLock()
	if idx < len(f.blks) {
		b = f.blks[idx]
	}
	f.l.RUnlock()

	if b != nil {
		return
	}

	f.l.Lock()
	defer f.l.Unlock()

	if idx < len(f.blks) {
		b = f.blks[idx]
	}
	if b != nil {
		return
	}

	b, err = f.v.alloc(f.fd, int32(idx), f.blkSize)
	if err != nil {
		return
	}
	f.add(b)
	return
}

// Fd returns the unique file id in this volume.
func (f *File) Fd() uint32 {
	return f.fd
}

//ReadAt implements io.ReaderAt
func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	blk, boff, err := f.getReadBlock(off)
	if err != nil {
		return
	}

	if blk == nil { // file hole
		nn := min(len(b), int(f.blkSize-boff)) // need fill nn zeros to b
		n = fillZero(b[:nn])
	} else {
		nn := min(len(b), int(blk.size-boff)) // need read nn bytes in this block
		n, err = blk.ReadAt(b[:nn], boff)     // read first nn bytes to b at off
		if err != nil {
			return
		}
		if n != nn {
			panic("buggy ReadAt")
		}
	}

	if n >= len(b) { // already fill all requested data
		return
	}

	// need read next block
	n1, err := f.ReadAt(b[n:], off+int64(n))
	n += n1
	return
}

// WriteAt implements io.WriterAt
func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	blk, boff, err := f.getWriteBlock(off)
	if err != nil {
		return
	}

	nn := min(len(b), int(blk.cap-boff)) // need write nn bytes in this block
	n, err = blk.WriteAt(b[:nn], boff)
	if err != nil {
		return
	}

	if n != nn {
		panic("buggy WriteAt")
	}

	if nn >= len(b) { // already write all data to block
		return
	}

	// need to write next block
	n1, err := f.WriteAt(b[n:], off+int64(n))
	n += n1
	return
}
