package mmap

import (
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"syscall"
)

const (
	// DefaultChunkSize of Large is 128MB.
	DefaultChunkSize = 128 << 20

	tblArraySize = 5003 // prime number
)

// Large is a large file combined with memory mapped files.
type Large struct {
	dir       string
	chunkSize int64
	tbls      []*tbl // tbl array, optimize concurrent access. idx -> memroy mapped file
	l         sync.RWMutex
}

// OpenLarge open an exist or create a new large file at dir.
func OpenLarge(dir string, chunkSize int64) (f *Large, err error) {
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}

	err = os.MkdirAll(dir, 0755)
	if os.IsExist(err) {
		err = nil
	}
	if err != nil {
		return
	}

	var tbls []*tbl
	for i := 0; i < tblArraySize; i++ {
		tbls = append(tbls, newTbl())
	}
	f = &Large{dir: dir, chunkSize: chunkSize, tbls: tbls}
	return
}

// Close the large file.
func (f *Large) Close() {
	for _, tbl := range f.tbls {
		for _, mapped := range tbl.Values() {
			syscall.Munmap(mapped)
		}
	}
}

func (f *Large) getReadChunk(off int64) (b []byte, boff int64, err error) {
	idx := off / f.chunkSize
	boff = off % f.chunkSize

	tbl := f.tbls[idx%tblArraySize]
	b, err = tbl.Get(idx, func() (b []byte, err error) {
		name := path.Join(f.dir, fmt.Sprint(idx))
		f0, err := os.OpenFile(name, os.O_RDWR, 0644) // FIXME: need to read disk every time if is read at wrong off.
		if err != nil {
			return
		}
		defer f0.Close()

		b, err = syscall.Mmap(int(f0.Fd()), 0, int(f.chunkSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return
		}
		return
	})
	if os.IsNotExist(err) {
		err = io.EOF
		return
	}
	return
}

func (f *Large) getWriteChunk(off int64) (b []byte, boff int64, err error) {
	idx := off / f.chunkSize
	boff = off % f.chunkSize

	tbl := f.tbls[idx%tblArraySize]
	b, err = tbl.Get(idx, func() (b []byte, err error) {
		name := path.Join(f.dir, fmt.Sprint(idx))
		f0, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return
		}
		defer f0.Close()

		fi, err := f0.Stat()
		if err != nil {
			return
		}
		if fi.Size() == 0 {
			err = f0.Truncate(f.chunkSize)
			if err != nil {
				return
			}
		} else if fi.Size() != f.chunkSize {
			err = fmt.Errorf("chunk size mismatch: %v != %v", fi.Size(), f.chunkSize)
			return
		}

		b, err = syscall.Mmap(int(f0.Fd()), 0, int(f.chunkSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return
		}
		return
	})
	return
}

// ReadAt implements io.ReaderAt
func (f *Large) ReadAt(b []byte, off int64) (n int, err error) {
	c, coff, err := f.getReadChunk(off)
	if err != nil {
		return
	}

	// FIXME: valid chunk size
	n = copy(b, c[coff:])
	if n == len(b) {
		return
	}

	n1, err := f.ReadAt(b[n:], off+int64(n))
	if err != nil {
		return
	}
	n += n1
	return
}

// WriteAt implements io.WriterAt
func (f *Large) WriteAt(b []byte, off int64) (n int, err error) {
	c, coff, err := f.getWriteChunk(off)
	if err != nil {
		return
	}

	n = copy(c[coff:], b)
	if n == len(b) {
		return
	}

	n1, err := f.WriteAt(b[n:], off+int64(n))
	n += n1
	return
}
