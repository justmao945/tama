package mfs

import (
	"errors"
	"fmt"
	"os"
	"path"
	"syscall"
)

var (
	errFullRound = errors.New("full round")
)

// round can have many(fixed) blocks belonging to different files.
// addr layout: | block | block | .... |
type round struct {
	v       *Volume  // root
	idx     int      // round index, unique in this Volume
	size    int64    // real round size: requested size / blkSize * realBlockSize(blkSize)
	blkSize int32    // logical block size, exluding the header size
	addr    []byte   // underlying memory mapped file
	blks    []*block // valid blocks, empty blocks will not in this list
}

func (r *round) String() string {
	return fmt.Sprintf("R{v:%v, i:%v, s:%v, bs:%v, p:%v, b:%v}",
		r.v.Name(), r.idx, r.size, r.blkSize, &r.addr, r.blks)
}

// newRound create or open an exist file on disk by mmap for faster read/write.
func newRound(v *Volume, idx int, size int64, blkSize int32) (r *round, err error) {
	if blkSize <= 0 {
		err = fmt.Errorf("blkSize %v should not <= 0", blkSize)
		return
	}

	if size < int64(blkSize) {
		err = fmt.Errorf("round size %v should not < block size %v", size, blkSize)
		return
	}

	if size%int64(blkSize) != 0 {
		err = fmt.Errorf("round size %v should be divided by blk size %v", size, blkSize)
		return
	}

	rblkSize := int64(realBlockSize(blkSize))
	rsize := size / int64(blkSize) * rblkSize

	if int64(int(rsize)) != rsize {
		err = fmt.Errorf("file size %v is too large", rsize)
		return
	}

	name := path.Join(v.Name(), fmt.Sprint(idx))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return
	}

	if fi.Size() == 0 { // new file, need to resize file
		err = f.Truncate(rsize)
		if err != nil {
			return
		}
	} else if fi.Size() != rsize {
		err = fmt.Errorf("file size %v is mismatch with the requested cap %v", fi.Size(), rsize)
		return
	}

	addr, err := syscall.Mmap(int(f.Fd()), 0, int(rsize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return
	}

	// only load valid blocks
	var blks []*block
	for off := int64(0); off < rsize; off += int64(rblkSize) { // FIXME: broken block ?
		var b *block
		b, err = openBlock(addr[off : off+rblkSize])
		if err == errNotBlock { // end of valiad blocks in this round
			err = nil
			break
		}
		if err != nil {
			return
		}
		if b.cap != blkSize {
			err = fmt.Errorf("invalid block at %v with cap %v != blkSize %v", off, b.cap, blkSize)
			return
		}
		blks = append(blks, b)
	}

	r = &round{v: v, idx: idx, size: rsize, blkSize: blkSize, addr: addr, blks: blks}
	return
}

// Load iterates all blocks and it to the corresponding file.
func (r *round) Load(files map[uint32]*File) {
	for _, b := range r.blks {
		f, ok := files[b.fd]
		if !ok {
			f = newFile(r.v, b.fd, r.blkSize)
			files[b.fd] = f
		}
		f.add(b)
	}
}

// Close the underlying mmap addr.
func (r *round) Close() error {
	return syscall.Munmap(r.addr)
}

// Alloc returns a block in this round if is available.
func (r *round) Alloc(fd uint32, idx, cap int32) (b *block, err error) {
	if r.Full() {
		err = errFullRound
		return
	}

	if cap != r.blkSize {
		panic("invalid cap")
	}

	rblkSize := int(realBlockSize(r.blkSize))
	off := len(r.blks) * rblkSize
	if int64(off) >= r.size {
		panic("alloc too many blocks")
	}

	b, err = newBlock(r.addr[off:off+rblkSize], fd, idx, cap)
	if err != nil {
		return
	}

	r.blks = append(r.blks, b)
	return
}

// Full returns true if there's no available block in this round.
func (r *round) Full() bool {
	return len(r.blks) >= int(r.size/int64(realBlockSize(r.blkSize)))
}
