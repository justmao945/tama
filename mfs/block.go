package mfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"unsafe"
)

const (
	blockHeaderSize = 128 // > sizeof(block{})
)

var (
	blockMagic = binary.LittleEndian.Uint32([]byte{'m', 'f', 's', 'b'})
)

var (
	errNotBlock = errors.New("not a block")
)

func init() {
	if blockHeaderSize < unsafe.Sizeof(block{}) {
		panic("too small header size")
	}
}

func realBlockSize(cap int32) int32 {
	return cap + blockHeaderSize
}

// ------------------------------------------------------------------

// Addr layout:
// | block header 128B | ...... block cap |
// FIXME: read and write overlapped ?
type block struct {
	magic uint32 // 4B predefined magic number, used to detect a block
	size  int32  // 4B valid data size
	cap   int32  // 4B block cap to store data, excluding header
	fd    uint32 // 4B owner file
	idx   int32  // 4B n-th block of owner file
	flag  int64  // 8B reserved
	data  []byte // 24
}

func (b *block) String() string {
	return fmt.Sprintf("B{f:%v, i:%v, s:%v, c:%v, f:%v}", b.fd, b.idx, b.size, b.cap, b.flag)
}

func (b *block) ReadAt(d []byte, off int32) (n int, err error) {
	if off >= b.size {
		err = io.EOF
		return
	}
	src := b.data[off:b.size]
	n = copy(d, src)
	if n < min(len(d), len(src)) {
		err = io.ErrUnexpectedEOF
		return
	}
	if n < len(d) {
		err = io.EOF
		return
	}
	return
}

func (b *block) WriteAt(d []byte, off int32) (n int, err error) {
	if off >= b.cap {
		err = io.ErrShortWrite
		return
	}

	dst := b.data[off:]
	n = copy(dst, d)
	if off+int32(n) > b.size {
		b.size = off + int32(n)
	}
	if n < min(len(dst), len(d)) || n < len(d) {
		err = io.ErrShortWrite
		return
	}
	return
}

// ------------------------------------------------------------------

// map struct block into bytes array
// [block header, data]
func mapBlock(addr []byte) *block {
	blkSize := int32(len(addr)) - blockHeaderSize
	if blkSize <= 0 {
		panic("not enough space to hold a block")
	}

	// map header
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&addr))
	blk := (*block)(unsafe.Pointer(sl.Data))

	// map data
	dsl := (*reflect.SliceHeader)(unsafe.Pointer(&blk.data))
	dsl.Len = int(blkSize)
	dsl.Cap = int(blkSize)
	dsl.Data = sl.Data + blockHeaderSize
	return blk
}

// create a new block at addr
func newBlock(addr []byte, fd uint32, idx, cap int32) (b *block, err error) {
	b = mapBlock(addr)
	if b.magic == blockMagic {
		return nil, fmt.Errorf("block at %v is already exist", &addr)
	}
	if int(cap) != len(b.data) {
		return nil, fmt.Errorf("block data len %v != cap %v", len(b.data), cap)
	}

	b.magic = blockMagic
	b.idx = idx
	b.size = 0
	b.cap = cap
	b.fd = fd
	b.flag = 0
	return
}

// open an exist block at addr
func openBlock(addr []byte) (b *block, err error) {
	b = mapBlock(addr)
	if b.magic != blockMagic {
		return nil, errNotBlock
	}
	if int(b.cap) != len(b.data) {
		return nil, fmt.Errorf("block data len %v != cap %v", len(b.data), b.cap)
	}
	return
}

// ------------------------------------------------------------------
