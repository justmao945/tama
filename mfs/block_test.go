package mfs

import (
	"io"
	"io/ioutil"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlock(t *testing.T) {
	blkSize := int32(1 << 20)
	flen := realBlockSize(blkSize)

	f, err := ioutil.TempFile("", "blk")
	require.NoError(t, err)
	defer f.Close()
	defer os.Remove(f.Name())

	t.Log(f.Name())

	err = f.Truncate(int64(flen))
	require.NoError(t, err)

	addr, err := syscall.Mmap(int(f.Fd()), 0, int(flen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	require.NoError(t, err)
	defer syscall.Munmap(addr)

	blk, err := openBlock(addr)
	require.Equal(t, errNotBlock, err)
	require.Nil(t, blk)

	blk, err = newBlock(addr, 1, 2, blkSize)
	require.NoError(t, err)

	b := make([]byte, 100)
	n, err := blk.ReadAt(b, 0)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)

	n, err = blk.WriteAt([]byte("hello"), 100)
	require.NoError(t, err)
	require.Equal(t, 5, n)

	n, err = blk.ReadAt(b[:2], 100)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("he"), b[:2])

	blk2, err := openBlock(addr)
	require.NoError(t, err)
	require.Equal(t, blkSize, blk2.cap)
	require.Equal(t, int32(100+5), blk2.size)
	require.Equal(t, uint32(1), blk2.fd)
	require.Equal(t, int32(2), blk2.idx)
}
