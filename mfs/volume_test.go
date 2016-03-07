package mfs

import (
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVolume(t *testing.T) {
	dir, err := ioutil.TempDir("", "volume")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	cfg := &Config{RoundSize: 10 << 10, BlockSize: 1 << 10}

	t.Log(dir)

	// new volume
	v, err := Open(dir, cfg)
	require.NoError(t, err)
	defer v.Close()

	// new file
	f0, err := v.Open(0)
	require.NoError(t, err)

	// read EOF
	b0 := make([]byte, 10)
	n, err := f0.ReadAt(b0, 0)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)

	// wirte normal
	n, err = f0.WriteAt([]byte("1234"), 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	// read normal
	b0 = make([]byte, 2)
	n, err = f0.ReadAt(b0, 1)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("23"), b0)

	// read partial
	b0 = make([]byte, 4)
	n, err = f0.ReadAt(b0, 1)
	require.Equal(t, io.EOF, err)
	require.Equal(t, []byte{'2', '3', '4', 0}, b0)
	require.Equal(t, 3, n)

	// write between blocks
	n, err = f0.WriteAt([]byte("abcd"), 1022)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	// read between blocks
	b0 = make([]byte, 4)
	n, err = f0.ReadAt(b0, 1022)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("abcd"), b0)

	// read at file hole
	b0 = make([]byte, 4)
	b0[2] = 'a'
	n, err = f0.ReadAt(b0, 234)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte{0, 0, 0, 0}, b0)

	// write at edge and have block hole
	n, err = f0.WriteAt([]byte("987654321"), 5022)
	require.NoError(t, err)
	require.Equal(t, 9, n)

	// read between blocks
	b0 = make([]byte, 9)
	n, err = f0.ReadAt(b0, 5022)
	require.NoError(t, err)
	require.Equal(t, 9, n)
	require.Equal(t, []byte("987654321"), b0)

	// read at block hole
	b0 = make([]byte, 9)
	n, err = f0.ReadAt(b0, 4002)
	require.NoError(t, err)
	require.Equal(t, 9, n)

	// new round
	for i := 1; i < 10; i++ {
		f, err := v.Open(uint32(i))
		require.NoError(t, err)
		n, err = f.WriteAt([]byte("xxx"), 0)
		require.NoError(t, err)
		require.Equal(t, 3, n)
	}

	// file in new round
	f10, err := v.Open(10)
	require.NoError(t, err)

	// wirte normal
	n, err = f10.WriteAt([]byte("wxyz"), 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	// write at the block
	n, err = f10.WriteAt([]byte("qwer"), 100)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	// read normal
	b0 = make([]byte, 2)
	n, err = f10.ReadAt(b0, 1)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []byte("xy"), b0)

	b0 = make([]byte, 4)
	n, err = f10.ReadAt(b0, 100)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("qwer"), b0)

	// open new volume to read
	v1, err := Open(dir, cfg)
	require.NoError(t, err)
	f10, err = v1.Open(10)
	require.NoError(t, err)

	b0 = make([]byte, 4)
	n, err = f10.ReadAt(b0, 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("wxyz"), b0)

	b0 = make([]byte, 4)
	n, err = f10.ReadAt(b0, 100)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("qwer"), b0)
}

func TestVolumeConcurrent(t *testing.T) {
	dir, err := ioutil.TempDir("", "volume")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	cfg := &Config{RoundSize: 10 << 10, BlockSize: 1 << 10}

	t.Log(dir)

	// new volume
	v, err := Open(dir, cfg)
	require.NoError(t, err)
	defer v.Close()

	var wg sync.WaitGroup
	wg.Add(1000)

	for i := 0; i < 1000; i++ {
		go func(i int) {
			defer wg.Done()
			f, err := v.Open(uint32(i))
			require.NoError(t, err)
			off := int64(rand.Intn(5000))
			n, err := f.WriteAt([]byte("hello world"), off)
			require.NoError(t, err)
			require.Equal(t, 11, n)
			b := make([]byte, 5)
			n, err = f.ReadAt(b, off)
			require.NoError(t, err)
			require.Equal(t, 5, n)
			require.Equal(t, []byte("hello"), b)
		}(i)
	}
	wg.Wait()
}
