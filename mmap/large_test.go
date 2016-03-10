package mmap

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLarge(t *testing.T) {
	dir, err := ioutil.TempDir("", "large")
	require.NoError(t, err)

	t.Log(dir)

	f, err := OpenLarge(dir, 1<<10)
	require.NoError(t, err)
	defer f.Close()

	data := []byte("hello world")

	n, err := f.WriteAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	b := make([]byte, len(data))
	n, err = f.ReadAt(b, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, b)

	n, err = f.WriteAt(data, 1020)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	b = make([]byte, len(data))
	n, err = f.ReadAt(b, 1020)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, b)

	syscall.Sync()

	_, err = os.Stat(path.Join(dir, "0"))
	require.NoError(t, err)
	_, err = os.Stat(path.Join(dir, "1"))
	require.NoError(t, err)
}

func TestLargeParallel(t *testing.T) {
	dir, err := ioutil.TempDir("", "large")
	require.NoError(t, err)

	t.Log(dir)

	f, err := OpenLarge(dir, 1<<10)
	require.NoError(t, err)
	defer f.Close()
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprint(i))
			n, err := f.WriteAt(data, int64(i*100))
			require.NoError(t, err)
			require.Equal(t, len(data), n)
		}(i)
	}

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()

			data := []byte(fmt.Sprint(i))
			data1 := make([]byte, len(data))
			for {
				n, err := f.ReadAt(data1, int64(i*100))
				if err == io.EOF {
					time.Sleep(1e7)
					continue
				}
				allZero := true
				for _, b := range data1 {
					if b != 0 {
						allZero = false
					}
				}
				if allZero {
					time.Sleep(1e7)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, data, data1)
				require.Equal(t, len(data1), n)
				break
			}
		}(i)
	}

	wg.Wait()
}
