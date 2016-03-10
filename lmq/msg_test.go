package lmq

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessage(t *testing.T) {
	f, err := ioutil.TempFile("", "msg")
	require.NoError(t, err)
	defer f.Close()

	t.Log(f.Name())

	var off int64
	for i := 0; i < 100; i++ {
		for j := 0; j < 100; j++ {
			data := []byte(fmt.Sprint(i))
			m := newMessage(uint32(j), data)
			n, err := m.writeAt(f, off)
			require.NoError(t, err)
			require.Equal(t, m.size(), int64(n))
			off += m.size()
		}
	}

	off = 0
	for i := 0; i < 100; i++ {
		for j := 0; j < 100; j++ {
			m, noff, err := readMessageAt(f, uint32(j), off)
			require.NoError(t, err, "d: %v, t: %v", i, j)
			data := []byte(fmt.Sprint(i))
			require.Equal(t, data, m.data)
			off = noff
		}
	}
}
