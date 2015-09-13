package bufio

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	b := &bytes.Buffer{}
	wb := NewWriter(b, 4)

	n, err := wb.Write([]byte("12"))
	assert.Equal(t, 2, n)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, b.Len())
	assert.Equal(t, 2, wb.Len())

	n, err = wb.Write([]byte("3456"))
	assert.Equal(t, 4, n)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte("123456"), b.Bytes())
	assert.Equal(t, 0, wb.Len())

	n, err = wb.Write([]byte("7"))
	assert.Equal(t, 1, n)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte("123456"), b.Bytes())
	assert.Equal(t, 1, wb.Len())

	n, err = wb.Write([]byte("8"))
	assert.Equal(t, 1, n)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte("123456"), b.Bytes())
	assert.Equal(t, 2, wb.Len())

	err = wb.Flush()
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte("12345678"), b.Bytes())
	assert.Equal(t, 0, wb.Len())
}
