package bufio

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReader(t *testing.T) {
	r := strings.NewReader("12345678912")
	rb := NewReader(r, 4)

	p := make([]byte, 2)
	n, err := rb.Read(p)
	assert.Equal(t, 2, n)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte("12"), p)

	p = make([]byte, 4)
	n, err = rb.Read(p)
	assert.Equal(t, 2, n)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{'3', '4', 0, 0}, p)

	p = make([]byte, 6)
	n, err = rb.Read(p)
	assert.Equal(t, 4, n)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{'5', '6', '7', '8', 0, 0}, p)

	p = make([]byte, 6)
	n, err = rb.Read(p)
	assert.Equal(t, 3, n)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{'9', '1', '2', 0, 0, 0}, p)

	p = make([]byte, 2)
	n, err = rb.Read(p)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte{0, 0}, p)

	p = make([]byte, 2)
	n, err = rb.Read(p)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte{0, 0}, p)
}
