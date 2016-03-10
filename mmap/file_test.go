// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mmap

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	tmp, err := ioutil.TempFile("", "mmap")
	require.NoError(t, err)
	tmp.Close()

	size := int64(1 << 20)
	off := int64(100)
	data := []byte("hello")

	m, err := OpenFile(tmp.Name(), size)
	require.NoError(t, err)
	require.NotNil(t, m.data)
	require.Equal(t, size, int64(m.Size()))
	defer m.Close()

	n, err := m.WriteAt(data, off)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	b := make([]byte, len(data))
	n, err = m.ReadAt(b, off)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, b)

	f, err := os.Open(tmp.Name())
	require.NoError(t, err)
	defer f.Close()

	c := make([]byte, len(data))
	n, err = f.ReadAt(c, off)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, b)
}
