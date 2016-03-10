// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package mmap provides a way to memory-map a file.
package mmap

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"syscall"
)

//
// The runtime.SetFinalizer documentation says that, "The finalizer for x is
// scheduled to run at some arbitrary time after x becomes unreachable. There
// is no guarantee that finalizers will run before a program exits", so we
// cannot automatically test that the finalizer runs. Instead, set this to true
// when running the manual test.

type File struct {
	data []byte
}

// Close closes the reader.
func (r *File) Close() error {
	if r.data == nil {
		return nil
	}
	data := r.data
	r.data = nil
	runtime.SetFinalizer(r, nil)
	return syscall.Munmap(data)
}

// Len returns the length of the underlying memory-mapped file.
func (r *File) Size() int {
	return len(r.data)
}

// At returns the byte at index i.
func (r *File) At(i int) byte {
	return r.data[i]
}

func (r *File) PutAt(i int, b byte) {
	r.data[i] = b
}

// ReadAt implements the io.ReadAt interface.
func (r *File) ReadAt(p []byte, off int64) (int, error) {
	if r.data == nil {
		return 0, errors.New("mmap: closed")
	}
	if off < 0 || int64(len(r.data)) < off {
		return 0, fmt.Errorf("mmap: invalid ReadAt offset %d", off)
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *File) WriteAt(p []byte, off int64) (int, error) {
	if r.data == nil {
		return 0, errors.New("mmap: closed")
	}
	if off < 0 || int64(len(r.data)) < off {
		return 0, fmt.Errorf("mmap: invalid WriteAt offset %d", off)
	}
	n := copy(r.data[off:], p)
	if n < len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func Open(name string) (*File, error) {
	return OpenFile(name, 0)
}

// Open memory-maps the named file for reading and writing.
// Readonly if size is 0.
func OpenFile(name string, size int64) (*File, error) {
	if size < 0 {
		return nil, fmt.Errorf("mmap: file %v has negative size", name)
	}
	if size != int64(int(size)) {
		return nil, fmt.Errorf("mmap: file %v is too large", name)
	}

	flag, perm := os.O_CREATE|os.O_RDWR, 0644
	if size == 0 {
		flag, perm = os.O_RDONLY, 0
	}
	f, err := os.OpenFile(name, flag, os.FileMode(perm))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	prot := syscall.PROT_READ
	if size == 0 {
		size = fi.Size()
	} else {
		prot |= syscall.PROT_WRITE
		if size != fi.Size() {
			if fi.Size() != 0 {
				return nil, fmt.Errorf("mmap: the size of file %v is %v != requested %v", name, fi.Size(), size)
			}
			err = f.Truncate(size)
			if err != nil {
				return nil, err
			}
		}
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), prot, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	r := &File{data}
	runtime.SetFinalizer(r, (*File).Close)
	return r, nil
}
