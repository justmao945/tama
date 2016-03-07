package mfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
)

// Config is the global config for mfs.
type Config struct {
	BlockSize int32
	RoundSize int64
}

// DefaultConfig has the default value, with 1MB per block and 1GB per round.
var DefaultConfig = &Config{
	BlockSize: 1 << 20,
	RoundSize: 1 << 30,
}

// Volume can have many files.
type Volume struct {
	*Config
	name  string
	files map[uint32]*File
	rnds  []*round
	l     sync.RWMutex
}

func (v *Volume) String() string {
	v.l.RLock()
	defer v.l.RUnlock()

	return fmt.Sprintf("V{n:%v, BS:%v, RS:%v, f:%v, r:%v}",
		v.Name(), v.BlockSize, v.RoundSize, v.files, v.rnds)
}

// Open an exist volume or create a new volume on disk.
func Open(name string, cfg *Config) (v *Volume, err error) {
	if cfg == nil {
		cfg = DefaultConfig
	}

	err = os.MkdirAll(name, 0755)
	if os.IsExist(err) {
		err = nil
	}
	if err != nil {
		return
	}

	fis, err := ioutil.ReadDir(name)
	if err != nil {
		return
	}

	v0 := &Volume{
		Config: cfg,
		name:   name,
	}

	var rnds []*round
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		// round name is start from 0, and increased automatically
		var idx int64
		idx, err = strconv.ParseInt(fi.Name(), 10, 64)
		if err != nil {
			return
		}

		var r *round
		r, err = newRound(v0, int(idx), cfg.RoundSize, cfg.BlockSize)
		if err != nil {
			return
		}
		rnds = append(rnds, r)
	}
	sort.Sort(byIdx(rnds))

	files := make(map[uint32]*File)

	for i, r := range rnds {
		if r.idx != i {
			panic("invalid load order")
		}
		r.Load(files) // must be loaded from 0 to ...
	}

	v0.rnds = rnds
	v0.files = files
	v = v0
	return
}

// Name is the name, also the path of the volume
func (v *Volume) Name() string {
	return v.name
}

// Files returns all files in this volume.
func (v *Volume) Files() (ret []*File) {
	v.l.RLock()
	for _, f := range v.files {
		ret = append(ret, f)
	}
	v.l.RUnlock()

	sort.Sort(byFd(ret))
	return
}

// Open a file on volume to read and write.
func (v *Volume) Open(fd uint32) (f *File, err error) {
	v.l.RLock()
	f, ok := v.files[fd]
	v.l.RUnlock()

	if ok {
		return
	}

	v.l.Lock()
	defer v.l.Unlock()

	f, ok = v.files[fd]
	if ok {
		return
	}

	f = newFile(v, fd, v.BlockSize)
	v.files[fd] = f
	return
}

// Close unmap all mapped files
func (v *Volume) Close() {
	v.l.Lock()
	defer v.l.Unlock()

	for _, r := range v.rnds {
		r.Close()
	}
}

// alloc returns a block, will create new round if is full for current rounds.
func (v *Volume) alloc(fd uint32, idx, cap int32) (b *block, err error) {
	v.l.Lock() // FIXME too big lock ?
	defer v.l.Unlock()

	var r *round
	n := len(v.rnds)
	if n == 0 || v.rnds[n-1].Full() {
		r, err = newRound(v, n, v.RoundSize, v.BlockSize)
		if err != nil {
			return
		}
		v.rnds = append(v.rnds, r)
	} else {
		r = v.rnds[n-1]
	}

	if cap != v.BlockSize {
		panic("invalid cap")
	}

	return r.Alloc(fd, idx, cap)
}
