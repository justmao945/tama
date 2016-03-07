package mfs

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fillZero(b []byte) int {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
	return len(b)
}

// byIdx implements sort.Interface.
type byIdx []*round

func (f byIdx) Len() int           { return len(f) }
func (f byIdx) Less(i, j int) bool { return f[i].idx < f[j].idx }
func (f byIdx) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

// byFd implements sort.Interface.
type byFd []*File

func (f byFd) Len() int           { return len(f) }
func (f byFd) Less(i, j int) bool { return f[i].fd < f[j].fd }
func (f byFd) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
