package mmap

import "sync"

type tbl struct {
	m map[int64][]byte // idx -> memroy mapped file
	l sync.RWMutex
}

func newTbl() *tbl {
	return &tbl{m: make(map[int64][]byte)}
}

func (t *tbl) Get(idx int64, f func() ([]byte, error)) (b []byte, err error) {
	t.l.RLock()
	b, ok := t.m[idx]
	t.l.RUnlock()

	if ok {
		return
	}

	t.l.Lock()
	defer t.l.Unlock()

	b, ok = t.m[idx]
	if ok {
		return
	}

	b, err = f()
	if err != nil {
		return
	}

	t.m[idx] = b
	return
}

func (t *tbl) Values() (res [][]byte) {
	t.l.RLock()
	for _, v := range t.m {
		res = append(res, v)
	}
	t.l.RUnlock()
	return
}
