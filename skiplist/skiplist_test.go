package skiplist

import (
	"testing"
)

func TestSkipList(t *testing.T) {
	elems := []int{1, 2, 3, 4, 5, 7, 10, 22, 56, 0, 100, 77, 68}

	l := New(func(a, b interface{}) bool {
		return a.(int) < b.(int)
	})
	t.Log(l.layout())

	for _, v := range elems {
		l.Insert(v, v+10000)
	}
	t.Log(l.layout())

	for i := 0; i < 200; i++ {
		has := false
		for _, v := range elems {
			if i == v {
				has = true
				break
			}
		}
		if has != l.Contains(i) {
			t.Error(i)
		}
	}

	//elems2 := []elem{1, 2, 3, 4, 100}
}
