package skiplist

import (
	"testing"
)

func TestSkipList(t *testing.T) {
	elems := []int{1, 2, 3, 4, 5, 7, 10, 22, 56, 0, 100, 77, 68}

	l := New(func(a, b interface{}) bool {
		return a.(int) < b.(int)
	})
	t.Logf("empty:\n%+v\n%s", l, l.layout())

	// test insert
	for _, v := range elems {
		e, ok := l.Insert(v, v+10000)
		if !ok {
			t.Error(v)
		}
		if e.Key.(int) != v {
			t.Error(v)
		}
		if e.Value.(int) != v+10000 {
			t.Error(v)
		}
	}
	t.Logf("inserted:\n%+v\n%s", l, l.layout())

	// test insert dup
	e, ok := l.Insert(100, 10100)
	if ok {
		t.Error(100)
	}

	e1, ok := l.Find(100)
	if !ok {
		t.Error(100)
	}
	if e1 != e {
		t.Error(100)
	}

	// test String()
	t.Logf("string:\n", l)
	if l.String() != "{0:10000, 1:10001, 2:10002, 3:10003, 4:10004, 5:10005, 7:10007, 10:10010, 22:10022, 56:10056, 68:10068, 77:10077, 100:10100}" {
		t.Error(l)
	}

	// test find
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
		e, ok := l.Find(i)
		if ok {
			if !has {
				t.Error(i)
			}
			if e.Key.(int) != i {
				t.Error(i)
			}
			if e.Value.(int) != i+10000 {
				t.Error(i)
			}
		}
	}

	// test len next
	if len(elems) != l.Len() {
		t.Error(l.Len())
	}

	// test remove
	if l.Remove(1234000) {
		t.Error("remove failed")
	}

	for _, v := range elems {
		l.Remove(v)
		t.Logf("remove %d:\n%+v\n%s", v, l, l.layout())
		if l.Contains(v) {
			t.Error(v)
		}
	}

	if l.Remove(1234000) {
		t.Error("remove failed")
	}
}
