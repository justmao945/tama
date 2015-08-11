// Package skiplist implements memory skip lists.
package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"
)

var (
	// Probability to go to next level.
	Probability = 0.5
	// With probability 0.5 use log2(n) as MaxLevel,
	// n is amount of elements to be expected for index.
	MaxLevel = 32
)

type node struct {
	key     interface{}
	value   interface{}
	forward []*node
}

func newNode(key, value interface{}, level int) *node {
	return &node{
		key:     key,
		value:   value,
		forward: make([]*node, level+1),
	}
}

// SkipList represents a linked list that allows fast search within
// an ordered sequence of elements.
type SkipList struct {
	// Less is used to compare keys in skip list, should return true
	// if a is less than b.
	Less func(a, b interface{}) bool

	level int
	head  *node
	rand  *rand.Rand
}

func New(less func(a, b interface{}) bool) *SkipList {
	return &SkipList{
		Less:  less,
		level: 0,
		head:  newNode(nil, nil, MaxLevel-1),
		rand:  rand.New(rand.NewSource(time.Now().Unix())),
	}
}

/**
 * Generates a random level number [0, MaxLevel)
 *
 * Distribution (hopefully) for returned value:
 * 0 => 50%
 * 1 => 25%
 * 2 => 12.5%
 * ...
 */
func (s *SkipList) randomLevel() int {
	i := 0
	for s.rand.Float64() < Probability && i < MaxLevel {
		i++
	}
	return i
}

func (s *SkipList) equal(a, b interface{}) bool {
	return !s.Less(a, b) && !s.Less(b, a)
}

// Find element in table and save previous pointer which
// needs to be modified on all levels
func (s *SkipList) find(key interface{}, update []*node) *node {
	cur := s.head
	for i := s.level; i >= 0; i-- {
		for i < len(cur.forward) && cur.forward[i] != nil && s.Less(cur.forward[i].key, key) {
			cur = cur.forward[i]
		}
		if update != nil {
			update[i] = cur
		}
	}
	return cur.forward[0]
}

func (s *SkipList) layout() string {
	var buf bytes.Buffer
	buf.WriteByte('\n')
	for i := 0; i < s.level; i++ {
		buf.WriteString(fmt.Sprintf("%d: ", i))
		for cur := s.head.forward[i]; cur != nil; cur = cur.forward[i] {
			if cur != s.head.forward[i] {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("%p:%v:%v", cur, cur.key, cur.value))
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

func (s *SkipList) String() string {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for cur := s.head.forward[0]; cur != nil; cur = cur.forward[0] {
		if cur != s.head.forward[0] {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v:%v", cur.key, cur.value))
	}
	buf.WriteByte('}')
	return buf.String()
}

func (s *SkipList) Insert(key, value interface{}) bool {
	update := make([]*node, MaxLevel)
	cur := s.find(key, update)

	if cur != nil && s.equal(cur.key, key) {
		return false
	}

	level := s.randomLevel()
	if level > s.level {
		for i := s.level + 1; i <= level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	cur = newNode(key, value, level)

	for i := 0; i <= level; i++ {
		cur.forward[i] = update[i].forward[i]
		update[i].forward[i] = cur
	}

	return true
}

func (s *SkipList) Remove(key interface{}) bool {
	update := make([]*node, MaxLevel)
	cur := s.find(key, update)

	if !s.equal(cur.key, key) {
		return false
	}

	for i := 0; i <= s.level; i++ {
		if update[i].forward[i] != cur {
			break
		}
		update[i].forward[i] = cur.forward[i]
	}

	for s.level > 0 && s.head.forward[s.level] == nil {
		s.level--
	}

	return true
}

func (s *SkipList) Find(key interface{}) (value interface{}, ok bool) {
	cur := s.find(key, nil)
	if cur != nil && s.equal(cur.key, key) {
		value = cur.value
		ok = true
	} else {
		ok = false
	}
	return
}

func (s *SkipList) Contains(key interface{}) bool {
	_, ok := s.Find(key)
	return ok
}
