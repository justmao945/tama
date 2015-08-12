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

// Element represents a node in skip list.
type Element struct {
	Key     interface{}
	Value   interface{}
	forward []*Element // len(forward) is in [1, MaxLevel]
}

func newElement(key, value interface{}, level int) *Element {
	return &Element{
		Key:     key,
		Value:   value,
		forward: make([]*Element, level+1),
	}
}

// Next iterate next element in skip list.
func (e *Element) Next() *Element {
	if e != nil {
		return e.forward[0]
	} else {
		return nil
	}
}

// SkipList represents a linked list that allows fast search within
// an ordered sequence of elements.
type SkipList struct {
	// Less is used to compare keys in skip list, should return true
	// if a is less than b.
	Less func(a, b interface{}) bool

	level int      // [0, MaxLevel)
	head  *Element // forward size is MaxLevel
	rand  *rand.Rand
}

// Creates a new SkipList with the compare function less.
// Function less is used to compare keys to insert.
func New(less func(a, b interface{}) bool) *SkipList {
	return &SkipList{
		Less:  less,
		level: 0,
		head:  newElement(nil, nil, MaxLevel-1),
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
func (s *SkipList) find(key interface{}, update []*Element) *Element {
	cur := s.head
	// go from level top to bottom
	for i := s.level; i >= 0; i-- {
		// go from left to right until equal or greater than key
		for i < len(cur.forward) && cur.forward[i] != nil && s.Less(cur.forward[i].Key, key) {
			cur = cur.forward[i]
		}
		if update != nil {
			// save the point that goes down
			update[i] = cur
		}
	}
	// the node that found, or stopped when not found
	return cur.forward[0]
}

// Insert returns the new element and sets ok to true if added successfully,
// otherwise returns the duplicated one and sets ok to false.
func (s *SkipList) Insert(key, value interface{}) (e *Element, ok bool) {
	update := make([]*Element, MaxLevel)
	cur := s.find(key, update)

	if cur != nil && s.equal(cur.Key, key) {
		e = cur
		ok = false
		return
	}

	level := s.randomLevel()
	// may have several new levels
	if level > s.level {
		for i := s.level + 1; i <= level; i++ {
			// need save to link the new node
			update[i] = s.head
		}
		s.level = level
	}

	cur = newElement(key, value, level)
	e = cur
	ok = true

	// insert the new node after all nodes in update[] for all levels
	for i := 0; i <= level; i++ {
		cur.forward[i] = update[i].forward[i]
		update[i].forward[i] = cur
	}

	return
}

// Remove returns false if key is not in skip list.
func (s *SkipList) Remove(key interface{}) bool {
	update := make([]*Element, MaxLevel)
	cur := s.find(key, update)

	if !s.equal(cur.Key, key) {
		return false
	}

	for i := 0; i <= s.level; i++ {
		if update[i].forward[i] != cur {
			break // stop when hit the top
		}
		// remove current node
		update[i].forward[i] = cur.forward[i]
	}

	// shrink level if has no element
	for s.level > 0 && s.head.forward[s.level] == nil {
		s.level--
	}

	return true
}

// Find returns the element and sets ok to true if key is in the skip list,
// otherwise sets ok to false.
func (s *SkipList) Find(key interface{}) (e *Element, ok bool) {
	e = s.find(key, nil)
	if e != nil && s.equal(e.Key, key) {
		ok = true
	} else {
		ok = false
	}
	return
}

// Contains test whether the key is in the list or not.
func (s *SkipList) Contains(key interface{}) bool {
	_, ok := s.Find(key)
	return ok
}

func (s *SkipList) Len() int {
	i := 0
	for p := s.Front(); p != nil; p = p.Next() {
		i++
	}
	return i
}

func (s *SkipList) Front() *Element {
	return s.head.forward[0]
}

func (s *SkipList) layout() string {
	var buf bytes.Buffer
	for i := 0; i < s.level; i++ {
		buf.WriteString(fmt.Sprintf("%d: ", i))
		for cur := s.head.forward[i]; cur != nil; cur = cur.forward[i] {
			if cur != s.head.forward[i] {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("%p:%v:%v", cur, cur.Key, cur.Value))
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// String returns the representation of the entire list:
//	 {k1:v1, k2:v2, ...}
func (s *SkipList) String() string {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for cur := s.Front(); cur != nil; cur = cur.Next() {
		if cur != s.Front() {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v:%v", cur.Key, cur.Value))
	}
	buf.WriteByte('}')
	return buf.String()
}
