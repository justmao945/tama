package mq

import (
	"sync"

	"github.com/justmao945/tama/mfs"
)

// Queue can have many topics.
type Queue struct {
	vol    *mfs.Volume
	topics map[uint32]*Topic
	l      sync.RWMutex
}

// NewQueue create a message queue.
func NewQueue(path string) (q *Queue, err error) {
	// pre-alloc a 256M round file with 512K/Block
	vol, err := mfs.Open(path, &mfs.Config{RoundSize: 256 << 20, BlockSize: 512 << 10})
	if err != nil {
		return
	}

	q = &Queue{vol: vol, topics: make(map[uint32]*Topic)}

	for _, f := range vol.Files() {
		_, err = q.Get(f.Fd())
		if err != nil {
			return
		}
	}
	return
}

// Topics returns all topics in this queue.
func (q *Queue) Topics() (res []*Topic) {
	q.l.RLock()
	q.l.RUnlock()

	for _, t := range q.topics {
		res = append(res, t)
	}
	return
}

// Get returns a topic from queue, will create a new one if is not exist.
func (q *Queue) Get(id uint32) (t *Topic, err error) {
	q.l.RLock()
	t, ok := q.topics[id]
	q.l.RUnlock()

	if ok {
		return
	}

	q.l.Lock()
	defer q.l.Unlock()

	t, ok = q.topics[id]
	if ok {
		return
	}

	t, err = newTopic(q, id)
	if err != nil {
		return
	}

	q.topics[id] = t
	return
}

// Close release all resources.
func (q *Queue) Close() {
	q.vol.Close()
}
