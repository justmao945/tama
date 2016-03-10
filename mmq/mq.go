package mmq

import (
	"errors"
	"sync"
)

var (
	// ErrOutOfTopic indicates can not create topics anymore.
	ErrOutOfTopic = errors.New("out of topic")
)

// Config the message queue.
type Config struct {
	QueueCap int // max num of topics a queue can hold
	TopicCap int // max memory size of messages a topic can hold
	MsgCap   int // max length of a message
}

// DefaultConfig use up to 16GB memory with max 8000 topics, 2MB per topic, 1KB per message.
var DefaultConfig = &Config{
	QueueCap: 8000,
	TopicCap: 2 << 20, // 2MB
	MsgCap:   1 << 10, // 1KB
}

// Queue can have many topics, all messages are kept in memory.
type Queue struct {
	*Config
	topics map[string]*Topic
	l      sync.RWMutex
}

// NewQueue create a message queue in memory.
func NewQueue(cfg *Config) (q *Queue, err error) {
	if cfg == nil {
		cfg = DefaultConfig
	}
	q = &Queue{Config: cfg, topics: make(map[string]*Topic)}
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
func (q *Queue) Get(id string) (t *Topic, err error) {
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

	if len(q.topics) >= q.TopicCap {
		err = ErrOutOfTopic
		return
	}

	t, err = newTopic(q, id)
	if err != nil {
		return
	}

	q.topics[id] = t
	return
}
