package mmq

import (
	"errors"
	"io"
	"sync"
)

var (
	// ErrFullTopic indicates this topic is full, and can not put anymore.
	ErrFullTopic = errors.New("full topic")

	// ErrClosedTopic indicates put to closed topic
	ErrClosedTopic = errors.New("closed topic")

	// ErrInvalidMsg indicates an invalid message
	ErrInvalidMsg = errors.New("invalid message")

	// ErrTooLargeMsg indicates message length exceed the max cap.
	ErrTooLargeMsg = errors.New("too large message")
)

// Topic is a FIFO queue to put and get messages.
type Topic struct {
	q      *Queue
	id     string
	msgs   []*Message
	cnt    int // all messags received
	closed bool
	l      sync.RWMutex
}

func newTopic(q *Queue, id string) (t *Topic, err error) {
	t = &Topic{q: q, id: id}
	return
}

// ID returns unique topic id
func (t *Topic) ID() string {
	return t.id
}

// Count returns all message had been put to this topic
func (t *Topic) Count() int {
	t.l.RLock()
	defer t.l.RUnlock()

	return t.cnt
}

// Pending returns messages can be get.
func (t *Topic) Pending() int {
	t.l.RLock()
	defer t.l.RUnlock()

	return len(t.msgs)
}

// Close marks this topic as closed. Message can not be put to this topic after closed,
// but is still able to read pending messages.
func (t *Topic) Close() (err error) {
	t.l.Lock()
	defer t.l.Unlock()

	t.closed = true
	return
}

func (t *Topic) peek() (m *Message, err error) {
	if len(t.msgs) == 0 {
		if t.closed {
			err = ErrClosedTopic
		} else {
			err = io.EOF
		}
		return
	}
	m = t.msgs[0]
	return
}

// Peek only returns the message, won't drop it.
func (t *Topic) Peek() (m *Message, err error) {
	t.l.RLock()
	defer t.l.RUnlock()

	return t.peek()
}

func (t *Topic) drop(m *Message) (err error) {
	if len(t.msgs) == 0 {
		if t.closed {
			err = ErrClosedTopic
		} else {
			err = io.EOF
		}
		return
	}
	if t.msgs[0].ID() != m.ID() {
		err = ErrInvalidMsg
		return
	}
	t.msgs = t.msgs[1:]
	return
}

// Drop should only be called after Peek and with the right message
func (t *Topic) Drop(m *Message) error {
	t.l.Lock()
	defer t.l.Unlock()

	return t.drop(m)
}

// Get returns a message in topic and drop it.
func (t *Topic) Get() (m *Message, err error) {
	t.l.Lock()
	defer t.l.Unlock()

	m, err = t.peek()
	if err != nil {
		return
	}

	err = t.drop(m)
	return
}

// Put a message to this topic.
func (t *Topic) Put(m *Message) (err error) {
	if len(m.Data()) > t.q.MsgCap {
		err = ErrTooLargeMsg
		return
	}

	t.l.Lock()
	defer t.l.Unlock()

	if t.closed {
		err = ErrClosedTopic
		return
	}

	if len(t.msgs) >= t.q.TopicCap {
		err = ErrFullTopic
		return
	}

	t.cnt++
	t.msgs = append(t.msgs, m)
	return
}
