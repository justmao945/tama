package lmq

import (
	"errors"
	"io"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

var (
	// ErrClosedTopic indicates put to closed topic
	ErrClosedTopic = errors.New("closed topic")
)

const (
	topicHeaderSize = 64
)

// ------------------------------------------------------------

// saved on disk
type topicHeader struct {
	magic  uint32 // topic header magic
	id     uint32 // topic id
	off    int64  // get from this off
	ped    int64  // pending messages
	cnt    int64  // all messags received
	closed bool
}

func mapTopicHeader(addr []byte) *topicHeader {
	if len(addr) < topicHeaderSize {
		panic("too small place to hold topic header")
	}
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&addr))
	return (*topicHeader)(unsafe.Pointer(sl.Data))
}

// ------------------------------------------------------------

// Topic is a FIFO queue to put and get messages.
type Topic struct {
	*topicHeader
	q *Queue
	l sync.RWMutex
}

func openTopic(q *Queue, h *topicHeader) (t *Topic) {
	if h.magic != magic {
		panic("invalid topic header, magic mismatch")
	}
	t = &Topic{topicHeader: h, q: q}
	return
}

func newTopic(q *Queue, id uint32, h *topicHeader) (t *Topic, err error) {
	if h.magic == magic {
		panic("already have topic header")
	}
	h.magic = magic
	h.id = id
	t = &Topic{topicHeader: h, q: q}
	return
}

// ID returns unique topic id
func (t *Topic) ID() uint32 {
	return t.id
}

// Count returns all message had been put to this topic
func (t *Topic) Count() int64 {
	t.l.RLock()
	defer t.l.RUnlock()

	return t.cnt
}

// Pending returns messages can be get.
func (t *Topic) Pending() int64 {
	t.l.RLock()
	defer t.l.RUnlock()

	return t.ped
}

// Close marks this topic as closed. Message can not be put to this topic after closed,
// but is still able to read pending messages.
func (t *Topic) Close() (err error) {
	t.l.Lock()
	defer t.l.Unlock()

	t.closed = true
	return
}

func (t *Topic) peek() (b []byte, err error) {
	if t.ped == 0 {
		if t.closed {
			err = ErrClosedTopic
		} else {
			err = io.EOF
		}
		return
	}
	var m *message
	off, woff := t.off, t.q.getWOff()
	for i := uint32(1); off < woff; i++ {
		if i%16 == 0 { // release CPU
			time.Sleep(0)
		}
		m, off, err = readMessageAt(t.q.large, t.id, off)
		if err == errMismatchTopic {
			continue
		}
		break
	}
	if err != nil {
		if off == woff {
			err = io.EOF
		} else if err == errMismatchTopic {
			panic("bug")
		}
		return
	}

	t.off = off - m.size()
	b = m.data
	return
}

// Peek only returns the message, won't drop it.
func (t *Topic) Peek() (b []byte, err error) {
	t.l.RLock()
	defer t.l.RUnlock()

	return t.peek()
}

func (t *Topic) drop() (err error) {
	if t.ped == 0 {
		if t.closed {
			err = ErrClosedTopic
		} else {
			err = io.EOF
		}
		return
	}
	_, noff, err := readMessageAt(t.q.large, t.id, t.off)
	if err != nil {
		return
	}
	t.off = noff
	t.ped--
	return
}

// Drop should only be called after Peek and with the right message
func (t *Topic) Drop() error {
	t.l.Lock()
	defer t.l.Unlock()

	return t.drop()
}

// Get returns a message in topic and drop it.
func (t *Topic) Get() (b []byte, err error) {
	t.l.Lock()
	defer t.l.Unlock()

	b, err = t.peek()
	if err != nil {
		return
	}

	err = t.drop()
	return
}

// Put a message to this topic.
func (t *Topic) Put(b []byte) (err error) {
	t.l.Lock()
	defer t.l.Unlock()

	if t.closed {
		err = ErrClosedTopic
		return
	}
	t.ped++
	t.cnt++
	return t.q.appendMessage(t.id, b)
}
