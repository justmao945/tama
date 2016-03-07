package mq

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"sync"

	"github.com/qiniu/bytes"
)

const (
	headerSize = 48 // 4 + 4 + 5 * 8
	flagClosed = 1
)

var (
	// ErrBrokenIndex indicates the index is broken: crc32 mismatch
	ErrBrokenIndex = errors.New("broken index")

	// ErrClosedTopic indicates put to closed topic
	ErrClosedTopic = errors.New("closed topic")
)

// -------------------------------------------------------------------

type index struct {
	Flag     int32 // index flag
	PutOff   int64
	GetOff   int64
	Count    int64 // all messages put to this Topic
	Pending  int64 // pending messages can be get
	Reserved int64
}

func (i *index) Bytes() []byte {
	b := make([]byte, headerSize)
	w := bytes.NewWriter(b[4:])
	binary.Write(w, binary.LittleEndian, i)
	binary.LittleEndian.PutUint32(b, crc32.ChecksumIEEE(b[4:]))
	return b
}

func parseIndex(b []byte) (idx *index, err error) {
	if binary.LittleEndian.Uint32(b) != crc32.ChecksumIEEE(b[4:]) {
		err = ErrBrokenIndex
		return
	}
	idx = &index{}
	r := bytes.NewReader(b[4:])
	binary.Read(r, binary.LittleEndian, idx)
	return
}

// -------------------------------------------------------------------

// Topic is a FIFO queue to put and get messages.
type Topic struct {
	q   *Queue
	id  uint32
	idx *index
	l   sync.RWMutex
}

func newTopic(q *Queue, id uint32) (t *Topic, err error) {
	f, err := q.vol.Open(id)
	if err != nil {
		return
	}

	idx := &index{PutOff: headerSize, GetOff: headerSize}
	raw := make([]byte, headerSize)
	_, err = f.ReadAt(raw, 0)
	if err == io.EOF { // new index
		err = nil
	} else if err != nil {
		return
	} else {
		idx, err = parseIndex(raw)
		if err != nil {
			return
		}
	}

	_, err = f.WriteAt(idx.Bytes(), 0)
	if err != nil {
		return
	}

	t = &Topic{
		q:   q,
		id:  id,
		idx: idx,
	}
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

	return t.idx.Count
}

// Pending returns messages can be get.
func (t *Topic) Pending() int64 {
	t.l.RLock()
	defer t.l.RUnlock()

	return t.idx.Pending
}

// Close marks this topic as closed. Message can not be put to this topic after closed,
// but is still able to read pending messages.
func (t *Topic) Close() (err error) {
	t.l.Lock()
	defer t.l.Unlock()

	if t.idx.Flag&flagClosed != 0 {
		return
	}

	newIdx := *t.idx
	newIdx.Flag |= flagClosed

	f, err := t.q.vol.Open(t.id)
	if err != nil {
		return
	}

	_, err = f.WriteAt(newIdx.Bytes(), 0)
	if err != nil {
		return
	}

	t.idx.Flag = newIdx.Flag
	return
}

func (t *Topic) peek() (b []byte, err error) {
	f, err := t.q.vol.Open(t.id)
	if err != nil {
		return
	}

	if t.idx.GetOff >= t.idx.PutOff {
		if t.idx.Flag&flagClosed != 0 {
			err = ErrClosedTopic
		} else {
			err = io.EOF
		}
		return
	}

	raw := make([]byte, 2)
	_, err = f.ReadAt(raw, t.idx.GetOff)
	if err != nil {
		return
	}
	len := binary.LittleEndian.Uint16(raw)

	raw = make([]byte, len)
	_, err = f.ReadAt(raw, t.idx.GetOff+2)
	if err != nil {
		return
	}

	b = raw
	return
}

// Peek only returns the message, won't drop it.
func (t *Topic) Peek() (b []byte, err error) {
	t.l.RLock()
	defer t.l.RUnlock()

	return t.peek()
}

func (t *Topic) drop(len int) (err error) {
	if t.idx.GetOff >= t.idx.PutOff {
		if t.idx.Flag&flagClosed != 0 {
			err = ErrClosedTopic
		} else {
			err = io.EOF
		}
		return
	}

	newIdx := *t.idx
	newIdx.GetOff += 2 + int64(len)
	newIdx.Pending--

	f, err := t.q.vol.Open(t.id)
	if err != nil {
		return
	}
	_, err = f.WriteAt(newIdx.Bytes(), 0)
	if err != nil {
		return
	}

	t.idx = &newIdx
	return
}

// Drop should only be called after Peek and with the right message
// FIXME: dangous if drop wrong message...
func (t *Topic) Drop(b []byte) error {
	t.l.Lock()
	defer t.l.Unlock()

	return t.drop(len(b))
}

// Get returns a message in topic and drop it.
func (t *Topic) Get() (b []byte, err error) {
	t.l.Lock()
	defer t.l.Unlock()

	b, err = t.peek()
	if err != nil {
		return
	}

	err = t.drop(len(b))
	return
}

// Put a message to this topic.
func (t *Topic) Put(b []byte) (err error) {
	if len(b) > math.MaxUint16 {
		panic("too large message to put to mq")
	}

	t.l.Lock()
	defer t.l.Unlock()

	if t.idx.Flag&flagClosed != 0 {
		err = ErrClosedTopic
		return
	}

	f, err := t.q.vol.Open(t.id)
	if err != nil {
		return
	}

	raw := make([]byte, 2+len(b))
	binary.LittleEndian.PutUint16(raw, uint16(len(b)))
	copy(raw[2:], b)

	_, err = f.WriteAt(raw, t.idx.PutOff)
	if err != nil {
		return
	}

	newIdx := *t.idx
	newIdx.PutOff += 2 + int64(len(b))
	newIdx.Count++
	newIdx.Pending++
	_, err = f.WriteAt(newIdx.Bytes(), 0)
	if err != nil {
		return
	}

	t.idx = &newIdx
	return
}
