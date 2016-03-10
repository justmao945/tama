package lmq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"syscall"
	"unsafe"

	"github.com/justmao945/tama/mmap"
)

const (
	queueHeaderSize = 1024
)

var (
	magic = binary.LittleEndian.Uint32([]byte("lmq1"))
)

var (
	// ErrOutOfTopic indicates can not create topics anymore.
	ErrOutOfTopic = errors.New("out of topic")
)

// ------------------------------------------------------------

// queue header saved on disk
type queueHeader struct {
	magic     uint32 // magic number
	maxTopics int32  // max num of topics
	woff      int64  // write off of large file
}

func mapQueueHeader(addr []byte) *queueHeader {
	if len(addr) < queueHeaderSize {
		panic("too small place to hold queue header")
	}
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&addr))
	return (*queueHeader)(unsafe.Pointer(sl.Data))
}

// ------------------------------------------------------------

// Queue can have many topics, all messages are write to disk sequencially.
type Queue struct {
	*queueHeader

	name   string
	topics map[uint32]*Topic // memory map

	record []byte      // | queue header | topic headers ...
	large  *mmap.Large // place to hold messages
	l      sync.RWMutex
}

// NewQueue create a message queue.
func NewQueue(name string, maxTopics int32) (q *Queue, err error) {
	err = os.MkdirAll(name, 0755)
	if os.IsExist(err) {
		err = nil
	}
	if err != nil {
		return
	}

	// open record file
	rf, err := os.OpenFile(name+".record", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return
	}
	defer rf.Close()

	rfSize := queueHeaderSize + topicHeaderSize*int64(maxTopics)

	rfi, err := rf.Stat()
	if err != nil {
		return
	}

	create := false
	if rfi.Size() == 0 { // empty record, resize it
		create = true
		err = rf.Truncate(rfSize)
		if err != nil {
			return
		}
	} else if rfi.Size() != rfSize {
		err = fmt.Errorf("mismatch record file size %v != %v", rfi.Size(), rfSize)
		return
	}

	// map record file
	rfaddr, err := syscall.Mmap(int(rf.Fd()), 0, int(rfSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			syscall.Munmap(rfaddr)
		}
	}()

	// read queue header
	qheader := mapQueueHeader(rfaddr)
	if qheader.magic != magic {
		if create {
			qheader.magic = magic
			qheader.maxTopics = maxTopics
		} else {
			err = fmt.Errorf("mismatch magic %v != %v", qheader.magic, magic)
			return
		}
	}
	if qheader.maxTopics != maxTopics {
		err = fmt.Errorf("mismatch max topics %v != %v", qheader.magic, maxTopics)
		return
	}

	// open large file
	large, err := mmap.OpenLarge(name, 0)
	if err != nil {
		return
	}

	// read topic headers
	topics := make(map[uint32]*Topic)
	q = &Queue{queueHeader: qheader, name: name, topics: topics, record: rfaddr, large: large}

	for off := int64(queueHeaderSize); off < rfSize; off += topicHeaderSize {
		theader := mapTopicHeader(rfaddr[off:])
		if theader.magic != magic {
			break
		}
		if _, ok := topics[theader.id]; ok {
			panic("dup topic")
		}
		topics[theader.id] = openTopic(q, theader)
	}

	return
}

// Close release all resources.
func (q *Queue) Close() {
	syscall.Munmap(q.record)
	q.large.Close()
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

func (q *Queue) appendMessage(topic uint32, b []byte) (err error) {
	m := newMessage(topic, b)

	// don't use atomic add, we assume large file is written sequencially.
	q.l.Lock()
	defer q.l.Unlock()

	n, err := m.writeAt(q.large, q.woff)
	if err != nil {
		return
	}
	if int64(n) != m.size() {
		panic("writeAt bug")
	}
	q.woff += m.size()
	return
}

func (q *Queue) getWOff() int64 {
	q.l.RLock()
	defer q.l.RUnlock()

	return q.woff
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

	if len(q.topics) >= int(q.maxTopics) {
		err = ErrOutOfTopic
		return
	}

	off := queueHeaderSize + int64(len(q.topics))*topicHeaderSize
	theader := mapTopicHeader(q.record[off:])

	t, err = newTopic(q, id, theader)
	if err != nil {
		return
	}

	q.topics[id] = t
	return
}
