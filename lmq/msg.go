package lmq

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

var (
	errMismatchTopic = errors.New("mismatch topic")
)

// | len 2B | topic 4B | data
type message struct {
	topic uint32
	data  []byte
}

func newMessage(topic uint32, data []byte) (m *message) {
	if len(data) > math.MaxUint16 {
		panic("too large message")
	}
	return &message{topic: topic, data: data}
}

func (m *message) size() int64 {
	return 2 + 4 + int64(len(m.data))
}

func (m *message) writeAt(w io.WriterAt, off int64) (n int, err error) {
	b := make([]byte, 2+4)
	binary.LittleEndian.PutUint16(b, uint16(len(m.data)))
	binary.LittleEndian.PutUint32(b[2:], m.topic)
	n, err = w.WriteAt(b, off)
	if err != nil {
		return
	}
	n1, err := w.WriteAt(m.data, off+2+4)
	n += n1
	return
}

func readMessageAt(r io.ReaderAt, topic uint32, off int64) (m *message, noff int64, err error) {
	b := make([]byte, 2+4)
	_, err = r.ReadAt(b, off)
	if err != nil {
		return
	}
	len := binary.LittleEndian.Uint16(b)
	noff = off + 2 + 4 + int64(len)

	rtopic := binary.LittleEndian.Uint32(b[2:])
	if topic != rtopic {
		err = errMismatchTopic
		return
	}

	data := make([]byte, len)
	_, err = r.ReadAt(data, off+2+4)

	m = &message{topic: topic, data: data}
	return
}
