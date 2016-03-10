package mmq

import "hash/crc32"

// Message can be put to or get from a topic.
type Message struct {
	id   uint32
	data []byte
}

// NewMessage create a message
func NewMessage(b []byte) *Message {
	return &Message{id: crc32.ChecksumIEEE(b), data: b}
}

// ID returns the crc32 of message.
func (m *Message) ID() uint32 {
	return m.id
}

// Data returns the real data.
func (m *Message) Data() []byte {
	return m.data
}
