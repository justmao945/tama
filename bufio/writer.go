package bufio

import "io"

type Writer struct {
	writer io.Writer
	data   []byte
	begin  int
	end    int
}

func NewWriter(w io.Writer, capacity int) *Writer {
	if capacity == 0 {
		capacity = 4 * 1024 * 1024 // 4MB
	}
	return &Writer{
		writer: w,
		data:   make([]byte, capacity),
		begin:  0,
		end:    0,
	}
}

func (b *Writer) Reset(w io.Writer) {
	b.writer = w
	b.begin = 0
	b.end = 0
}

func (b *Writer) Len() int {
	return b.end - b.begin
}

// Writer is the interface that wraps the basic Write method.
//
// Write writes len(p) bytes from p to the underlying data stream.
// It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
// Write must return a non-nil error if it returns n < len(p).
// Write must not modify the slice data, even temporarily.
//
// Implementations must not retain p.
func (b *Writer) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return
	}

	if b.end+len(p) >= len(b.data) { // time to flush data
		err = b.Flush()
		if err != nil {
			return
		}
	}

	// write to underlying writer directly
	if len(p) >= len(b.data) {
		if b.Len() != 0 {
			panic("bug")
		}
		n, err = b.writer.Write(p)
		return
	}

	n = copy(b.data[b.end:], p)
	b.end += n
	return
}

func (b *Writer) Flush() (err error) {
	n, err := b.writer.Write(b.data[b.begin:b.end])
	b.begin += n
	if b.begin == b.end { // reset when all success
		b.begin = 0
		b.end = 0
	}
	return
}
