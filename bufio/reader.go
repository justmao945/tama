package bufio

import "io"

// Buffered reader to reduce syscall.
type Reader struct {
	reader io.Reader
	data   []byte
	begin  int
	end    int
}

// NewReader creates a buffered reader upon r, default capacity is 4MB if is 0.
func NewReader(r io.Reader, capacity int) *Reader {
	if capacity == 0 {
		capacity = 4 * 1024 * 1024 // 4MB
	}
	return &Reader{
		reader: r,
		data:   make([]byte, capacity),
		begin:  0,
		end:    0,
	}
}

// Len returns left bytes len
func (b *Reader) Len() int {
	return b.end - b.begin
}

func (b *Reader) Reset(r io.Reader) {
	b.reader = r
	b.begin = 0
	b.end = 0
}

// Read reads up to len(p) bytes into p.  It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered.  Even if Read
// returns n < len(p), it may use all of p as scratch space during the call.
// If some data is available but not len(p) bytes, Read conventionally
// returns what is available instead of waiting for more.
//
// When Read encounters an error or end-of-file condition after
// successfully reading n > 0 bytes, it returns the number of
// bytes read.  It may return the (non-nil) error from the same call
// or return the error (and n == 0) from a subsequent call.
// An instance of this general case is that a Reader returning
// a non-zero number of bytes at the end of the input stream may
// return either err == EOF or err == nil.  The next Read should
// return 0, EOF regardless.
//
// Callers should always process the n > 0 bytes returned before
// considering the error err.  Doing so correctly handles I/O errors
// that happen after reading some bytes and also both of the
// allowed EOF behaviors.
//
// Implementations of Read are discouraged from returning a
// zero byte count with a nil error, except when len(p) == 0.
// Callers should treat a return of 0 and nil as indicating that
// nothing happened; in particular it does not indicate EOF.
//
// Implementations must not retain p.
func (b *Reader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return
	}

	if b.begin == b.end {
		b.begin = 0
		b.end, err = b.reader.Read(b.data)
	}

	end := b.begin + len(p)
	if end > b.end {
		end = b.end
	}

	n = copy(p, b.data[b.begin:end])
	b.begin += n
	return
}
