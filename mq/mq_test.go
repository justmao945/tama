package mq

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	idx := &index{PutOff: 1, GetOff: 2, Flag: 1, Count: 100, Pending: 20}
	idx2, err := parseIndex(idx.Bytes())
	require.NoError(t, err)
	require.Equal(t, idx, idx2)
}

func TestMQ(t *testing.T) {
	dir, err := ioutil.TempDir("", "fhck-mq")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	t.Log(dir)

	mq, err := NewQueue(dir)
	require.NoError(t, err)
	defer mq.Close()

	topic, err := mq.Get(0)
	require.NoError(t, err)

	err = topic.Drop([]byte{})
	require.Equal(t, io.EOF, err)
	_, err = topic.Peek()
	require.Equal(t, io.EOF, err)
	_, err = topic.Get()
	require.Equal(t, io.EOF, err)

	err = topic.Put([]byte("abcd"))
	require.NoError(t, err)
	require.Equal(t, int64(1), topic.Pending())
	require.Equal(t, int64(1), topic.Count())

	b, err := topic.Get()
	require.NoError(t, err)
	require.Equal(t, []byte("abcd"), b)
	require.Equal(t, int64(0), topic.Pending())
	require.Equal(t, int64(1), topic.Count())

	_, err = topic.Get()
	require.Equal(t, io.EOF, err)

	err = topic.Put([]byte("1234"))
	require.NoError(t, err)

	b, err = topic.Peek()
	require.NoError(t, err)
	require.Equal(t, []byte("1234"), b)

	b, err = topic.Peek()
	require.NoError(t, err)
	require.Equal(t, []byte("1234"), b)

	err = topic.Close()
	require.NoError(t, err)

	b, err = topic.Peek()
	require.NoError(t, err)
	require.Equal(t, []byte("1234"), b)

	err = topic.Put([]byte("hello"))
	require.Equal(t, ErrClosedTopic, err)

	err = topic.Drop(b)
	require.NoError(t, err)

	err = topic.Drop(b)
	require.Equal(t, ErrClosedTopic, err)
}

func TestMQParallel(t *testing.T) {
	dir, err := ioutil.TempDir("", "fhck-mq")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	t.Log(dir)

	mq, err := NewQueue(dir)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(200)

	for i := uint32(0); i < 100; i++ {
		go func(i uint32) {
			defer wg.Done()
			topic, err := mq.Get(i)
			require.NoError(t, err)
			for j := 0; j < 100; j++ {
				topic.Put([]byte(fmt.Sprint(i)))
			}
		}(i)
	}

	for i := uint32(0); i < 100; i++ {
		go func(i uint32) {
			defer wg.Done()
			topic, err := mq.Get(i)
			require.NoError(t, err)
			for j := 0; j < 100; j++ {
				b, err := topic.Get()
				if err == io.EOF {
					continue
				}
				require.NoError(t, err)
				require.Equal(t, []byte(fmt.Sprint(i)), b)
			}
		}(i)
	}

	wg.Wait()
}
