package lmq

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMQ(t *testing.T) {
	dir, err := ioutil.TempDir("", "lmq")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	mq, err := NewQueue(dir, 100)
	require.NoError(t, err)

	topic, err := mq.Get(0)
	require.NoError(t, err)

	_, err = topic.Peek()
	require.Equal(t, io.EOF, err)
	_, err = topic.Get()
	require.Equal(t, io.EOF, err)

	m := []byte("abcd")
	err = topic.Put(m)
	require.NoError(t, err)
	require.Equal(t, int64(1), topic.Pending())
	require.Equal(t, int64(1), topic.Count())

	woff := newMessage(0, m).size()
	require.Equal(t, woff, mq.getWOff())

	m1, err := topic.Get()
	require.NoError(t, err)
	require.Equal(t, m, m1)
	require.Equal(t, int64(0), topic.Pending())
	require.Equal(t, int64(1), topic.Count())

	_, err = topic.Get()
	require.Equal(t, io.EOF, err)

	m = []byte("1234")
	err = topic.Put(m)
	require.NoError(t, err)

	woff += newMessage(0, m).size()
	require.Equal(t, woff, mq.getWOff())

	m1, err = topic.Peek()
	require.NoError(t, err)
	require.Equal(t, m, m1)

	m1, err = topic.Peek()
	require.NoError(t, err)
	require.Equal(t, m, m1)

	err = topic.Close()
	require.NoError(t, err)

	m1, err = topic.Peek()
	require.NoError(t, err)
	require.Equal(t, m, m1)

	err = topic.Put(m)
	require.Equal(t, ErrClosedTopic, err)

	err = topic.Drop()
	require.NoError(t, err)

	err = topic.Drop()
	require.Equal(t, ErrClosedTopic, err)

	// another topic
	topic, err = mq.Get(10000)
	require.NoError(t, err)

	m = []byte("xyz")
	err = topic.Put(m)
	require.NoError(t, err)

	woff += newMessage(0, m).size()
	require.Equal(t, woff, mq.getWOff())

	m1, err = topic.Peek()
	require.NoError(t, err)
	require.Equal(t, m, m1)

	topic.Drop()

	m = []byte("9876")
	err = topic.Put(m)
	require.NoError(t, err)

	woff += newMessage(0, m).size()
	require.Equal(t, woff, mq.getWOff())

	m1, err = topic.Get()
	require.NoError(t, err)
	require.Equal(t, m, m1)
}

func TestMQParallel(t *testing.T) {
	dir, err := ioutil.TempDir("", "lmq")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	mq, err := NewQueue(dir, 1000)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(200)

	for i := uint32(0); i < 100; i++ {
		go func(i uint32) {
			defer wg.Done()
			topic, err := mq.Get(i)
			require.NoError(t, err)
			for j := 0; j < 100; j++ {
				err = topic.Put([]byte(fmt.Sprint(i)))
				require.NoError(t, err)
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
					time.Sleep(1e7)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, []byte(fmt.Sprint(i)), b)
			}
		}(i)
	}

	wg.Wait()
}
