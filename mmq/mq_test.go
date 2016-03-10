package mmq

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMQ(t *testing.T) {

	mq, err := NewQueue(nil)
	require.NoError(t, err)

	topic, err := mq.Get("0")
	require.NoError(t, err)

	_, err = topic.Peek()
	require.Equal(t, io.EOF, err)
	_, err = topic.Get()
	require.Equal(t, io.EOF, err)

	m := NewMessage([]byte("abcd"))
	err = topic.Put(m)
	require.NoError(t, err)
	require.Equal(t, 1, topic.Pending())
	require.Equal(t, 1, topic.Count())

	m1, err := topic.Get()
	require.NoError(t, err)
	require.Equal(t, m, m1)
	require.Equal(t, 0, topic.Pending())
	require.Equal(t, 1, topic.Count())

	_, err = topic.Get()
	require.Equal(t, io.EOF, err)

	m = NewMessage([]byte("1234"))
	err = topic.Put(m)
	require.NoError(t, err)

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

	err = topic.Drop(m)
	require.NoError(t, err)

	err = topic.Drop(m)
	require.Equal(t, ErrClosedTopic, err)
}

func TestMQParallel(t *testing.T) {
	mq, err := NewQueue(nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(200)

	for i := uint32(0); i < 100; i++ {
		go func(i uint32) {
			defer wg.Done()
			topic, err := mq.Get(fmt.Sprint(i))
			require.NoError(t, err)
			for j := 0; j < 100; j++ {
				topic.Put(NewMessage([]byte(fmt.Sprint(i))))
			}
		}(i)
	}

	for i := uint32(0); i < 100; i++ {
		go func(i uint32) {
			defer wg.Done()
			topic, err := mq.Get(fmt.Sprint(i))
			require.NoError(t, err)
			for j := 0; j < 100; j++ {
				b, err := topic.Get()
				if err == io.EOF {
					continue
				}
				require.NoError(t, err)
				require.Equal(t, []byte(fmt.Sprint(i)), b.Data())
			}
		}(i)
	}

	wg.Wait()
}
