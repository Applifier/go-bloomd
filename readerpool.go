package bloomd

import (
	"bufio"
	"io"
	"sync"
)

type readerpool struct {
	internal sync.Pool
}

func newReaderPool(size int) *readerpool {
	return &readerpool{
		internal: sync.Pool{
			New: func() interface{} {
				return bufio.NewReaderSize(nil, size)
			},
		},
	}
}

func (pool *readerpool) Get(r io.Reader) *bufio.Reader {
	bufReader := pool.internal.Get().(*bufio.Reader)
	bufReader.Reset(r)

	return bufReader
}

func (pool *readerpool) Put(r *bufio.Reader) {
	pool.internal.Put(r)
}
