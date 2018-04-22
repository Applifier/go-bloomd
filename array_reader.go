package bloomd

import (
	"sync"
)

type ArrayReader struct {
	arr []Key
	cur int
}

func NewArrayReader(arr ...Key) *ArrayReader {
	return &ArrayReader{
		arr: arr,
	}
}

func (sr *ArrayReader) Next() bool {
	sr.cur++
	if sr.cur > len(sr.arr) {
		return false
	}
	return true
}

func (sr ArrayReader) Current() Key {
	return sr.arr[sr.cur-1]
}

func (sr *ArrayReader) reset(arr []Key) {
	sr.arr = arr
	sr.cur = 0
}

var srPool = sync.Pool{
	New: func() interface{} {
		return NewArrayReader()
	},
}

func AccrueArrayReader(arr []Key) *ArrayReader {
	reader := srPool.Get().(*ArrayReader)
	reader.reset(arr)
	return reader
}

func ReleaseArrayReader(reader *ArrayReader) {
	srPool.Put(reader)
}
