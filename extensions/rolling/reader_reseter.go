package rolling

import (
	bloomd "github.com/Applifier/go-bloomd"
)

type KeyReaderReseter interface {
	bloomd.KeyReader
	Reset()
}

type ArrayReaderReseter struct {
	arr []bloomd.Key
	cur int
}

func (sr *ArrayReaderReseter) Next() bool {
	sr.cur++
	if sr.cur > len(sr.arr) {
		return false
	}
	return true
}

func (sr ArrayReaderReseter) Current() bloomd.Key {
	return sr.arr[sr.cur-1]
}

func (sr *ArrayReaderReseter) Reset() {
	sr.cur = 0
}

func NewArrayReaderReseter(keys ...bloomd.Key) *ArrayReaderReseter {
	return &ArrayReaderReseter{
		arr: keys,
	}
}
