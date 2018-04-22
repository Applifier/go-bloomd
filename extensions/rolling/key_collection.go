package rolling

import (
	"errors"

	bloomd "github.com/Applifier/go-bloomd"
)

type KeyCollection interface {
	Count() int
	GetBy(index int) bloomd.Key
}

type ArrayCollection struct {
	arr []bloomd.Key
}

func (ac ArrayCollection) Count() int {
	return len(ac.arr)
}

func (ac ArrayCollection) GetBy(index int) bloomd.Key {
	return ac.arr[index]
}

func NewArrayCollection(keys ...bloomd.Key) ArrayCollection {
	return ArrayCollection{
		arr: keys,
	}
}

var EmptyKeyCollection = &emptyKeyCollection{}

type emptyKeyCollection struct{}

func (ec emptyKeyCollection) Count() int {
	return 0
}

func (ec emptyKeyCollection) GetBy(index int) bloomd.Key {
	panic(errors.New("Collection has no keys"))
}

type CollectionReader struct {
	col KeyCollection
	cur int
}

func (cr *CollectionReader) Next() bool {
	cr.cur++
	if cr.cur > cr.col.Count() {
		return false
	}
	return true
}

func (cr CollectionReader) Current() bloomd.Key {
	return cr.col.GetBy(cr.cur - 1)
}

func (cr *CollectionReader) reset(col KeyCollection) {
	cr.col = col
	cr.cur = 0
}

func NewCollectionReader(col KeyCollection) *CollectionReader {
	return &CollectionReader{
		col: col,
	}
}
