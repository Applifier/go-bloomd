package aggregation

import (
	"errors"
	"sync"

	bloomd "github.com/Applifier/go-bloomd"
)

var ErrCursorOverLength = errors.New("ResultsSet:reader cursor is over length")

// ResultsSet is an aggregator for multiple filter result readers
type ResultsSet struct {
	internal     []bool
	readerCursor int
}

const defaultResultSetLength = 100

var resultSetPool = sync.Pool{
	New: func() interface{} {
		return &ResultsSet{
			internal: make([]bool, defaultResultSetLength, defaultResultSetLength),
		}
	},
}

// GetResultSet fetches results set of provided length from pool
func GetResultSet(length int) *ResultsSet {
	rs := resultSetPool.Get().(*ResultsSet)
	rs.reset(length)
	return rs
}

// FillFromReader makes initial load for result set from provided reader
func (rs *ResultsSet) FillFromReader(reader bloomd.ResultReader) error {
	defer reader.Close()
	for i := 0; i < rs.Length(); i++ {
		next, err := reader.Next()
		if err != nil {
			return err
		}
		rs.set(i, next)
	}
	return nil
}

// MergeFromReader merge results from reader with results that are already in set, merge is made for each corresponding pair using "or" logical operation
func (rs *ResultsSet) MergeFromReader(reader bloomd.ResultReader) error {
	defer reader.Close()
	for i := 0; i < rs.Length(); i++ {
		next, err := reader.Next()
		if err != nil {
			return err
		}
		rs.swapIf(i, next)
	}
	return nil
}

func (rs *ResultsSet) Length() int {
	return len(rs.internal)
}

func (rs *ResultsSet) reset(length int) {
	rs.readerCursor = 0
	if cap(rs.internal) >= length {
		rs.internal = rs.internal[:length]
	} else {
		rs.internal = make([]bool, length, length)
	}
}

func (rs *ResultsSet) set(i int, val bool) {
	rs.internal[i] = val
}

func (rs *ResultsSet) swapIf(i int, val bool) {
	if val && !rs.internal[i] {
		rs.internal[i] = val
	}
}

func (rs *ResultsSet) or(i int, val bool) bool {
	return val || rs.internal[i]
}

// Next returns value for current cursor position and increments internal cursor
func (rs *ResultsSet) Next() (bool, error) {
	rs.readerCursor++
	if rs.readerCursor > rs.Length() {
		return false, ErrCursorOverLength
	}
	return rs.internal[rs.readerCursor-1], nil
}

// Read reads content of result set into provided array
func (rs *ResultsSet) Read(p []bool) (int, error) {
	n := copy(p, rs.internal[rs.readerCursor:len(rs.internal)])
	rs.readerCursor = rs.readerCursor + n
	return n, nil
}

// Close returns result set into the pool
func (rs *ResultsSet) Close() error {
	releaseResultSet(rs)
	return nil
}

func releaseResultSet(rs *ResultsSet) {
	resultSetPool.Put(rs)
}
