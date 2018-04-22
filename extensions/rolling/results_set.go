package rolling

import (
	"io"
	"sync"

	bloomd "github.com/Applifier/go-bloomd"
)

type resultsSet struct {
	internal     []bool
	readerCursor int
}

const defaultResultSetLength = 100

var resultSetPool = sync.Pool{
	New: func() interface{} {
		return &resultsSet{
			internal: make([]bool, defaultResultSetLength, defaultResultSetLength),
		}
	},
}

func accrueResultSet(length int) *resultsSet {
	rs := resultSetPool.Get().(*resultsSet)
	rs.reset(length)
	return rs
}

func releaseResultSet(rs *resultsSet) {
	resultSetPool.Put(rs)
}

func (rs *resultsSet) fillFromReader(reader bloomd.ResultReader) error {
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

func (rs *resultsSet) mergeFromReader(reader bloomd.ResultReader) error {
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

func (rs *resultsSet) Length() int {
	return len(rs.internal)
}

func (rs *resultsSet) reset(length int) {
	rs.readerCursor = 0
	if cap(rs.internal) >= length {
		rs.internal = rs.internal[:length]
	} else {
		rs.internal = make([]bool, length, length)
	}
}

func (rs *resultsSet) set(i int, val bool) {
	rs.internal[i] = val
}

func (rs *resultsSet) swapIf(i int, val bool) {
	if val && !rs.internal[i] {
		rs.internal[i] = val
	}
}

func (rs *resultsSet) or(i int, val bool) bool {
	return val || rs.internal[i]
}

func (rs *resultsSet) Next() (bool, error) {
	rs.readerCursor++
	if rs.readerCursor > rs.Length() {
		return false, io.EOF
	}
	return rs.internal[rs.readerCursor-1], nil
}

func (rs *resultsSet) Read(p []bool) (int, error) {
	n := copy(p, rs.internal[rs.readerCursor:len(rs.internal)])
	rs.readerCursor = rs.readerCursor + n
	return n, nil
}

func (rs *resultsSet) Close() error {
	releaseResultSet(rs)
	return nil
}
