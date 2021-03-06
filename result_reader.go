package bloomd

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/Applifier/go-bloomd/utils/mathutils"
)

var ErrCursorOverLength = errors.New("resultReader: cursor is over length")

var yesToken = []byte("Yes")
var noToken = []byte("No")

// ResultReader allows to read filter results
type ResultReader interface {
	Next() (bool, error)
	Read(p []bool) (n int, err error)
	Length() int // Usually any Reader implementation should not contain Length() method but from bloomd protocol we always know the result length - so it could be usefull information
	Close() error
}

type resultReader struct {
	length int
	client *Client
	cursor int
}

func (r *resultReader) resetLength(resultLength int) {
	r.length = resultLength
	r.cursor = 0
}

func (r *resultReader) readFirstResult() (bool, error) {
	return r.readResultFor(true, itemDelimeter)
}

func (r *resultReader) readSingleResult() (bool, error) {
	return r.readResultFor(true, cmdDelimeter)
}

func (r *resultReader) readResult() (bool, error) {
	return r.readResultFor(false, itemDelimeter)
}

func (r *resultReader) readLastResult() (bool, error) {
	return r.readResultFor(false, cmdDelimeter)
}

func (r *resultReader) readResultFor(first bool, delimeter byte) (bool, error) {
	s, err := r.readSlice(delimeter)
	if err != nil {
		return false, err
	}
	if first && !isYes(s) && !isNo(s) { // if it is not expected token it is an error
		str, err := r.readToEnd()
		if err != nil {
			return false, r.client.handleReadError(err)
		}
		return false, fmt.Errorf("%s%c%s", s, itemDelimeter, str)
	}
	return isYes(s), nil
}

// handle client error and trims delimeter
func (r *resultReader) readSlice(delimeter byte) ([]byte, error) {
	s, err := r.client.reader.ReadSlice(delimeter)
	if err != nil {
		return nil, r.client.handleReadError(err)
	}
	return s[:len(s)-1], nil
}

func isYes(s []byte) bool {
	return bytes.Equal(s, yesToken)
}

func isNo(s []byte) bool {
	return bytes.Equal(s, noToken)
}

func (r resultReader) Length() int {
	return r.length
}

func (r *resultReader) Next() (bool, error) {
	if r.client.err != nil {
		return false, r.client.err
	}
	r.cursor++
	var s bool
	var err error
	switch {
	case r.cursor == 1:
		if r.length == 1 {
			s, err = r.readSingleResult()
		} else {
			s, err = r.readFirstResult()
		}
		break
	case r.cursor < r.length:
		s, err = r.readResult()
		break
	case r.cursor == r.length:
		s, err = r.readLastResult()
		break
	default:
		return false, ErrCursorOverLength
	}
	if err != nil {
		return false, err
	}
	return s, nil
}

func (r *resultReader) readToEnd() ([]byte, error) {
	if r.cursor < r.length {
		s, err := r.client.reader.ReadSlice(cmdDelimeter)
		r.cursor = r.length
		if err != nil {
			if err == io.EOF {
				return s, nil
			}
			return nil, r.client.handleReadError(err)
		}
	}
	var emptySlice []byte
	return emptySlice, nil
}

func (r *resultReader) Close() error {
	// just read everything left
	_, err := r.readToEnd()
	return err
}

func (rr *resultReader) Read(p []bool) (n int, err error) {
	n = mathutils.MinInt(len(p), rr.length-rr.cursor)
	for i := 0; i < n; i++ {
		p[i], err = rr.Next()
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
