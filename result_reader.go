package bloomd

import (
	"bytes"
	"io"
)

type ResultReader interface {
	Next() (bool, error)
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

func (r *resultReader) readResult() (bool, error) {
	return r.readResultFor(itemDelimeter)
}

func (r *resultReader) readLastResult() (bool, error) {
	return r.readResultFor(cmdDelimeter)
}

func (r *resultReader) readResultFor(delimeter byte) (bool, error) {
	s, err := r.client.reader.ReadSlice(delimeter)
	if err != nil {
		return false, r.client.handleReadError(err)
	}
	return bytes.Equal(s[:len(yes)], yes), nil
}

func (r *resultReader) Next() (bool, error) {
	if r.client.err != nil {
		return false, r.client.err
	}
	r.cursor++
	var s bool
	var err error
	switch {
	case r.cursor < r.length:
		s, err = r.readResult()
		break
	case r.cursor == r.length:
		s, err = r.readLastResult()
		break
	default:
		return false, io.EOF
	}
	if err != nil {
		return false, err
	}
	return s, nil
}

func (r *resultReader) Close() error {
	// just read everything left
	if r.cursor < r.length {
		_, err := r.client.reader.ReadSlice(cmdDelimeter)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return r.client.handleReadError(err)
		}
	}
	return nil
}
