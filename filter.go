package bloomd

import (
	"strings"
)

// Filter represents a single filter in the bloomd server
type Filter struct {
	Name string

	client *Client
}

var yes = []byte("Yes")

// BulkSet adds multiple keys to the filter
func (f Filter) BulkSet(reader KeyReader) (ResultReader, error) {
	count, err := f.sendBatchOp("b", reader)
	if err != nil {
		return nil, f.client.handleWriteError(err)
	}

	return f.readerFor(count), nil
}

// MultiCheck checks multiple keys for the filter
func (f Filter) MultiCheck(reader KeyReader) (ResultReader, error) {
	count, err := f.sendBatchOp("m", reader)
	if err != nil {
		return nil, f.client.handleWriteError(err)
	}

	return f.readerFor(count), nil
}

func (f Filter) sendBatchOp(op string, reader KeyReader) (int, error) {
	count := 0
	w := f.client.writer
	w.WriteString(op)
	w.WriteByte(itemDelimeter)
	w.WriteString(f.Name)
	for reader.Next() {
		count++
		w.WriteByte(itemDelimeter)
		w.Write(reader.Current())
	}
	w.WriteByte(cmdDelimeter)
	return count, w.Flush()
}

// Clear clears the filter
func (f Filter) Clear() error {
	return checkResponse(f.client.sendAndReceive([]byte("clear " + f.Name)))
}

// Close closes the filter on the server
func (f Filter) Close() error {
	return checkResponse(f.client.sendAndReceive([]byte("close " + f.Name)))
}

// Drop drops the filter on the server
func (f Filter) Drop() error {
	return checkResponse(f.client.sendAndReceive([]byte("drop " + f.Name)))
}

// Flush force flushes the filter
func (f Filter) Flush() error {
	return checkResponse(f.client.sendAndReceive([]byte("flush " + f.Name)))
}

// Info returns info map from the server
func (f Filter) Info() (map[string]string, error) {
	if err := f.client.send([]byte("info " + f.Name)); err != nil {
		return nil, err
	}

	lines, err := f.client.readList()
	if err != nil {
		return nil, err
	}

	resp := map[string]string{}

	for _, line := range lines {
		split := strings.SplitN(line, " ", 2)
		resp[split[0]] = split[1]
	}

	return resp, nil
}

// Set sets a single key to the bloom
func (f Filter) Set(key Key) (bool, error) {
	err := f.sendSingleOp("s", key)
	if err != nil {
		return false, f.client.handleWriteError(err)
	}

	return f.readSingle()
}

// Check gets a single key to the bloom
func (f Filter) Check(key Key) (bool, error) {
	err := f.sendSingleOp("c", key)
	if err != nil {
		return false, f.client.handleWriteError(err)
	}

	return f.readSingle()
}

func (f Filter) sendSingleOp(op string, key Key) error {
	w := f.client.writer
	w.WriteString(op)
	w.WriteByte(itemDelimeter)
	w.WriteString(f.Name)
	w.WriteByte(itemDelimeter)
	w.Write(key)
	w.WriteByte(cmdDelimeter)
	return w.Flush()
}

func (f Filter) readerFor(resultLength int) ResultReader {
	f.client.resultReader.resetLength(resultLength)
	return f.client.resultReader
}

func (f Filter) readSingle() (bool, error) {
	r := f.readerFor(1)
	defer r.Close()
	return r.Next()
}

func checkResponse(resp string, err error) error {
	if resp != "Done" {
		return Error{
			Message: "invalid response from server: " + resp,
		}
	}

	return err
}
