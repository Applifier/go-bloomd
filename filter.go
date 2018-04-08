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
func (f Filter) BulkSet(keyset KeySet) (ResultReader, error) {
	err := f.sendBatchOp("b", keyset)
	if err != nil {
		return nil, f.client.handleWriteError(err)
	}

	return f.receiveBatchResponse(keyset.Length())
}

// MultiCheck checks multiple keys for the filter
func (f Filter) MultiCheck(keyset KeySet) (ResultReader, error) {
	err := f.sendBatchOp("m", keyset)
	if err != nil {
		return nil, f.client.handleWriteError(err)
	}

	return f.receiveBatchResponse(keyset.Length())
}

func (f Filter) receiveBatchResponse(resultLength int) (ResultReader, error) {
	f.client.resultReader.resetLength(resultLength)
	return f.client.resultReader, nil
}

func (f Filter) sendBatchOp(op string, keyset KeySet) error {
	w := f.client.writer
	w.WriteString(op)
	w.WriteByte(itemDelimeter)
	w.WriteString(f.Name)
	w.WriteByte(itemDelimeter)
	w.ReadFrom(keyset.buffer)
	w.WriteByte(cmdDelimeter)
	return w.Flush()
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

	return f.client.resultReader.readLastResult()
}

// Check gets a single key to the bloom
func (f Filter) Check(key Key) (bool, error) {
	err := f.sendSingleOp("c", key)
	if err != nil {
		return false, f.client.handleWriteError(err)
	}

	return f.client.resultReader.readLastResult()
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

func checkResponse(resp string, err error) error {
	if resp != "Done" {
		return Error{
			Message: "invalid response from server: " + resp,
		}
	}

	return err
}
