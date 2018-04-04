package bloomd

import (
	"strings"
)

// Filter represents a single filter in the bloomd server
type Filter struct {
	Name string

	client *Client
}

// BulkSet adds multiple keys to the filter
func (f Filter) BulkSet(keys []string) (responses []bool, err error) {
	resp, err := f.client.sendAndReceive([]byte("b " + f.Name + " " + strings.Join(keys, " ")))
	if err != nil {
		return nil, err
	}

	responses = make([]bool, len(keys))
	respParts := strings.Split(resp, " ")

	for i, respPart := range respParts {
		responses[i] = respPart == "Yes"
	}

	return
}

// MultiCheck checks multiple keys for the filter
func (f Filter) MultiCheck(keys []string) (responses []bool, err error) {
	resp, err := f.client.sendAndReceive([]byte("m " + f.Name + " " + strings.Join(keys, " ")))
	if err != nil {
		return nil, err
	}

	responses = make([]bool, len(keys))
	respParts := strings.Split(resp, " ")

	for i, respPart := range respParts {
		responses[i] = respPart == "Yes"
	}

	return
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
func (f Filter) Set(key string) (bool, error) {
	resp, err := f.client.sendAndReceive([]byte("s " + f.Name + " " + key))
	if err != nil {
		return false, err
	}

	return resp == "Yes", nil
}

// Check gets a single key to the bloom
func (f Filter) Check(key string) (bool, error) {
	resp, err := f.client.sendAndReceive([]byte("c " + f.Name + " " + key))
	if err != nil {
		return false, err
	}

	return resp == "Yes", nil
}

func checkResponse(resp string, err error) error {
	if resp != "Done" {
		return Error{
			Message: "invalid response from server: " + resp,
		}
	}

	return err
}
