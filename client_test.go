package bloomd

import (
	"os"
	"testing"
)

func TestClientConnect(t *testing.T) {
	c, err := NewFromAddr(getBloomdAddr())
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.sendAndReceive([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}

	if resp != "Client Error: Command not supported" {
		t.Error("Wrong resp received", resp)
	}

	err = c.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientConnectionError(t *testing.T) {
	_, err := NewFromAddr("foo")
	if err.Error() != "error: could not create socket (address foo: missing port in address)" {
		t.Error(err)
	}
}

func getBloomdAddr() string {
	addr := os.Getenv("BLOOMD")
	if addr == "" {
		addr = "localhost:8673"
	}

	return addr
}
