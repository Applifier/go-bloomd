package bloomd

import (
	"testing"
)

func TestPool(t *testing.T) {
	pool, err := NewPoolFromAddr(5, 10, getBloomdAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	c, err := pool.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	resp, err := c.sendAndReceive([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}

	if resp != "Client Error: Command not supported" {
		t.Error("Wrong error received")
	}
}
