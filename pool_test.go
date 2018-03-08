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

	if pool.Len() != 4 {
		t.Error("Pool should have 4 connections", pool.Len())
	}

	resp, err := c.sendAndReceive([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}

	if resp != "Client Error: Command not supported" {
		t.Error("Wrong error received")
	}

	c.Close()
	if pool.Len() != 5 {
		t.Error("Pool should have 5 connections", pool.Len())
	}
}
