package bloomd

import (
	"fmt"
	"math/rand"
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

func BenchmarkPool(b *testing.B) {
	pool, err := NewPoolFromAddr(30, 50, getBloomdAddr())
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c, err := pool.Get()
		if err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}

func BenchmarkPoolParallel(b *testing.B) {
	pool, err := NewPoolFromAddr(30, 50, getBloomdAddr())
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c, err := pool.Get()
			if err != nil {
				b.Fatal(err)
			}
			c.Close()
		}
	})
}

func BenchmarkPoolParallelSetCheck(b *testing.B) {
	pool, err := NewPoolFromAddr(30, 100, getBloomdAddr())
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	c, err := pool.Get()
	if err != nil {
		b.Fatal(err)
	}

	_, err = c.CreateFilter(Filter{
		Name: "benchmarkfilter",
	})
	if err != nil {
		b.Fatal(err)
	}

	c.Close()

	b.RunParallel(func(pb *testing.PB) {
		c, err := pool.Get()
		if err != nil {
			b.Fatal(err)
		}
		defer c.Close()
		for pb.Next() {
			f := c.GetFilter("benchmarkfilter")
			key := fmt.Sprintf("key_%d", rand.Int())
			_, err := f.Set(key)
			if err != nil {
				b.Fatal(err)
			}
			_, err = f.Check(key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
