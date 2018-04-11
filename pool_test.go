package bloomd

import (
	"math/rand"
	"net/url"
	"testing"

	"github.com/Applifier/go-bloomd/utils/testutils"
)

func TestPool(t *testing.T) {
	testutils.TestForAllAddrs(t, func(url *url.URL, t *testing.T) {
		pool, err := NewPoolFromURL(5, 10, url)
		if err != nil {
			t.Fatal(err)
		}

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

		pool.Close()
	})
}

func BenchmarkPool(b *testing.B) {
	testutils.BenchForAllAddrs(b, func(url *url.URL, b *testing.B) {
		pool, err := NewPoolFromURL(30, 50, url)
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			c, err := pool.Get()
			if err != nil {
				b.Fatal(err)
			}
			c.Close()
		}
		pool.Close()
	})
}

func BenchmarkPoolParallel(b *testing.B) {
	testutils.BenchForAllAddrs(b, func(url *url.URL, b *testing.B) {
		b.Run("GetFromPool", func(b *testing.B) {
			pool, err := NewPoolFromURL(30, 50, url)
			if err != nil {
				b.Fatal(err)
			}

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

			pool.Close()
		})

		b.Run("SetCheck", func(b *testing.B) {

			pool, err := NewPoolFromURL(30, 100, url)
			if err != nil {
				b.Fatal(err)
			}

			c, err := pool.Get()
			if err != nil {
				b.Fatal(err)
			}

			f := createBenchmarkFilter(b, url, c, "benchmark_parallel_pool")
			filterName := f.Name

			c.Close()

			b.RunParallel(func(pb *testing.PB) {
				c, err := pool.Get()
				if err != nil {
					b.Fatal(err)
				}
				defer c.Close()
				for pb.Next() {
					f := c.GetFilter(filterName)
					key := keyf("key_%d", rand.Int())
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

			dropFilter(b, f)

			pool.Close()
		})
	})
}
