package bloomd

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Applifier/go-bloomd/utils/testutils"
)

func TestPool(t *testing.T) {
	for _, addr := range testutils.BloomdAddrs() {
		t.Run("Test address "+addr, func(t *testing.T) {
			pool, err := NewPoolFromURL(5, 10, testutils.ParseURL(t, addr))
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
}

func BenchmarkPool(b *testing.B) {
	for _, addr := range testutils.BloomdAddrs() {
		b.Run("Test address "+addr, func(b *testing.B) {
			pool, err := NewPoolFromURL(30, 50, testutils.ParseURL(b, addr))
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
}

func BenchmarkPoolParallel(b *testing.B) {
	for _, addr := range testutils.BloomdAddrs() {
		b.Run("Test address "+addr, func(b *testing.B) {
			url := testutils.ParseURL(b, addr)
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

				f := createBenchmarkFilter(b, c, fmt.Sprintf("%s_benchmark_parallel_pool", url.Scheme))
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
}
