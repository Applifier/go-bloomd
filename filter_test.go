package bloomd

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestFilter(t *testing.T) {
	c := createClient(t)
	defer closeClient(t, c)

	t.Run("create filter", func(t *testing.T) {
		f, err := c.CreateFilter(Filter{
			Name:     "somefilter",
			InMemory: true,
		})

		if err != nil {
			t.Fatal(err)
		}

		info, err := f.Info()
		if err != nil {
			t.Error(err)
		}

		if info["capacity"] != "100000" {
			t.Error("Wrong capacity returned")
		}

		t.Run("set key", func(t *testing.T) {
			_, err := f.Set("foo")
			if err != nil {
				t.Fatal(err)
			}

		})

		t.Run("check key", func(t *testing.T) {
			b, err := f.Check("foo")
			if err != nil {
				t.Fatal(err)
			}

			if !b {
				t.Error("Should be found")
			}
		})

		t.Run("check not existing key", func(t *testing.T) {
			b, err := f.Check("dsadasdsa")
			if err != nil {
				t.Fatal(err)
			}

			if b {
				t.Error("Should NOT be found")
			}
		})

		t.Run("set multiple keys", func(t *testing.T) {
			_, err := f.BulkSet([]string{"bar", "baz"})
			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("get multiple keys", func(t *testing.T) {
			resps, err := f.MultiCheck([]string{"foo", "bar", "baz", "biz"})
			if err != nil {
				t.Fatal(err)
			}

			if !(resps[0] == resps[1] == resps[2] == true) {
				t.Error("Wrong responses received")
			}

			if resps[3] {
				t.Error("Biz should not exist")
			}
		})
	})
}

const maxParallelism = 10

func BenchmarkParallelFilterOperations(b *testing.B) {
	filterName := fmt.Sprintf("%s_benchmark_parallel_filter", getBloomdURL(b).Scheme)

	createClientAndFilter := func(b *testing.B, filterName string) (*Client, Filter) {
		c := createClient(b)
		return c, createBenchmarkFilter(b, c, filterName)
	}

	b.Run("Set", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			c, f := createClientAndFilter(b, filterName)
			defer closeClient(b, c)
			for pb.Next() {
				key := fmt.Sprintf("key_%d", rand.Intn(b.N))
				_, err := f.Set(key)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("Check", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			c, f := createClientAndFilter(b, filterName)
			defer closeClient(b, c)
			for pb.Next() {
				key := fmt.Sprintf("key_%d", rand.Intn(b.N))
				_, err := f.Check(key)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("SetAndCheck", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			c, f := createClientAndFilter(b, filterName)
			defer closeClient(b, c)
			for pb.Next() {
				key := fmt.Sprintf("key_%d", rand.Intn(b.N))
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
	})

	c, f := createClientAndFilter(b, filterName)
	defer closeClient(b, c)
	defer dropFilter(b, f)
}

func BenchmarkFilterOperations(b *testing.B) {
	c := createClient(b)
	defer closeClient(b, c)

	b.Run("Set", func(b *testing.B) {
		f := createBenchmarkFilter(b, c, "benchmark_filter_set")
		defer dropFilter(b, f)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key_%d", i)
			_, err := f.Set(key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("CheckPositives", func(b *testing.B) {
		f := createBenchmarkFilter(b, c, "benchmark_filter_check_positives")
		defer dropFilter(b, f)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key_%d", i)
			_, err := f.Set(key)
			if err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key_%d", i)
			_, err := f.Check(key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("CheckNegatives", func(b *testing.B) {
		f := createBenchmarkFilter(b, c, "benchmark_filter_check_negatives")
		defer dropFilter(b, f)

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key_%d", i)
			_, err := f.Check(key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func createBenchmarkFilter(b *testing.B, c *Client, name string) Filter {
	f, err := c.CreateFilter(Filter{
		Name:     fmt.Sprintf("run_%s_%d", name, b.N),
		InMemory: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	return f
}

func dropFilter(b *testing.B, f Filter) {
	if err := f.Drop(); err != nil {
		b.Fatal(err)
	}
}
