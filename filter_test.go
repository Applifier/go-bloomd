package bloomd

import (
	"fmt"
	"math/rand"
	"net/url"
	"testing"

	"github.com/Applifier/go-bloomd/utils/testutils"
)

func TestFilter(t *testing.T) {
	testutils.TestForAllAddrs(t, func(url *url.URL, t *testing.T) {
		c := createClientFromURL(t, url)

		t.Run("create filter", func(t *testing.T) {
			f, err := c.CreateFilter("somefilter", 0, 0, true)

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
				_, err := f.Set(Key("foo"))
				if err != nil {
					t.Fatal(err)
				}

			})

			t.Run("check key", func(t *testing.T) {
				b, err := f.Check(Key("foo"))
				if err != nil {
					t.Fatal(err)
				}

				if !b {
					t.Error("Should be found")
				}
			})

			t.Run("check not existing key", func(t *testing.T) {
				b, err := f.Check(Key("dsadasdsa"))
				if err != nil {
					t.Fatal(err)
				}

				if b {
					t.Error("Should NOT be found")
				}
			})

			t.Run("set multiple keys", func(t *testing.T) {
				reader := NewArrayReader(Key("bar"), Key("baz"))
				results, err := f.BulkSet(reader)
				defer results.Close()
				if err != nil {
					t.Fatal(err)
				}
			})

			t.Run("check multiple keys", func(t *testing.T) {
				reader := NewArrayReader(Key("foo"), Key("bar"), Key("baz"), Key("biz"))
				resps, err := f.MultiCheck(reader)
				defer resps.Close()
				if err != nil {
					t.Fatal(err)
				}

				if !(next(t, resps) == next(t, resps) == next(t, resps) == true) {
					t.Error("Wrong responses received")
				}

				if next(t, resps) {
					t.Error("Biz should not exist")
				}
			})
		})

		closeClient(t, c)
	})
}

func next(t *testing.T, reader ResultReader) bool {
	next, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	return next
}

const maxParallelism = 10

func BenchmarkParallelFilterOperations(b *testing.B) {
	testutils.BenchForAllAddrs(b, func(url *url.URL, b *testing.B) {
		filterName := fmt.Sprintf("%s_benchmark_parallel_filter", url.Scheme)

		createClientAndFilter := func(b *testing.B, filterName string) (*Client, Filter) {
			c := createClientFromURL(b, url)
			return c, createBenchmarkFilter(b, url, c, filterName)
		}

		b.Run("Set", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				c, f := createClientAndFilter(b, filterName)
				defer closeClient(b, c)
				for pb.Next() {
					key := keyf("key_%d", rand.Intn(b.N))
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
					key := keyf("key_%d", rand.Intn(b.N))
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
					key := keyf("key_%d", rand.Intn(b.N))
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
		dropFilter(b, f)
		closeClient(b, c)
	})
}

func BenchmarkBatchFilterOperations(b *testing.B) {
	testutils.BenchForAllAddrs(b, func(url *url.URL, b *testing.B) {
		c := createClientFromURL(b, url)

		batchLengths := []int{10, 50, 100, 500}
		for _, batchLength := range batchLengths {
			suffixFilter := func(name string) string {
				return fmt.Sprintf("%s_bl%d", name, batchLength)
			}

			readResults := make([]bool, batchLength)
			arr := generateKeysArr(batchLength)

			b.Run(fmt.Sprintf("BulkSet_%d", batchLength), func(b *testing.B) {
				f := createBenchmarkFilter(b, url, c, suffixFilter("benchmark_filter_bulkset"))
				defer dropFilter(b, f)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					reader := GetArrayReader(arr)
					rr, err := f.BulkSet(reader)
					if err != nil {
						b.Fatal(err)
					}
					_, err = rr.Read(readResults)
					if err != nil {
						b.Fatal(err)
					}
					rr.Close()
					PutArrayReader(reader)
				}
			})

			b.Run(fmt.Sprintf("MultiCheck_%d", batchLength), func(b *testing.B) {
				f := createBenchmarkFilter(b, url, c, suffixFilter("benchmark_filter_multicheck"))
				defer dropFilter(b, f)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					reader := GetArrayReader(arr)
					rr, err := f.MultiCheck(reader)
					if err != nil {
						b.Fatal(err)
					}
					_, err = rr.Read(readResults)
					if err != nil {
						b.Fatal(err)
					}
					rr.Close()
					PutArrayReader(reader)
				}
			})
		}

		defer closeClient(b, c)
	})
}

func BenchmarkFilterOperations(b *testing.B) {
	testutils.BenchForAllAddrs(b, func(url *url.URL, b *testing.B) {
		c := createClientFromURL(b, url)

		b.Run("Set", func(b *testing.B) {
			f := createBenchmarkFilter(b, url, c, "benchmark_filter_set")
			defer dropFilter(b, f)

			keys := generateSeqKeys(b.N)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := f.Set(keys[i])
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("CheckPositives", func(b *testing.B) {
			f := createBenchmarkFilter(b, url, c, "benchmark_filter_check_positives")
			defer dropFilter(b, f)

			keys := generateSeqKeys(b.N)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := f.Set(keys[i])
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := f.Check(keys[i])
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("CheckNegatives", func(b *testing.B) {
			f := createBenchmarkFilter(b, url, c, "benchmark_filter_check_negatives")
			defer dropFilter(b, f)

			keys := generateSeqKeys(b.N)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := f.Check(keys[i])
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		closeClient(b, c)
	})
}

func generateKeysArr(count int) []Key {
	arr := make([]Key, 0)
	for i := 0; i < count; i++ {
		arr = append(arr, Key(fmt.Sprintf("key_%d", i)))
	}
	return arr
}

func generateSeqKeys(count int) []Key {
	keys := make([]Key, count)
	for i := 0; i < count; i++ {
		keys[i] = keyf("key_%d", i)
	}
	return keys
}

func createBenchmarkFilter(b *testing.B, url *url.URL, c *Client, name string) Filter {
	b.Helper()
	f, err := c.CreateFilter(fmt.Sprintf("run_%s_u%s_b%d", name, url.Scheme, b.N), 0, 0, true)
	if err != nil {
		b.Fatal(err)
	}
	return f
}

func dropFilter(b *testing.B, f Filter) {
	b.Helper()
	if err := f.Drop(); err != nil {
		b.Fatal(err)
	}
}

func keyf(format string, params ...interface{}) Key {
	return Key(fmt.Sprintf(format, params...))
}
