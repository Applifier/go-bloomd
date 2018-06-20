package rolling

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/Applifier/go-bloomd/utils/period"

	"github.com/Applifier/go-bloomd/utils/clock"

	bloomd "github.com/Applifier/go-bloomd"
	"github.com/Applifier/go-bloomd/utils/testutils"
)

// those timeouts are for tests not for benchmarks
var manageOperationTimeout = 1 * time.Second
var runtimeOperationTimeout = 10 * time.Millisecond
var units = []clock.Unit{RollDaily, RollMonthly, RollWeekly}

func TestOperations(t *testing.T) {
	testutils.TestForAllAddrs(t, func(url *url.URL, t *testing.T) {
		filterName := "test_operations_" + url.Scheme
		now := time.Now()
		clock.Static(now)
		defer clock.Reset()
		c := createClientFromURL(t, url)
		rf := createFilter(t, c, filterName, 3, RollWeekly)
		defer dropFilter(t, c, rf)

		t.Run("set key", func(t *testing.T) {
			setShouldAddNew(t, c, rf, "foo")
		})

		t.Run("check key", func(t *testing.T) {
			checkShouldFind(t, c, rf, "foo")
		})

		t.Run("check not existing key", func(t *testing.T) {
			checkShouldNotFind(t, c, rf, "dsadasdsa")
		})

		t.Run("set key on different units", func(t *testing.T) {
			defer clock.Static(now)
			clock.Static(clock.Now().Add(-period.Week))
			setShouldAddNew(t, c, rf, "foo-1")
			clock.Static(clock.Now().Add(-period.Week))
			setShouldAddNew(t, c, rf, "foo-2")
		})

		t.Run("check key on different units", func(t *testing.T) {
			checkShouldFind(t, c, rf, "foo")
			checkShouldFind(t, c, rf, "foo-1")
			checkShouldFind(t, c, rf, "foo-2")
		})

		t.Run("check key not found on last unit if period lower", func(t *testing.T) {
			c := createClientFromURL(t, url)
			rf := createFilter(t, c, filterName, 2, RollWeekly)
			checkShouldFind(t, c, rf, "foo")
			checkShouldFind(t, c, rf, "foo-1")
			checkShouldNotFind(t, c, rf, "foo-2")
		})

		t.Run("set multiple keys", func(t *testing.T) {
			rr := bulkSetShouldlNotFail(t, c, rf, "bar", "baz")
			rr.Close()
		})

		t.Run("get multiple keys", func(t *testing.T) {
			resps := multiCheckShouldlNotFail(t, c, rf, "foo", "bar", "baz", "biz")
			defer resps.Close()

			if !(next(t, resps) == next(t, resps) == next(t, resps) == true) {
				t.Error("Wrong responses received")
			}

			if next(t, resps) {
				t.Error("biz should not exist")
			}
		})

		t.Run("set multiple keys on different units", func(t *testing.T) {
			defer clock.Static(now)
			clock.Static(clock.Now().Add(-period.Week))
			rr := bulkSetShouldlNotFail(t, c, rf, "bar-1", "baz-1")
			rr.Close()
			clock.Static(clock.Now().Add(-period.Week))
			rr = bulkSetShouldlNotFail(t, c, rf, "bar-2", "baz-2")
			rr.Close()
		})

		t.Run("check multiple keys on different units", func(t *testing.T) {
			resps := multiCheckShouldlNotFail(t, c, rf, "foo", "bar-1", "baz-2", "biz")
			defer resps.Close()
			if !(next(t, resps) == next(t, resps) == next(t, resps) == true) {
				t.Error("Wrong responses received")
			}
			if next(t, resps) {
				t.Error("biz should not exist")
			}
		})

		t.Run("check multiple keys not found on last unit if period lower", func(t *testing.T) {
			c := createClientFromURL(t, url)
			rf := createFilter(t, c, filterName, 2, RollWeekly)
			resps := multiCheckShouldlNotFail(t, c, rf, "foo", "bar-1", "baz-2", "bar-2")
			defer resps.Close()
			if !(next(t, resps) == next(t, resps) == true) {
				t.Error("Wrong responses received")
			}
			if next(t, resps) || next(t, resps) {
				t.Error("should not exist")
			}
		})

		t.Run("check multiple non existing keys", func(t *testing.T) {
			resps := multiCheckShouldlNotFail(t, c, rf, "nonexk1", "nonexk2", "nonexk3")
			defer resps.Close()

			if next(t, resps) == next(t, resps) == next(t, resps) == true {
				t.Error("Wrong responses received")
			}
		})
	})
}

func TestFiltersManagement(t *testing.T) {
	testutils.TestForAllAddrs(t, func(url *url.URL, t *testing.T) {
		c := createClientFromURL(t, url)
		namer := NewNamer("test_management_"+url.Scheme, RollWeekly)

		// set current time as zero + 3 weeks
		clock.Static(time.Unix(0, 0).Add(period.Week * 3))
		defer clock.Reset()

		t.Run("test rolling filter create filters including in advance", func(t *testing.T) {
			filter := newFilter(t, namer, RollWeekly, 4)
			err := filter.CreateFilters(getContext(manageOperationTimeout), c, 1, 0, 0, true)
			if err != nil {
				t.Fatal(err)
			}
			checkFilterExists(t, c, namer.NameFor(1))
			checkFilterExists(t, c, namer.NameFor(2))
			checkFilterExists(t, c, namer.NameFor(3))
			checkFilterExists(t, c, namer.NameFor(4))
			checkFilterExists(t, c, namer.NameFor(5)) // created in advance
		})

		t.Run("test rolling filter drops old filters excluding tail", func(t *testing.T) {
			filter := newFilter(t, namer, RollWeekly, 2)
			err := filter.DropOlderFilters(getContext(manageOperationTimeout), c, 1)
			if err != nil {
				t.Fatal(err)
			}
			checkFilterDoesNotExists(t, c, namer.NameFor(0))
			checkFilterExists(t, c, namer.NameFor(2)) // saved as tail
			checkFilterExists(t, c, namer.NameFor(3))
			checkFilterExists(t, c, namer.NameFor(4))
		})

		t.Run("test rolling filter drop all its filters", func(t *testing.T) {
			// set current time as zero + 4 weeks to delete filter created in advance
			clock.Static(time.Unix(0, 0).Add(period.Week * 4))
			filter := newFilter(t, namer, RollWeekly, 5)
			err := filter.Drop(getContext(manageOperationTimeout), c)
			if err != nil {
				t.Fatal(err)
			}
			checkFilterDoesNotExists(t, c, namer.NameFor(1))
			checkFilterDoesNotExists(t, c, namer.NameFor(2))
			checkFilterDoesNotExists(t, c, namer.NameFor(3))
			checkFilterDoesNotExists(t, c, namer.NameFor(4))
			checkFilterDoesNotExists(t, c, namer.NameFor(5)) // created in advance
		})
	})
}

func Disabled_BenchmarkOperationsParallel(b *testing.B) {
	for _, addr := range testutils.BloomdAddrs() {
		url := testutils.ParseURL(b, addr)
		b.Run("Test address "+addr, func(b *testing.B) {
			c := createClientFromURL(b, url)
			cp := createClientPoolFromURL(b, url)
			periods := []clock.UnitNum{1, 5, 10}
			for _, period := range periods {
				b.Run(fmt.Sprintf("MultiCheck-p%d", period), func(b *testing.B) {
					rf := createBenchFilter(b, c, fmt.Sprintf("bench_operations_multicheck_%d_%s", period, url.Scheme), period, RollWeekly)
					defer dropFilter(b, c, rf)

					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							c := getClientFromPool(b, cp)
							ks := generateSeqKeyReaderReseter(10)
							readResults := make([]bool, 10)
							rr, err := rf.MultiCheck(context.Background(), c, ks)
							if err != nil {
								b.Fatal(err)
							}
							_, err = rr.Read(readResults)
							if err != nil {
								b.Fatal(err)
							}
							rr.Close()
							c.Close()
						}
					})
				})

				b.Run(fmt.Sprintf("BulkSet-p%d", period), func(b *testing.B) {
					rf := createBenchFilter(b, c, fmt.Sprintf("bench_operations_bulkset_%d_%s", period, url.Scheme), period, RollWeekly)
					defer dropFilter(b, c, rf)

					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							c := getClientFromPool(b, cp)
							ks := generateSeqKeyReaderReseter(10)
							readResults := make([]bool, 10)
							rr, err := rf.BulkSet(context.Background(), c, ks)
							if err != nil {
								b.Fatal(err)
							}
							_, err = rr.Read(readResults)
							if err != nil {
								b.Fatal(err)
							}
							rr.Close()
							c.Close()
						}
					})
				})
			}
		})
	}
}

func BenchmarkOperations(b *testing.B) {
	for _, addr := range testutils.BloomdAddrs() {
		url := testutils.ParseURL(b, addr)
		b.Run("Test address "+addr, func(b *testing.B) {
			c := createClientFromURL(b, url)
			periods := []clock.UnitNum{1, 5, 10}
			ks := generateSeqKeyReaderReseter(100)
			readResults := make([]bool, 100)
			for _, period := range periods {
				b.Run(fmt.Sprintf("MultiCheck-p%d", period), func(b *testing.B) {
					rf := createBenchFilter(b, c, fmt.Sprintf("bench_operations_multicheck_%d_%s", period, url.Scheme), period, RollWeekly)
					defer dropFilter(b, c, rf)

					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						ks.Reset()
						rr, err := rf.MultiCheck(context.Background(), c, ks)
						if err != nil {
							b.Fatal(err)
						}
						_, err = rr.Read(readResults)
						if err != nil {
							b.Fatal(err)
						}
						rr.Close()
					}
				})

				b.Run(fmt.Sprintf("BulkSet-p%d", period), func(b *testing.B) {
					rf := createBenchFilter(b, c, fmt.Sprintf("bench_operations_bulkset_%d_%s", period, url.Scheme), period, RollWeekly)
					defer dropFilter(b, c, rf)

					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						ks.Reset()
						rr, err := rf.BulkSet(context.Background(), c, ks)
						if err != nil {
							b.Fatal(err)
						}
						_, err = rr.Read(readResults)
						if err != nil {
							b.Fatal(err)
						}
						rr.Close()
					}
				})
			}
		})
	}
}

func checkFilterDoesNotExists(t *testing.T, c *bloomd.Client, name string) {
	t.Helper()
	if err := testutils.Eventually(func() error {
		fs, err := c.ListFilters()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range fs {
			if f.Name == name {
				return fmt.Errorf("Filter with name %s should not exist", name)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func checkFilterExists(t *testing.T, c *bloomd.Client, name string) {
	t.Helper()
	f := c.GetFilter(name)
	_, err := f.Info()
	if err != nil {
		t.Fatal(err)
	}
}

func setShouldAddNew(t *testing.T, c *bloomd.Client, rf *Filter, key string) {
	t.Helper()
	isNew, err := rf.Set(getContext(runtimeOperationTimeout), c, bloomd.Key(key))
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Errorf("%s should not exist", key)
	}
}

func bulkSetShouldlNotFail(t *testing.T, c *bloomd.Client, rf *Filter, keys ...string) bloomd.ResultReader {
	t.Helper()
	set := readerReseter(keys...)
	results, err := rf.BulkSet(getContext(runtimeOperationTimeout), c, set)
	if err != nil {
		t.Fatal(err)
	}
	return results
}

func multiCheckShouldlNotFail(t *testing.T, c *bloomd.Client, rf *Filter, keys ...string) bloomd.ResultReader {
	t.Helper()
	set := readerReseter(keys...)
	results, err := rf.MultiCheck(getContext(runtimeOperationTimeout), c, set)
	if err != nil {
		t.Fatal(err)
	}
	return results
}

func readerReseter(keys ...string) KeyReaderReseter {
	var arr []bloomd.Key
	for _, key := range keys {
		arr = append(arr, bloomd.Key(key))
	}
	return NewArrayReaderReseter(arr...)
}

func checkShouldFind(t *testing.T, c *bloomd.Client, rf *Filter, key string) {
	t.Helper()
	b, err := rf.Check(getContext(runtimeOperationTimeout), c, bloomd.Key(key))
	if err != nil {
		t.Fatal(err)
	}
	if !b {
		t.Errorf("%s should be found", key)
	}
}

func checkShouldNotFind(t *testing.T, c *bloomd.Client, rf *Filter, key string) {
	t.Helper()
	b, err := rf.Check(getContext(runtimeOperationTimeout), c, bloomd.Key(key))
	if err != nil {
		t.Fatal(err)
	}
	if b {
		t.Errorf("%s should not be found", key)
	}
}

func createBenchFilter(b *testing.B, c *bloomd.Client, prefix string, period clock.UnitNum, unit clock.Unit) *Filter {
	b.Helper()
	return createFilter(b, c, fmt.Sprintf("%s_%d", prefix, b.N), period, unit)
}

func createFilter(tb testing.TB, c *bloomd.Client, prefix string, period clock.UnitNum, unit clock.Unit) *Filter {
	tb.Helper()
	namer := NewNamer(prefix, unit)
	rf := newFilter(tb, namer, unit, period)
	err := rf.CreateFilters(getContext(manageOperationTimeout), c, 0, 0, 0, true)
	if err != nil {
		tb.Fatal(err)
	}
	return rf
}

func dropFilter(tb testing.TB, c *bloomd.Client, rf *Filter) error {
	tb.Helper()
	if err := rf.Drop(getContext(manageOperationTimeout), c); err != nil {
		tb.Fatal(err)
	}
	return nil
}

func createClientFromURL(tb testing.TB, addr *url.URL) *bloomd.Client {
	tb.Helper()
	c, err := bloomd.NewFromURL(addr)
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

func createClientPoolFromURL(tb testing.TB, addr *url.URL) *bloomd.Pool {
	tb.Helper()
	c, err := bloomd.NewPoolFromURL(100, 100, addr)
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

func closeClient(tb testing.TB, c *bloomd.Client) {
	tb.Helper()
	if err := c.Close(); err != nil {
		tb.Fatal(err)
	}
}

func next(t *testing.T, reader bloomd.ResultReader) bool {
	t.Helper()
	next, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func newFilter(tb testing.TB, namer Namer, unit clock.Unit, period clock.UnitNum) *Filter {
	tb.Helper()
	filter, err := NewFilter(namer, unit, period)
	if err != nil {
		tb.Fatal(err)
	}
	return filter
}

func generateSeqKeyReaderReseter(count int) KeyReaderReseter {
	var arr []bloomd.Key
	for i := 0; i < count; i++ {
		arr = append(arr, bloomd.Key(fmt.Sprintf("key_%d", i)))
	}
	return NewArrayReaderReseter(arr...)
}

func getContext(timeout time.Duration) context.Context {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	return ctx
}

func getClientFromPool(tb testing.TB, pool *bloomd.Pool) *bloomd.Client {
	tb.Helper()
	c, err := pool.Get()
	if err != nil {
		tb.Fatal(err)
	}
	return c
}
