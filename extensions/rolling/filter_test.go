package rolling

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/Applifier/go-bloomd/utils/period"

	"github.com/Applifier/go-bloomd/utils/clock"

	bloomd "github.com/Applifier/go-bloomd"
	"github.com/Applifier/go-bloomd/utils/testutils"
)

var units = []string{RollDaily, RollMonthly, RollWeekly}

func TestOperations(t *testing.T) {
	testutils.TestForAllAddrs(t, func(url *url.URL, t *testing.T) {
		filterName := "test_operations_" + url.Scheme
		now := time.Now()
		clock.Static(now)
		defer clock.Reset()
		c := createClientFromURL(t, url)
		rf := createFilter(t, c, filterName, 3, RollWeekly)
		defer dropFilter(t, rf)

		t.Run("set key", func(t *testing.T) {
			setShouldAddNew(t, rf, "foo")
		})

		t.Run("check key", func(t *testing.T) {
			checkShouldFind(t, rf, "foo")
		})

		t.Run("check not existing key", func(t *testing.T) {
			checkShouldNotFind(t, rf, "dsadasdsa")
		})

		t.Run("set key on different units", func(t *testing.T) {
			defer clock.Static(now)
			clock.Static(clock.Now().Add(-period.Week))
			setShouldAddNew(t, rf, "foo-1")
			clock.Static(clock.Now().Add(-period.Week))
			setShouldAddNew(t, rf, "foo-2")
		})

		t.Run("check key on different units", func(t *testing.T) {
			checkShouldFind(t, rf, "foo")
			checkShouldFind(t, rf, "foo-1")
			checkShouldFind(t, rf, "foo-2")
		})

		t.Run("check key not found on last unit if period lower", func(t *testing.T) {
			c := createClientFromURL(t, url)
			rf := createFilter(t, c, filterName, 2, RollWeekly)
			checkShouldFind(t, rf, "foo")
			checkShouldFind(t, rf, "foo-1")
			checkShouldNotFind(t, rf, "foo-2")
		})

		t.Run("set multiple keys", func(t *testing.T) {
			rr := bulkSetShouldlNotFail(t, rf, "bar", "baz")
			rr.Close()
		})

		t.Run("get multiple keys", func(t *testing.T) {
			resps := multiCheckShouldlNotFail(t, rf, "foo", "bar", "baz", "biz")
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
			rr := bulkSetShouldlNotFail(t, rf, "bar-1", "baz-1")
			rr.Close()
			clock.Static(clock.Now().Add(-period.Week))
			rr = bulkSetShouldlNotFail(t, rf, "bar-2", "baz-2")
			rr.Close()
		})

		t.Run("check multiple keys on different units", func(t *testing.T) {
			resps := multiCheckShouldlNotFail(t, rf, "foo", "bar-1", "baz-2", "biz")
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
			resps := multiCheckShouldlNotFail(t, rf, "foo", "bar-1", "baz-2", "bar-2")
			defer resps.Close()
			if !(next(t, resps) == next(t, resps) == true) {
				t.Error("Wrong responses received")
			}
			if next(t, resps) || next(t, resps) {
				t.Error("should not exist")
			}
		})

		t.Run("check multiple non existing keys", func(t *testing.T) {
			resps := multiCheckShouldlNotFail(t, rf, "nonexk1", "nonexk2", "nonexk3")
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
			filter := NewFilter(namer, RollWeekly, 4, c)
			err := filter.CreateFilters(1, 0, 0, true)
			if err != nil {
				t.Fatal(err)
			}
			checkFilterExists(t, c, namer.NameFor(0))
			checkFilterExists(t, c, namer.NameFor(1))
			checkFilterExists(t, c, namer.NameFor(2))
			checkFilterExists(t, c, namer.NameFor(3))
			checkFilterExists(t, c, namer.NameFor(4)) // created in advance
		})

		t.Run("test rolling filter drops old filters excluding tail", func(t *testing.T) {
			filter := NewFilter(namer, RollWeekly, 2, c)
			err := filter.DropOlderFilters(1)
			if err != nil {
				t.Fatal(err)
			}
			checkFilterDoesNotExists(t, c, namer.NameFor(0))
			checkFilterExists(t, c, namer.NameFor(1)) // saved as tail
			checkFilterExists(t, c, namer.NameFor(2))
			checkFilterExists(t, c, namer.NameFor(3))
		})

		t.Run("test rolling filter drop all its filters", func(t *testing.T) {
			// set current time as zero + 4 weeks to delete filter created in advance
			clock.Static(time.Unix(0, 0).Add(period.Week * 4))
			filter := NewFilter(namer, RollWeekly, 5, c)
			err := filter.Drop()
			if err != nil {
				t.Fatal(err)
			}
			checkFilterDoesNotExists(t, c, namer.NameFor(0))
			checkFilterDoesNotExists(t, c, namer.NameFor(1))
			checkFilterDoesNotExists(t, c, namer.NameFor(2))
			checkFilterDoesNotExists(t, c, namer.NameFor(3))
			checkFilterDoesNotExists(t, c, namer.NameFor(4)) // created in advance
		})
	})
}

func BenchmarkOperations(b *testing.B) {
	for _, addr := range testutils.BloomdAddrs() {
		url := testutils.ParseURL(b, addr)
		b.Run("Test address "+addr, func(b *testing.B) {
			c := createClientFromURL(b, url)
			periods := []int{1, 5, 10}
			ks := generateSeqKeySet(100)
			readResults := make([]bool, 100)
			for _, period := range periods {
				b.Run(fmt.Sprintf("MultiCheck-p%d", period), func(b *testing.B) {
					rf := createBenchFilter(b, c, fmt.Sprintf("bench_operations_multicheck_%d_%s", period, url.Scheme), period, RollWeekly)
					defer dropFilter(b, rf)

					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						rr, err := rf.MultiCheck(ks)
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
					defer dropFilter(b, rf)

					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						rr, err := rf.BulkSet(ks)
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
	f := c.GetFilter(name)
	_, err := f.Info()
	if err != nil {
		t.Fatal(err)
	}
}

func setShouldAddNew(t *testing.T, rf *Filter, key string) {
	isNew, err := rf.Set(bloomd.Key(key))
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Errorf("%s should not exist", key)
	}
}

func bulkSetShouldlNotFail(t *testing.T, rf *Filter, keys ...string) bloomd.ResultReader {
	set := keySet(keys...)
	results, err := rf.BulkSet(set)
	if err != nil {
		t.Fatal(err)
	}
	return results
}

func multiCheckShouldlNotFail(t *testing.T, rf *Filter, keys ...string) bloomd.ResultReader {
	set := keySet(keys...)
	results, err := rf.MultiCheck(set)
	if err != nil {
		t.Fatal(err)
	}
	return results
}

func keySet(keys ...string) *bloomd.KeySet {
	keySetPool := bloomd.NewKeySetPool()
	set := keySetPool.GetKeySet()
	for _, key := range keys {
		set.AddKey(bloomd.Key(key))
	}
	return set
}

func checkShouldFind(t *testing.T, rf *Filter, key string) {
	b, err := rf.Check(bloomd.Key(key))
	if err != nil {
		t.Fatal(err)
	}
	if !b {
		t.Errorf("%s should be found", key)
	}
}

func checkShouldNotFind(t *testing.T, rf *Filter, key string) {
	b, err := rf.Check(bloomd.Key(key))
	if err != nil {
		t.Fatal(err)
	}
	if b {
		t.Errorf("%s should not be found", key)
	}
}

func createBenchFilter(b *testing.B, c *bloomd.Client, prefix string, period int, unit string) *Filter {
	return createFilter(b, c, fmt.Sprintf("%s_%d", prefix, b.N), period, unit)
}

func createFilter(tb testing.TB, c *bloomd.Client, prefix string, period int, unit string) *Filter {
	namer := NewNamer(prefix, unit)
	rf := NewFilter(namer, unit, period, c)
	err := rf.CreateFilters(0, 0, 0, true)
	if err != nil {
		tb.Fatal(err)
	}
	return rf
}

func dropFilter(tb testing.TB, rf *Filter) error {
	if err := rf.Drop(); err != nil {
		tb.Fatal(err)
	}
	return nil
}

func createClientFromURL(tb testing.TB, addr *url.URL) *bloomd.Client {
	c, err := bloomd.NewFromURL(addr)
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

func closeClient(tb testing.TB, c *bloomd.Client) {
	if err := c.Close(); err != nil {
		tb.Fatal(err)
	}
}

func next(t *testing.T, reader bloomd.ResultReader) bool {
	next, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func generateSeqKeySet(count int) *bloomd.KeySet {
	ksPool := bloomd.NewKeySetPool()
	ks := ksPool.GetKeySet()
	for i := 0; i < count; i++ {
		ks.AddKey(bloomd.Key(fmt.Sprintf("key_%d", i)))
	}
	return ks
}
