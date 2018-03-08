package bloomd

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestFilter(t *testing.T) {
	c, err := NewFromAddr(getBloomdAddr())
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

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

func BenchmarkFilter(b *testing.B) {
	c, err := NewFromAddr(getBloomdAddr())
	if err != nil {
		b.Fatal(err)
	}

	defer c.Close()

	f, err := c.CreateFilter(Filter{
		Name:     "benchmarkfilter",
		InMemory: true,
	})
	if err != nil {
		b.Fatal(err)
	}

	info, _ := f.Info()
	if info["in_memory"] != "1" {
		b.Fatal("Not in memory")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
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
}
