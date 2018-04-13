package bloomd

import (
	"fmt"
	"testing"
)

func TestKeyBuffer(t *testing.T) {
	kbPool := NewKeyBufferPool()
	t.Run("AddString", func(t *testing.T) {
		kb := kbPool.GetKeyBufferString("hello")
		defer kbPool.PutKeyBuffer(kb)
		kb.AddString("world")
		kb.AddString("!")
		actual := kb.KeySlice()
		expected := Key("hello;world;!")
		if !expected.Equal(actual) {
			t.Fatalf("Key is expected to be equal %s but was %s", expected, actual)
		}
	})

	t.Run("SetStringExceedsBufferInitialLength", func(t *testing.T) {
		veryLongString := "verylooo" + repeatString("ooo", initialKeyBufferCapacity) + "ooongstring"
		kb := kbPool.GetKeyBufferString("miniStr")
		defer kbPool.PutKeyBuffer(kb)
		kb.SetString(veryLongString)
		actual := kb.KeySlice()
		expected := Key(veryLongString)
		if !expected.Equal(actual) {
			t.Fatalf("Key is expected to be equal %s but was %s", expected, actual)
		}
	})
}

func TestKeySet(t *testing.T) {
	ksPool := NewKeySetPool()
	t.Run("Empty", func(t *testing.T) {
		ks := ksPool.GetKeySet()
		ks.AddKey(Key("Hello"))
		ks.AddKey(Key("world!"))
		ks.Empty()
		ks.AddKey(Key("Hello"))
		ks.AddKey(Key("world!"))
		expected := "Hello world!"
		actual := string(ks.set)
		if expected != actual {
			t.Fatalf("KeySet.set is expected to be equal %s but was %s", expected, actual)
		}
		if ks.Length() != 2 {
			t.Fatalf("KeySet.n is expected to be equal %d but was %d", 2, ks.Length())
		}
	})
}

func BenchmarkKeyBuffer(b *testing.B) {
	pool := NewKeyBufferPool()
	b.Run("AddString", func(b *testing.B) {
		keys := generateSeqStringKeys(b.N)

		l := 0

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			kb := pool.GetKeyBufferString(keys[i])
			kb.AddString(keys[b.N-i-1])
			l += len(kb.KeySlice())
			pool.PutKeyBuffer(kb)
		}
	})
}

func generateSeqStringKeys(count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}
	return keys
}

func repeatString(str string, times int) string {
	result := str
	for i := 1; i < times; i++ {
		result += str
	}
	return result
}
