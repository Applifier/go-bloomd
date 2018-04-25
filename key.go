package bloomd

// Key is bloom filter key
type Key []byte

type KeyReader interface {
	Next() bool
	Current() Key
}
