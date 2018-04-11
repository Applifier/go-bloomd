package bloomd

import (
	"bytes"
	"sync"
)

const keyPartsDelimeter byte = ';'
const initialKeyBufferCapacity = 32
const initialKeySetCapacity = 512

// Key is bloom filter key
type Key []byte

// KeyBuffer is reusable buffer to construct key
type KeyBuffer struct {
	buf []byte
}

// Equal checks equality of two keys
func (k Key) Equal(k2 Key) bool {
	return bytes.Equal(k, k2)
}

// SetString sets KeyBuffer value to be equal to string
func (kb *KeyBuffer) SetString(str string) {
	kb.SetBytes([]byte(str))
}

// SetBytes sets KeyBuffer value to be equal to bytes array
func (kb *KeyBuffer) SetBytes(bytes []byte) {
	b := kb.buf
	l := copy(b, bytes)
	if l < len(bytes) {
		b = append(kb.buf, bytes[l:]...)
	}
	kb.buf = b[:len(bytes)]
}

// AddString concatenates string array to key
func (kb *KeyBuffer) AddString(str string) {
	kb.Add([]byte(str)) // conversion of string to byte array should allocate memory on stack unless if string is over 32 bytes
}

// Add concatenates bytes array to key
func (kb *KeyBuffer) Add(bytes []byte) {
	kb.appendDelimeter()
	kb.buf = append(kb.buf, bytes...)
}

// KeySlice returns a Key as a slice currently stored in buffer
func (kb *KeyBuffer) KeySlice() Key {
	return kb.buf
}

func (kb *KeyBuffer) appendDelimeter() {
	kb.buf = append(kb.buf, keyPartsDelimeter)
}

// KeyBufferPool pool for compound keys
type KeyBufferPool struct {
	pool sync.Pool
}

// NewKeyBufferPool creates an empty compound key pool
func NewKeyBufferPool() *KeyBufferPool {
	return &KeyBufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &KeyBuffer{
					buf: make([]byte, 0, initialKeyBufferCapacity),
				}
			},
		},
	}
}

// GetKeyBufferString gets KeyBuffer from a pool initailizing it with string str
func (kp *KeyBufferPool) GetKeyBufferString(str string) *KeyBuffer {
	kb := kp.pool.Get().(*KeyBuffer)
	kb.SetString(str)
	return kb
}

// PutKeyBuffer returns key to a pool
func (kp *KeyBufferPool) PutKeyBuffer(kb *KeyBuffer) {
	kp.pool.Put(kb)
}

const keysDelimeter byte = ' '

// KeySet is a set of keys
type KeySet struct {
	set []byte
	n   int
}

// AddKey adds key to keyset
func (p *KeySet) AddKey(key Key) {
	if p.n > 0 {
		p.set = append(p.set, keysDelimeter)
	}
	p.set = append(p.set, key...)
	p.n++
	// we could probably return key to buffer here but it also can be reused in the same application block
}

// Length returns a number of keys in the key set
func (p *KeySet) Length() int {
	return p.n
}

// Empty removes all keys from the key set, allowing to reuse it
func (p *KeySet) Empty() {
	p.set = p.set[:0]
	p.n = 0
}

// KeySetPool key set pool
type KeySetPool struct {
	internal sync.Pool
}

// NewKeySetPool returns an empty key set pool
func NewKeySetPool() *KeySetPool {
	return &KeySetPool{
		internal: sync.Pool{
			New: func() interface{} {
				return &KeySet{
					set: make([]byte, 0, initialKeySetCapacity),
				}
			},
		},
	}
}

// GetKeySet get a key set from a pool and empties it
func (p *KeySetPool) GetKeySet() *KeySet {
	b := p.internal.Get().(*KeySet)
	b.Empty()
	return b
}

// PutKeySet return keyset to a pool
func (p *KeySetPool) PutKeySet(ks *KeySet) {
	p.internal.Put(ks)
}
