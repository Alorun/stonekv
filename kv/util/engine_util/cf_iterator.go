package engine_util

import (
	"bytes"

	"github.com/Alorun/stonekv/kv/util/rocketdb"
)

// CFItem is the current key/value pair an iterator points at. The bytes are
// private copies captured when the item was produced (rocketdb's iterator Key
// and Value already return copies), so they stay valid even after the iterator
// advances.
type CFItem struct {
	key       []byte // full key, including the "<cf>_" prefix
	value     []byte
	prefixLen int
}

func (i *CFItem) Key() []byte {
	return i.key[i.prefixLen:]
}

func (i *CFItem) KeyCopy(dst []byte) []byte {
	return append(dst[:0], i.key[i.prefixLen:]...)
}

func (i *CFItem) Value() ([]byte, error) {
	return i.value, nil
}

func (i *CFItem) ValueSize() int {
	return len(i.value)
}

func (i *CFItem) ValueCopy(dst []byte) ([]byte, error) {
	return append(dst[:0], i.value...), nil
}

// BadgerIterator wraps a rocketdb iterator and restricts it to a single column
// family. rocketdb has no prefix-scan option, so the "<cf>_" prefix is enforced
// here: Seek prepends it, and Valid checks that the underlying key still starts
// with it (a key that no longer has the prefix means we walked past the CF).
type BadgerIterator struct {
	iter   *rocketdb.Iterator
	prefix []byte
}

func NewCFIterator(cf string, txn *Txn) *BadgerIterator {
	return &BadgerIterator{
		iter:   txn.db.NewIterator(txn.ro),
		prefix: []byte(cf + "_"),
	}
}

func (it *BadgerIterator) Item() DBItem {
	return &CFItem{
		key:       it.iter.Key(),
		value:     it.iter.Value(),
		prefixLen: len(it.prefix),
	}
}

// Valid reports whether the iterator points at a key still inside this CF.
func (it *BadgerIterator) Valid() bool {
	return it.iter.Valid() && bytes.HasPrefix(it.iter.Key(), it.prefix)
}

func (it *BadgerIterator) ValidForPrefix(prefix []byte) bool {
	full := append(append([]byte{}, it.prefix...), prefix...)
	return it.iter.Valid() && bytes.HasPrefix(it.iter.Key(), full)
}

func (it *BadgerIterator) Close() {
	it.iter.Close()
}

func (it *BadgerIterator) Next() {
	it.iter.Next()
}

func (it *BadgerIterator) Seek(key []byte) {
	it.iter.Seek(append(append([]byte{}, it.prefix...), key...))
}

// Rewind moves to the first key of this CF.
func (it *BadgerIterator) Rewind() {
	it.iter.Seek(it.prefix)
}

type DBIterator interface {
	// Item returns pointer to the current key-value pair.
	Item() DBItem
	// Valid returns false when iteration is done.
	Valid() bool
	// Next would advance the iterator by one. Always check it.Valid() after a Next()
	// to ensure you have access to a valid it.Item().
	Next()
	// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
	// greater than provided.
	Seek([]byte)

	// Close the iterator
	Close()
}

type DBItem interface {
	// Key returns the key.
	Key() []byte
	// KeyCopy returns a copy of the key of the item, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	KeyCopy(dst []byte) []byte
	// Value retrieves the value of the item.
	Value() ([]byte, error)
	// ValueSize returns the size of the value.
	ValueSize() int
	// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	ValueCopy(dst []byte) ([]byte, error)
}
