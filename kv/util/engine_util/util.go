package engine_util

import (
	"bytes"

	"github.com/Alorun/stonekv/kv/util/rocketdb"
	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
)

// Shared, immutable option singletons used by all immediate (non-snapshot)
// reads and writes. They hold only configuration, are safe to share across
// goroutines, and live for the whole process, so there is no per-operation
// C-resource leak.
var (
	defaultReadOptions  = rocketdb.NewReadOptions()
	defaultWriteOptions = rocketdb.NewWriteOptions()
)

// Txn is a consistent read view over a rocketdb database. It replaces the
// badger read-only transaction: a Txn pins a snapshot and binds it into a
// ReadOptions, so every Get/iterator created from it observes the exact same
// state for the Txn's whole lifetime.
//
// A Txn owns C resources (snapshot + ReadOptions) and MUST be released with
// Discard when no longer needed.
type Txn struct {
	db   *rocketdb.DB
	snap *rocketdb.Snapshot
	ro   *rocketdb.ReadOptions
}

// NewTxn pins a snapshot on db and returns a consistent read view bound to it.
func NewTxn(db *rocketdb.DB) *Txn {
	snap := db.NewSnapshot()
	ro := rocketdb.NewReadOptions()
	ro.SetSnapshot(snap)
	return &Txn{db: db, snap: snap, ro: ro}
}

// Discard releases the snapshot and ReadOptions held by the Txn. Safe to call
// more than once.
func (t *Txn) Discard() {
	if t.ro != nil {
		t.ro.Close()
		t.ro = nil
	}
	if t.snap != nil {
		t.db.ReleaseSnapshot(t.snap)
		t.snap = nil
	}
}

// NewRawIterator returns an iterator over the whole keyspace at the Txn's
// pinned snapshot. Unlike NewCFIterator it is NOT restricted to a column
// family; callers that scan raw keys use it. Note rocketdb's Seek panics on an
// empty key, so seek with SeekToFirst when the start key is empty.
func (t *Txn) NewRawIterator() *rocketdb.Iterator {
	return t.db.NewIterator(t.ro)
}

func KeyWithCF(cf string, key []byte) []byte {
	return append([]byte(cf+"_"), key...)
}

// GetCF does an immediate (latest-state) read of cf/key.
func GetCF(db *rocketdb.DB, cf string, key []byte) (val []byte, err error) {
	val, err = db.Get(defaultReadOptions, KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	if val == nil {
		// rocketdb returns (nil, nil) on a miss; translate to the error the
		// upper layers (and tests) still expect.
		return nil, badger.ErrKeyNotFound
	}
	return val, nil
}

// GetCFFromTxn reads cf/key from the Txn's pinned snapshot.
func GetCFFromTxn(txn *Txn, cf string, key []byte) (val []byte, err error) {
	val, err = txn.db.Get(txn.ro, KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, badger.ErrKeyNotFound
	}
	return val, nil
}

func PutCF(engine *rocketdb.DB, cf string, key []byte, val []byte) error {
	return engine.Put(defaultWriteOptions, KeyWithCF(cf, key), val)
}

func GetMeta(engine *rocketdb.DB, key []byte, msg proto.Message) error {
	val, err := engine.Get(defaultReadOptions, key)
	if err != nil {
		return err
	}
	if val == nil {
		return badger.ErrKeyNotFound
	}
	return proto.Unmarshal(val, msg)
}

func GetMetaFromTxn(txn *Txn, key []byte, msg proto.Message) error {
	val, err := txn.db.Get(txn.ro, key)
	if err != nil {
		return err
	}
	if val == nil {
		return badger.ErrKeyNotFound
	}
	return proto.Unmarshal(val, msg)
}

func PutMeta(engine *rocketdb.DB, key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return engine.Put(defaultWriteOptions, key, val)
}

func DeleteCF(engine *rocketdb.DB, cf string, key []byte) error {
	return engine.Delete(defaultWriteOptions, KeyWithCF(cf, key))
}

func DeleteRange(db *rocketdb.DB, startKey, endKey []byte) error {
	batch := new(WriteBatch)
	// Iterate over a pinned snapshot so the range we collect is consistent,
	// then apply the deletes atomically. rocketdb has no native range delete.
	txn := NewTxn(db)
	defer txn.Discard()
	for _, cf := range CFs {
		deleteRangeCF(txn, batch, cf, startKey, endKey)
	}

	return batch.WriteToDB(db)
}

func deleteRangeCF(txn *Txn, batch *WriteBatch, cf string, startKey, endKey []byte) {
	it := NewCFIterator(cf, txn)
	defer it.Close()
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if ExceedEndKey(key, endKey) {
			break
		}
		batch.DeleteCF(cf, key)
	}
}

func ExceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
