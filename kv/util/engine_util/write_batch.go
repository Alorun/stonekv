package engine_util

import (
	"github.com/Alorun/stonekv/kv/util/rocketdb"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
)

// writeBatchEntry is one staged mutation. An empty Value means "delete this
// key" — this matches the semantics the badger-backed implementation used, so
// upper layers behave identically.
type writeBatchEntry struct {
	Key   []byte
	Value []byte
}

type WriteBatch struct {
	entries       []*writeBatchEntry
	size          int
	safePoint     int
	safePointSize int
	safePointUndo int
}

const (
	CfDefault string = "default"
	CfWrite   string = "write"
	CfLock    string = "lock"
)

var CFs [3]string = [3]string{CfDefault, CfWrite, CfLock}

func (wb *WriteBatch) Len() int {
	return len(wb.entries)
}

func (wb *WriteBatch) SetCF(cf string, key, val []byte) {
	wb.entries = append(wb.entries, &writeBatchEntry{
		Key:   KeyWithCF(cf, key),
		Value: val,
	})
	wb.size += len(key) + len(val)
}

func (wb *WriteBatch) DeleteMeta(key []byte) {
	wb.entries = append(wb.entries, &writeBatchEntry{
		Key: key,
	})
	wb.size += len(key)
}

func (wb *WriteBatch) DeleteCF(cf string, key []byte) {
	wb.entries = append(wb.entries, &writeBatchEntry{
		Key: KeyWithCF(cf, key),
	})
	wb.size += len(key)
}

func (wb *WriteBatch) SetMeta(key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.entries = append(wb.entries, &writeBatchEntry{
		Key:   key,
		Value: val,
	})
	wb.size += len(key) + len(val)
	return nil
}

func (wb *WriteBatch) SetSafePoint() {
	wb.safePoint = len(wb.entries)
	wb.safePointSize = wb.size
}

func (wb *WriteBatch) RollbackToSafePoint() {
	wb.entries = wb.entries[:wb.safePoint]
	wb.size = wb.safePointSize
}

func (wb *WriteBatch) WriteToDB(db *rocketdb.DB) error {
	if len(wb.entries) == 0 {
		return nil
	}
	// Build a transient C-side write batch, write it atomically, then free it.
	// Keeping the C resource local to this call avoids any leak.
	rwb := rocketdb.NewWriteBatch()
	defer rwb.Close()
	for _, entry := range wb.entries {
		if len(entry.Value) == 0 {
			rwb.Delete(entry.Key)
		} else {
			rwb.Put(entry.Key, entry.Value)
		}
	}
	if err := db.Write(defaultWriteOptions, rwb); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (wb *WriteBatch) MustWriteToDB(db *rocketdb.DB) {
	err := wb.WriteToDB(db)
	if err != nil {
		panic(err)
	}
}

func (wb *WriteBatch) Reset() {
	wb.entries = wb.entries[:0]
	wb.size = 0
	wb.safePoint = 0
	wb.safePointSize = 0
	wb.safePointUndo = 0
}
