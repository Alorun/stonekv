package raft_storage

import (
	"bytes"

	"github.com/Alorun/stonekv/kv/raftstore/util"
	"github.com/Alorun/stonekv/kv/util/engine_util"
	"github.com/Alorun/stonekv/proto/pkg/metapb"
)

type RegionReader struct {
	txn    *engine_util.Txn
	region *metapb.Region
}

func NewRegionReader(txn *engine_util.Txn, region metapb.Region) *RegionReader {
	return &RegionReader{
		txn:    txn,
		region: &region,
	}
}

func (r *RegionReader) GetCF(cf string, key []byte) ([]byte, error) {
	if err := util.CheckKeyInRegion(key, r.region); err != nil {
		return nil, err
	}
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == engine_util.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *RegionReader) IterCF(cf string) engine_util.DBIterator {
	return NewRegionIterator(engine_util.NewCFIterator(cf, r.txn), r.region)
}

func (r *RegionReader) Close() {
	r.txn.Discard()
}

// RegionIterator wraps a db iterator and only allow it to iterate in the region. It behaves as if underlying
// db only contains one region.
type RegionIterator struct {
	iter   *engine_util.CFIterator
	region *metapb.Region
}

func NewRegionIterator(iter *engine_util.CFIterator, region *metapb.Region) *RegionIterator {
	return &RegionIterator{
		iter:   iter,
		region: region,
	}
}

func (it *RegionIterator) Item() engine_util.DBItem {
	return it.iter.Item()
}

func (it *RegionIterator) Valid() bool {
	if !it.iter.Valid() {
		return false
	}
	key := it.iter.Item().Key()
	return bytes.Compare(key, it.region.StartKey) >= 0 && !engine_util.ExceedEndKey(key, it.region.EndKey)
}

func (it *RegionIterator) ValidForPrefix(prefix []byte) bool {
	if !it.iter.ValidForPrefix(prefix) {
		return false
	}
	key := it.iter.Item().Key()
	return bytes.Compare(key, it.region.StartKey) >= 0 && !engine_util.ExceedEndKey(key, it.region.EndKey)
}

func (it *RegionIterator) Close() {
	it.iter.Close()
}

func (it *RegionIterator) Next() {
	it.iter.Next()
}

func (it *RegionIterator) Seek(key []byte) {
	if bytes.Compare(key, it.region.StartKey) < 0 {
		key = it.region.StartKey
	}
	it.iter.Seek(key)
}

func (it *RegionIterator) Rewind() {
	it.iter.Seek(it.region.StartKey)
}
