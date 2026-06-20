package rocketdb

import (
	"encoding/binary"
	"io/ioutil"
	"sync"
	"testing"
)

func raftLogKey(region, idx uint64) []byte {
	b := make([]byte, 19)
	b[0] = 0x01
	binary.BigEndian.PutUint64(b[1:9], region)
	b[9] = 0x01
	binary.BigEndian.PutUint64(b[10:18], idx)
	b[18] = 0x00
	return b
}

func TestEngineConcurrentSnapshotIter(t *testing.T) {
	dir, err := ioutil.TempDir("", "engine_repro")
	if err != nil {
		t.Fatal(err)
	}
	opts := NewOptions()
	opts.SetCreateIfMissing(true)
	db, err := Open(dir, opts)
	opts.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const goroutines = 12
	const iters = 4000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(region uint64) {
			defer wg.Done()
			wo := NewWriteOptions()
			defer wo.Close()
			for i := uint64(1); i <= iters; i++ {
				_ = db.Put(wo, raftLogKey(region, i), []byte("entry"))
				if i > 100 {
					_ = db.Delete(wo, raftLogKey(region, i-100))
				}
				snap := db.NewSnapshot()
				ro := NewReadOptions()
				ro.SetSnapshot(snap)
				it := db.NewIterator(ro)
				it.Seek(raftLogKey(region, i))
				_ = it.Valid()
				it.Close()
				ro.Close()
				db.ReleaseSnapshot(snap)
			}
		}(uint64(g + 1))
	}
	wg.Wait()
	t.Log("done")
}
