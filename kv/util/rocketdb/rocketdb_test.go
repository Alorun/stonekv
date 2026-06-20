package rocketdb

import (
	"bytes"
	"testing"
)

// openTestDB opens a fresh DB in a temp dir with create_if_missing.
// The returned cleanup closes the DB and the shared options.
func openTestDB(t *testing.T) (*DB, func()) {
	t.Helper()
	opts := NewOptions()
	opts.SetCreateIfMissing(true)
	db, err := Open(t.TempDir(), opts)
	if err != nil {
		opts.Close()
		t.Fatalf("Open failed: %v", err)
	}
	return db, func() {
		db.Close()
		opts.Close()
	}
}

// TestTwoIndependentDBs mirrors tinykv's need for two separate engines
// (Kv and Raft): writes to one must not be visible in the other.
func TestTwoIndependentDBs(t *testing.T) {
	kv, closeKv := openTestDB(t)
	defer closeKv()
	raft, closeRaft := openTestDB(t)
	defer closeRaft()

	wo := NewWriteOptions()
	defer wo.Close()
	ro := NewReadOptions()
	defer ro.Close()

	if err := kv.Put(wo, []byte("only-in-kv"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := raft.Put(wo, []byte("only-in-raft"), []byte("2")); err != nil {
		t.Fatal(err)
	}

	// cross-checks: each key must be absent from the other DB
	if v, err := kv.Get(ro, []byte("only-in-raft")); err != nil || v != nil {
		t.Fatalf("kv leaked raft key: v=%q err=%v", v, err)
	}
	if v, err := raft.Get(ro, []byte("only-in-kv")); err != nil || v != nil {
		t.Fatalf("raft leaked kv key: v=%q err=%v", v, err)
	}
}

func TestPutGetDelete(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	wo := NewWriteOptions()
	defer wo.Close()
	ro := NewReadOptions()
	defer ro.Close()

	key, val := []byte("hello"), []byte("world")

	// missing key -> nil, no error
	if v, err := db.Get(ro, key); err != nil || v != nil {
		t.Fatalf("expected miss, got v=%q err=%v", v, err)
	}

	if err := db.Put(wo, key, val); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ro, key); err != nil || !bytes.Equal(v, val) {
		t.Fatalf("get after put: v=%q err=%v", v, err)
	}

	if err := db.Delete(wo, key); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ro, key); err != nil || v != nil {
		t.Fatalf("get after delete should miss: v=%q err=%v", v, err)
	}
}

// TestSnapshotConsistentRead verifies a snapshot bound to a ReadOptions
// gives a stable point-in-time view across both Get and iteration.
func TestSnapshotConsistentRead(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	wo := NewWriteOptions()
	defer wo.Close()

	// state at snapshot time
	for _, k := range []string{"a", "b", "c"} {
		if err := db.Put(wo, []byte(k), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}

	snap := db.NewSnapshot()
	defer db.ReleaseSnapshot(snap)

	snapRO := NewReadOptions()
	defer snapRO.Close()
	snapRO.SetSnapshot(snap)

	// mutate after the snapshot: overwrite + add a new key
	if err := db.Put(wo, []byte("b"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(wo, []byte("d"), []byte("new")); err != nil {
		t.Fatal(err)
	}

	// Get through the snapshot must still see the old value...
	if v, err := db.Get(snapRO, []byte("b")); err != nil || !bytes.Equal(v, []byte("old")) {
		t.Fatalf("snapshot Get(b)=%q err=%v, want old", v, err)
	}
	// ...and must not see the post-snapshot key.
	if v, err := db.Get(snapRO, []byte("d")); err != nil || v != nil {
		t.Fatalf("snapshot Get(d)=%q err=%v, want miss", v, err)
	}

	// Iteration through the snapshot sees exactly a,b,c all "old".
	it := db.NewIterator(snapRO)
	defer it.Close()
	got := map[string]string{}
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got[string(it.Key())] = string(it.Value())
	}
	if err := it.GetError(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	want := map[string]string{"a": "old", "b": "old", "c": "old"}
	if len(got) != len(want) {
		t.Fatalf("snapshot iter saw %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("snapshot iter[%s]=%q, want %q", k, got[k], v)
		}
	}
}

// TestIteratorSeek exercises Seek / Valid / Next / Key / Value / Close.
func TestIteratorSeek(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	wo := NewWriteOptions()
	defer wo.Close()
	for _, k := range []string{"k1", "k2", "k3", "k4"} {
		if err := db.Put(wo, []byte(k), []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}

	ro := NewReadOptions()
	defer ro.Close()
	it := db.NewIterator(ro)
	defer it.Close()

	it.Seek([]byte("k2"))
	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if err := it.GetError(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	want := []string{"k2", "k3", "k4"}
	if len(keys) != len(want) {
		t.Fatalf("seek walk got %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("seek walk[%d]=%q, want %q", i, keys[i], want[i])
		}
	}
}

// TestWriteBatch verifies batched Put/Delete commit atomically via DB.Write.
func TestWriteBatch(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	wo := NewWriteOptions()
	defer wo.Close()
	ro := NewReadOptions()
	defer ro.Close()

	// pre-existing key that the batch will delete
	if err := db.Put(wo, []byte("stale"), []byte("x")); err != nil {
		t.Fatal(err)
	}

	wb := NewWriteBatch()
	defer wb.Close()
	wb.Put([]byte("batch1"), []byte("v1"))
	wb.Put([]byte("batch2"), []byte("v2"))
	wb.Delete([]byte("stale"))

	if err := db.Write(wo, wb); err != nil {
		t.Fatalf("Write batch: %v", err)
	}

	if v, err := db.Get(ro, []byte("batch1")); err != nil || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("batch1=%q err=%v", v, err)
	}
	if v, err := db.Get(ro, []byte("batch2")); err != nil || !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("batch2=%q err=%v", v, err)
	}
	if v, err := db.Get(ro, []byte("stale")); err != nil || v != nil {
		t.Fatalf("stale should be deleted: v=%q err=%v", v, err)
	}
}

func TestVersion(t *testing.T) {
	// Just confirm the version symbols link and return sane values.
	if GetMajorVersion() < 0 || GetMinorVersion() < 0 {
		t.Fatalf("bad version %d.%d", GetMajorVersion(), GetMinorVersion())
	}
}
