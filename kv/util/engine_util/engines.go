package engine_util

import (
	"os"

	"github.com/Alorun/stonekv/kv/util/rocketdb"
	"github.com/Alorun/stonekv/log"
)

// Engines keeps references to and data for the engines used by unistore.
// All engines are rocketdb key/value databases.
// the Path fields are the filesystem path to where the data is stored.
type Engines struct {
	// Data, including data which is committed (i.e., committed across other nodes) and un-committed (i.e., only present
	// locally).
	Kv     *rocketdb.DB
	KvPath string
	// Metadata used by Raft.
	Raft     *rocketdb.DB
	RaftPath string
}

func NewEngines(kvEngine, raftEngine *rocketdb.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		Kv:       kvEngine,
		KvPath:   kvPath,
		Raft:     raftEngine,
		RaftPath: raftPath,
	}
}

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToDB(en.Kv)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return wb.WriteToDB(en.Raft)
}

func (en *Engines) Close() error {
	// rocketdb.DB.Close() does not return an error; the signature is kept as
	// `error` so callers (raftstore, tests) need no change.
	if en.Kv != nil {
		en.Kv.Close()
	}
	if en.Raft != nil {
		en.Raft.Close()
	}
	return nil
}

func (en *Engines) Destroy() error {
	if err := en.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(en.KvPath); err != nil {
		return err
	}
	if err := os.RemoveAll(en.RaftPath); err != nil {
		return err
	}
	return nil
}

// CreateDB creates a new RocketDB on disk at path.
// The `raft` flag is kept for signature compatibility; rocketdb does not need
// the badger-specific value-threshold tuning the raft engine used.
func CreateDB(path string, raft bool) *rocketdb.DB {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	opts := rocketdb.NewOptions()
	// Options is a C resource. Open copies what it needs, so we can release it
	// right after Open returns to avoid leaking the C struct.
	defer opts.Close()
	opts.SetCreateIfMissing(true)
	db, err := rocketdb.Open(path, opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
