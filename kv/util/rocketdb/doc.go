// Package rocketdb provides Go bindings for the rocketdb storage engine's
// stable C API (rocketdb_* symbols, exposed via the engine's c.h).
//
// It is adapted from jmhodges/levigo and bound specifically to rocketdb: the
// C symbol prefix is rocketdb_ (not leveldb_), the header is the engine's
// c.h, and it links against the clean (non-AddressSanitizer) release build of
// librocketdb.a. See cgo.go for the cgo build directives.
//
// The package wraps the C handles in Go types:
//
//	DB           - an open database handle
//	Options      - database options used at Open time
//	ReadOptions  - per-read options (verify checksums, fill cache, snapshot)
//	WriteOptions - per-write options (sync)
//	WriteBatch   - a batch of Put/Delete applied atomically by DB.Write
//	Iterator     - an ordered cursor over the keyspace
//	Snapshot     - a consistent point-in-time view, bound to a ReadOptions
//	Cache        - an LRU block cache
//	FilterPolicy - a bloom filter policy
//	Env          - the OS environment handle
//
// Memory ownership follows the C API: handles created by New*/Open must be
// released by their Close (or, for snapshots, DB.ReleaseSnapshot). Byte
// slices passed to Put/Get/Delete are copied by the engine, so callers may
// reuse them immediately.
package rocketdb
