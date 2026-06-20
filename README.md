# StoneKV

This is a Go implementation of a TiKV/TinyKV style distributed key-value (KV) project, module name: github.com/Alorun/stonekv. The core consists of two services: kv/main.go starts the KV gRPC server, and scheduler/main.go starts the scheduler.

### Main Structure:

- raft/: Self-developed Raft core, including election, log replication, RawNode, in-memory storage, and testing.

- kv/server/: External gRPC API, implementing both the Raw KV and transactional (MVCC) interfaces.

- kv/storage/: Abstract storage layer, including single-machine Badger storage and Raft-backed storage.

- kv/raftstore/: TiKV-like logic such as Region, Peer, Raft message processing, snapshots, splitting, and scheduling heartbeats.

- kv/transaction/: MVCC, locks, latches, and transaction command logic.

- scheduler/: A scheduling service similar to PD, managing store/region, TSO, operator, and balance scheduler.

- proto/: gRPC/protobuf definition and generation code.

### Prerequisites: RocketDB Engine Dependency

The underlying key-value engine of this project is the self-developed RocketDB, called through the `kv/util/rocketdb` cgo binding.

The RocketDB C++ source code and compilation artifacts **are not included in this repository**, and need to be prepared locally and symbolic links created:

1. Compile RocketDB elsewhere to obtain include/ header files and build-release/librocketdb.a
2. Create two symbolic links in this repository (ignored by .gitignore):

ln -s <rocketdb>/include kv/util/rocketdb/cdeps/include

ln -s <rocketdb>/build-release kv/util/rocketdb/cdeps/lib

Then compile/test using `CGO_ENABLED=1`:

CGO_ENABLED=1 go build ./...

CGO_ENABLED=1 go test ./kv/util/rocketdb/