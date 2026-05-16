# StoneKV

This is a Go implementation of a TiKV/TinyKV style distributed key-value (KV) project, module name: github.com/Alorun/stonekv. The core consists of two services: kv/main.go starts the KV gRPC server, and scheduler/main.go starts the scheduler.

### Main Structure:

- raft/: Self-developed Raft core, including election, log replication, RawNode, in-memory storage, and testing.

- kv/server/: External gRPC API. The Raw KV interface is implemented, but the transaction interface is still in place.

- kv/storage/: Abstract storage layer, including single-machine Badger storage and Raft-backed storage.

- kv/raftstore/: TiKV-like logic such as Region, Peer, Raft message processing, snapshots, splitting, and scheduling heartbeats.

- kv/transaction/: MVCC, locks, latches, and transaction command logic, but currently many parts are still "Your Code Here".

- scheduler/: A scheduling service similar to PD, managing store/region, TSO, operator, and balance scheduler.

- proto/: gRPC/protobuf definition and generation code.