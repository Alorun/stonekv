# StoneKV

This is a Go implementation of a TiKV/TinyKV style distributed key-value (KV) project, module name: github.com/Alorun/stonekv. The core consists of two services: kv/main.go starts the KV gRPC server, and scheduler/main.go starts the scheduler.

### Main Structure:

- raft/: Self-developed Raft core, including election, log replication, RawNode, in-memory storage, and testing.

- kv/server/: External gRPC API, implementing both the Raw KV and transactional (MVCC) interfaces.

- kv/storage/: Abstract storage layer, including single-machine RocketDB storage and Raft-backed storage.

- kv/raftstore/: TiKV-like logic such as Region, Peer, Raft message processing, snapshots, splitting, and scheduling heartbeats.

- kv/transaction/: MVCC, locks, latches, and transaction command logic.

- scheduler/: A scheduling service similar to PD, managing store/region, TSO, operator, and balance scheduler.

- proto/: gRPC/protobuf definition and generation code.

### RocketDB Engine Dependency

The underlying key-value engine is the self-developed RocketDB, called through the `kv/util/rocketdb` cgo binding.

The public headers and the compiled release static library are bundled in `kv/util/rocketdb/cdeps`, so building StoneKV does not depend on an external RocketDB checkout or symbolic links.

To update the bundled RocketDB artifacts after building RocketDB elsewhere:

```sh
cp -a <rocketdb>/include/. kv/util/rocketdb/cdeps/include/
cp <rocketdb>/build-release/librocketdb.a kv/util/rocketdb/cdeps/lib/librocketdb.a
CGO_ENABLED=1 make BUILD_FLAG=-a default
```

`BUILD_FLAG=-a` forces Go to relink against the updated static library.
