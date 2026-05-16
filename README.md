# StoneKV

这是一个 Go 实现的 TiKV/TinyKV 风格分布式 KV 项目，模块名是
github.com/Alorun/stonekv。核心由两个服务组成：kv/main.go 启动 KV gRPC server，scheduler/main.go 启动调度器。

### 主要结构：

- raft/：自研 Raft 核心，包括选举、日志复制、RawNode、内存存储和测试。
- kv/server/：对外 gRPC API。Raw KV 接口已实现，事务接口还有明显占位。
- kv/storage/：抽象存储层，包含单机 Badger 存储和 Raft-backed 存储。
- kv/raftstore/：Region、Peer、Raft 消息处理、快照、分裂、调度心跳等 TiKV 类逻辑。
- kv/transaction/：MVCC、锁、latch、事务命令逻辑，但目前多处仍是 Your Code Here。
- scheduler/：类似 PD 的调度服务，管理 store/region、TSO、operator、balance scheduler。
- proto/：gRPC/protobuf 定义和生成代码。