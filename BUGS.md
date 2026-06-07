# 开发过程问题记录(Project 3)

记录 TinyKV Project 3(multi-raft)开发中遇到的 bug:原因、现象、涉及模块、解决方法。
按发现顺序排列。

---

## Bug 1:`handleAppendEntries` term 不匹配拒绝分支漏写 `return`

- **涉及**:`raft/raft.go` → `handleAppendEntries`
- **现象**:follower 一直卡在 "requesting snapshot" 循环,永远无法应用快照;
  `TestBasicConfChange3B` 无法推进。
- **原因**:prevLog 的 term 一致性检查失败时,发出 reject 的 `MsgAppendResponse` 后
  **没有 `return`**,代码继续往下执行追加/提交逻辑,把 `committed` 改坏。
  之后 `handleSnapshot` 里 `snapIndex <= committed` 的判断走错分支,快照被丢弃。
- **解决**:reject 之后立即 `return`,不再执行后续追加与 commit 更新。

---

## Bug 2:`CheckApplyingSnap` 收到完成通知后没有重置 snapState

- **涉及**:`kv/raftstore/peer_storage.go` → `CheckApplyingSnap`
- **现象**:某 peer 应用了一次快照后("begin to apply snapshot" 只出现 1 次),
  之后收到几十条 `MsgAppend` 却零响应,snapState 永远停在 `Applying`,
  `HandleRaftReady` 顶部的 `CheckApplyingSnap()` 一直返回 true,该 peer 不再处理任何 Ready。
- **原因**:region worker 应用完快照后通过 `Notifier` 发信号,`CheckApplyingSnap` 从
  channel 读到信号后**没有把 `snapState.StateType` 重置为 `Relax`**。
- **解决**:读到 Notifier 信号后,`ps.snapState.StateType = snap.SnapState_Relax` 再返回 false。

```go
select {
case <-ps.snapState.Notifier:
    ps.snapState.StateType = snap.SnapState_Relax   // 关键:重置
    return false
default:
    return true
}
```

---

## Bug 3:apply 阶段缺少逐 key 的 `CheckKeyInRegion`

- **涉及**:`kv/raftstore/peer_msg_handler.go` → `processCommittedEntry`
- **现象**:`TestOneSplit3B` 失败——split 之后,对已经不属于本 region 的 key 执行
  Get(如在左 region 上读 k2)没有返回错误。
- **原因**:执行 Put/Delete/Get 前,没有逐个 key 校验该 key 是否仍落在当前 region 范围内。
  split 改变了 region 边界,旧请求里的 key 可能已被划到兄弟 region。
- **解决**:在真正执行前,对每条 request 的 key 做 `util.CheckKeyInRegion(key, d.Region())`,
  不通过则用 `ErrResp` 回复并跳过执行。

---

## Bug 4:apply 阶段缺少 `CheckRegionEpoch`(TOCTOU)

- **涉及**:`kv/raftstore/peer_msg_handler.go` → `processCommittedEntry`
- **现象**:`TestSplitRecoverManyClients3B` panic:"key ... is not in region",
  栈在 `Cluster.Scan` → `RegionIterator.Seek`。
- **原因**:一个 Snap 请求在 split **之前**被 propose,等它被 commit/apply 时 region 已经 split,
  epoch 变了。propose 时的 epoch 校验(`preProposeRaftCommand`)已经过期(TOCTOU),
  必须在 **apply 时**再校验一次 RegionEpoch。
- **解决**:`processCommittedEntry` 里执行普通请求前,先
  `util.CheckRegionEpoch(msg, d.Region(), true)`,失败则回 `ErrResp` 并跳过。
  (与 Bug 3 是两类校验:epoch 校验防整体过期,key 校验防单 key 路由错误,二者都要。)

---

## Bug 5:未初始化 peer 以空 `Prs` 自封 leader,毒化整个 raft 组 ★

- **涉及**:`raft/raft.go` → `startElection`(触发链路:`tickElection` → `MsgHup` →
  `stepFollower`/`stepCandidate`)
- **现象**:`TestConfChangeSnapshotUnreliableRecover3B` **偶发**(约 1/3)`panic: request timeout`。
  埋点显示卡死瞬间出现两个 leader:
  ```
  r=9  term=10 prs[ 9:(m=960,n=961) 10:(m=0,n=710) ]   ← 真 leader
  r=10 term=12 prs[]                                    ← 幽灵 leader,Prs 为空,term 反而更高
  ```
- **原因**:
  1. conf change 频繁,新 peer 常由 `maybeCreatePeer` → `replicatePeer` 凭空创建。
     此时构造的 region **只有 Id、没有 Peers**(未初始化),
     `InitialState()` 返回空 ConfState → `newRaft` 里 `Prs` 是**空 map**。
     该 peer 只是空壳,等着收快照后由 `handleSnapshot` 重建 `Prs`。
  2. 空窗期内若它的选举计时器先超时 → `MsgHup` → `startElection`。
     `MsgHup` 分支**没有**像 `MsgTimeoutNow` 分支那样检查"自己是否在 Prs 里"。
  3. `quorum() = len(空Prs)/2 + 1 = 1` → 它**立刻自选为 leader 并抬高 term**。
  4. 高 term 幽灵 leader 拒收真 leader 的 append/snapshot(term 落后),
     于是它永远拿不到快照、`Match` 永远为 0;真 leader 在两节点组里永远凑不齐多数派,
     `committed` 卡死 → client 的 Scan 5s 超时 panic。
- **关键认识**:`Prs` 身兼两职——**value(Match/Next)是 leader 专用的复制进度;
  key 是全体节点都要的成员表/投票集合**。空壳 peer 的成员表为空,既不该拉票也不该当选。
- **解决**:在 `startElection` 入口加可提升性判断(等价于 etcd raft 的 `promotable()`),
  一处覆盖所有入口:

```go
func (r *Raft) startElection() {
    // 不在自己 Prs 里(未初始化空壳 peer / 已被移除的 peer)绝不能发起选举
    if _, ok := r.Prs[r.id]; !ok {
        return
    }
    r.becomeCandidate()
    // ...
}
```

- **验证**:加该判断后,同环境压测 12 轮全过(此前失败率约 1/3)。

---

## 更正:3B 的 split 测试 **不依赖** 真调度器的 `processRegionHeartbeat`(3C)

> 早期一度以为重型 split 测试要等 3C 的 `scheduler/server/cluster.go::processRegionHeartbeat`
> 实现。**这是错的**。`kv/test_raftstore` 用的是 `MockSchedulerClient`
> (`kv/test_raftstore/scheduler.go`),它自带 region 跟踪(`RegionHeartbeat` /
> `handleHeartbeatVersion` / `getRegionLocked`),完全不走真调度器。所以 3B split 测试
> 与 3C 的 `processRegionHeartbeat` 空桩无关。

---

## Bug 6:`...ConcurrentPartition3B` 偶发 panic —— 分裂后调度器 region 视图滞后(liveness 竞态,非确定性 bug)

- **涉及**:`kv/raftstore/peer_msg_handler.go::processSplit` + 心跳上报 +
  `kv/test_raftstore/scheduler.go`(mock 调度器)+ 测试的 `Cluster.Scan`/`GetRegion`
- **现象**:`TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B`
  偶发(约 1/3),两种失败形态:
  - `panic: request timeout`(`cluster.go:204`)
  - `panic: key ... is not in region ...`(`Scan` → `RegionIterator.Seek`)
- **根因(埋点实证)**:测试客户端 `Scan` 靠 `GetRegion(key)` 向 **mock 调度器** 问"哪个
  region 含这个 key",再把 Snap 发给该 region 的 leader。mock 的 region 范围靠 raftstore
  leader 的心跳来更新。
  - 抓到的时间线:服务端 15:48:59.5 把 region 6 在 `"3 00000003"` 处**二次分裂**
    (region 6 缩成 v2 `end="3 00000003"`,新建 region 16);但 **mock 整整 5 秒**仍认为
    region 6 是 v1 `end=""`(覆盖到无穷)。这 5 秒里 `GetRegion("4 00000000")` 一直返回旧
    region 6,于是请求被路由/读到一个**已不含该 key 的 region**,最终 timeout 或 Seek panic。
  - 这 5 秒滞后的原因:窗口内分区每 ~0.5s 轮换一次,缩小后的 region 6 选不出一个能稳定存活、
    成功上报心跳的 leader(即使有一次 ~0.5s 全连通也没撑到"选主→apply 分裂→心跳上报")。
- **排除项(都已确认无 bug)**:
  - `startElection` 的可提升性判断已修(见 Bug 5)。
  - `processSplit` 正确(分裂后老 region 在 `d.IsLeader()` 时立即心跳)。
  - 调度器心跳 tick 已正确接线:每 `PeerTickSchedulerHeartbeat`(测试配置 **100ms**)
    leader 调 `HeartbeatScheduler`。间隔很短,不是瓶颈。
  - apply 阶段 `CheckRegionEpoch`(Bug 4)在位。
- **结论**:这是 3B 最重的测试(split + conf change + snapshot + unreliable + crash +
  并发分区),失败是**分裂后调度器视图短暂滞后**导致的 liveness/timing 竞态,**不是确定性
  逻辑错误**,且对机器负载敏感(沙箱越慢越容易触发)。多跑几次通常能过。
- **可选缓解(非必须)**:peer 刚当选 leader 时主动补一次 `HeartbeatScheduler`,
  缩短新 leader 上报前的空窗;但对"5 秒内根本没有稳定 leader"这种情况帮助有限。

---