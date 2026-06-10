## 一、读事务:读 key=K,快照时间戳 read_ts

```
START 读 K @ read_ts
  │
  ▼
①查 Lock CF：K 上有锁吗？
  │
  ├─ 没锁 ───────────────────────────────────┐
  │                                            │
  └─ 有锁 lock                                 │
       │                                       │
       ▼                                       │
     lock.start_ts ≤ read_ts ?                 │
       │                                       │
       ├─ 否：锁是"我之后"才出现的事务，        │
       │      对我不可见 → 当没锁处理 ──────────┤
       │                                       │
       └─ 是：撞上可能影响我的未决事务          │
              → 去 ResolveLock(见第三节)        │
                 resolve 完回到 ① 重试          │
                                               │
  ┌─────────────────────────────────────────── ┘
  ▼
②查 Write CF：在 (K,*) 里找 commit_ts ≤ read_ts 的【最大】commit_ts 那条
  │
  ├─ 一条都没有 → K 对我不存在 → 返回空
  │
  └─ 找到 write 记录，看类型：
       ├─ PUT     → 取出它记的 start_ts，进 ③
       ├─ DELETE  → 这是个"删除版本" → 返回空
       └─ ROLLBACK→ 跳过它，继续往更小的 commit_ts 找
  │
  ▼
③拿 write.start_ts，去 Default CF 读 (K, start_ts) → 真实 value
  │
  ▼
返回 value
```

读路径就一句话:**Lock 看能不能读 → Write 定位是哪个版本 → Default 取真值。** 永远不直接翻 Default。

## 二、写事务:2PC,事务拿 start_ts,要写一批 key

```
START 拿 start_ts，缓冲所有 (key,value)
  │
  ▼
选 1 个 key 当 primary，其余 secondary
  │
  ▼
══════════ Prewrite 阶段(对每个 key 都做) ══════════
  │
  ▼
 对 key K：
  1. 冲突检测A — 查 Write CF：有 commit_ts ≥ start_ts 的记录吗？
     │    （我开始之后别人已提交过 K）
     ├─ 有 → 写写冲突 → 整事务 ABORT
     └─ 无 → 下一步
  2. 冲突检测B — 查 Lock CF：K 上已有别的锁吗？
     ├─ 有 → 别人占着 → ABORT（或等待/resolve）
     └─ 无 → 下一步
  3. 写 Lock CF：K → { primary指针, start_ts, TTL }
  4. 写 Default CF：(K, start_ts) → value      ← 真值此刻已落盘，但不可见
  │
  ▼
 所有 key 都 prewrite 成功？
  ├─ 否 → ABORT：回滚已写的锁
  └─ 是 → 进 Commit
  │
  ▼
══════════ Commit 阶段 ══════════
  │
  ▼
 拿 commit_ts（ > start_ts ）
  │
  ▼
【关键】先 commit primary：
  - 校验 primary 的锁还在（没被别人 resolve 掉）
  - 写 Write CF：(primary, commit_ts) → start_ts
  - 删 primary 的 Lock
  │
  ├─ 失败 → 事务整体未提交（失败）
  │
  └─ 成功 ★★★ 这一刻整个事务"已提交"，不可逆！
        │
        ▼
     再 commit 各 secondary（可异步/惰性）：
        对每个 secondary K：
          - 写 Write CF：(K, commit_ts) → start_ts
          - 删 K 的 Lock
        ↑ 这步中途崩了也无所谓：别人读到残留的 secondary 锁，
          会顺 primary 指针发现已提交，帮忙补完(roll forward)
  │
  ▼
DONE
```

写路径记牢两点:**Prewrite = 写 Default + 上 Lock(此时不可见);Commit = 写 Write + 删 Lock(才可见),且 Commit 不碰 Default。** primary 那一条 Write 是整个事务"成 / 败"的唯一原子开关。

## 三、碰到锁怎么办:ResolveLock(读和恢复都靠它)

```
ResolveLock( 撞到的 lock )
  │
  ▼
从 lock 里取出 primary 指针 (lock.primary, lock.start_ts)
  │
  ▼
查 primary 在 Write CF 里有没有 start_ts == lock.start_ts 的记录？
  │
  ├─ 有 PUT/DELETE 提交记录(commit_ts=C)：
  │     → 整事务已提交！帮当前 key 补 Write(K,C)→start_ts，删锁
  │       (roll forward，前滚)
  │
  ├─ 有 ROLLBACK 记录：
  │     → 整事务已回滚 → 把当前 key 的锁也清掉
  │
  └─ primary 上啥都没有(没提交也没回滚)：
        看 lock 的 TTL
        ├─ 没过期：持锁者可能还活着 → 等一会儿，重试
        └─ 已过期：判定持锁者崩了 →
             在 primary 写一条 ROLLBACK，再清掉当前锁
```

这个子流程是整套机制的恢复核心:**任何一把"孤儿锁"的命运,都不由它自己决定,而是去问 primary。** primary 提交了它就跟着提交,primary 回滚/过期它就跟着回滚——所以即使 commit secondary 的过程中节点崩溃,数据也不会处于"半提交"的不一致状态。

---

三条路径串起来的总闭环:

- **写**靠 Prewrite 上锁占位、Commit 翻 Write 开可见性,primary 一条记录定生死;
- **读**靠 Lock 判断要不要等、Write 定版本、Default 取值;
- **故障 / 撞锁**靠 ResolveLock 回去问 primary,前滚或回滚,把中间态收敛掉。