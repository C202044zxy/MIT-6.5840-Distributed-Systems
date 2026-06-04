# Plan: Cross-shard transactions in shardkv (2PC + 2PL, wait-die)

## Context

`kvraft1` already has atomic, linearizable transactions (etcd-style `If/Then/Else`)
because the whole transaction commits as **one Raft entry** — every key lives in
one Raft group, so a single `DoOp` under the server mutex gives atomicity for
free (`docs/kvraft-transactions-plan.md`).

`shardkv1` breaks that assumption: keys are spread across **independent Raft
groups** (`shardcfg.Key2Shard` → `cfg.Shards[shard]` → gid, each gid its own rsm
cluster, `shardkv1/client.go:54-93`). A transaction touching keys in different
groups cannot be one Raft entry. To make several Puts/Gets atomic *across shards*
we need a distributed-commit protocol: **two-phase commit (2PC)** for atomicity
plus **two-phase locking (2PL)** for isolation.

Goal: a developer can submit a flat batch of Puts/Gets (with optional version
preconditions) spanning any shards and have it apply all-or-nothing, isolated
from concurrent transactions and single-key ops, and survive leader crashes.

### Confirmed design decisions

1. **Interface** — *flat atomic batch*: a list of ops (+ optional `Compare`
   preconditions that must all hold or the txn aborts). No `Then/Else` branching.
2. **Coordinator** — *group-based* (fault-tolerant): the chosen coordinator shard
   group logs the txn intent + decision in its own rsm and drives 2PC, so a
   leader crash mid-commit is recoverable; a crashed client never strands locks.
3. **Locking** — *wait-die* timestamp-ordered 2PL (deadlock- and starvation-free).
4. **Reconfiguration** — *static-config demo*: tests exercise cross-shard txns
   under a fixed config; as a safety net `FreezeShard` refuses (retries) while a
   key in its shard is locked, so migration never races a prepared txn. Txns
   running *concurrently with* join/leave are out of scope.

## Mechanism / why it is correct

- **Atomicity (2PC).** A txn is split per participant group. Phase 1 (`Prepare`)
  each group locks its keys, checks its preconditions, buffers its writes, and
  *votes*. Only if every group votes COMMIT does the coordinator durably decide
  COMMIT (logged to its rsm) and run Phase 2 (`Commit`) at every group; otherwise
  it decides ABORT and every group discards its buffer. No group applies writes
  until the global decision exists → all-or-nothing.
- **Isolation (2PL).** Each group holds per-key locks from `Prepare` until
  `Commit`/`Abort`. Single-key Get/Put and other txns that touch a locked key are
  bounced (`ErrLocked` → clerk retries), so no one observes or overwrites
  uncommitted state. Each group applies its committed writes in one rsm entry
  under the group mutex, exactly like today's ops.
- **Deadlock/starvation freedom (wait-die).** Every txn carries a stable
  timestamp `Ts`. On a per-group lock conflict: requester **older** than the
  holder ⇒ group votes WAIT (coordinator keeps its other locks and re-prepares
  that group later); requester **younger** ⇒ group votes DIE (coordinator aborts
  the whole txn; client retries). In any wait-chain ages strictly decrease, so no
  cycle (no deadlock). A dying txn retries with the **same `Ts`**, so it ages and
  eventually becomes oldest and never dies again (no starvation). The oldest live
  txn never dies.
- **Coordinator fault tolerance.** `Begin`, `Decide`, and `Finish` are rsm ops in
  the coordinator group, so the intent and the commit decision are Raft-replicated.
  A driver goroutine runs only where rsm is leader (each driving step ends in an
  `rsm.Submit`, which returns `ErrWrongLeader` off-leader); a new leader re-reads
  the logged txn and resumes driving. The client just re-sends `CoordTxn` to the
  new leader and waits for the stored final reply.
- **At-most-once.** Coordinator dedups whole txns by `Tid` (returns the stored
  `Finish` reply on resend). Each participant phase is keyed by `Tid` (re-`Prepare`
  returns the cached vote; `Commit`/`Abort` are idempotent no-ops once decided).
  This reuses the same idea as the existing per-client session cache
  (`shardgrp/server.go:53-69, 102-113`).

## Interface

### Reused RPC types — `src/kvsrv1/rpc/rpc.go`

Reuse the existing `Compare`, `TxnOp`, `OpResult` (already defined, lines 45-98).
Add one error constant:

```go
ErrLocked = "ErrLocked"   // key is locked by an in-flight txn; caller should retry
```

### New 2PC/coordinator payloads — `src/shardkv1/shardgrp/shardrpc/shardrpc.go`

```go
type Vote int
const ( VoteCommit Vote = iota; VoteCondFail; VoteWait; VoteDie )

// Participant 2PC ops (each submitted through the participant group's rsm)
type PrepareArgs struct {
    Tid   int64; Ts int64           // txn id + wait-die timestamp (stable across retries)
    Conds []rpc.Compare             // this group's subset of preconditions
    Ops   []rpc.TxnOp               // this group's subset of ops
    Idx   []int                     // original index of each op in the client's flat list
}
type PrepareReply struct { Vote Vote; Results []rpc.OpResult; Idx []int; Err rpc.Err } // Get reads at prepare
type CommitArgs struct { Tid int64 }
type CommitReply struct { Results []rpc.OpResult; Idx []int; Err rpc.Err }             // new versions for Puts
type AbortArgs  struct { Tid int64 }
type AbortReply struct { Err rpc.Err }

// Coordinator-facing op (client -> coordinator group)
type TxnPart struct { Gid tester.Tgid; Servers []string; Conds []rpc.Compare; Ops []rpc.TxnOp; Idx []int }
type CoordTxnArgs struct { Tid int64; Ts int64; ClientId int64; SeqNum int64; Parts []TxnPart }
type CoordTxnReply struct { Committed bool; Results []rpc.OpResult; Err rpc.Err }  // Err=ErrRetry ⇒ client retries

// Internal coordinator rsm ops (logged in the coordinator group)
type TxnBeginOp  struct { Args CoordTxnArgs }
type TxnDecideOp struct { Tid int64; Commit bool; Results []rpc.OpResult }
type TxnFinishOp struct { Tid int64; Reply CoordTxnReply }
```

### Client builder — `src/shardkv1/client.go`

```go
type ShardTxn struct { ck *Clerk; conds []rpc.Compare; ops []rpc.TxnOp }
func (ck *Clerk) Begin() *ShardTxn
func (t *ShardTxn) If(c ...rpc.Compare) *ShardTxn      // preconditions, all must hold
func (t *ShardTxn) Do(op ...rpc.TxnOp) *ShardTxn       // flat op list (order preserved in Results)
func (t *ShardTxn) Commit() (rpc.OK/false, []rpc.OpResult, rpc.Err)
```

Reuse the existing `VersionEq`/`ValueEq`/`PutOp`/`GetOp` constructors style from
`kvraft1/client.go:131-162` (add equivalents in `shardkv1/client.go`).

## System design

### Client `Commit()` — `src/shardkv1/client.go`

1. Allocate `Tid` and `Ts` **once** (e.g. `time.Now().UnixNano()`-based, unique);
   keep both stable across retries of this logical txn (wait-die aging).
2. `cfg := ck.sck.Query()`. For every key in `conds`+`ops`, map key→shard→gid and
   bucket into `TxnPart`s (carry each op's original index in `Idx` for result
   reassembly). Record `cfg.Groups[gid]` as `Servers`.
3. Pick the **coordinator group deterministically** (e.g. smallest participant
   gid) and send `CoordTxn` to it via a leader-discovery loop mirroring
   `shardgrp/client.go:38-58` (round-robin on `!ok`/`ErrWrongLeader`).
4. On `reply.Err == ErrRetry` (lock DIE/conflict): re-query config, rebuild parts,
   retry with the **same `Ts`** after randomized backoff. On stale config
   (a part's group no longer owns a key) re-query and rebuild. Otherwise return
   `(Committed, Results, OK)`.

A new `shardgrp/client.go` method `CoordTxn(args) CoordTxnReply` (leader loop), and
participant methods `Prepare/Commit/Abort` are **not** needed on the clerk — the
coordinator talks to participants directly with internal clerks built from
`TxnPart.Servers` (`shardgrp.MakeClerk`), so add clerk methods for the participant
RPCs: `Prepare`, `CommitTxn`, `AbortTxn` (each a leader loop).

### Participant role — `src/shardkv1/shardgrp/server.go`

New replicated state on `KVServer` (added to `Snapshot`/`Restore` and the
`StartServerShardGrp` `labgob.Register` block):

```go
locks    map[string]int64               // key -> Tid holding it
prepared map[int64]*PreparedTxn          // Tid -> {Ts, lockedKeys, bufferedPuts, getResults, idx, condOK}
decided  map[int64]bool                  // Tid -> committed? (idempotent Commit/Abort + dedup)
```

`DoOp` dispatch gains `case shardrpc.PrepareArgs / CommitArgs / AbortArgs`.

- `prepareImpl` (under `kv.mu`, all-or-nothing per group):
  - If `decided[Tid]` exists or `prepared[Tid]` exists → return cached vote (dedup).
  - Ownership check on every key (`ownedShards`, else `ErrWrongGroup`).
  - Lock acquisition: if any key is in `locks` held by another `Tid'`, compare
    `Ts`: requester younger than *any* holder ⇒ `VoteDie`; else ⇒ `VoteWait`
    (grant nothing, no state change). If all keys free → lock them all for `Tid`.
  - Evaluate `Conds` with the existing comparison logic (port `evalCompare` from
    `kvraft1/server.go:165-198`). If any fails → keep locks, record `condOK=false`,
    `VoteCondFail` (locks held so the decision is stable until Abort).
  - Execute Gets into `getResults` (read current value/version); buffer Puts
    (do **not** mutate `kv.mp`). Store `prepared[Tid]`. `VoteCommit`.
- `commitImpl`: apply buffered Puts using the version-bump logic from
  `putImpl` (`server.go:125-143`), fill `CommitReply.Results` with new versions,
  release `prepared[Tid]` locks, set `decided[Tid]=true`. Idempotent if already
  decided.
- `abortImpl`: drop buffer, release locks, `decided[Tid]=false`. Idempotent.
- **2PL on single-key ops:** in `getImpl`/`putImpl`, after the ownership check, if
  the key is in `locks` (held by any txn) → return `rpc.ErrLocked`. Add `ErrLocked`
  handling (retry w/ 10ms backoff) to `shardgrp/client.go` `Get`/`Put` loops.
- **Freeze safety:** in `freezeShardImpl`, if any locked key maps to `args.Shard`
  → return a retryable error; add that case to the `FreezeShard` clerk loop
  (`shardgrp/client.go:96-118`) so `ChangeConfigTo` waits the txn out.

### Coordinator role — `src/shardkv1/shardgrp/server.go`

New replicated state: `coordTxns map[int64]*CoordState` where `CoordState =
{Args CoordTxnArgs, Phase int /*Began|Decided|Done*/, Commit bool, Reply CoordTxnReply}`.
Add to `Snapshot`/`Restore` + `labgob.Register`.

- `DoOp` gains `case TxnBeginOp / TxnDecideOp / TxnFinishOp`, each mutating
  `coordTxns[Tid].Phase` and storing results/reply.
- RPC handler `KVServer.CoordTxn(args, reply)`: `rsm.Submit(TxnBeginOp{args})`
  (dedup: if `Done`, return stored `Reply`); then wait (poll `coordTxns[Tid].Phase`
  or a per-Tid cond/channel) until `Done`; return stored `Reply`.
- **Driver goroutine** (started in `StartServerShardGrp`, runs forever): scan
  `coordTxns` for not-`Done` entries; for each, depending on `Phase`:
  - `Began`: send `Prepare` to all `Parts` in parallel (internal `shardgrp.Clerk`s
    from `TxnPart.Servers`). Aggregate votes:
    - any `VoteDie` → decide ABORT, mark reply `Err=ErrRetry`.
    - else any `VoteWait` (and no Die) → keep granted locks, re-`Prepare` the WAIT
      groups after small delay; loop until all non-WAIT.
    - else any `VoteCondFail` → decide ABORT, reply `Committed=false` (+ reads).
    - all `VoteCommit` → decide COMMIT, stash Get `Results`.
    `rsm.Submit(TxnDecideOp{Tid, commit, results})`.
  - `Decided`: send `Commit` (or `Abort`) to every prepared participant until all
    ack (idempotent, retried). Merge `CommitReply.Results` (Put new versions) with
    prepared Get results, reassemble by `Idx` into final ordered `Results`.
    `rsm.Submit(TxnFinishOp{Tid, reply})`.
  - Each `rsm.Submit` fails with `ErrWrongLeader` off-leader, so only the leader
    drives; a new leader resumes from the logged `Phase`.

No changes to `rsm.go` or `raft1/` — they are payload-agnostic (confirmed:
`rsm.Submit` wraps any registered type, `kvraft1/rsm/rsm.go`).

## Files to modify / create

- `src/kvsrv1/rpc/rpc.go` — add `ErrLocked` (reuse `Compare`/`TxnOp`/`OpResult`).
- `src/shardkv1/shardgrp/shardrpc/shardrpc.go` — add the 2PC + coordinator types above.
- `src/shardkv1/shardgrp/server.go` — participant impls (`prepareImpl`/`commitImpl`/
  `abortImpl` + `evalCompare`), coordinator impls + driver goroutine, `DoOp` cases,
  RPC handlers `Prepare`/`CommitTxn`/`AbortTxn`/`CoordTxn`, 2PL checks in
  `getImpl`/`putImpl`, freeze safety, `Snapshot`/`Restore` + `labgob.Register` for
  all new state and op types.
- `src/shardkv1/shardgrp/client.go` — add `Prepare`/`CommitTxn`/`AbortTxn`/`CoordTxn`
  leader-loop methods; add `ErrLocked` retry to `Get`/`Put` and a retry case to
  `FreezeShard`.
- `src/shardkv1/client.go` — `Begin`/`If`/`Do`/`Commit` builder, key→group bucketing,
  coordinator selection, retry-with-stable-`Ts` loop, op/compare constructors.
- `src/shardkv1/test.go` — add `MakeTxnClerk() (*Clerk, *tester.Clnt)` exposing the
  concrete `*Clerk` (the harness `IKVClerk` only has Get/Put), mirroring
  `MakeClerk` (`test.go:66-70`); reuse `setupKVService`/`joinGroups` to get ≥2 groups.
- `src/shardkv1/txn_test.go` — **new** tests (below).

## Tests — `src/shardkv1/txn_test.go`

Use `MakeTest(t, "TXN", reliable)` + `setupKVService()` + `joinGroups` to get ≥2
groups, then a helper `keysOnDistinctGroups(cfg, n)` that picks keys whose
`Key2Shard` resolves to distinct gids (so the txn is genuinely cross-shard).

1. **TestTxnCrossShardBasic5D** — 2 groups, reliable. Atomic batch of Puts on keys
   in different groups; assert all visible via `Get`, `Results` ordered, versions
   correct.
2. **TestTxnPreconditionAbort5D** — a `Compare` precondition fails; assert
   `Committed=false`, no key mutated, `Results` carry current reads.
3. **TestTxnAtomicTransfer5D** — N keys spread across groups, balance 100 each; M
   concurrent clients do `If(VersionEq(src,vs),VersionEq(dst,vd)).Do(PutOp(src,…),
   PutOp(dst,…))`, retrying on abort. Assert the **sum invariant** holds (proves
   cross-group all-or-nothing). Mirrors `kvraft1/txn_test.go` transferLoad.
4. **TestTxnIsolationCounter5D** — many clients increment a shared counter via
   read-then-CAS txn; final value == number of committed increments (serializable).
5. **TestTxnWaitDieProgress5D** — high contention on a few cross-group keys; assert
   every client eventually commits (no deadlock/starvation).
6. **TestTxnCoordinatorFailover5D** — kill the coordinator group's leader mid-txn
   (`ts.Group(gid).ShutdownServer`/restart); assert the txn still completes and the
   sum invariant holds (proves group-based coordinator fault tolerance).
7. **TestTxnUnreliable5D** — run the transfer test with `reliable=false` to exercise
   dedup-on-resend and the `ErrLocked`/retry paths.

## Verification

```bash
cd src
go build ./...                                          # type-check new RPC + clerks
go test -race -run TestTxn ./shardkv1/                  # new cross-shard txn suite
go test -race -run TestTxnAtomicTransfer5D -count 10 ./shardkv1/   # shake out races/deadlock
go test -race -run TestTxnCoordinatorFailover5D -count 5 ./shardkv1/
go test -race ./shardkv1/...                            # 5A/5B/5C regression-free
go test -race ./kvsrv1/... ./kvraft1/...                # Get/Put wire contract unchanged
```

Green criterion: all existing shardkv/kvsrv/kvraft/lock tests still pass (single-key
Get/Put wire contract unchanged) and the new `TestTxn*5D` suite passes under `-race`,
including repeated transfer/failover runs. Follow the repo workflow (CLAUDE.md): on
green, commit per-step with `Feat(5D): …` messages and push to `main`.

## Out of scope

- Transactions concurrent with reconfiguration (shard migration mid-txn) — the
  freeze-on-locked safety net blocks migration until the txn drains instead.
- Range/prefix ops, deletes, nested txns, numeric (non-lexicographic) compares.
