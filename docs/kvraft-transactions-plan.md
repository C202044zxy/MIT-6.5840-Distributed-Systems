# Plan: Transactions in kvraft (etcd-style If/Then/Else)

## Context

Today `kvraft1` exposes only versioned `Get`/`Put` (`kvsrv1/rpc/rpc.go`). A
developer cannot group several reads/writes into one atomic, linearizable
operation — e.g. "transfer balance from key A to key B" or "compare-and-set
across two keys." Versioned Put gives single-key conditional writes but nothing
multi-key.

We will add **transactions** modeled on etcd's `Txn`: a set of *compare*
conditions, a *then* branch of ops, and an *else* branch. The whole transaction
commits as **one Raft log entry**, so atomicity and linearizability fall out of
the existing `rsm` machinery for free — `rsm.go`'s `reader()` applies each
committed op via a single `DoOp` call under the server's mutex (see
`kvraft1/rsm/rsm.go:127-130` and `kvraft1/server.go:136-151`). A transaction is
just "one big op."

Transactions **subsume versioned Put**: a versioned Put is a transaction with a
single version-equality condition and a single Put in the *then* branch. We keep
the `Put`/`Get` RPCs for backward compatibility (the lock package, `IKVClerk`,
and the provided test harness depend on them), but server-side they will share
the same op-executor as `Txn`, demonstrating the subsumption.

**Scope:** `kvraft1` only (the Raft-replicated KV). Interface: etcd-style fluent
builder. Versioned Put: kept, reimplemented on top of the shared executor.

## Mechanism / why it is correct

- **Atomicity & isolation:** `rsm.Submit` puts the entire `TxnArgs` into a
  single Raft entry. When committed, `reader()` calls `DoOp(TxnArgs)` exactly
  once, which runs the whole compare-and-execute under `kv.mu`. No other op
  interleaves. All replicas apply the same entry in log order → identical state.
- **Linearizability:** order is the Raft commit order, same guarantee the
  current Get/Put already rely on.
- **At-most-once under retries:** conditional transactions are **not**
  idempotent (a Put bumps the version, so a naive re-apply would fail its own
  compare). The existing per-client dedup cache
  (`kvraft1/server.go:44-78, 90-101`, keyed by `ClientId` with
  `LastSeqNum`/`LastReply`) makes a resend return the *cached* `TxnReply`
  verbatim. This is the same `ErrMaybe` situation versioned Put faces, resolved
  by the dedup cache the server already maintains.

## Interface

### RPC types — `src/kvsrv1/rpc/rpc.go` (add to existing file)

```go
// --- Transaction comparison ---
type CmpTarget int
const ( CmpVersion CmpTarget = iota; CmpValue )

type CmpOp int
const ( CmpEqual CmpOp = iota; CmpNotEqual; CmpLess; CmpGreater )

type Compare struct {
    Key     string
    Target  CmpTarget  // compare the key's Version or its Value
    Op      CmpOp
    Version Tversion   // expected, when Target==CmpVersion
    Value   string     // expected, when Target==CmpValue
}

// --- Transaction op ---
type TxnOpType int
const ( OpGet TxnOpType = iota; OpPut )

type TxnOp struct {
    Type  TxnOpType
    Key   string
    Value string // for OpPut
}

// --- RPC payloads ---
type TxnArgs struct {
    Conds    []Compare
    ThenOps  []TxnOp
    ElseOps  []TxnOp
    ClientId int64
    SeqNum   int64
}

type OpResult struct {   // one per executed TxnOp, in order
    Value   string       // OpGet
    Version Tversion     // OpGet (0 ⇒ key absent), or new version after OpPut
    Exists  bool         // OpGet: whether key existed
    Err     Err          // per-op error (e.g. ErrNoKey on a Get)
}

type TxnReply struct {
    Succeeded bool        // true ⇒ Conds all held, ThenOps ran; false ⇒ ElseOps ran
    Results   []OpResult
    Err       Err         // OK / ErrWrongLeader
}
```

Notes:
- Version semantics match the current store: version 0 ⇒ key absent; a Put on an
  absent key requires no version check inside a txn (txn Puts are unconditional —
  conditioning is expressed via `Conds`), and bumps version to `old+1` (new key →
  1), mirroring `put_impl` at `kvraft1/server.go:103-121`.
- `CmpValue` comparisons (`Less`/`Greater`) compare strings lexicographically;
  callers store fixed-width / JSON-encoded numbers if they need numeric order
  (the harness already has `GetJson`/`PutJson` in `kvtest1/kvtest.go`).

### Clerk fluent builder — `src/kvraft1/client.go`

```go
// Constructors (in rpc/ or kvraft1 — keep with the Clerk for ergonomics):
func VersionEq(key string, v rpc.Tversion) rpc.Compare
func ValueEq(key, val string) rpc.Compare
func PutOp(key, val string) rpc.TxnOp
func GetOp(key string) rpc.TxnOp

type Txn struct { ck *Clerk; conds []rpc.Compare; then, els []rpc.TxnOp }
func (ck *Clerk) Txn() *Txn
func (t *Txn) If(c ...rpc.Compare) *Txn
func (t *Txn) Then(op ...rpc.TxnOp) *Txn
func (t *Txn) Else(op ...rpc.TxnOp) *Txn
func (t *Txn) Commit() (rpc.TxnReply, rpc.Err)
```

Usage (etcd-flavored):

```go
rep, err := ck.Txn().
    If(VersionEq("x", v)).
    Then(PutOp("x", "new")).
    Else(GetOp("x")).
    Commit()
// rep.Succeeded, rep.Results[0]...
```

`Commit()` reuses the **exact** leader-discovery + resend loop of `Clerk.Put`
(`kvraft1/client.go:94-128`): assign `SeqNum = ck.seqNum.Add(1)`, set
`ClientId`, round-robin `leader_id` on `!ok`/`ErrWrongLeader`, sleep 10ms. On a
resend the server dedup returns the cached reply, so `Commit` can safely return
it. Calls `ck.clnt.Call(ck.servers[leader], "KVServer.Txn", &args, &reply)`.

## System design / server changes — `src/kvraft1/server.go`

1. **Shared op executor.** Add helpers that operate on `kv.mp` under the held
   lock (factor out of the current `get_impl`/`put_impl` bodies):
   - `func (kv *KVServer) applyGet(key string) rpc.OpResult`
   - `func (kv *KVServer) applyPut(key, val string) rpc.OpResult` (version bump
     logic from `server.go:103-121`)
   - `func (kv *KVServer) evalCompare(c rpc.Compare) bool`

2. **`txn_impl(args *rpc.TxnArgs, reply *rpc.TxnReply)`** — same dedup preamble
   as `put_impl` (`server.go:90-101`): if `args.SeqNum == session.LastSeqNum`,
   return `session.LastReply.(rpc.TxnReply)`. Otherwise:
   - `ok := true; for _, c := range args.Conds { if !kv.evalCompare(c) { ok=false; break } }`
   - choose `ops := ThenOps` if `ok` else `ElseOps`; set `reply.Succeeded = ok`.
   - execute ops in order, appending `applyGet`/`applyPut` results to
     `reply.Results`.
   - `reply.Err = OK`; store `sessions[ClientId] = {SeqNum, *reply}`.

3. **`DoOp` dispatch** — add `case rpc.TxnArgs:` alongside the existing
   `GetArgs`/`PutArgs` cases (`server.go:136-151`), routing to `txn_impl`.

4. **Reimplement Get/Put atop the executor (subsumption).**
   `get_impl`/`put_impl` keep their dedup preamble but delegate the actual state
   change to `applyGet`/`applyPut`, so there is a single code path for mutating
   `kv.mp`. (`Clerk.Put` may optionally be expressed as a one-condition Txn for
   demonstration, but keeping the dedicated Put RPC avoids changing the wire
   contract the lock lab relies on.)

5. **`KVServer.Txn` RPC handler** — mirror `KVServer.Put`
   (`server.go:195-205`): `err, rep := kv.rsm.Submit(*args)`; on `err != OK` set
   `reply.Err = err`; else `*reply = rep.(rpc.TxnReply)`.

6. **labgob registration** — in `StartKVServer` (`server.go:230-235`) add:
   `labgob.Register(rpc.TxnArgs{})` and `labgob.Register(rpc.TxnReply{})`
   (nested `Compare`/`TxnOp`/`OpResult` are encoded as part of these; `TxnReply`
   must be registered because it is stored in `LastReply any` and snapshotted via
   `Snapshot`/`Restore`, `server.go:153-181`).

No changes needed in `rsm.go` or `raft1/` — they are payload-agnostic.

## Files to modify

- `src/kvsrv1/rpc/rpc.go` — add Compare / TxnOp / TxnArgs / TxnReply / OpResult.
- `src/kvraft1/server.go` — executor helpers, `txn_impl`, `DoOp` case,
  `KVServer.Txn`, labgob registration, refactor `get_impl`/`put_impl`.
- `src/kvraft1/client.go` — `Txn` builder + `Commit` + op/compare constructors.
- `src/kvraft1/test.go` — add a helper returning the concrete `*Clerk`
  (`MakeTxnClerk`) since `kvtest.IKVClerk` exposes only Get/Put.
- `src/kvraft1/txn_test.go` — **new** test file (see below).

## Tests — `src/kvraft1/txn_test.go`

Use `MakeTest(t, "TXN", nclients, nservers, reliable, crash, partitions, maxraftstate, randomkeys)`
(`kvraft1/test.go:25`) to spin up the replicated group, then drive the concrete
`*Clerk`.

1. **TestTxnBasic** — 1 client, 3 servers, reliable. Seed `a=1`. Txn with
   `If(VersionEq("a",v)).Then(PutOp("a","2"), PutOp("b","x")).Else(GetOp("a"))`.
   Assert `Succeeded`, both writes visible via `Get`, `Results` populated.
2. **TestTxnElseBranch** — condition fails (wrong version); assert `!Succeeded`,
   `ElseOps` ran, no mutation from `ThenOps`.
3. **TestTxnAtomicTransfer** — multiple clients concurrently move a unit from one
   of N keys to another using a CAS txn
   (`If(VersionEq(src,vs),VersionEq(dst,vd)).Then(PutOp(src,...),PutOp(dst,...))`),
   retrying on `!Succeeded`. After the run, assert the **sum invariant** holds —
   proves multi-key atomicity (no partial application).
4. **TestTxnSubsumesVersionedPut** — show a version-conditioned Txn behaves
   exactly like `Clerk.Put`: succeeds on matching version, `!Succeeded` on stale
   version, and creating an absent key via `If(VersionEq(k,0)).Then(PutOp(k,v))`.
5. **TestTxnConcurrentCAS** — many clients increment a shared counter via
   read-then-CAS-txn loop; assert the final value equals the number of committed
   increments (linearizable compare-and-set).
6. **TestTxnUnreliable / restarts** *(optional, mirrors GenericTest knobs)* — run
   the transfer test with `reliable=false` and/or `crash=true` to exercise the
   dedup-on-resend path and snapshot/restore of `TxnReply` in sessions.

## Verification

```bash
cd src
go build ./...                                   # type-check new RPC + clerk
go test -race -run TestTxn ./kvraft1/            # all new transaction tests
go test -race -run TestTxnConcurrentCAS -count 10 ./kvraft1/   # shake out races
go test -race ./kvraft1/...                      # ensure 4A/4B regression-free
go test -race ./kvsrv1/...                       # lock + kvsrv unaffected
```

Green criterion: all existing kvraft/kvsrv/lock tests still pass (Put/Get
unchanged on the wire) and the new `TestTxn*` suite passes under `-race`,
including repeated runs of the concurrent CAS / transfer tests.

## Out of scope (possible extensions)

- `OpDelete` (etcd has it; current store has no delete) and nested txns.
- Numeric comparisons beyond lexicographic (callers encode via `PutJson`).
- Porting transactions to `shardkv1` (cross-group txns need 2PC — large).
