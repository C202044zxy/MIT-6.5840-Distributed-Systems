# Plan: Leader lease reads in kvraft (Raft §8 read optimization)

## Context

Today **every** `Get` in `kvraft1` goes through the full replication pipeline:
`KVServer.Get` → `rsm.Submit` → `raft.Start` → AppendEntries to a majority →
commit → apply → reply (`kvraft1/server.go:299-309`, `kvraft1/rsm/rsm.go:151-192`).
That is correct but wasteful: a read does not change state, yet it costs a log
entry, ≥2 AppendEntries RPCs (one per follower on a 3-server group), disk
persistence on every replica, and one commit+apply round-trip of latency
(~15-20 ms with the current 5 ms commit / 10 ms apply tickers).

The end of **§8 of the Raft paper** describes the fix: the *leader* already has
all committed data, so it can answer reads from its local state machine without
writing to the log — *provided it can prove it is still the leader at the time
of the read*. Without that proof, a deposed leader that doesn't yet know about
a new leader would happily serve **stale data**, breaking linearizability.

The proof mechanism is a **lease**: a time window during which the leader knows
no other server can have committed anything. The price is paid at **term
switches**: a freshly elected leader must wait until the previous leader's
lease has provably expired before it commits or serves anything, so leader
changes get slower by up to one lease duration. That trade-off is inherent to
the design and is exactly what one of the new tests demonstrates.

**Deliverables**

1. Leader serves `Get` locally, bypassing `rsm.Submit`/the Raft log, guarded by
   a lease. `Put`/`Txn` unchanged.
2. All existing kvraft tests (`go test -race ./kvraft1/...`) still pass —
   including the porcupine linearizability checks under partitions/crashes.
3. New test: optimized Gets use **fewer RPCs** (measured via the tester's
   `Config.RpcTotal()`).
4. New test: **term switches are slower** because the new leader waits out the
   old lease.

**Scope:** `raft1` + `kvraft1/rsm` + `kvraft1` only. Leases are *opt-in* per
Raft instance, so `raft1`'s own tests, the `rsm` 4A tests, and `shardkv1`
(which all build on the same code) keep today's behavior and timing exactly.

## Mechanism / why it is correct

### The problem with naive leader-local reads

Suppose leader L1 is partitioned away. The other servers elect L2, which
commits `Put(k, v2)` and acks the client. L1 still believes it is leader (it
has no way to know otherwise — its term never bumped). If a client now asks L1
to `Get(k)` and L1 answers from local state, it returns the old `v1`. A read
that *starts after a write completed* returned the pre-write value → not
linearizable. Going through the log prevents this today because L1's
AppendEntries for the Get's log entry would be rejected (`reply.Term > term`,
forcing step-down) and the Get would never commit on L1.

So a local read is safe only if, *at the moment of the read*, the leader can
rule out the existence of a newer leader that may have committed something.

### The lease invariant

A **lease** is a promise the leader derives from heartbeat acknowledgments:

> If a heartbeat round was *started* at wall-clock time `t0` and a **majority**
> of servers (counting the leader itself) acknowledged it while still in the
> leader's term, then the leader may serve local reads until `t0 +
> leaseDuration`.

By itself this proves nothing — followers don't promise to refuse votes. The
safety comes from pairing it with a rule on the *next* leader:

> **Holdoff rule.** When a server becomes leader, it must not advance its
> `commitIndex` (and therefore must not apply or acknowledge *anything*) until
> `electionTime + leaseDuration` has passed.

**Why the pair is safe.** Let the old leader's last majority-acked round start
at `t0`, so its lease expires at `t0 + L`. Suppose a new leader is elected at
time `T_e` (the moment it collects its majority of votes). The ack-majority and
the vote-majority intersect in at least one server S. Two observations about S:

1. S acked the old leader's round at some time `r ≥ t0`, and at that moment S's
   `currentTerm` equaled the old leader's term (an ack counts only if the reply
   carries the leader's own term; a server already in a higher term replies
   with that higher term and `Success=false` — see
   `raft_append_entries.go:29-34`).
2. S voted for the new leader at some time `v`, at which moment S's
   `currentTerm` was the new (strictly higher) term.

Terms at a single server never decrease, so `v > r ≥ t0`, and the new leader's
election completes no earlier than its last needed vote: `T_e ≥ v ≥ t0`.
Therefore:

```
old lease expiry  =  t0 + L  ≤  T_e + L  =  new leader's holdoff end
```

So during the *entire* window in which the old leader may serve local reads,
the new leader has not committed (hence not acked) anything. The old leader's
local committed state is still the freshest committed state in the cluster, and
its reads are linearizable. Conversely, by the time the new leader starts
committing, the old leader's lease has expired and its fast path shuts off
(reads then fall back to the log path, where they hang until the old leader
learns the new term — same as today).

Facts that make this argument airtight in *this* codebase:

- `t0` must be captured **before** the heartbeat RPCs are sent, so it
  underestimates every ack time (conservative).
- All servers run in one process and share `time.Now()` — **no clock skew**.
  (The paper has to assume bounded clock drift; we get it for free.)
- On the leader, `commitLog()` is the **only** code path that advances
  `rf.commitIndex` (the `AppendEntries`/`InstallSnapshot` follower paths never
  run on a current-term leader), so pausing `commitLog` during holdoff really
  does gate every ack. There is no leak.
- A leader leaves the Leader state only via a term bump, so re-checking
  "still leader, same term" under the lock when an ack arrives is a sound
  validity test for that ack.
- Lease state must be **volatile** (never persisted). A restarted server comes
  back as Follower; if it later wins an election, the holdoff rule covers
  whatever lease it (or anyone else) held before the crash. Monotonic
  `time.Now()` is preserved within the process.

### Two more gates the fast read needs

The lease proves "no *other* leader has committed anything newer." Two local
staleness hazards remain, each with its own gate:

**Gate 1 — the leader must know everything that is committed.** Right after
winning an election, a leader's `commitIndex` can lag reality: entries from
prior terms may be committed cluster-wide without this leader knowing (Raft
forbids counting replicas of old-term entries — see Figure 8 of the paper; our
`commitLog()` enforces `rf.log[N-rf.offset].Term == term`). The standard fix is
the rule "a leader knows all committed entries once it has committed an entry
*of its own term*." So the fast path requires:

```
term(log[commitIndex]) == currentTerm
```

(using `lastIncludedTerm` when `commitIndex == offset-1`, i.e. the snapshot
boundary). If the gate fails, the Get simply falls back to the log path — and
that very fallback *commits a current-term entry*, opening the fast path for
subsequent reads. Self-healing; no no-op entry needed (a no-op appended in
`convert2Leader` would shift log indices and break raft1's 3B index
assertions, so we deliberately avoid it).

**Gate 2 — the state machine must have caught up to `commitIndex`.** The lease
lives in Raft, but the read is served from `KVServer.mp`, which trails
`commitIndex` by the apply pipeline (`applyLog` ticker + `rsm.reader`). A write
could be committed+acked at index 100 while the kv map only reflects index 97.
So the fast path captures `readIndex := commitIndex` *while the lease is
valid*, then **waits until `rsm.lastApplied ≥ readIndex`** before reading the
map. (This is the paper's "readIndex" idea, with the lease replacing the
round of heartbeats.)

Why it is OK that the lease might expire while waiting: linearizability lets us
place the read's linearization point anywhere between its invocation and its
response. We place it at the instant the lease check passed. At that instant,
every write that had ever been acked (by anyone) is ≤ `readIndex` (gates +
lease), and we return state ≥ `readIndex`. Writes committed afterwards
linearize after our read point — allowed, since their responses don't precede
our invocation.

### Why the fast path skips the duplicate-detection table

`get_impl` (`kvraft1/server.go:40-79`) consults **and updates**
`kv.sessions`. The fast path must not do either:

- *Must not update:* the write would happen only on the leader, diverging its
  `sessions` map from the replicas' (which only see replicated ops). Snapshots
  encode `sessions`, so divergence would even propagate.
- *Need not consult:* dedup exists to stop non-idempotent ops (Put) from
  executing twice. A Get re-executed on retry just reads again; returning a
  fresher value on the retry is linearizable because the retry extends the
  operation's invocation-response window.

So the fast path reads `kv.mp` directly under `kv.mu` and touches nothing
else. The slow path keeps using `get_impl` unchanged (it is replicated, so its
session updates stay consistent).

### Choosing `leaseDuration = 500 ms`

- **Renewal margin:** heartbeats fire every 150 ms (`heartbeat()`), so a
  healthy leader renews its lease >3× per lease window; the fast path stays
  continuously open in steady state.
- **Safety does not constrain it.** In the classic lease scheme (followers
  promise not to *vote* during a lease) the lease must be shorter than the
  minimum election timeout. Our holdoff-based scheme has no such coupling — any
  `L` is safe — so we pick it for testability.
- **Testability:** the term-switch test must distinguish "new leader waited out
  the lease" (~500 ms) from "clerk just took a while to find the new leader"
  (≤ ~270 ms worst case, see Tests). 500 ms gives a clean >100 ms margin on
  both sides of a 400 ms threshold; 300 ms would not.
- Cost: each leader election delays the first commit by ≤500 ms. Affects only
  kvraft (leases are opt-in) and only at term switches.

## Interface

### Raft — `src/raft1/raft.go` (new public methods, *not* added to `raftapi.Raft`)

```go
// leaseDuration is how long a majority-acked heartbeat round entitles the
// leader to serve local reads, and equally how long a fresh leader must
// hold off committing. Renewed every 150ms heartbeat; safety does not
// depend on its value (see holdoff rule), only liveness/testability do.
const leaseDuration = 500 * time.Millisecond

// EnableLeaseRead turns on lease maintenance and the new-leader commit
// holdoff for this peer. Called once at startup (before the first
// election matters) by services that intend to use LeaseRead. Raft peers
// that never enable it behave exactly as before.
func (rf *Raft) EnableLeaseRead()

// LeaseRead returns (readIndex, true) if this peer may serve a local read
// right now: it is leader, its lease is valid, its new-leader holdoff has
// passed, and it has committed an entry of its current term. readIndex is
// the current commitIndex; the caller must wait until its state machine
// has applied through readIndex before reading. Returns (0, false)
// otherwise, in which case the caller should use the log path.
func (rf *Raft) LeaseRead() (int, bool)
```

`raftapi/raftapi.go` is left untouched (it is the course-provided seam); the
rsm layer reaches these methods via type assertion (e.g.
`rsm.rf.(interface{ LeaseRead() (int, bool) })`), so any `raftapi.Raft` that
lacks them simply never gets a fast path.

### RSM — `src/kvraft1/rsm/rsm.go`

```go
// EnableLeaseRead forwards to the underlying Raft if it supports leases.
func (rsm *RSM) EnableLeaseRead()

// ReadIndex asks Raft for a lease-protected read index. (0,false) means
// "no fast path right now" — fall back to Submit.
func (rsm *RSM) ReadIndex() (int, bool)

// WaitApplied blocks until the local state machine has applied through
// index, or returns false after timeout (caller falls back to Submit).
func (rsm *RSM) WaitApplied(index int, timeout time.Duration) bool
```

### KVServer — `src/kvraft1/server.go`

No RPC or client changes at all. `rpc.GetArgs`/`rpc.GetReply`, the Clerk, and
the retry/`ErrWrongLeader` protocol are untouched; only the server-side
handling of `Get` gains a fast path. `StartKVServer` additionally calls
`kv.rsm.EnableLeaseRead()`.

## System design

Each component's responsibility, in one or two sentences; the *how* is up to
the implementer, but the stated invariants must hold.

1. **Lease state (`raft.go`).** Three new volatile fields on `Raft`, all
   guarded by `rf.mu`, all excluded from `persist()`: the enable flag, the
   lease expiry instant, and the new-leader holdoff instant.

2. **Granting/renewing (`heartbeat()` in `raft_append_entries.go`).** Each
   150 ms round captures its start time and counts acks (self included) across
   its per-peer reply handlers. An ack is valid only if the reply succeeded
   and, under the lock, the peer is still leader in the same term. On reaching
   a majority, extend the lease to `roundStart + leaseDuration` — taking the
   *max* with the current expiry, since rounds can complete out of order.
   Renewal lives in `heartbeat()` **only**; do not also count `replicateLog()`
   acks (150 ms heartbeats already renew a 500 ms lease with 3× margin, and
   `Success=false` there can mean "term fine, log mismatch", which invites a
   counting bug).

3. **New-leader holdoff (`convert2Leader()` + `commitLog()` in `raft.go`).**
   On becoming leader (when enabled): set the holdoff to `now + leaseDuration`
   and **clear any leftover lease** — a deposed-and-re-elected leader must not
   carry a lease across terms; it re-earns one from its first acked round (which
   may complete during the holdoff — fine, the read gate checks both).
   `commitLog()` must not advance `commitIndex` while the holdoff is pending.
   That single pause point gates every externally visible effect (apply →
   `Submit` reply → client ack), which is precisely the holdoff rule.

4. **The read gate (`LeaseRead()` in `raft.go`).** Under `rf.mu`, succeed only
   when *all* hold: leases enabled, not killed, currently leader, holdoff
   passed, lease unexpired, and Gate 1 (the entry at `commitIndex` carries
   `currentTerm`; use `lastIncludedTerm` at the snapshot boundary). Return
   `commitIndex` as the read index. The holdoff check is technically redundant
   (during holdoff, `commitIndex` cannot yet point at a current-term entry) —
   keep it as defense in depth.

5. **rsm plumbing (`rsm.go`).** `EnableLeaseRead`/`ReadIndex` are thin
   type-asserted forwards to Raft. `WaitApplied` polls `rsm.lastApplied`
   (under `rsm.mu`, ~1 ms period) up to a timeout. The timeout exists for one
   known cold-start corner: after a crash-restart with a snapshot,
   `rsm.lastApplied` starts at 0 (existing TODO at `rsm.go:81`) and only jumps
   once a fresh entry applies — that can't cause a *wrong* read (Gate 1 forces
   a fresh commit, whose apply raises `lastApplied` past the snapshot), only a
   slow one, which the timeout converts into a fallback.

6. **KVServer fast path (`server.go`).** `Get` tries
   `ReadIndex` → `WaitApplied(readIndex, ~100ms)` → read `kv.mp` under
   `kv.mu` (fill Value/Version/`OK`, or `ErrNoKey`) — **no `kv.sessions`
   access** — and returns. On any gate failure or timeout it falls through to
   the existing `rsm.Submit` code, byte-for-byte unchanged. `Put`/`Txn` and
   the Clerk are untouched.

Concurrency ground rules: every new field is read/written only under `rf.mu`
(resp. `rsm.mu`); per-round ack counters are mutated only inside the reply
handlers' existing locked sections; no atomics needed. `-race` is the referee.

### Failure walk-throughs (sanity checks)

- **Partitioned old leader, client on its side:** for ≤500 ms it serves reads
  from its (still globally freshest) committed state — safe by the holdoff
  rule. Then its lease expires, `LeaseRead` fails, Gets fall into `Submit`,
  which hangs (no majority) until the partition heals and it observes the new
  term — exactly the behavior `TestOnePartition4B` already demands of the
  minority.
- **Leader crash + restart:** restarts as Follower (state volatile); no lease,
  no fast path. If re-elected, the holdoff covers any lease that existed
  before the crash.
- **Unreliable network:** heartbeat rounds sporadically miss majority acks →
  lease lapses → reads transparently fall back to the log path. Correct,
  merely slower; the lease re-establishes on the next acked round.
- **Snapshots:** `readIndex` can sit at the snapshot boundary
  (`offset-1`/`lastIncludedTerm` case in Gate 1); `WaitApplied` compares
  against `rsm.lastApplied`, which snapshot restores also advance.

## Files to modify / create

| File | Change |
| --- | --- |
| `src/raft1/raft.go` | `leaseDuration` const; 3 lease fields; `EnableLeaseRead`; `LeaseRead`; holdoff set in `convert2Leader`; holdoff pause in `commitLog` |
| `src/raft1/raft_append_entries.go` | per-round start time + ack counting in `heartbeat()`; lease extension on majority |
| `src/kvraft1/rsm/rsm.go` | `EnableLeaseRead`, `ReadIndex`, `WaitApplied` |
| `src/kvraft1/server.go` | fast path in `Get`; `EnableLeaseRead()` call in `StartKVServer` |
| `src/kvraft1/lease_test.go` | **new** — the two tests below |

Untouched on purpose: `raftapi/raftapi.go`, `kvraft1/client.go`,
`kvsrv1/rpc/rpc.go`, all tester/labrpc code, `shardkv1` (its shardgrp rsm never
calls `EnableLeaseRead`, so it keeps today's behavior).

## Tests — `src/kvraft1/lease_test.go`

`package kvraft`, naming per the existing custom-feature convention in
`txn_test.go` (descriptive names, part string `"LEASE"`, no lab suffix). Both
tests use a **reliable** network and `maxraftstate = -1` — important because
with snapshots enabled, `sendSnapshot()` broadcasts InstallSnapshot to all
peers every 150 ms once `offset > 0`, which would pollute RPC counts.

### `TestLeaseGetRPCCount` — fewer RPCs per Get

3 servers. Steps: one `PutAtLeastOnce` + `CheckGet` warmup (elects a leader
*and* commits a current-term entry, opening Gate 1); sleep ~600 ms (several
heartbeat rounds establish the lease, election traffic drains); snapshot
`n0 := ts.Config.RpcTotal()`; issue `N = 200` sequential `ck.Get("k")`,
asserting each value; assert `RpcTotal() - n0 < 2*N`.

Why the bound separates: optimized, each Get costs exactly 1 client RPC, and
background traffic is only heartbeat (2 RPCs/150 ms) + replicateLog
(2 RPCs/100 ms) ≈ 33 RPC/s → delta ≈ 230-260. Unoptimized, every Get adds a
log entry → ≥2 AppendEntries on top of the client RPC → delta ≥ 600.

### `TestLeaseNewLeaderHoldoff` — term switches wait out the lease

5 servers. Steps: warmup Put/CheckGet; find the leader with
`rsm.Leader(ts.Config, Gid)`; `MakePartition(l)` + `Partition(p1, p2)` (the
old leader lands in the 2-server minority `p2`); `ckp1 := ts.MakeClerkTo(p1)`.
Poll every 5 ms (≤5 s bound) for a leader **among p1 only** — a local helper
over `ts.Group(Gid).Services()` type-asserted to `raftapi.Raft` is required,
because the deposed leader in p2 reports `isLeader == true` forever (the
election ticker only fires for Follower/Candidate), so `rsm.Leader` would keep
returning it. Record `tElect` at first sighting; then time one
`ckp1.Get("k")` from `tElect`. Assert: correct value, **elapsed ≥ 400 ms**
(the holdoff happened), elapsed < 3 s (liveness).

Why 400 ms separates: with the holdoff, the Get cannot complete before
`T_e + 500 ms` and `tElect ≤ T_e + ~10 ms` → elapsed ≥ ~490 ms. Without it,
the Get completes once the clerk finds the new leader and one entry commits:
≤2 failed calls to disconnected p2 servers (labrpc fails calls to disabled
ends within ≤100 ms when `longDelays` is off — kvraft tests never set it) +
~10 ms per `ErrWrongLeader` from p1 followers + ~30 ms commit ≈ ≤270 ms.
>120 ms margin on each side of the threshold.

### Existing-test impact (checked against current sources)

- **`TestOnePartition4B`**: the majority-side Put it performs after
  partitioning now takes election (≥300 ms of heartbeat silence) + 500 ms
  holdoff — by which time the minority leader's lease (≤500 ms from partition)
  is long dead, so the test's "minority Get must not complete within 1 s"
  assertion is never racing a live lease. The post-heal re-election + holdoff
  (~1.1 s) is absorbed by the test's 1 s sleep + 3 s completion windows.
- **`TestSpeed4B`/`TestSpeed4C`**: the `ck.Get("x")` warmup precedes
  `start := time.Now()` and absorbs election + holdoff; no leader change
  happens during the timed loop (reliable network), so the 33 ms/op budget is
  unaffected.
- **rsm 4A tests** (`TestLeaderFailure4A` etc. with 1 s windows): unaffected —
  `rsm/test.go` builds RSMs directly, never through kvraft's `StartKVServer`,
  so `leaseEnabled` stays false and the holdoff never engages.
- **raft1 3A-3D and shardkv1 5A-5D**: same reason — leases never enabled,
  zero behavioral delta; the new code is dead weight behind the flag.
- **Partition/crash `GenericTest` variants with porcupine**: these become the
  main *correctness* stress for the lease (fast reads racing repartitions);
  the linearizability checker is the oracle.

## Verification

From `src/` (all with `-race`):

```bash
go test -race ./raft1/                       # raft internals changed
go test -race ./kvraft1/rsm/                 # 4A unaffected by the flag
go test -race -run Lease ./kvraft1/          # the two new tests
go test -race -run Lease -count 5 ./kvraft1/ # timing stability
go test -race ./kvraft1/...                  # full 4B/4C + porcupine suites
go test -race ./shardkv1/...                 # confirm rsm/raft changes inert
```

Watch specifically: `TestOnePartition4B`, `TestSpeed4B`, `TestSpeed4C`, and the
`Partition`/`Persist` Generic variants (linearizability under leases). Flaky
candidates should be re-run with `-count 10`. Negative-test the new tests once:
disable the fast path (return `(0,false)` from `ReadIndex`) → RPC-count test
must fail; remove the `commitLog` holdoff → holdoff test must fail. Finally
reproduce CI locally (build + vet with the exclusion lists from `CLAUDE.md`).

## Out of scope (possible extensions)

- **ReadIndex without leases** (paper §8's other variant): leader confirms
  leadership with a fresh heartbeat round *per read batch* — no holdoff and no
  clock assumptions, but each read still costs a heartbeat round-trip.
- **Follower reads**: followers could serve reads at a leader-supplied
  readIndex; needs an extra RPC and the same apply-wait.
- **Lease reads for read-only `Txn`s** and for `shardkv1` shardgrps (would
  need per-shard ownership reasoning on top of the lease).
- **Restoring `rsm.lastApplied` from the snapshot index** at startup (existing
  TODO) — would remove the one cold-start fallback case in `WaitApplied`.
