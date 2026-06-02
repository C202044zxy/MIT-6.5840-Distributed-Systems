# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This is an implementation of the MIT 6.5840 (formerly 6.824) Distributed Systems
course labs in Go. `go.mod` lives in `src/`, so **all `go` commands must be run
from `src/`** (module name `6.5840`, requires Go 1.22+).

## Commands

Run a whole lab's test suite (always with `-race`):

```bash
cd src
go test -race ./raft1/          # Lab 3: Raft (3A election, 3B log, 3C persist, 3D snapshot)
go test -race ./kvraft1/...     # Lab 4: fault-tolerant KV (4A rsm, 4B kvraft)
go test -race ./kvsrv1/...      # Lab 2: KV server + lock
go test -race ./shardkv1/...    # Lab 5: sharded KV (5A shardcfg/ctrler, 5B/5C shardkv)
```

Run a single test by name (use the suffix to scope to a lab part, e.g. `3A`, `5B`):

```bash
cd src
go test -race -run TestBackup3B ./raft1/
go test -race -run 3A ./raft1/          # all 3A tests
```

Flaky distributed tests should be run repeatedly to catch races:

```bash
cd src
go test -race -run TestFigure8Unreliable3C -count 20 ./raft1/
```

MapReduce (Lab 1) is NOT run via `go test`; it uses shell harnesses that build
plugins and spawn coordinator/worker processes:

```bash
cd src/main
bash test-mr.sh          # full MR correctness + crash tests
bash test-mr-many.sh 3   # run test-mr.sh N times
```

## Continuous integration

`.github/workflows/ci.yml` runs a lightweight **build + vet** check on every
push to `main` and on pull requests (Go 1.22, all commands from `src/`). It
deliberately does **not** run `go test` — the distributed suites are slow and
intentionally fault-injecting, so they are flaky on shared runners; run them
locally with `-race` instead.

Scope is filtered to keep the check green on the course-provided tree:

- **Build** excludes `main/` (orphan scaffolding — `diskvd`/`lockc`/`pbc`/`viewd`
  reference old-lab packages absent from this repo) and `mrapps/` (a set of
  standalone MapReduce plugins; each redeclares `Map`/`Reduce` and is built
  individually with `-buildmode=plugin`, so it won't compile as one package).
- **Vet** additionally skips packages whose provided test/harness files carry
  pre-existing `go vet` findings we don't own (unkeyed `kvtest1` struct
  literals, `t.Fatalf` from non-test goroutines): `labrpc`, `tester1`, `kvsrv1`,
  `kvsrv1/lock`, `kvraft1`, `kvraft1/rsm`, `shardkv1`. Vet still covers `raft1`
  and the shardkv implementation subpackages. To reproduce CI locally:
  `go build $(go list ./... | grep -vE '/(main|mrapps)$')` then
  `go vet $(go list ./... | grep -vE '/(main|mrapps|labrpc|tester1|kvsrv1|kvraft1|shardkv1|lock|rsm)$')`.

## Architecture

The labs build a layered stack — each lab consumes the layer below as a library.
The dependency direction matters when changing shared types:

```
Lab 1  mr/ + mrapps/                MapReduce (standalone, plugin-based)
Lab 2  kvsrv1/ + kvsrv1/lock        single KV server; lock built on it via versioned Put
Lab 3  raft1/                       Raft consensus, implements raftapi.Raft
Lab 4  kvraft1/rsm/ -> kvraft1/     replicated state machine over Raft, then KV on top
Lab 5  shardkv1/                    sharded KV: shardctrler + shardgrp, each group an rsm
```

Key contracts / seams:

- **`raftapi/raftapi.go`** is the interface boundary. `raft1` implements
  `raftapi.Raft`; everything above (`rsm`) depends only on this interface, never
  on the concrete `raft1` package. Committed entries flow up via `ApplyMsg` on
  the `applyCh` passed to `Make`.
- **`kvraft1/rsm/rsm.go`** generalizes "replicate a service over Raft." A service
  implements the `StateMachine` interface (`DoOp`, `Snapshot`, `Restore`); `RSM`
  submits ops through Raft, waits for them to apply, and triggers snapshots when
  the log exceeds `maxraftstate`. Both `kvraft1/server.go` and each
  `shardkv1/shardgrp/server.go` are built as `StateMachine`s on top of `RSM`.
- **`kvsrv1/rpc/rpc.go`** defines the versioned Get/Put RPC (`Tversion`) shared
  across Labs 2/4/5. Put is conditional on version, which is the primitive the
  lock (`kvsrv1/lock`) and the shardctrler rely on for linearizable, idempotent
  updates.
- **`shardkv1`**: `shardctrler` stores configurations (it is itself a client of a
  `kvsrv`) and drives reconfiguration via `ChangeConfigTo`; `shardcfg` defines the
  shard->group mapping and `NShards`; `shardgrp` is a replicated group serving its
  assigned shards, with shard-migration RPCs in `shardgrp/shardrpc`. The shardkv
  `Clerk` queries the controller, then routes each key to the owning group's clerk.

Test infrastructure (do not modify when solving labs — the tester relies on it):

- **`tester1/`** — the harness (`config.go`, `srv.go`, `clnts.go`, `group.go`,
  `persister.go`, `annotation.go`) that creates servers/clients, injects network
  partitions and crashes, and persists state across simulated restarts.
- **`labrpc/`** — simulated network with configurable unreliability/delays/drops;
  all inter-node RPC goes through it, never real sockets.
- **`labgob/`** — gob wrapper used for RPC + persistence; warns on encoding
  non-capitalized struct fields (a common silent bug — RPC/snapshot fields must be
  exported).
- **`kvtest1/`** and **`models1/`** — KV test helpers and the porcupine
  linearizability model used to check KV correctness.

## Conventions

- Each lab package gates verbose logging behind a `const Debug = false` plus a
  `DPrintf`. `raft1/util.go`'s `DPrintf` appends to a `debug.log` file (not stdout)
  when enabled.
- RPC argument/reply structs and anything stored in Raft (e.g. `rsm.Op`) must have
  **exported (capitalized) fields**, or labgob will silently drop them and tests
  will fail mysteriously.
- `notes/` and `paper/` contain the course papers (MapReduce, Raft, VM-FT) and
  reading notes — useful background, not code.
