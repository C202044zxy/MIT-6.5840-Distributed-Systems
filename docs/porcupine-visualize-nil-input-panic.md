# Bug: porcupine `Visualize` panics on nil input when a history is `Illegal`

Status: **open** (pre-existing, deferred — to be fixed later)
Discovered: 2026-06-11, while adding the kvraft lease-read feature.

## Symptom

Running the porcupine-checked KV suites intermittently **panics** instead of
failing with a clean `history is not linearizable` message:

```
panic: interface conversion: interface {} is nil, not models.KvInput [recovered]
	panic: interface conversion: interface {} is nil, not models.KvInput

6.5840/models1.init.func4({0x0, 0x0}, ...)            # DescribeOperation
	src/models1/kv.go:69
github.com/anishathalye/porcupine.computeVisualizationData(...)
	porcupine@v1.0.0/visualization.go:115
github.com/anishathalye/porcupine.Visualize(...)
	porcupine@v1.0.0/visualization.go:179
6.5840/kvtest1.checkPorcupine(...)
	src/kvtest1/porcupine.go:105
```

Reproduced in (at least):

- `TestBasic4B` — `go test -race ./kvraft1/` (≈1 in 5–10 runs)
- `TestOneConcurrentClerkReliable5A` — `go test -race ./shardkv1/`

Both abort the **whole test binary** (a panic, not a `t.Fatal`), so any later
tests in the package never run.

## Root cause (two layers)

1. **Trigger — a genuine `Illegal` result.** `checkPorcupine`
   (`src/kvtest1/porcupine.go:86`) only enters the visualization path when
   `porcupine.CheckOperationsVerbose` returns `porcupine.Illegal`, i.e. the
   recorded history is **actually non-linearizable**. So there is a rare, real
   linearizability violation in the base kvraft/shardkv implementations that
   this masks. (A timeout would return `Unknown` and is handled separately, so
   this is not a timeout.)

2. **Crash — porcupine's visualizer passes a nil input.** On an illegal
   history, `porcupine@v1.0.0`'s `computeVisualizationData`
   (`visualization.go:115`) calls the model's `DescribeOperation` with a `nil`
   input for some synthesized/partial-linearization entry. `models1/kv.go:69`
   does an unchecked `input.(KvInput)` type assertion and panics. The intended
   clean failure (`t.Fatal("history is not linearizable")` at
   `porcupine.go:112`) never runs because `Visualize` panics first.

## Not caused by the lease feature

Confirmed pre-existing:

- Reproduces at `HEAD` with **all lease changes stashed** (`TestBasic4B`).
- Reproduces in `shardkv1`, which **never calls `EnableLeaseRead`** — every
  new lease field in `raft1` is gated by `leaseEnabled`, which stays `false`
  there, so the shardkv code path is byte-for-byte identical to `HEAD`.

So this is independent of the lease work and lives in the test/model
infrastructure plus a latent correctness bug in the base KV layer.

## Why it was deferred

`models1/` and `kvtest1/` are part of the course-provided tester tree that
`CLAUDE.md` says not to modify when solving labs, and the underlying `Illegal`
needs its own investigation. Out of scope for the lease-read change.

## Fix ideas (for later)

Two independent pieces, fix both:

1. **Stop the crash (test infra).** Make `models1/kv.go`'s `DescribeOperation`
   (and the other `KvModel` closures that assert `input.(KvInput)` /
   `output.(KvOutput)`) nil-tolerant, e.g. return `"<unknown>"` /
   `comma-ok` assertions, so an `Illegal` history fails cleanly via
   `t.Fatal` and writes a usable visualization instead of panicking. Consider
   bumping `porcupine` past v1.0.0 if a newer release no longer feeds nil
   inputs to `DescribeOperation`.

2. **Find the real violation (base KV).** Once the crash is gone, capture the
   visualization HTML (`VIS_ENABLE`/`VIS_FILE` env vars in
   `kvtest1/porcupine.go`) on a failing seed and trace which Get/Put pair is
   non-linearizable. Start from the duplicate-detection / session-cache paths
   in `kvraft1/server.go` and the shard-migration handoff in `shardkv1`.

## Repro

```bash
cd src
go test -race -count=20 -run TestBasic4B ./kvraft1/          # panics within a few runs
go test -race -run TestOneConcurrentClerkReliable5A ./shardkv1/
```
