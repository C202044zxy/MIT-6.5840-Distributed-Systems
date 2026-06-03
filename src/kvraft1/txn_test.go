package kvraft

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
)

// readInt reads key as a base-10 integer plus its version. It is
// goroutine-safe (no t.Fatalf) so it can be called from worker goroutines;
// callers decide how to react to !ok.
func readInt(ck *Clerk, key string) (int, rpc.Tversion, bool) {
	val, ver, err := ck.Get(key)
	if err != rpc.OK {
		return 0, 0, false
	}
	n, e := strconv.Atoi(val)
	if e != nil {
		return 0, 0, false
	}
	return n, ver, true
}

// TestTxnBasic: a transaction whose conditions hold runs its ThenOps
// atomically; both writes become visible and Results is populated.
func TestTxnBasic(t *testing.T) {
	ts := MakeTest(t, "TXN", 1, 3, true, false, false, -1, false)
	defer ts.Cleanup()
	ck := ts.MakeTxnClerk()

	// Seed a=1 (version 1).
	if err := ck.Put("a", "1", 0); err != rpc.OK {
		t.Fatalf("seed Put(a) err %v", err)
	}
	_, v, err := ck.Get("a")
	if err != rpc.OK {
		t.Fatalf("Get(a) err %v", err)
	}

	rep, e := ck.Txn().
		If(VersionEq("a", v)).
		Then(PutOp("a", "2"), PutOp("b", "x")).
		Else(GetOp("a")).
		Commit()
	if e != rpc.OK {
		t.Fatalf("Commit err %v", e)
	}
	if !rep.Succeeded {
		t.Fatalf("expected Succeeded=true (ThenOps), got false")
	}
	if len(rep.Results) != 2 {
		t.Fatalf("expected 2 results, got %d: %+v", len(rep.Results), rep.Results)
	}
	// Put a (existed at v=1) -> new version 2; Put b (new key) -> version 1.
	if rep.Results[0].Version != v+1 {
		t.Fatalf("Put a new version = %d, want %d", rep.Results[0].Version, v+1)
	}
	if rep.Results[1].Version != 1 {
		t.Fatalf("Put b new version = %d, want 1", rep.Results[1].Version)
	}

	// Both writes are visible.
	if val, ver, _ := ck.Get("a"); val != "2" || ver != v+1 {
		t.Fatalf("Get(a) = (%q,%d), want (\"2\",%d)", val, ver, v+1)
	}
	if val, ver, _ := ck.Get("b"); val != "x" || ver != 1 {
		t.Fatalf("Get(b) = (%q,%d), want (\"x\",1)", val, ver)
	}
}

// TestTxnElseBranch: a failed condition runs ElseOps, never ThenOps, and
// leaves the store unmutated by the Then side.
func TestTxnElseBranch(t *testing.T) {
	ts := MakeTest(t, "TXN", 1, 3, true, false, false, -1, false)
	defer ts.Cleanup()
	ck := ts.MakeTxnClerk()

	if err := ck.Put("a", "1", 0); err != rpc.OK {
		t.Fatalf("seed Put(a) err %v", err)
	}

	// Wrong version (key is at version 1, condition asks for 5).
	rep, e := ck.Txn().
		If(VersionEq("a", 5)).
		Then(PutOp("a", "99")).
		Else(GetOp("a")).
		Commit()
	if e != rpc.OK {
		t.Fatalf("Commit err %v", e)
	}
	if rep.Succeeded {
		t.Fatalf("expected Succeeded=false (ElseOps)")
	}
	if len(rep.Results) != 1 {
		t.Fatalf("expected 1 result from Else, got %d", len(rep.Results))
	}
	got := rep.Results[0]
	if !got.Exists || got.Value != "1" || got.Version != 1 || got.Err != rpc.OK {
		t.Fatalf("Else GetOp(a) result = %+v, want value 1 version 1 exists", got)
	}
	// ThenOps must not have run.
	if val, ver, _ := ck.Get("a"); val != "1" || ver != 1 {
		t.Fatalf("a mutated by Then branch: (%q,%d), want (\"1\",1)", val, ver)
	}
}

// TestTxnSubsumesVersionedPut: a version-conditioned Txn reproduces the
// semantics of Clerk.Put — create-if-absent, succeed on matching version,
// fail (and fall to Else) on a stale version.
func TestTxnSubsumesVersionedPut(t *testing.T) {
	ts := MakeTest(t, "TXN", 1, 3, true, false, false, -1, false)
	defer ts.Cleanup()
	ck := ts.MakeTxnClerk()

	// Create an absent key: version 0 condition holds for a missing key.
	rep, e := ck.Txn().If(VersionEq("k", 0)).Then(PutOp("k", "v0")).Commit()
	if e != rpc.OK || !rep.Succeeded {
		t.Fatalf("create-if-absent: err=%v succeeded=%v", e, rep.Succeeded)
	}
	if val, ver, _ := ck.Get("k"); val != "v0" || ver != 1 {
		t.Fatalf("after create: (%q,%d), want (\"v0\",1)", val, ver)
	}

	// Matching version succeeds and bumps the version.
	rep, e = ck.Txn().If(VersionEq("k", 1)).Then(PutOp("k", "v1")).Commit()
	if e != rpc.OK || !rep.Succeeded {
		t.Fatalf("matching-version put: err=%v succeeded=%v", e, rep.Succeeded)
	}
	if val, ver, _ := ck.Get("k"); val != "v1" || ver != 2 {
		t.Fatalf("after matching put: (%q,%d), want (\"v1\",2)", val, ver)
	}

	// Stale version fails like ErrVersion would; Else observes the unchanged key.
	rep, e = ck.Txn().
		If(VersionEq("k", 1)). // stale: key is now at version 2
		Then(PutOp("k", "bad")).
		Else(GetOp("k")).
		Commit()
	if e != rpc.OK {
		t.Fatalf("stale put: err=%v", e)
	}
	if rep.Succeeded {
		t.Fatalf("stale version should not succeed")
	}
	if got := rep.Results[0]; got.Value != "v1" || got.Version != 2 {
		t.Fatalf("Else GetOp = %+v, want (\"v1\",2)", got)
	}
	if val, ver, _ := ck.Get("k"); val != "v1" || ver != 2 {
		t.Fatalf("stale put mutated key: (%q,%d), want (\"v1\",2)", val, ver)
	}
}

// transferLoad runs many clients that atomically move one unit between two
// random keys via a two-key CAS transaction, and verifies the sum invariant
// afterwards. Shared by the reliable and unreliable transfer tests.
func transferLoad(t *testing.T, ts *Test) {
	const (
		NKEYS = 5
		INIT  = 100
		NCLI  = 5
		DUR   = 3 * time.Second
	)
	keys := make([]string, NKEYS)
	seed := ts.MakeTxnClerk()
	for i := range keys {
		keys[i] = "bal" + strconv.Itoa(i)
		if err := seed.Put(keys[i], strconv.Itoa(INIT), 0); err != rpc.OK {
			t.Fatalf("seed Put(%s) err %v", keys[i], err)
		}
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for c := 0; c < NCLI; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ck := ts.MakeTxnClerk()
			r := rand.New(rand.NewSource(int64(c) + 1))
			for {
				select {
				case <-done:
					return
				default:
				}
				i, j := r.Intn(NKEYS), r.Intn(NKEYS)
				if i == j {
					continue
				}
				vi, veri, oki := readInt(ck, keys[i])
				vj, verj, okj := readInt(ck, keys[j])
				if !oki || !okj || vi <= 0 {
					continue
				}
				// Atomic transfer: both balances change in one Raft entry, or
				// neither does (if a concurrent writer bumped a version).
				ck.Txn().
					If(VersionEq(keys[i], veri), VersionEq(keys[j], verj)).
					Then(PutOp(keys[i], strconv.Itoa(vi-1)),
						PutOp(keys[j], strconv.Itoa(vj+1))).
					Commit()
			}
		}(c)
	}
	time.Sleep(DUR)
	close(done)
	wg.Wait()

	// Sum invariant: no unit was created or destroyed -> no partial txn applied.
	sum := 0
	for _, k := range keys {
		v, _, ok := readInt(seed, k)
		if !ok {
			t.Fatalf("final read of %s failed", k)
		}
		sum += v
	}
	if sum != NKEYS*INIT {
		t.Fatalf("sum invariant broken: got %d want %d", sum, NKEYS*INIT)
	}
}

// TestTxnAtomicTransfer: concurrent two-key CAS transfers preserve the total
// balance, proving multi-key atomicity (all-or-nothing application).
func TestTxnAtomicTransfer(t *testing.T) {
	ts := MakeTest(t, "TXN", 5, 3, true, false, false, -1, false)
	defer ts.Cleanup()
	transferLoad(t, ts)
}

// TestTxnConcurrentCAS: many clients increment a shared counter via a
// read-then-CAS-txn loop; the final value must equal the number of committed
// increments (a linearizable compare-and-set).
func TestTxnConcurrentCAS(t *testing.T) {
	ts := MakeTest(t, "TXN", 5, 3, true, false, false, -1, false)
	defer ts.Cleanup()

	seed := ts.MakeTxnClerk()
	if err := seed.Put("counter", "0", 0); err != rpc.OK {
		t.Fatalf("seed counter err %v", err)
	}

	const (
		NCLI = 5
		DUR  = 3 * time.Second
	)
	done := make(chan struct{})
	counts := make([]int, NCLI)
	var wg sync.WaitGroup
	for c := 0; c < NCLI; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ck := ts.MakeTxnClerk()
			for {
				select {
				case <-done:
					return
				default:
				}
				v, ver, ok := readInt(ck, "counter")
				if !ok {
					continue
				}
				rep, err := ck.Txn().
					If(VersionEq("counter", ver)).
					Then(PutOp("counter", strconv.Itoa(v+1))).
					Commit()
				if err == rpc.OK && rep.Succeeded {
					counts[c]++
				}
			}
		}(c)
	}
	time.Sleep(DUR)
	close(done)
	wg.Wait()

	total := 0
	for _, n := range counts {
		total += n
	}
	final, _, ok := readInt(seed, "counter")
	if !ok {
		t.Fatalf("final read of counter failed")
	}
	if total == 0 {
		t.Fatalf("no increments committed")
	}
	if final != total {
		t.Fatalf("counter = %d, but %d increments committed (CAS not linearizable)", final, total)
	}
}

// TestTxnUnreliable: run the transfer workload over an unreliable network so
// transactions are resent; the dedup cache must make each commit exactly-once,
// preserving the sum invariant.
func TestTxnUnreliable(t *testing.T) {
	ts := MakeTest(t, "TXN", 5, 3, false, false, false, -1, false)
	defer ts.Cleanup()
	transferLoad(t, ts)
}
