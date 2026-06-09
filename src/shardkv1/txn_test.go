package shardkv

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

// distinctGroupKeys returns n keys whose shards resolve to n *distinct* groups
// under cfg, so a transaction over them is genuinely cross-shard.
func distinctGroupKeys(t *testing.T, cfg *shardcfg.ShardConfig, n int) []string {
	seen := make(map[tester.Tgid]bool)
	keys := make([]string, 0, n)
	for i := 0; len(keys) < n; i++ {
		if i > 1000000 {
			t.Fatalf("could not find %d keys on distinct groups", n)
		}
		k := fmt.Sprintf("key-%d", i)
		gid := cfg.Shards[shardcfg.Key2Shard(k)]
		if !seen[gid] {
			seen[gid] = true
			keys = append(keys, k)
		}
	}
	return keys
}

// keysAcrossGroups returns n keys that, together, span at least 2 groups (so
// the transfer workload exercises real cross-shard commits).
func keysAcrossGroups(t *testing.T, cfg *shardcfg.ShardConfig, prefix string, n int) []string {
	keys := make([]string, 0, n)
	groups := make(map[tester.Tgid]bool)
	for i := 0; len(keys) < n; i++ {
		k := fmt.Sprintf("%s-%d", prefix, i)
		keys = append(keys, k)
		groups[cfg.Shards[shardcfg.Key2Shard(k)]] = true
	}
	if len(groups) < 2 {
		t.Fatalf("keys %v do not span >=2 groups", keys)
	}
	return keys
}

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

// setup brings up >=2 shard groups and returns the controller's config.
func setupGroups(ts *Test, ngroups int) *shardcfg.ShardConfig {
	ts.setupKVService()
	sck := ts.ShardCtrler()
	if ngroups > 1 {
		grps := ts.groups(ngroups - 1) // Gid1 is already up
		if ok := ts.joinGroups(sck, grps); !ok {
			ts.t.Fatalf("joinGroups failed")
		}
	}
	return sck.Query()
}

// TestTxnCrossShardBasic5D: an atomic batch of Puts on keys in different groups
// becomes visible all-at-once with correct versions and ordered Results.
func TestTxnCrossShardBasic5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard basic ...", true)
	defer ts.Cleanup()
	cfg := setupGroups(ts, 2)

	ck, _ := ts.MakeTxnClerk()
	keys := distinctGroupKeys(t, cfg, 2)

	committed, results, err := ck.Begin().
		Do(PutOp(keys[0], "v0"), PutOp(keys[1], "v1")).
		Commit()
	if err != rpc.OK {
		t.Fatalf("Commit err %v", err)
	}
	if !committed {
		t.Fatalf("expected committed=true")
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %+v", len(results), results)
	}
	for i, r := range results {
		if r.Version != 1 {
			t.Fatalf("Put result[%d] version = %d, want 1", i, r.Version)
		}
	}
	if v, ver, e := ck.Get(keys[0]); v != "v0" || ver != 1 || e != rpc.OK {
		t.Fatalf("Get(%s) = (%q,%d,%v), want (\"v0\",1,OK)", keys[0], v, ver, e)
	}
	if v, ver, e := ck.Get(keys[1]); v != "v1" || ver != 1 || e != rpc.OK {
		t.Fatalf("Get(%s) = (%q,%d,%v), want (\"v1\",1,OK)", keys[1], v, ver, e)
	}
}

// TestTxnPreconditionAbort5D: a failed precondition aborts the whole batch;
// no key is mutated.
func TestTxnPreconditionAbort5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): precondition abort ...", true)
	defer ts.Cleanup()
	cfg := setupGroups(ts, 2)

	ck, _ := ts.MakeTxnClerk()
	keys := distinctGroupKeys(t, cfg, 2)

	if err := ck.Put(keys[0], "A", 0); err != rpc.OK {
		t.Fatalf("seed Put(%s) err %v", keys[0], err)
	}

	// Precondition is false (key is at version 1, not 99): abort.
	committed, _, err := ck.Begin().
		If(VersionEq(keys[0], 99)).
		Do(PutOp(keys[0], "B"), PutOp(keys[1], "C")).
		Commit()
	if err != rpc.OK {
		t.Fatalf("Commit err %v", err)
	}
	if committed {
		t.Fatalf("expected committed=false on failed precondition")
	}
	if v, ver, _ := ck.Get(keys[0]); v != "A" || ver != 1 {
		t.Fatalf("%s mutated by aborted txn: (%q,%d), want (\"A\",1)", keys[0], v, ver)
	}
	if _, _, e := ck.Get(keys[1]); e != rpc.ErrNoKey {
		t.Fatalf("%s should not exist after abort, err=%v", keys[1], e)
	}
}

// transferLoad runs M concurrent clients that atomically move one unit between
// two random cross-shard keys (CAS on both versions), then checks the sum
// invariant — proving cross-group all-or-nothing.
func transferLoad(t *testing.T, ts *Test, dur time.Duration) {
	const (
		NKEYS = 6
		INIT  = 100
		NCLI  = 5
	)
	seed, _ := ts.MakeTxnClerk()
	cfg := ts.ShardCtrler().Query()
	keys := keysAcrossGroups(t, cfg, "bal", NKEYS)
	for _, k := range keys {
		if err := seed.Put(k, strconv.Itoa(INIT), 0); err != rpc.OK {
			t.Fatalf("seed Put(%s) err %v", k, err)
		}
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for c := 0; c < NCLI; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ck, _ := ts.MakeTxnClerk()
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
				ck.Begin().
					If(VersionEq(keys[i], veri), VersionEq(keys[j], verj)).
					Do(PutOp(keys[i], strconv.Itoa(vi-1)),
						PutOp(keys[j], strconv.Itoa(vj+1))).
					Commit()
			}
		}(c)
	}
	time.Sleep(dur)
	close(done)
	wg.Wait()

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

// TestTxnAtomicTransfer5D: concurrent cross-shard CAS transfers preserve the
// total balance (all-or-nothing across groups).
func TestTxnAtomicTransfer5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): atomic cross-shard transfer ...", true)
	defer ts.Cleanup()
	setupGroups(ts, 3)
	transferLoad(t, ts, 3*time.Second)
}

// TestTxnIsolationCounter5D: many clients increment a shared counter via a
// read-then-CAS txn; the final value equals the number of committed
// increments (serializable under 2PL).
func TestTxnIsolationCounter5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): isolated counter ...", true)
	defer ts.Cleanup()
	setupGroups(ts, 2)

	seed, _ := ts.MakeTxnClerk()
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
			ck, _ := ts.MakeTxnClerk()
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
				committed, _, err := ck.Begin().
					If(VersionEq("counter", ver)).
					Do(PutOp("counter", strconv.Itoa(v+1))).
					Commit()
				if err == rpc.OK && committed {
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
		t.Fatalf("counter = %d, but %d increments committed (not serializable)", final, total)
	}
}

// TestTxnWaitDieProgress5D: high contention on a few cross-group keys; every
// client must eventually finish its quota of commits (no deadlock/starvation).
func TestTxnWaitDieProgress5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): wait-die progress ...", true)
	defer ts.Cleanup()
	cfg := setupGroups(ts, 2)

	keys := distinctGroupKeys(t, cfg, 2)
	seed, _ := ts.MakeTxnClerk()
	for _, k := range keys {
		if err := seed.Put(k, "0", 0); err != rpc.OK {
			t.Fatalf("seed Put(%s) err %v", k, err)
		}
	}

	const (
		NCLI    = 6
		PERCLI  = 5
		TIMEOUT = 20 * time.Second
	)
	var wg sync.WaitGroup
	finished := make([]int, NCLI)
	for c := 0; c < NCLI; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ck, _ := ts.MakeTxnClerk()
			for finished[c] < PERCLI {
				// No precondition: contention is purely over the locks, so a
				// commit fails only on a wait-die DIE (retried internally) and
				// must eventually succeed.
				committed, _, err := ck.Begin().
					Do(GetOp(keys[0]), GetOp(keys[1])).
					Commit()
				if err == rpc.OK && committed {
					finished[c]++
				}
			}
		}(c)
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(TIMEOUT):
		t.Fatalf("wait-die did not make progress: finished=%v", finished)
	}
	for c := 0; c < NCLI; c++ {
		if finished[c] != PERCLI {
			t.Fatalf("client %d finished %d/%d (starvation?)", c, finished[c], PERCLI)
		}
	}
}

// TestTxnCoordinatorFailover5D: crash (and restart) the coordinator group's
// leader while cross-shard transfers are in flight. Because the coordinator
// logs its intent + decision in its own rsm, a new leader resumes driving 2PC;
// the txns still complete and the sum invariant holds.
func TestTxnCoordinatorFailover5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): coordinator failover ...", true)
	defer ts.Cleanup()
	cfg := setupGroups(ts, 2)

	// Kill the leader of the smallest gid: it is the chosen coordinator for
	// every txn that spans both groups, so this disrupts coordinators mid-2PC.
	coordGid := tester.Tgid(0)
	for gid := range cfg.Groups {
		if coordGid == 0 || gid < coordGid {
			coordGid = gid
		}
	}

	const (
		NKEYS = 6
		INIT  = 100
		NCLI  = 4
	)
	seed, _ := ts.MakeTxnClerk()
	keys := keysAcrossGroups(t, cfg, "fo", NKEYS)
	for _, k := range keys {
		if err := seed.Put(k, strconv.Itoa(INIT), 0); err != rpc.OK {
			t.Fatalf("seed Put(%s) err %v", k, err)
		}
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for c := 0; c < NCLI; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ck, _ := ts.MakeTxnClerk()
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
				ck.Begin().
					If(VersionEq(keys[i], veri), VersionEq(keys[j], verj)).
					Do(PutOp(keys[i], strconv.Itoa(vi-1)),
						PutOp(keys[j], strconv.Itoa(vj+1))).
					Commit()
			}
		}(c)
	}

	// A couple of discrete leader crashes mid-flight, each with enough time to
	// re-elect and drain in-flight 2PC before the next disruption.
	for i := 0; i < 2; i++ {
		time.Sleep(1 * time.Second)
		if ok, l := rsm.Leader(ts.Config, coordGid); ok {
			ts.Group(coordGid).ShutdownServer(l)
			time.Sleep(1500 * time.Millisecond) // re-elect + resume driving
			ts.Group(coordGid).StartServer(l)
			// StartServer adds the server back to the network but does NOT
			// reconnect it; without this the restarted server stays isolated
			// and a later kill would drop the group below quorum.
			ts.Group(coordGid).ConnectAll()
		}
	}
	time.Sleep(1 * time.Second)

	close(done)
	wg.Wait()

	sum := 0
	for _, k := range keys {
		v, _, ok := readInt(seed, k)
		if !ok {
			t.Fatalf("final read of %s failed", k)
		}
		sum += v
	}
	if sum != NKEYS*INIT {
		t.Fatalf("sum invariant broken after failover: got %d want %d", sum, NKEYS*INIT)
	}
}

// TestTxnUnreliable5D: the transfer workload over an unreliable network, to
// exercise dedup-on-resend and the ErrLocked / wait-die retry paths.
func TestTxnUnreliable5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard transfer (unreliable) ...", false)
	defer ts.Cleanup()
	setupGroups(ts, 3)
	transferLoad(t, ts, 5*time.Second)
}
