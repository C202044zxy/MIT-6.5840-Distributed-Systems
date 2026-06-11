package kvraft

import (
	"testing"
	"time"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// leaderAmong returns the index of a server in `among` that currently believes
// it is the Raft leader, or (-1,false) if none does. It is needed by the
// holdoff test because a deposed leader stuck in the minority keeps reporting
// isLeader==true forever (its election ticker only fires for Follower/
// Candidate), so the global rsm.Leader helper would keep pointing at it. Here
// we restrict the search to the partition we care about.
func leaderAmong(ts *Test, among []int) (int, bool) {
	inSet := func(i int) bool {
		for _, s := range among {
			if s == i {
				return true
			}
		}
		return false
	}
	for i, ss := range ts.Group(Gid).Services() {
		if !inSet(i) {
			continue
		}
		for _, s := range ss {
			if r, ok := s.(raftapi.Raft); ok {
				if _, isLeader := r.GetState(); isLeader {
					return i, true
				}
			}
		}
	}
	return -1, false
}

// TestLeaseGetRPCCount verifies that lease-protected Gets answer from the
// leader's local state machine instead of replicating a log entry, so a burst
// of N sequential Gets costs far fewer than 2*N RPCs (without the optimization
// each Get adds a log entry => >=2 AppendEntries on top of the client RPC).
//
// Reliable network + maxraftstate = -1 so no InstallSnapshot broadcasts pollute
// the RPC count.
func TestLeaseGetRPCCount(t *testing.T) {
	const N = 200
	ts := MakeTest(t, "LEASE", 1, 3, true, false, false, -1, false)
	tester.AnnotateTest("TestLeaseGetRPCCount", ts.nservers)
	defer ts.Cleanup()
	ck := ts.MakeClerk()

	// Warmup: a Put elects a leader and, crucially, commits a current-term
	// entry, opening Gate 1 of the fast path.
	ver := ts.PutAtLeastOnce(ck, "k", "v", rpc.Tversion(0), 0)
	ts.CheckGet(ck, "k", "v", ver)

	// Let several heartbeat rounds (150ms each) establish the lease and let the
	// election/warmup traffic drain before we start counting.
	time.Sleep(600 * time.Millisecond)

	n0 := ts.Config.RpcTotal()
	for i := 0; i < N; i++ {
		val, gotVer, err := ck.Get("k")
		if err != rpc.OK {
			t.Fatalf("Get #%d err %v", i, err)
		}
		if val != "v" || gotVer != ver {
			t.Fatalf("Get #%d returned (%v,%v), want (v,%v)", i, val, gotVer, ver)
		}
	}
	delta := ts.Config.RpcTotal() - n0

	// Optimized: ~1 client RPC per Get plus background heartbeat/replicate
	// traffic (~33 RPC/s) => well under 2*N. Unoptimized: each Get adds a log
	// entry => >= 2 extra AppendEntries each => delta >= ~600.
	if delta >= 2*N {
		t.Fatalf("lease Gets used too many RPCs: delta=%d for %d Gets (want < %d) — fast path not engaged?", delta, N, 2*N)
	}
	t.Logf("lease Gets: delta=%d RPCs for %d Gets", delta, N)
}

// TestLeaseNewLeaderHoldoff verifies the new-leader holdoff: after a leader is
// partitioned away and a new leader is elected in the majority, the new leader
// must not commit (and thus cannot answer the client's Get) until the old
// leader's lease has provably expired (~leaseDuration). Without the holdoff in
// commitLog the new leader would commit immediately and the Get would return in
// tens of ms.
func TestLeaseNewLeaderHoldoff(t *testing.T) {
	ts := MakeTest(t, "LEASE", 1, 5, true, false, true, -1, false)
	tester.AnnotateTest("TestLeaseNewLeaderHoldoff", ts.nservers)
	defer ts.Cleanup()
	ck := ts.MakeClerk()

	ver := ts.PutAtLeastOnce(ck, "k", "v", rpc.Tversion(0), 0)
	ts.CheckGet(ck, "k", "v", ver)

	// Let the leader establish its lease.
	time.Sleep(600 * time.Millisecond)

	ok, l := rsm.Leader(ts.Config, Gid)
	if !ok {
		t.Fatalf("no leader found before partition")
	}

	// Partition so the old leader lands in the 2-server minority p2; the
	// majority p1 (3 servers) will elect a fresh leader.
	p1, p2 := ts.Group(Gid).MakePartition(l)
	ts.Group(Gid).Partition(p1, p2)
	ckp1 := ts.MakeClerkTo(p1)

	// Poll (<=5s) for a leader among p1 only, recording when it first appears.
	var tElect time.Time
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, found := leaderAmong(ts, p1); found {
			tElect = time.Now()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no leader elected in majority partition within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Time one Get against the new leader, measured from the election instant.
	val, _, err := ckp1.Get("k")
	elapsed := time.Since(tElect)
	if err != rpc.OK {
		t.Fatalf("Get after re-election err %v", err)
	}
	if val != "v" {
		t.Fatalf("Get after re-election returned %v, want v", val)
	}

	// With the holdoff the Get cannot complete before T_e + leaseDuration, and
	// tElect <= T_e + ~10ms, so elapsed >= ~490ms. Threshold 400ms separates
	// this cleanly from the ~<=270ms a Get would take without the holdoff.
	if elapsed < 400*time.Millisecond {
		t.Fatalf("Get completed in %v after re-election; new-leader holdoff not enforced (want >= 400ms)", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("Get took %v after re-election; liveness problem (want < 3s)", elapsed)
	}
	t.Logf("new-leader holdoff: Get completed %v after re-election", elapsed)
}
