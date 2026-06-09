package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"math/rand"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
	grps map[tester.Tgid]*shardgrp.Clerk
	// clientId is stable for this clerk and seqNum is allocated once per
	// logical Get/Put, so the dedup key survives RPC retries, group restarts,
	// and shard migration (the new owner inherits the dedup table).
	clientId int64
	seqNum   atomic.Int64
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:     clnt,
		sck:      sck,
		grps:     make(map[tester.Tgid]*shardgrp.Clerk),
		clientId: time.Now().UnixNano() + rand.Int63(),
	}
	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	seqNum := ck.seqNum.Add(1)
	for {
		shard := shardcfg.Key2Shard(key)
		cfg := ck.sck.Query()
		gid := cfg.Shards[shard]
		grp, ok := ck.grps[gid]
		if !ok {
			ck.grps[gid] = shardgrp.MakeClerk(ck.clnt, cfg.Groups[gid])
			grp = ck.grps[gid]
		}
		value, version, err := grp.Get(key, ck.clientId, seqNum)
		if err != rpc.ErrWrongGroup {
			return value, version, err
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	seqNum := ck.seqNum.Add(1)
	for {
		shard := shardcfg.Key2Shard(key)
		cfg := ck.sck.Query()
		gid := cfg.Shards[shard]
		grp, ok := ck.grps[gid]
		if !ok {
			ck.grps[gid] = shardgrp.MakeClerk(ck.clnt, cfg.Groups[gid])
			grp = ck.grps[gid]
		}
		err := grp.Put(key, value, version, ck.clientId, seqNum)
		if err != rpc.ErrWrongGroup {
			return err
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// --- Cross-shard transaction ---

// Compare/op constructors, mirroring kvraft1/client.go's style.
func VersionEq(key string, v rpc.Tversion) rpc.Compare {
	return rpc.Compare{
		Key:     key,
		Target:  rpc.CmpVersion,
		Op:      rpc.CmpEqual,
		Version: v,
	}
}

func ValueEq(key, val string) rpc.Compare {
	return rpc.Compare{
		Key:    key,
		Target: rpc.CmpValue,
		Op:     rpc.CmpEqual,
		Value:  val,
	}
}

func PutOp(key, val string) rpc.TxnOp {
	return rpc.TxnOp{
		Type:  rpc.OpPut,
		Key:   key,
		Value: val,
	}
}

func GetOp(key string) rpc.TxnOp {
	return rpc.TxnOp{
		Type: rpc.OpGet,
		Key:  key,
	}
}

// ShardTxn is a flat atomic batch of ops with optional preconditions,
// spanning any shards, committed all-or-nothing via 2PC + 2PL.
type ShardTxn struct {
	ck    *Clerk
	conds []rpc.Compare
	ops   []rpc.TxnOp
}

// Begin starts building a cross-shard transaction.
func (ck *Clerk) Begin() *ShardTxn {
	return &ShardTxn{ck: ck}
}

// If adds preconditions; all must hold or the txn aborts.
func (t *ShardTxn) If(c ...rpc.Compare) *ShardTxn {
	t.conds = c
	return t
}

// Do adds the flat op list (order preserved in Results).
func (t *ShardTxn) Do(op ...rpc.TxnOp) *ShardTxn {
	t.ops = op
	return t
}

// Commit runs the transaction, returning whether it committed, the
// per-op results (ordered to match Do), and an error.
//
// Ts is allocated once and kept stable across retries so the txn ages under
// wait-die (an aborted-by-DIE txn eventually becomes oldest and never dies
// again). Tid is reallocated only on a genuine wait-die ABORT (ErrRetry): the
// coordinator dedups by Tid, so transient failures must reuse the same Tid to
// stay at-most-once, while a real retry needs a fresh Tid (the old one's stored
// reply is a permanent ErrRetry).
func (t *ShardTxn) Commit() (bool, []rpc.OpResult, rpc.Err) {
	ck := t.ck
	ts := time.Now().UnixNano()
	seqNum := ck.seqNum.Add(1)
	tid := ck.clientId + seqNum

	for {
		cfg := ck.sck.Query()

		// Bucket conds + ops by owning group, carrying each op's original
		// index for result reassembly at the coordinator.
		partsByGid := make(map[tester.Tgid]*shardrpc.TxnPart)
		part := func(gid tester.Tgid) *shardrpc.TxnPart {
			p, ok := partsByGid[gid]
			if !ok {
				p = &shardrpc.TxnPart{Gid: gid, Servers: cfg.Groups[gid]}
				partsByGid[gid] = p
			}
			return p
		}
		for _, c := range t.conds {
			gid := cfg.Shards[shardcfg.Key2Shard(c.Key)]
			p := part(gid)
			p.Conds = append(p.Conds, c)
		}
		for i, op := range t.ops {
			gid := cfg.Shards[shardcfg.Key2Shard(op.Key)]
			p := part(gid)
			p.Ops = append(p.Ops, op)
			p.Idx = append(p.Idx, i)
		}

		// Deterministic coordinator: the smallest participant gid.
		var coordGid tester.Tgid
		parts := make([]shardrpc.TxnPart, 0, len(partsByGid))
		for gid, p := range partsByGid {
			if coordGid == 0 || gid < coordGid {
				coordGid = gid
			}
			parts = append(parts, *p)
		}

		args := shardrpc.CoordTxnArgs{
			Tid:      tid,
			Ts:       ts,
			ClientId: ck.clientId,
			SeqNum:   seqNum,
			Parts:    parts,
		}

		grp, ok := ck.grps[coordGid]
		if !ok {
			ck.grps[coordGid] = shardgrp.MakeClerk(ck.clnt, cfg.Groups[coordGid])
			grp = ck.grps[coordGid]
		}
		reply := grp.CoordTxn(&args)

		switch reply.Err {
		case rpc.ErrWrongGroup:
			// Coordinator unreachable / leader churn / stale config. Same Tid
			// (at-most-once) + same Ts; re-query and resend.
			time.Sleep(10 * time.Millisecond)
		case rpc.ErrRetry:
			// Genuine wait-die ABORT. Fresh Tid (the old one's reply is a
			// permanent ErrRetry), same Ts so the txn ages.
			seqNum = ck.seqNum.Add(1)
			tid = ck.clientId + seqNum
			time.Sleep(time.Duration(rand.Intn(20)+5) * time.Millisecond)
		default:
			return reply.Committed, reply.Results, reply.Err
		}
	}
}
