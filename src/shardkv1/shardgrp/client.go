package shardgrp

import (
	"log"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId atomic.Int32
}

func (ck *Clerk) LoadLeaderId() int {
	return int(ck.leaderId.Load())
}

func (ck *Clerk) StoreLeaderId(id int) {
	ck.leaderId.Store(int32(id))
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get/Put take the caller's clientId and seqNum so that the idempotency key is
// stable per logical client operation across retries and shard migration. The
// shardkv Clerk owns these identifiers; see shardkv1/client.go.
func (ck *Clerk) Get(key string, clientId int64, seqNum int64) (string, rpc.Tversion, rpc.Err) {
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	failCount := 0
	for {
		args := rpc.GetArgs{Key: key, ClientId: clientId, SeqNum: seqNum}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			failCount++
			leader_id = (leader_id + 1) % len(ck.servers)
			if failCount >= len(ck.servers) {
				// All servers unreachable; signal outer loop to re-query config
				failCount = 0
				return "", 0, rpc.ErrWrongGroup
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrLocked {
			// Key is locked by an in-flight cross-shard txn; retry (same seqNum).
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply.Value, reply.Version, reply.Err
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion, clientId int64, seqNum int64) rpc.Err {
	// You will have to modify this function.
	firstTry := true
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	failCount := 0
	for {
		args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: clientId, SeqNum: seqNum}
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.Put", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			firstTry = false
			failCount++
			leader_id = (leader_id + 1) % len(ck.servers)
			if failCount >= len(ck.servers) {
				// All servers unreachable; signal outer loop to re-query config.
				// Return ErrMaybe since the put may have been applied (firstTry=false).
				failCount = 0
				return rpc.ErrWrongGroup
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrLocked {
			// Key is locked by an in-flight cross-shard txn; the Put was not
			// applied (lock checked before write), so retry without flipping
			// firstTry (no ErrMaybe risk).
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrVersion {
			if firstTry {
				// first rpc. put not performed on server
				return rpc.ErrVersion
			}
			// resend rpc. put might be performed on server
			return rpc.ErrMaybe
		}
		return reply.Err
	}
}

// CoordTxn sends a cross-shard txn to this group acting as coordinator. It is a
// leader-discovery loop: the coordinator's RPC handler blocks until the txn
// reaches a final decision and returns the stored reply (Err=OK or ErrRetry).
// If every server is unreachable / not leader, returns Err=ErrWrongGroup so the
// client re-queries the config and resends (the coordinator dedups by Tid).
func (ck *Clerk) CoordTxn(args *shardrpc.CoordTxnArgs) shardrpc.CoordTxnReply {
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	failCount := 0
	for {
		a := *args
		reply := shardrpc.CoordTxnReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.CoordTxn", &a, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			failCount++
			leader_id = (leader_id + 1) % len(ck.servers)
			if failCount >= len(ck.servers) {
				return shardrpc.CoordTxnReply{Err: rpc.ErrWrongGroup}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply
	}
}

// Prepare runs Phase 1 of 2PC at this participant group. It is a one-shot
// leader-discovery call: returns the group's vote, or Err=ErrWrongGroup if every
// server is unreachable / not leader (the coordinator then aborts-and-retries).
func (ck *Clerk) Prepare(args *shardrpc.PrepareArgs) shardrpc.PrepareReply {
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	failCount := 0
	for {
		a := *args
		reply := shardrpc.PrepareReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.Prepare", &a, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			failCount++
			leader_id = (leader_id + 1) % len(ck.servers)
			if failCount >= len(ck.servers) {
				return shardrpc.PrepareReply{Err: rpc.ErrWrongGroup}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply
	}
}

// CommitTxn runs Phase 2 (commit) at a participant. It is a one-shot
// leader-discovery call (one pass over the servers): on success it returns the
// idempotent reply, else Err=ErrWrongGroup so the *coordinator's* driver can
// retry — but only while it is still the coordinator-group leader (a demoted
// driver must stop, or it spins forever after a leadership change).
func (ck *Clerk) CommitTxn(args *shardrpc.CommitArgs) shardrpc.CommitReply {
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	failCount := 0
	for {
		a := *args
		reply := shardrpc.CommitReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.CommitTxn", &a, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			failCount++
			leader_id = (leader_id + 1) % len(ck.servers)
			if failCount >= len(ck.servers) {
				return shardrpc.CommitReply{Err: rpc.ErrWrongGroup}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply
	}
}

// AbortTxn runs Phase 2 (abort) at a participant. One-shot like CommitTxn: the
// idempotent reply on success, else Err=ErrWrongGroup for the driver to retry.
func (ck *Clerk) AbortTxn(args *shardrpc.AbortArgs) shardrpc.AbortReply {
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	failCount := 0
	for {
		a := *args
		reply := shardrpc.AbortReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.AbortTxn", &a, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			failCount++
			leader_id = (leader_id + 1) % len(ck.servers)
			if failCount >= len(ck.servers) {
				return shardrpc.AbortReply{Err: rpc.ErrWrongGroup}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, []byte, rpc.Err) {
	// Your code here
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	for {
		args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
		reply := shardrpc.FreezeShardReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.FreezeShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			// keep trying if rpc fails
			leader_id = (leader_id + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrLocked {
			// A key in this shard is locked by an in-flight cross-shard txn;
			// wait it out so migration never races a prepared txn.
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrVersion {
			return nil, nil, reply.Err
		}
		if reply.Err == rpc.OK {
			return reply.State, reply.Sessions, reply.Err
		}
		log.Fatalf("FreezeShard should not receive err message %v", reply.Err)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, sessions []byte, num shardcfg.Tnum) rpc.Err {
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	for {
		args := shardrpc.InstallShardArgs{Shard: s, State: state, Sessions: sessions, Num: num}
		reply := shardrpc.InstallShardReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.InstallShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			// keep trying if rpc fails
			leader_id = (leader_id + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply.Err
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	for {
		args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
		reply := shardrpc.DeleteShardReply{}

		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.DeleteShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			// keep trying if rpc fails
			leader_id = (leader_id + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply.Err
	}
}
