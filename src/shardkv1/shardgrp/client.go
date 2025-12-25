package shardgrp

import (
	"log"
	"math/rand"
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
	clientId int64
	seqNum   atomic.Int64
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
	// Generate unique client ID using time and random number
	ck.clientId = time.Now().UnixNano() + rand.Int63()
	ck.seqNum.Store(0)
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	seqNum := ck.seqNum.Add(1)
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	for {
		args := rpc.GetArgs{Key: key, ClientId: ck.clientId, SeqNum: seqNum}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			// keep trying if rpc fails
			leader_id = (leader_id + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply.Value, reply.Version, reply.Err
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	seqNum := ck.seqNum.Add(1)
	firstTry := true
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	for {
		args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.clientId, SeqNum: seqNum}
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.Put", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			firstTry = false
			leader_id = (leader_id + 1) % len(ck.servers)
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

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
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
		if reply.Err == rpc.ErrVersion {
			return nil, reply.Err
		}
		if reply.Err == rpc.OK {
			return reply.State, reply.Err
		}
		log.Fatalf("FreezeShard should not receive err message %v", reply.Err)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	for {
		args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
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
