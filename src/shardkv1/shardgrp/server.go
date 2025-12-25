package shardgrp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type ClientSession struct {
	LastSeqNum int64
	LastReply  any
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your definitions here.
	mu           sync.Mutex
	mp           map[string]ValueVersion
	sessions     map[int64]*ClientSession // Track last request per client
	shardNums    map[shardcfg.Tshid]shardcfg.Tnum
	frozenShards map[shardcfg.Tshid]bool
	ownedShards  map[shardcfg.Tshid]bool
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) getImpl(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if this is a duplicate request
	if session, ok := kv.sessions[args.ClientId]; ok {
		if args.SeqNum == session.LastSeqNum {
			// Duplicate request - return cached result
			*reply = session.LastReply.(rpc.GetReply)
			return
		} else if args.SeqNum < session.LastSeqNum {
			// Old request - should not happen, but handle gracefully
			pair, exists := kv.mp[args.Key]
			if exists {
				reply.Value = pair.Value
				reply.Version = pair.Version
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
			return
		}
	}

	if v, ok := kv.ownedShards[shardcfg.Key2Shard(args.Key)]; !ok || !v {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	pair, ok := kv.mp[args.Key]
	if ok {
		reply.Value = pair.Value
		reply.Version = pair.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}

	// Update session with new result
	kv.sessions[args.ClientId] = &ClientSession{
		LastSeqNum: args.SeqNum,
		LastReply:  *reply,
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) putImpl(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if this is a duplicate request
	if session, ok := kv.sessions[args.ClientId]; ok {
		if args.SeqNum == session.LastSeqNum {
			// Duplicate request - return cached result
			*reply = session.LastReply.(rpc.PutReply)
			return
		} else if args.SeqNum < session.LastSeqNum {
			// Old request - should not happen, but return OK to be safe
			reply.Err = rpc.OK
			return
		}
	}

	if v, ok := kv.ownedShards[shardcfg.Key2Shard(args.Key)]; !ok || !v {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	if v, ok := kv.frozenShards[shardcfg.Key2Shard(args.Key)]; ok && v {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	pair, ok := kv.mp[args.Key]
	if ok {
		// the key exist
		if pair.Version == args.Version {
			kv.mp[args.Key] = ValueVersion{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		// the key not exist
		if args.Version == 0 {
			// create a new key
			kv.mp[args.Key] = ValueVersion{args.Value, 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}

	// Update session with new result
	kv.sessions[args.ClientId] = &ClientSession{
		LastSeqNum: args.SeqNum,
		LastReply:  *reply,
	}
	// fmt.Printf("me = %v, args = %v, map = %v\n", kv.me, args, kv.mp)
}

func (kv *KVServer) freezeShardImpl(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.shardNums[args.Shard]; ok && args.Num < v {
		reply.Num = v
		reply.Err = rpc.ErrVersion
		return
	}
	kv.shardNums[args.Shard] = args.Num
	kv.frozenShards[args.Shard] = true
	shardMap := make(map[string]ValueVersion)
	for k, v := range kv.mp {
		if shardcfg.Key2Shard(k) == args.Shard {
			shardMap[k] = v
		}
	}
	b, err := json.Marshal(shardMap)
	if err != nil {
		log.Fatalf("Marshal err %v", err)
	}
	reply.Err = rpc.OK
	reply.Num = args.Num
	reply.State = b
}

func (kv *KVServer) installShardImpl(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.shardNums[args.Shard]; ok && args.Num < v {
		reply.Err = rpc.ErrVersion
		return
	}
	kv.shardNums[args.Shard] = args.Num
	kv.ownedShards[args.Shard] = true
	var shardMap map[string]ValueVersion
	if err := json.Unmarshal(args.State, &shardMap); err != nil {
		log.Fatalf("Unmarshal err %v", err)
	}
	for k, v := range shardMap {
		kv.mp[k] = v
	}
	reply.Err = rpc.OK
}

func (kv *KVServer) deleteShardImpl(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.shardNums[args.Shard]; ok && args.Num < v {
		reply.Err = rpc.ErrVersion
		return
	}
	kv.shardNums[args.Shard] = args.Num
	delete(kv.ownedShards, args.Shard)
	keys := make([]string, 0)
	for k := range kv.mp {
		if shardcfg.Key2Shard(k) == args.Shard {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		delete(kv.mp, k)
	}
	reply.Err = rpc.OK
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(raw_req any) any {
	// detect req type and dispatch to req implementations
	switch req := raw_req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		kv.getImpl(&req, &reply)
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}
		kv.putImpl(&req, &reply)
		return reply
	case shardrpc.FreezeShardArgs:
		reply := shardrpc.FreezeShardReply{}
		kv.freezeShardImpl(&req, &reply)
		return reply
	case shardrpc.InstallShardArgs:
		reply := shardrpc.InstallShardReply{}
		kv.installShardImpl(&req, &reply)
		return reply
	case shardrpc.DeleteShardArgs:
		reply := shardrpc.DeleteShardReply{}
		kv.deleteShardImpl(&req, &reply)
		return reply
	default:
		fmt.Printf("DoOp: unknown req type = %T\n", raw_req)
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.mp)
	e.Encode(kv.sessions)
	e.Encode(kv.shardNums)
	e.Encode(kv.frozenShards)
	e.Encode(kv.ownedShards)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) < 1 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.mp) != nil {
		log.Fatalf("%v couldn't decode kv.mp", kv.me)
	}
	if d.Decode(&kv.sessions) != nil {
		log.Fatalf("%v couldn't decode kv.sessions", kv.me)
	}
	if d.Decode(&kv.shardNums) != nil {
		log.Fatalf("%v couldn't decode kv.shardNums", kv.me)
	}
	if d.Decode(&kv.frozenShards) != nil {
		log.Fatalf("%v couldn't decode kv.frozenShards", kv.me)
	}
	if d.Decode(&kv.ownedShards) != nil {
		log.Fatalf("%v couldn't decode kv.ownedShards", kv.me)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardReply{})
	labgob.Register(ClientSession{})

	kv := &KVServer{gid: gid, me: me}
	kv.mp = make(map[string]ValueVersion)
	kv.sessions = make(map[int64]*ClientSession)
	kv.shardNums = make(map[shardcfg.Tshid]shardcfg.Tnum)
	kv.frozenShards = make(map[shardcfg.Tshid]bool)
	kv.ownedShards = make(map[shardcfg.Tshid]bool)
	// Initialize ownedShards for Gid1 - the initial config assigns all shards to Gid1
	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.ownedShards[shardcfg.Tshid(i)] = true
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
