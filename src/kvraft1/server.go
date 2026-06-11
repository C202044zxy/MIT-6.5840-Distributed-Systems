package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
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

	// Your definitions here.
	mu       sync.Mutex
	mp       map[string]ValueVersion
	sessions map[int64]*ClientSession // Track last request per client
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) get_impl(args *rpc.GetArgs, reply *rpc.GetReply) {
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
func (kv *KVServer) put_impl(args *rpc.PutArgs, reply *rpc.PutReply) {
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

// --- Transaction ---
func (kv *KVServer) applyGet(key string) rpc.OpResult {
	pair, ok := kv.mp[key]
	if ok {
		return rpc.OpResult{
			Value:   pair.Value,
			Version: pair.Version,
			Exists:  true,
			Err:     rpc.OK,
		}
	} else {
		return rpc.OpResult{
			Exists: false,
			Err:    rpc.ErrNoKey,
		}
	}
}

func (kv *KVServer) applyPut(key, value string) rpc.OpResult {
	// Fix(Claude): populate OpResult.Version with the new version after the
	// Put. The plan (rpc.OpResult doc) specifies OpPut returns "new version
	// after OpPut", but the executor previously returned only Err, leaving
	// callers unable to learn the resulting version (e.g. to chain a CAS).
	pair, ok := kv.mp[key]
	var newVer rpc.Tversion
	if ok {
		newVer = pair.Version + 1
	} else {
		newVer = 1
	}
	kv.mp[key] = ValueVersion{value, newVer}
	return rpc.OpResult{Version: newVer, Err: rpc.OK}
}

func (kv *KVServer) evalCompare(c rpc.Compare) bool {
	pair, ok := kv.mp[c.Key]
	if !ok {
		return c.Target == rpc.CmpVersion && c.Op == rpc.CmpEqual && c.Version == 0
	}

	if c.Target == rpc.CmpVersion {
		switch c.Op {
		case rpc.CmpEqual:
			return pair.Version == c.Version
		case rpc.CmpNotEqual:
			return pair.Version != c.Version
		case rpc.CmpLess:
			return pair.Version < c.Version
		case rpc.CmpGreater:
			return pair.Version > c.Version
		default:
			return false
		}
	} else {
		switch c.Op {
		case rpc.CmpEqual:
			return pair.Value == c.Value
		case rpc.CmpNotEqual:
			return pair.Value != c.Value
		case rpc.CmpLess:
			return pair.Value < c.Value
		case rpc.CmpGreater:
			return pair.Value > c.Value
		default:
			return true
		}
	}
}

func (kv *KVServer) txn_impl(args *rpc.TxnArgs, reply *rpc.TxnReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if session, ok := kv.sessions[args.ClientId]; ok {
		if args.SeqNum == session.LastSeqNum {
			// Duplicate request - return cached result
			*reply = session.LastReply.(rpc.TxnReply)
			return
		} else if args.SeqNum < session.LastSeqNum {
			// Old request - should not happen, but return OK to be safe
			reply.Err = rpc.OK
			return
		}
	}

	reply.Succeeded = true
	ops := args.ThenOps
	for _, c := range args.Conds {
		if !kv.evalCompare(c) {
			reply.Succeeded = false
			ops = args.ElseOps
			break
		}
	}

	for _, op := range ops {
		result := rpc.OpResult{}
		if op.Type == rpc.OpGet {
			result = kv.applyGet(op.Key)
		} else {
			result = kv.applyPut(op.Key, op.Value)
		}
		reply.Results = append(reply.Results, result)
	}

	reply.Err = rpc.OK
	kv.sessions[args.ClientId] = &ClientSession{
		LastSeqNum: args.SeqNum,
		LastReply:  *reply,
	}
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
		kv.get_impl(&req, &reply)
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}
		kv.put_impl(&req, &reply)
		return reply
	case rpc.TxnArgs:
		reply := rpc.TxnReply{}
		kv.txn_impl(&req, &reply)
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
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	if readIndex, ok := kv.rsm.ReadIndex(); ok {
		if kv.rsm.WaitApplied(readIndex, 100*time.Millisecond) {
			kv.mu.Lock()
			pair, exists := kv.mp[args.Key]
			if exists {
				reply.Value = pair.Value
				reply.Version = pair.Version
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
			kv.mu.Unlock()
			return
		}
	}

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

func (kv *KVServer) Txn(args *rpc.TxnArgs, reply *rpc.TxnReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.TxnReply)
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(ClientSession{})
	labgob.Register(rpc.TxnArgs{})
	labgob.Register(rpc.TxnReply{})

	kv := &KVServer{me: me}

	// Initialize maps BEFORE MakeRSM, since Restore() may populate them
	kv.mp = make(map[string]ValueVersion)
	kv.sessions = make(map[int64]*ClientSession)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// Opt into lease reads so Get can take the fast path (see Get).
	kv.rsm.EnableLeaseRead()
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
