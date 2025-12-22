package kvraft

import (
	"math/rand"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
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

// LoadLeaderId atomic read leaderId
func (ck *Clerk) LoadLeaderId() int {
	return int(ck.leaderId.Load())
}

// StoreLeaderId atomic write leaderId
func (ck *Clerk) StoreLeaderId(id int) {
	ck.leaderId.Store(int32(id))
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	// Generate unique client ID using time and random number
	ck.clientId = time.Now().UnixNano() + rand.Int63()
	ck.seqNum.Store(0)
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	seqNum := ck.seqNum.Add(1)
	leader_id := ck.LoadLeaderId()
	defer ck.StoreLeaderId(leader_id)
	for {
		args := rpc.GetArgs{Key: key, ClientId: ck.clientId, SeqNum: seqNum}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader_id], "KVServer.Get", &args, &reply)
		if !ok {
			// keep trying if rpc fails
			leader_id = (leader_id + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrNoKey {
			// key does not exist
			return "", 0, rpc.ErrNoKey
		}
		if reply.Err == rpc.OK {
			return reply.Value, reply.Version, reply.Err
		}
		leader_id = (leader_id + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
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
		if !ok {
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
		if reply.Err == rpc.ErrNoKey {
			return rpc.ErrNoKey
		}
		if reply.Err == rpc.OK {
			return rpc.OK
		}
		firstTry = false
		leader_id = (leader_id + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}
