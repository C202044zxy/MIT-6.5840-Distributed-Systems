package rsm

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	idCounter int64
	registry  sync.Map
	index2Id  sync.Map
	stopCh    chan struct{}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		stopCh:       make(chan struct{}),
		sm:           sm,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) NextId() int {
	return int(rand.Int63())
	// return int(atomic.AddInt64(&rsm.idCounter, 1))
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		cmd := msg.Command.(Op)
		// fmt.Printf("me = %d: Reader received message, id = %d, cmd = %v\n", rsm.me, cmd.Id, cmd)
		ret := rsm.sm.DoOp(cmd.Req)
		if chRaw, ok := rsm.registry.Load(cmd.Id); ok {
			ch := chRaw.(chan any)
			select {
			case ch <- ret:
			default:
				fmt.Printf("me = %d: channel closed for id = %v, dropping message\n", rsm.me, cmd.Id)
			}
		}
	}
	// fmt.Printf("me = %d: Applych closed, return\n", rsm.me)
	close(rsm.stopCh)
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	id := rsm.NextId()
	op := Op{Me: rsm.me, Id: id, Req: req}
	respCh := make(chan any, 1)
	// fmt.Printf("me = %d: Generate id = %d\n", rsm.me, id)
	rsm.registry.Store(id, respCh)
	defer func() {
		rsm.registry.Delete(id)
		close(respCh)
	}()
	index, term, leader := rsm.rf.Start(op)
	if !leader {
		return rpc.ErrWrongLeader, nil
	}
	for {
		select {
		case ret := <-respCh:
			// fmt.Printf("me = %d: Command OK, id = %d\n", rsm.me, id)
			return rpc.OK, ret
		case <-rsm.stopCh:
			// fmt.Printf("me = %d: Submit stopped, applyCh closed\n", rsm.me)
			return rpc.ErrWrongLeader, nil
		default:
			currentTerm, isLeader := rsm.Raft().GetState()
			if !isLeader || currentTerm != term {
				return rpc.ErrWrongLeader, nil
			}
			id2, ok := rsm.index2Id.Load(index)
			if ok && id2 != id {
				// different request has appeared at the index returned by Start()
				return rpc.ErrWrongLeader, nil
			}
			time.Sleep(time.Duration(5) * time.Millisecond)
		}
	}
}
