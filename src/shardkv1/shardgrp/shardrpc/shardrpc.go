package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type FreezeShardReply struct {
	State    []byte
	Sessions []byte // serialized client dedup table (for shard migration)
	Num      shardcfg.Tnum
	Err      rpc.Err
}

type InstallShardArgs struct {
	Shard    shardcfg.Tshid
	State    []byte
	Sessions []byte // serialized client dedup table (for shard migration)
	Num      shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
}

// --- shardkv Transaction ---
type Vote int

const (
	VoteCommit Vote = iota
	VoteCondFail
	VoteWait
	VoteDie
)

// Participant 2PC ops (each submitted through the participant group's rsm)
type PrepareArgs struct {
	Tid   int64         // txn id (stable across retries)
	Ts    int64         // wait-die timestamp (stable across retries)
	Conds []rpc.Compare // this group's subset of preconditions
	Ops   []rpc.TxnOp   // this group's subset of ops
	Idx   []int         // original index of each op in the client's flat list
}

type PrepareReply struct {
	Vote    Vote
	Results []rpc.OpResult
	Idx     []int
	Err     rpc.Err
} // Get reads at prepare

type CommitArgs struct{ Tid int64 }

type CommitReply struct {
	Results []rpc.OpResult
	Idx     []int
	Err     rpc.Err
} // new versions for Puts

type AbortArgs struct{ Tid int64 }

type AbortReply struct{ Err rpc.Err }

// Coordinator-facing op (client -> coordinator group)
type TxnPart struct {
	Gid     tester.Tgid
	Servers []string
	Conds   []rpc.Compare
	Ops     []rpc.TxnOp
	Idx     []int
}

type CoordTxnArgs struct {
	Tid      int64
	Ts       int64
	ClientId int64
	SeqNum   int64
	Parts    []TxnPart
}

type CoordTxnReply struct {
	Committed bool
	Results   []rpc.OpResult
	Err       rpc.Err
} // Err=ErrRetry ⇒ client retries

// Internal coordinator rsm ops (logged in the coordinator group)
type TxnBeginOp struct{ Args CoordTxnArgs }

type TxnDecideOp struct {
	Tid     int64
	Commit  bool
	Retry   bool           // true ⇒ ABORT because of wait-die DIE / stale config; client retries
	Results []rpc.OpResult // aggregated prepare results (Get reads + Put placeholders)
}

type TxnFinishOp struct {
	Tid   int64
	Reply CoordTxnReply
}
