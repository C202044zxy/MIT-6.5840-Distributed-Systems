package shardgrp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

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

type PreparedTxn struct {
	Ts           int64          // wait-die timestamp (for lock-conflict comparisons by later txns)
	LockedKeys   []string       // keys this group locked for this Tid (to release on Commit/Abort)
	BufferedPuts []BufferedPut  // writes staged but NOT yet applied to kv.mp
	GetResults   []rpc.OpResult // values/versions read during Prepare
	Idx          []int          // original op indices, to reassemble ordered Results at the coordinator
	CondOK       bool           // whether this group's Conds all held
}

type BufferedPut struct {
	Key   string
	Value string
	Pos   int // index of this Put in the per-part Results slice (parallel to Idx)
}

// CoordTxnPhase tracks how far the coordinator has driven a txn. It is part of
// the replicated CoordState so a new leader can resume from the logged phase.
type CoordTxnPhase int

const (
	TxnBegan   CoordTxnPhase = iota // intent logged; driver must run Phase 1 (Prepare)
	TxnDecided                      // global COMMIT/ABORT logged; driver must run Phase 2
	TxnDone                         // final reply logged; return it on client resend
)

// CoordState is the replicated per-txn coordinator record: the original args,
// the current 2PC phase, the global decision, and the final client reply.
type CoordState struct {
	Args   shardrpc.CoordTxnArgs
	Phase  CoordTxnPhase
	Commit bool
	Reply  shardrpc.CoordTxnReply
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

	locks    map[string]int64       // key -> Tid holding it
	prepared map[int64]*PreparedTxn // Tid -> {Ts, lockedKeys, bufferedPuts, getResults, idx, condOK}
	decided  map[int64]bool         // Tid -> committed? (idempotent Commit/Abort + dedup)

	// Coordinator role: replicated per-txn state for txns this group drives.
	coordTxns map[int64]*CoordState // Tid -> coordinator record (intent, phase, decision, reply)

	// Coordinator role: transient (not replicated) driver bookkeeping.
	clnt    *tester.Clnt   // outbound client to reach participant groups
	driving map[int64]bool // Tid -> a driver goroutine is currently driving it
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

	// 2PL: a key locked by an in-flight cross-shard txn cannot be read until
	// the txn commits/aborts. Don't cache this; the clerk retries.
	if _, locked := kv.locks[args.Key]; locked {
		reply.Err = rpc.ErrLocked
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

	// 2PL: a key locked by an in-flight cross-shard txn cannot be overwritten
	// until the txn commits/aborts. Don't cache this; the clerk retries.
	if _, locked := kv.locks[args.Key]; locked {
		reply.Err = rpc.ErrLocked
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
}

func (kv *KVServer) freezeShardImpl(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.shardNums[args.Shard]; ok && args.Num < v {
		reply.Num = v
		reply.Err = rpc.ErrVersion
		return
	}
	// Freeze safety: refuse (retryably) while any key in this shard is locked by
	// an in-flight cross-shard txn, so migration never races a prepared txn.
	for key := range kv.locks {
		if shardcfg.Key2Shard(key) == args.Shard {
			reply.Err = rpc.ErrLocked
			return
		}
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
	// Migrate the client dedup table alongside the shard data so that a
	// client retrying an in-flight op at the new owner is recognized as a
	// duplicate (and gets the cached reply) instead of re-executing.
	reply.Sessions = encodeSessions(kv.sessions)
}

func (kv *KVServer) installShardImpl(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.shardNums[args.Shard]; ok && args.Num <= v {
		// Duplicate or stale install. This shard has already been installed at
		// this config number (or a newer one); plain Puts don't bump shardNums,
		// so re-installing the (possibly empty) snapshot here would clobber
		// writes accepted since. Treat as an idempotent no-op.
		reply.Err = rpc.OK
		return
	}
	kv.shardNums[args.Shard] = args.Num
	kv.ownedShards[args.Shard] = true
	delete(kv.frozenShards, args.Shard)
	var shardMap map[string]ValueVersion
	if err := json.Unmarshal(args.State, &shardMap); err != nil {
		log.Fatalf("Unmarshal err %v", err)
	}
	for k, v := range shardMap {
		kv.mp[k] = v
	}
	// Merge the migrated dedup table. seqNum is globally monotonic per client,
	// so the entry with the larger LastSeqNum is the more recent op; keeping the
	// max preserves the only op a client could still be retrying.
	for clientId, incoming := range decodeSessions(args.Sessions) {
		if cur, ok := kv.sessions[clientId]; !ok || incoming.LastSeqNum > cur.LastSeqNum {
			kv.sessions[clientId] = incoming
		}
	}
	reply.Err = rpc.OK
}

func encodeSessions(sessions map[int64]*ClientSession) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(sessions); err != nil {
		log.Fatalf("encode sessions err %v", err)
	}
	return w.Bytes()
}

func decodeSessions(data []byte) map[int64]*ClientSession {
	sessions := make(map[int64]*ClientSession)
	if len(data) < 1 {
		return sessions
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&sessions); err != nil {
		log.Fatalf("decode sessions err %v", err)
	}
	return sessions
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

// --- shardkv Transaction: participant (2PC + 2PL) ---

// prepareImpl runs Phase 1 for this group: dedup on Tid, ownership-check every
// key, acquire per-key locks (wait-die on conflict), evaluate this group's
// Conds, read Gets and buffer Puts, then vote. Holds locks until Commit/Abort.
func (kv *KVServer) prepareImpl(args *shardrpc.PrepareArgs, reply *shardrpc.PrepareReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Tid := args.Tid
	Ts := args.Ts

	// Dedup: if a record for this Tid already exists, return its cached vote.
	// A prepared record may be either a COMMIT vote (CondOK) or a CondFail vote
	// (locks held, no buffered results) — re-Prepare after a coordinator leader
	// change must reproduce the SAME vote, not blindly VoteCommit (a CondFail
	// record has a non-empty Idx but nil Results, which would also panic the
	// coordinator's result reassembly).
	if prepared, ok := kv.prepared[Tid]; ok {
		if prepared.CondOK {
			*reply = shardrpc.PrepareReply{
				Vote:    shardrpc.VoteCommit,
				Results: prepared.GetResults,
				Idx:     prepared.Idx,
				Err:     rpc.OK,
			}
		} else {
			*reply = shardrpc.PrepareReply{Vote: shardrpc.VoteCondFail, Idx: prepared.Idx, Err: rpc.OK}
		}
		return
	}
	if decided, ok := kv.decided[Tid]; ok {
		// The prepared record was already released. decided==true ⇒ committed
		// (idempotent VoteCommit); decided==false ⇒ aborted ⇒ VoteDie so the
		// coordinator re-aborts rather than re-locking and re-executing.
		if decided {
			*reply = shardrpc.PrepareReply{Vote: shardrpc.VoteCommit, Err: rpc.OK}
		} else {
			*reply = shardrpc.PrepareReply{Vote: shardrpc.VoteDie, Err: rpc.OK}
		}
		return
	}

	// Ownship checks on Every key
	var keys []string
	for _, cond := range args.Conds {
		keys = append(keys, cond.Key)
	}
	for _, op := range args.Ops {
		keys = append(keys, op.Key)
	}
	for _, key := range keys {
		if owned, ok := kv.ownedShards[shardcfg.Key2Shard(key)]; !ok || !owned {
			reply.Err = rpc.ErrWrongGroup
			return
		}
	}

	// Lock acquisition
	unlockKeys := func(keys []string) {
		for _, key := range keys {
			delete(kv.locks, key)
		}
	}

	reply.Err = rpc.OK
	for i, key := range keys {
		lockTid, ok := kv.locks[key]
		if !ok {
			kv.locks[key] = Tid
			continue
		}
		if lockTid == Tid {
			// We already hold this key (duplicate key across conds/ops).
			continue
		}
		holder, ok := kv.prepared[lockTid]
		if !ok {
			// Holder has no prepared record (should not happen: locks and
			// prepared are mutated together under kv.mu). Treat as free.
			kv.locks[key] = Tid
			continue
		}
		// wait-die: an OLDER requester waits, a YOUNGER one dies. Order is the
		// strict total order (Ts, Tid) — the Tid tiebreak makes ties impossible,
		// so two equal-Ts txns can never wait on each other (no deadlock).
		if Ts < holder.Ts || (Ts == holder.Ts && Tid < lockTid) {
			reply.Vote = shardrpc.VoteWait
		} else {
			reply.Vote = shardrpc.VoteDie
		}
		unlockKeys(keys[:i])
		return
	}

	// Evaluate Conds
	for _, cond := range args.Conds {
		if !kv.evalCompare(cond) {
			// Do not release the lock. Wait until commit.
			reply.Vote = shardrpc.VoteCondFail
			kv.prepared[Tid] = &PreparedTxn{
				Ts:         Ts,
				LockedKeys: keys,
				Idx:        args.Idx,
				CondOK:     false,
			}
			return
		}
	}

	// Execute Ops
	bufferedPuts := make([]BufferedPut, 0)
	getResults := make([]rpc.OpResult, 0)
	for _, op := range args.Ops {
		v, ok := kv.mp[op.Key]
		switch op.Type {
		case rpc.OpGet:
			if !ok {
				getResults = append(getResults, rpc.OpResult{
					Version: 0,
					Exists:  false,
					Err:     rpc.ErrNoKey,
				})
			} else {
				getResults = append(getResults, rpc.OpResult{
					Value:   v.Value,
					Version: v.Version,
					Exists:  true,
					Err:     rpc.OK,
				})
			}

		case rpc.OpPut:
			// Record the result slot for this Put; commit fills in the new
			// version here once the write is applied.
			bufferedPuts = append(bufferedPuts, BufferedPut{Key: op.Key, Value: op.Value, Pos: len(getResults)})
			getResults = append(getResults, rpc.OpResult{Err: rpc.OK})
		}
	}

	// fill PreparedTxn and reply
	kv.prepared[Tid] = &PreparedTxn{
		Ts:           Ts,
		LockedKeys:   keys,
		BufferedPuts: bufferedPuts,
		GetResults:   getResults,
		Idx:          args.Idx,
		CondOK:       true,
	}

	*reply = shardrpc.PrepareReply{
		Vote:    shardrpc.VoteCommit,
		Results: getResults,
		Idx:     args.Idx,
		Err:     rpc.OK,
	}
}

// commitImpl runs Phase 2 (commit): apply buffered Puts with the version-bump
// rule, fill reply with new versions, release locks, mark decided. Idempotent.
func (kv *KVServer) commitImpl(args *shardrpc.CommitArgs, reply *shardrpc.CommitReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Tid := args.Tid
	prepared := kv.prepared[Tid]

	if prepared == nil {
		// Never prepared here (e.g. this group only had a WAIT/DIE vote), or
		// already cleaned up. Nothing to apply; ack idempotently.
		reply.Err = rpc.OK
		return
	}

	// Idempotent resend: GetResults already holds the committed values/versions
	// (we overwrite it below on first commit), so just return the cached reply.
	if decided, ok := kv.decided[Tid]; ok && decided {
		*reply = shardrpc.CommitReply{
			Results: prepared.GetResults,
			Idx:     prepared.Idx,
			Err:     rpc.OK,
		}
		return
	}

	for _, op := range prepared.BufferedPuts {
		version := rpc.Tversion(1)
		if v, ok := kv.mp[op.Key]; ok {
			version = v.Version + 1
		}
		kv.mp[op.Key] = ValueVersion{op.Value, version}
		// Report the new version in this Put's result slot.
		prepared.GetResults[op.Pos] = rpc.OpResult{Version: version, Exists: true, Err: rpc.OK}
	}
	for _, key := range prepared.LockedKeys {
		delete(kv.locks, key)
	}

	kv.decided[Tid] = true
	*reply = shardrpc.CommitReply{
		Results: prepared.GetResults,
		Idx:     prepared.Idx,
		Err:     rpc.OK,
	}
}

// abortImpl runs Phase 2 (abort): drop the buffer, release locks, mark decided.
// Idempotent.
func (kv *KVServer) abortImpl(args *shardrpc.AbortArgs, reply *shardrpc.AbortReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Tid := args.Tid

	if _, ok := kv.decided[Tid]; ok {
		reply.Err = rpc.OK
		return
	}

	// prepared may be nil: this group voted WAIT/DIE (released its locks and
	// never stored a record) yet the coordinator still broadcasts Abort to all
	// parts. Record the decision so a resend is an idempotent no-op.
	if prepared := kv.prepared[Tid]; prepared != nil {
		for _, key := range prepared.LockedKeys {
			delete(kv.locks, key)
		}
		delete(kv.prepared, Tid)
	}

	kv.decided[Tid] = false
	reply.Err = rpc.OK
}

// --- shardkv Transaction: coordinator (drives 2PC, fault-tolerant via rsm) ---

// txnBeginImpl applies a TxnBeginOp: record (or dedup) the txn intent so the
// driver goroutine can pick it up. Returns the stored final reply if the txn is
// already TxnDone (client resend). Caller holds kv.mu via DoOp.
func (kv *KVServer) txnBeginImpl(op *shardrpc.TxnBeginOp) shardrpc.CoordTxnReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if txn, ok := kv.coordTxns[op.Args.Tid]; ok {
		if txn.Phase == TxnDone {
			return txn.Reply
		}
		return shardrpc.CoordTxnReply{}
	}
	kv.coordTxns[op.Args.Tid] = &CoordState{
		Args:  op.Args,
		Phase: TxnBegan,
	}
	return shardrpc.CoordTxnReply{}
}

// txnDecideImpl applies a TxnDecideOp: durably record the global COMMIT/ABORT
// decision (and prepared Get results) and advance the phase to TxnDecided.
func (kv *KVServer) txnDecideImpl(op *shardrpc.TxnDecideOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	txn, ok := kv.coordTxns[op.Tid]
	if !ok || txn.Phase != TxnBegan {
		return
	}
	txn.Phase = TxnDecided
	txn.Commit = op.Commit
	txn.Reply = shardrpc.CoordTxnReply{
		Committed: op.Commit,
		Results:   op.Results,
		Err:       rpc.OK,
	}
	if op.Retry {
		txn.Reply.Err = rpc.ErrRetry
	}
}

// txnFinishImpl applies a TxnFinishOp: store the final reassembled reply and
// advance the phase to TxnDone (so resends return it and the driver stops).
func (kv *KVServer) txnFinishImpl(op *shardrpc.TxnFinishOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	txn, ok := kv.coordTxns[op.Tid]
	if !ok || txn.Phase != TxnDecided {
		return
	}
	txn.Phase = TxnDone
	txn.Reply = op.Reply
}

// CoordTxn is the client-facing RPC: log the intent (TxnBeginOp), then block
// until the txn reaches TxnDone and return the stored reply. Off-leader
// rsm.Submit returns ErrWrongLeader so the client retries another server.
func (kv *KVServer) CoordTxn(args *shardrpc.CoordTxnArgs, reply *shardrpc.CoordTxnReply) {
	err, resp := kv.rsm.Submit(shardrpc.TxnBeginOp{Args: *args})
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if r, ok := resp.(shardrpc.CoordTxnReply); ok && r.Err != "" {
		*reply = r
		return
	}

	Tid := args.Tid
	for !kv.killed() {
		// Read Phase and copy Reply under the lock: coordTxns[Tid] is a pointer
		// the rsm apply goroutine mutates (txnDecide/txnFinishImpl).
		kv.mu.Lock()
		txn, ok := kv.coordTxns[Tid]
		done := ok && txn.Phase == TxnDone
		var stored shardrpc.CoordTxnReply
		if done {
			stored = txn.Reply
		}
		kv.mu.Unlock()
		if done {
			*reply = stored
			return
		}
		if _, isLeader := kv.rsm.Raft().GetState(); !isLeader {
			reply.Err = rpc.ErrWrongLeader
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// coordDriver runs forever (started in StartServerShardGrp). On the leader it
// scans coordTxns for not-yet-TxnDone entries and drives each forward:
//   - TxnBegan:   Prepare all participants, aggregate votes (wait-die), then
//     rsm.Submit a TxnDecideOp with the COMMIT/ABORT decision.
//   - TxnDecided: Commit/Abort every prepared participant, reassemble Results
//     by Idx, then rsm.Submit a TxnFinishOp.
//
// Each rsm.Submit fails off-leader, so only the leader drives; a new leader
// resumes from the logged phase.
func (kv *KVServer) coordDriver() {
	clean := func(tid int64) {
		kv.mu.Lock()
		delete(kv.driving, tid)
		kv.mu.Unlock()
	}
	for !kv.killed() {
		if _, isLeader := kv.rsm.Raft().GetState(); !isLeader {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// snapshot the list of not-yet-Done txns
		kv.mu.Lock()
		pending := make([]int64, 0, len(kv.coordTxns))
		for k, txn := range kv.coordTxns {
			if txn.Phase != TxnDone {
				pending = append(pending, k)
			}
		}
		kv.mu.Unlock()

		for _, tid := range pending {
			kv.mu.Lock()
			if kv.driving[tid] {
				kv.mu.Unlock()
				continue
			}
			kv.driving[tid] = true
			snap := *kv.coordTxns[tid]
			kv.mu.Unlock()

			go func(tid int64, snap CoordState) {
				defer clean(tid)
				if snap.Phase == TxnBegan {
					kv.runBegan(snap)
				} else {
					kv.runDecide(snap)
				}
			}(tid, snap)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) runBegan(txn CoordState) {
	args := txn.Args
	lenResult := 0
	for _, part := range args.Parts {
		lenResult += len(part.Idx)
	}
	results := make([]rpc.OpResult, lenResult)
	voteCommit := make([]bool, len(args.Parts))
	decideOp := shardrpc.TxnDecideOp{Tid: args.Tid}

	for !kv.killed() {
		// Only the coordinator-group leader drives; a demoted leader must stop
		// re-Preparing so the new leader owns the drive (it resumes from the
		// logged TxnBegan phase).
		if !kv.isLeader() {
			return
		}
		var wg sync.WaitGroup
		var voteDie atomic.Bool
		var voteCondFail atomic.Bool
		var voteWait atomic.Bool
		var prepareFailed atomic.Bool

		for i, part := range args.Parts {
			if voteCommit[i] {
				// Already voted COMMIT in an earlier round; its locks are held,
				// so don't re-Prepare it (only WAIT groups are re-driven).
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				clerk := MakeClerk(kv.clnt, part.Servers)
				reply := clerk.Prepare(&shardrpc.PrepareArgs{
					Tid:   args.Tid,
					Ts:    args.Ts,
					Conds: part.Conds,
					Ops:   part.Ops,
					Idx:   part.Idx,
				})

				if reply.Err != rpc.OK {
					// Participant unreachable / not leader. Don't treat as a
					// vote; retry this round so we never decide COMMIT for a
					// group that never voted.
					prepareFailed.Store(true)
					return
				}
				switch reply.Vote {
				case shardrpc.VoteDie:
					voteDie.Store(true)
				case shardrpc.VoteCondFail:
					voteCondFail.Store(true)
				case shardrpc.VoteWait:
					voteWait.Store(true)
				case shardrpc.VoteCommit:
					// reassemble the result
					voteCommit[i] = true
					for k := range reply.Idx {
						if k < len(reply.Results) && reply.Idx[k] < len(results) {
							results[reply.Idx[k]] = reply.Results[k]
						}
					}
				}
			}()
		}

		wg.Wait()
		if voteCondFail.Load() {
			// A precondition is authoritatively false; aborting and retrying
			// would just fail again, so don't retry.
			decideOp.Commit = false
			decideOp.Retry = false
			decideOp.Results = results
			break
		}
		if voteDie.Load() {
			// Lost a wait-die lock race (younger than a holder); abort and let
			// the client retry with the same Ts so it ages and eventually wins.
			decideOp.Commit = false
			decideOp.Retry = true
			break
		}
		if voteWait.Load() || prepareFailed.Load() {
			// Older than a holder (keep the locks we did get and re-Prepare the
			// WAIT groups), or a participant was transiently unreachable.
			time.Sleep(time.Millisecond * 10)
			continue
		}
		decideOp.Commit = true
		decideOp.Retry = false
		decideOp.Results = results
		break
	}
	kv.rsm.Submit(decideOp)
}

func (kv *KVServer) runDecide(txn CoordState) {
	args := txn.Args

	// Phase 2: tell every prepared participant the global decision. Both
	// Commit and Abort are idempotent and retried until acked, so prepared
	// locks are always released regardless of which way we decided.
	//
	// On COMMIT we start from the prepare-time reply (Get reads + Put
	// placeholders, keyed by global Idx) and overwrite each Put placeholder
	// with the new version the participant reports. On ABORT we keep the
	// decided reply verbatim (Committed=false, Err=OK/ErrRetry, partial reads).
	results := append([]rpc.OpResult(nil), txn.Reply.Results...)

	var wg sync.WaitGroup
	for _, part := range args.Parts {
		wg.Add(1)
		go func(part shardrpc.TxnPart) {
			defer wg.Done()
			clerk := MakeClerk(kv.clnt, part.Servers)
			// Retry the (idempotent) decision until acked — but stop the moment
			// this server is no longer the coordinator leader, so a demoted
			// driver doesn't spin forever; the new leader re-drives from the
			// logged TxnDecided phase.
			for !kv.killed() && kv.isLeader() {
				if txn.Commit {
					reply := clerk.CommitTxn(&shardrpc.CommitArgs{Tid: args.Tid})
					if reply.Err == rpc.OK {
						for k := range reply.Idx {
							if k < len(reply.Results) && reply.Idx[k] < len(results) {
								results[reply.Idx[k]] = reply.Results[k]
							}
						}
						return
					}
				} else if clerk.AbortTxn(&shardrpc.AbortArgs{Tid: args.Tid}).Err == rpc.OK {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(part)
	}
	wg.Wait()

	// If we lost leadership mid-Phase-2 the results may be incomplete; don't
	// log a final reply — the new leader resumes and submits its own.
	if !kv.isLeader() {
		return
	}
	reply := txn.Reply // carries Committed + Err (OK or ErrRetry) from the decision
	if txn.Commit {
		reply.Results = results
	}
	kv.rsm.Submit(shardrpc.TxnFinishOp{Tid: args.Tid, Reply: reply})
}

// evalCompare reports whether a single precondition holds against current
// state (ported from kvraft1/server.go). Caller holds kv.mu.
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
	case shardrpc.PrepareArgs:
		reply := shardrpc.PrepareReply{}
		kv.prepareImpl(&req, &reply)
		return reply
	case shardrpc.CommitArgs:
		reply := shardrpc.CommitReply{}
		kv.commitImpl(&req, &reply)
		return reply
	case shardrpc.AbortArgs:
		reply := shardrpc.AbortReply{}
		kv.abortImpl(&req, &reply)
		return reply
	case shardrpc.TxnBeginOp:
		return kv.txnBeginImpl(&req)
	case shardrpc.TxnDecideOp:
		kv.txnDecideImpl(&req)
		return nil
	case shardrpc.TxnFinishOp:
		kv.txnFinishImpl(&req)
		return nil
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
	// Participant + coordinator 2PC state, so a prepared/decided txn survives a
	// snapshot/restart on this group.
	e.Encode(kv.locks)
	e.Encode(kv.prepared)
	e.Encode(kv.decided)
	e.Encode(kv.coordTxns)
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
	if d.Decode(&kv.locks) != nil {
		log.Fatalf("%v couldn't decode kv.locks", kv.me)
	}
	if d.Decode(&kv.prepared) != nil {
		log.Fatalf("%v couldn't decode kv.prepared", kv.me)
	}
	if d.Decode(&kv.decided) != nil {
		log.Fatalf("%v couldn't decode kv.decided", kv.me)
	}
	if d.Decode(&kv.coordTxns) != nil {
		log.Fatalf("%v couldn't decode kv.coordTxns", kv.me)
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

func (kv *KVServer) Prepare(args *shardrpc.PrepareArgs, reply *shardrpc.PrepareReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.PrepareReply)
}

func (kv *KVServer) CommitTxn(args *shardrpc.CommitArgs, reply *shardrpc.CommitReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.CommitReply)
}

func (kv *KVServer) AbortTxn(args *shardrpc.AbortArgs, reply *shardrpc.AbortReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.AbortReply)
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

// isLeader reports whether this server is currently the Raft leader of its
// group. The coordinator driver uses it to ensure only the leader drives a txn.
func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rsm.Raft().GetState()
	return isLeader
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
// mkclnt builds an outbound *tester.Clnt this server uses (in its coordinator
// role) to reach participant groups. It may be nil for callers that never drive
// cross-shard txns.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int, mkclnt func() *tester.Clnt) []tester.IService {
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
	labgob.Register(shardrpc.PrepareArgs{})
	labgob.Register(shardrpc.CommitArgs{})
	labgob.Register(shardrpc.AbortArgs{})
	labgob.Register(shardrpc.PrepareReply{})
	labgob.Register(shardrpc.CommitReply{})
	labgob.Register(shardrpc.AbortReply{})
	labgob.Register(shardrpc.TxnBeginOp{})
	labgob.Register(shardrpc.TxnDecideOp{})
	labgob.Register(shardrpc.TxnFinishOp{})
	labgob.Register(shardrpc.CoordTxnReply{})

	kv := &KVServer{gid: gid, me: me}
	kv.mp = make(map[string]ValueVersion)
	kv.sessions = make(map[int64]*ClientSession)
	kv.shardNums = make(map[shardcfg.Tshid]shardcfg.Tnum)
	kv.frozenShards = make(map[shardcfg.Tshid]bool)
	kv.ownedShards = make(map[shardcfg.Tshid]bool)
	kv.locks = make(map[string]int64)
	kv.prepared = make(map[int64]*PreparedTxn)
	kv.decided = make(map[int64]bool)
	kv.coordTxns = make(map[int64]*CoordState)
	kv.driving = make(map[int64]bool)
	if mkclnt != nil {
		kv.clnt = mkclnt()
	}
	// Initialize ownedShards for Gid1 - the initial config assigns all shards to Gid1
	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.ownedShards[shardcfg.Tshid(i)] = true
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Coordinator driver: drives logged txns through 2PC on the leader.
	go kv.coordDriver()

	return []tester.IService{kv, kv.rsm.Raft()}
}
