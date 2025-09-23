package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	Follower = iota
	Candidate
	Leader
)

type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain

	applyCh chan raftapi.ApplyMsg

	// Persistent state on all servers
	currentTerm      int
	votedFor         int
	log              []Log
	snapshot         []byte
	offset           int
	lastIncludedTerm int

	// Volatile state on all servers
	state         int
	numPeers      int
	lastHeartbeat time.Time
	// 3B
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == Leader)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != Leader {
		return index, term, isLeader
	}
	index = len(rf.log) + rf.offset
	DPrintf("Svr %d receives command, index = %d", rf.me, index)
	term = rf.currentTerm
	isLeader = true
	rf.log = append(rf.log, Log{command, term})
	rf.persist()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// should be called when rf is locked
func (rf *Raft) convert2Follower(term int) {
	if rf.state == Leader {
		rf.nextIndex = nil
		rf.matchIndex = nil
	}
	// set new term and convert to follower
	rf.currentTerm = term
	rf.state = Follower
	// term is up-to-date, the votedFor needs to be clear
	rf.votedFor = -1
	// only heartbeat message can reset the timer, but not here
	rf.persist()
}

// should be called when rf is locked
func (rf *Raft) convert2Leader() {
	DPrintf("Svr %v becomes leader", rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int, rf.numPeers)
	rf.matchIndex = make([]int, rf.numPeers)
	for i := 0; i < rf.numPeers; i++ {
		rf.nextIndex[i] = len(rf.log) + rf.offset
		rf.matchIndex[i] = -1 // match nothing here
	}
	go rf.heartbeat()
	go rf.replicateLog()
	go rf.commitLog()
	go rf.sendSnapshot()
}

func (rf *Raft) commitLog() {
	// commit period 10 ms
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	for !rf.killed() {
		// check that state hasn't changed
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if rf.commitIndex+1 < rf.offset+len(rf.log) {
			for N := rf.commitIndex + 1; N < rf.offset+len(rf.log); N++ {
				cnt := 1
				for i := 0; i < rf.numPeers; i++ {
					if i == rf.me {
						continue
					}
					// IMPORTANT: rf.log[N].term = term
					if rf.matchIndex[i]+rf.offset >= N && rf.log[N-rf.offset].Term == term {
						cnt++
					}
					if cnt*2 > rf.numPeers {
						rf.commitIndex = N
						DPrintf("Agree at log entry %d", N)
						break
					}
				}
			}
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	// election timeout [500, 1000) ms

	timeout := time.Duration(500+rand.Int63()%500) * time.Millisecond
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		now := time.Now()
		if (rf.state == Follower || rf.state == Candidate) && now.Sub(rf.lastHeartbeat) > timeout {
			// convert the server to Candidate state
			rf.currentTerm++
			rf.persist()
			rf.state = Candidate
			rf.lastHeartbeat = time.Now()
			timeout = time.Duration(500+rand.Int63()%500) * time.Millisecond
			// start a new go routine to collect votes
			go rf.collectVotes()
		}
		rf.mu.Unlock()
		// pause for 50 milliseconds.
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyLog() {
	// check period 10 ms

	for !rf.killed() {

		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			DPrintf("Svr %d Apply log %d into the channel", rf.me, rf.lastApplied+1)
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied+1-rf.offset].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.lastApplied++
			rf.applyCh <- msg
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// 3A
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.numPeers = len(rf.peers)
	rf.lastHeartbeat = time.Now()

	// 3B
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]Log, 1)
	rf.log[0].Term = 0
	rf.applyCh = applyCh

	// 3D
	rf.snapshot = nil
	rf.offset = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()

	return rf
}
