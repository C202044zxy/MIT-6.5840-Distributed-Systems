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
	currentTerm int
	votedFor    int
	log         []Log

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := args.Term
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	if term < rf.currentTerm || args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		// reject the request
		reply.Term = rf.currentTerm
		reply.VoteGranted = 0
		return
	}

	if term > rf.currentTerm {
		rf.convert2Follower(term)
	}
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 {
		// can vote for the requesting candidate
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = 1
	} else {
		reply.VoteGranted = 0
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := args.Term
	if term < rf.currentTerm {
		// reject the request
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.PrevLogIndex == -1 {
		// heartbeat message
		if term > rf.currentTerm || rf.state != Follower {
			rf.convert2Follower(term)
		}
		// receive heartbeat message from leader
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.lastHeartbeat = time.Now()
		return
	}
	// append entries message
	DPrintf("Svr %d receives append entries message", rf.me)
	// consistency check
	reply.Term = rf.currentTerm
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommit := args.LeaderCommit

	if len(rf.log) <= prevLogIndex || prevLogTerm != rf.log[prevLogIndex].Term {
		// no logs at that index or the log is not consistent with leader
		DPrintf("Svr %d Consitency check failed, return. ", rf.me)
		reply.Success = false
		if len(rf.log) <= prevLogIndex {
			reply.NextIndex = len(rf.log)
		} else {
			// skip invalid log in a row
			invalidTerm := rf.log[prevLogIndex].Term
			for i := prevLogIndex; i >= 0 && rf.log[i].Term == invalidTerm; i-- {
				reply.NextIndex = i
			}
		}
		return
	}
	// prev must be the last committed entry
	// fix(2B): must +1 here, should include log entry at prevLogIndex
	rf.log = append(rf.log[:prevLogIndex+1], args.Entries...)
	reply.Success = true
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, len(rf.log)-1)
	}
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
	DPrintf("Svr %d receives command", rf.me)
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = true
	rf.log = append(rf.log, Log{command, term})
	DPrintf("Svr %d log length %d after append", rf.me, len(rf.log))
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
	// start the timer
	rf.lastHeartbeat = time.Now()
}

// should be called when rf is locked
func (rf *Raft) convert2Leader() {
	DPrintf("Svr %v becomes leader", rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int, rf.numPeers)
	rf.matchIndex = make([]int, rf.numPeers)
	for i := 0; i < rf.numPeers; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	go rf.heartbeat()
	go rf.replicateLog()
	go rf.commitLog()
}

func (rf *Raft) heartbeat() {
	// heartbeat period 150 ms
	for !rf.killed() {
		// check that state hasn't changed
		rf.mu.Lock()
		term := rf.currentTerm
		me := rf.me
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := 0; i < rf.numPeers; i++ {
			if i == rf.me {
				continue
			}
			go func() {
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     me,
					PrevLogIndex: -1,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.convert2Follower(reply.Term)
					return
				}
				if rf.currentTerm != term || rf.state != Leader {
					// the state has changed. halt
					return
				}
			}()
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}

func (rf *Raft) replicateLog() {
	// replicateLog period 150 ms
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	for !rf.killed() {
		// check that state hasn't changed
		rf.mu.Lock()
		me := rf.me
		// may be optimized to the least index of nextIndex
		log := rf.log
		nextIndex := rf.nextIndex
		commitIndex := rf.commitIndex
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := 0; i < rf.numPeers; i++ {
			if i == rf.me {
				continue
			}
			// send request even if no new log to append
			// since this RPC does distribute commit information
			go func() {
				prev := nextIndex[i] - 1
				// TODO: should I append all logs at one time?
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     me,
					PrevLogIndex: prev,
					PrevLogTerm:  log[prev].Term,
					Entries:      log[nextIndex[i]:],
					LeaderCommit: commitIndex,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.convert2Follower(reply.Term)
					return
				}
				if rf.currentTerm != term || rf.state != Leader {
					// the state has changed. halt
					return
				}
				if reply.Success {
					// have pushed all available logs into the follower
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = len(rf.log) - 1
				} else {
					// decrease the nextIndex
					nextIndex[i] = reply.NextIndex
				}
			}()
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
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
		if rf.commitIndex+1 < len(rf.log) {
			for N := rf.commitIndex + 1; N < len(rf.log); N++ {
				cnt := 1
				for i := 0; i < rf.numPeers; i++ {
					if i == rf.me {
						continue
					}
					// IMPORTANT: rf.log[N].term = term
					if rf.matchIndex[i] >= N && rf.log[N].Term == term {
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

func (rf *Raft) collectVotes() {
	// make a copy
	// since you must release the lock when you call RPC
	// check the state hasn't changed when receiving reply
	rf.mu.Lock()
	voteCount := 1
	successed := false
	term := rf.currentTerm
	me := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()
	var voteMutex sync.Mutex

	for i := 0; i < rf.numPeers; i++ {
		if i == rf.me {
			continue
		}
		go func() {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				return
			}

			// process the reply. Hold the lock now
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.convert2Follower(reply.Term)
				return
			}
			if rf.currentTerm != term || rf.state != Candidate {
				// the state has changed. halt
				return
			}
			if reply.VoteGranted == 1 {
				voteMutex.Lock()
				voteCount++
				if voteCount*2 > rf.numPeers && !successed {
					// get the majority of votes
					successed = true
					rf.convert2Leader()
				}
				voteMutex.Unlock()
			}
		}()
	}
}

func (rf *Raft) ticker() {
	// election timeout [500, 1000) ms
	// check period [50, 350) ms

	timeout := time.Duration(500+rand.Int63()%500) * time.Millisecond
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		now := time.Now()
		if (rf.state == Follower || rf.state == Candidate) && now.Sub(rf.lastHeartbeat) > timeout {
			// convert the server to Candidate state
			rf.currentTerm++
			rf.state = Candidate
			rf.lastHeartbeat = time.Now()
			timeout = time.Duration(500+rand.Int63()%500) * time.Millisecond
			// start a new go routine to collect votes
			go rf.collectVotes()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
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
				Command:      rf.log[rf.lastApplied+1].Command,
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()

	return rf
}
