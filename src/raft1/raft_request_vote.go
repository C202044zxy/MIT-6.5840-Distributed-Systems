package raft

import (
	"sync"
	"time"
)

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

	DPrintf("Svr %d receives request vote message", rf.me)
	term := args.Term
	lastLogIndex := len(rf.log) - 1 + rf.offset
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	// IMPT: the term must be synchronized (whether reject the vote request or not)
	// this must before the following up-to-date check
	// because if I am more up-to-date but my term lags behind, I can not finally become the leader
	if term > rf.currentTerm {
		rf.convert2Follower(term)
	}

	if term < rf.currentTerm || args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		// reject the request if I am more up-to-date
		DPrintf("Svr %d Reject the vote request from svr ?", rf.me)
		if term < rf.currentTerm {
			DPrintf("my term is %d, candidate term is %d", term, rf.currentTerm)
		} else {
			DPrintf("my last log term is %d, candidate last log term is %d", lastLogTerm, args.LastLogTerm)
			DPrintf("my log index is %d, candidate log index is %d", lastLogIndex, args.LastLogIndex)
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = 0
		return
	}

	reply.Term = rf.currentTerm
	if rf.votedFor == -1 {
		// can vote for the requesting candidate
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = 1
	} else {
		reply.VoteGranted = 0
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
	lastLogIndex := rf.offset - 1 + len(rf.log)
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()
	var voteMutex sync.Mutex

	DPrintf("Svr %d starts collecting votes", rf.me)
	for i := 0; i < rf.numPeers; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
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
				DPrintf("Svr %d Get Votes from svr %d", me, server)
				if voteCount*2 > rf.numPeers && !successed {
					// get the majority of votes
					successed = true
					rf.convert2Leader()
				}
				voteMutex.Unlock()
			}
		}(i)
	}
}
