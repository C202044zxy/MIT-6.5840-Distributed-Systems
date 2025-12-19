package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
	Offset       int
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
	// Reject stale term immediately.
	if term < rf.currentTerm {
		// reject the request
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// Step down for newer or same-term leader and reset election timer.
	if term > rf.currentTerm || rf.state != Follower {
		rf.convert2Follower(term)
	}
	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex == -1 {
		// heartbeat message (no log append)
		reply.Success = true
		return
	}

	// append entries message
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommit := args.LeaderCommit
	offset := args.Offset

	// Consistency checks with snapshot boundary awareness.
	// Leader is behind follower's snapshot.
	if prevLogIndex < rf.offset-1 {
		reply.Success = false
		reply.NextIndex = rf.offset
		return
	}
	// PrevLogIndex hits the snapshot boundary: verify lastIncludedTerm.
	if prevLogIndex == rf.offset-1 && prevLogTerm != rf.lastIncludedTerm {
		reply.Success = false
		reply.NextIndex = rf.offset
		return
	}
	// PrevLogIndex within current log range?
	if prevLogIndex >= rf.offset+len(rf.log) {
		reply.Success = false
		reply.NextIndex = rf.offset + len(rf.log)
		return
	}
	if prevLogIndex >= rf.offset {
		// log term must match
		if prevLogTerm != rf.log[prevLogIndex-rf.offset].Term {
			reply.Success = false
			// backtrack to first index of the conflicting term
			invalidTerm := rf.log[prevLogIndex-rf.offset].Term
			rollback := prevLogIndex - rf.offset
			for rollback >= 0 && rf.log[rollback].Term == invalidTerm {
				rollback--
			}
			reply.NextIndex = max(offset, rollback+1+rf.offset)
			return
		}
	}

	// Apply the new entries, truncating any conflicting suffix.
	insertPoint := prevLogIndex + 1 - rf.offset
	if insertPoint < 0 {
		insertPoint = 0
	}
	if insertPoint < len(rf.log) {
		rf.log = append(rf.log[:insertPoint], args.Entries...)
	} else {
		rf.log = append(rf.log, args.Entries...)
	}
	rf.persist()

	reply.Success = true
	if leaderCommit > rf.commitIndex {
		newCommitIndex := min(leaderCommit, len(rf.log)-1+rf.offset)
		rf.commitIndex = max(rf.offset-1, newCommitIndex)
	}
}

func (rf *Raft) heartbeat() {
	// heartbeat period 150 ms
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
		me := rf.me
		rf.mu.Unlock()

		for i := 0; i < rf.numPeers; i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     me,
					PrevLogIndex: -1,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
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
			}(i)
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
		rf.mu.Lock()
		// check that state hasn't changed
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		// lab(3D): only solve index >= offset. so max is applied
		me := rf.me
		log := rf.log
		// IMPT: nextIndex := rf.nextIndex just copies reference
		for i := 0; i < rf.numPeers; i++ {
			rf.nextIndex[i] = max(rf.nextIndex[i], rf.offset)
			rf.nextIndex[i] = min(rf.nextIndex[i], rf.offset+len(rf.log))
		}
		nextIndex := CopySlice(rf.nextIndex)
		commitIndex := rf.commitIndex
		offset := rf.offset
		lastIncludedTerm := rf.lastIncludedTerm
		rf.mu.Unlock()

		for i := 0; i < rf.numPeers; i++ {
			if i == rf.me || len(log) == 0 {
				continue
			}
			// send request even if no new log to append
			// since this RPC does distribute commit information
			go func(server int) {
				prev := nextIndex[server] - 1
				prevLogTerm := lastIncludedTerm
				if prev >= offset && prev-offset < len(log) {
					prevLogTerm = log[prev-offset].Term
				}
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     me,
					PrevLogIndex: prev,
					PrevLogTerm:  prevLogTerm,
					Entries:      log[nextIndex[server]-offset:],
					LeaderCommit: commitIndex,
					Offset:       offset,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
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
					rf.nextIndex[server] = len(log) + offset
					rf.matchIndex[server] = len(log) - 1 + offset
				} else {
					// decrease the nextIndex
					rf.nextIndex[server] = reply.NextIndex
				}
			}(i)
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}
