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
	// consistency check
	reply.Term = rf.currentTerm
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommit := args.LeaderCommit
	offset := args.Offset

	if rf.offset+len(rf.log) <= prevLogIndex ||
		(prevLogIndex-rf.offset >= 0 && prevLogTerm != rf.log[prevLogIndex-rf.offset].Term) {
		// no logs at that index or the log is not consistent with leader
		reply.Success = false
		if rf.offset+len(rf.log) <= prevLogIndex {
			reply.NextIndex = len(rf.log) + rf.offset
		} else {
			// skip invalid log in a row
			invalidTerm := rf.log[prevLogIndex-rf.offset].Term
			for i := prevLogIndex - rf.offset; i >= 0 && rf.log[i].Term == invalidTerm; i-- {
				reply.NextIndex = i + rf.offset
			}
		}
		reply.NextIndex = max(offset, reply.NextIndex)
		return
	}
	// prev must be the last committed entry
	// fix(2B): must +1 here, should include log entry at prevLogIndex
	if prevLogIndex+1-rf.offset <= 0 {
		rf.log = make([]Log, 0)
		rf.log = append(rf.log, args.Entries...)
	} else {
		rf.log = append(rf.log[:prevLogIndex+1-rf.offset], args.Entries...)
	}
	rf.persist()
	reply.Success = true
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, len(rf.log)-1+rf.offset)
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
		nextIndex := CopySlice(rf.nextIndex)
		commitIndex := rf.commitIndex
		offset := rf.offset
		lastIncludedTerm := rf.lastIncludedTerm
		for i := 0; i < rf.numPeers; i++ {
			rf.nextIndex[i] = max(rf.nextIndex[i], rf.offset)
		}
		rf.mu.Unlock()

		for i := 0; i < rf.numPeers; i++ {
			if i == rf.me || len(log) == 0 {
				continue
			}
			// send request even if no new log to append
			// since this RPC does distribute commit information
			go func() {
				prev := nextIndex[i] - 1
				prevLogTerm := lastIncludedTerm
				if prev >= offset {
					prevLogTerm = log[prev-offset].Term
				}
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     me,
					PrevLogIndex: prev,
					PrevLogTerm:  prevLogTerm,
					Entries:      log[nextIndex[i]-offset:],
					LeaderCommit: commitIndex,
					Offset:       offset,
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
					rf.nextIndex[i] = len(log) + offset
					rf.matchIndex[i] = len(log) - 1 + offset
				} else {
					// decrease the nextIndex
					nextIndex[i] = reply.NextIndex
				}
			}()
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}
