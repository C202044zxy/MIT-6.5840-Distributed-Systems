package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
	offset       int
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
	offset := args.offset

	if rf.offset+len(rf.log) <= prevLogIndex+offset ||
		(prevLogIndex+offset-rf.offset >= 0 && prevLogTerm != rf.log[prevLogIndex+offset-rf.offset].Term) {
		// no logs at that index or the log is not consistent with leader
		reply.Success = false
		if rf.offset+len(rf.log) <= prevLogIndex+offset {
			reply.NextIndex = max(0, len(rf.log)+rf.offset-offset)
		} else {
			// skip invalid log in a row
			invalidTerm := rf.log[prevLogIndex+offset-rf.offset].Term
			for i := prevLogIndex; i >= 0 && rf.log[i].Term == invalidTerm; i-- {
				reply.NextIndex = max(0, i+rf.offset-offset)
			}
		}
		return
	}
	// prev must be the last committed entry
	// fix(2B): must +1 here, should include log entry at prevLogIndex
	if prevLogIndex+1+offset-rf.offset <= 0 {
		rf.log = make([]Log, 0)
		rf.log = append(rf.log, args.Entries...)
	} else {
		rf.log = append(rf.log[:prevLogIndex+1+offset-rf.offset], args.Entries...)
	}
	rf.persist()
	reply.Success = true
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, len(rf.log)-1+rf.offset)
	}
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
		offset := rf.offset
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
				prev := nextIndex[i] - 1 - offset
				// TODO: should I append all logs at one time?
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     me,
					PrevLogIndex: prev,
					PrevLogTerm:  log[prev].Term,
					Entries:      log[nextIndex[i]-offset:],
					LeaderCommit: commitIndex,
					offset:       offset,
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
