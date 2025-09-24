package raft

import (
	"time"

	"6.5840/raftapi"
)

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
	Succ bool
}

// Discard old log entries in a way that allows
// the Go garbage collector to free and re-use the memory.
// This requires that there be no reachable references (pointers)
// to the discarded log entries.
func TrimFront[T any](s []T, offset int) []T {
	if offset <= 0 {
		return s
	}
	if offset >= len(s) {
		return nil
	}
	newSlice := make([]T, len(s)-offset)
	copy(newSlice, s[offset:])
	return newSlice
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	// TODO(lockzhou): will the snapshot contains the very first item?
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index+1 <= rf.offset {
		// not fast-forward
		return
	}
	rf.snapshot = snapshot
	DPrintf("svr %d receives snapshot message", rf.me)
	if rf.snapshot == nil {
		rf.offset = 0
		rf.lastIncludedTerm = 0
		return
	}
	if index+1 > rf.offset {
		// trim the log
		rf.lastIncludedTerm = rf.log[index-rf.offset].Term
		rf.log = TrimFront(rf.log, index+1-rf.offset)
		rf.offset = index + 1
	}
	DPrintf("Svr %d snapshot succ, lastIncludedIndex = %d", rf.me, rf.offset-1)
	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm
	data := args.Data
	reply.Succ = false
	if term < rf.currentTerm {
		// reject immediately
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = term
	if term > rf.currentTerm {
		rf.convert2Follower(term)
	}
	if data == nil || lastIncludedIndex+1 <= rf.offset {
		// don't move backward
		return
	}
	if lastIncludedIndex+1 >= rf.offset+len(rf.log) ||
		rf.log[lastIncludedIndex+1-rf.offset].Term != lastIncludedTerm {
		// match failed. discard the entire log
		rf.log = make([]Log, 0)
	} else {
		rf.log = TrimFront(rf.log, lastIncludedIndex+1-rf.offset)
	}
	rf.offset = lastIncludedIndex + 1
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = data
	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  lastIncludedTerm,
		SnapshotIndex: lastIncludedIndex,
	}
	rf.lastApplied = max(rf.lastApplied, lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, lastIncludedIndex)
	rf.applyCh <- msg
	rf.persist()
	reply.Succ = true
	DPrintf("Svr %d Install snapshot succ, lastIncludedIndex = %d", rf.me, lastIncludedIndex)
}

func (rf *Raft) sendSnapshot() {
	// sendSnapshot period 150 ms
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if rf.offset == 0 {
			// stop snapshot synchronization temporarily
			rf.mu.Unlock()
			time.Sleep(time.Duration(150) * time.Millisecond)
			continue
		}
		snapshot := rf.snapshot
		offset := rf.offset
		lastIncludedTerm := rf.lastIncludedTerm
		rf.mu.Unlock()
		for i := 0; i < rf.numPeers; i++ {
			if i == rf.me {
				continue
			}
			DPrintf("master %d sends InstallSnapshot to svr %d", rf.me, i)
			go func() {
				args := InstallSnapshotArgs{
					Term:              term,
					LastIncludedIndex: offset - 1,
					LastIncludedTerm:  lastIncludedTerm,
					Data:              snapshot,
				}
				reply := InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(i, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.convert2Follower(reply.Term)
					return
				}
				if reply.Succ {
					rf.matchIndex[i] = max(rf.matchIndex[i], offset-1)
				}
			}()
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}
