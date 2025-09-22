package raft

import "6.5840/raftapi"

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
	rf.snapshot = snapshot
	if rf.snapshot == nil {
		rf.offset = 0
		return
	}
	if index > rf.offset {
		// trim the log
		rf.log = rf.log[index-rf.offset:]
		rf.offset = index
	}
	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm
	data := args.Data
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
		rf.log = rf.log[lastIncludedIndex+1-rf.offset:]
	}
	rf.offset = lastIncludedIndex + 1
	rf.snapshot = data
	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  lastIncludedTerm,
		SnapshotIndex: lastIncludedIndex,
	}
	rf.applyCh <- msg
	rf.persist()
}

func (rf *Raft) sendSnapshot() {
	// sendSnapshot
	for !rf.killed() {
		rf.mu.Lock()
		term := rf.currentTerm
		snapshot := rf.snapshot
		offset := rf.offset
	}
}
