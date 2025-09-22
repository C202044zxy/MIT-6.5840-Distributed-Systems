package raft

import (
	"bytes"

	"6.5840/labgob"
)

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshot)
	e.Encode(rf.offset)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tmpTerm int
	var tmpVotedFor int
	var tmpLog []Log
	var tmpSnapshot []byte
	var tmpOffset int
	var tmpLastIncludedTerm int
	if d.Decode(&tmpTerm) != nil || d.Decode(&tmpVotedFor) != nil ||
		d.Decode(&tmpLog) != nil || d.Decode(&tmpSnapshot) != nil ||
		d.Decode(&tmpOffset) != nil || d.Decode(&tmpLastIncludedTerm) != nil {
		DPrintf("Err: Decode persist state failed")
		return
	} else {
		rf.currentTerm = tmpTerm
		rf.votedFor = tmpVotedFor
		rf.log = tmpLog
		rf.snapshot = tmpSnapshot
		rf.offset = tmpOffset
		rf.lastIncludedTerm = tmpLastIncludedTerm
		// set some volatile state

	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
