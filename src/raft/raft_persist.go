package raft

import (
	"bytes"
	"log"

	"6.5840/labgob"
)


// 2C: Persistence

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) stateEncoded() []byte {
	// Warning: no Lock
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.persister.Save(rf.stateEncoded(), rf.persister.snapshot)
	rf.DPrintf("persist()")
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	// Your code here (2C).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.persister.Save(rf.stateEncoded(), snapshot)
	rf.DPrintf("persistSnapshot()")
}

//
// restore previously persisted state
//
func (rf *Raft) readPersist(data []byte) {
	rf.DPrintf("readPersist()")
	defer rf.DPrintf("readPersist() ends")

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || 
		d.Decode(&votedFor) != nil || 
		d.Decode(&logs) != nil {
		log.Fatal("Error reading persist")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.logs = logs
	}
	rf.commitIndex, rf.lastApplied = rf.logs[0].Index, rf.logs[0].Index
	rf.DPrintf("readPersist():     {Term: %d, LastLogIndex: %d, CommitId: %d, AppliedId: %d, votedFor: %d, zeroIndex: %d, zeroTerm: %d}", rf.currentTerm, rf.lastLogIndex(), rf.commitIndex, rf.lastApplied, rf.votedFor, rf.zeroLogIndex(), rf.zeroLogTerm())
}