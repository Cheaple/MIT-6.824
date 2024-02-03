package raft

// 2D: Log Compaction

// Service
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("Snapshot(): index = %d", index)
	defer rf.DPrintf("Snapshot() ends")

	// Trim logs
	zeroIndex := rf.zeroLogIndex()
	rf.DPrintf("Snapshot(): (first index %d) trying to store Snapshot with index %d", zeroIndex, index)
	if index <= zeroIndex {
		rf.DPrintf("Snapshot(): (first index %d) Rejects Snapshot with index %d", zeroIndex, index)
		return
	}  // at least contains 1 entry log
	rf.logs = append([]LogEntry{}, rf.logRange(index, -1)...)
	rf.logs[0].Command = nil  // zombie entry
	rf.persistSnapshot(snapshot)
	rf.DPrintf("Snapshot(): (first index %d) trying to store Snapshot with index %d", zeroIndex, index)
	rf.DPrintf("Snapshot(): finished Snapshot: (%d, %d]", rf.zeroLogIndex(), rf.lastLogIndex())
}

//
// RPC InstallSnapshot
//
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// Broadcast InstallSnapshot RPC to all servers
//
func (rf *Raft) broadcastInstallSnapshot(heartbeat bool) {
	rf.DPrintf("broadcastInstallSnapshot(): heartbeat = %t", heartbeat)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendInstallSnapshotRequest(server)
	}
}

//
// Send a InstallSnapshot RPC to a single server
//
func (rf *Raft) sendInstallSnapshotRequest(server int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()  
		return  // make sure that the sender is still a LEADER
	}
	rf.DPrintf("sendInstallSnapshotRequest(): server = %d", server)
	defer rf.DPrintf("sendInstallSnapshotRequest() ends")

	// Send the entire Snapshot in a single RPC.
	// No implement Figure 13's offset mechanism.
	args := &InstallSnapshotArgs{
		Term: 				rf.currentTerm,
		LeaderId: 			rf.me,
		LastIncludedIndex:	rf.zeroLogIndex(),
		LastIncludedTerm:	rf.zeroLogTerm(),
		Offset:				0,
		Data:				rf.persister.snapshot,
		Done:				true,
	}
	reply := &InstallSnapshotReply{}
	rf.DPrintf("sendInstallSnapshotRequest():    Args: LastIncludeIndex: %d, LastIncludeTerm: %d", args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(server, args, reply)
	if !ok {
		return
	}

	// Handle InstallSnapshot response (according to Figure 2.4.4.3)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		rf.DPrintf("sendInstallSnapshotRequest(): switches to FOLLOWER (term %d)", reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		rf.resetElectionTimer()
		return
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.DPrintf("sendInstallSnapshotRequest(): got an InstallSnapshot reply from server [%d]: %+v", server, reply)
	rf.DPrintf("sendInstallSnapshotRequest():     nextIndex: %#v", rf.nextIndex)
	rf.DPrintf("sendInstallSnapshotRequest():     matchIndex: %#v", rf.matchIndex)
}

//
// Handler for InstallSnapshot RPC
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (2A, 2B).
	rf.DPrintf("InstallSnapshot(): received an InstallSnapshot RPC from server [%d] at term %d", args.LeaderId, args.Term)
	defer rf.DPrintf("InstallSnapshot() ends")
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("InstallSnapshot():     Args: LastIncludeIndex: %d, LastIncludeTerm: %d", args.LastIncludedIndex, args.LastIncludedTerm)
	rf.DPrintf("InstallSnapshot():     {Term: %d, zeroIndex: %d, LastLogIndex: %d, LastLogTerm: %d, CommitId: %d, AppliedId: %d, votedFor: %d}", rf.currentTerm, rf.zeroLogIndex(), rf.lastLogIndex(), rf.lastLogTerm(), rf.commitIndex, rf.lastApplied, rf.votedFor)
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		rf.DPrintf("InstallSnapshot(): DENY InstallSnapshot of LEADER [%d] (term %d)", args.LeaderId, args.Term)
		return
	}

	// If the leader’s term (included in its RPC) is at least as large as the candidate’s current term
	// recognizes the leader as legitimate
	rf.DPrintf("InstallSnapshot(): switches to FOLLOWER (term %d)", reply.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = FOLLOWER  // returns to follower state
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.commitIndex {
		// if the local state is newer than the snapshot, no need to store this snapshot
		rf.DPrintf("InstallSnapshot(): (commitIndex index %d) not need a Snapshot with index %d", rf.commitIndex, args.LastIncludedIndex)
		return
	}

	// Trim logs
	if args.LastIncludedIndex > rf.lastLogIndex() {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = append([]LogEntry{}, rf.logRange(args.LastIncludedIndex, -1)...)
	}
	rf.logs[0].Command = nil  // zombie entry
	rf.logs[0].Index = args.LastIncludedIndex
	rf.logs[0].Term = args.LastIncludedTerm
	rf.commitIndex, rf.lastApplied = args.LastIncludedIndex, args.LastIncludedIndex
	rf.persistSnapshot(args.Data)
	rf.applyMu.Lock()
	func() {
		rf.applyCh <- ApplyMsg {
			CommandValid: 	false,
			SnapshotValid: 	true,
			Snapshot: 		args.Data,
			SnapshotTerm: 	args.LastIncludedTerm,
			SnapshotIndex: 	args.LastIncludedIndex,
		}
	}()  // synchronously install snapshot to the client
	rf.DPrintf("InstallSnapshot(): applied InstallSnapshot (%d, ...]: %+v", args.LastIncludedIndex, args.Data)
	rf.applyMu.Unlock()
}

// Service
// install a local snapshot to raft server entirely
func (rf *Raft) CondInstallSnapshot (lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.DPrintf("CondInstallSnapshot(): received an condInstallSnapshot (term %d, index %d) from client", lastIncludedTerm, lastIncludedIndex)
	defer rf.DPrintf("CondInstallSnapshot() ends")

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		// if the local state is newer than the snapshot, no need to store this snapshot
		rf.DPrintf("CondInstallSnapshot(): (commitIndex index %d) not need a Snapshot with index %d", rf.commitIndex, lastIncludedIndex)
		return false
	}

	if lastIncludedIndex > rf.lastLogIndex() {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = append([]LogEntry{}, rf.logRange(lastIncludedIndex, -1)...)
	}
	rf.logs[0].Command = nil  // zombie entry
	rf.logs[0].Index = lastIncludedIndex
	rf.logs[0].Term = lastIncludedTerm
	rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex
	rf.persistSnapshot(snapshot)
	rf.DPrintf("CondInstallSnapshot(): finished CondInstallSnapshot: (%d, %d]", lastIncludedIndex, lastIncludedIndex + rf.lastLogIndex())
	return true
}
