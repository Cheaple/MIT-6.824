package raft

// 2B: Log

//
// RPC AppendEntries
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// Broadcast AppendEntries RPC to all servers
//
func (rf *Raft) broadcastAppendEntries(heartbeat bool) {
	rf.DPrintf("broadcastAppendEntries(): heartbeat = %t", heartbeat)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendAppendEntriesRequest(server, heartbeat)
	}
}

//
// Send a AppendEntries RPC to a single server
//
func (rf *Raft) sendAppendEntriesRequest(server int, heartbeat bool) {
	rf.mu.Lock()
	rf.DPrintf("sendAppendEntriesRequest(): server = %d, heartbeat = %t", server, heartbeat)
	defer rf.DPrintf("sendAppendEntriesRequest() ends")

	if rf.state != LEADER {
		rf.mu.Unlock()  
		return  // make sure that the sender is still a LEADER
	}

	// When the leader has already discarded the next log entry that it needs to send to a follower
	// send a snapshot
	if rf.nextIndex[server] <= rf.zeroLogIndex() {
		rf.mu.Unlock()
		go rf.sendInstallSnapshotRequest(server)
		return
	}

	rf.DPrintf("sendAppendEntriesRequest(): sends a AppendEntries RPC to server [%d]", server)
	args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,  // index of log entry immediately preceding new ones
		PrevLogTerm: rf.logAt(rf.nextIndex[server] - 1).Term,
		LeaderCommit: rf.commitIndex,
		Entries: nil,
	}
	if !heartbeat {
		args.LeaderCommit = rf.commitIndex
		args.Entries = append([]LogEntry{}, rf.logRange(rf.nextIndex[server], -1)...)
	}
	reply := &AppendEntriesReply{}
	rf.DPrintf("sendAppendEntriesRequest():     args:  {Leader: %d, Term: %d, PrevLogIndex: %d, PrevLogTerm: %d, leaderCommit: %d, new entries count: %d}", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}

	// Handle AppendEntries response (according to Figure 2.4.4.3)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return
	}
	rf.DPrintf("sendAppendEntriesRequest(): got a reply from server [%d]: %+v", server, reply)
	if rf.currentTerm < reply.Term {
		rf.DPrintf("sendAppendEntriesRequest(): switches to FOLLOWER (term %d)", reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		rf.resetElectionTimer()
	}
	if rf.currentTerm != args.Term {
		return 
	}
	if reply.Reset {
		rf.nextIndex[server] = rf.zeroLogIndex() + len(rf.logs)  // reinitialize to lastlog's index + 1
		rf.DPrintf("sendAppendEntriesRequest(): got a RESET reply from server [%d]", server)
		rf.DPrintf("sendAppendEntriesRequest():     nextIndex: %#v", rf.nextIndex)
		rf.DPrintf("sendAppendEntriesRequest():     matchIndex: %#v", rf.matchIndex)
		go rf.sendAppendEntriesRequest(server, heartbeat)
	} else if reply.Success {
		// If successful: update nextIndex and matchIndex for follower
		// log.Printf("%s [%d] receive a success from server [%d]; update nextIndex[%d] from %d to %d", rf.me, server, server, rf.nextIndex[server], args.PrevLogIndex + len(args.Entries) + 1)
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.DPrintf("sendAppendEntriesRequest(): got a SUCCESS reply from server [%d]", server)
		rf.DPrintf("sendAppendEntriesRequest():     nextIndex: %#v", rf.nextIndex)
		rf.DPrintf("sendAppendEntriesRequest():     matchIndex: %#v", rf.matchIndex)
		
		if rf.commitIndex < rf.matchIndex[server] {  // if the newest replicated entry has not been committed
			rf.tryCommit(rf.matchIndex[server])  // try to commit it
		}
	} else if !reply.Success && reply.Term <= args.Term {
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		rf.DPrintf("sendAppendEntriesRequest(): got a NON-SUCCESS reply from server [%d]", server)
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server] -= 1  // 2B 
		}  // nextIndex >= 1
		rf.DPrintf("sendAppendEntriesRequest():     nextIndex: %#v", rf.nextIndex)


		// 2C Optimization: bypass all conflicting entries in that conflict term
		if reply.ConflictIndex > 0 {
			rf.DPrintf("sendAppendEntriesRequest():  Optimization - bypass all conflicting entries in that conflict term")
			if reply.ConflictIndex + 1 < rf.nextIndex[server] {
				rf.nextIndex[server] = reply.ConflictIndex
				i := rf.nextIndex[server]
				for ; i > rf.zeroLogIndex() && rf.logAt(i).Term == reply.ConflictTerm; i -= 1 {}
				rf.nextIndex[server] = i  // new entries starts from the conflicting entry
			}
		}
		rf.DPrintf("sendAppendEntriesRequest():     nextIndex: %#v", rf.nextIndex)


		if rf.nextIndex[server] > rf.zeroLogIndex() {
			go rf.sendAppendEntriesRequest(server, false)
		} else {
			// 2D: Send an InstallSnapshot RPC
			// if it doesn't have the log entries required to bring a follower up to date
			go rf.sendInstallSnapshotRequest(server)
		}
	}  
}

//
// Commit a log
//
func (rf *Raft) tryCommit(logIndex int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// if rf.logAt(logIndex).Term != rf.currentTerm {
	// 	return  // according to Figure 2.4.4.4
	// }
	rf.DPrintf("tryCommit(): logIndex = %d", logIndex)
	defer rf.DPrintf("tryCommit() ends")

	cnt := 0  // count the number of this log entry' replications on all servers
	for _, matchIdx := range rf.matchIndex {
		if matchIdx >= logIndex {
			cnt += 1 
		}
	}
	rf.DPrintf("tryCommit(): log %d has been replicated to %d servers.", logIndex, cnt)
	if cnt > len(rf.peers) / 2 {
		// if the log entry has been replicated it on a majority of the servers
		rf.commitIndex = logIndex  // commit it
		rf.DPrintf("tryCommit(): commits log %d", logIndex)
	}
	rf.broadcastAppendEntries(false)  // must broadcast this newly committed entry immediately
	rf.applyCond.Broadcast()
	// Then, newly committed log entries will be applied to the client asynchronously
}

//
// Handler for AppendEntries RPC
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("AppendEntries()")
	defer rf.DPrintf("AppendEntries() ends")

	rf.DPrintf("AppendEntries(): received an AppendEntries from server [%d] (term %d)", args.LeaderId, args.Term)
	rf.DPrintf("AppendEntries():     Args:  {Leader: %d, Term: %d, PrevLogIndex: %d, PrevLogTerm: %d, leaderCommit: %d, new entries count: %d}", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	rf.DPrintf("AppendEntries():     curstate: {Term: %d, zeroIndex: %d, LastLogIndex: %d, LastLogTerm: %d, CommitId: %d, AppliedId: %d, votedFor: %d}", rf.currentTerm, rf.zeroLogIndex(), rf.lastLogIndex(), rf.lastLogTerm(), rf.commitIndex, rf.lastApplied, rf.votedFor)
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	reply.Reset = false
	if rf.currentTerm > args.Term {
		reply.Success = false  // Reply false if term < currentTerm (according to Figure 2.2.3.1)
		return
	} 

	// If the leader’s term (included in its RPC) is at least as large as the candidate’s current term
	// recognizes the leader as legitimate
	rf.DPrintf("AppendEntries(): switches to FOLLOWER (term %d)", args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = FOLLOWER  // returns to follower state
	rf.resetElectionTimer()

	// Consistency Check
	if args.PrevLogIndex < rf.zeroLogIndex() {
		// 2D
		reply.Reset = true
		return
	} else if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false  // reply false (according to Figure 2.2.3.2)
		reply.ConflictTerm = rf.lastLogTerm()
		i := rf.lastLogIndex()
		for ; i > rf.zeroLogIndex() && rf.logAt(i).Term == reply.ConflictTerm; i -= 1 {}
		reply.ConflictIndex = i + 1
		rf.DPrintf("AppendEntries(): Reply: {ConflictTerm: %d, ConflictIndex: %d}", reply.ConflictTerm, reply.ConflictIndex)
		return
	} else if rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		//  if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm,
		reply.Success = false	
		reply.ConflictTerm = rf.logAt(args.PrevLogIndex).Term
		i := args.PrevLogIndex
		for ; i > rf.zeroLogIndex() && rf.logAt(i).Term == reply.ConflictTerm; i -= 1 {}
		reply.ConflictIndex = i + 1
		rf.DPrintf("AppendEntries(): Reply: {ConflictTerm: %d, ConflictIndex: %d}", reply.ConflictTerm, reply.ConflictIndex)
		return 
	}

	// Delete conflicting entries and all the following entries
	for i, entry := range args.Entries {
		if args.PrevLogIndex + 1 + i <= rf.lastLogIndex() && entry.Term != rf.logAt(args.PrevLogIndex + 1 +i).Term {
			rf.logs = append([]LogEntry{}, rf.logRange(-1, args.PrevLogIndex + 1 + i)...)
			rf.DPrintf("AppendEntries(): cut conflicting entries")
			// rf.DPrintf("AppendEntries():     logs: %v", rf.logs)
			break
		}
	}

	// Append new entires not in the logs
	if args.Entries != nil {
		if args.PrevLogIndex + len(args.Entries) > rf.lastLogIndex() {
			rf.logs = append(rf.logRange(-1, args.PrevLogIndex + 1), args.Entries...)
			rf.persist()
			rf.DPrintf("AppendEntries(): append %d new entries", len(args.Entries))
			// rf.DPrintf("AppendEntries():     logs: %v", rf.logs)
		}
	}

	// Apply newly committed log enties
	if args.LeaderCommit > rf.commitIndex {
		// set commitIndex = min(leaderCommit, index of last new entry (according to Figure 2.2.3.5)
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
		rf.DPrintf("AppendEntries(): commits log %d", rf.commitIndex)
		rf.applyCond.Broadcast()
	}
}