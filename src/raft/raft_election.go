package raft

// 2A: Leader Election

//
// election
//
func (rf *Raft) electLeader() {
	rf.mu.Lock()
	rf.DPrintf("electLeader()")
	defer rf.DPrintf("electLeader() ends")
	rf.currentTerm += 1
	rf.state = CANDIDATE
	rf.votedFor = rf.me  // rf.me never change, so no need to avoid race
	rf.votesGranted = 1
	rf.persist()
	term := rf.currentTerm  // record currentTerm to avoid race
	rf.DPrintf("electLeader(): start an election")
	args := &RequestVoteArgs{
		Term: term,
		CandidateId: rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm: rf.lastLogTerm(),
	}
	rf.mu.Unlock()

	// broadcast RequestVote RPC
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.DPrintf("electLeader(): send a vote request to server [%d]", server)
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, args, &reply)
			if !ok {
				return
			}

			// Handle RequestVote RPC response
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.DPrintf("electLeader(): got a RequestVote reply from server [%d]: %+v", server, reply)
			if rf.state != CANDIDATE || rf.currentTerm != args.Term {
				return
			}
			if reply.VoteGranted {
				// Tally votes, if it were still a Candidate
				rf.DPrintf("electLeader(): receive a vote from server [%d]", server)
				rf.votesGranted += 1
				if rf.votesGranted > len(rf.peers) / 2 {
					rf.DPrintf("electLeader(): switches to LEADER with %d logs", len(rf.logs) - 1)
					rf.state = LEADER
					for i, _ := range rf.peers {
						rf.nextIndex[i] = rf.zeroLogIndex() + len(rf.logs)  // reinitialize to lastlog's index + 1
						rf.matchIndex[i] = 0  // reinitialize to 0
					}
					// rf.nextIndex[rf.me] = rf.lastLogIndex() + 1
					rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
					rf.broadcastAppendEntries(true)  // heartbeat
				}
			} else if reply.Term > rf.currentTerm {
				rf.DPrintf("electLeader(): switches to FOLLOWER (term %d)", reply.Term)
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.resetElectionTimer()
				rf.persist()
			}
		}(server)
	}
}

//
// RPC RequestVote
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// Handler for RequestVote RPC 
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("RequestVote()")
	defer rf.DPrintf("RequestVote() ends")

	rf.DPrintf("RequestVote(): received an vote request from CANDIDATE [%d] (term %d)", args.CandidateId, args.Term)
	rf.DPrintf("RequestVote():     request:  {Candidate: %d, Term: %d, LastLogIndex: %d, LastLogTerm: %d}", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	rf.DPrintf("RequestVote():     curstate: {LastLogIndex: %d, LastLogTerm: %d, CommitId: %d, AppliedId: %d, votedFor: %d}", rf.lastLogIndex(), rf.lastLogTerm(), rf.commitIndex, rf.lastApplied, rf.votedFor)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		rf.DPrintf("RequestVote(): DENY voting for server [%d] (term %d)", args.CandidateId, args.Term)
		return
	}
	if rf.currentTerm < args.Term {
		rf.DPrintf("RequestVote(): switches to FOLLOWER (term %d)", args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		rf.resetElectionTimer()
	}

	if rf.currentTerm < args.Term || rf.votedFor == -1|| rf.votedFor == args.CandidateId {
		// if candidate’s log is not up-to-date as receiver’s log, reply false (according to Figure 2.3.3.2)
		if rf.lastLogTerm() > args.LastLogTerm {
			rf.DPrintf("RequestVote(): DENY voting for server [%d] (term %d)", args.CandidateId, args.Term)
			return
		}
		if rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex {
			rf.DPrintf("RequestVote(): DENY voting for server [%d] (term %d)", args.CandidateId, args.Term)
			return
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.DPrintf("RequestVote(): votes for server [%d] (term %d)", args.CandidateId, args.Term)
	}
}