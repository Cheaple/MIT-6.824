package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// server states
const (
	FOLLOWER = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER = "LEADER"
)

// server states
const (
	MaxElapse = 500		// milliseconds
	MinElapse = 100			// milliseconds
	HeartbeatElapse = 30	// milliseconds
)


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state		string

	// Persistent state on all servers:
	currentTerm	int		// latest term server has seen (initialized to 0 on first boot, increaes monotonically)
	votedFor	int		// candidatedId that received vote in current term (or null if none)
	logs		[]LogEntry	// log entries
	// the first entry is (LastSnapshotIndex, LastSnapshotTerm, nil)

	// Volatile state on all servers:
	commitIndex	int		// index of highest log entry known to be commited (initialized to 0, increaes monotonically)
	lastApplied int		// index of highest log entry applied to state machine (initialized to 0, increaes monotonically)

	// Volatile state on leaders (Reinitialized after election):
	nextIndex	[]int	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex	[]int	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increaes monotonically)

	// Others
	applyCh				chan ApplyMsg
	applyCond      		*sync.Cond
	electionTimer		*time.Timer
	heartbeatTimer		*time.Timer
	votesGranted		int

	// for Log Compaction (Snapshot)
}

type LogEntry struct {
	Index	int
	Term	int
	Command interface{}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A)
	Term		int		// currentTerm, for candidate to update itself
	VoteGranted	bool	// true means candidate received vote
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int		// current term, for leader to update itself
	Success 		bool	// true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm 	int		// term of the conflicting entry 
	ConflictIndex	int		// the first index it stores for that term

	// 2D
	Reset			bool
}

type InstallSnapshotArgs struct {
	Term				int
	LeaderId			int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Offset				int		// byte offset where chunk is positioned in the snapshot file
	Data				[]byte		// raw bytes of the snapshot chunk, starting at offset
	Done				bool		// true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term		int
}

func (rf *Raft) zeroLogIndex() int {
	return rf.logs[0].Index
}

func (rf *Raft) zeroLogTerm() int {
	return rf.logs[0].Term
}

func (rf *Raft) lastLogIndex() int {
	return rf.logs[len(rf.logs) - 1].Index
}

func (rf *Raft) lastLogTerm() int {
	return rf.logs[len(rf.logs) - 1].Term
}

func (rf *Raft) logAt(idx int) LogEntry {
	log.Printf("     logAt: %d - %d", idx, rf.logs[0].Index)
	return rf.logs[idx - rf.logs[0].Index]
}

func (rf *Raft) logRange(lo int, hi int) []LogEntry {
	if lo == -1 {
		return append([]LogEntry{}, rf.logs[: hi - rf.logs[0].Index]...)
	}
	if hi == -1 {
		return append([]LogEntry{}, rf.logs[lo - rf.logs[0].Index: ]...)
	}
	return append([]LogEntry{}, rf.logs[lo - rf.logs[0].Index: hi - rf.logs[0].Index]...)
}

// 2A
// Leader Election
func (rf *Raft) electLeader() {
	rf.mu.Lock()
	log.Printf("%s [%d] (term %d) switch to CANDIDATE (term %d)", rf.state, rf.me, rf.currentTerm, rf.currentTerm + 1)
	rf.currentTerm += 1
	rf.state = CANDIDATE
	rf.votedFor = rf.me  // rf.me never change, so no need to avoid race
	rf.votesGranted = 1
	rf.persist()
	term := rf.currentTerm  // record currentTerm to avoid race
	log.Printf("Candidate [%d] started an election at term %d", rf.me, rf.currentTerm)
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
			log.Printf("%s [%d] send a vote request to server [%d]", rf.state, rf.me, server)
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, args, &reply)
			if !ok {
				return
			}

			// Handle RequestVote RPC response
			rf.mu.Lock()
			defer rf.mu.Unlock()
			log.Printf("%s [%d] (term %d) got a RequestVote reply from server [%d]: %+v", rf.state, rf.me, rf.currentTerm, server, reply)
			if rf.state != CANDIDATE || rf.currentTerm != args.Term {
				return
			}
			log.Printf("Leader [%d] (term %d) handles a RequestVote reply from server [%d]: %+v", rf.me, rf.currentTerm, server, reply)
			if reply.VoteGranted {
				// Tally votes, if it were still a Candidate
				log.Printf("%s [%d] receive a vote from server [%d]", rf.state, rf.me, server)
				rf.votesGranted += 1
				if rf.votesGranted > len(rf.peers) / 2 {
					log.Printf("%s [%d] becomes LEADER at term %d with %d logs", rf.state, rf.me, rf.currentTerm, len(rf.logs) - 1)
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
				log.Printf("%s [%d] (term %d) switch to FOLLOWER (term %d)", rf.state, rf.me, rf.currentTerm, reply.Term)
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.resetElectionTimer()
				rf.persist()
				log.Printf("%s [%d] (term %d) switch to FOLLOWER (term %d)", rf.state, rf.me, rf.currentTerm, reply.Term)
			}
		}(server)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Handler for RequestVote RPC 
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%s [%d] (term %d) received an vote request from server [%d] at term %d", rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term)
	log.Printf("    Args: {Candidate: %d, Term: %d, LastLogIndex: %d, LastLogTerm: %d}", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	log.Printf("    %s [%d]: {Term: %d, LastLogIndex: %d, LastLogTerm: %d, CommitId: %d, AppliedId: %d, votedFor: %d}", rf.state, rf.me, rf.currentTerm, rf.lastLogIndex(), rf.lastLogTerm(), rf.commitIndex, rf.lastApplied, rf.votedFor)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		log.Printf("%s [%d] (term %d) DENIed voting for server [%d] (term %d)", rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}
	if rf.currentTerm < args.Term {
		log.Printf("%s [%d] (term %d) switch to FOLLOWER (term %d)", rf.state, rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		rf.resetElectionTimer()
	}

	if rf.currentTerm < args.Term || rf.votedFor == -1|| rf.votedFor == args.CandidateId {
		// if candidate’s log is not up-to-date as receiver’s log, reply false (according to Figure 2.3.3.2)
		if rf.lastLogTerm() > args.LastLogTerm {
			log.Printf("%s [%d] (term %d) DENIed voting for server [%d] (term %d)", rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term)
			return
		}
		if rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex {
			log.Printf("%s [%d] (term %d) DENIed voting for server [%d] (term %d)", rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term)
			return
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		log.Printf("%s [%d] (term %d) votes for server [%d] (term %d)", rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}
}



// 2B
// Log
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// Broadcast AppendEntries RPC to all servers
//
func (rf *Raft) broadcastAppendEntries(heartbeat bool) {
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
	log.Printf("Leader [%d] try to send a AppendEntries RPC to server [%d]", rf.me, server)
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

	log.Printf("Leader [%d] send a AppendEntries RPC to server [%d]", rf.me, server)
	// if heartbeat {
	// 	log.Printf("     HEARTBEAT!")
	// }
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
	log.Printf("    Args: {Leader: %d, Term: %d, PrevLogIndex: %d, PrevLogTerm: %d, leaderCommit: %d, new entries count: %d}", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
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
	log.Printf("Leader [%d] (term %d) got a reply from server [%d]: %+v", rf.me, rf.currentTerm, server, reply)
	if rf.currentTerm < reply.Term {
		log.Printf("LEADER [%d] (term %d) switch to FOLLOWER (term %d)", rf.me, rf.currentTerm, reply.Term)
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
		log.Printf("Leader [%d] (term %d) got a RESET reply from server [%d]", rf.me, rf.currentTerm, server)
		log.Printf("    nextIndex: %#v", rf.nextIndex)
		log.Printf("    matchIndex: %#v", rf.matchIndex)
		go rf.sendAppendEntriesRequest(server, heartbeat)
	} else if reply.Success {
		// If successful: update nextIndex and matchIndex for follower
		// log.Printf("%s [%d] receive a success from server [%d]; update nextIndex[%d] from %d to %d", rf.me, server, server, rf.nextIndex[server], args.PrevLogIndex + len(args.Entries) + 1)
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		log.Printf("Leader [%d] (term %d) got a SUCCESS reply from server [%d]", rf.me, rf.currentTerm, server)
		log.Printf("    nextIndex: %#v", rf.nextIndex)
		log.Printf("    matchIndex: %#v", rf.matchIndex)
		
		if rf.commitIndex < rf.matchIndex[server] {  // if the newest replicated entry has not been committed
			rf.tryCommit(rf.matchIndex[server])  // try to commit it
		}
	} else if !reply.Success && reply.Term <= args.Term {
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		log.Printf("Leader [%d] (term %d) got a NON-SUCCESS reply from server [%d]", rf.me, rf.currentTerm, server)
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server] -= 1  // 2B 
		}  // nextIndex >= 1
		log.Printf("Leader [%d] (term %d): nextIndex: %#v", rf.me, rf.currentTerm, rf.nextIndex)

		// 2C Optimization: bypass all conflicting entries in that conflict term
		if reply.ConflictIndex > 0 {
			if reply.ConflictIndex + 1 < rf.nextIndex[server] {
				rf.nextIndex[server] = reply.ConflictIndex
				i := rf.nextIndex[server]
				for ; i > rf.zeroLogIndex() && rf.logAt(i).Term == reply.ConflictTerm; i -= 1 {}
				rf.nextIndex[server] = i  // new entries starts from the conflicting entry
			}
		}
		log.Printf("Leader [%d] (term %d): nextIndex: %#v", rf.me, rf.currentTerm, rf.nextIndex)

		if rf.nextIndex[server] > rf.zeroLogIndex() {
			go rf.sendAppendEntriesRequest(server, false)
		} else {
			// 2D: Send an InstallSnapshot RPC
			// if it doesn't have the log entries required to bring a follower up to date
			go rf.sendInstallSnapshotRequest(server)
		}
	}  
}


// Commit a log
func (rf *Raft) tryCommit(logIndex int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// if rf.logAt(logIndex).Term != rf.currentTerm {
	// 	return  // according to Figure 2.4.4.4
	// }
	cnt := 0  // count the number of this log entry' replications on all servers
	for _, matchIdx := range rf.matchIndex {
		if matchIdx >= logIndex {
			cnt += 1 
		}
	}
	log.Printf("    log %d has been replicated to %d servers.", logIndex, cnt)
	if cnt > len(rf.peers) / 2 {
		// if the log entry has been replicated it on a majority of the servers
		rf.commitIndex = logIndex  // commit it
		log.Printf("Leader [%d] (term %d) commits log %d", rf.me, rf.currentTerm, logIndex)
	}
	rf.broadcastAppendEntries(false)  // must broadcast this newly committed entry immediately
	rf.applyCond.Broadcast()
	// Then, newly committed log entries will be applied to the client asynchronously
}


// Handler for AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%s [%d] (term %d) received an AppendEntries from server [%d] at term %d", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
	log.Printf("    Args: {Leader: %d, Term: %d, PrevLogIndex: %d, PrevLogTerm: %d, leaderCommit: %d, new entries count: %d}", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	log.Printf("    %s [%d]: {Term: %d, zeroIndex: %d, LastLogIndex: %d, LastLogTerm: %d, CommitId: %d, AppliedId: %d, votedFor: %d}", rf.state, rf.me, rf.currentTerm, rf.zeroLogIndex(), rf.lastLogIndex(), rf.lastLogTerm(), rf.commitIndex, rf.lastApplied, rf.votedFor)
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
	log.Printf("%s [%d] (term %d) switch to FOLLOWER (term %d)", rf.state, rf.me, rf.currentTerm, args.Term)
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
		log.Printf("    %s [%d] Reply: {ConflictTerm: %d, ConflictIndex: %d}", rf.state, rf.me, reply.ConflictTerm, reply.ConflictIndex)	
		return
	} else if rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		//  if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm,
		reply.Success = false	
		reply.ConflictTerm = rf.logAt(args.PrevLogIndex).Term
		i := args.PrevLogIndex
		for ; i > rf.zeroLogIndex() && rf.logAt(i).Term == reply.ConflictTerm; i -= 1 {}
		reply.ConflictIndex = i + 1
		log.Printf("    %s [%d] Reply: {ConflictTerm: %d, ConflictIndex: %d}", rf.state, rf.me, reply.ConflictTerm, reply.ConflictIndex)	
		return 
	}

	// Delete conflicting entries and all the following entries
	for i, entry := range args.Entries {
		if args.PrevLogIndex + 1 + i <= rf.lastLogIndex() && entry.Term != rf.logAt(args.PrevLogIndex + 1 +i).Term {
			rf.logs = append([]LogEntry{}, rf.logRange(-1, args.PrevLogIndex + 1 + i)...)
			log.Printf("Follower [%d] cut conflicting entries", rf.me)
			log.Printf("    %v", rf.logs)
			break
		}
	}

	// Append new entires not in the logs
	if args.Entries != nil {
		if args.PrevLogIndex + len(args.Entries) > rf.lastLogIndex() {
			rf.logs = append(rf.logRange(-1, args.PrevLogIndex + 1), args.Entries...)
			rf.persist()
			log.Printf("Follower [%d] append %d new entries", rf.me, len(args.Entries))
			// log.Printf("    %v", rf.logs)
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
		log.Printf("%s [%d] (term %d) commits log %d", rf.state, rf.me, rf.currentTerm, rf.commitIndex)
		rf.applyCond.Broadcast()
	}

}



// 2C
// Persistence
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
	log.Printf("%s [%d] (term %d) store persist.", rf.state, rf.me, rf.currentTerm)
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	// Your code here (2C).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.persister.Save(rf.stateEncoded(), snapshot)
	log.Printf("%s [%d] (term %d) store persist (state & snapshot).", rf.state, rf.me, rf.currentTerm)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	log.Printf("%s [%d] (term %d) read persist.", rf.state, rf.me, rf.currentTerm)
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
	log.Printf("    %s [%d]: {Term: %d, LastLogIndex: %d, CommitId: %d, AppliedId: %d, votedFor: %d, zeroIndex: %d, zeroTerm: %d}", rf.state, rf.me, rf.currentTerm, rf.lastLogIndex(), rf.commitIndex, rf.lastApplied, rf.votedFor, rf.zeroLogIndex(), rf.zeroLogTerm())
	log.Printf("    %v", rf.logs)
}



// 2D
// Log Compaction

// Service
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	log.Printf("%s [%d] (term %d) Snapshots %d", rf.state, rf.me, rf.currentTerm, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Trim logs
	zeroIndex := rf.zeroLogIndex()
	log.Printf("%s [%d] (first index %d) trying to store Snapshot with index %d", rf.state, rf.me, zeroIndex, index)
	if index <= zeroIndex {
		log.Printf("%s [%d] (first index %d) rejects Snapshot with index %d", rf.state, rf.me, zeroIndex, index)
		return
	}  // at least contains 1 entry log
	rf.logs = append([]LogEntry{}, rf.logRange(index, -1)...)
	rf.logs[0].Command = nil  // zombie entry
	rf.persistSnapshot(snapshot)
	log.Printf("%s [%d] (first index %d) store Snapshot with index %d", rf.state, rf.me, zeroIndex, index)
	log.Printf("%s [%d] finished Snapshot: (%d, %d]", rf.state, rf.me, rf.zeroLogIndex(), rf.lastLogIndex())
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// Broadcast InstallSnapshot RPC to all servers
//
func (rf *Raft) broadcastInstallSnapshot(heartbeat bool) {
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
	log.Printf("Leader [%d] send a InstallSnapshot RPC to server [%d]", rf.me, server)

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
	log.Printf("    Args: LastIncludeIndex: %d, LastIncludeTerm: %d", args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(server, args, reply)
	if !ok {
		return
	}

	// Handle InstallSnapshot response (according to Figure 2.4.4.3)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		log.Printf("%s [%d] (term %d) switch to FOLLOWER (term %d)", rf.state, rf.me, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		rf.resetElectionTimer()
		return
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	log.Printf("Leader [%d] (term %d) got an InstallSnapshot reply from server [%d]: %+v", rf.me, rf.currentTerm, server, reply)
	log.Printf("    nextIndex: %#v", rf.nextIndex)
	log.Printf("    matchIndex: %#v", rf.matchIndex)
}

// Handler for InstallSnapshot RPC 
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (2A, 2B).
	log.Printf("%s [%d] (term %d) received an InstallSnapshot RPC from server [%d] at term %d", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("    Args: LastIncludeIndex: %d, LastIncludeTerm: %d", args.LastIncludedIndex, args.LastIncludedTerm)
	log.Printf("    %s [%d]: {Term: %d, zeroIndex: %d, LastLogIndex: %d, LastLogTerm: %d, CommitId: %d, AppliedId: %d, votedFor: %d}", rf.state, rf.me, rf.currentTerm, rf.zeroLogIndex(), rf.lastLogIndex(), rf.lastLogTerm(), rf.commitIndex, rf.lastApplied, rf.votedFor)
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		log.Printf("%s [%d] (term %d) DENIed InstallSnapshot of LEADER [%d] (term %d)", rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	// If the leader’s term (included in its RPC) is at least as large as the candidate’s current term
	// recognizes the leader as legitimate
	log.Printf("%s [%d] (term %d) switch to FOLLOWER (term %d)", rf.state, rf.me, rf.currentTerm, args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = FOLLOWER  // returns to follower state
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.commitIndex {
		// if the local state is newer than the snapshot, no need to store this snapshot
		log.Printf("%s [%d] (commitIndex index %d) not need a Snapshot with index %d", rf.state, rf.me, rf.commitIndex, args.LastIncludedIndex)
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
	func() {
		rf.applyCh <- ApplyMsg {
			CommandValid: 	false,
			SnapshotValid: 	true,
			Snapshot: 		args.Data,
			SnapshotTerm: 	args.LastIncludedTerm,
			SnapshotIndex: 	args.LastIncludedIndex,
		}
	}()  // synchronously install snapshot to the client
	log.Printf("%s [%d] applied InstallSnapshot (%d, ...]: %+v", rf.state, rf.me, args.LastIncludedIndex, args.Data)
}

// Service
// install a local snapshot to raft server entirely
func (rf *Raft) CondInstallSnapshot (lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	log.Printf("%s [%d] (term %d) received an condInstallSnapshot (term %d, index %d) from client", rf.state, rf.me, rf.currentTerm, lastIncludedTerm, lastIncludedIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		// if the local state is newer than the snapshot, no need to store this snapshot
		log.Printf("%s [%d] (commitIndex index %d) not need a Snapshot with index %d", rf.state, rf.me, rf.commitIndex, lastIncludedIndex)
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
	log.Printf("%s [%d] finished CondInstallSnapshot: (%d, %d]", rf.state, rf.me, lastIncludedIndex, lastIncludedIndex + rf.lastLogIndex())
	return true
}




// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if thiscomma
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	isLeader = (rf.state == LEADER)

	if isLeader {
		log.Printf("Leader [%d] (term %d) get a new commmand with an index %d. Now it has %d logs", rf.me, term, index, len(rf.logs))
		rf.logs = append(rf.logs, LogEntry{
			Index: index,
			Term: term,
			Command: command,
		})
		rf.persist()
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.broadcastAppendEntries(false)  // must broadcast this newly appended entry
	}
	

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <- rf.electionTimer.C:
			if rf.state != LEADER { 
				rf.electLeader()
			}
			rf.resetElectionTimer()
		case <- rf.heartbeatTimer.C:
			if rf.state == LEADER {
				rf.broadcastAppendEntries(false)
			}
			rf.resetHeartbeatTimer()
		default:
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Apply committed log entries to the client asynchronously (the process is slow)
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitIndex := rf.commitIndex
		entries := rf.logRange(rf.lastApplied + 1, rf.commitIndex + 1)
		rf.mu.Unlock()
		for _, entry := range entries {
			log.Printf("%s [%d] (term %d) applied log entry %d: %+v", rf.state, rf.me, rf.currentTerm, entry.Index, entry)
			rf.applyCh <- ApplyMsg {
				Command: entry.Command,
				CommandIndex: entry.Index,
				CommandValid: true,
			}
		}

		rf.mu.Lock()
		// Note: in 2D, after Snapshot(), it is possible that the current rf.lastApplied is greater than commitIndex
		if rf.lastApplied < commitIndex {
			rf.lastApplied = commitIndex
		}
		rf.broadcastAppendEntries(false)
		rf.mu.Unlock()
	}

}


func (rf *Raft)resetElectionTimer() {
	elapse := int64(MinElapse) + int64(rand.Intn(MaxElapse - MinElapse))
	rf.electionTimer.Reset(time.Duration(elapse) * time.Millisecond)
}

func (rf *Raft)resetHeartbeatTimer() {
	rf.heartbeatTimer.Reset(time.Duration(HeartbeatElapse) * time.Millisecond)
} 


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)

	return term, isleader
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.electionTimer = time.NewTimer(time.Millisecond * 200)
	rf.resetElectionTimer()
	rf.heartbeatTimer = time.NewTimer(time.Millisecond * 200)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	// start applier to apply commited logs to client
	rf.applyCond = sync.NewCond(&rf.mu)
	go rf.applier()

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
