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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	applyMu				sync.Mutex
	electionTimer		*time.Timer
	heartbeatTimer		*time.Timer
	votesGranted		int

	verbose				bool

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
	// log.Printf("     logAt: %d - %d", idx, rf.logs[0].Index)
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

//
//  Helper function for printng debugging logs
//
func (rf *Raft) DPrintf(str string, args ...interface{}) {
	if rf.verbose != true {
		return
	}
	message := fmt.Sprintf(str, args...)
	log.Printf(fmt.Sprintf("  %s [%d] (term %d) - %s ", rf.state, rf.me, rf.currentTerm, message))
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
		rf.DPrintf("Start(): get a new commmand with an index %d. Now it has %d logs", index, len(rf.logs))
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

//
// Apply committed log entries to the client asynchronously (the process is slow)
//
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitIndex := rf.commitIndex
		entries := rf.logRange(rf.lastApplied + 1, rf.commitIndex + 1)
		rf.mu.Unlock()

		rf.applyMu.Lock()
		for _, entry := range entries {
			rf.DPrintf("applier(): applied log entry %d: %+v", entry.Index, entry)
			rf.applyCh <- ApplyMsg {
				Command: entry.Command,
				CommandIndex: entry.Index,
				CommandValid: true,
			}
		}
		rf.applyMu.Unlock()

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
	rf := &Raft{
		verbose: false,
	}
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