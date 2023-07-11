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
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// server states
const (
	MaxElapse = 2000		// milliseconds
	MinElapse = 800			// milliseconds
	HeartbeatElapse = 50	// milliseconds
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

	state		int

	// Persistent state on all servers:
	currentTerm	int		// latest term server has seen (initialized to 0 on first boot, increaes monotonically)
	votedFor	int		// candidatedId that received vote in current term (or null if none)
	logs		[]LogEntry	// log entries

	// Volatile state on all servers:
	commitIndex	int		// index of highest log entry known to be commited (initialized to 0, increaes monotonically)
	lastApplied int		// index of highest log entry applied to state machine (initialized to 0, increaes monotonically)

	// Volatile state on leaders (Reinitialized after election):
	nextIndex	[]int	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex	[]int	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increaes monotonically)

	// Others
	applyCh				chan ApplyMsg
	electionTimer		*time.Timer
	heartbeatTimer		*time.Timer
	votesGranted		int
}

type LogEntry struct {
	Index	int
	Term	int
	Command interface{}
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	Term	int		// current term, for leader to update itself
	Success bool	// true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Server [%d] (term %d) received an vote request from server [%d] at term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm < args.Term || rf.votedFor == -1|| rf.votedFor == args.CandidateId {
		// if candidate’s log is not up-to-date as receiver’s log, reply false (according to Figure 2.3.3.2)
		if rf.lastLogTerm() > args.LastLogTerm {
			return
		} else if rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex {
			return
		}
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
		rf.resetElectionTimer()
		// todo: candidate’s log should be at least as up-to-date as receiver’s log
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Server [%d] (term %d) received an AppendEntries from server [%d] at term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = true
	if rf.currentTerm > args.Term {
		reply.Success = false  // Reply false if term < currentTerm (according to Figure 2.2.3.1)
		return
	} 

	// If the leader’s term (included in its RPC) is at least as large as the candidate’s current term
	// recognizes the leader as legitimate
	rf.currentTerm = args.Term
	rf.state = FOLLOWER  // returns to follower state
	rf.resetElectionTimer()

	// Consistency Check
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		//  if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm,
		reply.Success = false  // reply false (according to Figure 2.2.3.2)
		return 
	}

	// Append new entires
	if args.Entries != nil {  
		rf.logs = append(rf.logs[:args.PrevLogIndex + 1], args.Entries...)
	} 
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.logs[len(rf.logs) - 1].Term
}


// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) broadcastAppendEntries(heartbeat bool) {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int, heartbeat bool) {
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
			}
			if heartbeat {
				args.PrevLogIndex = rf.commitIndex
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			} else {
				args.PrevLogIndex = rf.matchIndex[server]  // index of highest log entry known to be replicated on server
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				args.Entries = rf.logs[rf.nextIndex[server]:]
			}
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)
		}(server, heartbeat)
	}
	rf.heartbeatTimer = time.NewTimer(time.Millisecond * 200)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
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
	index = rf.commitIndex + 1
	term = rf.currentTerm
	isLeader = (rf.state == LEADER)

	if isLeader {
		rf.logs = append(rf.logs, LogEntry{
			Index: len(rf.logs),
			Term: rf.currentTerm,
			Command: command,
		})
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

// 2A
func (rf *Raft)resetElectionTimer() {
	elapse := int64(MinElapse) + int64(rand.Intn(MaxElapse - MinElapse))
	rf.electionTimer.Reset(time.Duration(elapse) * time.Millisecond)
}

// 2A
func (rf *Raft)resetHeartbeatTimer() {
	rf.heartbeatTimer.Reset(time.Duration(HeartbeatElapse) * time.Millisecond)
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


// 2A
func (rf *Raft) electLeader() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.state = CANDIDATE
	rf.votedFor = rf.me  // rf.me never change, so no need to avoid race
	rf.votesGranted = 1
	term := rf.currentTerm  // record currentTerm to avoid race
	log.Printf("Server [%d] started an election at term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	// broadcast RequestVote RPC
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			log.Printf("Server [%d] send a vote request to server [%d]", rf.me, server)
			args := RequestVoteArgs{
				Term: term,
				CandidateId: rf.me,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm: rf.lastLogTerm(),
			}
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state == CANDIDATE && reply.VoteGranted {
				// Tally votes, if it were still a Candidate
				log.Printf("Server [%d] receive a vote from server [%d]", rf.me, server)
				rf.votesGranted += 1
				if rf.votesGranted > len(rf.peers) / 2{
					log.Printf("Server [%d] becomes LEADER at term %d", rf.me, rf.currentTerm)
					rf.state = LEADER
					for i, _ := range rf.peers {
						rf.nextIndex[i] = len(rf.logs)  // reinitialize to last log + 1 (the first log's index is 1, the last log's index is len(logs) - 1)
						rf.matchIndex[i] = 0  // reinitialize to 0
					}
					go rf.broadcastAppendEntries(true)  // heartbeat
				}
			} else if rf.state == CANDIDATE && reply.Term > rf.currentTerm {
				// log.Printf("Server [%d] find a new leader server [%d] in term %d", rf.me, server, reply.Term)
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
			}
		}(server)
	}
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

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
