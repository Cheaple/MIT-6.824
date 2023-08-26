package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key		string
	Value	string
	Op		string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate 	int 	// snapshot if log grows this big

	// Your definitions here.
	notifyChan		map[int]chan CommandResponse 	// channels used to notify clients after logs get applied
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		Key: args.Key,
		Op: "Get",
	}
	i, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// Check Raft leader
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan CommandResponse, 1)
	kv.mu.Lock()
	kv.notifyChan[i] = ch
	kv.mu.Unlock()

	// Wait until the command got commited and applied by Raft
	select {
	case response := <- ch:
		reply.Err, reply.Value = response.Err, response.Value 
	case <- time.After(TIMEOUT_INTEVAL):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := &Op{
		Key: args.Key,
		Value: args.Value,
		Op: args.Op,
	}
	i, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// Check Raft leader
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan CommandResponse, 1)
	kv.mu.Lock()
	kv.notifyChan[i] = ch
	kv.mu.Unlock()

	// Wait until the command got commited and applied by Raft
	select {
	case response := <- ch:
		reply.Err = response.Err
	case <- time.After(TIMEOUT_INTEVAL):
		reply.Err = ErrTimeout
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case msg := <- kv.applyCh:
			log.Printf("Client receives an ApplyMsg: %+v", msg)
			if msg.CommandValid {
				kv.notifyChan[msg.CommandIndex] <- CommandResponse{
					Err: OK,
				}
			}
			if !msg.CommandValid {
				// Snapshot message
				return
			}
		}
	
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.notifyChan = make(map[int]chan CommandResponse)
	go kv.applier()

	return kv
}
