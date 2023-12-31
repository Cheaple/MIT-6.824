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
	Op		Opr
	ClientId	int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate 		int 	// snapshot if log grows this big

	lastApplied				int
	clientLastCommand		map[int64]int		// each client's last command's id
	clientCommandReply		map[int64]CommandReply  // each client's last command's respond
	
	// Your definitions here.
	notifyChan		map[int]chan CommandReply 	// channels used to notify clients after logs get applied

	// Key-Value Store
	kvStore KVStore
}

func (kv *KVServer) CommandHandler(args *CommandArgs, reply *CommandReply) {
	log.Printf("Server [%d] receives a command from client [%d]: %+v", kv.me, args.ClientId, *args)

	// Check if leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		log.Printf("   Not Leader, drop")
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// Check duplicate command
	if lastCommand, ok := kv.clientLastCommand[args.ClientId]; ok && lastCommand >= args.CommandId {
		log.Printf("   Server [%d]: Duplicate client command, drop", kv.me)
		reply.Value = kv.clientCommandReply[args.ClientId].Value
		reply.Err = kv.clientCommandReply[args.ClientId].Err
		kv.mu.Unlock()
		return
	}
	kv.clientLastCommand[args.ClientId] = args.CommandId  // to prevent duplicate commands 
	kv.mu.Unlock()

	op := Op{
		Key: args.Key,
		Value: args.Value,
		Op: args.Op,
		ClientId: args.ClientId,
	}
	i, _, isLeader := kv.rf.Start(op)  // should not be locked, to increase throughput
	log.Printf("Server [%d]: KV Server Starts command %d: %+v", kv.me, i, op)
	if !isLeader {
		// Check Raft leader
		log.Printf("   Not Leader, drop")
		reply.Err = ErrWrongLeader
		// kv.mu.Unlock()
		return
	}
	kv.mu.Lock()
	log.Printf("Server [%d]: KV Server finish Starts command %d: %+v", kv.me, i, op)
	ch := make(chan CommandReply)
	kv.notifyChan[i] = ch
	log.Printf("Server [%d]: prepare notify channel [%d] response", kv.me, i)
	kv.mu.Unlock()

	// Wait until the command got commited and applied by Raft
	select {
	case response := <- ch:
		reply.Err, reply.Value = response.Err, response.Value 
		log.Printf("Server [%d]: KV Server finish command %d: %+v", kv.me, i, reply)
	case <- time.After(TIMEOUT_INTEVAL):
		reply.Err = ErrTimeout
		log.Printf("Server [%d]: KV Server finish command %d: %+v", kv.me, i, reply)
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

// Receive messages from Raft and apply logs to state machine
func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case msg := <- kv.applyCh:
			log.Printf("Server [%d]: KV Server receives an ApplyMsg: %+v", kv.me, msg)
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					log.Printf("   Server [%d]: Duplicate ApplyMsg, drop", kv.me)
					kv.mu.Unlock()
					continue
				}
				cmd := msg.Command.(Op)
				reply := CommandReply{}

				// apply to state machine
				if cmd.Op == GET {
					reply.Err, reply.Value = kv.kvStore.Get(cmd.Key)
				} else if cmd.Op == PUT {
					reply.Err = kv.kvStore.Put(cmd.Key, cmd.Value)
				} else if cmd.Op == APPEND {
					reply.Err = kv.kvStore.Append(cmd.Key, cmd.Value)
				}
				kv.clientLastCommand[cmd.ClientId] = msg.CommandIndex
				kv.clientCommandReply[cmd.ClientId] = reply
				kv.lastApplied = msg.CommandIndex
				ch := kv.notifyChan[msg.CommandIndex]

				// After leader applies a log to the state machine, notify the client
				if _, isLeader := kv.rf.GetState(); isLeader {
					log.Printf("Server [%d]: After applying command [%d], KV Server notifies response: %+v", kv.me, msg.CommandIndex, msg)
					go func() { ch <- reply }()
				}
				kv.mu.Unlock()

				

			}
			if !msg.CommandValid {
				// Snapshot message
				return
			}
		}
	
	}
}

// Apply a log to state machine
// func (kv *KVServer) apply() string {

// }


// Key-Value Store
type KVStore struct {
	data map[string]string
}

func MakeKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (kvStore *KVStore) Get(key string) (Err, string) {
	if value, ok := kvStore.data[key]; ok {
		log.Printf("KVStore Get: kv[%s] = %s", key, kvStore.data[key])
		return OK, value
	}
	log.Printf("KVStore GET: No kv[%s", key)
	return ErrNoKey, ""
}

func (kvStore *KVStore) Put(key string, value string) Err {
	kvStore.data[key] = value
	log.Printf("KVStore Put: kv[%s] = %s", key, kvStore.data[key])
	return OK
}

// Note: non-idempotence
func (kvStore *KVStore) Append(key string, value string) Err {
	kvStore.data[key] += value
	log.Printf("KVStore Append: kv[%s] = %s", key, kvStore.data[key])
	return OK
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
	kv.clientLastCommand = make(map[int64]int)
	kv.clientCommandReply = make(map[int64]CommandReply)
	kv.notifyChan = make(map[int]chan CommandReply)

	kv.kvStore = *MakeKVStore()
	go kv.applier()

	return kv
}
