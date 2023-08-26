package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "log"
import "sync/atomic"
import "time"



type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	leaderId	int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}
	i := ck.currentServer()

	// keeps trying forever in the face of all other errors.
	for true {
		log.Printf("Client send Get command to leader [%d]", i)
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				log.Printf("Client Get %s: %s", key, reply.Value)
				return reply.Value
			}
		}
		i = ck.nextServer(i)
		time.Sleep(RETRY_INTEVAL)
	}

	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
	}
	i := ck.currentServer()

	// keeps trying forever in the face of all other errors.
	for true {
		log.Printf("Client send PutAppend command to leader [%d]", i)
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				log.Printf("Client %s %s: %s", op, key, value)
				return
			} else if reply.Err == ErrTimeout {
				log.Printf("Client %s %s command timeout", op, key)
			}
		}
		i = ck.nextServer(i)
		time.Sleep(RETRY_INTEVAL)
	}	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) currentServer() int32 {
	return atomic.LoadInt32(&ck.leaderId)
}


func (ck *Clerk) nextServer(current int32) int32 {
	next := (current + 1) % int32(len(ck.servers))
	atomic.StoreInt32(&ck.leaderId, next)
	return next
}