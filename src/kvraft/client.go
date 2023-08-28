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
	clientId	int64
	nextRequest	int
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
	ck.clientId = nrand()
	ck.nextRequest = 1
	return ck
}

func (ck *Clerk) Command(key string, value string, op Opr) string {
	args := CommandArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		CommandId: ck.nextRequest,
	}
	i := ck.currentServer()

	// keeps trying forever in the face of all other errors.
	for true {
		log.Printf("Client [%d] send %s command to server [%d]: %+v", ck.clientId, op, i, args)
		reply := CommandReply{}
		ok := ck.servers[i].Call("KVServer.CommandHandler", &args, &reply)
		if ok {
			if reply.Err == OK {
				log.Printf("Client [%d] %s %s: %s", ck.clientId, op, key, reply.Value)
				ck.nextRequest += 1
				return reply.Value
			} else if reply.Err == ErrTimeout {
				log.Printf("Client [%d] %s %s command timeout", ck.clientId, op, key)
			}
		}
		i = ck.nextServer(i)
		time.Sleep(RETRY_INTEVAL)
	}

	return ""
}

func (ck *Clerk) Get(key string) string {
	return ck.Command(key, "", "Get")
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(key, value, "Append")
}

func (ck *Clerk) currentServer() int32 {
	return atomic.LoadInt32(&ck.leaderId)
}


func (ck *Clerk) nextServer(current int32) int32 {
	if current + 1 == int32(len(ck.servers)) {
		time.Sleep(TIMEOUT_INTEVAL)
	}
	next := (current + 1) % int32(len(ck.servers))
	atomic.StoreInt32(&ck.leaderId, next)
	return next
}