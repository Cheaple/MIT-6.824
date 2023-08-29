package kvraft

const (
	OK             	= "OK"
	ErrNoKey       	= "ErrNoKey"
	ErrWrongLeader 	= "ErrWrongLeader"
	ErrTimeout		= "ErrTimeout"
)

const (
	GET		= 		"Get"
	PUT		=		"Put"
	APPEND	=		"Append"
)

const (
	RETRY_INTEVAL = 100  // milliseconds
	TIMEOUT_INTEVAL = 1500
)

type Err string
type Opr string

// // Put or Append
// type PutAppendArgs struct {
// 	Key   string
// 	Value string
// 	Op    string // "Put" or "Append"
// 	// You'll have to add definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// }

// type PutAppendReply struct {
// 	Err Err
// }

// type GetArgs struct {
// 	Key string
// 	// You'll have to add definitions here.
// }

// type GetReply struct {
// 	Err   Err
// 	Value string
// }

type CommandArgs struct {
	Key		string
	Value	string
	Op		Opr
	ClientId	int64
	CommandId	int
	// Non			int64
}

type CommandReply struct {
	Err		Err
	Value	string
}