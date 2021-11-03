package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID [16]byte
	ReqID    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID [16]byte
	ReqID    int64
}

type GetReply struct {
	Err   Err
	Value string
}

type KVRPCServer struct {
	kv *KVServer
}

func (kv *KVRPCServer) Get(args *GetArgs, reply *GetReply) (err error) {
	kv.kv.Get(args, reply)
	return
}

func (kv *KVRPCServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) (err error) {
	kv.kv.PutAppend(args, reply)
	return
}
