package kvraft

import (
	"context"

	"github.com/Chronostasys/raft/pb"
)

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
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type LargerArgs struct {
	Than  string
	Max   int64
	Limit int64
	Skip  int64
}

type GetReply struct {
	Err   Err
	Value string
}

type KVRPCServer struct {
	pb.UnimplementedKVServiceServer
	kv *KVServer
}

func (kv *KVRPCServer) Get(ctx context.Context, args *pb.GetArgs) (reply *pb.GetReply, err error) {
	reply = &pb.GetReply{}
	kv.kv.Get(args, reply)
	return
}

func (kv *KVRPCServer) PutAppend(ctx context.Context, args *pb.PutAppendArgs) (reply *pb.PutAppendReply, err error) {
	reply = &pb.PutAppendReply{}
	kv.kv.PutAppend(args, reply)
	return
}

func (kv *KVRPCServer) Larger(args *pb.LargerArgs, server pb.KVService_LargerServer) error {
	return kv.kv.Larger(args, server)
}
