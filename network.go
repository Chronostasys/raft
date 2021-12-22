package raft

import (
	"context"
	"time"

	"github.com/Chronostasys/raft/pb"
	"google.golang.org/grpc"
)

type RPCEnd interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool
}

type ClientEnd struct {
	c     *grpc.ClientConn
	KVend pb.KVServiceClient
	end   string
}

func (c *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	var err error
	if svcMeth[0] == 'R' {
		svcMeth = svcMeth[5:]
		a := &pb.GobMessage{Msg: gobEncode(args)}
		var r *pb.GobMessage
		if svcMeth == "AppendEntries" {
			r, err = pb.NewRaftServiceClient(c.c).AppendEntries(context.Background(), a)
		} else if svcMeth == "InstallSnapshot" {
			r, err = pb.NewRaftServiceClient(c.c).InstallSnapshot(context.Background(), a)
		} else {
			r, err = pb.NewRaftServiceClient(c.c).RequestVote(context.Background(), a)
		}
		if err == nil {
			gobDecode(r.GetMsg(), reply)
		}
		return err == nil
	} else {
		svcMeth = svcMeth[9:]
		if svcMeth == "Get" {
			// re := reply.(*pb.GetReply)
			err = c.c.Invoke(context.Background(), "/KVService/Get", args, reply)
		} else {
			err = c.c.Invoke(context.Background(), "/KVService/PutAppend", args, reply)
		}
		return err == nil
	}
}

func MakeRPCEnds(ends []string) []RPCEnd {
	rpcends := make([]RPCEnd, len(ends))
	for i, v := range ends {
		c := &ClientEnd{
			end: v,
		}
		for {
			ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*40)
			client, err := grpc.DialContext(ctx, c.end, grpc.WithInsecure())
			if err == nil {
				c.c = client
				c.KVend = pb.NewKVServiceClient(c.c)
				rpcends[i] = c
				break
			}
		}
	}
	return rpcends
}
