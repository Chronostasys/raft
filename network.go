package raft

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

type RPCEnd interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool
}

type clientEnd struct {
	c   *rpc.Client
	end string
	o   *sync.Once
}

func (c *clientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	c.o.Do(func() {
		for {
			client, err := rpc.DialHTTP("tcp", c.end)
			if err == nil {
				c.c = client
				return
			}
			fmt.Println(err, svcMeth)
			time.Sleep(time.Millisecond * 100)
		}
	})
	err := c.c.Call(svcMeth, args, reply)
	return err == nil
}

func MakeRPCEnds(ends []string) []RPCEnd {
	rpcends := make([]RPCEnd, len(ends))
	for i, v := range ends {
		rpcends[i] = &clientEnd{
			o:   &sync.Once{},
			end: v,
		}
	}
	return rpcends
}
