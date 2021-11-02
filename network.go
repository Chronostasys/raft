package raft

import (
	"net/rpc"
	"sync"
)

type RPCEnd interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool
}

type clientEnd struct {
	c   *rpc.Client
	end string
	mu  *sync.Mutex
}

func (c *clientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	c.mu.Lock()
	if c.c == nil {
		client, err := rpc.DialHTTP("tcp", c.end)
		if err == nil {
			c.c = client
		} else {
			c.mu.Unlock()
			return false
		}
	}
	c.mu.Unlock()
	err := c.c.Call(svcMeth, args, reply)
	if err == rpc.ErrShutdown {
		// conn lost, reconnect
		c.mu.Lock()
		c.c = nil
		c.mu.Unlock()
	}
	return err == nil
}

func MakeRPCEnds(ends []string) []RPCEnd {
	rpcends := make([]RPCEnd, len(ends))
	for i, v := range ends {
		rpcends[i] = &clientEnd{
			mu:  &sync.Mutex{},
			end: v,
		}
	}
	return rpcends
}
