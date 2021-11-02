package kvraft

import (
	"sync"
	"testing"
	"time"

	"github.com/Chronostasys/raft"
)

func TestRealServer(t *testing.T) {
	ends := []string{":1234", ":1235", ":1236"}
	rpcends := raft.MakeRPCEnds(ends)
	servers := make([]*KVServer, len(ends))
	for i := range rpcends {
		servers[i] = StartKVServer(rpcends, i, raft.MakePersister(), 10000)
		go servers[i].Serve(ends[i])
	}
	client := MakeClerk(rpcends)
	client.Put("a", "b")
	v := client.Get("a")
	if v != "b" {
		t.Fatalf("expect key %s value %s, get %s", "a", "b", v)
	}
}

func BenchmarkAppend(b *testing.B) {
	ends := []string{":1234", ":1235", ":1236"}
	rpcends := raft.MakeRPCEnds(ends)
	servers := make([]*KVServer, len(ends))
	for i := range rpcends {
		servers[i] = StartKVServer(rpcends, i, raft.MakePersister(), 10000)
		go servers[i].Serve(ends[i])
	}
	client := MakeClerk(rpcends)
ELECTION:
	for {
		for _, v := range servers {
			if v.checkLeader() {
				break ELECTION
			}
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			client.Append("a", "b")
			wg.Done()
		}()
	}
	wg.Wait()
	b.StopTimer()
	for i := 0; i < len(ends); i++ {
		servers[i].Close()
	}
	time.Sleep(time.Millisecond * 100)
}
