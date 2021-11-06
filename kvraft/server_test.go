package kvraft

import (
	"net"
	"strings"
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
	defer func() {
		for i := 0; i < len(ends); i++ {
			servers[i].Close()
		}
		time.Sleep(time.Millisecond * 100)
	}()
	client := MakeClerk(rpcends)
	client.Put("a", "b")
	v := client.Get("a")
	if v != "b" {
		t.Fatalf("expect key %s value %s, get %s", "a", "b", v)
	}
}

func BenchmarkGet(b *testing.B) {
	benchmarkOp(func(client *Clerk) {
		client.Get("a")
	}, b, true)
}

func BenchmarkPut(b *testing.B) {
	benchmarkOp(func(client *Clerk) {
		client.Put("a", "b")
	}, b, true)
}

func BenchmarkAppend(b *testing.B) {
	benchmarkOp(func(client *Clerk) {
		client.Append("a", "b")
	}, b, true)
}

func benchmarkOp(benchfunc func(client *Clerk), b *testing.B, startServer bool) {
	ends := []string{":1234", ":1235", ":1236"}
	rpcends := raft.MakeRPCEnds(ends)
	ext := raw_connect("", ends)
	if startServer {
		if ext {
			b.SkipNow()
		}
		servers := make([]*KVServer, len(ends))
		for i := range rpcends {
			servers[i] = StartKVServer(rpcends, i, raft.MakePersister(), 1000)
			go servers[i].Serve(ends[i])
		}
	ELECTION:
		for {
			for _, v := range servers {
				if v.checkLeader() {
					break ELECTION
				}
			}
			time.Sleep(time.Millisecond * 100)
		}
		defer func() {
			for i := 0; i < len(ends); i++ {
				servers[i].Close()
			}
			time.Sleep(time.Millisecond * 100)
		}()
	} else {
		if !ext {
			b.SkipNow()
		}
	}
	client := MakeClerk(rpcends)
	wg := sync.WaitGroup{}
	wg.Add(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			benchfunc(client)
			wg.Done()
		}()
	}
	wg.Wait()
	b.StopTimer()
}
func raw_connect(host string, ports []string) bool {
	for _, port := range ports {
		timeout := time.Second
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strings.Trim(port, ":")), timeout)
		if err != nil {
			return false
		}
		if conn != nil {
			defer conn.Close()
		}
	}
	return true
}

func BenchmarkRealServerPut(b *testing.B) {
	benchmarkOp(func(client *Clerk) {
		client.Put("a", "b")
	}, b, false)
}

func BenchmarkRealServerAppend(b *testing.B) {
	benchmarkOp(func(client *Clerk) {
		client.Append("a", "b")
	}, b, false)
}

func BenchmarkRealServerGet(b *testing.B) {
	benchmarkOp(func(client *Clerk) {
		client.Get("a")
	}, b, false)
}

func BenchmarkConcurrentMapWrite(b *testing.B) {
	mu := sync.Mutex{}
	m := map[int]int{}
	wg := sync.WaitGroup{}
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		go func(i int) {
			mu.Lock()
			defer mu.Unlock()
			m[i] = m[i] + i
			wg.Done()
		}(i)
	}
	wg.Wait()

}
