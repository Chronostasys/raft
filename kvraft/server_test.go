package kvraft

import (
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chronostasys/raft"
	"github.com/Chronostasys/trees/query"
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
	benchmarkOp(func(client *Clerk, i int) {
		client.Get("a")
	}, b, true)
}

func BenchmarkPut(b *testing.B) {
	benchmarkOp(func(client *Clerk, i int) {
		client.Put(fmt.Sprintf("%d", i), "b")
	}, b, true)
}

func BenchmarkAppend(b *testing.B) {
	benchmarkOp(func(client *Clerk, i int) {
		client.Append("a", "b")
	}, b, true)
}

func benchmarkOp(benchfunc func(client *Clerk, i int), b *testing.B, startServer bool) {
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
	// warm up
	wg := sync.WaitGroup{}
	wg.Add(10000)
	for i := 0; i < 10000; i++ {
		go func(i int) {
			benchfunc(client, 0)
			wg.Done()
		}(i)
	}
	wg.Wait()
	b.SetParallelism(512)
	runtime.GC()
	rands := rand.Perm(b.N)
	b.ResetTimer()
	i := int64(-1)
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			benchfunc(client, rands[int(atomic.AddInt64(&i, 1))])
		}

	})
	b.StopTimer()
	// client.Put("a", "")
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
	benchmarkOp(func(client *Clerk, i int) {
		client.Put(strconv.Itoa(i), "b")
	}, b, false)
}

func BenchmarkRealServerAppend(b *testing.B) {
	benchmarkOp(func(client *Clerk, i int) {
		client.Append(strconv.Itoa(i), "b")
	}, b, false)
}

func BenchmarkRealServerGet(b *testing.B) {
	benchmarkOp(func(client *Clerk, i int) {
		client.Get(strconv.Itoa(i))
	}, b, false)
}

func BenchmarkRealServerQuery(b *testing.B) {
	o := sync.Once{}
	var q *query.TableQuerier
	t := &Test{TestInt: 0}
	benchmarkOp(func(client *Clerk, i int) {
		o.Do(func() {
			InitQueryEngine(client)
			query.Register(&Test{})
			query.CreateTable(&Test{})
			q, _ = query.Table(&Test{})
		})
		t.TestInt = i
		q.Insert(t)
	}, b, false)
}
