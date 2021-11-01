package raft

import (
	"testing"
	"time"
)

func TestReal(t *testing.T) {
	ends := []string{":1234", ":1235", ":1236"}
	rpcends := MakeRPCEnds(ends)
	rfs := [3]*Raft{}
	chs := [3]chan ApplyMsg{}
	for i := range rpcends {
		chs[i] = make(chan ApplyMsg)
		rfs[i] = Make(rpcends, i, MakePersister(), chs[i])
		go rfs[i].Serve(ends[i])
	}
	cmd := "XXXX"
CMD:
	for {
		for _, v := range rfs {
			_, _, leader := v.Start(cmd)
			if leader {
				break CMD
			}
		}
	}
	for i := 0; i < 3; i++ {
		select {
		case v := <-chs[0]:
			if v.Command.(string) != cmd {
				t.Fatal("cmd not equal")
			}
		case v := <-chs[1]:
			if v.Command.(string) != cmd {
				t.Fatal("cmd not equal")
			}
		case v := <-chs[2]:
			if v.Command.(string) != cmd {
				t.Fatal("cmd not equal")
			}

		}
	}
}
func Start(rfs []*Raft, cmd []interface{}, last int) int {
	_, _, leader := rfs[last].StartMulti(cmd...)
	if leader {
		return last
	}
	for {
		for i, v := range rfs {
			if i != last {
				_, _, leader := v.StartMulti(cmd...)
				if leader {
					return i
				}
			}
		}
	}
}
func BenchmarkRaft(b *testing.B) {
	b.StopTimer()
	ends := []string{":1234", ":1235", ":1236"}
	rpcends := MakeRPCEnds(ends)
	rfs := make([]*Raft, len(ends))
	ch := make(chan ApplyMsg, 100)
	for i := range rpcends {
		rfs[i] = Make(rpcends, i, MakePersister(), ch)
		go rfs[i].Serve(ends[i])
	}
ELECTION:
	for {
		for _, v := range rfs {
			if _, leader := v.GetState(); leader {
				break ELECTION
			}
		}
	}
	b.StartTimer()
	iter := 100000
	chiter := len(ends) * iter
	for n := 0; n < b.N; n++ {
		b.Log("start")
		go func() {
			Start(rfs[:], make([]interface{}, iter), 0)
		}()
		for i := 0; i < chiter; i++ {
			<-ch
		}
	}
	b.StopTimer()
	for i := 0; i < len(ends); i++ {
		rfs[i].Close()
	}
	time.Sleep(time.Millisecond * 100)
}
