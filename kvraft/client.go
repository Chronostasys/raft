package kvraft

import (
	"crypto/rand"
	"encoding/binary"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chronostasys/raft"
	"github.com/Chronostasys/raft/pb"
)

type Clerk struct {
	servers []raft.RPCEnd
	// You will have to modify this struct.
	id         [16]byte
	reqid      int64
	prevLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []raft.RPCEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	unix := time.Now().Unix()
	ran := nrand()
	b1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b1, uint64(unix))
	b2 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b2, uint64(ran))
	var id [16]byte
	copy(id[:], b1)
	copy(id[8:], b2)
	ck.id = id
	return ck
}
func (ck *Clerk) getID() int64 {
	return atomic.AddInt64(&ck.reqid, 1)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	reply := &pb.GetReply{}
	id := ck.getID()
	// You will have to modify this function.
	for {
		ok := ck.OneDone(func(server int, id int64) bool {
			re := &pb.GetReply{}
			ok := ck.servers[server].Call("KVServer.Get", &pb.GetArgs{
				Key:      key,
				ReqId:    id,
				ClientId: ck.id[:],
			}, re)
			if ok && len(re.Err) == 0 {
				reply = re
				return true
			}
			return false
		}, id)
		if ok {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	return reply.Value
}

type jobfunc func(server int, id int64) bool

func (ck *Clerk) OneDone(job jobfunc, id int64) bool {
	prev := ck.prevLeader
	ok := job(prev, id)
	if ok {
		return true
	}
	ch := make(chan struct{})
	donech := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(len(ck.servers) - 1)
	go func() {
		wg.Wait()
		close(donech)
		close(ch)
	}()
	// You will have to modify this function.
	for i := range ck.servers {
		if i != prev {
			go func(i int) {
				ok := job(i, id)
				if ok {
					ck.prevLeader = i
					select {
					case <-donech:
					case ch <- struct{}{}:

					}

				}
				wg.Done()
			}(i)
		}
	}
	select {
	case <-ch:
		return true
	case <-donech:
		return false
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	id := ck.getID()
	// You will have to modify this function.
	for {
		ok := ck.OneDone(func(server int, id int64) bool {
			re := &pb.PutAppendReply{}
			ok := ck.servers[server].Call("KVServer.PutAppend", &pb.PutAppendArgs{
				Key:      key,
				Value:    value,
				Op:       op,
				ReqId:    id,
				ClientId: ck.id[:],
			}, re)
			if ok && len(re.Err) == 0 {
				return true
			}
			// fmt.Println(ok, re.Err)
			return false
		}, id)
		if ok {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	// fmt.Println("done", id)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
