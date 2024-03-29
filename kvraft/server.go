package kvraft

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chronostasys/raft"
	"github.com/Chronostasys/raft/labgob"
	"github.com/Chronostasys/raft/labrpc"
	"github.com/Chronostasys/raft/pb"
	"github.com/Chronostasys/trees/btree"
	"google.golang.org/grpc"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Args     interface{}
	ClientID [16]byte
	ReqID    int64
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data    *btree.Tree
	idmap   clientMap
	l       net.Listener
	encodeM map[[16]byte]*ReqStatusMap
	rwmu    *sync.RWMutex
}

type reqsignal struct {
	ch   chan struct{}
	err  Err
	once *sync.Once
	done bool
	// result string
}

type ReqStatus struct {
	Done bool
	Err  Err
}

type ReqStatusMap struct {
	M         map[int64]bool
	SuccMaxID int64
}

type reqMap struct {
	m         map[int64]*reqsignal
	mu        *sync.RWMutex
	succMaxID int64
}

type clientMap struct {
	m  map[[16]byte]*reqMap
	mu *sync.RWMutex
}

func (kv *KVServer) map2cm(m map[[16]byte]*ReqStatusMap) {
	kv.idmap = clientMap{
		m:  map[[16]byte]*reqMap{},
		mu: &sync.RWMutex{},
	}
	kv.idmap.mu.Lock()
	defer kv.idmap.mu.Unlock()
	for k, v := range m {
		m1 := &reqMap{
			m:         map[int64]*reqsignal{},
			mu:        &sync.RWMutex{},
			succMaxID: v.SuccMaxID,
		}
		m1.mu.Lock()
		for k1 := range v.M {
			m1.m[k1] = &reqsignal{
				err:  "",
				done: true,
				ch:   make(chan struct{}),
				once: &sync.Once{},
			}
		}
		m1.mu.Unlock()
		kv.idmap.m[k] = m1

	}
}

func (m reqMap) get(k int64) (mu *reqsignal, ext bool) {
	m.mu.RLock()
	v, ok := m.m[k]
	if ok {
		m.mu.RUnlock()
		return v, ok
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok = m.m[k]
	if !ok {
		v = &reqsignal{
			err:  "",
			ch:   make(chan struct{}),
			once: &sync.Once{},
		}
		m.m[k] = v
	}
	return v, ok
}

func (m clientMap) get(k [16]byte) *reqMap {
	m.mu.RLock()
	v, ok := m.m[k]
	if ok {
		m.mu.RUnlock()
		return v
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok = m.m[k]
	if !ok {
		v = &reqMap{
			m:  make(map[int64]*reqsignal),
			mu: &sync.RWMutex{},
		}
		m.m[k] = v
	}
	return v
}

func (r reqMap) delete(id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, id)
}

func (kv *KVServer) getv(key string) string {
	kv.rwmu.RLock()
	defer kv.rwmu.RUnlock()
	d := kv.data.Search(btree.KV{K: key})
	if d == nil {
		return ""
	}
	return d.(btree.KV).V
}
func (kv *KVServer) larger(key string, max, limit, skip int, callback func(btree.Item) bool) {
	kv.rwmu.RLock()
	defer kv.rwmu.RUnlock()
	kv.data.Larger(btree.KV{K: key}, max, limit, skip, callback)
}

func (kv *KVServer) setv(key, val string) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	kv.data.Insert(btree.KV{K: key, V: val})
}
func (kv *KVServer) delv(key string) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	kv.data.Delete(btree.KV{K: key})
}
func (kv *KVServer) appendv(key, val string) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	d := kv.data.Search(btree.KV{K: key})
	if d == nil {
		kv.data.Insert(btree.KV{K: key, V: val})
		return
	}
	kv.data.Insert(btree.KV{K: key, V: d.(btree.KV).V + val})
}

func (kv *KVServer) checkLeader() bool {
	_, isleader := kv.rf.GetState()
	return isleader
}
func (kv *KVServer) EnableLog() {
	kv.rf.SetLogger(log.Default())
}

func (kv *KVServer) Get(args *pb.GetArgs, reply *pb.GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = "killed"
		return
	} else if !kv.checkLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	id := [16]byte{}
	copy(id[:], args.ClientId)
	sig, _ := kv.idmap.get(id).get(args.ReqId)
	ok := kv.rf.StartWithCache(Op{Args: GetArgs{Key: args.Key}, ClientID: id, ReqID: args.ReqId})
	// fmt.Println("start get", args.ClientID, args.ReqID)
	if ok {
		select {
		case <-sig.ch:
			reply.Err = string(sig.err)
		case <-time.After(2 * time.Second):
			reply.Err = "time out"
		}
		if kv.killed() {
			reply.Err = "killed"
			return
		} else if !kv.checkLeader() {
			reply.Err = ErrWrongLeader
			return
		}
		// kv.idmap.get(args.ClientID).delete(args.ReqID)
		if len(reply.Err) == 0 {
			reply.Value = kv.getv(args.Key)
			// fmt.Println(kv.me, "get", args.Key, reply.Err, reply.Value, kv.checkLeader())
		}
	} else {
		reply.Err = "err raft start"
	}
}

var (
	errKill        = fmt.Errorf("err killed")
	errWrongLeader = fmt.Errorf(ErrWrongLeader)
)

func (kv *KVServer) Larger(args *pb.LargerArgs, server pb.KVService_LargerServer) error {
	// Your code here.
	if kv.killed() {
		return errKill
	} else if !kv.checkLeader() {
		return errWrongLeader
	}
	id := [16]byte{}
	copy(id[:], args.ClientId)
	sig, _ := kv.idmap.get(id).get(args.ReqId)
	ok := kv.rf.StartWithCache(Op{Args: LargerArgs{Than: args.Than, Max: args.Max, Limit: args.Limit, Skip: args.Skip}, ClientID: id, ReqID: args.ReqId})
	// fmt.Println("start get", args.ClientID, args.ReqID)
	if ok {
		var err error
		select {
		case <-sig.ch:
			if len(sig.err) > 0 {
				err = fmt.Errorf(string(sig.err))
			}
		case <-time.After(2 * time.Second):
			err = fmt.Errorf("time out")
		}
		if kv.killed() {
			err = errKill
			return err
		} else if !kv.checkLeader() {
			err = errWrongLeader
			return err
		}
		// kv.idmap.get(args.ClientID).delete(args.ReqID)
		if err == nil {
			kv.larger(args.Than, int(args.Max), int(args.Limit), int(args.Skip), func(i btree.Item) bool {
				kv := i.(btree.KV)
				err := server.Send(&pb.LargerReply{
					K: kv.K,
					V: kv.V,
				})
				return err == nil
			})
			server.Send(&pb.LargerReply{
				Err: "end",
			})
			// fmt.Println(kv.me, "get", args.Key, reply.Err, reply.Value, kv.checkLeader())
		}
		return err
	} else {
		return fmt.Errorf("err raft start")
	}
}

func (kv *KVServer) PutAppend(args *pb.PutAppendArgs, reply *pb.PutAppendReply) {
	// bs, _ := json.Marshal(args)
	// fmt.Println(string(bs))
	// Your code here.
	if kv.killed() {
		reply.Err = "killed"
		return
	} else if !kv.checkLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	id := [16]byte{}
	copy(id[:], args.ClientId)
	sig, _ := kv.idmap.get(id).get(args.ReqId)
	// fmt.Println("leader")
	// sig.mu.Lock()
	// defer sig.mu.Unlock()
	// fmt.Println("before start pa", args.ClientID, args.ReqID, args.Value, kv.checkLeader())
	ok := kv.rf.StartWithCache(Op{Args: PutAppendArgs{Key: args.Key, Value: args.Value, Op: args.Op}, ClientID: id, ReqID: args.ReqId})
	// fmt.Println("start pa", args.ClientID, args.ReqID)
	if ok {
		select {
		case <-sig.ch:
			reply.Err = string(sig.err)
		case <-time.After(2 * time.Second):
			reply.Err = "time out"
		}
		if kv.killed() {
			reply.Err = "killed"
			return
		} else if !kv.checkLeader() {
			reply.Err = ErrWrongLeader
			return
		}
		// fmt.Println(kv.me, "done pa", args.ClientID, args.ReqID, args.Value, reply.Err)
	} else {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ServeWithReqLog(addr string) {
	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if info.FullMethod[1] != 'R' {
			t := time.Now()
			defer func() {
				if err == nil {
					log.Println("[info]", info.FullMethod, "complete in", time.Since(t))
				} else {
					log.Println("[err]", info.FullMethod, "error in", time.Since(t), "err:", err)
				}
			}()
		}
		// 继续处理请求
		return handler(ctx, req)
	}
	kv.Serve(addr, grpc.UnaryInterceptor(interceptor))
}

func (kv *KVServer) Serve(addr string, opt ...grpc.ServerOption) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln(err.Error())
	}
	kv.l = l
	server := grpc.NewServer(append(opt, grpc.MaxRecvMsgSize(32*10e6))...)

	pb.RegisterKVServiceServer(server, &KVRPCServer{
		kv: kv,
	})
	pb.RegisterRaftServiceServer(server, &raft.RaftRPCServer{
		Rf: kv.rf,
	})
	server.Serve(l)
}
func (kv *KVServer) Close() {
	kv.Kill()
	kv.l.Close()
}

func (kv *KVServer) loadSnapshot(state []byte) {
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	m := map[[16]byte]*ReqStatusMap{}
	if d.Decode(&m) != nil {
		fmt.Println("read snapshot err")
	}
	kv.encodeM = m
	sn := r.Bytes()
	da := btree.LoadSnapshot(sn, fmt.Sprintf("data/%d", kv.me))
	if da != nil {
		kv.data = da
	}
	// kv.encMu.Unlock()
	kv.map2cm(m)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []raft.RPCEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(raft.Log{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(pb.GetArgs{})
	labgob.Register(pb.PutAppendArgs{})
	labgob.Register(pb.GetReply{})
	labgob.Register(pb.PutAppendReply{})
	labgob.Register(pb.LargerReply{})
	labgob.Register(pb.LargerArgs{})
	labgob.Register(ReqStatus{})
	labgob.Register(ReqStatusMap{})
	labgob.Register(btree.BinNode{})
	labgob.Register(btree.TreeMeta{})
	labgob.Register(btree.Int(0))
	labgob.Register([]byte{})
	labgob.Register(btree.KV{})
	labgob.Register(LargerArgs{})
	labgob.Register(&map[[16]byte]*ReqStatusMap{})

	kv := &KVServer{
		data: btree.MakePersist(1024),
		idmap: clientMap{
			m:  make(map[[16]byte]*reqMap),
			mu: &sync.RWMutex{},
		},
		encodeM: map[[16]byte]*ReqStatusMap{},
		rwmu:    &sync.RWMutex{},
	}
	kv.me = me
	kv.maxraftstate = maxraftstate
	os.MkdirAll("data", os.ModePerm)
	var f *os.File
	if _, ok := servers[0].(*labrpc.ClientEnd); !ok {
		f, _ = os.OpenFile(fmt.Sprintf("data/%d.req", kv.me), os.O_CREATE|os.O_RDWR, 0644)
	}
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	// load snapshot
	if persister.ManageSnapshot() {
		kv.loadSnapshot(persister.ReadSnapshot())
	} else {
		bs, _ := io.ReadAll(f)
		r := bytes.NewBuffer(bs)
		d := labgob.NewDecoder(r)
		m := map[[16]byte]*ReqStatusMap{}
		err := d.Decode(&m)
		if err != nil {
			fmt.Println("read snapshot err", err, len(bs))
		}
		kv.encodeM = m
		kv.map2cm(m)
		load := btree.Load(fmt.Sprintf("data/%d", kv.me))
		if load != nil {
			kv.data = load
		}
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.MaxRaftStateSize = kv.maxraftstate
	kv.rf.SnapshotFunc = func() []byte {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		err := e.Encode(kv.encodeM)
		if err != nil {
			fmt.Println("save snapshot err", err)
		}
		if _, ok := servers[0].(*labrpc.ClientEnd); !ok {
			bs := w.Bytes()
			f.Truncate(0)
			f.Seek(0, 0)
			f.Write(bs)
			f.Sync()
		}
		sn := kv.data.PersistWithSnapshot(fmt.Sprintf("data/%d", kv.me))
		w.Write(sn)
		data := w.Bytes()
		return data
	}
	kv.rf.WaitForDone = true
	kv.rf.SetLogger(log.New(io.Discard, "", 0))
	if _, ok := servers[0].(*labrpc.ClientEnd); !ok {
		kv.rf.MinCommitBTWSnapshots = 3000
	}
	// You may need initialization code here.
	go func() {
		for {
			if kv.killed() {
				return
			}
			apply := <-kv.applyCh
			func() {
				defer apply.Done()
				if !apply.CommandValid {
					return
				}
				if apply.IsSnapshot {
					kv.loadSnapshot(apply.Command.([]byte))
					return
				}
				op := apply.Command.(Op)
				err := "raft err"
				reqmap := kv.idmap.get(op.ClientID)
				sig, _ := reqmap.get(op.ReqID)
				if !sig.done && op.ReqID > atomic.LoadInt64(&reqmap.succMaxID) {
					switch cmd := op.Args.(type) {
					case PutAppendArgs:
						if cmd.Op == "Put" {
							kv.setv(cmd.Key, cmd.Value)
							err = ""

						} else if cmd.Op == "Append" {
							kv.appendv(cmd.Key, cmd.Value)
							err = ""
						} else if cmd.Op == "Delete" {
							kv.delv(cmd.Key)
							err = ""
						}
					case GetArgs, LargerArgs:
						err = ""
					default:
					}
					sig.err = Err(err)
					sig.done = true
					// compress succ results
					if len(sig.err) == 0 {
						if _, ext := kv.encodeM[op.ClientID]; !ext {
							kv.encodeM[op.ClientID] = &ReqStatusMap{
								M: map[int64]bool{
									op.ReqID: true,
								},
							}
						} else if !kv.encodeM[op.ClientID].M[op.ReqID] && op.ReqID > kv.encodeM[op.ClientID].SuccMaxID {
							kv.encodeM[op.ClientID].M[op.ReqID] = true
						}
						i := reqmap.succMaxID + 1
						c := kv.encodeM[op.ClientID].SuccMaxID + 1
						if i != c {
							fmt.Println("fatal", i, c)
						}
						for {

							reqmap.mu.Lock()
							el, ext := reqmap.m[i]
							if !ext {
								reqmap.mu.Unlock()
								break
							}
							if el.done && len(el.err) == 0 {
								delete(reqmap.m, i)
								atomic.AddInt64(&reqmap.succMaxID, 1)
								delete(kv.encodeM[op.ClientID].M, c)
								kv.encodeM[op.ClientID].SuccMaxID++
							} else {
								reqmap.mu.Unlock()
								break
							}
							reqmap.mu.Unlock()
							i++
							c++
						}
					}
				} else if atomic.LoadInt64(&reqmap.succMaxID) >= op.ReqID {
					sig.done = true
					reqmap.delete(op.ReqID)
				}
				sig.once.Do(func() {
					close(sig.ch)
				})
			}()
		}
	}()
	return kv
}
