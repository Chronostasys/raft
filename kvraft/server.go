package kvraft

import (
	"bytes"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chronostasys/raft"
	"github.com/Chronostasys/raft/labgob"
)

const (
	Debug = 0
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

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
	data  map[string]string
	rwmu  *sync.RWMutex
	idmap clientMap
	l     net.Listener
}

type reqsignal struct {
	ch   chan struct{}
	err  Err
	once *sync.Once
	done bool
}

type ReqStatus struct {
	Done bool
	Err  Err
}

type ReqStatusMap struct {
	M         map[int64]ReqStatus
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

func (kv *KVServer) cm2map() map[[16]byte]ReqStatusMap {
	mu := kv.idmap.mu
	mu.RLock()
	defer mu.RUnlock()
	m := map[[16]byte]ReqStatusMap{}
	for k, v := range kv.idmap.m {
		m1 := map[int64]ReqStatus{}
		mu1 := v.mu
		mu1.RLock()
		for k1, v1 := range v.m {
			m1[k1] = ReqStatus{
				Err:  v1.err,
				Done: v1.done,
			}
		}
		mu1.RUnlock()
		m[k] = ReqStatusMap{
			M:         m1,
			SuccMaxID: v.succMaxID,
		}
	}
	return m
}
func (kv *KVServer) map2cm(m map[[16]byte]ReqStatusMap) {
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
		for k1, v1 := range v.M {
			m1.m[k1] = &reqsignal{
				err:  v1.Err,
				done: v1.Done,
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

// func (m clientMap) delete(k string) {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
// 	_, ok := m.m[k]
// 	if ok {
// 		delete(m.m, k)
// 	}
// 	return
// }

func (kv *KVServer) getv(key string) string {
	kv.rwmu.RLock()
	defer kv.rwmu.RUnlock()
	return kv.data[key]
}

func (kv *KVServer) setv(key, val string) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	kv.data[key] = val
}
func (kv *KVServer) appendv(key, val string) {
	kv.rwmu.Lock()
	defer kv.rwmu.Unlock()
	kv.data[key] = kv.data[key] + val
}

func (kv *KVServer) checkLeader() bool {
	_, isleader := kv.rf.GetState()
	return isleader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = "killed"
		return
	} else if !kv.checkLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	sig, _ := kv.idmap.get(args.ClientID).get(args.ReqID)
	ok := kv.rf.StartWithCache(Op{Args: *args, ClientID: args.ClientID, ReqID: args.ReqID})
	// fmt.Println("start get", args.ClientID, args.ReqID)
	if ok {
		select {
		case <-sig.ch:
			reply.Err = sig.err
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = "killed"
		return
	} else if !kv.checkLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	sig, _ := kv.idmap.get(args.ClientID).get(args.ReqID)
	// fmt.Println("leader")
	// sig.mu.Lock()
	// defer sig.mu.Unlock()
	// fmt.Println("before start pa", args.ClientID, args.ReqID, args.Value, kv.checkLeader())
	ok := kv.rf.StartWithCache(Op{Args: *args, ClientID: args.ClientID, ReqID: args.ReqID})
	// fmt.Println("start pa", args.ClientID, args.ReqID)
	if ok {
		select {
		case <-sig.ch:
			reply.Err = sig.err
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

var servemu = sync.Mutex{}

func (kv *KVServer) Serve(addr string) {
	servemu.Lock()

	// ===== workaround ==========
	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	// ===========================
	server := rpc.NewServer()
	server.RegisterName("KVServer", &KVRPCServer{
		kv: kv,
	})
	server.RegisterName("Raft", &raft.RaftRPCServer{
		Rf: kv.rf,
	})
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	// ===== workaround ==========
	http.DefaultServeMux = oldMux
	// ===========================
	servemu.Unlock()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln(err.Error())
	}
	kv.l = l
	http.Serve(l, mux)
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
	m := map[[16]byte]ReqStatusMap{}
	kv.rwmu.Lock()
	if d.Decode(&kv.data) != nil ||
		d.Decode(&m) != nil {
		log.Fatalln("read snapshot err")
	}
	kv.rwmu.Unlock()
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
	labgob.Register(ReqStatus{})
	labgob.Register(ReqStatusMap{})

	kv := &KVServer{
		rwmu: &sync.RWMutex{},
		data: make(map[string]string),
		idmap: clientMap{
			m:  make(map[[16]byte]*reqMap),
			mu: &sync.RWMutex{},
		},
	}
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	// load snapshot
	kv.loadSnapshot(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.MaxRaftStateSize = kv.maxraftstate
	kv.rf.SnapshotFunc = func() []byte {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		kv.rwmu.RLock()
		e.Encode(kv.data)
		kv.rwmu.RUnlock()
		m := kv.cm2map()
		e.Encode(m)
		data := w.Bytes()
		return data
	}
	kv.rf.WaitForDone = true

	// You may need initialization code here.
	go func() {
		for {
			if kv.killed() {
				return
			}
			apply := <-kv.applyCh
			if !apply.CommandValid {
				close(apply.Ch)
				continue
			}
			if apply.IsSnapshot {
				kv.loadSnapshot(apply.Command.([]byte))
				close(apply.Ch)
				continue
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
						// fmt.Println("put", cmd.Value)

					} else if cmd.Op == "Append" {
						kv.appendv(cmd.Key, cmd.Value)
						err = ""
						// fmt.Println("append", cmd.Value, kv.me, kv.checkLeader())
						// fmt.Println("append", kv.me, apply.CommandIndex, cmd.Key, cmd.Value)
					}
				case GetArgs:
					err = ""
					// fmt.Println(kv.me, "done get", cmd.Key, err)
					// fmt.Println("get", kv.me, apply.CommandIndex, cmd.Key)
				default:
				}

			}
			if !sig.done && op.ReqID > atomic.LoadInt64(&reqmap.succMaxID) {
				sig.err = Err(err)
				sig.done = true
				// compress succ results
				if len(sig.err) == 0 {
					i := reqmap.succMaxID + 1
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
						} else {
							reqmap.mu.Unlock()
							break
						}
						reqmap.mu.Unlock()
						i++
					}
				}
			} else if atomic.LoadInt64(&reqmap.succMaxID) >= op.ReqID {
				sig.done = true
				reqmap.delete(op.ReqID)
			}
			close(apply.Ch)
			sig.once.Do(func() {
				close(sig.ch)
			})
		}
	}()
	return kv
}
