package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chronostasys/raft/labgob"
)

type Persister struct {
	mu        sync.RWMutex
	raftstate []byte
	snapshot  []byte
	killed    int32
	w         *bytes.Buffer
	e         *labgob.LabEncoder
	me        int
}

func MakePersister() *Persister {
	persister := &Persister{
		me: -1,
	}
	return persister
}

func MakrRealPersister(me int) *Persister {
	persister := &Persister{}
	bs, err := ioutil.ReadFile(fmt.Sprintf("%d.rast", me))
	if err == nil {
		w := new(bytes.Buffer)
		w.Write(bs)
		d := labgob.NewDecoder(w)
		d.Decode(&persister.raftstate)
		d.Decode(&persister.snapshot)
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	persister.w = w
	persister.e = e
	persister.me = me
	go func() {
		for {
			time.Sleep(time.Second)
			if atomic.LoadInt32(&persister.killed) == 1 {
				return
			}
			save(persister)
		}
	}()
	return persister
}
func save(persister *Persister) {
	if persister.me == -1 {
		return
	}
	me := persister.me
	w := persister.w
	e := persister.e
	w.Reset()
	persister.mu.RLock()
	e.Encode(persister.raftstate)
	e.Encode(persister.snapshot)
	persister.mu.RUnlock()

	err := ioutil.WriteFile(fmt.Sprintf("%d.rast", me), w.Bytes(), 0777)
	if err != nil {
		fmt.Println("save error", err)
	}
}
func (ps *Persister) Kill() {
	atomic.StoreInt32(&ps.killed, 1)
	save(ps)
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.snapshot)
}
