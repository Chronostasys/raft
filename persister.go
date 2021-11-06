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
	"fmt"
	"log"
	"os"
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
	e         *labgob.LabEncoder
	sne       *labgob.LabEncoder
	me        int
	f         *os.File
	snapshotF *os.File

	needsave    int32
	needsaveLog int32
}

func MakePersister() *Persister {
	persister := &Persister{
		me: -1,
	}
	return persister
}

func MakrRealPersister(me int) *Persister {
	persister := &Persister{}
	f, err := os.OpenFile(fmt.Sprintf("%d.rast", me), os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0777))
	if err == nil {
		d := labgob.NewDecoder(f)
		d.Decode(&persister.raftstate)
		d.Decode(&persister.snapshot)
	} else {
		log.Fatal(err)
	}
	f1, err := os.OpenFile(fmt.Sprintf("%d-sn.rast", me), os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0777))
	if err == nil {
		d := labgob.NewDecoder(f)
		d.Decode(&persister.snapshot)
	} else {
		log.Fatal(err)
	}
	persister.f = f
	persister.snapshotF = f1
	e := labgob.NewEncoder(f)
	persister.e = e
	e1 := labgob.NewEncoder(f1)
	persister.sne = e1
	persister.me = me
	go func() {
		for {
			time.Sleep(time.Second)
			if atomic.LoadInt32(&persister.killed) == 1 {
				return
			}
			if atomic.CompareAndSwapInt32(&persister.needsave, 1, 0) {
				// fmt.Println("persist")
				save(persister)
			} else if atomic.CompareAndSwapInt32(&persister.needsaveLog, 1, 0) {
				persister.snapshotF.Truncate(0)
				persister.mu.RLock()
				persister.e.Encode(persister.raftstate)
				persister.mu.RUnlock()
			}
		}
	}()
	return persister
}
func save(persister *Persister) {
	if persister.me == -1 {
		return
	}
	e := persister.e
	wg := sync.WaitGroup{}
	wg.Add(2)
	persister.mu.RLock()
	go func() {
		persister.f.Truncate(0)
		e.Encode(persister.raftstate)
		wg.Done()
	}()
	go func() {
		persister.snapshotF.Truncate(0)
		persister.sne.Encode(persister.snapshot)
		wg.Done()
	}()
	wg.Wait()
	persister.mu.RUnlock()
}
func (ps *Persister) Kill() {
	i := atomic.AddInt32(&ps.killed, 1)
	if i == 1 {
		save(ps)
		ps.f.Close()
		ps.snapshotF.Close()
	}
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
	// _, _, line, _ := runtime.Caller(2)
	// fmt.Println(line)
	atomic.StoreInt32(&ps.needsaveLog, 1)
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
	atomic.StoreInt32(&ps.needsave, 1)
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
