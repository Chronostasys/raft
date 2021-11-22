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
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edsrzf/mmap-go"
)

type Persister struct {
	mu        sync.RWMutex
	raftstate []byte
	snapshot  []byte
	killed    int32
	me        int
	f         *os.File
	snapshotF *os.File
	mmap      mmap.MMap

	needsave    int32
	needsaveLog int32
	manageSN    bool
}

func MakePersister() *Persister {
	persister := &Persister{
		me:       -1,
		manageSN: true,
	}
	return persister
}

func MakrRealPersister(me int, managgSnapshot bool) *Persister {
	persister := &Persister{
		manageSN: managgSnapshot,
	}
	os.MkdirAll("data", os.ModePerm)
	f, err := os.OpenFile(fmt.Sprintf("data/%d.rast", me), os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0777))
	if err != nil {
		log.Fatal(err)
	}
	info, _ := f.Stat()
	f.Seek(0, 0)
	err = f.Truncate(20000 * 1024)
	if err != nil {
		panic(err)
	}
	err = f.Sync()
	if err != nil {
		panic(err)
	}
	persister.mmap, _ = mmap.MapRegion(f, -1, mmap.RDWR, 0, 0)
	if info.Size() == 20000*1024 {
		l := int64(binary.LittleEndian.Uint64(persister.mmap[:8]))
		if l > 0 {
			persister.raftstate = persister.mmap[8 : 8+l]
		}
	} else {
		binary.LittleEndian.PutUint64(persister.mmap[:8], uint64(0))
	}
	f1, err := os.OpenFile(fmt.Sprintf("data/%d-sn.rast", me), os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0777))
	if err == nil {
		persister.snapshot, _ = io.ReadAll(f1)
	} else {
		log.Fatal(err)
	}
	persister.f = f
	persister.snapshotF = f1
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
			}
		}
	}()
	return persister
}
func save(persister *Persister) {
	if persister.me == -1 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if persister.manageSN {
			persister.snapshotF.Truncate(0)
			persister.snapshotF.Seek(0, 0)
			persister.snapshotF.Write(persister.snapshot)
		}
		wg.Done()
	}()
	wg.Wait()
}
func (ps *Persister) Kill() {
	i := atomic.AddInt32(&ps.killed, 1)
	if i == 1 {
		save(ps)
		ps.mmap.Flush()
		ps.mmap.Unmap()
		ps.f.Sync()
		ps.f.Close()
		ps.snapshotF.Close()
	}
}
func (ps *Persister) ManageSnapshot() bool {
	return ps.manageSN
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
	if ps.mmap != nil {
		binary.LittleEndian.PutUint64(ps.mmap[:8], uint64(len(state)))
		copy(ps.mmap[8:len(state)+8], state)
	}
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
