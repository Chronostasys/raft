package raft

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	IsSnapshot   bool
	Ch           chan struct{}
}

const (
	follower  int64 = 0
	candidate int64 = 1
	leader    int64 = 2
)

type Log struct {
	Command interface{}
	Term    int64
	Index   int
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderID     int
	PrevLogTerm  int64
	PrevLogindex int
	Entries      []Log
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
	XTerm   int64
	Xindex  int
	XLen    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        *sync.Mutex // Lock to protect shared access to this peer's state
	peers     []RPCEnd    // RPC end points of all peers
	persister *Persister  // Object to hold this peer's persisted state
	me        int         // this peer's index into peers[]
	dead      int32       // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int64
	votedFor          int64 // -1 for no vote
	commitIndex       int64
	role              int64
	logs              []Log
	heartbeatCh       chan struct{}
	applych           chan ApplyMsg
	nextIndex         []int64
	matchIndex        []int64
	appendmu          []*appendMutex
	idxmu             *sync.Mutex
	killch            chan struct{}
	electionChan      chan struct{}
	lastIncludedIndex int
	lastIncludedTerm  int64
	logger            *log.Logger
	debug             bool
	l                 net.Listener
	cache             []interface{}
	cachemu           *sync.Mutex
	cacheSuccCh       chan struct{}
	cacheFailCh       chan struct{}
	cacheidx          int
	sendCh            chan struct{}
	cacheThreshold    int
	reqPer100ms       int64

	// public
	MaxRaftStateSize int
	SnapshotFunc     TakeSnapshot
	WaitForDone      bool
}

type appendMutex struct {
	mu    *sync.Mutex
	queue int64
}

func (mu *appendMutex) Lock() bool {
	qu := atomic.AddInt64(&mu.queue, 1)
	if qu > 2 {
		atomic.AddInt64(&mu.queue, -1)
		return false
	}
	mu.mu.Lock()
	return true
}

func (mu *appendMutex) Unlock() {
	atomic.AddInt64(&mu.queue, -1)
	mu.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateID  int64
	LastLogIndex int
	LastLogTerm  int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}
type InstallSnapshotArgs struct {
	Term              int64
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int64
	Data              []byte
	Done              bool
	Offset            int
	Logs              []Log
}
type InstallSnapshotReply struct {
	Term int64
}
type jobfunc func(id int) bool

type RaftRPCServer struct {
	Rf *Raft
}

func (rf *RaftRPCServer) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) (err error) {
	rf.Rf.InstallSnapshot(args, reply)
	return
}
func (rf *RaftRPCServer) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) (err error) {
	rf.Rf.AppendEntries(args, reply)
	return
}
func (rf *RaftRPCServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) (err error) {
	rf.Rf.RequestVote(args, reply)
	return
}
