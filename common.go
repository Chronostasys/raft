package raft

import (
	"bytes"
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/Chronostasys/raft/labgob"
	"github.com/Chronostasys/raft/pb"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	IsSnapshot   bool
	wg           *sync.WaitGroup
}

func (msg ApplyMsg) Done() {
	msg.wg.Done()
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
	lowcount          int
	killo             sync.Once

	lastSNIdx int

	// public
	MaxRaftStateSize      int
	SnapshotFunc          TakeSnapshot
	WaitForDone           bool
	MinCommitBTWSnapshots int
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
	pb.UnimplementedRaftServiceServer
	Rf *Raft
}

func gobEncode(i interface{}) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(i)
	return w.Bytes()
}
func gobDecode(b []byte, i interface{}) {
	r := bytes.NewBuffer(b)
	d := labgob.NewDecoder(r)
	d.Decode(i)
}

func (rf *RaftRPCServer) InstallSnapshot(ctx context.Context, args *pb.GobMessage) (repl *pb.GobMessage, err error) {
	a := &InstallSnapshotArgs{}
	gobDecode(args.GetMsg(), a)
	re := &InstallSnapshotReply{}
	rf.Rf.InstallSnapshot(a, re)

	repl = &pb.GobMessage{
		Msg: gobEncode(re),
	}
	return
}
func (rf *RaftRPCServer) AppendEntries(ctx context.Context, args *pb.GobMessage) (repl *pb.GobMessage, err error) {
	a := &AppendEntriesArgs{}
	gobDecode(args.GetMsg(), a)
	re := &AppendEntriesReply{}
	rf.Rf.AppendEntries(a, re)
	repl = &pb.GobMessage{
		Msg: gobEncode(re),
	}
	return
}
func (rf *RaftRPCServer) RequestVote(ctx context.Context, args *pb.GobMessage) (repl *pb.GobMessage, err error) {
	a := &RequestVoteArgs{}
	gobDecode(args.GetMsg(), a)
	re := &RequestVoteReply{}
	rf.Rf.RequestVote(a, re)
	repl = &pb.GobMessage{
		Msg: gobEncode(re),
	}
	return
}
