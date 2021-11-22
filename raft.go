package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chronostasys/raft/labgob"
	"github.com/Chronostasys/raft/pb"
	"google.golang.org/grpc"
)

type TakeSnapshot func() []byte

func (rf *Raft) initLog() {
	rf.logger = log.New(io.Discard, "", 0)
}
func (rf *Raft) SetLogger(logger *log.Logger) {
	rf.logger = logger
}
func (rf *Raft) SetDebug(debug bool) {
	rf.debug = debug
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(atomic.LoadInt64(&rf.currentTerm)), atomic.LoadInt64(&rf.role) == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// rf.persistemu.RLock()
	// defer rf.persistemu.RUnlock()
	data := rf.encodeState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.logs) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.lastIncludedIndex) != nil ||
		d.Decode(&rf.lastIncludedTerm) != nil {
		log.Println("read persist err")
		return
	}
	rf.commitIndex = int64(rf.lastIncludedIndex)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
		rf.matchIndex[i] = rf.commitIndex
	}
	rf.matchIndex[rf.me] = 0
	rf.Log("logs", rf.logs, "votefor", rf.votedFor)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Log("get request vote from", args.CandidateID)
	// rf.checkTerm(args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.Log("candidate", args.CandidateID, "term is", args.Term, "too small, failed to vote")
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		atomic.StoreInt64(&rf.role, follower)
		rf.votedFor = -1
		rf.persist()
	}
	idx, term := rf.getPrevLogInfo()
	if !((idx <= args.LastLogIndex && term == args.LastLogTerm) || term < args.LastLogTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.Log("does not vote for", args.CandidateID, "for log not up to date", args.LastLogIndex, args.LastLogTerm, idx, term)
		return
	}
	rf.Log("args idx", args.LastLogIndex, "args term", args.LastLogTerm, "idx", idx, "term", term)
	if args.Term == rf.currentTerm {
		reply.Term = args.Term
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			atomic.StoreInt64(&rf.votedFor, args.CandidateID)
			reply.VoteGranted = true
			rf.Log("votes for", args.CandidateID)
			rf.persist()
			return
		}
		reply.VoteGranted = false
	} else {
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		atomic.StoreInt64(&rf.role, follower)
		atomic.StoreInt64(&rf.votedFor, args.CandidateID)
		rf.persist()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.Log("votes for", args.CandidateID)
	}
}
func (rf *Raft) applyCommits(commitIndex int64) {
	applied := false
	var wg *sync.WaitGroup
	if rf.WaitForDone {
		wg = &sync.WaitGroup{}
	}
	for i := rf.commitIndex; i < commitIndex && int(i) < len(rf.logs)+rf.lastIncludedIndex; i++ {
		rf.Log("apply commit", i+1)
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: int(i) + 1,
			Command:      rf.logs[int(i)-rf.lastIncludedIndex].Command,
		}
		if rf.WaitForDone {
			msg.wg = wg
			wg.Add(1)
		}
		atomic.AddInt64(&rf.commitIndex, 1)
		rf.applych <- msg
		// fmt.Println(rf.me, rf.logs[int(i)-rf.lastIncludedIndex].Command)
		rf.Log("applied commit", i+1, rf.logs[int(i)-rf.lastIncludedIndex].Command)
		applied = true
	}
	if rf.WaitForDone {
		wg.Wait()
	}
	if rf.MaxRaftStateSize != -1 && applied {
		rf.persist()
		rf.checkAndSaveSnapshot()
	}
}
func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.logs[len(rf.logs)-1].Index
}
func (rf *Raft) getIndexTerm(idx int) int64 {
	realidx := idx - 1 - rf.lastIncludedIndex
	if realidx == -1 {
		return rf.lastIncludedTerm
	} else if realidx < -1 {
		return -1
	}
	return rf.logs[realidx].Term
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

func (rf *Raft) SaveSnapshot(snapshot []byte) {
	// save snapshot & state
	realLastIdx := int(rf.commitIndex) - 1 - rf.lastIncludedIndex
	if realLastIdx != -1 {
		lastIncludedLog := rf.logs[realLastIdx]
		rf.lastIncludedIndex = lastIncludedLog.Index
		rf.lastIncludedTerm = lastIncludedLog.Term
	}
	rf.logs = rf.logs[realLastIdx+1:] // discard snapshoted logs
	data := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	rf.lastSNIdx = int(rf.commitIndex)
	// fmt.Println(rf.persister.RaftStateSize(), rf.persister.SnapshotSize())
}
func (rf *Raft) getHeartBeat() {
	select {
	case rf.heartbeatCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	if rf.role == candidate && args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		atomic.StoreInt64(&rf.role, follower)
		rf.votedFor = int64(args.LeaderID)
		rf.persist()
	}
	prevT := rf.currentTerm
	if !rf.checkTerm(args.Term, args.LeaderID) {
		reply.Success = false
		rf.Log("requester", args.LeaderID, "term too small")
		return
	}
	if rf.currentTerm > prevT {
		rf.persist()
	}
	rf.Log("leader commitindex", args.LeaderCommit)
	// rf.votedFor = int64(args.LeaderID)
	// heart beat
	if len(args.Entries) == 0 {
		rf.getHeartBeat()
		reply.Success = true
		if args.PrevLogindex == 0 ||
			(args.PrevLogindex <= rf.getLastLogIndex() && rf.getIndexTerm(args.PrevLogindex) == args.PrevLogTerm) {
			rf.applyCommits(args.LeaderCommit)
		} else {
			reply.Success = false
		}
		rf.Log("received heart beat", args.LeaderCommit, args.LeaderID)
		return
	}
	// command logs
	// fmt.Println(rf.me, rf.currentTerm, "appentry", len(args.Entries))
	rf.Log("start cmd log")
	if rf.role == leader {
		return
	}
MERLOG:
	if args.PrevLogindex-rf.lastIncludedIndex == 0 || (args.PrevLogindex <= rf.getLastLogIndex() && rf.getIndexTerm(args.PrevLogindex) == args.PrevLogTerm) {
		rf.logs = rf.logs[:(args.PrevLogindex - rf.lastIncludedIndex)]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
		// rf.checkAndSaveSnapshot(false)
		reply.Success = true
		rf.applyCommits(args.LeaderCommit)
		rf.getHeartBeat()
		// rf.Log("log succ", rf.logs)
	} else if args.PrevLogindex-rf.lastIncludedIndex < 0 {
		idx := rf.lastIncludedIndex - args.PrevLogindex
		if len(args.Entries) > idx {
			args.PrevLogindex = args.Entries[idx-1].Index
			args.PrevLogTerm = args.Entries[idx-1].Term
			args.Entries = args.Entries[idx:]
			if !(args.PrevLogindex-rf.lastIncludedIndex == 0 || (args.PrevLogindex <= rf.getLastLogIndex() && rf.getIndexTerm(args.PrevLogindex) == args.PrevLogTerm)) &&
				args.PrevLogindex-rf.lastIncludedIndex < 0 {
				log.Fatalln("bad", args.PrevLogTerm, args.PrevLogindex, args.Entries)
			}
			// fmt.Println(rf.me, args.PrevLogindex, rf.lastIncludedIndex, args.Entries)
			goto MERLOG
		}
		reply.Success = true
		return
	} else {
		rf.Log("cmd log failed")
		rf.buildXParams(args, reply)
		rf.getHeartBeat()
		reply.Success = false

	}
}

func (rf *Raft) buildXParams(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.PrevLogindex <= rf.getLastLogIndex() && rf.getIndexTerm(args.PrevLogindex) != args.PrevLogTerm {
		// conflict
		reply.XTerm = rf.getIndexTerm(args.PrevLogindex)
		reply.XLen = -1
		for i := len(rf.logs) - 2; i >= 0; i-- {
			if rf.logs[i].Term < reply.XTerm {
				reply.Xindex = i + 2 + rf.lastIncludedIndex
				break
			}
		}
		if reply.Xindex == 0 {
			if rf.lastIncludedTerm == reply.XTerm {
				reply.Xindex = rf.lastIncludedIndex
			} else {
				reply.Xindex = rf.lastIncludedIndex + 1
			}
		}
	} else {
		// log too short
		reply.XLen = len(rf.logs) + 1 + rf.lastIncludedIndex
		reply.XTerm = -1
		reply.Xindex = -1
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		// update term
		reply.Term = rf.currentTerm
	}()
	prevT := rf.currentTerm
	if !rf.checkTerm(args.Term, args.LeaderID) {
		// if it's term is smaller than us, do nothing and return!
		return
	}
	if rf.currentTerm > prevT {
		rf.persist()
	}
	rf.getHeartBeat()
	if args.Done {
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.logs = args.Logs
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: -1,
			IsSnapshot:   true,
			Command:      args.Data,
		}
		if rf.WaitForDone {
			msg.wg = &sync.WaitGroup{}
			msg.wg.Add(1)
		}
		rf.applych <- msg
		if rf.WaitForDone {
			msg.wg.Wait()
		}
		rf.commitIndex = int64(args.LastIncludedIndex)

		rf.SaveSnapshot(args.Data)
		rf.Info("installed snapshot from", args.LeaderID)

	} else {
		// TODO implement 分片传输
		log.Fatalln("not implement yet")
	}
}

func (rf *Raft) appendEntries(server int, reply *AppendEntriesReply) (succ bool) {
	locked := rf.appendmu[server].Lock()
	if !locked {
		return true
	}
	defer rf.appendmu[server].Unlock()
START:
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return false
	}
	if rf.role != leader {
		rf.mu.Unlock()
		return false
	}
	currNext := rf.nextIndex[server]
	var (
		idx  int
		term int64
		logs []Log
	)
	rf.Log("currnext", currNext)

	idx = int(currNext - 1)
	if rf.getLastLogIndex() <= idx {
		rf.mu.Unlock()
		return false
	}
	realIdx := idx - rf.lastIncludedIndex
	if realIdx >= 0 {
		logs = rf.logs[realIdx:]
	} else {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
			Done:              true,
			Offset:            -1, // TODO implement
			Logs:              rf.logs,
		}
		re := &InstallSnapshotReply{}
		// call install snapshot and goto START
		rf.Log("install", re)
		// fmt.Println(rf.me, "install", server)
		rf.mu.Unlock()
		ok := rf.sendInstallSnapshot(server, args, re)
		rf.mu.Lock()
		rf.Log("install returned", re)
		// if install succ, modify match index and next index
		if ok && re.Term == args.Term {
			atomic.StoreInt64(&rf.nextIndex[server], int64(args.LastIncludedIndex)+1)
			atomic.StoreInt64(&rf.matchIndex[server], int64(args.LastIncludedIndex))
			rf.leaderCommit()
		} else {
			rf.checkTerm(re.Term, -1)
			rf.persist()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
		goto START
	}
	if len(logs) == 0 {
		rf.mu.Unlock()
		return true
	}
	termidx := currNext - 2 - int64(rf.lastIncludedIndex)
	if termidx == -1 {
		term = rf.lastIncludedTerm
	} else if termidx >= 0 {
		term = rf.logs[termidx].Term
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogTerm:  term,
		PrevLogindex: idx,
		LeaderCommit: rf.commitIndex,
		Entries:      logs,
	}
	rf.mu.Unlock()
	if rf.killed() {
		return false
	}
	ok := rf.sendAppendEntries(server, args, reply)
	rf.Log("server", server, "args", args, "reply", reply)
	rf.mu.Lock()
	if !ok {
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		goto START
	}
	if reply.Term > rf.currentTerm {
		rf.checkTerm(reply.Term, -1)
		rf.persist()
		rf.mu.Unlock()
		return false
	}
	if rf.role != leader {
		rf.mu.Unlock()
		return false
	}
	if ok && !reply.Success && rf.nextIndex[server] > 1 {
		// fmt.Println(reply.XLen, reply.XTerm, reply.Xindex)
		if reply.XLen > 0 {
			atomic.StoreInt64(&rf.nextIndex[server], int64(reply.XLen))
			atomic.StoreInt64(&rf.matchIndex[server], int64(reply.XLen)-1)
		} else if reply.XTerm > 0 {
			atomic.StoreInt64(&rf.nextIndex[server], int64(reply.Xindex))
			atomic.StoreInt64(&rf.matchIndex[server], int64(reply.Xindex)-1)
		} else {
			// fmt.Println("dec")
			num := atomic.AddInt64(&rf.nextIndex[server], -1)
			atomic.StoreInt64(&rf.matchIndex[server], num-1)
		}
		rf.mu.Unlock()
		goto START
	}
	if reply.Success {
		atomic.AddInt64(&rf.nextIndex[server], int64(len(args.Entries)))
		atomic.AddInt64(&rf.matchIndex[server], int64(len(args.Entries)))
		rf.leaderCommit()
		rf.mu.Unlock()
		return true
	}
	rf.mu.Unlock()
	return ok && reply.Success
}

func (rf *Raft) leaderCommit() {
	// auto commit commands that reached agreement on majority of servers
	// see the last point in raft extended papaer Figure2
	ss := make([]int64, len(rf.matchIndex))
	copy(ss, rf.matchIndex)
	// TODO as there's only one member change the value eachtime,
	// there should be a better way than resort every time the matchindex changed
	sort.Slice(ss, func(i, j int) bool {
		return ss[i] < ss[j]
	})
	rf.Log("match index", rf.matchIndex)
	commitIDX := ss[len(ss)/2+1]
	idx := commitIDX - 1 - int64(rf.lastIncludedIndex)
	// check Figure 8
	if idx >= 0 && rf.logs[idx].Term == rf.currentTerm {
		rf.applyCommits(commitIDX)
		rf.Log("leader applied commit", commitIDX)
	}
}

func (rf *Raft) sendHeartbeat(server int) bool {
	rf.mu.Lock()
	if rf.role != leader {
		rf.mu.Unlock()
		return false
	}
	re := &AppendEntriesReply{}
	var t int64 = 0
	if rf.commitIndex-1 >= 0 {
		termidx := rf.commitIndex - 1 - int64(rf.lastIncludedIndex)
		if termidx == -1 {
			t = rf.lastIncludedTerm
		} else {
			if termidx < 0 {
				log.Fatalln(rf.commitIndex, rf.lastIncludedIndex)
			}
			t = rf.logs[termidx].Term
		}
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderCommit: rf.commitIndex,
		LeaderID:     rf.me,
		PrevLogTerm:  t,
		PrevLogindex: int(rf.commitIndex),
	}
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, args, re)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prev := rf.currentTerm
	rf.checkTerm(re.Term, -1)
	if prev < rf.currentTerm {
		rf.persist()
	}
	return ok && re.Success
}
func (rf *Raft) checkTerm(term int64, leader int) bool {
	if term > rf.currentTerm {
		atomic.StoreInt64(&rf.currentTerm, term)
		atomic.StoreInt64(&rf.role, follower)
		atomic.StoreInt64(&rf.votedFor, int64(leader))
		return true
	} else if term < rf.currentTerm {
		return false
	} else if rf.votedFor != int64(leader) && rf.votedFor != -1 {
		return false
	}
	return true
}

func (rf *Raft) getPrevLogInfo() (idx int, term int64) {
	idx = len(rf.logs)
	if idx == 0 {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	term = rf.logs[idx-1].Term
	idx = idx + rf.lastIncludedIndex
	return
}

// func (rf *Raft) PersistAndSaveSnapshot(lock bool) {
// 	rf.persist()
// 	rf.checkAndSaveSnapshot(lock)
// }
func (rf *Raft) checkAndSaveSnapshot() bool {
	if rf.MaxRaftStateSize != -1 && rf.persister.RaftStateSize() > rf.MaxRaftStateSize {
		if rf.lastSNIdx >= int(rf.commitIndex)-rf.MinCommitBTWSnapshots {
			return false
		}
		rf.SaveSnapshot(rf.SnapshotFunc())
		return true
	}
	return false
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader := false
	idx := -1
	term := rf.currentTerm
	// Your code here (2B).
	if rf.role == leader {
		isleader = true
		if len(rf.logs) != 0 {
			idx = rf.logs[len(rf.logs)-1].Index + 1
		} else {
			idx = rf.lastIncludedIndex + 1
		}
		rf.logs = append(rf.logs, Log{Command: command, Term: term, Index: idx})
		rf.persist()
		// rf.checkAndSaveSnapshot(false)
		rf.Log("get command", command, "idx", idx)
		go func() {
			rf.morethanHalf(func(id int) bool {
				rf.Log("start appentry", id)
				re := &AppendEntriesReply{}
				ok := rf.appendEntries(id, re)
				rf.Log("done appentry", id)
				return ok && re.Success

			})

		}()
	}
	return idx, int(term), isleader
}
func (rf *Raft) StartWithCache(command interface{}) bool {
	// if atomic.LoadInt64(&rf.role) != leader {
	// 	return false
	// }
	rf.cachemu.Lock()
	rps := atomic.AddInt64(&rf.reqPer100ms, 1)
	succCh := rf.cacheSuccCh
	failCh := rf.cacheFailCh
	rf.cache[rf.cacheidx] = command
	rf.cacheidx++
	if rf.cacheThreshold < 100 {
		// low latency mode
		if rps >= 100 {
			rf.cacheThreshold = 1000
		}
		_, _, succ := rf.Start(command)
		rf.cacheidx = 0
		rf.cachemu.Unlock()
		return succ
	} else if rf.cacheidx >= rf.cacheThreshold {
		select {
		case rf.sendCh <- struct{}{}:
		default:
		}
		succ := rf.StartMulti(rf.cache[:rf.cacheidx]...)
		rf.cacheidx = 0
		if succ {
			close(succCh)
			rf.cacheSuccCh = make(chan struct{})
		} else {
			close(failCh)
			rf.cacheFailCh = make(chan struct{})
		}
		if rf.cacheThreshold < 100 {
			rf.cacheThreshold = 2 * rf.cacheThreshold
		} else if rf.cacheThreshold < 1000 {
			rf.cacheThreshold = int(float64(rf.cacheThreshold) * 1.5)
		} else {
			rf.cacheThreshold = int(float64(rf.cacheThreshold) * 1.2)
		}
		if rf.cacheThreshold > len(rf.cache) {
			rf.cacheThreshold = len(rf.cache)
		}
		rf.cachemu.Unlock()
		return succ
	}
	rf.cachemu.Unlock()
	select {
	case <-succCh:
		return true
	case <-failCh:
		return false

	}
}
func (rf *Raft) StartMulti(commands ...interface{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader := false
	idx := -1
	term := rf.currentTerm
	// Your code here (2B).
	if rf.role == leader {
		isleader = true
		if len(rf.logs) != 0 {
			idx = rf.logs[len(rf.logs)-1].Index + 1
		} else {
			idx = rf.lastIncludedIndex + 1
		}
		newlogs := make([]Log, len(commands))
		for i, v := range commands {
			newlogs[i] = Log{Command: v, Term: term, Index: idx}
			idx++
		}
		rf.logs = append(rf.logs, newlogs...)
		rf.persist()
		// rf.checkAndSaveSnapshot(false)
		rf.Log("get commands", commands)
		go func() {
			rf.morethanHalf(func(id int) bool {
				rf.Log("start appentry", id)
				re := &AppendEntriesReply{}
				ok := rf.appendEntries(id, re)
				rf.Log("done appentry", id)
				return ok && re.Success

			})

		}()
	}
	return isleader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.killo.Do(func() {
		close(rf.killch)
	})
	// Your code here, if desired.
	rf.persister.Kill()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Log(msg ...interface{}) {
	if rf.debug {
		logs := append([]interface{}{"[debug]", rf.me, "in term", rf.currentTerm}, msg...)
		rf.logger.Println(logs...)
	}
}

func (rf *Raft) Info(msg ...interface{}) {
	logs := append([]interface{}{"[info]", "term", rf.currentTerm}, msg...)
	rf.logger.Println(logs...)
}

// this function will execute job in parallel for every single server
// except caller, and will return when more than half of the job returned
// true(the job target caller is preset to true). However, some jobs may still
// execute in background. All jobs are garented to execute to end
func (rf *Raft) morethanHalf(job jobfunc) bool {
	chsucc, chfail := make(chan struct{}), make(chan struct{})
	var half int32 = int32(len(rf.peers) / 2)
	var succ int32
	var fail int32
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(id int) {
				if rf.killed() {
					return
				}
				re := job(id)

				if re {
					num := atomic.AddInt32(&succ, 1)
					if num == half {
						close(chsucc)
					}
				} else {
					num := atomic.AddInt32(&fail, 1)
					if num == half {
						close(chfail)
					}
				}
			}(i)
		}
	}
	select {
	case <-chsucc:
		return true
	case <-chfail:
		return false
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []RPCEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:               &sync.Mutex{},
		votedFor:         -1,
		logs:             make([]Log, 0),
		role:             follower,
		heartbeatCh:      make(chan struct{}, 1),
		applych:          applyCh,
		nextIndex:        make([]int64, len(peers)),
		matchIndex:       make([]int64, len(peers)),
		appendmu:         make([]*appendMutex, len(peers)),
		idxmu:            &sync.Mutex{},
		commitIndex:      0,
		electionChan:     make(chan struct{}, 1),
		MaxRaftStateSize: -1,
		killch:           make(chan struct{}),
		cache:            make([]interface{}, 10000),
		cachemu:          &sync.Mutex{},
		cacheSuccCh:      make(chan struct{}),
		cacheFailCh:      make(chan struct{}),
		sendCh:           make(chan struct{}, 1),
		cacheThreshold:   1000,
	}
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.appendmu[i] = &appendMutex{mu: &sync.Mutex{}}
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.initLog()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {
			time.Sleep(time.Millisecond * time.Duration(300+rand.Intn(300)))
			select {
			case rf.electionChan <- struct{}{}:
			default:
			}
		}
	}()
	go func() {
		for {
			<-rf.electionChan
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.role == leader {
				rf.mu.Unlock()
				rf.Log("already leader, skip election")
				continue
			}
			heartbeat := false
			select {
			case <-rf.heartbeatCh:
				heartbeat = true
				rf.Log("get heartbeat succ")
			default:
				rf.Log("get heartbeat failed")
			}
			if (!heartbeat && rf.role == follower) || rf.role == candidate {
				atomic.StoreInt64(&rf.role, candidate)
				atomic.StoreInt64(&rf.votedFor, int64(rf.me))
				rf.currentTerm++
				rf.Log("start election")
				currterm := rf.currentTerm
				idx, term := rf.getPrevLogInfo()
				rf.persist()
				go func() {

					ok := rf.morethanHalf(func(id int) bool {
						re := &RequestVoteReply{}
						ok := rf.sendRequestVote(id, &RequestVoteArgs{
							Term:         currterm,
							CandidateID:  int64(rf.me),
							LastLogIndex: idx,
							LastLogTerm:  term,
						}, re)
						rf.mu.Lock()
						if re.Term > rf.currentTerm {
							atomic.StoreInt64(&rf.currentTerm, re.Term)
							atomic.StoreInt64(&rf.role, follower)
							// trigger an immediate reelection if our term is not up to date
						}
						rf.persist()
						rf.mu.Unlock()
						if ok && re.VoteGranted {
							rf.Log("get vote from", id)
						}

						return ok && re.VoteGranted
					})
					rf.mu.Lock()
					if currterm != rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					if ok {
						rf.Log("become leader")
						rf.Info("become leader")
						atomic.StoreInt64(&rf.role, leader)
						go func() {
							for {
								if rf.killed() {
									return
								}
								if atomic.LoadInt64(&rf.role) == leader {
									rf.morethanHalf(func(id int) bool {
										return rf.sendHeartbeat(id)
									})
								}
								time.Sleep(time.Millisecond * 100)
							}
						}()
					}
					rf.mu.Unlock()
				}()
			}
			rf.mu.Unlock()
		}
	}()

	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			rp100ms := atomic.LoadInt64(&rf.reqPer100ms)
			atomic.AddInt64(&rf.reqPer100ms, -rp100ms)
			select {
			case <-rf.sendCh:
			default:
				go func(rp100ms int64) {
					rf.cachemu.Lock()
					defer rf.cachemu.Unlock()
					if rf.cacheidx != 0 {
						succCh := rf.cacheSuccCh
						failCh := rf.cacheFailCh
						succ := rf.StartMulti(rf.cache[:rf.cacheidx]...)
						if rp100ms < int64(rf.cacheidx) {
							rp100ms = int64(rf.cacheidx)
						}
						rf.cacheidx = 0
						if succ {
							close(succCh)
							rf.cacheSuccCh = make(chan struct{})
						} else {
							close(failCh)
							rf.cacheFailCh = make(chan struct{})
						}
						if rp100ms <= 100 {
							rf.lowcount++
							// low latency mode
							if rf.lowcount > 5 {
								rf.cacheThreshold = 1
								rf.lowcount = 0
							}
						} else {
							rf.cacheThreshold = int(rp100ms)
						}
						if rf.cacheThreshold > len(rf.cache) {
							rf.cacheThreshold = len(rf.cache)
						}
					}
				}(rp100ms)
			}
		}
	}()

	return rf
}

func (rf *Raft) Serve(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln(err.Error())
	}
	rf.l = l
	s := &RaftRPCServer{
		Rf: rf,
	}
	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, s)
	grpcServer.Serve(l)
}

func (rf *Raft) Close() {
	rf.Kill()
	rf.l.Close()
}
