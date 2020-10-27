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
	//"log"
	//"bytes"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "bytes"
import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandData  []byte
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm           int
	votedFor              int
	log                   []LogEntry
	applyCh               chan ApplyMsg
	role                  int
	heartBeatWaitDuration int
	MinWaitTime           int
	MaxWaitTime           int

	//记录follower在这个currentTerm 的投票的时间，或收到真leader的appendentry的时间
	electionTimeouUpdatedAt time.Time
	leaderID                int
	commitIndex             int
	lastApplied             int
	nextIndex               []int
	matchIndex              []int
	lastSnapshotIndex       int
	lastSnapshotTerm        int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) becomeCandidate() {
	rf.role = CANDIDATE
	rf.leaderID = -1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimeouUpdatedAt = time.Now()
	rf.persist()
}

func (rf *Raft) becomeFollower(term int) {
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.leaderID = -1
	rf.votedFor = -1
	rf.electionTimeouUpdatedAt = time.Now()
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.role = LEADER
	rf.leaderID = rf.me
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = rf.lastSnapshotIndex
		rf.nextIndex[i] = len(rf.log) + rf.lastSnapshotIndex
	}
	rf.persist()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	isleader = rf.role == LEADER
	term = rf.currentTerm

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	raftState := rf.GetRaftState()
	rf.persister.SaveRaftState(raftState)
}

func (rf *Raft) GetRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.lastSnapshotIndex)
	data := w.Bytes()
	return data
}

func (rf *Raft) readRaftStatePersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var log []LogEntry
	var votedFor int
	var lastSnapshotTerm int
	var lastSnapshotIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&lastSnapshotIndex) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.lastSnapshotIndex = lastSnapshotIndex

		rf.commitIndex = rf.lastSnapshotIndex
		rf.lastApplied = rf.lastSnapshotIndex
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.readRaftStatePersist(data)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

func (rf *Raft) attemptElection(term int, LastLogIndex int, LastLogTerm int) {
	votes := 1
	finished := 1
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: LastLogIndex,
				LastLogTerm:  LastLogTerm,
			}
			var reply = RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			voteGranted := reply.VoteGranted
			mu.Lock()
			defer mu.Unlock()
			if voteGranted {
				votes++
			}
			finished++
			cond.Broadcast()
		}(server)
	}
	mu.Lock()
	for votes < len(rf.peers)/2+1 && finished != len(rf.peers) {
		cond.Wait()
	}
	mu.Unlock()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != term || rf.role != CANDIDATE {
		return
	}
	if votes >= len(rf.peers)/2+1 {
		rf.becomeLeader()
	}
}

func (rf *Raft) runCandidate() {
	rf.mu.Lock()
	if rf.role != CANDIDATE {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	LastLogTerm, LastLogIndex := rf.lastLogTermIndex()
	rf.mu.Unlock()

	go rf.attemptElection(term, LastLogIndex, LastLogTerm)
	for {
		rf.mu.Lock()
		if rf.role != CANDIDATE || term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		randNum := rf.randNum()
		dt := rf.electionTimeouUpdatedAt.Add(time.Millisecond * time.Duration(randNum))
		if time.Now().After(dt) {
			rf.becomeCandidate()
			term := rf.currentTerm
			LastLogTerm, LastLogIndex := rf.lastLogTermIndex()
			rf.mu.Unlock()

			go rf.attemptElection(term, LastLogIndex, LastLogTerm)
			continue
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) randNum() int {
	rand.Seed(time.Now().UnixNano())
	randNum := rand.Intn(rf.MaxWaitTime-rf.MinWaitTime) + rf.MinWaitTime

	return randNum
}

//
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果candidate的任期 小于 这个server的任期， 则不会给这个candidate投票
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	if rf.currentTerm < args.Term {
		//println(time.Now().UnixNano() / 1e6,rf.me,"receive RV in term", rf.currentTerm, "[args] term" , args.Term, "server id", args.CandidateId)
		rf.becomeFollower(args.Term)
	}

	if rf.votedFor == args.CandidateId && args.Term == rf.currentTerm {
		reply.VoteGranted = true
		rf.electionTimeouUpdatedAt = time.Now()
		return
	}

	if rf.votedFor == -1 {
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.electionTimeouUpdatedAt = time.Now()
			rf.persist()
			return
		}
	}
	// Your code here (2A, 2B).
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.peers[server].Call("Raft.RequestVote", args, reply)

	//如果其他server的currentTerm 大于 leader，要把leader设置位follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != CANDIDATE || rf.currentTerm != args.Term {
		return
	}
	if rf.currentTerm < reply.Term {
		rf.becomeFollower(reply.Term)
	}
	return
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

	term := rf.currentTerm
	isLeader := rf.role == LEADER
	_, lastIndex := rf.lastLogTermIndex()
	index := lastIndex + 1

	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	return index, term, isLeader
	// Your code here (2B).
	//return index, term, isLeader
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	term := rf.log[len(rf.log)-1].Term
	index := rf.lastSnapshotIndex + len(rf.log) - 1

	return term, index
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartBeatWaitDuration = 100

	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.role = FOLLOWER
	rf.currentTerm = 1
	rf.leaderID = -1
	rf.votedFor = -1
	rf.lastSnapshotIndex = 0
	rf.electionTimeouUpdatedAt = time.Now()

	rf.MinWaitTime = 150
	rf.MaxWaitTime = 300

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Run()
	go rf.ApplyLoop()
	return rf
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommit      int
	LastSnapshotIndex int
}

type AppendEntriesReply struct {
	Term                  int
	Success               bool
	NeedDecreaseNextIndex bool
	XLen                  int
	XIndex                int
	XTerm                 int
}

func (rf *Raft) sendAppendEntries(peerIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER || rf.currentTerm != args.Term {
		return
	}
	//如果其他server的currentTerm 大于 leader，要把leader设置位follower
	if rf.currentTerm < reply.Term {
		rf.becomeFollower(reply.Term)
	} else {
		if reply.Success {
			replyNextIndex := args.PrevLogIndex + 1 + len(args.Entries)
			//考虑request 和 response乱序了
			//例如 request a，request b,response b，response a
			if replyNextIndex < rf.nextIndex[peerIndex] {
				return
			}
			rf.nextIndex[peerIndex] = replyNextIndex
			rf.matchIndex[peerIndex] = rf.nextIndex[peerIndex] - 1

			//需要保证论文图2 右下角的这个条件: log[N].term == currentTerm
			if rf.matchIndex[peerIndex] > rf.commitIndex && len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
				cnt := 0
				for otherPeerIndex, _ := range rf.peers {
					if otherPeerIndex == peerIndex {
						continue
					}
					if rf.matchIndex[otherPeerIndex] >= rf.matchIndex[peerIndex] {
						cnt += 1
					}
					if cnt >= len(rf.peers)/2 {
						rf.commitIndex = rf.matchIndex[peerIndex]
						break
					}
				}
			}
		} else if reply.NeedDecreaseNextIndex {
			rf.nextIndex[peerIndex] = rf.getNextIndex(reply.XLen, reply.XIndex, reply.XTerm, args)
		}
	}
}

func (rf *Raft) getInstallSnapshotArgs() InstallSnapshotArgs {
	installSnapshotArgs := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		LastLogCommand:    rf.log[0].Command,
		Data:              rf.persister.ReadSnapshot(),
	}
	return installSnapshotArgs
}

func (rf *Raft) getAppendEntriesArgs(peerIndex int) AppendEntriesArgs {
	realNextIndex := rf.nextIndex[peerIndex] - rf.lastSnapshotIndex
	entries := make([]LogEntry, len(rf.log[realNextIndex:]))
	copy(entries, rf.log[realNextIndex:])
	args := AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      rf.nextIndex[peerIndex] - 1,
		PrevLogTerm:       rf.log[rf.nextIndex[peerIndex]-1-rf.lastSnapshotIndex].Term,
		LeaderCommit:      rf.commitIndex,
		Entries:           entries,
		LastSnapshotIndex: rf.lastSnapshotIndex,
	}
	return args
}

//1. 每隔一段时间，向非leader节点发送append entry心跳
func (rf *Raft) runLeader() {
	for {
		for peerIndex, _ := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			rf.mu.Lock()

			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
			//判断是否需要发送 InstallSnapshot
			if rf.nextIndex[peerIndex] <= rf.lastSnapshotIndex {
				installSnapshotArgs := rf.getInstallSnapshotArgs()
				rf.mu.Unlock()
				var installSnapshotReply = InstallSnapshotReply{}
				go rf.sendInstallSnapshot(peerIndex, &installSnapshotArgs, &installSnapshotReply)
			} else {
				appendEntriesArgs := rf.getAppendEntriesArgs(peerIndex)
				rf.mu.Unlock()
				var appendEntriesReply = AppendEntriesReply{}
				go rf.sendAppendEntries(peerIndex, &appendEntriesArgs, &appendEntriesReply)
			}
		}
		time.Sleep(time.Millisecond * time.Duration(rf.heartBeatWaitDuration))
	}
}

//某个term在log的第一个、最后一个logentry的index
func (rf *Raft) termIndexesInLog(term int) (int, int) {
	firstTermIndex, lastTermIndex := -1, -1
	hasTerm := false
	for logIndex, _ := range rf.log {
		if rf.log[logIndex].Term == term {
			if !hasTerm {
				hasTerm = true
				firstTermIndex = logIndex + rf.lastSnapshotIndex
				lastTermIndex = logIndex + rf.lastSnapshotIndex
				continue
			}
		} else if hasTerm {
			lastTermIndex = logIndex - 1 + rf.lastSnapshotIndex
			break
		}
	}
	return firstTermIndex, lastTermIndex
}

//XTerm XIndex  XLen这三个参数来自 AE 的receiver
//XTerm:  term in the conflicting entry (if any)
//XIndex: index of first entry with that term (if any)
//XLen:   log length
//Case 1 (leader doesn't have XTerm):
//nextIndex = XIndex
//Case 2 (leader has XTerm):
//nextIndex = leader's last entry for XTerm
//Case 3 (follower's log is too short):
//nextIndex = XLen
//参考  https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt
func (rf *Raft) getNextIndex(XLen int, XIndex int, XTerm int, args *AppendEntriesArgs) int {
	if XLen-1 < args.PrevLogIndex {
		return XLen
	}
	firstIndex, lastIndex := rf.termIndexesInLog(XTerm)
	if firstIndex == -1 {
		return XIndex
	}
	return lastIndex
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	LastLogCommand    interface{}
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) getRelativeLogIndex(index int) int {
	// index of rf.log
	return index - rf.lastSnapshotIndex
}

func (rf *Raft) sendInstallSnapshot(peerIndex int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[peerIndex].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		} else {
			if rf.matchIndex[peerIndex] < args.LastIncludedIndex {
				rf.matchIndex[peerIndex] = args.LastIncludedIndex
			}
			rf.nextIndex[peerIndex] = rf.matchIndex[peerIndex] + 1
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.lastSnapshotIndex {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	rf.leaderID = args.LeaderId
	rf.electionTimeouUpdatedAt = time.Now()

	// 6. if existing log entry has same index and term with
	//    last log entry in snapshot, retain log entries following it
	lastIncludedRelativeIndex := rf.getRelativeLogIndex(args.LastIncludedIndex)
	if len(rf.log) > lastIncludedRelativeIndex &&
		rf.log[lastIncludedRelativeIndex].Term == args.LastIncludedTerm {
		rf.log = rf.log[lastIncludedRelativeIndex:]
	} else {
		// 7. discard entire log
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: args.LastLogCommand}}
	}
	rf.lastSnapshotIndex = args.LastIncludedIndex

	//rf.snapshot = args.Data
	if rf.commitIndex < rf.lastSnapshotIndex {
		rf.commitIndex = rf.lastSnapshotIndex - 1
	}
	if rf.lastApplied < rf.lastSnapshotIndex {
		rf.lastApplied = rf.lastSnapshotIndex - 1
	}
	rf.persister.SaveStateAndSnapshot(rf.GetRaftState(), args.Data)

	installSnapshotCommand := ApplyMsg{
		CommandIndex: rf.lastSnapshotIndex,
		Command:      "InstallSnapshot",
		CommandValid: false,
		CommandData:  args.Data,
	}
	rf.mu.Unlock()
	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(installSnapshotCommand)
}

func (rf *Raft) SaveSnapshot(snapshot []byte, lastSnapshotIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastSnapshotIndex <= rf.lastSnapshotIndex {
		return
	}
	relativeLastSnapshotIndex := rf.getRelativeLogIndex(lastSnapshotIndex)
	rf.lastSnapshotTerm = rf.log[relativeLastSnapshotIndex].Term
	rf.log = rf.log[relativeLastSnapshotIndex:]
	rf.lastSnapshotIndex = lastSnapshotIndex
	raftState := rf.GetRaftState()

	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_, lastLogIndex := rf.lastLogTermIndex()
	//rule 1:  Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm || rf.role != FOLLOWER {
		rf.becomeFollower(args.Term)
	}
	rf.electionTimeouUpdatedAt = time.Now()
	// Save the current leader
	rf.leaderID = args.LeaderId

	//在rf.log中实际的index
	argsRealPrevLogIndex := args.PrevLogIndex - rf.lastSnapshotIndex

	if args.PrevLogIndex <= rf.lastSnapshotIndex {
		reply.Success = true
		if args.PrevLogIndex+len(args.Entries) > rf.lastSnapshotIndex {
			startIdx := rf.lastSnapshotIndex - args.PrevLogIndex
			rf.log = rf.log[:1]
			rf.log = append(rf.log, args.Entries[startIdx:]...)
			rf.persist()
		}
		return
	}

	//rule 2:  Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	//rule 3:  If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	if args.PrevLogIndex > lastLogIndex || rf.log[argsRealPrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NeedDecreaseNextIndex = true
		reply.XLen = lastLogIndex + 1
		if args.PrevLogIndex <= lastLogIndex {
			reply.XTerm = rf.log[argsRealPrevLogIndex].Term
			reply.XIndex, _ = rf.termIndexesInLog(rf.log[argsRealPrevLogIndex].Term)
		}
		return
	}

	// rule 4:  Append any new entries not already in the log compare from rf.log[args.PrevLogIndex + 1]
	unmatch_idx := -1
	for idx := range args.Entries {
		if len(rf.log)+rf.lastSnapshotIndex < (args.PrevLogIndex+2+idx) ||
			rf.log[(argsRealPrevLogIndex+1+idx)].Term != args.Entries[idx].Term {
			// unmatch log found
			unmatch_idx = idx
			break
		}
	}

	if unmatch_idx != -1 {
		rf.log = rf.log[:(argsRealPrevLogIndex + 1 + unmatch_idx)]
		rf.log = append(rf.log, args.Entries[unmatch_idx:]...)
		rf.persist()
	}

	//rule 5:  If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		_, lastLogIndex := rf.lastLogTermIndex()
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	return
}

func min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

func (rf *Raft) runFollower() {
	for {
		rf.mu.Lock()
		if rf.role != FOLLOWER {
			rf.mu.Unlock()
			return
		}
		//如果time.now - rf.electionTimeouUpdatedAt > 150--300ms间的时间 ，则变为candidate
		randNum := rf.randNum()
		dt := rf.electionTimeouUpdatedAt.Add(time.Millisecond * time.Duration(randNum))
		if time.Now().After(dt) {
			rf.becomeCandidate()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) ApplyLoop() {
	for {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			i := rf.lastApplied + 1
			for i <= rf.commitIndex {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i-rf.lastSnapshotIndex].Command,
					CommandIndex: i,
				}
				i += 1
				rf.lastApplied += 1
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

func (rf *Raft) Run() {
	for {
		//当更新currentTerm后，要进入下面的switch逻辑
		switch rf.role {
		case LEADER:
			rf.runLeader()
		case FOLLOWER:
			rf.runFollower()
		case CANDIDATE:
			rf.runCandidate()
		default:
			panic("Invalid peer state!")
		}
	}
}
