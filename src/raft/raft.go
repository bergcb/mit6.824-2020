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
}

type LogEntry struct {
	Term    int
	Idx     int // only for debug log
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

	currentTerm int
	votedFor    int
	log         []LogEntry
	applyCh     chan ApplyMsg

	role                  int
	electionTimer         *time.Timer
	heartBeatWaitDuration int
	electWaitDuration     int
	MinWaitTime           int
	MaxWaitTime           int

	//记录follower在这个currentTerm 的投票的时间，或收到真leader的appendentry的时间
	lastVoteOrRecvAppendEntryTime time.Time
	leaderID                      int

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) becomeCandidate() {
	rf.role = CANDIDATE
	rf.leaderID = -1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastVoteOrRecvAppendEntryTime = time.Now()

	rf.persist()
	//DPrintf("%s change to candidate", rf)
}

func (rf *Raft) becomeFollower(term int) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.leaderID = -1
	rf.votedFor = -1
	rf.lastVoteOrRecvAppendEntryTime = time.Now()
	//rf.lastVoteOrRecvAppendEntryTime = time.Now()
	//println(time.Now().UnixNano() / 1e6," become follower","server ",rf.me,"term", rf.currentTerm, "its role", rf.role)

	rf.persist()
	//DPrintf("%s change to follower with term %d", rf, term)
}

func (rf *Raft) becomeLeader() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//println(time.Now().UnixNano() / 1e6," become leader","server ",rf.me,"term", rf.currentTerm, "its role", rf.role)
	//for idx,_ :=range rf.log {
	//	log.Printf("[%v] idx: %v, command: %v, currentterm: %v", rf.me, idx, rf.log[idx].Command , rf.currentTerm)
	//}
	rf.role = LEADER
	rf.leaderID = rf.me
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	rf.persist()
	//DPrintf("%s change to leader", rf)
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
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	//e.Encode(rf.commitIndex)
	//e.Encode(rf.lastApplied)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	//println("in persist()")
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//println("in readPersist()")
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	//var lastApplied int
	//var commitIndex int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		//d.Decode(&lastApplied) != nil ||
		//d.Decode(&commitIndex) != nil ||
		d.Decode(&votedFor) != nil {
		//println("[decode error]", d.Decode(&log), d.Decode(&currentTerm),d.Decode(&votedFor))
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		//println("[no decode error]", d.Decode(&log), d.Decode(&currentTerm),d.Decode(&votedFor))

		//rf.lastApplied = lastApplied
		//rf.commitIndex = commitIndex
	}
	//println("in [readPersist] server",rf.me, "log len:", len(rf.log), "currentTerm:",rf.currentTerm)
	//println("in [readPersist] rf.log[0].Term", rf.log[0].Term)

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
			//log.Printf("[%v]  votes[%d]", voteGranted, votes)
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
	//println(time.Now().UnixNano() / 1e6,"run candidate server","server ",rf.me,"term", term, "its role", rf.role)

	if rf.currentTerm != term || rf.role != CANDIDATE {
		return
	}
	if votes >= len(rf.peers)/2+1 {
		//println(time.Now().UnixNano() / 1e6,"be [leader] --","server ",rf.me,"receive votes:",votes,"term", rf.currentTerm, "its role", rf.role)
		rf.becomeLeader()
	}
	return
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
	//log.Printf("[%d]start Attempt election, in term [%d], rf dir %d", rf.me, rf.currentTerm,&rf)
	for {
		rf.mu.Lock()
		if rf.role != CANDIDATE || term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		randNum := rf.randNum()
		dt := rf.lastVoteOrRecvAppendEntryTime.Add(time.Millisecond * time.Duration(randNum))
		if time.Now().After(dt) {
			rf.becomeCandidate()
			term := rf.currentTerm
			LastLogTerm, LastLogIndex := rf.lastLogTermIndex()
			rf.mu.Unlock()

			go rf.attemptElection(term, LastLogIndex, LastLogTerm)

			//println(time.Now().UnixNano() / 1e6, rf.me, "become candidate in server" ,rf.me,"term", rf.currentTerm, "its role", rf.role)
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
	//如果candidate的任期 小于 这个follower的任期， 则不会给这个candidate投票

	//log.Printf("rf.currentTerm, rf.role, args.CandidateId, args.Term %v, %v, %v. %v", rf.currentTerm, rf.role, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
		return
	}

	if rf.votedFor == -1 {
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.lastVoteOrRecvAppendEntryTime = time.Now()
			rf.persist()
			return
		}
	}
	return

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
		//println("in start, idx:", index, "leader:", rf.me, "CT:",rf.currentTerm)
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
			Idx:     index,
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
	//index := rf.lastSnapshotIndex + len(rf.logEntries) - 1
	index := len(rf.log) - 1

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
	rf.electWaitDuration = 300

	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.role = FOLLOWER
	rf.currentTerm = 1
	rf.leaderID = -1
	rf.votedFor = -1
	rf.lastVoteOrRecvAppendEntryTime = time.Now()

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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
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
	go func(peerIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
		//println(time.Now().UnixNano() / 1e6, rf.me, "[start SAE]" ,rf.me,"term", rf.currentTerm, "its role", rf.role, "leader id", rf.leaderID)
		rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)

		//如果其他server的currentTerm 大于 leader，要把leader设置位follower
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//println(time.Now().UnixNano() / 1e6, rf.me, "[recev  SAE]" ,rf.me,"term", rf.currentTerm, "its role", rf.role, "leader id", rf.leaderID)

		if rf.role != LEADER || rf.currentTerm != args.Term {
			return
		}
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
				//println(time.Now().UnixNano() / 1e6,"server",peerIndex, "CT", rf.currentTerm, "leader", rf.leaderID, "idx", replyNextIndex)
				rf.nextIndex[peerIndex] = replyNextIndex
				rf.matchIndex[peerIndex] = rf.nextIndex[peerIndex] - 1

				////需要保证论文图2 右下角的这个条件: log[N].term == currentTerm
				//TODO 把leader 增加 commitIndex的逻辑放到 ApplyLoopzhogn
				//if rf.matchIndex[peerIndex] > rf.commitIndex && (len(args.Entries) > 0 && rf.currentTerm == args.Entries[len(args.Entries)-1].Term ) {
				//	cnt := 0
				//	for otherPeerIndex, _ := range rf.peers {
				//		if otherPeerIndex == peerIndex {
				//			continue
				//		}
				//		if rf.matchIndex[otherPeerIndex] >= rf.matchIndex[peerIndex] {
				//			cnt += 1
				//		}
				//		if cnt >= len(rf.peers)/2 {
				//			rf.commitIndex = rf.matchIndex[peerIndex]
				//			break
				//		}
				//	}
				//}
			} else if reply.NeedDecreaseNextIndex {
				//println(time.Now().UnixNano() / 1e6,"server",peerIndex, "CT", rf.currentTerm, "leader", rf.leaderID)
				res := rf.getNextIndex(reply.XLen, reply.XIndex, reply.XTerm, args)
				//println("log len:", reply.XLen, "before nextidx:", rf.nextIndex[peerIndex], "next next idx", res)
				rf.nextIndex[peerIndex] = res
			}
		}
		return
		//sendEnd := time.Now()
		//log.Println("resp", ok, "heartbeat response from peer", peerIndex, "received in", sendEnd.Sub(sendBegin), "success", reply.Success)
	}(peerIndex, args, reply)

}

func (rf *Raft) getAppendEntriesArgs(peerIndex int) AppendEntriesArgs {
	_, lastIndex := rf.lastLogTermIndex()
	var endIndex int
	//if lastIndex - rf.nextIndex[peerIndex] > 50 {
	//	endIndex = 50 + rf.nextIndex[peerIndex]
	//} else{
	//	endIndex = lastIndex+1
	//}
	endIndex = lastIndex + 1
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peerIndex] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peerIndex]-1].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      rf.log[rf.nextIndex[peerIndex]:endIndex],
	}
	return args
}

//1. 每隔一段时间，向非leader节点发送append entry心跳
func (rf *Raft) runLeader() {
	for {
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		replyArray := make([]AppendEntriesReply, len(rf.peers))
		argsArray := make([]AppendEntriesArgs, len(rf.peers))
		for peerIndex, _ := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			argsArray[peerIndex] = rf.getAppendEntriesArgs(peerIndex)
		}

		rf.mu.Unlock()
		//println(time.Now().UnixNano() / 1e6,"server", rf.me, "run leader in term", rf.currentTerm)

		for peerIndex, _ := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			rf.sendAppendEntries(peerIndex, &argsArray[peerIndex], &replyArray[peerIndex])
		}
		time.Sleep(time.Millisecond * time.Duration(rf.heartBeatWaitDuration))
	}
}

//在log中找出 某个term的第一个、最后一个logentry的index
func (rf *Raft) termIndexesInLog(term int) (int, int) {
	firstTermIndex, lastTermIndex := -1, -1
	hasTerm := false
	for logIndex, _ := range rf.log {
		if rf.log[logIndex].Term == term {
			if !hasTerm {
				hasTerm = true
				firstTermIndex = logIndex
				lastTermIndex = logIndex
				continue
			}
		} else if hasTerm {
			lastTermIndex = logIndex - 1
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
func (rf *Raft) getNextIndex(XLen int, XIndex int, XTerm int, args *AppendEntriesArgs) int {
	if XLen-1 < args.PrevLogIndex {
		return XLen
	}
	firstIndex, lastIndex := rf.termIndexesInLog(XTerm)
	if firstIndex == -1 {
		return XIndex
	}
	return lastIndex + 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_, lastLogIndex := rf.lastLogTermIndex()

	//rule 1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm || rf.role != FOLLOWER {
		rf.becomeFollower(args.Term)
	}
	rf.lastVoteOrRecvAppendEntryTime = time.Now()
	// Save the current leader
	rf.leaderID = args.LeaderId

	//rule 2:  Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > lastLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NeedDecreaseNextIndex = true
		reply.XLen = lastLogIndex + 1
		if args.PrevLogIndex <= lastLogIndex {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			reply.XIndex, _ = rf.termIndexesInLog(rf.log[args.PrevLogIndex].Term)
		}
		return
	}

	//如果不是心跳请求
	if len(args.Entries) > 0 {
		//乱序的请求
		//例如，leader先发送的参数中，entries=[a,b], 然后又发送了entries=[a,b,c,d,e],
		//如果第二的请求先到接受方，当第一个请求最终到接受方的时候，使用下面的逻辑，直接返回false
		//println("here", rf.me, rf.leaderID)
		//println("len(args.Entries) + args.PrevLogIndex <= lastLogIndex", len(args.Entries) , args.PrevLogIndex, lastLogIndex)
		if len(args.Entries)+args.PrevLogIndex <= lastLogIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
		//_, lastLogIndex := rf.lastLogTermIndex()
		//println("before", lastLogIndex)
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		//_, alastLogIndex := rf.lastLogTermIndex()
		//println("after", alastLogIndex)

		rf.persist()
		//if lastLogIndex == args.PrevLogIndex || rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		//}

	} else {
		if len(args.Entries)+args.PrevLogIndex < lastLogIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}

		//心跳包也有 leader 删除 follower的log的功能
		//leader [1]  term 3
		//follower [1,2]  term 3
		//此时leader的心跳包可以删除 follower 的2
		if lastLogIndex > args.PrevLogIndex {
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			rf.persist()
		}
	}

	//rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		_, lastLogIndex := rf.lastLogTermIndex()

		//println("leaderCommit:",args.LeaderCommit, "commit idx:", rf.commitIndex, "lastLogIndex:", lastLogIndex)
		if args.LeaderCommit > lastLogIndex {
			rf.commitIndex = lastLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) runFollower() {
	for {
		rf.mu.Lock()
		if rf.role != FOLLOWER {
			rf.mu.Unlock()
			return
		}
		//如果time.now - rf.lastVoteOrRecvAppendEntryTime > 150--300ms间的时间 ，则变为candidate
		randNum := rf.randNum()
		dt := rf.lastVoteOrRecvAppendEntryTime.Add(time.Millisecond * time.Duration(randNum))
		if time.Now().After(dt) {
			rf.becomeCandidate()
			//println(time.Now().UnixNano() / 1e6, rf.me, "become candidate in server" ,rf.me,"term", rf.currentTerm, "its role", rf.role)
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
			//println(time.Now().UnixNano() / 1e6, rf.me, "[start applyloop]" ,rf.me,"term", rf.currentTerm, "its role", rf.role, "leader id", rf.leaderID)
			//println("[applych] server",rf.me,"leaderid:",rf.leaderID,"commit idx", rf.commitIndex, "lastapplied", rf.lastApplied, "log len", len(rf.log))
			//for idx,_ :=range rf.log {
			//	log.Printf("[%v] idx: %v, command: %v, currentterm: %v", rf.me, idx, rf.log[idx].Command , rf.currentTerm)
			//}
			i := rf.lastApplied + 1
			for i <= rf.commitIndex {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
				//println("[applych] server",rf.me,"commit idx", rf.commitIndex, "lastapplied", "command", rf.log[i].Command,rf.lastApplied, "log len", len(rf.log))
				//log.Printf("command %v", rf.log[i].Command)
				i += 1
				rf.lastApplied += 1
			}
			//println(time.Now().UnixNano() / 1e6, rf.me, "[end applyloop]" ,rf.me,"term", rf.currentTerm, "its role", rf.role, "leader id", rf.leaderID)
		}

		for i := rf.commitIndex + 1; i <= rf.matchIndex[rf.me]; i++ {
			cnt := 0
			//需要保证论文图2 右下角的这个条件: log[N].term == currentTerm
			if rf.log[i].Term != rf.currentTerm {
				continue
			}
			for peerIndex, _ := range rf.peers {
				if rf.matchIndex[peerIndex] >= i {
					cnt++
				}
				if cnt > len(rf.peers)/2 {
					rf.commitIndex = i
					continue
				}
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
