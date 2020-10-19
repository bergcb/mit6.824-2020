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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.role = CANDIDATE
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
	//rf.lastVoteOrRecvAppendEntryTime = time.Now()
	//println(time.Now().UnixNano() / 1e6," be follower","server ",rf.me,"term", rf.currentTerm, "its role", rf.role)

	rf.persist()
	//rf.electionTimer.Reset(getRandElectTimeout())
	//DPrintf("%s change to follower with term %d", rf, term)
}

func (rf *Raft) becomeLeader() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//println("become leader server id",rf.me )
	rf.role = LEADER
	rf.leaderID = rf.me
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	rf.persist()
	//
	//rf.pingTimer.Reset(heartbeatInterval)
	//go rf.pingLoop()
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

//func (rf *Raft) runCandidate() {
//	for {
//		rf.mu.Lock()
//		if rf.role != CANDIDATE {
//			rf.mu.Unlock()
//			return
//		}
//		rf.AttemptElection()
//		rf.becomeCandidate()
//	}
//}

func (rf *Raft) runCandidate() {
	//log.Printf("[%d]start Attempt election, in term [%d], rf dir %d", rf.me, rf.currentTerm,&rf)
	for {
		rf.mu.Lock()
		if rf.role != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm

		rf.mu.Unlock()
		//println(time.Now().UnixNano() / 1e6,"run candidate server","server ",rf.me,"term", term, "its role", rf.role)

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
					Term:        term,
					CandidateId: rf.me,
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
		//println(time.Now().UnixNano() / 1e6,"run candidate server","server ",rf.me,"term", term, "its role", rf.role)

		if rf.currentTerm != term || rf.role != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		if votes >= len(rf.peers)/2+1 {
			//println(time.Now().UnixNano() / 1e6,"be [leader] --","server ",rf.me,"term", rf.currentTerm, "its role", rf.role)
			rf.becomeLeader()
			rf.mu.Unlock()
			return
		} else {
			//println("lost")
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(rf.randNum()) * time.Millisecond)

		rf.mu.Lock()
		if rf.currentTerm != term || rf.role != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		rf.becomeCandidate()
		rf.mu.Unlock()
	}
}

func (rf *Raft) randNum() int {
	rand.Seed(time.Now().UnixNano())
	randNum := rand.Intn(rf.MaxWaitTime-rf.MinWaitTime) + rf.MinWaitTime

	return randNum
}

//组装RequestVoteArgs ， RequestVoteReply， 并且打印日志
//func (rf *Raft) CallRequestVote(server int, term int) bool {
//	args := RequestVoteArgs{
//		Term:        term,
//		CandidateId: rf.me,
//	}
//	var reply = RequestVoteReply{}
//	rf.sendRequestVote(server, &args, &reply)
//	//log.Println(rf.me, "in term", rf.currentTerm, "CallRequestVote to ", server, "reply VoteGranted", reply.VoteGranted)
//	ok := reply.VoteGranted
//	if ok {
//		return true
//	} else {
//		return false
//	}
//}

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

	if rf.currentTerm < args.Term {
		//println(time.Now().UnixNano() / 1e6,rf.me,"receive RV in term", rf.currentTerm, "[args] term" , args.Term, "server id", args.CandidateId)
		rf.becomeFollower(args.Term)
		//rf.lastVoteOrRecvAppendEntryTime = time.Now()

	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.log[len(rf.log)-1].Term > args.LastLogTerm || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 >= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.lastVoteOrRecvAppendEntryTime = time.Now()
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
	if rf.currentTerm < reply.Term {
		rf.becomeFollower(reply.Term)
	}
	rf.mu.Unlock()
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
	if rf.leaderID == rf.me {

		println("start", rf.leaderID, rf.me)
	}
	term := rf.currentTerm
	isLeader := rf.role == LEADER
	_, lastIndex := rf.lastLogTermIndex()
	index := lastIndex + 1

	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
			Idx:     index,
		})
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	if len(rf.log) > 1 {
		//println("in start, log len", len((rf.log)), "server id", rf.me, "log addr", &rf.log, "leader id", rf.leaderID)
	}
	//rf.resetHeartBeatTimers()
	return index, term, isLeader

	//index := len(rf.log)
	//term := rf.currentTerm
	//isLeader := true
	////println("here", rf.me, rf.leaderID, len(rf.log))
	//
	//if rf.leaderID != rf.me {
	//	isLeader = false
	//	return index, term, isLeader
	//}
	//rf.log = append(rf.log, LogEntry{
	//	Term:    rf.currentTerm,
	//	Command: command,
	//	Idx:     index,
	//})
	//rf.matchIndex[rf.me] = index
	//println("xxxxxxxxxxxxx", rf.me, rf.leaderID, len(rf.log))
	//rf.matchIndex[rf.me] = index
	//rf.persist()

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

	// Your initialization code here (2A, 2B, 2C).
	rf.becomeFollower(1)
	//rf.currentTerm = 1
	//rf.votedFor = -1
	//rf.leaderID = -1
	rf.MinWaitTime = 150
	rf.MaxWaitTime = 300

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Run()

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
}

func (rf *Raft) sendAppendEntries(peerIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	go func() {
		//sendBegin := time.Now()
		//if len(args.Entries) != 0 {
		//	println("not heart beat")
		//}
		rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)

		//如果其他server的currentTerm 大于 leader，要把leader设置位follower
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.becomeFollower(reply.Term)
		} else {
			if reply.Success {
				//println("here", reply.Success)
				//println("here", rf.me,rf.leaderID, len(args.Entries))
				//log.Println("reply success", rf.nextIndex[peerIndex] + len(args.Entries))
				//println("before",rf.nextIndex[peerIndex], "len(args.Entries)", len(args.Entries),"server", peerIndex, "leader", rf.leaderID)
				rf.nextIndex[peerIndex] = rf.nextIndex[peerIndex] + len(args.Entries)
				//println("after",rf.nextIndex[peerIndex], "len(args.Entries)", len(args.Entries), "server", peerIndex, "leader", rf.leaderID)

				//println("leader commit idx", rf.commitIndex)
				//println("rf.nextIndex[peerIndex] + len(args.Entries)-1", rf.nextIndex[peerIndex] , len(args.Entries))
				rf.matchIndex[peerIndex] = rf.nextIndex[peerIndex] - 1
				//println("rf.matchIndex[peerIndex]", rf.matchIndex[peerIndex], peerIndex)

				if rf.matchIndex[peerIndex] > rf.commitIndex {
					//log.Println("want to update commitindex")
					cnt := 0
					for otherPeerIndex, _ := range rf.peers {
						if otherPeerIndex == peerIndex {
							continue
						}
						if rf.matchIndex[otherPeerIndex] >= rf.matchIndex[peerIndex] {
							cnt += 1
						}
						if cnt >= len(rf.peers)/2 {
							//log.Println(rf.commitIndex)
							rf.commitIndex = rf.matchIndex[peerIndex]
							//println("here leader commit idx", rf.commitIndex, "leader", rf.me, rf.log[len(rf.log)-1].Term)
							//log.Println("undate success", rf.commitIndex)
							break
						}
					}
				}

			} else if reply.NeedDecreaseNextIndex && rf.nextIndex[peerIndex]-1 > 0 {
				rf.nextIndex[peerIndex] = rf.nextIndex[peerIndex] - 1
			}
		}
		rf.mu.Unlock()

		//sendEnd := time.Now()
		//log.Println("resp", ok, "heartbeat response from peer", peerIndex, "received in", sendEnd.Sub(sendBegin), "success", reply.Success)
	}()

}

func (rf *Raft) getHeartBeatArgs(peerIndex int) AppendEntriesArgs {
	//if len(rf.log)-1 > rf.nextIndex[peerIndex] {
	//	entries := rf.log[rf.nextIndex[peerIndex]:]
	//} else {
	//	entries := []LogEntry{}
	//}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peerIndex] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peerIndex]-1].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      rf.log[rf.nextIndex[peerIndex]:],
		//Entries: []LogEntry{},
	}
	//println("log lenth--",len(rf.log), "server id:", rf.me, "last log term",rf.log[len(rf.log)-1].Term, "log addr", &rf.log, "leader id", rf.leaderID)
	//println("[AR ARGS]","server id:", rf.me, "last log term",rf.log[len(rf.log)-1].Term, "log len:",len(rf.log),"args len:", len(args.Entries),)
	//println(len(args.Entries), len(rf.log))
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
		rf.mu.Unlock()
		//println(time.Now().UnixNano() / 1e6,"server", rf.me, "run leader in term", rf.currentTerm)

		for peerIndex, _ := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			args := rf.getHeartBeatArgs(peerIndex)
			rf.sendAppendEntries(peerIndex, &args, &replyArray[peerIndex])
		}
		time.Sleep(time.Millisecond * time.Duration(rf.heartBeatWaitDuration))
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Println(len(rf.log), args.PrevLogIndex)
	_, lastLogIndex := rf.lastLogTermIndex()

	if args.Term < rf.currentTerm {

		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm || rf.role != FOLLOWER {
		rf.becomeFollower(args.Term)
	}

	rf.lastVoteOrRecvAppendEntryTime = time.Now()
	//println(time.Now().UnixNano() / 1e6,"in [AE] leader id", args.LeaderId, "leader term", args.Term, "peer id", rf.me, "peer term", rf.currentTerm, "ITS ROLE", rf.role, rf.lastVoteOrRecvAppendEntryTime.UnixNano() / 1e6)

	// Save the current leader
	rf.leaderID = args.LeaderId

	if args.PrevLogIndex > lastLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NeedDecreaseNextIndex = true
		return
	}

	//如果不是心跳请求
	if len(args.Entries) > 0 {
		if len(rf.log) == args.PrevLogIndex+1 || rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
			println(rf.me, rf.leaderID)

			//println(len(rf.log) == args.PrevLogIndex+1)
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		}

		//乱序的请求
		//例如，leader先发送的参数中，entries=[a,b], 然后又发送了entries=[a,b,c,d,e],
		//如果第二的请求先到接受方，当第一个请求最终到接受方的时候，使用下面的逻辑，直接返回false
		//println("here", rf.me, rf.leaderID)
		//println("len(args.Entries) + args.PrevLogIndex <= lastLogIndex", len(args.Entries) , args.PrevLogIndex, lastLogIndex)
		if len(args.Entries)+args.PrevLogIndex <= lastLogIndex {
			//if len(args.Entries) > 0{
			//	println("here", rf.me , rf.leaderID, "len(args.Entries) + args.PrevLogIndex+1 <= len(rf.log)", len(args.Entries) , args.PrevLogIndex+1, len(rf.log))
			//}
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
	}
	if args.LeaderCommit > len(rf.log)-1 {
		rf.commitIndex = len(rf.log) - 1
	} else {
		rf.commitIndex = args.LeaderCommit
	}
	//println("follower commitx", rf.commitIndex, rf.log[len(rf.log)-1].Term)

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

func (rf *Raft) Run() {
	for {
		//fmt.Println(rf.PrefixPrint(), "taking a roll")

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
