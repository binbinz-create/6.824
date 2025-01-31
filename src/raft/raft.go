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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"


import "../labgob"

const (
	HeartbeatInterval    = time.Duration(120) * time.Millisecond
	ElectionTimeoutLower = time.Duration(300) * time.Millisecond
	ElectionTimeoutUpper = time.Duration(400) * time.Millisecond
)

type NodeState uint8

const (
	Follower  = NodeState(1)
	Candidate = NodeState(2)
	Leader    = NodeState(3)
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

func (rf *Raft) convertTo(s NodeState) {
	if rf.state == s {
		return
	}
	DPrintf("Term : %d , server %d convert form %v to %v \n", rf.currentTerm, rf.me, rf.state, s)
	rf.state = s
	switch s {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
		rf.votedFor = -1
	case Candidate:
		rf.startElection()
	case Leader:
		for i := range rf.nextIndex{
			//initialized to leader last log index + 1
			rf.nextIndex[i] = len(rf.logs)
		}
		for i := range rf.matchIndex{
			rf.matchIndex[i] = 0
		}
		rf.electionTimer.Stop()
		//广播心跳包
		rf.broadcastHeartbeat()
		rf.heartbeatTimer.Reset(HeartbeatInterval) //重置心跳超时
	}
}

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
	Command interface{}
	Term  	int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int          //2A
	votedFor       int          //2A
	electionTimer  *time.Timer  //2A
	heartbeatTimer *time.Timer  //2A
	state          NodeState    //2A

	logs 		[]LogEntry		//2B
	commitIndex int				//2B
	lastApplied int				//2B
	nextIndex	[]int			//2B
	matchIndex	[]int			//2B
	applyCh 	chan ApplyMsg 	//2B

}

func (rf Raft) String() string {
	return fmt.Sprintf("[node(%d) , state(%v) , term(%d) , votedFor(%d)", rf.me, rf.state, rf.currentTerm, rf.votedFor)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm ,votedFor int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil|| d.Decode(&logs) != nil{
		DPrintf("%v error recover from persisit",rf)
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs



}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int //2A
	CandidateId int //2A

	LastLogIndex int //2B
	LastLogTerm  int //2B
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //2A
	VoteGranted bool //2A
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() //executed before unlock
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) { //term < currentTerm  或者 此follower已经投过票了(投过了票，则term已经更新，和args.term会相等)
		reply.Term = rf.currentTerm //返回让candidate更新自己的term，并且转为follower
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm{  //如果大于curentTerm，则立即converTo Follower ， 这里和日志无关
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
		//do not return here
	}

	//2B: candidate's vote should be at least up-to-date as receiver's log
	// "up-to-date" is defined in thesis 5.4.1
	lastLogIndex := len(rf.logs) - 1
	if args.LastLogTerm < rf.logs[lastLogIndex].Term ||
		(args.LastLogTerm == rf.logs[lastLogIndex].Term && args.LastLogIndex < lastLogIndex){
		// Receiver is more up-to-date, does not grant vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	//reply.Term = rf.currentTerm  //not used , for better logging
	reply.VoteGranted = true
	//reset timer after grant vote
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

}

type AppendEntriesArgs struct {
	Term     int //2A
	LeaderId int //2A

	PrevLogTerm 	int 		//2B
	PrevLogIndex 	int 		//2B
	Entries 		[]LogEntry  //2B
	LeaderCommit 	int 		//2B
}

type AppendEntriesReply struct {
	Term    int  //2A
	Success bool //2A

	//OPTIMIZE : 3c
	ConflictTerm int   //3C
	ConflictIndex int  //3C
}

//follower和candidate 接收心跳包
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() //executed before unlock
	if args.Term < rf.currentTerm{
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
		//do not return here
	}

	//reset election timer even log does not match
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	//entires before args.PrevLogIndex might be unmatch
	//return false and ask Leader to decrement PrevLogIndex
	if len(rf.logs)  < args.PrevLogIndex + 1{
		reply.Success = false
		reply.Term = rf.currentTerm
		// optimistically thinks receiver's log matches with Leader's as a subset
		reply.ConflictIndex = len(rf.logs)
		// no conflict term
		reply.ConflictTerm = -1
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm{
		reply.Success = false
		reply.Term = rf.currentTerm
		// receiver's log in certain term unmatches Leader's log
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term

		// expecting Leader to check the former term
		// so set ConflictIndex to the first one of entries in ConflictTerm
		conflictIndex := args.PrevLogIndex
		// apparently, since rf.logs[0] are ensured to match among all servers
		// ConflictIndex must be > 0, safe to minus 1
		//log.Printf("conflictIndex:%d\n",conflictIndex)
		for rf.logs[conflictIndex-1].Term == reply.ConflictTerm{
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return

	}

	//找到follower的log与leader的log不同entry的索引,用来overwrite
	//这里还是有点不懂？？？,感觉这部分可以省略
	//unmatch_idx := -1
	//for idx := range args.Entries{
	//	if len(rf.logs) - 1 < args.PrevLogIndex + 1 + idx ||
	//		rf.logs[args.PrevLogIndex+1+idx].Term != args.Entries[idx].Term{
	//		unmatch_idx = idx
	//		break
	//	}
	//}

	//overwrite
	//if unmatch_idx != -1{
	//	// there are unmatch entries
	//	// truncate unmatch Follower entries, and apply Leader entries
	//	rf.logs = rf.logs[:args.PrevLogIndex+1+unmatch_idx]
	//	rf.logs = append(rf.logs,args.Entries[unmatch_idx:]...)
	//}

	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs,args.Entries[0:]...)

	//leader to guarantee to have all comitted entries
	if args.LeaderCommit > rf.commitIndex{
		lastLogIndex := len(rf.logs) - 1
		if args.LeaderCommit >= lastLogIndex{
			rf.commitIndex = lastLogIndex
		}else{
			rf.commitIndex = args.LeaderCommit
		}
		//after updating the commitIndex,should apply all entries betwwen lastApplied and committed
		rf.apply()
	}

	reply.Success = true

	return
}

func (rf *Raft) apply(){
	// apply all entries between lastApplied and committed
	// should be called after commitIndex updated
	if rf.commitIndex > rf.lastApplied{
		go func(start_idx int , entries []LogEntry){
			for idx,entry := range entries{
				var msg ApplyMsg
				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = start_idx+idx
				//apply
				rf.applyCh <- msg
				// do not forget to update lastApplied index
				// this is another goroutine, so protect it with lock
				rf.mu.Lock()
				rf.lastApplied = msg.CommandIndex
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1,rf.logs[rf.lastApplied+1:rf.commitIndex+1]) //从lastApplied+1到 commitIdex的日志都需要提交
	}
}

//should be called with lock , leader 发送心跳包
func (rf *Raft) broadcastHeartbeat() {

	for i := range rf.peers{
		if i == rf.me{
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader{
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[server] - 1
			entries := make([]LogEntry,len(rf.logs[prevLogIndex+1:]))
			copy(entries,rf.logs[prevLogIndex+1:])
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: rf.logs[prevLogIndex].Term,
				Entries: entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server,&args,&reply){
				rf.mu.Lock()
				if reply.Success{
					//successfully replicated args.Entries
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					// check if we need to update commitIndex
					// from the last log entry to committed one
					for i := len(rf.logs) - 1 ; i > rf.commitIndex; i--{  //从最后一个entry一直到commitIndex的那个entry，判断哪一个entry已经复制到大部分节点了，如果哪一个entry已经复制到大部分节点了，则更新leader的commitIndex，并且apply
						count := 0
						for _,matchIndex := range rf.matchIndex{
							if matchIndex >= i{
								count++
							}
						}

						if count > len(rf.peers) / 2{
							//most of nodes agreed on rf.logs[i]
							rf.commitIndex = i
							rf.apply()
							break
						}
					}

				}else{

					if reply.Term > rf.currentTerm{
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
						rf.persist()
					}else{
						// log unmatch, update nextIndex[server] for the next trial
						rf.nextIndex[server] = reply.ConflictIndex

						// if term found, override it to
						// the first entry after entries in ConflictTerm
						if reply.ConflictTerm != -1{
							for i := args.PrevLogIndex ; i >= 1; i--{
								if rf.logs[i-1].Term == reply.ConflictTerm{
									// in next trial, check if log entries in ConflictTerm matches
									rf.nextIndex[server] = i;
									break;
								}
							}
						}


						//	TODO : retry or later
					}

				}
				rf.mu.Unlock()
			}
		}(i)
	}

}

//should be called with lock , candidated 发起选举
func (rf *Raft) startElection() {

	rf.currentTerm += 1
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	lastLogIndex := len(rf.logs) - 1
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: rf.logs[lastLogIndex].Term,
	}

	var voteCount int32
	for i := range rf.peers {

		if i == rf.me {
			rf.votedFor = rf.me
			atomic.AddInt32(&voteCount, 1)
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()

				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt32(&voteCount, 1)
					if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2) {
						rf.convertTo(Leader)
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
						rf.persist()
					}
				}

				rf.mu.Unlock()
			} else {
				DPrintf("%v send request vote to server:%d failed", rf, server)
			}
		}(i)

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term,isLeader = rf.GetState()
	if isLeader{
		rf.mu.Lock()
		index = len(rf.logs)
		rf.logs = append(rf.logs,LogEntry{Command: command,Term: term})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
		rf.mu.Unlock()
	}

	return index, term, isLeader
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.electionTimer = time.NewTimer(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	rf.applyCh = applyCh
	rf.logs = make([]LogEntry,1) //start from index 1
	rf.nextIndex = make([]int,len(rf.peers))
	for i := range rf.nextIndex{
		//initialized to leader last log index + 1
		//都初始化为自己的下一个log entry index,无需同步
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.matchIndex = make([]int,len(rf.peers))

	go func(node *Raft) {
		for {
			select {
			case <-node.electionTimer.C:
				node.mu.Lock()
				if node.state == Follower {
					node.convertTo(Candidate)
				} else {
					node.startElection()
				}
				node.mu.Unlock()
			case <-node.heartbeatTimer.C:
				node.mu.Lock()
				if node.state == Leader{
					node.broadcastHeartbeat()
					node.heartbeatTimer.Reset(HeartbeatInterval)
				}
				node.mu.Unlock()
			}
		}

	}(rf)

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	return rf
}

func randTimeDuration(lower, upper time.Duration) time.Duration {
	num := rand.Int63n(upper.Nanoseconds()-lower.Nanoseconds()) + lower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}
