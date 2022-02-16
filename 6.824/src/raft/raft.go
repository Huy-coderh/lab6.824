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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term    int
	Command interface{}
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
	currentTerm int   // 当前term
	votedFor    int   // 当前收到该server投票的候选人id
	log         []Log // log entries (first index is 1)

	commitIndex int // 已提交的日志条目最大索引
	lastApplied int // 已应用到状态机的日志条目最大索引

	nextIndex   []int // 发送给每个server的下一条日志条目的索引
	matachIndex []int // 复制给其他server的日志条目index

	electionTimer  *time.Timer // 选举定时器
	heartbeatTimer *time.Timer // 心跳定时器

	applyCh chan ApplyMsg // 提交日志的chan
	killCh  chan bool
}

const heartbeatTime = 150
const electionTimeLo = 200
const electionTimeRange = 150

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.votedFor == rf.me
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的term
	CandidateId  int // 候选人id
	LastLogIndex int // 候选人日志的最大index
	LastLogTerm  int // 候选人日志的最大term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current team, for candidate to update itself
	VoteGranted bool // vote result
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) resetElectionTimer() {
	// 重置选举计时器
	rf.electionTimer.Stop()
	electionTime := electionTimeLo + rand.Intn(electionTimeRange)
	rf.electionTimer.Reset(time.Duration(electionTime) * time.Millisecond)
}

func (rf *Raft) resetHeratbeatTimer() {
	// 重置心跳计时器
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(heartbeatTime * time.Millisecond)
	//rf.heartbeatTimer = time.NewTimer(heartbeatTime * time.Millisecond)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Println(rf.me, "receive request vote", args, rf.log)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = -1
	rf.heartbeatTimer.Stop()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
			reply.VoteGranted = true
		}
		if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1 {
			reply.VoteGranted = true
		}
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		//fmt.Println(rf.me, "vote for ", args.CandidateId, reply.VoteGranted, rf.log, args)
	}
	reply.Term = rf.currentTerm

}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Println(rf.me, "receive append entries", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		// 心跳相关
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		rf.resetElectionTimer()
		rf.heartbeatTimer.Stop()

		// 日志相关
		if len(args.Entries) > 0 {
			reply.Term = args.Term
			if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false
				return
			}

			// 复制日志
			logCopy := make([]Log, len(rf.log))
			copy(logCopy, rf.log)
			rf.log = append(logCopy[:args.PrevLogIndex+1], args.Entries...)
			//rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

			//fmt.Println(rf.me, "logs ", rf.log)

		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			// 提交日志
			//fmt.Println("committing.....")
			go rf.applyLogs(rf.lastApplied, rf.commitIndex)
		}
		reply.Success = true
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
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
	//fmt.Println(rf.me, " send request vote to ", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server int, term int, leaderId int) {

	rf.mu.Lock()
	args := AppendEntriesArgs{}
	args.Term = term
	args.LeaderId = leaderId
	args.LeaderCommit = min(rf.commitIndex, rf.matachIndex[server])
	//fmt.Println("heartbeat-------", rf.commitIndex, "/", args.LeaderCommit)
	rf.mu.Unlock()
	//fmt.Println(rf.me, " send heartbeat to ", server, args)
	rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
}

func (rf *Raft) applyLogs(lastApplied int, commitIndex int) {
	//fmt.Println(rf.me, "apply", lastApplied, "/", commitIndex)
	for lastApplied < commitIndex {
		rf.mu.Lock()
		lastApplied++
		msg := ApplyMsg{}
		msg.CommandValid = true
		if lastApplied >= len(rf.log) {
			break
		}
		msg.Command = rf.log[lastApplied].Command
		msg.CommandIndex = lastApplied
		//fmt.Println(rf.me, "commit ", msg.CommandIndex, "/", msg.Command)

		rf.applyCh <- msg
		rf.lastApplied = lastApplied
		//fmt.Println(rf.me, "logs when commit", rf.log)
		rf.mu.Unlock()
	}
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
	/* index := -1
	term := -1
	isLeader := true */

	// Your code here (2B).
	rf.mu.Lock()
	if rf.votedFor != rf.me {
		rf.mu.Unlock()
		return -1, -1, false
	}
	rf.log = append(rf.log, Log{rf.currentTerm, command})
	curLen := len(rf.log)
	//rf.commitIndex = len(rf.log) - 1
	curTerm := rf.currentTerm
	curIndex := len(rf.log) - 1
	rf.mu.Unlock()

	// 发送AppendEtries
	ch := make(chan AppendEntriesReply)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int, curTerm int) {
			for {
				rf.mu.Lock()
				args := AppendEntriesArgs{}
				args.Term = curTerm
				args.LeaderId = rf.me
				if rf.nextIndex[i] > len(rf.log) {
					rf.nextIndex[i] = len(rf.log)
				}
				prevLogIndex := rf.nextIndex[i] - 1
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = rf.log[prevLogIndex].Term
				args.Entries = rf.log[rf.nextIndex[i]:]
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				//fmt.Println(rf.me, " send append entries to ", i, args)
				status := rf.sendAppendEntries(i, &args, &reply)
				//fmt.Println("received append entries ---", status, "/", reply.Success)
				if status && reply.Success {
					rf.mu.Lock()
					rf.nextIndex[i] = prevLogIndex + len(args.Entries) + 1
					rf.matachIndex[i] = rf.nextIndex[i] - 1
					rf.mu.Unlock()
					ch <- reply
					break
				}
				// if rf.tag {
				// 	fmt.Println(rf.me, " send to ", i, " fail, again")
				// }
				rf.mu.Lock()
				if rf.votedFor != rf.me {
					rf.mu.Unlock()
					break
				}
				if rf.nextIndex[i] > 1 {
					rf.nextIndex[i]--
				}
				rf.mu.Unlock()
			}
		}(i, curTerm)
	}
	rf.resetHeratbeatTimer()

	go func(command interface{}) {
		// 等待结果
		count := 1
		for i := 0; i < len(rf.peers)-1; i++ {
			reply := <-ch
			rf.mu.Lock()
			if reply.Term == rf.currentTerm && reply.Success {
				count++
				if count > len(rf.peers)/2 {
					rf.mu.Unlock()
					break
				}
			}
			rf.mu.Unlock()
		}
		// 提交日志到状态机
		rf.mu.Lock()
		if count > len(rf.peers)/2 && rf.currentTerm == curTerm {
			if rf.commitIndex < curLen-1 {
				//fmt.Println("commitIndex ++++++++++++++", curLen-1)
				rf.commitIndex = curLen - 1
			}
			//fmt.Println("master", rf.me, " committing.............")
			go rf.applyLogs(rf.lastApplied, rf.commitIndex)
		}
		rf.mu.Unlock()
	}(command)
	return curIndex, curTerm, true
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
	close(rf.killCh)
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Stop()

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		select {

		case <-rf.killCh:

		case <-rf.electionTimer.C:
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().

			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = -1
			curTerm := rf.currentTerm

			// send requestVote
			ch := make(chan RequestVoteReply)
			args := RequestVoteArgs{}
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log)
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
			args.Term = rf.currentTerm
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(i int) {
					reply := RequestVoteReply{}
					status := rf.sendRequestVote(i, &args, &reply)
					if status {
						ch <- reply
					} else {
						ch <- RequestVoteReply{0, false}
					}
				}(i)
			}
			// 统计票数
			count := 1
			for i := 0; i < len(rf.peers)-1; i++ {
				reply := <-ch
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
				}
				rf.mu.Unlock()
				if reply.VoteGranted {
					count++
					if count > len(rf.peers)/2 {
						break
					}
				}
			}

			rf.mu.Lock()
			//fmt.Println("ticket", count, "/", "peers", len(rf.peers))
			if count > len(rf.peers)/2 && rf.votedFor == -1 && rf.currentTerm == curTerm {
				// win the election
				rf.votedFor = rf.me

				//fmt.Println(rf.me, "wins the election")

				//fmt.Println(rf.me, "logs when wins the election", rf.log)
				// 发送 AppendEntries rpc
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.sendHeartbeat(i, rf.currentTerm, rf.me)
				}
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matachIndex[i] = 0
				}
				// 设置心跳计数器
				rf.resetHeratbeatTimer()
			} else {
				// lose the election
				rf.resetElectionTimer()
			}
			rf.mu.Unlock()

		}

	}
}

func (rf *Raft) heartbeatTicker() {
	// send heartbeat when the heartbeat timer times out
	for !rf.killed() {
		select {
		case <-rf.killCh:

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			// fmt.Println(strconv.Itoa(rf.me) + " send heartbeat...")
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go rf.sendHeartbeat(i, rf.currentTerm, rf.me)
			}
			if rf.votedFor == rf.me {
				// 重新发起心跳计时
				rf.resetHeratbeatTimer()
			}
			rf.mu.Unlock()
		}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	//fmt.Println("peers", len(peers))
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.votedFor = -1
	rf.me = me
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.killCh = make(chan bool)

	// Your initialization code here (2A, 2B, 2C).
	rf.log = []Log{}
	rf.log = append(rf.log, Log{-1, ""})
	electionTime := electionTimeLo + rand.Intn(electionTimeRange)
	rf.electionTimer = time.NewTimer(time.Duration(electionTime) * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(heartbeatTime * time.Millisecond)
	rf.heartbeatTimer.Stop()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matachIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start heartbeat ticker goroutine
	go rf.heartbeatTicker()

	return rf
}

func min(i, j int) int {
	if i <= j {
		return i
	} else {
		return j
	}
}
