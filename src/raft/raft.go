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

	"fmt"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	heartbeat int32               // CAS
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persisted state
	currentTerm int32 //increase monotonically
	votedFor    int32
	log         []*Entry // index starting from 1
	applyCh     chan ApplyMsg
	// volatile state
	commitIndex int32
	lastApplied int32
	state       int32 // 1,2,3 leader, follower, candidate
	// volatiel state on leader
	nextIndex  []int // optimistic
	matchIndex []int // pessimistic
}

type Entry struct {
	Id      int // deduplication
	Term    int
	Command interface{}
}

func (e *Entry) String() string {
	s := fmt.Sprintf("%+v", e.Command)
	if len(s) < 5 {
		return s
	}
	return s[0:5]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = int(rf.currentTerm)
	leaderFlag := rf.state
	rf.mu.Unlock()
	isleader = leaderFlag == 1
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
	Term         int32
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

//
// example RequestVote RPC handler.
// Receiver implementation: 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// (2A, 2B).
	// 2A
	// FIXME(done) requestVote also set the heartbeat preventing other follower time-out
	// FIXME (done)requestVote only reset heartbeat when it grant the vote
	rf.mu.Lock() // protect rf's field
	// last entry's term
	// TODO (done)if leader or candidate finds out that it's term is out of date
	// it will immediately update its term and turn to follower
	// FIXME (done)update the receiver's term if it's smaller
	// FIXME (done)should we update while we receive voterequest?
	// this may cause the term to increase even no one get elected
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 2
		//clear voted for
		rf.votedFor = -1
	}
	lastTerm := 0
	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
	}
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("server %d(%d)reject voting server %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		// not candidate
	} else if rf.votedFor == -1 || rf.votedFor == int32(args.CandidateId) {
		// candidate is at least up-to-date as receiver
		if lastTerm < args.LastLogTerm || len(rf.log) <= args.LastLogIndex {
			atomic.StoreInt32(&rf.heartbeat, 1)
			reply.VoteGranted = true
			rf.votedFor = int32(args.CandidateId)
			DPrintf("server %d(%d) vote for server %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else {
			reply.VoteGranted = false
			DPrintf("server %d(%d) rejects server %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		}
		reply.Term = args.Term
	} else { // already voted this term
		reply.VoteGranted = false
		reply.Term = args.Term
		DPrintf("server %d(%d) rejects server %d(%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}

	rf.mu.Unlock()
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
// TODO: concurrent command in a short time interval will only sendout the last one
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader || rf.killed() {
		return index, term, isLeader
	}
	entry := &Entry{}
	entry.Term = term
	entry.Command = command
	rf.mu.Lock()
	rf.log = append(rf.log, entry)
	index = len(rf.log)
	DPrintf("sever: %d new cmd arrives at index: %d, term: %d, log: %+v", rf.me, index, term, rf.log)
	rf.mu.Unlock()
	go rf.processLogReplication()
	return index, term, isLeader
}

func (rf *Raft) processLogReplication() {
	//TODO: code deduplication
	respChan := make(chan int)
	commitChan := make(chan int)
	// timerChan := make(chan int)
	// go func() {
	// 	time.Sleep(150 * time.Millisecond)
	// 	timerChan <- 1
	// }()

	count := int32(len(rf.peers) - 1)
	majority := int32(len(rf.peers) / 2)
	DPrintf("server %d needs %d yes to commit the command", rf.me, majority)
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(i int) {
				args := &AppendEntriesArgs{}
				rf.buildAppendEntriesArgs(args, i) // contains lock might block each other
				reply := &AppendEntriesReply{}
				DPrintf("send entry to %d", i)
				rf.sendAppendEntries(i, args, reply)
				// if not ok the leader will indefinitely retry the RPC
				term, isLeader := rf.GetState()
				for isLeader && term == int(args.Term) && !rf.killed() { // only process when rpc call is correctly responsed
					if reply.Success {
						DPrintf("server %d accepted server %d's append", i, args.LeaderId)
						atomic.AddInt32(&majority, -1)
						if atomic.LoadInt32(&majority) == 0 {
							commitChan <- 1
						}
						rf.mu.Lock()
						// nextIndex might be updated already
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						rf.mu.Unlock()
						atomic.AddInt32(&count, -1)
						if atomic.LoadInt32(&count) == 0 { // guarantee that only one goroutine get 0
							respChan <- 1
						}
						return
					} else {
						rf.mu.Lock()
						// when a server find its current term is smaller
						// it would turn into follower
						if rf.currentTerm < reply.Term {
							rf.state = 2
							isLeader = false
							rf.currentTerm = reply.Term
							rf.mu.Unlock()
							atomic.AddInt32(&count, -1)
							if atomic.LoadInt32(&count) == 0 { // guarantee that only one goroutine get 0
								respChan <- 1
							}
							return
						} else {
							// TODO: synchronize the log with follower i, can optimize it with term backwards
							// decrease the nextIndex, which is initialized to len(leader_log) + 1
							// append the prev entry at the head and resend the appendRPC
							// nextIndex shouldn't be smaller than matchIndex
							rf.nextIndex[i] = reply.FirstConflictTermEntryIndex
							if rf.nextIndex[i] <= rf.matchIndex[i] {
								rf.nextIndex[i] = rf.matchIndex[i] + 1 // >= 1
							}
							DPrintf("follwer %d nextIndex %d", i, rf.nextIndex[i])
							rf.mu.Unlock()
							//DPrintf("entry: %+v", rf.log[rf.nextIndex[i]-1])
							args = &AppendEntriesArgs{}
							//args.Entries = append([]*Entry{rf.log[rf.nextIndex[i]-1]}, args.Entries...)
							rf.buildAppendEntriesArgs(args, i)
							reply = &AppendEntriesReply{}
							rf.sendAppendEntries(i, args, reply)
							term, isLeader = rf.GetState()
						}
					}
				}

				//DPrintf("server %d connected with leader %d? %v", i, rf.me, ok)
			}(idx)
		}
	}
	select {
	case <-commitChan:
		rf.mu.Lock()
		//if it's still leader
		if rf.state == 1 && !rf.killed() {
			// send commit grantedRPC(heartbeat) to all followers
			// rf.commitIndex = int32(len(rf.log))
			// send the command from old commitIndex to current
			// TODO: only commit the current term by checking the term of the last entry
			lastEntryTerm := 0
			if len(rf.log) > 0 {
				lastEntryTerm = rf.log[len(rf.log)-1].Term
			}
			if lastEntryTerm == int(rf.currentTerm) {
				rf.commitIndex = int32(len(rf.log))
				go rf.checkApply(rf.commitIndex)
			}
			DPrintf("current log of leader %d: %+v, commit index: %d", rf.me, rf.log, rf.commitIndex)

		}
		rf.mu.Unlock()
	case <-respChan: // rejected or rpc fail
		rf.mu.Lock()
		DPrintf("server %d commit not succeed for term %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		// case <-timerChan:
		// 	rf.mu.Lock()
		// 	DPrintf("server %d commit not succeed for term %d, due to time out\n", rf.me, rf.currentTerm)
		// 	rf.mu.Unlock()
	}

}

func (rf *Raft) buildAppendEntriesArgs(args *AppendEntriesArgs, peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO: wrap nextIndex till last entry in leader as entries
	// if nextIndex > index of lastEntry then heartbeat
	// entries are the next[i] to last
	args.Entries = []*Entry{}
	// check currentTerm and last entry term
	// only commit the entry at the currentTerm
	// but we can send commit one even if its stale
	lastTerm := 0
	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
	}
	DPrintf("check stale entry term, currentTerm: %d, lastEntry Term: %d", rf.currentTerm, lastTerm)
	for j := rf.nextIndex[peer] - 1; j < len(rf.log); j++ {
		if j <= int(rf.commitIndex) || lastTerm == int(rf.currentTerm) {
			args.Entries = append(args.Entries, rf.log[j])
		}
	}
	//DPrintf("args entries: %+v", args.Entries)
	args.PrevLogIndex = rf.nextIndex[peer] - 1
	args.PrevLogTerm = 0
	if args.PrevLogIndex > 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
	}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// simple time out: when time.sleep for a random time and finds no heartbeat received
// time-out for each server is randomly set between 100ms-300ms
// it would start election
func (rf *Raft) ticker() {
	for !rf.killed() {
		currentTerm, isleader := rf.GetState()
		if !isleader {
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep()
			interval := rand.Intn(300) + 150
			time.Sleep(time.Duration(interval) * time.Millisecond)
			// hb := atomic.LoadInt32(&rf.heartbeat)
			// heartbeat reset to 0
			if !atomic.CompareAndSwapInt32(&rf.heartbeat, 1, 0) {
				// increment the current term by one
				// FIXME: should startElection using another new goroutine?
				rf.startElection(int32(currentTerm) + 1)
			}
		} else {
			// wins the election turn into leader loop
			// send out AppendEntries RPC as heartbeat
			time.Sleep(time.Duration(120) * time.Millisecond)
			rf.processLogReplication()
		}

	}
}

// this function send heartbeat to all peers concurrently
// func (rf *Raft) sendHeartBeat() {
// 	for idx, _ := range rf.peers {
// 		if idx != rf.me {
// 			go func(i int) {
// 				req := &AppendEntriesArgs{}
// 				rf.buildAppendEntriesArgs(req, i)
// 				reply := &AppendEntriesReply{}
// 				rf.sendAppendEntries(i, req, reply)
// 			}(idx)
// 		}
// 	}
// }

// This function appendEntries to followers
// note that this message also establish the leadership, so it will clear the voteFor
// if the current server is a candidate, it will compare the term
// if currentTerm <= receivedTerm it would turn to follower at once
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	// update term first
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// sender's term is at least as big as the receiver
	// turn to follower and clear the votefor
	rf.state = 2
	rf.votedFor = -1
	rf.currentTerm = args.Term
	atomic.CompareAndSwapInt32(&rf.heartbeat, 0, 1)
	// compare the log and commitIndex
	// only when log is consistent can we implement the commit logic
	prevTerm := 0
	if len(rf.log) >= args.PrevLogIndex && len(rf.log) > 0 && args.PrevLogIndex > 0 {
		prevTerm = rf.log[args.PrevLogIndex-1].Term
		//DPrintf("server %d received appendEntries, term: %d, %d", rf.me, prevTerm, args.PrevLogTerm)
	}

	if prevTerm == args.PrevLogTerm {
		// append entries from prevLogIndex
		if len(args.Entries) > 0 {
			DPrintf("server %d entries to append from server %d, entries: %v, prevTerm: %d, prevIndex: %d, leaderterm: %d, followerTerm: %d", rf.me, args.LeaderId, args.Entries, args.PrevLogTerm, args.PrevLogIndex, args.Term, rf.currentTerm)
			rf.log = append(rf.log[0:args.PrevLogIndex], args.Entries...)
			DPrintf("server %d append successfully, log: %+v", rf.me, rf.log)
		} else {
			DPrintf("server %d received heartbeat", rf.me)
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			go rf.checkApply(args.LeaderCommit)
		}
		reply.Success = true

	} else {
		reply.Success = false
		DPrintf("server %d rejected leader %d, log: %+v, prevIndex: %d, prevTerm: %d, args.prevTerm: %d", rf.me, args.LeaderId, rf.log, args.PrevLogIndex, prevTerm, args.PrevLogTerm)
		//TODO: reply the conflict term and the first entry for that term
		reply.ConflictTerm = prevTerm
		if len(rf.log) < args.PrevLogIndex {
			reply.FirstConflictTermEntryIndex = len(rf.log)
		} else if prevTerm > 0 {
			// find the first entry in that term
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if i == 0 || rf.log[i-1].Term < prevTerm {
					reply.FirstConflictTermEntryIndex = i + 1
				}
			}
		} else {
			reply.FirstConflictTermEntryIndex = 1
		}
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) checkApply(target int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	target = int32(min(int(target), len(rf.log)))
	for i := rf.lastApplied + 1; i <= target; i++ {
		var applyMsg ApplyMsg
		applyMsg.Command = rf.log[i-1].Command
		applyMsg.CommandIndex = int(i)
		applyMsg.CommandValid = true
		rf.applyCh <- applyMsg
	}
	rf.lastApplied = target
}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// check if rf is still the leader
	// avoid the bug that buildentry with a new updated term number
	DPrintf("args: %+v", req)
	ok := rf.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry // empty for heartbeat
	LeaderCommit int32
}

type AppendEntriesReply struct {
	Term                        int32
	Success                     bool
	ConflictTerm                int
	FirstConflictTermEntryIndex int
}

// this function checks the node's currentTerm with the one it communicating
// if it's in candidate state or leader state, it will turn into follower state
// return false if currentTerm is bigger than receivedTerm
func (rf *Raft) checkTerm(receivedTerm int32) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > receivedTerm {
		return false
	}
	if rf.currentTerm < receivedTerm {
		rf.currentTerm = receivedTerm
		// turn to follower
		rf.state = 2
	}
	return true
}

func (rf *Raft) startElection(termForElection int32) {
	rf.mu.Lock()
	rf.votedFor = int32(rf.me)
	rf.state = 3 // turn to candidate
	rf.currentTerm = termForElection
	DPrintf("server %d starts an election for term %d\n, peers: %d", rf.me, rf.currentTerm, len(rf.peers))
	lastLogIndex := len(rf.log)
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	rf.mu.Unlock()

	respChan := make(chan int)
	voteChan := make(chan int)
	timerChan := make(chan int)
	go func() {
		time.Sleep(150 * time.Millisecond)
		timerChan <- 1
	}()
	count := int32(len(rf.peers) - 1)
	majority := int32(len(rf.peers) / 2)
	DPrintf("server %d needs %d votes", rf.me, majority)
	for index, _ := range rf.peers {
		if index != rf.me {
			go func(idx int) {
				req := &RequestVoteArgs{}
				req.CandidateId = rf.me
				req.Term = termForElection
				req.LastLogIndex = lastLogIndex
				req.LastLogTerm = lastLogTerm
				resp := &RequestVoteReply{}
				ok := rf.sendRequestVote(idx, req, resp)
				// network failure cause the response delayed by 2sec
				// so we shouldn't wait for it to join if we already got majority votes
				// if majority = 0 we can triger the following logic
				// wait for majority votes
				atomic.AddInt32(&count, -1)
				if atomic.LoadInt32(&count) == 0 { // guarantee that only one goroutine get 0
					respChan <- 1
				}
				if ok {
					if resp.VoteGranted {
						atomic.AddInt32(&majority, -1)
						if atomic.LoadInt32(&majority) == 0 {
							voteChan <- 1
						}
					} else {
						rf.mu.Lock()
						// when a server find its current term is smaller
						// it would turn into follower
						if rf.currentTerm < resp.Term {
							rf.state = 2
							rf.currentTerm = resp.Term
						}
						rf.mu.Unlock()
					}
				} else {
					DPrintf("server %d not respond vote for  leader %d", idx, rf.me)
				}
			}(index)

		}
	}
	// interval := rand.Intn(30) + 30
	// time.Sleep(time.Duration(interval) * time.Millisecond)
	// since Call() must return, we don't need set the timeout but use waitgroup
	// timeout check majority vote
	// TODOuse select to observe two info: all response / majority granted vote
	select {
	case <-voteChan:
		rf.initializeLeaderState()
	case <-respChan:
		rf.mu.Lock()
		rf.state = 2 // turn to follower
		DPrintf("server %d lose election for term %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
	case <-timerChan:
		rf.mu.Lock()
		rf.state = 2 // turn to follower
		DPrintf("server %d stop election for term %d, due to time out\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
	}
	// FIXME both voteChan and respChan not respond?
	// possible if network partition happened
	// so we need time-out to transfer the state from candidate to follower
	// end this go routine and go back to ticker() loop
}

func (rf *Raft) initializeLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == 3 {
		rf.state = 1 // turn to leader
		// initialize the matchIndex and nextIndex
		rf.nextIndex = make([]int, len(rf.peers))
		for idx, _ := range rf.nextIndex {
			rf.nextIndex[idx] = len(rf.log) + 1 // next index to send
		}
		rf.matchIndex = make([]int, len(rf.peers))
		DPrintf("server %d becomes leader in term %d\n", rf.me, rf.currentTerm)
	} else { // already turn to follower due to other term exchange rpc
		DPrintf("server %d lose election for term %d\n", rf.me, rf.currentTerm)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []*Entry{} // slice make(,len,cap)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = 2 // initialized as follower
	numOfPeers := len(peers)
	rf.nextIndex = make([]int, numOfPeers)
	rf.matchIndex = make([]int, numOfPeers)

	rf.applyCh = applyCh
	rf.heartbeat = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
