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
	currentTerm int32 //increase monotonically FIXME: use Mutex to protect
	votedFor    int32
	log         []*Entry // index starting from 1
	// volatile state
	commitIndex int32
	lastApplied int
	state       int32 // 1,2,3 leader, follower, candidate
	// volatiel state on leader
	nextIndex  []int
	matchIndex []int
}

type Entry struct {
	Id      int // deduplication
	Term    int
	Command interface{}
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
// reset the timer?
// Receiver implementation: 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// (2A, 2B).
	// 2A
	// FIXME: requestVote also set the heartbeat preventing other follower time-out
	// FIXME: requestVote only reset heartbeat when it grant the vote
	rf.checkTerm(args.Term)
	rf.mu.Lock() // protect rf's field
	// last entry's term
	lastTerm := 0
	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
	}
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if rf.votedFor == -1 || rf.votedFor == int32(args.CandidateId) {
		// candidate is at least up-to-date as receiver
		if lastTerm < args.LastLogTerm || len(rf.log) <= args.LastLogIndex {
			atomic.StoreInt32(&rf.heartbeat, 1)
			reply.VoteGranted = true
			rf.votedFor = int32(args.CandidateId)
			reply.Term = args.Term
		} else {
			reply.VoteGranted = false
			reply.Term = args.Term
		}
	} else { // voted
		reply.VoteGranted = false
		reply.Term = args.Term
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// simple time out: when time.sleep for a random time and finds no heartbeat received
// time-out for each server is randomly set between 100ms-300ms
// it would start election
func (rf *Raft) ticker() {
	for rf.killed() == false {
		currentTerm, isleader := rf.GetState()
		if !isleader {
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep()
			interval := rand.Intn(150) + 150
			time.Sleep(time.Duration(interval) * time.Millisecond)
			// hb := atomic.LoadInt32(&rf.heartbeat)
			// heartbeat reset to 0
			if !atomic.CompareAndSwapInt32(&rf.heartbeat, 1, 0) {
				// increment the current term by one
				rf.startElection(int32(currentTerm) + 1)
			}
		} else {
			// wins the election turn into leader loop
			// send out AppendEntries RPC as heartbeat
			time.Sleep(time.Duration(120) * time.Millisecond)
			rf.sendHeartBeat()
		}

	}
}

// this function send heartbeat to all peers concurrently
func (rf *Raft) sendHeartBeat() {
	req := &AppendEntriesArgs{}
	rf.mu.Lock()
	req.Term = rf.currentTerm
	req.LeaderId = rf.me
	req.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()
	// rf.mu.Lock()
	// req.PrevLogIndex = len(rf.log)
	// prevLogTerm := 0
	// if len(rf.log) > 0 {
	// 	prevLogTerm = rf.log[len(rf.log)-1].Term
	// }
	// req.PrevLogTerm = prevLogTerm
	// rf.mu.Unlock()
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(i int) {
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(i, req, reply)
			}(idx)
		}
	}
}

// This function appendEntries to followers
// note that this message also establish the leadership, so it will clear the voteFor
// if the current server is a candidate, it will compare the term
// if currentTerm <= receivedTerm it would turn to follower at once
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
	if args.Entries == nil { //heartbeat
		//set heartbeat flag
		atomic.CompareAndSwapInt32(&rf.heartbeat, 0, 1)
	}
}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	Term    int32
	Success bool
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
	lastLogIndex := len(rf.log)
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	rf.mu.Unlock()
	var wg sync.WaitGroup
	majority := int32(len(rf.peers) / 2)
	for index, _ := range rf.peers {
		if index != rf.me {
			wg.Add(1)
			go func(idx int) {
				req := &RequestVoteArgs{}
				req.CandidateId = rf.me
				req.Term = termForElection
				req.LastLogIndex = lastLogIndex
				req.LastLogTerm = lastLogTerm
				resp := &RequestVoteReply{}
				ok := rf.sendRequestVote(idx, req, resp)
				// wait for majority votes
				if ok && resp.VoteGranted {
					atomic.AddInt32(&majority, -1)
				}
				// when a server find its current term is smaller
				// it would turn into follower
				rf.checkTerm(resp.Term)
				wg.Done()
			}(index)

		}
	}
	// interval := rand.Intn(30) + 30
	// time.Sleep(time.Duration(interval) * time.Millisecond)
	// since Call() must return, we don't need set the timeout but use waitgroup
	// timeout check majority vote
	wg.Wait()
	rf.mu.Lock()
	if rf.state == 3 {
		if int(atomic.LoadInt32(&majority)) <= 0 { // win the election
			rf.state = 1 // turn to leader
		} else { // no winner
			rf.state = 2 // turn to follower
		}
	}
	// someone already win or current term is stale
	rf.mu.Unlock()
	// if no majority votes received, then we can turn to follower after a specific time out
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
	rf.log = make([]*Entry, 0) // slice make(,len,cap)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = 2 // initialized as follower
	numOfPeers := len(peers)
	rf.nextIndex = make([]int, numOfPeers)
	rf.matchIndex = make([]int, numOfPeers)

	rf.heartbeat = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
