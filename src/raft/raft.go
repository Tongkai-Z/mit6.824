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

	// persisted state
	currentTerm int32 //increase monotonically
	votedFor    int32
	log         []*Entry // index starting from 1

	applyCh chan ApplyMsg
	// volatile state
	commitIndex int32
	lastApplied int32
	state       int32 // 1,2,3 leader, follower, candidate
	// volatiel state on leader
	nextIndex  []int // optimistic
	matchIndex []int // pessimistic
	batchFlag  int32
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
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = int(rf.currentTerm)
	if rf.state == 1 {
		isLeader = true
	}
	rf.mu.Unlock()
	return term, isLeader
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
	term, isLeader = rf.GetState()
	if !isLeader || rf.killed() {
		return index, term, isLeader
	}
	entry := &Entry{}
	entry.Term = term
	entry.Command = command
	rf.mu.Lock()
	rf.log = append(rf.log, entry)
	rf.persist()
	index = len(rf.log)
	DPrintf("sever: %d new cmd arrives at index: %d, term: %d, log: %+v", rf.me, index, term, rf.log)
	rf.mu.Unlock()
	// TODO: use batch process to reduce the number of rpcs and go routines
	// 2C: too many existing goroutine
	// use heartbeat interval to process log replication in batches
	//rf.processLogReplication()
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
				// TODO: should startElection using another new goroutine?
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
	rf.batchFlag = 0
	rf.applyCh = applyCh
	rf.heartbeat = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
