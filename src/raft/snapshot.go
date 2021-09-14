package raft

import "sync/atomic"

type InstallSnapshotArgs struct {
	Term              int32
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int32
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// snapshot contains the changes that have been applied on leader
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex < int(rf.commitIndex) {
		// refuse to install old snapshot
		DPrintf("server %d resist install the snapshot", rf.me)
		return false
	}
	// trim the log and persist the new state
	rf.log = &RaftLog{
		Log:               rf.log.SliceToTail(lastIncludedIndex + 1),
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
	}
	rf.lastApplied = int32(lastIncludedIndex)
	rf.commitIndex = int32(lastIncludedIndex)
	rf.persister.SaveStateAndSnapshot(rf.getStateBytes(), snapshot)
	DPrintf("server %d installs the snapshot", rf.me)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.log.LastIncludedIndex >= index {
		return
	}
	rf.log.LastIncludedTerm = rf.log.Get(index).Term
	rf.log.Log = rf.log.SliceToTail(index + 1)
	rf.log.LastIncludedIndex = index
	if rf.lastApplied < int32(index) {
		rf.lastApplied = int32(index)
	}
	rf.persister.SaveStateAndSnapshot(rf.getStateBytes(), snapshot)
	DPrintf("server %d persists its state to snapshot", rf.me)
}

func (rf *Raft) sendInstallSnapshot(peer int) {
	rf.mu.Lock()
	req := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.LastIncludedIndex,
		LastIncludedTerm:  rf.log.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := &InstallSnapshotReply{}
	DPrintf("server %d send snapshot to server %d args: %+v", req.LeaderId, peer, req)
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", req, reply)
	if ok {
		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			DPrintf("server %d append rejected by server %d due to lower term", rf.me, peer)
			rf.state = 2
			rf.currentTerm = reply.Term
			rf.persist()
		}
		rf.mu.Unlock()
	}
}

// check term and send the snapshot via apply channel
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	rf.state = 2
	rf.votedFor = -1
	rf.currentTerm = args.Term
	rf.persist()
	atomic.CompareAndSwapInt32(&rf.heartbeat, 0, 1)
	// send snapshot via apply channel
	go rf.sendSnapShotToApplyChan(args.Data, args.LastIncludedTerm, args.LastIncludedIndex)
}

func (rf *Raft) sendSnapShotToApplyChan(snapshot []byte, term, index int) {
	var applyMsg ApplyMsg
	applyMsg.SnapshotValid = true
	applyMsg.Snapshot = snapshot
	applyMsg.SnapshotIndex = index
	applyMsg.SnapshotTerm = term
	rf.applyCh <- applyMsg
}
