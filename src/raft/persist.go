package raft

import (
	"bytes"

	"6.824/labgob"
)

type PersistedState struct {
	CurrentTerm int32
	VotedFor    int32
	Log         *RaftLog
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// persister got its own lock, so we should put lock in persist()
// always persist when the persisted states changed
func (rf *Raft) persist() {
	data := rf.getStateBytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	decoded := PersistedState{}
	if d.Decode(&decoded) != nil {
		DPrintf("readPersist data error for server %d", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = decoded.CurrentTerm
		rf.votedFor = decoded.VotedFor
		rf.log = decoded.Log
		rf.mu.Unlock()
		rf.sendSnapShotToApplyChan(snapshot, rf.log.LastIncludedTerm, rf.log.LastIncludedIndex)
	}

}

func (rf *Raft) getStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	s := PersistedState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}
	e.Encode(s)
	return w.Bytes()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// snapshot contains the changes that have been applied on leader
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex < int(rf.lastApplied) {
		// refuse to install old snapshot
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
	rf.persister.SaveStateAndSnapshot(rf.getStateBytes(), snapshot)
}
