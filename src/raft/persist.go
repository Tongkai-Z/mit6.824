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
	DPrintf("server %d persist its data", rf.me)
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
		rf.lastApplied = int32(rf.log.LastIncludedIndex)
		rf.mu.Unlock()
		if len(snapshot) > 0 {
			go rf.sendSnapShotToApplyChan(snapshot, rf.log.LastIncludedTerm, rf.log.LastIncludedIndex)
		}
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
