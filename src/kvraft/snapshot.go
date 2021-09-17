package kvraft

import (
	"bytes"

	"6.824/labgob"
)

type ServerPersistedState struct {
	Term          int32
	MaxAppliedCmd int64
	Ma            map[string]string
	SerialMap     map[int64]int64
}

// this function checks whether the raft log exceeds the raftmaxstate
func (kv *KVServer) condSnapshot() {
	if kv.maxraftstate != -1 && kv.maxraftstate <= kv.rf.GetLogSize() {
		kv.mu.Lock()
		// persist kvServer state
		DPrintf("[server %d] snapshot state, index: %d", kv.me, kv.maxAppliedCmd)
		kvState := kv.encodeState()
		// send snapshot to raft
		kv.rf.Snapshot(int(kv.maxAppliedCmd), kvState)
		kv.mu.Unlock()
	}
}

// this function must be called within a critical section
func (kv *KVServer) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	s := ServerPersistedState{
		Term:          kv.term,
		MaxAppliedCmd: kv.maxAppliedCmd,
		Ma:            kv.ma,
		SerialMap:     kv.serialMap,
	}
	e.Encode(s)
	return w.Bytes()
}

// blocking other cmd application
func (kv *KVServer) applySnapshot(term, index int, snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(term, index, snapshot) {
		// read state from snapshot
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		decoded := ServerPersistedState{}
		if d.Decode(&decoded) != nil {
			DPrintf("[server %d] readPersist data error", kv.me)
		} else {
			kv.term = decoded.Term
			kv.maxAppliedCmd = decoded.MaxAppliedCmd
			kv.ma = decoded.Ma
			kv.serialMap = decoded.SerialMap
			DPrintf("[server %d] snapshot appied, index: %d", kv.me, kv.maxAppliedCmd)
		}

	}
}
