package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	OpName       string
	SerialNumber int64
	ClientID     int64
	Args         interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	term    int32

	maxAppliedCmd int64
	maxraftstate  int // snapshot if log grows this big

	ma map[string]string // kv implementation
	// subscriber map for leader node
	subscriberMap map[int64]map[int64]chan int
	//idempotent number for request
	serialMap map[int64]int64
}

// get can read from any server in majority
// but get should not read stale data, so easiest way is only reading from leader
// we can ignore the idempotent check for get, since it has not side effect
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Err = kv.checkProcessStatus(args.ClientID, args.SerialNumber)
	if reply.Err != "" {
		return
	}

	// make the op
	op := &Op{
		OpName:       GetOp,
		Args:         args,
		SerialNumber: args.SerialNumber,
		ClientID:     args.ClientID,
	}
	commandIndex, _, isLeader := kv.rf.Start(op)
	// not leader return error
	if !isLeader {
		reply.Err = "server not leader"
		return
	}
	// subscribe the operation and wait for applychan
	sub := make(chan int, 1)
	kv.subscribe(args.ClientID, args.SerialNumber, sub)
	DPrintf("[server %d]subscribe for get cmd %d, key: %s from [clerk %d] serial number: %d", kv.me, commandIndex, args.Key, args.ClientID, args.SerialNumber)
	select {
	case _, ok := <-sub:
		if !ok { //chan closed
			reply.Err = "same request in higher term is processed"
			return
		}

		DPrintf("[server %d]get cmd %d, notified from [clerk %d] serial number: %d", kv.me, commandIndex, args.ClientID, args.SerialNumber)

		kv.mu.Lock()
		reply.Value = kv.ma[args.Key]
		DPrintf("[server %d]get cmd %d, send to [clerk %d] serial number: %d, key: %s, val: %s", kv.me, commandIndex, args.ClientID, args.SerialNumber, args.Key, reply.Value)
		kv.mu.Unlock()
	case <-time.After(ServerTimeOut):
		reply.Err = "raft time out"
		return
	}

}

func (kv *KVServer) checkProcessStatus(clientID, serialNumber int64) Err {
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		return "server not leader"
	}

	kv.mu.Lock()
	// lower term let req pass
	if kv.term < int32(term) {
		kv.term = int32(term)
		// clear kv.sub
		if clientMap, ok := kv.subscriberMap[clientID]; ok {
			// lower term request in progress
			if respChan, ok := clientMap[serialNumber]; ok {
				close(respChan)
			}
		}
	} else {
		// check if the request is still in progress
		if sub, ok := kv.subscriberMap[clientID]; ok {
			if _, ok := sub[serialNumber]; ok {
				kv.mu.Unlock()
				return "duplicate call, still in processing"
			}
		}
	}
	kv.mu.Unlock()
	return ""
}

// only leader can register the serialNumber
// duplicate put should return normally since the response could be lost due to internet
// so for processed opts, we should still response gracefully
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err = kv.checkProcessStatus(args.ClientID, args.SerialNumber)
	if reply.Err != "" {
		return
	}

	//idempotent
	kv.mu.Lock()
	if kv.serialMap[args.ClientID] >= args.SerialNumber {
		kv.mu.Unlock()
		return

	}
	kv.mu.Unlock()

	op := &Op{
		OpName:       PutAppendOp,
		Args:         args,
		SerialNumber: args.SerialNumber,
		ClientID:     args.ClientID,
	}
	commandIndex, _, isLeader := kv.rf.Start(op)
	// not leader return error
	if !isLeader {
		reply.Err = "server not leader"
		return
	}

	// subscribe the operation and wait for applychan
	sub := make(chan int, 1)
	kv.subscribe(args.ClientID, args.SerialNumber, sub)
	DPrintf("[server %d]subscribe for putAppend cmd %d, key: %s, val: %s, from [clerk %d] serial number: %d", kv.me, commandIndex, args.Key, args.Value, args.ClientID, args.SerialNumber)

	select {
	case _, ok := <-sub:
		if !ok { //chan closed
			reply.Err = "same request in higher term is processed"
			return
		}
		DPrintf("[server %d]put cmd %d, notified from [clerk %d] serial number: %d, key: %s, val: %s", kv.me, commandIndex, args.ClientID, args.SerialNumber, args.Key, args.Value)
	case <-time.After(ServerTimeOut):
		reply.Err = "raft time out"
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})
	labgob.Register(&PutAppendArgs{})
	labgob.Register(&GetArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ma = make(map[string]string)
	kv.subscriberMap = make(map[int64]map[int64]chan int)
	kv.serialMap = make(map[int64]int64)
	//kv.appliedMap = make(map[int64]int64)
	go kv.applyObserver()

	return kv
}
