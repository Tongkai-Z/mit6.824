package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	Debug       = true
	GetOp       = "getOperation"
	PutAppendOp = "putAppendOperation"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpName string
	Args   interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	rwLock *sync.RWMutex
	ma     map[string]string // kv implementation
	// subscriber map for leader node
	subscriberMap map[int]chan int
	//idempotent number for request
	serialMap map[int64]int64
	// applied msg idempotent
	appliedCmdIdx int64
}

// get can read from any server in majority
// but get should not read stale data, so easiest way is only reading from leader
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// make the op
	op := &Op{
		OpName: GetOp,
		Args:   args,
	}
	commandIndex, _, isLeader := kv.rf.Start(op)
	// not leader return error
	if !isLeader {
		reply.Err = "server not leader"
		return
	}
	kv.rwLock.Lock()
	if kv.serialMap[args.ClientID] >= args.SerialNumber {
		reply.Err = "duplicate call"
		return
	}
	kv.serialMap[args.ClientID] = args.SerialNumber
	kv.rwLock.Unlock()
	// subscribe the operation and wait for applychan
	sub := make(chan int, 1)
	kv.subscribe(commandIndex, sub)
	DPrintf("[server %d]subscribe for get cmd %d, key: %s from [clerk %d]", kv.me, commandIndex, args.Key, args.ClientID)
	<-sub
	DPrintf("[server %d]get cmd %d, notified from [clerk %d]", kv.me, commandIndex, args.ClientID)
	kv.rwLock.RLock()
	reply.Value = kv.ma[args.Key]
	kv.rwLock.RUnlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := &Op{
		OpName: PutAppendOp,
		Args:   args,
	}
	commandIndex, _, isLeader := kv.rf.Start(op)
	// not leader return error
	if !isLeader {
		reply.Err = "server not leader"
		return
	}

	kv.rwLock.Lock()
	if kv.serialMap[args.ClientID] >= args.SerialNumber {
		reply.Err = "duplicate call"
		return
	}
	kv.serialMap[args.ClientID] = args.SerialNumber
	kv.rwLock.Unlock()

	// subscribe the operation and wait for applychan
	sub := make(chan int, 1)
	kv.subscribe(commandIndex, sub)
	DPrintf("[server %d]subscribe for putAppend cmd %d, key: %s, val: %s, from [clerk %d]", kv.me, commandIndex, args.Key, args.Value, args.ClientID)
	<-sub
	DPrintf("[server %d]put cmd %d, notified from [clerk %d]", kv.me, commandIndex, args.ClientID)
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
	kv.subscriberMap = make(map[int]chan int)
	kv.rwLock = new(sync.RWMutex)
	kv.serialMap = make(map[int64]int64)
	go kv.applyObserver()

	return kv
}

func (kv *KVServer) subscribe(cmdIdx int, subChan chan int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.subscriberMap[cmdIdx] = subChan
}

// consume the apply msg and if this node is leader publish the msg to subscriber accordingly
//TODO: inorder to guarantee serialization we must apply first and then publish
func (kv *KVServer) applyObserver() {
	for {
		cmd := <-kv.applyCh
		// apply this cmd
		if cmd.CommandValid {
			kv.applyCommand(cmd.Command.(*Op), cmd.CommandIndex)
		}
		// leader node pub
		if _, isLeader := kv.rf.GetState(); isLeader {
			go kv.publishCommand(cmd.CommandIndex)
		}
	}
}

func (kv *KVServer) applyCommand(op *Op, cmdIdx int) {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()
	// check applied cmdIdx
	if cmdIdx <= int(kv.appliedCmdIdx) {
		return
	}
	kv.appliedCmdIdx = int64(cmdIdx)
	DPrintf("[server %d]cmd %d applied", kv.me, cmdIdx)
	// discard get
	if op.OpName == GetOp {
		return
	}
	if op.OpName == PutAppendOp {
		args := op.Args.(*PutAppendArgs)
		putOrAppend := args.Op
		if putOrAppend == "Append" {
			kv.ma[args.Key] = kv.ma[args.Key] + args.Value
		} else {
			kv.ma[args.Key] = args.Value
		}

	}
}

func (kv *KVServer) publishCommand(commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if ch, ok := kv.subscriberMap[commandIndex]; ok {
		ch <- 1
		delete(kv.subscriberMap, commandIndex)
	}
}
