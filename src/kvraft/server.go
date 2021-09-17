package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	Debug         = false
	ServerTimeOut = 1 * time.Second
	GetOp         = "getOperation"
	PutAppendOp   = "putAppendOperation"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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

	maxraftstate int // snapshot if log grows this big

	rwLock *sync.RWMutex
	ma     map[string]string // kv implementation
	// subscriber map for leader node
	subscriberMap map[int64]map[int64]chan int
	//idempotent number for request
	serialMap map[int64]int64
	// applied msg idempotent
	appliedMap map[int64]int64
}

// get can read from any server in majority
// but get should not read stale data, so easiest way is only reading from leader
// we can ignore the idempotent check for get, since it has not side effect
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = "server not leader"
		return
	}
	kv.mu.Lock()
	// lower term let req pass
	if atomic.LoadInt32(&kv.term) < int32(term) {
		atomic.StoreInt32(&kv.term, int32(term))
		// clear kv.sub
		if clientMap, ok := kv.subscriberMap[args.ClientID]; ok {
			// lower term request in progress
			if respChan, ok := clientMap[args.SerialNumber]; ok {
				close(respChan)
			}
		}
	} else {
		// check if the request is still in progress
		if sub, ok := kv.subscriberMap[args.ClientID]; ok {
			if _, ok := sub[args.SerialNumber]; ok {
				reply.Err = "duplicate call, still in processing"
				kv.mu.Unlock()
				return
			}
		}
	}
	kv.mu.Unlock()

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

		kv.rwLock.RLock()
		reply.Value = kv.ma[args.Key]
		DPrintf("[server %d]get cmd %d, send to [clerk %d] serial number: %d, key: %s, val: %s", kv.me, commandIndex, args.ClientID, args.SerialNumber, args.Key, reply.Value)
		kv.rwLock.RUnlock()
	case <-time.After(ServerTimeOut):
		reply.Err = "raft time out"
		return
	}

}

// only leader can register the serialNumber
// duplicate put should return normally since the response could be lost due to internet
// so for processed opts, we should still response gracefully
//TODO: 新term的重复请求被放行的话，怎么回收之前那个被监听的chan，并返回err
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = "server not leader"
		return

	}

	kv.mu.Lock()
	// lower term let req pass
	if atomic.LoadInt32(&kv.term) < int32(term) {
		atomic.StoreInt32(&kv.term, int32(term))
		// clear kv.sub
		if clientMap, ok := kv.subscriberMap[args.ClientID]; ok {
			// lower term request in progress
			if respChan, ok := clientMap[args.SerialNumber]; ok {
				close(respChan)
			}
		}
	} else {
		// check if the request is still in progress
		if sub, ok := kv.subscriberMap[args.ClientID]; ok {
			if _, ok := sub[args.SerialNumber]; ok {
				reply.Err = "duplicate call, still in processing"
				kv.mu.Unlock()
				return
			}
		}
	}
	kv.mu.Unlock()

	//idempotent
	kv.rwLock.RLock()
	if kv.serialMap[args.ClientID] >= args.SerialNumber {
		kv.rwLock.RUnlock()
		return

	}
	kv.rwLock.RUnlock()

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

		kv.rwLock.Lock()
		if args.SerialNumber > kv.serialMap[args.ClientID] {
			kv.serialMap[args.ClientID] = args.SerialNumber
		}
		kv.rwLock.Unlock()
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
	kv.rwLock = new(sync.RWMutex)
	kv.serialMap = make(map[int64]int64)
	kv.appliedMap = make(map[int64]int64)
	go kv.applyObserver()

	return kv
}

func (kv *KVServer) subscribe(clientID, serialNumber int64, subChan chan int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.subscriberMap[clientID] == nil {
		kv.subscriberMap[clientID] = make(map[int64]chan int)
	}
	kv.subscriberMap[clientID][serialNumber] = subChan
}

// consume the apply msg and if this node is leader publish the msg to subscriber accordingly
// inorder to guarantee serialization we must apply first and then publish
func (kv *KVServer) applyObserver() {
	for cmd := range kv.applyCh {
		// apply this cmd
		if cmd.CommandValid {
			kv.applyCommand(cmd.Command.(*Op), cmd.CommandIndex)
			// leader node pub
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.publishCommand(cmd.Command.(*Op).ClientID, cmd.Command.(*Op).SerialNumber)
			}
		}

	}
}

func (kv *KVServer) applyCommand(op *Op, cmdIdx int) {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	if op.OpName == GetOp {
		args := op.Args.(*GetArgs)
		DPrintf("[server %d]get cmd %d serial number %d from client %d applied key: %s, val: %s",
			kv.me, cmdIdx, args.SerialNumber, args.ClientID, args.Key, kv.ma[args.Key])
		return
	}
	if op.OpName == PutAppendOp {
		args := op.Args.(*PutAppendArgs)
		if kv.appliedMap[args.ClientID] >= args.SerialNumber {
			//already applied
			DPrintf("[server %d]putAppend cmd %d serial number %d from client %d duplicate call, key: %s, val: %s",
				kv.me, cmdIdx, args.SerialNumber, args.ClientID, args.Key, kv.ma[args.Key])
			return
		}
		// update apply map
		kv.appliedMap[args.ClientID] = args.SerialNumber
		putOrAppend := args.Op
		if putOrAppend == "Append" {
			kv.ma[args.Key] = kv.ma[args.Key] + args.Value
		} else {
			kv.ma[args.Key] = args.Value
		}
		DPrintf("[server %d]putAppend cmd %d serial number %d from client %d applied, key: %s, val: %s",
			kv.me, cmdIdx, args.SerialNumber, args.ClientID, args.Key, kv.ma[args.Key])
	}
}

func (kv *KVServer) publishCommand(clientID, serialNumber int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if sub, ok := kv.subscriberMap[clientID]; ok {
		if ch, ok := sub[serialNumber]; ok {
			ch <- 1
			delete(sub, serialNumber)
		}
	}
}
