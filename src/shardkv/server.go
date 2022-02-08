package shardkv

import (
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	term    int32
	dead    int32

	make_end           func(string) *labrpc.ClientEnd
	gid                int
	ctrlers            []*labrpc.ClientEnd
	sc                 *shardctrler.Clerk
	config             *shardctrler.Config
	shardTable         [shardctrler.NShards]int
	shardMigrationChan chan *MigrationArgs

	maxAppliedCmd int64
	maxraftstate  int // snapshot if log grows this big

	ma [shardctrler.NShards]map[string]string // kv implementation shard map
	// subscriber map for leader node
	subscriberMap map[int64]map[int64]chan string
	//idempotent number for request
	serialMap map[int64]int64
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	resp := kv.Serve(args)
	reply.Err = resp.GetErr()
	if val := resp.GetValue(); val != nil {
		reply.Value = *resp.GetValue()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	resp := kv.Serve(args)
	reply.Err = resp.GetErr()
}

func (kv *ShardKV) get(args *GetArgs) *string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ma := kv.ma[key2shard(args.Key)]
	val := ma[args.Key]
	return &val
}

func (kv *ShardKV) putAppend(args *PutAppendArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.serialMap[args.ClientID] >= args.SerialNumber {
		//already applied
		DPrintf("[server %d group %d]putAppend serial number %d from client %d duplicate call, key: %s, val: %s",
			kv.me, kv.gid, args.SerialNumber, args.ClientID, args.Key, kv.ma[key2shard(args.Key)][args.Key])
		return
	}
	// update serial map
	kv.serialMap[args.ClientID] = args.SerialNumber
	putOrAppend := args.Op
	shard := key2shard(args.Key)
	ma := kv.ma[shard]
	if putOrAppend == "Append" {
		ma[args.Key] = ma[args.Key] + args.Value
	} else {
		ma[args.Key] = args.Value
	}
	DPrintf("[server %d group %d]putAppend serial number %d from client %d applied, key: %s, shard: %d, val: %s",
		kv.me, kv.gid, args.SerialNumber, args.ClientID, args.Key, shard, ma[args.Key])
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) isKilled() bool {
	return atomic.LoadInt32(&kv.dead) == int32(1)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})
	labgob.Register(&PutAppendArgs{})
	labgob.Register(&GetArgs{})
	labgob.Register(&UpdateConfigArgs{})
	labgob.Register(&MigrationArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.sc = shardctrler.MakeClerk(kv.ctrlers)
	kv.shardMigrationChan = make(chan *MigrationArgs, 10000)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for idx, _ := range kv.ma {
		kv.ma[idx] = make(map[string]string)
	}

	kv.subscriberMap = make(map[int64]map[int64]chan string)
	kv.serialMap = make(map[int64]int64)

	go kv.applyObserver()
	go kv.pollShardConfig()
	go kv.shardMigrationProcessor()

	return kv
}
