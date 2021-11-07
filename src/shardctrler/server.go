package shardctrler

import (
	"fmt"
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	clientSerialNum map[int64]int64
	configs         []Config // indexed by config num
}

type Op struct {
	// Your data here.
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	if _, leader := sc.rf.GetState(); !leader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	// detect duplicate call
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.clientSerialNum[args.ClientID] >= args.SerialNum {
		// duplicate req, reply normally
		return
	} else {
		// update the serialNum
		sc.clientSerialNum[args.ClientID] = args.SerialNum
	}

	// update the configuration
	err, newConfig := sc.cloneConfig(args.Servers)
	if len(err) > 0 {
		reply.Err = err
		return
	}
	sc.reBalanceShards(newConfig)
	sc.configs = append(sc.configs, *newConfig)
}

func (sc *ShardCtrler) cloneConfig(servers map[int][]string) (Err, *Config) {
	newConfig := new(Config)
	newConfig.Num = len(sc.configs)
	prev := len(sc.configs) - 1
	newConfig.Groups = make(map[int][]string)
	for key, val := range sc.configs[prev].Groups {
		newConfig.Groups[key] = val
	}
	for key, val := range servers {
		if _, ok := newConfig.Groups[key]; ok {
			return Err(fmt.Sprintf("GID already in use: %d", key)), nil
		}
		newConfig.Groups[key] = val
	}
	return "", newConfig
}

// split shards as even as possible and move as few shards as possible
func (sc *ShardCtrler) reBalanceShards(config *Config) {

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	// config num is started from zero
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
