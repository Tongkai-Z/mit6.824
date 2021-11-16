package shardctrler

import (
	"fmt"
	"sort"
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

type group struct {
	groupID int
	shards  []int
}

type groupSlice []*group

func (g groupSlice) Len() int {
	return len(g)
}

// desc
func (g groupSlice) Less(i, j int) bool {
	return len(g[i].shards) > len(g[j].shards)
}

func (g groupSlice) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// check leader
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
	newConfig := sc.cloneConfig()
	for key, val := range args.Servers {
		if _, ok := newConfig.Groups[key]; ok {
			reply.Err = Err(fmt.Sprintf("GID already in use: %d", key))
			return
		}
		newConfig.Groups[key] = val
	}
	sc.reBalanceShards(newConfig)
	sc.configs = append(sc.configs, *newConfig)
}

func (sc *ShardCtrler) cloneConfig() *Config {
	newConfig := new(Config)
	newConfig.Num = len(sc.configs)
	prevConfig := sc.configs[len(sc.configs)-1]
	newConfig.Groups = make(map[int][]string)
	for key, val := range prevConfig.Groups {
		newConfig.Groups[key] = val
	}
	// clone shards
	for shardID, gID := range prevConfig.Shards {
		newConfig.Shards[shardID] = gID
	}
	return newConfig
}

// split shards as even as possible and move as few shards as possible
func (sc *ShardCtrler) reBalanceShards(config *Config) {
	average := len(config.Shards) / len(config.Groups)
	// sort the group based on number of shards they served
	var gSlice groupSlice
	groupMap := make(map[int]*group)
	for shardID, groupID := range config.Shards {
		if g, ok := groupMap[groupID]; ok {
			g.shards = append(g.shards, shardID)
		} else {
			newG := new(group)
			newG.shards = append(newG.shards, shardID)
			groupMap[groupID] = newG
			gSlice = append(gSlice, newG)
		}
	}
	sort.Sort(gSlice)

	// rebalance
	left := 0
	right := len(gSlice) - 1

	for left < right {
		// move shards from left to right
		leftG := gSlice[left]
		rightG := gSlice[right]
		deltaL := len(leftG.shards) - average
		deltaR := average - len(rightG.shards)
		moved := min(deltaL, deltaR)
		if moved != 0 {
			rightG.shards = append(rightG.shards, leftG.shards[:moved]...)
			leftG.shards = leftG.shards[moved:]
		}
		if deltaL == 0 {
			left++
		}
		if deltaR == 0 {
			right--
		}
	}

	for _, group := range gSlice {
		for _, s := range group.shards {
			config.Shards[s] = group.groupID
		}
	}
}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

func min(i, j int) int {
	if i > j {
		return j
	}
	return i
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// check leader
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

	newConfig := sc.cloneConfig()
	for _, gID := range args.GIDs {
		delete(newConfig.Groups, gID)
	}
	sc.reBalanceShards(newConfig)
	sc.configs = append(sc.configs, *newConfig)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// move shard to group
	// check leader
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

	newConfig := sc.cloneConfig()
	newConfig.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, *newConfig)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {

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
