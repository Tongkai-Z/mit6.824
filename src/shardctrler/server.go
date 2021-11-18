package shardctrler

import (
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
	term    int32

	clientSerialNum map[int64]int64
	// subscriber map for leader node
	subscriberMap map[int64]map[int64]chan int
	configs       []Config // indexed by config num
}

type Op struct {
	Conf      Config
	ClientID  int64
	SerialNum int64
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

func (sc *ShardCtrler) cloneConfig() *Config {
	newConfig := new(Config)
	newConfig.Num = len(sc.configs)
	DPrintf("config number: %d", newConfig.Num)
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

func (sc *ShardCtrler) checkProcessStatus(clientID, serialNumber int64, term int) Err {
	sc.mu.Lock()
	// lower term let req pass
	if sc.term < int32(term) {
		sc.term = int32(term)
		// clear sc.sub
		if clientMap, ok := sc.subscriberMap[clientID]; ok {
			// lower term request in progress
			if respChan, ok := clientMap[serialNumber]; ok {
				close(respChan)
			}
		}
	} else {
		// check if the request is still in progress
		if sub, ok := sc.subscriberMap[clientID]; ok {
			if _, ok := sub[serialNumber]; ok {
				sc.mu.Unlock()
				return "duplicate call, still in processing"
			}
		}
	}
	sc.mu.Unlock()
	return ""
}

// split shards as even as possible and move as few shards as possible
func (sc *ShardCtrler) reBalanceShards(config *Config) {
	DPrintf("config %d rebalance starts, shards: %v", config.Num, config.Shards)
	if len(config.Groups) == 0 {
		// assign all shard to zero
		for shardID := range config.Shards {
			config.Shards[shardID] = 0
		}
	} else {
		// assign random group for shard with no group
		var rGroup int
		for g := range config.Groups {
			rGroup = g
			break
		}
		for idx, group := range config.Shards {
			if group == 0 {
				config.Shards[idx] = rGroup
			}
		}
		// multiStart first assign all shard to a random one
		average := len(config.Shards) / len(config.Groups)
		DPrintf("average  %d", average)
		// inverse map
		// sort the group based on number of shards they served
		var gSlice groupSlice
		gID2sID := make(map[int][]int, len(config.Groups))
		for shardID, groupID := range config.Shards {
			gID2sID[groupID] = append(gID2sID[groupID], shardID)
		}

		for groupID := range config.Groups {
			newG := new(group)
			newG.shards = gID2sID[groupID]
			newG.groupID = groupID
			gSlice = append(gSlice, newG)
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
	DPrintf("config %d rebalance finished, shards: %v", config.Num, config.Shards)

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
	sc.subscriberMap = make(map[int64]map[int64]chan int)
	sc.clientSerialNum = make(map[int64]int64)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
