package shardctrler

import (
	"fmt"
)

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	gReply := sc.Serve(args)
	reply.Err = gReply.GetError()
	reply.WrongLeader = gReply.GetWrongLeader()
}

func (sc *ShardCtrler) join(args *JoinArgs) Err {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// update the configuration
	newConfig := sc.cloneConfig()
	for key, val := range args.Servers {
		if _, ok := newConfig.Groups[key]; ok {
			return Err(fmt.Sprintf("GID already in use: %d", key))
		}
		newConfig.Groups[key] = val
	}
	sc.reBalanceShards(newConfig)
	sc.configs = append(sc.configs, *newConfig)
	DPrintf("join opt finished, config: %+v", newConfig)
	return ""
}
