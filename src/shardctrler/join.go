package shardctrler

import "fmt"

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// check leader
	var term int
	var leader bool
	if term, leader = sc.rf.GetState(); !leader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	err := sc.checkProcessStatus(args.ClientID, args.SerialNum, term)
	if err != "" {
		reply.Err = err
		return
	}

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
	DPrintf("join opt finished, config: %+v", newConfig)
}
