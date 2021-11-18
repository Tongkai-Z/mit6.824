package shardctrler

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
		// unassign the shard
		for idx, id := range newConfig.Shards {
			if gID == id {
				newConfig.Shards[idx] = 0
			}
		}
	}
	sc.reBalanceShards(newConfig)
	sc.configs = append(sc.configs, *newConfig)
	DPrintf("leave opt finished, config: %+v", newConfig)
}
