package shardctrler

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
	DPrintf("move opt finished, config: %+v", newConfig)
}
