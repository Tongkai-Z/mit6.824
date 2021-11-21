package shardctrler

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	gReply := sc.Serve(args)
	reply.Err = gReply.GetError()
	reply.WrongLeader = gReply.GetWrongLeader()
}

func (sc *ShardCtrler) leave(args *LeaveArgs) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
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
