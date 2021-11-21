package shardctrler

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	gReply := sc.Serve(args)
	reply.Err = gReply.GetError()
	reply.WrongLeader = gReply.GetWrongLeader()
}

func (sc *ShardCtrler) move(args *MoveArgs) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	newConfig := sc.cloneConfig()
	newConfig.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, *newConfig)
	DPrintf("move opt finished, config: %+v", newConfig)
}
