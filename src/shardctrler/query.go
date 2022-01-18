package shardctrler

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	gReply := sc.Serve(args)
	if gReply.GetConfig() != nil {
		reply.Config = *gReply.GetConfig()
	}
	reply.Err = gReply.GetError()
	reply.WrongLeader = gReply.GetWrongLeader()
}

func (sc *ShardCtrler) query(args *QueryArgs) *Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	var ret Config
	// get the config
	// -1 or bigger than the largest config num, return latest
	if args.Num >= len(sc.configs) || args.Num < 0 {
		// DPrintf("config num %d out of range, len: %d", args.Num, len(sc.configs))
		ret = sc.configs[len(sc.configs)-1]
	} else {
		ret = sc.configs[args.Num]
	}
	return &ret

}
