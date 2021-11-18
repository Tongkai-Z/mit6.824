package shardctrler

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	defer func() {
		DPrintf("[server %d]Query req: %+v", sc.me, args)
		DPrintf("[server %d]Query reply: %+v", sc.me, reply)
	}()
	// check leader
	if _, leader := sc.rf.GetState(); !leader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// get the config
	// -1 or bigger than the largest config num, return latest
	if args.Num >= len(sc.configs) || args.Num < 0 {
		DPrintf("config num %d out of range, len: %d", args.Num, len(sc.configs))
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
}
