package shardkv

import (
	"time"

	"6.824/shardctrler"
)

func (kv *ShardKV) pollShardConfig() {
	for !kv.isKilled() {
		time.Sleep(PollConfigInterval)
		_, isLeader := kv.rf.GetState()
		if isLeader {
			config := kv.sc.Query(-1)
			kv.mu.Lock()
			if kv.config == nil || kv.config.Num < config.Num {
				// config change
				configArgs := &UpdateConfigArgs{
					ConfigNum: config.Num,
				}
				commandIndex, _, isLeader := kv.rf.Start(&Op{
					Args: configArgs,
				})
				if isLeader {
					DPrintf("[server %d gid %d] replicate config cmd %d, %+v", kv.me, kv.gid, commandIndex, configArgs)
				}
			}
			kv.mu.Unlock()
		}
	}
}

// check if the group is qualified to serve the key
func (kv *ShardKV) checkShard(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config == nil {
		return ErrWrongGroup
	}
	shard := key2shard(key)
	if kv.gid != kv.config.Shards[shard] { // check shard
		return ErrWrongGroup
	}

	// check shard migration status
	if kv.shardTable[shard] != kv.config.Num {
		DPrintf("[server %d gid %d] check shard key: %s, shard: %d version: %d, config version: %d", kv.me, kv.gid, key, shard, kv.shardTable[shard], kv.config.Num)
		return ErrInternal // let client retry
	}

	return OK

}

// Process reconfiguration one at a time in order
// reconfig should not skip configs
func (kv *ShardKV) reConfig(num int) {
	currConfig := kv.sc.Query(num)
	kv.mu.Lock()
	prevConfig := kv.config
	if prevConfig == nil || prevConfig.Num < currConfig.Num {
		prevNum := 0
		if prevConfig != nil {
			prevNum = prevConfig.Num
		}
		DPrintf("[server %d gid %d] config updated, prev %d, curr %d, shard2Gid: %v", kv.me, kv.gid, prevNum, currConfig.Num, currConfig.Shards)
		kv.config = &currConfig
	}
	kv.mu.Unlock()
	currNum := 0
	if prevConfig != nil {
		currNum = prevConfig.Num
	}
	for currNum <= num {
		next := kv.sc.Query(currNum + 1)
		kv.updateShardTable(prevConfig, &next)
		prevConfig = &next
		currNum = next.Num
	}

}

// if the desGID has not updated its config before it receive the install payload,
// then updateConfig would cause the shard to be pending forever since it has already been installed.
//  MigrationShard check config version first
func (kv *ShardKV) updateShardTable(prev *shardctrler.Config, curr *shardctrler.Config) {
	if prev != nil && prev.Num >= curr.Num {
		return
	}

	for shard, currGid := range curr.Shards {
		if currGid == kv.gid {
			if prev == nil || prev.Num == 0 {
				kv.mu.Lock()
				kv.shardTable[shard] = curr.Num
				kv.mu.Unlock()
			} else if prev.Shards[shard] == kv.gid { // non-migration shard
				go func() {
					for {
						kv.mu.Lock()
						currVersion := kv.shardTable[shard]
						if currVersion == prev.Num {
							kv.shardTable[shard] = curr.Num
							kv.mu.Unlock()
							return
						}
						kv.mu.Unlock()
					}
				}()
			}
		} else if prev != nil && prev.Shards[shard] == kv.gid { // ready or pending
			// migration to currGid
			kv.shardMigrationChan <- &MigrationArgs{
				Shard:       shard,
				DesGid:      currGid,
				PrevVersion: prev.Num,
				Version:     curr.Num,
			}
		}
	}
	prevShard := [10]int{}
	if prev != nil {
		prevShard = prev.Shards
	}
	DPrintf("[server %d gid %d] shard table updated: %+v, prev: %v, cur: %v", kv.me, kv.gid, kv.shardTable, prevShard, curr.Shards)
}

func (kv *ShardKV) shardMigrationProcessor() {
	for args := range kv.shardMigrationChan {
		kv.sendShardToGroup(args)
		if kv.isKilled() {
			return
		}
	}
}

// Send shard to another group via rpc
// note that for shard1 there might has been one migration seq: a -> b -> c -> b -> e
// if a -> b takes too long, we need a mechanism to recognize b->c or b->e when b is ready
// the sendShardToGroup RPCs shoud be lined up
func (kv *ShardKV) sendShardToGroup(args *MigrationArgs) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	for { // wait until shard is ready
		kv.mu.Lock()
		version := kv.shardTable[args.Shard]
		kv.mu.Unlock()

		if version == args.PrevVersion {
			// fetch shard data
			kv.mu.Lock()
			args.PayLoad = make(map[string]string)
			for key, val := range kv.ma[args.Shard] {
				args.PayLoad[key] = val
			}
			// destination
			servers := kv.config.Groups[args.DesGid]
			args.ConfigNum = kv.config.Num
			kv.mu.Unlock()
			for _, server := range servers {
				srv := kv.make_end(server)
				reply := new(MigrationReply)
				ok := srv.Call("ShardKV.MigrationShard", args, reply)
				DPrintf("[server %d gid %d] send shard: %+v", kv.me, kv.gid, args)
				if ok && reply.Err == OK {
					return
				}
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}

}

// RPC
// 1. if curr group is in lower config ?
func (kv *ShardKV) MigrationShard(args *MigrationArgs, reply *MigrationReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// update shard version first
	kv.mu.Lock()
	version := kv.shardTable[args.Shard]
	kv.mu.Unlock()
	if version >= args.Version { // shard already applied
		reply.Err = OK
		return
	}

	op := &Op{
		Args: args,
	}
	_, _, isLeader = kv.rf.Start(op)
	// not leader return error
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK

}

// Should update config consecutively
func (kv *ShardKV) updateShards(args *MigrationArgs) {
	kv.mu.Lock()
	currConfigNum := kv.config.Num
	version := kv.shardTable[args.Shard]
	kv.mu.Unlock()
	// check if config need to be updated
	if args.ConfigNum > currConfigNum {
		kv.reConfig(args.ConfigNum)
	}

	if version >= args.Version {
		return
	}

	// must wait until version == args.PreVersion
	go func() {
		for {
			kv.mu.Lock()
			currVer := kv.shardTable[args.Shard]
			if currVer == args.PrevVersion {
				// check shard version
				for key, val := range args.PayLoad {
					kv.ma[args.Shard][key] = val
				}
				kv.shardTable[args.Shard] = args.Version
				kv.mu.Unlock()
				DPrintf("[server %d gid %d] shard %d installed, version: %d", kv.me, kv.gid, args.Shard, args.Version)
				return
			}
			kv.mu.Unlock()
		}
	}()

}
