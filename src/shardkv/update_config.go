package shardkv

import "6.824/shardctrler"

func (kv *ShardKV) checkShard(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config == nil {
		return ErrWrongGroup
	}
	shard := key2shard(key)
	if kv.gid != kv.config.Shards[shard] {
		return ErrWrongGroup
	}

	// check shard migration status
	if !kv.shardTable[shard] {
		return ErrInternal
	}

	return OK

}

func (kv *ShardKV) updateConfig(args *UpdateConfigArgs) {
	currConfig := kv.sc.Query(args.ConfigNum)
	kv.mu.Lock()
	prevConfig := kv.config
	if prevConfig == nil || prevConfig.Num < currConfig.Num {
		DPrintf("[server %d] config updated, prev %d, curr %d", kv.me, kv.config.Num, currConfig.Num)
		kv.config = &currConfig
		kv.updateShardTable(prevConfig, &currConfig)
	}
	kv.mu.Unlock()

}

func (kv *ShardKV) updateShardTable(prev *shardctrler.Config, curr *shardctrler.Config) {
	for shard, currGid := range curr.Shards {
		if currGid == kv.gid {
			if prev == nil || prev.Shards[shard] == kv.gid { // same shard
				kv.shardTable[shard] = true
			} else { // wait for migration
				kv.shardTable[shard] = false
			}
		} else if kv.shardTable[shard] {
			// migration to currGid
			if _, isLeader := kv.rf.GetState(); isLeader {

			}
			kv.shardTable[shard] = false
		}
	}
}
