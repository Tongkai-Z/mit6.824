package shardkv

import (
	"sync/atomic"
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
	if kv.shardTable[shard] != ShardReady {
		if _, isLeader := kv.rf.GetState(); isLeader || !LeaderLog {
			DPrintf("[server %d gid %d] check shard key: %s, shard: %d status: %d", kv.me, kv.gid, key, shard, kv.shardTable[shard])
		}
		return ErrKeyNotReady // let client retry
	}

	return OK
}

// Process reconfiguration one at a time in order
// reconfig should not skip
func (kv *ShardKV) reConfig(num int) {
	kv.mu.Lock()
	prevConfig := kv.config
	kv.mu.Unlock()
	start := 0
	if prevConfig != nil {
		start = prevConfig.Num
	}
	for start < num {
		start++
		curr := kv.sc.Query(start)
		kv.configUpdateChan <- &curr
	}
}

// FIXME: change leader synchronize config updateChan
func (kv *ShardKV) updateConfigProcessor() {
	for !kv.isKilled() {
		if kv.checkShardStatus() {
			config := <-kv.configUpdateChan
			kv.updateConfig(config) // synchronized update
		} else {
			time.Sleep(50 * time.Millisecond)
		}

	}
}

func (kv *ShardKV) checkShardStatus() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, status := range kv.shardTable {
		if status != ShardReady {
			return false
		}
	}
	return true
}

func (kv *ShardKV) updateConfig(config *shardctrler.Config) {
	kv.mu.Lock()
	if kv.config != nil && kv.config.Num >= config.Num {
		kv.mu.Unlock()
		return
	}
	// update config
	prev := kv.config
	// update config
	kv.config = config
	if _, isLeader := kv.rf.GetState(); isLeader || !LeaderLog {
		DPrintf("[server %d gid %d] config num: %d config updated: %+v", kv.me, kv.gid, kv.config.Num, kv.config.Shards)
	}
	kv.mu.Unlock()
	// update shard table based on the diff between prev and curr
	kv.updateShardTable(prev, config)
}

func (kv *ShardKV) updateShardTable(prev *shardctrler.Config, curr *shardctrler.Config) {
	if prev == nil || prev.Num == 0 {
		return
	}
	for shard, currGid := range curr.Shards {
		shard := shard
		currGid := currGid
		if currGid == kv.gid {
			if prev.Shards[shard] != kv.gid {
				kv.mu.Lock()
				kv.shardTable[shard] = ShardPending // wait to be installed
				kv.mu.Unlock()
			}
		} else if prev.Shards[shard] == kv.gid {
			kv.mu.Lock()
			kv.shardTable[shard] = ShardNeedToBeSent //1
			kv.mu.Unlock()
			// send to des
			go kv.sendShardToGroup(shard, curr.Shards[shard])
		}
	}
	if _, isLeader := kv.rf.GetState(); isLeader || !LeaderLog {
		DPrintf("[server %d gid %d] config num: %d shard table updated: %+v", kv.me, kv.gid, kv.config.Num, kv.shardTable)
	}
}

// Send shard to another group via rpc
// note that for shard1 there might has been one migration seq: a -> b -> c -> b -> e
// if a -> b takes too long, we need a mechanism to recognize b->c or b->e when b is ready
// the sendShardToGroup RPCs shoud be lined up
// FIXME: sender fault tolerance
// push style: when restart, check push status
func (kv *ShardKV) sendShardToGroup(shard int, gid int) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	args := new(MigrationArgs)
	// fetch shard data
	kv.mu.Lock()
	args.Shard = shard
	args.DesGid = gid
	args.ClientID = int64(kv.gid)
	args.SerialNumber = atomic.LoadInt64(&kv.serialNumber)
	atomic.AddInt64(&kv.serialNumber, 1)
	args.PayLoad = make(map[string]string)
	for key, val := range kv.ma[shard] {
		args.PayLoad[key] = val
	}
	// destination
	servers := kv.config.Groups[gid]
	args.ConfigNum = kv.config.Num
	kv.mu.Unlock()
	curr := 0
	for { // retry
		server := servers[curr%len(servers)]
		srv := kv.make_end(server)
		reply := new(MigrationReply)
		ok := srv.Call("ShardKV.MigrationShard", args, reply)
		if ok && reply.Err == OK {
			DPrintf("[server %d gid %d] send shard: %+v", kv.me, kv.gid, args)
			// success
			op := &Op{
				Args: &AlterShardStatus{
					Shard:  shard,
					Status: ShardReady,
				},
			}
			kv.rf.Start(op)
			return
		} else {
			curr++
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// RPC
// 1. if curr group is in lower config ?
func (kv *ShardKV) MigrationShard(args *MigrationArgs, reply *MigrationReply) {
	shardKvReply := kv.Serve(args)
	reply.Err = shardKvReply.GetErr()
}

func (kv *ShardKV) updateShardstatus(args *AlterShardStatus) {
	kv.mu.Lock()
	if kv.shardTable[args.Shard] != int32(args.Status) {
		kv.shardTable[args.Shard] = int32(args.Status)
	}
	kv.mu.Unlock()

}

// Should update config consecutively
func (kv *ShardKV) updateShards(args *MigrationArgs) string {
	kv.mu.Lock()
	currConfigNum := kv.config.Num
	status := kv.shardTable[args.Shard]
	kv.mu.Unlock()

	if args.ConfigNum < currConfigNum {
		return OK
	}

	// check if config need to be updated
	if args.ConfigNum > currConfigNum {
		// config change
		configArgs := &UpdateConfigArgs{
			ConfigNum: args.ConfigNum,
		}
		commandIndex, _, isLeader := kv.rf.Start(&Op{
			Args: configArgs,
		})
		if isLeader {
			DPrintf("[server %d gid %d] replicate config cmd %d, %+v", kv.me, kv.gid, commandIndex, configArgs)
		}
		return ErrConfigNotMatch
	}

	if status == ShardReady {
		return OK
	}

	kv.mu.Lock()
	// check shard version
	for key, val := range args.PayLoad {
		kv.ma[args.Shard][key] = val
	}
	kv.shardTable[args.Shard] = ShardReady
	kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); isLeader || !LeaderLog {
		DPrintf("[server %d gid %d] shard %d installed", kv.me, kv.gid, args.Shard)
	}
	return OK
}
