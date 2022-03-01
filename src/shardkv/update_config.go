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
				kv.mu.Unlock()
				kv.reConfig(config.Num)
			} else {
				kv.mu.Unlock()
			}

		}
	}
}

// check if the group is qualified to serve the key
func (kv *ShardKV) checkShard(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if key == "" {
		return OK
	}
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
			DPrintf("[server %d group %d] check shard key: %s, shard: %d status: %d", kv.me, kv.gid, key, shard, kv.shardTable[shard])
		}
		return ErrKeyNotReady // let client retry
	}

	return OK
}

// FIXME: change leader synchronize config updateChan
func (kv *ShardKV) updateConfigProcessor() {
	for !kv.isKilled() {
		if kv.checkShardStatus() {
			kv.mu.Lock()
			num := 0
			if kv.config != nil {
				num = kv.config.Num
			}
			if num < kv.targetConfig {
				kv.mu.Unlock()
				kv.replicateUpdateConfig(num + 1) // synchronized update
			} else {
				kv.mu.Unlock()
			}

		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) checkShardStatus() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config == nil {
		return true
	}
	for idx, status := range kv.shardTable {
		if status != ShardReady {
			if status == ShardNeedToBeSent {
				go kv.sendShardToGroup(idx, kv.config.Shards[idx])
			}
			return false
		}
	}
	return true
}

func (kv *ShardKV) updateConfig(configNum int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := kv.sc.Query(configNum)
	if kv.config != nil && kv.config.Num >= config.Num {
		return
	}
	// update config
	prev := kv.config
	// update config
	kv.config = &config
	if _, isLeader := kv.rf.GetState(); isLeader || !LeaderLog {
		DPrintf("[server %d group %d] config num: %d prev: %v config updated: %+v", kv.me, kv.gid, kv.config.Num, prev, kv.config.Shards)
	}
	// update shard table based on the diff between prev and curr
	kv.updateShardTable(prev, &config)
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
				kv.shardTable[shard] = ShardPending // wait to be installed
			}
		} else if prev.Shards[shard] == kv.gid {
			kv.shardTable[shard] = ShardNeedToBeSent //1
			// send to des
			go kv.sendShardToGroup(shard, curr.Shards[shard])
		}
	}
	if _, isLeader := kv.rf.GetState(); isLeader || !LeaderLog {
		DPrintf("[server %d group %d] config num: %d shard table updated: %+v", kv.me, kv.gid, kv.config.Num, kv.shardTable)
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
			DPrintf("[server %d group %d] send shard: %+v", kv.me, kv.gid, args)
			// success
			op := &Op{
				Args: &AlterShardStatus{
					Shard:     shard,
					Status:    ShardReady,
					SerialNum: args.SerialNumber,
				},
			}
			// FIXME: leaderShip change make state inconsistent
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

// FIXME: how to dedup
func (kv *ShardKV) updateShardstatus(args *AlterShardStatus) {
	kv.mu.Lock()
	if kv.shardTable[args.Shard] == ShardNeedToBeSent {
		kv.shardTable[args.Shard] = int32(args.Status)
		// Delete shard
		kv.ma[args.Shard] = make(map[string]string)
		DPrintf("[server %d group %d] shardTab updated:%+v,  %+v serial: %d", kv.me, kv.gid, args, kv.shardTable, args.SerialNum)
	}

	kv.mu.Unlock()

}

// Should update config consecutively
func (kv *ShardKV) updateShards(args *MigrationArgs) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if kv.serialMap[args.ClientID] >= args.SerialNumber {
	// 	//already applied
	// 	DPrintf("[server %d group %d] migration serial number %d from client %d duplicate call",
	// 		kv.me, kv.gid, args.SerialNumber, args.ClientID)
	// 	return OK
	// }
	// // update serial map
	// kv.serialMap[args.ClientID] = args.SerialNumber

	if kv.shardTable[args.Shard] == ShardReady {
		DPrintf("[server %d group %d] config num %d migration serial number %d from client %d duplicate call, shardTable %v",
			kv.me, kv.gid, kv.config.Num, args.SerialNumber, args.ClientID, kv.shardTable)
		return OK
	}
	// check shard version
	for key, val := range args.PayLoad {
		kv.ma[args.Shard][key] = val
	}
	kv.shardTable[args.Shard] = ShardReady

	if _, isLeader := kv.rf.GetState(); isLeader || !LeaderLog {
		DPrintf("[server %d group %d] config %d shard %d installed from client %d serial number %d, shardTable %v", kv.me, kv.gid, kv.config.Num, args.Shard, args.ClientID, args.SerialNumber, kv.shardTable)
	}
	return OK
}

// note that we should return gracefully if kv.config.Num > configNum
func (kv *ShardKV) checkConfig(configNum int) Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config == nil || kv.config.Num < configNum {
		if kv.targetConfig < configNum {
			kv.reConfig(configNum)
		}
		return ErrConfigNotMatch
	}
	// if kv.config.Num > configNum {
	// 	return ErrConfigNotMatch
	// }
	return OK
}

// Process reconfiguration one at a time in order
// reconfig should not skip
// FIXME: must update config synchronized
// e.g. leader: config update -> put , follower config update -> put discard
func (kv *ShardKV) reConfig(num int) {
	kv.targetConfig = num
}

func (kv *ShardKV) replicateUpdateConfig(configNum int) {
	// config change
	configArgs := &UpdateConfigArgs{
		ConfigNum: configNum,
	}
	commandIndex, _, isLeader := kv.rf.Start(&Op{
		Args: configArgs,
	})
	if isLeader {
		kv.mu.Lock()
		configNum := 0
		if kv.config != nil {
			configNum = kv.config.Num
		}
		DPrintf("[server %d group %d] current config %d shardTable %v replicate config cmd %d, %+v", kv.me, kv.gid, configNum, kv.shardTable, commandIndex, configArgs)
		kv.mu.Unlock()
	}
}
