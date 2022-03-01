package shardkv

func (kv *ShardKV) subscribe(clientID, serialNumber int64, subChan chan string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.subscriberMap[clientID] == nil {
		kv.subscriberMap[clientID] = make(map[int64]chan string)
	}
	kv.subscriberMap[clientID][serialNumber] = subChan
}

// consume the apply msg and if this node is leader publish the msg to subscriber accordingly
// inorder to guarantee serialization we must apply first and then publish
func (kv *ShardKV) applyObserver() {
	for cmd := range kv.applyCh {
		if kv.isKilled() {
			return
		}
		// apply this cmd
		if cmd.CommandValid {
			kv.applyCommand(cmd.Command.(*Op), cmd.CommandIndex)
		} else if cmd.SnapshotValid {
			kv.applySnapshot(cmd.SnapshotTerm, cmd.SnapshotIndex, cmd.Snapshot)
		}
	}
}

func (kv *ShardKV) applyCommand(op *Op, cmdIdx int) {
	defer func() {
		kv.mu.Lock()
		if kv.maxAppliedCmd < int64(cmdIdx) {
			kv.maxAppliedCmd = int64(cmdIdx)
		}
		kv.mu.Unlock()
		go kv.condSnapshot()
	}()

	switch args := op.Args.(type) {
	case *GetArgs:
		// do nothing
		// leader node pub
		msg := kv.checkConfigAndShard(args.ConfigNum, args.Key)
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.publishCommand(op.ClientID, op.SerialNumber, string(msg))
		}
	case *PutAppendArgs:
		// check config and shard

		msg := kv.checkConfigAndShard(args.ConfigNum, args.Key)

		if msg == OK {
			kv.putAppend(args)
		}
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.publishCommand(op.ClientID, op.SerialNumber, string(msg))
		}
	case *UpdateConfigArgs:
		kv.updateConfig(args.ConfigNum)
	case *MigrationArgs:
		msg := kv.updateShards(args)
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.publishCommand(op.ClientID, op.SerialNumber, msg)
		}
	case *AlterShardStatus:
		kv.updateShardstatus(args)
	}
}

func (kv *ShardKV) publishCommand(clientID, serialNumber int64, msg string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if sub, ok := kv.subscriberMap[clientID]; ok {
		if ch, ok := sub[serialNumber]; ok {
			ch <- msg
			delete(sub, serialNumber)
		}
	}
}

func (kv *ShardKV) checkConfigAndShard(configNum int, key string) Err {
	configErr := kv.checkConfig(configNum)
	if configErr != OK {
		return configErr
	}
	shardErr := kv.checkShard(key)
	if shardErr != OK {
		return Err(shardErr)
	}
	return OK
}
