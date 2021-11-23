package shardkv

func (kv *ShardKV) subscribe(clientID, serialNumber int64, subChan chan int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.subscriberMap[clientID] == nil {
		kv.subscriberMap[clientID] = make(map[int64]chan int)
	}
	kv.subscriberMap[clientID][serialNumber] = subChan
}

// consume the apply msg and if this node is leader publish the msg to subscriber accordingly
// inorder to guarantee serialization we must apply first and then publish
func (kv *ShardKV) applyObserver() {
	for cmd := range kv.applyCh {
		// apply this cmd
		if cmd.CommandValid {
			kv.applyCommand(cmd.Command.(*Op), cmd.CommandIndex)
			// leader node pub
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.publishCommand(cmd.Command.(*Op).ClientID, cmd.Command.(*Op).SerialNumber)
			}
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
	case *PutAppendArgs:
		kv.putAppend(args)
	}
}

func (kv *ShardKV) publishCommand(clientID, serialNumber int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if sub, ok := kv.subscriberMap[clientID]; ok {
		if ch, ok := sub[serialNumber]; ok {
			ch <- 1
			delete(sub, serialNumber)
		}
	}
}
