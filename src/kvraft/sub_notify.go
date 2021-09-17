package kvraft

func (kv *KVServer) subscribe(clientID, serialNumber int64, subChan chan int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.subscriberMap[clientID] == nil {
		kv.subscriberMap[clientID] = make(map[int64]chan int)
	}
	kv.subscriberMap[clientID][serialNumber] = subChan
}

// consume the apply msg and if this node is leader publish the msg to subscriber accordingly
// inorder to guarantee serialization we must apply first and then publish
func (kv *KVServer) applyObserver() {
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

func (kv *KVServer) applyCommand(op *Op, cmdIdx int) {
	kv.mu.Lock()
	defer func() {
		if kv.maxAppliedCmd < int64(cmdIdx) {
			kv.maxAppliedCmd = int64(cmdIdx)
		}
		kv.mu.Unlock()
		go kv.condSnapshot()
	}()

	if op.OpName == GetOp {
		args := op.Args.(*GetArgs)
		DPrintf("[server %d]get cmd %d serial number %d from client %d applied key: %s, val: %s",
			kv.me, cmdIdx, args.SerialNumber, args.ClientID, args.Key, kv.ma[args.Key])
	} else {
		args := op.Args.(*PutAppendArgs)
		if kv.serialMap[args.ClientID] >= args.SerialNumber {
			//already applied
			DPrintf("[server %d]putAppend cmd %d serial number %d from client %d duplicate call, key: %s, val: %s",
				kv.me, cmdIdx, args.SerialNumber, args.ClientID, args.Key, kv.ma[args.Key])
			return
		}
		// update serial map
		kv.serialMap[args.ClientID] = args.SerialNumber
		putOrAppend := args.Op
		if putOrAppend == "Append" {
			kv.ma[args.Key] = kv.ma[args.Key] + args.Value
		} else {
			kv.ma[args.Key] = args.Value
		}
		DPrintf("[server %d]putAppend cmd %d serial number %d from client %d applied, key: %s, val: %s",
			kv.me, cmdIdx, args.SerialNumber, args.ClientID, args.Key, kv.ma[args.Key])
	}
}

func (kv *KVServer) publishCommand(clientID, serialNumber int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if sub, ok := kv.subscriberMap[clientID]; ok {
		if ch, ok := sub[serialNumber]; ok {
			ch <- 1
			delete(sub, serialNumber)
		}
	}
}
