package shardctrler

func (sc *ShardCtrler) subscribe(clientID, serialNumber int64, subChan chan int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.subscriberMap[clientID] == nil {
		sc.subscriberMap[clientID] = make(map[int64]chan int)
	}
	sc.subscriberMap[clientID][serialNumber] = subChan
}

func (sc *ShardCtrler) applyObserver() {
	for cmd := range sc.applyCh {
		if cmd.CommandValid {
			op := cmd.Command.(*Op)
			sc.applyCommand(op)
			if _, isLeader := sc.rf.GetState(); isLeader {
				sc.publishCommand(op.ClientID, op.SerialNum)
			}
		}
	}
}

func (sc *ShardCtrler) publishCommand(clientID, serialNum int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sub, ok := sc.subscriberMap[clientID]; ok {
		if ch, ok := sub[serialNum]; ok {
			ch <- 1
			delete(sub, serialNum)
		}
	}
}

func (sc *ShardCtrler) applyCommand(op *Op) {
	switch arg := op.Args.(type) {
	case *JoinArgs:
		sc.join(arg)
	case *LeaveArgs:
		sc.leave(arg)
	case *MoveArgs:
		sc.move(arg)
	case *QueryArgs:
		// do nothing
	}
	// update the serial map
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.clientSerialNum[op.ClientID] = op.SerialNum
}
