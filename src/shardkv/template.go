package shardkv

import (
	"reflect"
	"time"
)

type shardKVReq interface {
	GetKey() string
	GetSerialNum() int64
	GetClientID() int64
	GetConfigNum() int
}

type shardKvReply interface {
	GetErr() Err
	GetValue() *string
	SetErr(Err)
	SetValue(*string)
}

type Op struct {
	SerialNumber int64
	ClientID     int64
	Args         interface{}
}

type GeneralReply struct {
	Err   Err
	Value *string
}

func (g *GeneralReply) GetErr() Err {
	return g.Err
}

func (g *GeneralReply) GetValue() *string {
	return g.Value
}

func (g *GeneralReply) SetErr(err Err) {
	g.Err = err
}

func (g *GeneralReply) SetValue(val *string) {
	g.Value = val
}

func stringVal(ptr *string) string {
	if ptr != nil {
		return *ptr
	}
	return "nil"
}

func (kv *ShardKV) Serve(req shardKVReq) (reply shardKvReply) {
	defer func() {
		kv.mu.Lock()
		if reply.GetErr() != ErrWrongLeader {
			configNum := 0
			if kv.config != nil {
				configNum = kv.config.Num
			}
			DPrintf("[server %d group %d] configNum: %d, shardTab: %v, targetConfig: %d req: %+v, Err: %v, Value: %v", kv.me, kv.gid, configNum, kv.shardTable, kv.targetConfig, req, reply.GetErr(), stringVal(reply.GetValue()))
		}
		kv.mu.Unlock()

	}()

	reply = new(GeneralReply)
	err := kv.checkProcessStatus(req.GetKey(), req.GetClientID(), req.GetSerialNum())
	if err != "" {
		reply.SetErr(err)
		return
	}

	// idempotent
	kv.mu.Lock()
	if kv.serialMap[req.GetClientID()] >= req.GetSerialNum() {
		kv.mu.Unlock()
		reply.SetErr(OK)
		return

	}
	kv.mu.Unlock()

	// check config and shard
	err = kv.checkConfig(req.GetConfigNum())
	if err != OK {
		reply.SetErr(err)
		return
	}

	err = Err(kv.checkShard(req.GetKey()))
	if err != OK {
		reply.SetErr(err)
		return
	}

	// replicate opt via Raft
	op := &Op{
		Args:         req,
		SerialNumber: req.GetSerialNum(),
		ClientID:     req.GetClientID(),
	}
	commandIndex, _, isLeader := kv.rf.Start(op)
	// not leader return error
	if !isLeader {
		reply.SetErr(ErrWrongLeader)
		return
	}

	// subscribe the opt and wait for applyChan
	sub := make(chan string, 1)
	kv.subscribe(req.GetClientID(), req.GetSerialNum(), sub)
	DPrintf("[server %d group %d]subscribe for %s cmd %d, %+v, from [clerk %d]",
		kv.me, kv.gid, reflect.TypeOf(req), commandIndex, req, req.GetClientID())

	select {
	case msg, ok := <-sub:
		if !ok { //chan closed
			reply.SetErr(ErrInternal)
			return
		}
		if msg == OK {
			// if it's a get req, should return val
			if getReq, ok := req.(*GetArgs); ok {
				reply.SetValue(kv.get(getReq))
			}
		}
		reply.SetErr(Err(msg))
		DPrintf("[server %d group %d]cmd %d, notified from [clerk %d] serial number: %d, msg: %s", kv.me, kv.gid, commandIndex, req.GetClientID(), req.GetSerialNum(), msg)
	case <-time.After(ServerTimeOut):
		DPrintf("[server %d group %d]timeout cmd %d, %+v, from [clerk %d]",
			kv.me, kv.gid, commandIndex, req, req.GetClientID())
		reply.SetErr(ErrTimeOut)
		return
	}
	return
}

func (kv *ShardKV) checkProcessStatus(key string, clientID, serialNumber int64) Err {
	// check leader
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		return ErrWrongLeader
	}

	kv.mu.Lock()
	// lower term let req pass
	if kv.term < int32(term) {
		kv.term = int32(term)
		// clear kv.sub
		if clientMap, ok := kv.subscriberMap[clientID]; ok {
			// lower term request in progress
			if respChan, ok := clientMap[serialNumber]; ok {
				close(respChan)
			}
		}
	} else {
		// check if the request is still in progress
		if sub, ok := kv.subscriberMap[clientID]; ok {
			if _, ok := sub[serialNumber]; ok {
				kv.mu.Unlock()
				return ErrInprogress
			}
		}
	}
	kv.mu.Unlock()
	return ""
}
