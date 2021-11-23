package shardkv

import (
	"reflect"
	"time"
)

type shardKVReq interface {
	GetKey() string
	GetSerialNum() int64
	GetClientID() int64
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

func (kv *ShardKV) Serve(req shardKVReq) (reply shardKvReply) {
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
		return

	}
	kv.mu.Unlock()

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
	sub := make(chan int, 1)
	kv.subscribe(req.GetClientID(), req.GetSerialNum(), sub)
	DPrintf("[server %d]subscribe for %s cmd %d, %+v, from [clerk %d]", kv.me, reflect.TypeOf(req), commandIndex, req, req.GetClientID())

	select {
	case _, ok := <-sub:
		if !ok { //chan closed
			reply.SetErr(ErrInternal)
			return
		}
		// if it's a get req, should return val
		if getReq, ok := req.(*GetArgs); ok {
			reply.SetValue(kv.get(getReq))
		}
		DPrintf("[server %d]cmd %d, notified from [clerk %d] serial number: %d", kv.me, commandIndex, req.GetClientID(), req.GetSerialNum())
	case <-time.After(ServerTimeOut):
		reply.SetErr(ErrInternal)
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

	// check key mapping
	// TODO get latest config

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
				return ErrInternal
			}
		}
	}
	kv.mu.Unlock()
	return ""
}
