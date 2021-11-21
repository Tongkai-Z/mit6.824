package shardctrler

import (
	"reflect"
	"time"
)

type ShardCtrlerArgs interface {
	GetClientID() int64
	GetSerialNum() int64
}

type ShardCtrlerReply interface {
	GetWrongLeader() bool
	GetError() Err
	GetConfig() *Config
	SetWrongLeader(bool)
	SetError(Err)
	SetConfig(*Config)
}

type GeneralReply struct {
	WrongLeader bool
	Err         Err
	Config      *Config
}

func (g *GeneralReply) GetWrongLeader() bool {
	return g.WrongLeader
}

func (g *GeneralReply) GetError() Err {
	return g.Err
}

func (g *GeneralReply) GetConfig() *Config {
	return g.Config
}

func (g *GeneralReply) SetWrongLeader(isWrongLeader bool) {
	g.WrongLeader = isWrongLeader
}

func (g *GeneralReply) SetError(err Err) {
	g.Err = err
}

func (g *GeneralReply) SetConfig(config *Config) {
	g.Config = config
}

func (sc *ShardCtrler) Serve(args ShardCtrlerArgs) (reply ShardCtrlerReply) {
	defer func() {
		// only print the log from leader
		if !reply.GetWrongLeader() {
			DPrintf("[server %d] req: %s:%+v from [clerk %d]", sc.me, reflect.TypeOf(args), args, args.GetClientID())
			DPrintf("[server %d] resp: %+v from [clerk %d]", sc.me, reply, args.GetClientID())
		}
	}()
	reply = new(GeneralReply)
	// check leader
	var term int
	var leader bool
	if term, leader = sc.rf.GetState(); !leader {
		reply.SetWrongLeader(true)
		return
	}
	reply.SetWrongLeader(false)
	err := sc.checkProcessStatus(args.GetClientID(), args.GetSerialNum(), term)
	if err != "" {
		reply.SetError(err)
		return
	}

	// idempotent
	sc.mu.Lock()
	if sc.clientSerialNum[args.GetClientID()] >= args.GetSerialNum() {
		// duplicate req, reply normally
		DPrintf("[server %d] duplicate call %d from [clerk %d]", sc.me, args.GetSerialNum(), args.GetClientID())
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	// send the opt to other server via Raft
	op := &Op{
		Args:      args,
		ClientID:  args.GetClientID(),
		SerialNum: args.GetSerialNum(),
	}
	commandIndex, _, isLeader := sc.rf.Start(op)
	// not leader return error
	if !isLeader {
		reply.SetWrongLeader(true)
		return
	}

	// subscribe the operation and wait for applyCh
	sub := make(chan int, 1)
	sc.subscribe(args.GetClientID(), args.GetSerialNum(), sub)

	DPrintf("[server %d]subscribe for args %s cmd %d from [clerk %d] serial number: %d",
		sc.me,
		reflect.TypeOf(args),
		commandIndex,
		args.GetClientID(),
		args.GetSerialNum())

	select {
	case _, ok := <-sub:
		if !ok { //chan closed
			reply.SetError(Err("internal error"))
			return
		}
		DPrintf("[server %d]cmd %d, notified from [clerk %d] serial number: %d", sc.me, commandIndex, args.GetClientID(), args.GetSerialNum())
		// query req add config to reply
		if queryArgs, ok := args.(*QueryArgs); ok {
			reply.SetConfig(sc.query(queryArgs))
		}
		return
	case <-time.After(ServerTimeOut):
		reply.SetError(Err("raft time out"))
		return
	}
}
