package shardkv

import (
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	Debug = true

	ServerTimeOut      = 1 * time.Second
	PollConfigInterval = 100 * time.Millisecond
	OK                 = "OK"
	ApplySuccess       = "Success"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrInternal        = "ErrInternal"
	ShardReady         = 0
	ShardPending       = 1
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key          string
	Value        string
	Op           string // "Put" or "Append"
	SerialNumber int64
	ClientID     int64
}

func (p *PutAppendArgs) GetKey() string {
	return p.Key
}

func (p *PutAppendArgs) GetSerialNum() int64 {
	return p.SerialNumber
}

func (p *PutAppendArgs) GetClientID() int64 {
	return p.ClientID
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key          string
	SerialNumber int64
	ClientID     int64
}

func (p *GetArgs) GetKey() string {
	return p.Key
}

func (p *GetArgs) GetSerialNum() int64 {
	return p.SerialNumber
}

func (p *GetArgs) GetClientID() int64 {
	return p.ClientID
}

type GetReply struct {
	Err   Err
	Value string
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type UpdateConfigArgs struct {
	ConfigNum int
}

type MigrationArgs struct {
	Shard       int
	DesGid      int
	PrevVersion int
	Version     int
	ConfigNum   int
	PayLoad     map[string]string
}

type MigrationReply struct {
	Err Err
}
