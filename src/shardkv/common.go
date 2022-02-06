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
	Debug              = true
	LeaderLog          = false
	ServerTimeOut      = 1 * time.Second
	PollConfigInterval = 100 * time.Millisecond
	OK                 = "OK"
	ApplySuccess       = "Success"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrConfigNotMatch  = "ErrConfigNotMatch"
	ErrInternal        = "ErrInternal"
	ErrKeyNotReady     = "ErrKeyNotReady"
	ErrTimeOut         = "ErrTimeOut"
	ErrInprogress      = "ErrInprogress"
	ShardReady         = 0
	ShardNeedToBeSent  = 1
	ShardPending       = 2
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
	ConfigNum    int
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

func (p *PutAppendArgs) GetConfigNum() int {
	return p.ConfigNum
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key          string
	SerialNumber int64
	ClientID     int64
	ConfigNum    int
}

func (g *GetArgs) GetKey() string {
	return g.Key
}

func (g *GetArgs) GetSerialNum() int64 {
	return g.SerialNumber
}

func (g *GetArgs) GetClientID() int64 {
	return g.ClientID
}

func (g *GetArgs) GetConfigNum() int {
	return g.ConfigNum
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
	Shard        int
	DesGid       int
	ConfigNum    int
	PayLoad      map[string]string
	SerialNumber int64
	ClientID     int64
}

type MigrationReply struct {
	Err Err
}

type AlterShardStatus struct {
	Shard  int
	Status int
}

func (p *MigrationArgs) GetKey() string {
	return ""
}

func (p *MigrationArgs) GetSerialNum() int64 {
	return p.SerialNumber
}

func (p *MigrationArgs) GetClientID() int64 {
	return p.ClientID
}

func (p *MigrationArgs) GetConfigNum() int {
	return p.ConfigNum
}
