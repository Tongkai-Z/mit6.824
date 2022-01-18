package shardctrler

import (
	"log"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK            = "OK"
	Debug         = false
	ServerTimeOut = 1 * time.Second
)

type Err string
type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	SerialNum int64
	ClientID  int64
}

func (a *JoinArgs) GetClientID() int64 {
	return a.ClientID
}

func (a *JoinArgs) GetSerialNum() int64 {
	return a.SerialNum
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	SerialNum int64
	ClientID  int64
}

func (a *LeaveArgs) GetClientID() int64 {
	return a.ClientID
}

func (a *LeaveArgs) GetSerialNum() int64 {
	return a.SerialNum
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	SerialNum int64
	ClientID  int64
}

func (a *MoveArgs) GetClientID() int64 {
	return a.ClientID
}

func (a *MoveArgs) GetSerialNum() int64 {
	return a.SerialNum
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	SerialNum int64
	ClientID  int64
}

func (a *QueryArgs) GetClientID() int64 {
	return a.ClientID
}

func (a *QueryArgs) GetSerialNum() int64 {
	return a.SerialNum
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

func min(i, j int) int {
	if i > j {
		return j
	}
	return i
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
