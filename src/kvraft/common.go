package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	Debug          = false
	ServerTimeOut  = 1 * time.Second
	GetOp          = "getOperation"
	PutAppendOp    = "putAppendOperation"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key          string
	Value        string
	Op           string // "Put" or "Append"
	SerialNumber int64
	ClientID     int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key          string
	SerialNumber int64
	ClientID     int64
}

type GetReply struct {
	Err   Err
	Value string
}
