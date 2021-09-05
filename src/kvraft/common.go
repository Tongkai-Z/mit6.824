package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key          string
	Value        string
	Op           string // "Put" or "Append"
	SerialNumber int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key          string
	SerialNumber int64
}

type GetReply struct {
	Err   Err
	Value string
}
