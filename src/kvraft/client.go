package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

const (
	InitialSerialNumber = 1
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prefer       int // prefered servered set by last successful call
	mu           sync.Mutex
	serialNumber int64
	clientID     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.serialNumber = InitialSerialNumber
	ck.clientID = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key:          key,
		SerialNumber: atomic.LoadInt64(&ck.serialNumber),
		ClientID:     ck.clientID,
	}
	// increase serialnumber
	atomic.AddInt64(&ck.serialNumber, 1)
	// set the timer
	const timeout = 1 * time.Second
	t := time.NewTimer(timeout)
	defer t.Stop()

	//done chan
	done := make(chan *GetReply, len(ck.servers))
	switchServer := make(chan *GetReply, len(ck.servers))
	var r *GetReply

	offset := 0
	ck.mu.Lock()
	prefer := ck.prefer
	ck.mu.Unlock()
	// retry another server indefinitely
	for {
		cur := (prefer + offset) % len(ck.servers)
		s := ck.servers[cur]
		go func() {
			cur := cur
			reply := new(GetReply)
			DPrintf("[clerk %d] called get opt to server %d, key %s", ck.clientID, cur, key)
			// sync
			success := s.Call("KVServer.Get", args, reply)
			if success {
				if reply.Err == "" {
					//update prefer
					prefer = cur
					DPrintf("[clerk %d] get operation serial number %d by server %d finished, key: %s, val: %s", ck.clientID, args.SerialNumber, cur, args.Key, reply.Value)
					done <- reply // success
				} else if reply.Err == "server not leader" {
					switchServer <- reply
				}
			}
		}()
		select {
		case r = <-done:
			goto Done
		case <-t.C:
			// timeout
			offset++
			DPrintf("[clerk %d] get operation by server %d time out", ck.clientID, cur)
			t.Reset(timeout)
		case <-switchServer:
			DPrintf("[clerk %d] get not leader err by server %d", ck.clientID, cur)
			offset++
			t.Reset(timeout)
		}
	}

Done:
	ck.mu.Lock()
	ck.prefer = prefer
	ck.mu.Unlock()
	return r.Value
}

//
// shared by Put and Append.
// iterate the servers until it get the correct response
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:          key,
		Value:        value,
		Op:           op,
		SerialNumber: atomic.LoadInt64(&ck.serialNumber),
		ClientID:     ck.clientID,
	}
	atomic.AddInt64(&ck.serialNumber, 1)
	// set the timer
	const timeout = 1 * time.Second
	t := time.NewTimer(timeout)
	defer t.Stop()

	//done chan
	done := make(chan *PutAppendReply, len(ck.servers))
	switchServer := make(chan *PutAppendReply, len(ck.servers))

	ck.mu.Lock()
	prefer := ck.prefer
	ck.mu.Unlock()
	offset := 0
	// retry another server indefinitely
	for {
		cur := (prefer + offset) % len(ck.servers)
		s := ck.servers[cur]
		DPrintf("[clerk %d] called put operation to server %d, key %s, val %s", ck.clientID, cur, key, value)
		go func() {
			// sync
			reply := new(PutAppendReply)
			cur := cur
			success := s.Call("KVServer.PutAppend", args, reply)
			if success {
				if reply.Err == "" {
					prefer = cur
					DPrintf("[clerk %d] put operation serial number %d by server %d finished", ck.clientID, args.SerialNumber, cur)
					done <- reply // success
				} else if reply.Err == "server not leader" {
					switchServer <- reply
				}
			}
		}()
		select {
		case <-done:
			goto Done
		case <-t.C:
			// timeout
			offset++
			DPrintf("[clerk %d] put operation by server %d time out", ck.clientID, cur)
			t.Reset(timeout)
		case <-switchServer:
			offset++
			DPrintf("[clerk %d] put not leader err by server %d", ck.clientID, cur)
			t.Reset(timeout)
		}
	}
Done:
	ck.mu.Lock()
	ck.prefer = prefer
	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
