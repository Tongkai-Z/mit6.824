package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm           *shardctrler.Clerk
	config       shardctrler.Config
	make_end     func(string) *labrpc.ClientEnd
	clientID     int64
	serialNumber int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientID = nrand()
	ck.serialNumber = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientID = ck.clientID
	args.SerialNumber = atomic.LoadInt64(&ck.serialNumber)
	atomic.AddInt64(&ck.serialNumber, 1)
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ConfigNum = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; ; si++ {
				srv := ck.make_end(servers[si%len(servers)])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrConfigNotMatch) {
					break
				}
				if ok && reply.Err == ErrKeyNotReady {
					time.Sleep(100 * time.Millisecond)
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientID = ck.clientID
	args.SerialNumber = atomic.LoadInt64(&ck.serialNumber)
	atomic.AddInt64(&ck.serialNumber, 1)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ConfigNum = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			// FIXME: timeout may exit loop and cause client to query new config
			for si := 0; ; si++ {
				srv := ck.make_end(servers[si%len(servers)])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrConfigNotMatch) {
					break
				}

				if ok && reply.Err == ErrKeyNotReady {
					time.Sleep(100 * time.Millisecond)
				}
				// not wrong
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
