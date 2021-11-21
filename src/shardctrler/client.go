package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

const (
	InitialSerialNumber = 1
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	id        int64
	serialNum int64
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
	ck.id = nrand()
	ck.serialNum = InitialSerialNumber
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:       num,
		SerialNum: atomic.LoadInt64(&ck.serialNum),
		ClientID:  ck.id,
	}
	atomic.AddInt64(&ck.serialNum, 1)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:   servers,
		SerialNum: atomic.LoadInt64(&ck.serialNum),
		ClientID:  ck.id,
	}
	atomic.AddInt64(&ck.serialNum, 1)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:      gids,
		SerialNum: atomic.LoadInt64(&ck.serialNum),
		ClientID:  ck.id,
	}
	atomic.AddInt64(&ck.serialNum, 1)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		SerialNum: atomic.LoadInt64(&ck.serialNum),
		ClientID:  ck.id,
	}
	atomic.AddInt64(&ck.serialNum, 1)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
