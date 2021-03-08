package kvraft

import (
	"../labrpc"
	logger "github.com/rexlien/go-utils/xln-utils/logger"
	"go.uber.org/zap"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

var nextClientID int64 = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct

	id int64
	nextReqID int32

	logger *zap.SugaredLogger
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

	ck.logger = logger.CreateLogContext().GetSugarLogger() //rf.contextLogger
	ck.id = atomic.AddInt64(&nextClientID, 1)

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

	// You will have to modify this function.

	args := GetArgs{Key: key, }
	reply := GetReply{}

	ck.logger.Debugf("GET key staring: %s", args.Key)

	for {
		for _, server := range ck.servers {

			ok := server.Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == OK {
					ck.logger.Debugf("Got Key %s: Value: %s", key, reply.Value)
					return reply.Value
				} else if reply.Err == ErrNoKey {
					ck.logger.Debugf("No Key")
					return ""
				} else if reply.Err == ErrWrongLeader {
					//fmt.Println("Wrong leader")
				}
			} else {
				ck.logger.Debugf("Get RPC failed")
			}
		}

	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	reqID := atomic.AddInt32(&ck.nextReqID, 1)

	args := PutAppendArgs{Key: key, Value: value, Op: op, RequestID: reqID, ClientID: ck.id}
	reply := PutAppendReply{}


	for {
		for index, server := range ck.servers {

			ck.logger.Debugf("Put Append calls start: server %d, key %s, value %s", index, args.Key, args.Value)
			ok := server.Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == OK {
					ck.logger.Debugf("Put Append End Successfully : %d", index)
					return
				} else {
					ck.logger.Debugf("Put Append Failed : Wrong leader %d", index)
				}
			} else {
				ck.logger.Debugf("PutAppend RPC failed: %d", index)
			}
		}

	}


}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
