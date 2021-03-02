package kvraft

import (
	"../labgob"
	"../labrpc"
	"fmt"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpType int
const GET OpType = 0
const PUT OpType = 1
const APPEND OpType = 2

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType OpType
	Key string
	Value string//interface{}

	ResultChan chan *OpResult

}

type OpResult struct {

	opType OpType
	result interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvState map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Println("GET starts" + args.Key)
	resultChan := make(chan *OpResult)
	op := Op{ OpType: GET, Key: args.Key, ResultChan: resultChan }
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader

	} else {

		select {
		case result := <-resultChan:
			fmt.Println("GET Result channel")
			if result.result == nil {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = result.result.(string)
			}

		}
	}



}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Println("Put Append starts")
	resultChan := make(chan *OpResult)

	var opType OpType
	if args.Op == "Put" {
		opType = PUT
	} else {
		opType = APPEND
	}

	op := Op{ OpType: opType, Key: args.Key, Value: args.Value, ResultChan: resultChan }
	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {

		select {
		case _ = <-resultChan:
			reply.Err = OK
		}

	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.kvState = make(map[string]string)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			select {
			case applyMsg := <- kv.applyCh:

				fmt.Printf("apply start: %d\n", kv.me)
				op := applyMsg.Command.(Op)
				var result interface{} = nil
				if op.OpType == GET {
					result = kv.kvState[op.Key]
				} else if op.OpType == PUT {
					kv.kvState[op.Key] = op.Value
					result = kv.kvState[op.Key]
				} else {
					//append(kv.kvState[op.Key].([]string), op.Value)
					kv.kvState[op.Key] += op.Value
					result = kv.kvState[op.Key]
				}

				opResult := &OpResult{opType: op.OpType, result: result }
				//fmt.Printf("start result channel")
				if applyMsg.Command.(Op).ResultChan != nil {
					applyMsg.Command.(Op).ResultChan <- opResult
				}
				fmt.Printf("apply end\n")
			}
		}
	}()

	return kv
}
