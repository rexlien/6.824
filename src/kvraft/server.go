package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	xlnraft "../go-utils/xln-utils/raft"
	"github.com/rexlien/go-utils/xln-utils/logger"
	"go.uber.org/zap"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	Value string

	//ResultChan chan *OpResult
	ClientID int64
	ReqID int32
}

type OpResult struct {

	opType OpType
	result interface{}
}

type PutAppendRequestRecord struct {
	requestID int32
	reply *PutAppendReply
}

type GetRequestRecord struct {
	requestID int32
	reply *GetReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//kvState map[string]string
	kv *xlnraft.StringKV
	ticker *time.Ticker


	clientReqRecord sync.Map
	getReqRecord sync.Map
	curClientReqID sync.Map
	clientResultChan sync.Map
	clientResultChanMu sync.Mutex

	clientMutexMapMu sync.Mutex
	clientMutexMap sync.Map


	logger *zap.SugaredLogger
}

func (kv *KVServer) getChannelMap(clientID int64) *sync.Map {

	defer func() {
		kv.clientResultChanMu.Unlock()
	}()
	kv.clientResultChanMu.Lock()
	channelMap, ok := kv.clientResultChan.Load(clientID)
	if ok {
		return channelMap.(*sync.Map)
	} else {

		newChannelMap := &sync.Map{}
		kv.clientResultChan.Store(clientID, newChannelMap)
		return newChannelMap
	}


}

func (kv *KVServer) getClientMutex(clientID int64) *sync.Mutex {

	mutex, ok := kv.clientMutexMap.Load(clientID)
	if ok {
		return mutex.(*sync.Mutex)
	} else {

		defer func() {
			kv.clientMutexMapMu.Unlock()
		}()
		kv.clientMutexMapMu.Lock()
		mutex, ok := kv.clientMutexMap.Load(clientID)
		if !ok {

			newMutex := &sync.Mutex{}
			kv.clientMutexMap.Store(clientID, newMutex)
			return newMutex
		} else {
			return mutex.(*sync.Mutex)
		}

	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	record, ok := kv.getReqRecord.Load(args.ClientID)
	if ok {
		lastRecord := record.(*GetRequestRecord)
		if lastRecord.requestID == args.RequestID {

			kv.logger.Debugf("Duplicated retry ignored")
			reply.Value = lastRecord.reply.Value
			reply.Err = lastRecord.reply.Err
			return
		}
	}

	defer func() {
		kv.getClientMutex(args.ClientID).Unlock()
	}()
	kv.getClientMutex(args.ClientID).Lock()
	channelMap := kv.getChannelMap(args.ClientID)


	resultChan := make(chan *OpResult)
	channelMap.Store(args.RequestID, resultChan)

	op := Op{ OpType: GET, Key: args.Key, ClientID: args.ClientID, ReqID: args.RequestID}
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		looping := true
		for looping {
			select {
			case result := <-resultChan:
				kv.logger.Debugf("GET Result channel")
				if result.result == nil {
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
					reply.Value = result.result.(string)
				}
				kv.getReqRecord.Store(args.ClientID, &GetRequestRecord{requestID: args.RequestID, reply: reply})
				looping = false
				//close(resultChan)
			case <- time.After(1 * time.Second):
				if term != kv.rf.GetTerm() {
					kv.logger.Warnf("Leader lossed")
					reply.Err = ErrWrongLeader
					looping = false
					//close(resultChan)
				}

			}
		}
	}

	channelMap.Delete(args.RequestID)



}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply)  {
	// Your code here.

	record, ok := kv.clientReqRecord.Load(args.ClientID)//[args.ClientID]
	if ok {
		lastRecord := record.(*PutAppendRequestRecord)
		if lastRecord.requestID == args.RequestID {

			kv.logger.Debugf("Duplicated retry ignored")
			reply.Err = lastRecord.reply.Err

			return
		}
	}

	defer func() {
		kv.getClientMutex(args.ClientID).Unlock()
	}()
	kv.getClientMutex(args.ClientID).Lock()

	channelMap := kv.getChannelMap(args.ClientID)
	resultChan := make(chan *OpResult)

	channelMap.Store(args.RequestID, resultChan)

	var opType OpType
	if args.Op == "Put" {
		opType = PUT
	} else {
		opType = APPEND
	}

	op := Op{ OpType: opType, Key: args.Key, Value: args.Value, ClientID: args.ClientID, ReqID: args.RequestID}
	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {

		looping:= true
		for looping  {
			select {
			case _ = <-resultChan:
				kv.logger.Debugf("Put Append Result: %s", args.Value)
				reply.Err = OK
				kv.clientReqRecord.Store(args.ClientID, &PutAppendRequestRecord{requestID: args.RequestID, reply: reply})
				reply.Index = index
				looping = false
				//close(resultChan)

			case <- time.After(1 * time.Second):
				if term != kv.rf.GetTerm() {

					kv.logger.Warnf("Leader lossed")
					reply.Err = ErrWrongLeader
					reply.Index = index
					//close(resultChan)
					looping = false
				}
			}

		}
	}

	channelMap.Delete(args.RequestID)


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


	kv.logger = logger.CreateLogContext(zap.Int("server", kv.me)).GetSugarLogger()


	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.kv = xlnraft.NewStringKV()//make(map[string]string)
	kv.clientReqRecord = sync.Map{} // make(map[int64]*PutAppendRequestRecord)
	kv.curClientReqID = sync.Map{}
	kv.clientResultChan = sync.Map{}
	kv.getReqRecord = sync.Map{}

	kv.clientMutexMap = sync.Map{}
	kv.ticker = time.NewTicker(5000 * time.Millisecond)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func(maxRaftState int) {
		for !kv.killed() {
			select {
			//case <- time.After(10 * time.Millisecond):
			//	break
			case applyMsg := <- kv.applyCh:

				if applyMsg.CommandValid {
					op := applyMsg.Command.(Op)
					kv.logger.Debugf("apply start server: %d key:%s, value:%s", kv.me, op.Key, op.Value)
					var result interface{} = nil
					if op.OpType == GET {
						result = kv.kv.KV[op.Key]
					} else {

						reqID, ok := kv.curClientReqID.Load(op.ClientID)
						duplicateFound := false
						if ok && reqID == op.ReqID {
							duplicateFound = true
						}
						if !duplicateFound {
							if op.OpType == PUT {
								kv.kv.KV[op.Key] = op.Value
								result = kv.kv.KV[op.Key]
							} else {
								//append(kv.kvState[op.Key].([]string), op.Value)
								kv.kv.KV[op.Key] += op.Value
								result = kv.kv.KV[op.Key]
							}
							kv.curClientReqID.Store(op.ClientID, op.ReqID)
						}
					}
					opResult := &OpResult{opType: op.OpType, result: result}
					//fmt.Printf("start result channel")

					channelMap, ok := kv.clientResultChan.Load(op.ClientID)
					if ok {
						channel, ok := channelMap.(*sync.Map).Load(op.ReqID)
						//go func() {
						if ok {
							select {
							case channel.(chan *OpResult) <- opResult:
								break
							case <-time.After(1000 * time.Millisecond):
								kv.logger.Warn("result wait too long, must be leader changed")
							}
						}
					}

					currentSize := persister.RaftStateSize()

					if maxraftstate != -1 && currentSize >= maxRaftState {

						bytes := kv.kv.Encode()
						snapShotIndex := applyMsg.CommandIndex
						kv.logger.Warnf("Start snapshot from index: %d Size: %d / %d", snapShotIndex, currentSize, maxraftstate)

						doneChannel := make(chan *raft.Message)
						kv.rf.Snapshot(bytes, snapShotIndex, doneChannel)

						_ = <-doneChannel

					}
				} else {
					//apply snapshot
					snapShot := applyMsg.Command.(*raft.Snapshot)
					kv.kv.Decode(snapShot.Snapshot)

				}


				//kv.logger.Debugf("apply end")
			}


		}
	}(kv.maxraftstate)

	return kv
}
