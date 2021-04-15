package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../go-utils/xln-utils/raft"
// import "bytes"
import "../labgob"

type State int

const LEADER State = 0
const FOLLOWER State = 1
const CANDIDATE State = 2

type MessageType int

const MsgSentRequestVote MessageType = 0
const MsgReceiveRequest MessageType = 1
const MsgAppendEntriesResp MessageType = 2
const MsgStartCommand MessageType = 3
const MsgHeartbeatResp MessageType = 4
const MsgCommitted MessageType = 5
const MsgRequestDone MessageType = 6
const MsgTakeSnapShot MessageType = 7
const MsgInstallSnapshot MessageType = 8
const MsgInstallSnapshotResp MessageType = 9


type AEResult int
const AeEntriesAppendSuccess AEResult = 0
const AeEntriesAppendFailed AEResult = 1
const AeSentError AEResult = 2

type RequestPair struct {
	request interface{}
	reply interface{}

}

type StartCommand struct {
	logEntry *raft.LogEntry
	resIndex int
}

type CommitMsg struct {
	commitIndex int
	//this is needed since log might be
	command interface{}


	snapshot *Snapshot
	doneCh chan *CommitMsg
}

type SnapshotMsg struct {
	snapshot *Snapshot

}

type AppendEntryRequest struct {

	RequestID int64

	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []*raft.LogEntry
	LeaderCommit int

}

type AppendEntryReply struct {

	XTerm int
	XIndex int
	XLen int


	Term int
	Success AEResult
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type InstallSnapshotRequest struct {
	Term int
	LeaderID int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {

	Term int
}


type Message struct {
	Type    MessageType
	payload interface{}

	doneChan chan *Message
}

type AppendEntriesMessage struct {

	toServerId int
	request *AppendEntryRequest
	reply *AppendEntryReply

}

type InstallSnapshotMessage struct {

	toServerId int
	snapshotIndex int
	request *InstallSnapshotRequest
	reply *InstallSnapshotReply

}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}



//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persisted
	currentTerm int
	//log         []interface{}
	votedFor    int

	//volatile
	commitIndex int
	lastApplied int

	//leader only
	nextIndex []int
	matchIndex []int

	state State
	ticker    *time.Ticker

	tickCh    chan uint64
	startCh chan *Message
	messageCh chan *Message
	receiveChan chan *Message
	applyCh chan ApplyMsg
	commitCh chan *CommitMsg
	//snapshotCh chan

	appendResultsMu sync.Mutex
	appendResults map[int64][]*AppendEntryReply
	appendReqID int64

	electionTimeout       int
	heartbeatTimeout      int
	randomElectionTimeout int
	elapsedElectionTime   int
	elapsedHeartbeatTime  int
	r                     *rand.Rand


	quorum      int
	countedReplica int

	currentVoteCount int
	currentLeader int

	rootLoggerContext *LogContext// *zap.SugaredLogger
	//loggerContext *LogContext
	logger *zap.SugaredLogger

	logs *raft.Logs
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	defer func() {
		rf.mu.Unlock()
	}()

	var term int
	var isLeader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.me == rf.currentLeader
	//rf.logger.Infof("Server %d GetState Term: %d, curLeader = %d, IsLeader = %t", rf.me, term, rf.currentLeader, isLeader)
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.encodeState()
	rf.persister.SaveRaftState(data)

	//rf.currentTerm
	//rf.log
	//rf
}

func (rf *Raft) encodeState() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		rf.logger.Fatalf("persist error: %s", err.Error())
	}

	err = e.Encode(rf.votedFor)
	if err != nil {
		rf.logger.Fatalf("persist error: %s", err.Error())
	}
	err = e.Encode(rf.logs.Offset())//e.Encode(rf.logs.Log)
	if err != nil {
		rf.logger.Fatalf("persist error: %s", err.Error())
	}
	err = e.Encode(rf.logs.Log)//e.Encode(rf.logs.Log)
	if err != nil {
		rf.logger.Fatalf("persist error: %s", err.Error())
	}
	data := w.Bytes()
	return data

}

func (rf *Raft) encodeLog() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(rf.logs.Offset())//e.Encode(rf.logs.Log)
	if err != nil {
		rf.logger.Fatalf("persist error: %s", err.Error())
	}
	err = e.Encode(rf.logs.Log)//e.Encode(rf.logs.Log)
	if err != nil {
		rf.logger.Fatalf("persist error: %s", err.Error())
	}
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) bool {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return false
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	curTerm := 0
	votedFor := 0
	offset := 0
	logs := make([]*raft.LogEntry, 0)

	if d.Decode(&curTerm) != nil ||
	    d.Decode(&votedFor) != nil ||
		d.Decode(&offset) != nil ||
		d.Decode(&logs) != nil {

		panic("read persist failed")

	 } else {

	 	rf.setTerm(curTerm, false)
	 	rf.setVotedFor(votedFor, false)
		fmt.Printf("Read Persistent Term : %d, VotedFor: %d, %+v\n", curTerm, votedFor, logs)
	    rf.logs.SetOffset(offset)
	 	rf.logs.Clear()

	   for _, log := range logs {

		   //logEntry := log.(raft.LogEntry)
	   		rf.logs.AppendEntries(log)
		   //rf.log = append(rf.log, &logEntry)
	   }
	 }

	 return true
}



//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	req := &Message{MsgReceiveRequest,&RequestPair{args, reply}, make(chan *Message)}
	rf.receiveChan <- req
	req = <- req.doneChan

}

func (rf *Raft) AppendEntries(args *AppendEntryRequest, reply *AppendEntryReply) {
	// Your code here (2A, 2B).

	req := &Message{MsgReceiveRequest,&RequestPair{args, reply}, make(chan *Message)}
	rf.receiveChan <- req
	req = <- req.doneChan
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {

	req := &Message{MsgInstallSnapshot,&RequestPair{args, reply}, make(chan *Message)}
	rf.receiveChan <- req
	req = <- req.doneChan

}






//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	//rf.logger.Infof("Server: %d, Len: %d", server, len(rf.peers))

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryRequest, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) installSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) bool {

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (retIndex int, retTerm int , retIsLeader bool) {

	defer func() {
		//rf.mu.Unlock()
		//retIndex = -1
		//retTerm = 0
		//retIsLeader = false
	}()

	index := -1
	term := -1
	isLeader := true
	//currentLeader = -1

	// Your code here (2B).



	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.me == rf.currentLeader
	//currentLeader = rf.currentLeader
	rf.mu.Unlock()

	waitChannel := make(chan *Message)

	if !isLeader {
		return -1, term, false
	} else {
		commandMsg := &Message{Type: MsgStartCommand, payload: &StartCommand{logEntry: &raft.LogEntry{Command: command, Term: term}}, doneChan: waitChannel}
		go func() {
			rf.startCh <- commandMsg
		}()


	}

	looping := true
	for looping {
		select {
		case msg := <-waitChannel:
			index = msg.payload.(*StartCommand).resIndex
			looping = false
		case <-time.After(3 * time.Second):
			if rf.killed() {
				looping = false
			}

		}
	}
	fmt.Printf("Start Return index: %d\n", index)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

var rMutex = sync.Mutex{}
var globalR = rand.New(rand.NewSource(time.Now().UnixNano()))

func init() {
	gob.Register(raft.LogEntry{})
	//gob.Register(NonOpCommand{})
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {


	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//rf.log = make([]interface{}, 0)
	rf.logs = raft.NewLogs()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.quorum = len(peers)/2 + 1

	rf.state = FOLLOWER
	rf.ticker = time.NewTicker(100 * time.Millisecond)
	rf.electionTimeout = 10
	rf.heartbeatTimeout = 3
	rf.messageCh = make(chan *Message)
	rf.receiveChan = make(chan *Message)
	rf.startCh = make(chan* Message)
	rf.commitCh = make(chan *CommitMsg, 5000)
	rf.appendResults = make(map[int64][]*AppendEntryReply)
	rf.applyCh = applyCh
	rf.setVotedFor(-1, false)//votedFor = -1
	rf.currentLeader = -1

	rf.rootLoggerContext = CreateLogContext(zap.Int("server", rf.me))

	if !rf.readPersist(persister.ReadRaftState()) {
		rf.logger = WithLogContext(rf.rootLoggerContext, []zapcore.Field{}...).GetSugarLogger() //rf.contextLogger
	}

	rf.logger.Debugf("Raft created")

	rf.resetElectionTimeout()
	rf.resetPeerIndices()

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for !rf.killed() {
			select {
			case msg := <- rf.startCh:
				if rf.state == LEADER {

					rf.logger.Infof("Start a new aggrement %+v", msg.payload.(*StartCommand).logEntry)
					logEntry := msg.payload.(*StartCommand).logEntry
					//rf.log = append(rf.log, logEntry)
					rf.logs.AppendEntries(logEntry)
					msg.payload.(*StartCommand).resIndex = rf.logs.LastIndex()//len(rf.log)

					msg.doneChan <- msg
					rf.syncAppendEntries()


				} else {

					rf.logger.Infof("Can't start: not a leader")
					msg.payload.(*StartCommand).resIndex = -1
					msg.doneChan <- msg
				}
			case _ = <-rf.ticker.C:
				if rf.state == FOLLOWER {
					rf.elapsedElectionTime++

					if rf.elapsedElectionTime >= rf.randomElectionTimeout {
						rf.logger.Infof("Election timeout..Becoming candidate")

						rf.state = CANDIDATE
						rf.setTerm(rf.currentTerm + 1, true)
						rf.setLeader(-1)
						rf.currentVoteCount = 1
						rf.setVotedFor(rf.me, true)

						index, _, lastTerm := rf.logs.GetEntriesFromLastByIndex(0)

						for i := 0; i < len(peers); i++ {
							if i != rf.me {

								go func(i int, term int, logger *zap.SugaredLogger) {
									reply := RequestVoteReply{}
									req := &RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: index, LastLogTerm: lastTerm}
									ok := rf.sendRequestVote(i, req, &reply)
									if ok {
										rf.messageCh <- &Message{Type: MsgSentRequestVote, payload: &reply}
									} else {
										//logger.Warnf("Send Request Vote to %d failed", i)
									}
								}(i, rf.currentTerm, rf.logger)
							}
						}

						rf.resetElectionTimeout()

					}
				}
				if rf.state == CANDIDATE {

					rf.elapsedElectionTime++
					if rf.elapsedElectionTime >= rf.randomElectionTimeout {

						rf.logger.Infof("Election timeout, become follower")
						rf.state = FOLLOWER
						rf.currentVoteCount = 0
						rf.setVotedFor(-1, true)

						rf.resetElectionTimeout()
					}

				}
				if rf.state == LEADER {

					rf.elapsedHeartbeatTime++
					//send heart if heartbeat timeout passed
					if rf.elapsedHeartbeatTime >= rf.heartbeatTimeout {

						rf.sendHeartbeat()
						rf.elapsedHeartbeatTime = 0

					}

				}
			case msg := <-rf.messageCh:
				if msg.Type == MsgInstallSnapshotResp {
					rf.logger.Debugf("install snapshot response received")

					installMsg := msg.payload.(*InstallSnapshotMessage)
					if installMsg.reply.Term > rf.currentTerm {
						rf.logger.Debugf("Become follower from installSnapshot")
						rf.setTerm(installMsg.reply.Term, true)
						rf.setVotedFor(-1, true)
						rf.toFollower()
						rf.setLeader(-1)
						rf.resetElectionTimeout()

					} else {

						rf.logger.Debugf("snapshot install received with index %d", installMsg.snapshotIndex)
						nextIndex := installMsg.snapshotIndex + 1
						if rf.nextIndex[installMsg.toServerId] < nextIndex {
							rf.nextIndex[installMsg.toServerId] = nextIndex
						}

					}


				} else if msg.Type == MsgTakeSnapShot {

					snapshotMsg := msg.payload.(*SnapshotMsg)

					ok, logSnap, includeTerm := rf.logs.SnapShot(snapshotMsg.snapshot.SnapshotIndex)

					if ok {
						state := rf.encodeState()
						snapshotMsg.snapshot.Log = logSnap
						snapshotMsg.snapshot.SnapshotTerm = includeTerm
						snapShotByte := snapshotMsg.snapshot.Encode()
						rf.persister.SaveStateAndSnapshot(state, snapShotByte)
					}
					msg.doneChan <- msg

				} else if msg.Type == MsgSentRequestVote {
					//if rf.state == CANDIDATE {
					voteReply := msg.payload.(*RequestVoteReply)
					rf.logger.Debugf("RequestVoteResp receive %t from %d", voteReply.VoteGranted, voteReply.Term)
					if voteReply.Term > rf.currentTerm {
						rf.logger.Debugf("Larger term : %d vote received", voteReply.Term)
						rf.state = FOLLOWER
						//rf.currentTerm = voteReply.Term
						rf.setTerm(voteReply.Term, true)
						rf.currentVoteCount = 0
						//rf.votedFor = -1
						rf.setVotedFor(-1, true)
						rf.resetElectionTimeout()
						break
					} else if voteReply.Term == rf.currentTerm {
						//fmt.Println("same term vote received")
						if voteReply.VoteGranted {
							rf.logger.Debugf("Got Vote granted")
							rf.currentVoteCount++
							if rf.state == CANDIDATE {
								if rf.currentVoteCount >= rf.quorum {
									rf.logger.Infof("Became Leader")
									rf.setLeader(rf.me)
									rf.state = LEADER
									rf.resetPeerIndices()
									rf.elapsedHeartbeatTime = 0

									//for i, _ := range rf.nextIndex {
									//	rf.nextIndex[i] = len(rf.log)
									//}
									rf.sendHeartbeat()

									//go func() {
									//	rf.Start(NonOpCommand{})
									//}()
								}
							}
						}
					}
					//}

				} else if msg.Type == MsgHeartbeatResp || msg.Type == MsgAppendEntriesResp{

					appendEntriesMsg := msg.payload.(*AppendEntriesMessage)
					appendReply := appendEntriesMsg.reply

					if appendReply.Term > rf.currentTerm {

						rf.logger.Debugf("Become follower from Appendentries")
						rf.setTerm(appendReply.Term, true)
						rf.setVotedFor(-1, true)
						rf.toFollower()
						rf.setLeader(-1)
						rf.resetElectionTimeout()

					} else if appendReply.Term == rf.currentTerm {

						//if rf.state != LEADER {
						//	break
						//}
						//if msg.Type == MsgAppendEntriesResp {




							rf.logger.Debugf("Got MsgAppendEntriesResp PrevIndex: %d from Server : %d", appendEntriesMsg.request.PrevLogIndex, appendEntriesMsg.toServerId)

							//rf.addAppendEntriesResult(appendEntriesMsg.request.RequestID, appendEntriesMsg.reply)

							if appendReply.Success == AeEntriesAppendSuccess {
								newCommitIndex := appendEntriesMsg.request.PrevLogIndex + len(appendEntriesMsg.request.Entries)
								rf.logger.Debugf("New Commit index should be : %d", newCommitIndex)
								if rf.matchIndex[appendEntriesMsg.toServerId] < newCommitIndex {

									rf.matchIndex[appendEntriesMsg.toServerId] = newCommitIndex
									rf.nextIndex[appendEntriesMsg.toServerId] = newCommitIndex + 1

									n := rf.commitIndex
									nextCommit := n

									for {
										//rf.logger.Debugf("updating n %d...", n)
										if n > rf.logs.LastIndex() {
											rf.logger.Warnf("break: commit index %d greater than last log index: %d", n, rf.logs.LastIndex())
											break
										}

										count := 0
										for _, match := range rf.matchIndex {
											if match >= n {
												count++
											}
										}

											if n > rf.logs.Offset() && rf.logs.GetEntry(n-1).Term != rf.currentTerm {
												//continue
												rf.logger.Debugf("Skip can't commit 8c Index %d:", n)
												n++
												continue
											}

										if count >= rf.quorum-1 {
											nextCommit = n
											n++
										} else {
											break
										}

									}

									rf.logger.Debugf("next Commit index: %d", nextCommit)

									//if success == rf.quorum - 1 {
									if nextCommit > rf.commitIndex {

										rf.logger.Debugf("Do actual commit %d", nextCommit)
										rf.commitIndex = nextCommit
										//go func(logger *zap.SugaredLogger) {

											//logger.Infof("Trigger commit!")
										//	rf.commitCh <- &CommitMsg{commitIndex: rf.commitIndex}
										//
										//}(rf.logger)
										rf.applyCommitted(rf.commitIndex)
									}

								}

							} else if appendReply.Success == AeEntriesAppendFailed {

								if rf.state == LEADER && appendEntriesMsg.request.Term == rf.currentTerm {


									rf.logger.Info(spew.Sprintf("append entry false : %#v", appendEntriesMsg))

									//if follower has empty index, send from follower's length
									//TODO : check install snapshot
									if appendEntriesMsg.reply.XTerm == -1 {

										rf.nextIndex[appendEntriesMsg.toServerId] = appendEntriesMsg.reply.XLen + 1

										//newPrev := appendEntriesMsg.reply.XLen
										//rf.logger.Debugf("XLen = %d", newPrev )
										//newPrevTerm := 0


										if rf.nextIndex[appendEntriesMsg.toServerId] < rf.logs.FirstIndex() {
											rf.sendInstallSnapshot(appendEntriesMsg.toServerId, rf.me, rf.currentTerm, rf.persister.ReadSnapshot())
										} else {

											newPrev, _, newPrevTerm := rf.logs.PrevEntry(rf.nextIndex[appendEntriesMsg.toServerId])
											//if newPrev > 0 {

											//	newPrevTerm = rf.logs.GetEntryByLogIndex(newPrev).Term
											//}
											entries := rf.logs.GetEntriesByIndex(rf.nextIndex[appendEntriesMsg.toServerId], -1)

											rf.resendAppendEntriesMessage(entries, appendEntriesMsg, newPrev, newPrevTerm)
										}


									} else {
										//need to send from xindex
										rf.logger.Debugf("Term recovery from Term :%d, Index :%d", appendEntriesMsg.reply.Term, appendEntriesMsg.reply.XIndex)
										rf.nextIndex[appendEntriesMsg.toServerId] = appendEntriesMsg.reply.XIndex


										//if newPrev < 0 {
										//	newPrev = 0
										//}

										if rf.nextIndex[appendEntriesMsg.toServerId] < rf.logs.FirstIndex() {
											rf.sendInstallSnapshot(appendEntriesMsg.toServerId, rf.me, rf.currentTerm, rf.persister.ReadSnapshot())
										} else {
											//newPrev := appendEntriesMsg.reply.XIndex - 1
											newPrev, _, newPrevTerm := rf.logs.PrevEntry(rf.nextIndex[appendEntriesMsg.toServerId])
											//newPrevTerm := rf.logs.GetEntry(newPrev - 1).Term
											entries := rf.logs.GetEntriesByIndex(rf.nextIndex[appendEntriesMsg.toServerId], -1)

											rf.resendAppendEntriesMessage(entries, appendEntriesMsg, newPrev, newPrevTerm)
										}
									}

								} else {
									rf.logger.Infof("append entry ignore for Server: %d", appendEntriesMsg.toServerId)
								}

							}
						//}
					}
				}

				case msg := <-rf.receiveChan:

					if msg.Type == MsgInstallSnapshot {

						request := msg.payload.(*RequestPair).request.(*InstallSnapshotRequest)
						reply := msg.payload.(*RequestPair).reply.(*InstallSnapshotReply)
						reply.Term = rf.currentTerm
						if rf.state == LEADER {
							rf.logger.Warnf("Leader should not install snapshot")
							msg.doneChan <- msg
							break
						}
						if request.Term < rf.currentTerm {
							reply.Term = rf.currentTerm
							rf.logger.Infof("Install sansp shot ignored due to outdated term: %d -> %d", request.Term, rf.currentTerm)
							msg.doneChan <- msg
							break
						}

						rf.logger.Debugf("Install snapshot start!")
						//Do install
						doneChan := make(chan *CommitMsg)
						snapshot := NewSnapshot(request.Data)
						//install log
						rf.logs.Decode(snapshot.Log)

						//send commit msg to install kv
						rf.commitCh <- &CommitMsg{snapshot: snapshot, doneCh: doneChan, commitIndex: snapshot.SnapshotIndex}
						_ = <-doneChan

						rf.logger.Debugf("Install snapshot done!")
						msg.doneChan <- msg


					} else if msg.Type == MsgReceiveRequest {
						reqPair := msg.payload.(*RequestPair)
						if requestVoteArg, _ := reqPair.request.(*RequestVoteArgs); requestVoteArg != nil {

							rf.logger.Debugf("Request Vote Received, LastLogTerm: %d, LastLogIndex: %d", requestVoteArg.LastLogTerm, requestVoteArg.LastLogIndex)
							requestVoteReply := reqPair.reply.(*RequestVoteReply)

							requestVoteReply.Term = rf.currentTerm
							requestVoteReply.VoteGranted = false

							if requestVoteArg.Term < rf.currentTerm {
								msg.doneChan <- msg
								rf.logger.Infof("Vote Not Granted because requets Term: %d is less than current: %d", requestVoteArg.Term, rf.currentTerm)
								break
							}

							if requestVoteArg.Term != rf.currentTerm {
								rf.setVotedFor(-1, true)
								//rf.currentTerm = requestVoteArg.Term
								rf.setTerm(requestVoteArg.Term, true)
							}
							//newer Term
							requestVoteReply.Term = rf.currentTerm
							//TODO: check index
							if rf.votedFor == -1 || rf.votedFor == requestVoteArg.CandidateId {
								//rf.votedFor = requestVoteArg.CandidateId

								index, _, term := rf.logs.GetEntriesFromLastByIndex(0)

								if requestVoteArg.LastLogTerm < term {
									rf.logger.Infof("reject vote to %d, due to outdated term", requestVoteArg.CandidateId)
									requestVoteReply.VoteGranted = false

								} else {

									if requestVoteArg.LastLogTerm == term && requestVoteArg.LastLogIndex < index {
										rf.logger.Infof("reject vote to %d, due to outdated index", requestVoteArg.CandidateId)
										requestVoteReply.VoteGranted = false
									} else {

										rf.setVotedFor(requestVoteArg.CandidateId, true)
										rf.logger.Infof("Voted:%d\n", rf.votedFor)
										requestVoteReply.VoteGranted = true
										rf.resetElectionTimeout()
									}
								}

								msg.doneChan <- msg
								break
							} else {
								rf.logger.Infof("Server: %d already voted %d", rf.me, rf.votedFor)
							}

							msg.doneChan <- msg

						} else if appendEntriesArg, _ := reqPair.request.(*AppendEntryRequest); appendEntriesArg != nil {

							appendEntriesReply := reqPair.reply.(*AppendEntryReply)

							//if rf.currentTerm == appendEntriesArg.Term {

							appendEntriesReply.Term = rf.currentTerm
							//the sender has less term
							if appendEntriesArg.Term < rf.currentTerm {

								rf.logger.Debugf("Got ex-term AppendEntries from Term: %d leader: %d", appendEntriesArg.Term, appendEntriesArg.LeaderId)
								appendEntriesReply.Success = AeEntriesAppendFailed
							} else {

								if appendEntriesArg.Term > rf.currentTerm {
									//rf.logger.Debugf("Got newer term AppendEntries from Term: %d leader: %d", appendEntriesArg.Term, appendEntriesArg.LeaderId)
									rf.setTerm(appendEntriesArg.Term, true)
									rf.setVotedFor(appendEntriesArg.LeaderId, true)
								}

								if appendEntriesArg.Entries != nil {
									rf.logger.Debugf("Receive append entries from Term: %d, PrevIndex: %d, PrevLogTerm: %d", appendEntriesArg.Term, appendEntriesArg.PrevLogIndex, appendEntriesArg.PrevLogTerm)

									if appendEntriesArg.PrevLogIndex > 0 {

										rf.logger.Debugf("PrevLogIndex: %d, LastLogIndex: %d", appendEntriesArg.PrevLogIndex, rf.logs.LastIndex())
										logIndex := appendEntriesArg.PrevLogIndex
										if logIndex <= rf.logs.LastIndex() {

											//less than first index
											if logIndex < rf.logs.FirstIndex() {

												if logIndex == rf.logs.Offset() {
													if appendEntriesArg.PrevLogTerm == rf.logs.IncludedTerm {
														rf.logs.ReplaceEntriesFrom(appendEntriesArg.Entries, logIndex, true)
														rf.logger.Debugf("[Log] conflict update, %s", rf.logs.ToString())
														appendEntriesReply.Success = AeEntriesAppendSuccess
													} else {
														appendEntriesReply.XIndex = logIndex
														appendEntriesReply.Success = AeEntriesAppendFailed
													}
												} else {

													appendEntriesReply.XIndex = logIndex
													appendEntriesReply.Success = AeEntriesAppendFailed
												}


											} else if appendEntriesArg.PrevLogTerm != rf.logs.GetEntryByLogIndex(logIndex).Term {

												appendEntriesReply.XTerm = rf.logs.GetEntryByLogIndex(logIndex).Term
												xIndex := logIndex
												for ;xIndex >= rf.logs.FirstIndex(); xIndex-- {

													if rf.logs.GetEntryByLogIndex(xIndex).Term != appendEntriesReply.XTerm {
														//xIndex++
														break
													}
												}
												xIndex++
												appendEntriesReply.XIndex = xIndex
												//rf.logger.Debugf("append entries not consistent of term %d, from %d: log: %s", appendEntriesReply.XTerm, appendEntriesReply.XIndex, rf.printLog())
												appendEntriesReply.Success = AeEntriesAppendFailed

											} else {

												//TODO: check first index if consistent and discard when inconsistency is found
												//rf.log = append(rf.log[0:arrayIndex+1], interfaces...)
												mergedIndex := logIndex + len(appendEntriesArg.Entries)

												//original log is more complete, mostly can ignore but but check need to discard
												if rf.logs.LastIndex() > mergedIndex {

													rf.logger.Debugf("current array index longer then merged index: current :%d, Merged array index: %d", rf.logs.LastIndex(), mergedIndex)
													replaceLog := rf.logs.GetEntriesFromByIndex(logIndex) //log[arrayIndex:len(rf.log)]

													conflictFound := false
													for i := len(appendEntriesArg.Entries) - 1; i >= 0; i-- {

														if i < len(replaceLog) && appendEntriesArg.Entries[i].Term != replaceLog[i].Term {
															conflictFound = true
														}

													}
													if conflictFound {
														//discard
														//rf.log = append(rf.log[0:arrayIndex+1], interfaces...)
														rf.logs.ReplaceEntriesFrom(appendEntriesArg.Entries, logIndex, true)
														rf.logger.Debugf("[Log] Conflict update, %s", rf.logs.ToString())
													} else {
														rf.logger.Debugf("[Log] Outdated update!!!!!!!!!!!!!!!!!!!!!!!!!! Non conflict ignore: %s", rf.logs.ToString())

													}

												} else {
													//rf.log = append(rf.log[0:arrayIndex+1], interfaces...)

													rf.logs.ReplaceEntriesFrom(appendEntriesArg.Entries, logIndex, true)
													rf.logger.Debugf("[Log] conflict update, %s", rf.logs.ToString())
												}

												rf.persist()

												//rf.logger.Debugf("Server: %d, Append Sucessfully: %s", rf.me, rf.printLog())
												appendEntriesReply.Success = AeEntriesAppendSuccess
											}

											//appendEntriesReply.Success = false
										} else {

											//prev index is empty
											appendEntriesReply.XTerm = -1
											appendEntriesReply.XIndex = -1
											appendEntriesReply.XLen = rf.logs.LastIndex()//rf.logs.Len()

											rf.logger.Debugf("prev Index not exist :%s", rf.logs.ToString())
											appendEntriesReply.Success = AeEntriesAppendFailed

										}
									} else {
										//just copy from start
										//TODO: probably not truncate

										rf.logs.Clear() //log = nil
										//for i:= 0; i < len(appendEntriesArg.Entries); i++ {
										//rf.log = append(rf.log, appendEntriesArg.Entries[i])
										//}
										rf.logs.AppendEntries(appendEntriesArg.Entries...)
										rf.persist()
										rf.logger.Debugf("[Log] Server: %d, Copy from start sucessfully: %s", rf.me, rf.logs.ToString())
										appendEntriesReply.Success = AeEntriesAppendSuccess
									}

								} else {

									if appendEntriesArg.PrevLogIndex > 0 {
										//arrayIndex := appendEntriesArg.PrevLogIndex - 1
										rf.logger.Debugf("PrevLogIndex: %d", appendEntriesArg.PrevLogIndex)
										logIndex := appendEntriesArg.PrevLogIndex
										if logIndex <= rf.logs.LastIndex() {

											if appendEntriesArg.PrevLogTerm != rf.logs.GetEntryByLogIndex(logIndex).Term {

												appendEntriesReply.XTerm = rf.logs.GetEntryByLogIndex(logIndex).Term
												xIndex := logIndex
												for ; xIndex >= rf.logs.FirstIndex(); xIndex-- {

													if rf.logs.GetEntryByLogIndex(xIndex).Term != appendEntriesReply.XTerm {
														//xIndex++
														break
													}
												}
												xIndex++
												appendEntriesReply.XIndex = xIndex
												//rf.logger.Debugf("append entries not consistent of term %d, from %d: log: %s", appendEntriesReply.XTerm, appendEntriesReply.XIndex, rf.printLog())
												appendEntriesReply.Success = AeEntriesAppendFailed
											}
										} else {
											appendEntriesReply.XTerm = -1
											appendEntriesReply.XIndex = -1
											appendEntriesReply.XLen = rf.logs.LastIndex()//rf.logs.Len()

											rf.logger.Debugf("prev Index not exist :%s", rf.logs.ToString())
											appendEntriesReply.Success = AeEntriesAppendFailed
										}
									}
									//appendEntriesReply.Success = AeEntriesAppendSuccess
								}

								//rf.commitIndex

								//rf.currentLeader = appendEntriesArg.LeaderId
								rf.setLeader(appendEntriesArg.LeaderId)
								rf.toFollower()
								rf.resetElectionTimeout()

								lastIndex, _, term := rf.logs.GetEntriesFromLastByIndex(0)

								if appendEntriesArg.LeaderCommit > rf.commitIndex && appendEntriesReply.Success == AeEntriesAppendSuccess && appendEntriesReply.Term == term {

									rf.logger.Debugf("New entries Commit:%d", appendEntriesArg.LeaderCommit)
									rf.commitIndex = appendEntriesArg.LeaderCommit

									//min of last index and commit index
									if lastIndex < rf.commitIndex {
										rf.commitIndex = lastIndex
									}
									rf.logger.Debugf("send commit, commit index:%d", rf.commitIndex)

									//go func(commitIndex int) {
									//	rf.commitCh <- &CommitMsg{commitIndex: commitIndex}
									//} (rf.commitIndex)
									rf.applyCommitted(rf.commitIndex)
								}
							}

							msg.doneChan <- msg
						}
					}
			}
		}
		fmt.Printf("Raft killed %d\n", rf.me)
	}()

	go func() {
		for !rf.killed() {
			select {
				//WWcase
				case commitMsg := <-rf.commitCh:
					rf.logger.Debugf("Committed received")
					//if it's not a snap shot commit
					if commitMsg.snapshot == nil {
						//rf.applyCommitted(commitMsg.commitIndex)
						applyMsg := ApplyMsg{Command: commitMsg.command, CommandValid: true, CommandIndex: commitMsg.commitIndex}
						rf.applyCh <- applyMsg
					} else {
						rf.logger.Debugf("Applying snapshot")
						rf.applySnapshot(commitMsg.snapshot)
					}
					if commitMsg.doneCh != nil {
						commitMsg.doneCh <- commitMsg
					}
			}
		}
	}()

	return rf
}

func (rf *Raft) setTerm(term int, persist bool) {

	defer func() {
		rf.mu.Unlock()
	}()

	rf.mu.Lock()
	rf.currentTerm = term
	if persist {
		rf.persist()
	}
	rf.logger = WithLogContext(rf.rootLoggerContext, zap.Int("term", rf.currentTerm)).GetSugarLogger()

}

func (rf *Raft) GetTerm() int {
	defer func() {
		rf.mu.Unlock()
	}()

	rf.mu.Lock()
	return rf.currentTerm
}

func (rf *Raft) setLeader(id int) {
	defer func() {
		rf.mu.Unlock()
	}()

	rf.mu.Lock()
	rf.currentLeader = id
}

func (rf *Raft) getLeader() int {
	defer func() {
		rf.mu.Unlock()
	}()

	rf.mu.Lock()
	return rf.currentLeader
}

func (rf *Raft) applyCommitted(commitIndex int) {

	if rf.lastApplied < commitIndex {

		for i := rf.lastApplied + 1; i <= commitIndex; i++ {
			command := rf.logs.GetEntry(i-1).Command

			//go func(i int) {
			rf.commitCh <- &CommitMsg{commitIndex: i, command: command}
			//}(i)
			//applyMsg := ApplyMsg{Command: command, CommandValid: true, CommandIndex: i}
			//rf.applyCh <- applyMsg

		}
		rf.logger.Infof("End apply from %d to %d", rf.lastApplied + 1, rf.commitIndex)
		rf.lastApplied = commitIndex
	}
}

func (rf *Raft) applySnapshot(snapShot *Snapshot) {


	applyMsg := ApplyMsg{Command: snapShot, CommandValid: false, CommandIndex: snapShot.SnapshotIndex}
	rf.applyCh <- applyMsg
	rf.lastApplied = snapShot.SnapshotIndex
}


func (rf *Raft) resetElectionTimeout()  {

	defer func() {
		rMutex.Unlock()
	}()
	rMutex.Lock()
	rf.randomElectionTimeout = rf.electionTimeout + globalR.Intn(rf.electionTimeout)
	rf.elapsedElectionTime = 0
	//rf.logger.Infof("Next Election timeout: %d\n",rf.randomElectionTimeout)
}

func (rf *Raft) toFollower() {

	rf.state = FOLLOWER
	//rf.votedFor = -1
	rf.currentVoteCount = 0

}

func (rf *Raft) setVotedFor(votedFor int, persist bool) {

	defer func() {
		rf.mu.Unlock()
	}()
	rf.mu.Lock()
	rf.votedFor = votedFor
	if persist {
		rf.persist()
	}
}

func (rf *Raft) getVotedFor() int {
	defer func() {
		rf.mu.Unlock()
	}()
	rf.mu.Lock()
	return rf.votedFor
}

func (rf *Raft) sendHeartbeat() {

	lastIndex, _, lastTerm := rf.logs.GetEntriesFromLastByIndex(0)

	//if rf.elapsedHeartbeatTime >= rf.heartbeatTimeout {
		//rf.logger.Infof("Server %d Send Heartbeat commit: %d \n", rf.me, rf.commitIndex)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				var newEntries []*raft.LogEntry

				prevIndex, _, prevTerm := rf.logs.PrevEntry(rf.logs.FirstIndex())
				rf.logger.Debugf("firstIndex: %d, nextIndex: %d for server: %d", prevIndex, rf.nextIndex[i], i)

				//if already snapshot from follower's next index, should send install snapshot
				if rf.nextIndex[i] <= prevIndex {

					rf.logger.Debugf("install Snaphost for server: %d", i)

					rf.sendInstallSnapshot(i, rf.me, rf.currentTerm, rf.persister.ReadSnapshot())

				} else {
					if lastIndex >= rf.nextIndex[i] {

						newEntries = rf.logs.GetEntriesByIndex(rf.nextIndex[i], -1)
						prevIndex, _, prevTerm = rf.logs.PrevEntry(rf.nextIndex[i])
					}

					go func(i int, term int, leaderCommitIndex int) {
						reply := AppendEntryReply{}
						var req *AppendEntryRequest
						if newEntries == nil {
							req = &AppendEntryRequest{Term: term, LeaderId: rf.me, PrevLogIndex: lastIndex, PrevLogTerm: lastTerm, LeaderCommit: leaderCommitIndex, Entries: newEntries}
						} else {
							req = &AppendEntryRequest{Term: term, LeaderId: rf.me, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, LeaderCommit: leaderCommitIndex, Entries: newEntries}
						}
						rf.logger.Debugf("Heart send new append message: %+v to server: %d", req, i)
						ok := rf.sendAppendEntries(i, req, &reply)
						if ok {
							if newEntries == nil {
								appendMsg := &AppendEntriesMessage{request: req, reply: &reply, toServerId: i}
								rf.messageCh <- &Message{Type: MsgHeartbeatResp, payload: appendMsg}
							} else {
								appendMsg := &AppendEntriesMessage{request: req, reply: &reply, toServerId: i}
								rf.messageCh <- &Message{Type: MsgAppendEntriesResp, payload: appendMsg}
							}
						} else {
							//rf.logger.Debugf("Send heartbeat failed to server: %d", i)
						}
					}(i, rf.currentTerm, rf.commitIndex)
				}
			}
		}
	//}
}

func (rf *Raft) syncAppendEntries() {

	lastIndex, logEntry, _ := rf.logs.GetEntriesFromLastByIndex(0)
	_, _, prevTerm := rf.logs.GetEntriesFromLastByIndex(1) ///getLogFromLast(1)

	appendReqID := atomic.AddInt64(&rf.appendReqID, 1)
	//leaderCommit := rf.commitIndex
	//rf.createAppendEntriesResultEntry(appendReqID)

	rf.logger.Debugf("send Append Entries, Leader logs are:%s", rf.logs.ToString() )

	rf.persist()
	//commitCh := make(chan int)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if lastIndex >= rf.nextIndex[i]  {
				rf.logger.Debugf("Send Server %d from lastIndex: %d, nextIndex: %d", i, lastIndex, rf.nextIndex[i])
				go func(i int, term int, index int, leaderCommitIndex int, appendReqID int64) {
					reply := AppendEntryReply{}
					entries := make([]*raft.LogEntry, 1)
					entries[0] = logEntry
					req := &AppendEntryRequest{RequestID: appendReqID, Term: term, LeaderId: rf.me, PrevLogIndex: lastIndex - 1, PrevLogTerm: prevTerm, LeaderCommit: leaderCommitIndex, Entries: entries}
					//req := &AppendEntryRequest{Term: term, LeaderId: rf.me, PrevLogIndex: 0, PrevLogTerm: -1, LeaderCommit: 0}
					//fmt.Printf("Append entries sent...\n")
					ok := rf.sendAppendEntries(i, req, &reply)
					//fmt.Printf("Append entries sent successfully\n")
					if ok {
						payload := &AppendEntriesMessage{request: req, reply: &reply, toServerId: i}
						rf.messageCh <- &Message{Type: MsgAppendEntriesResp, payload: payload}
					} else {
						//rf.logger.Debugf("Append entries rpc failed")
					}
				}(i, rf.currentTerm, rf.nextIndex[i], rf.commitIndex, appendReqID)
			}
		}
	}
	/*
	go func(logger* zap.SugaredLogger) {

		looping:= true
		for looping {
			select {
			case _ = <-commitCh:
				//doneMessage.doneChan <- doneMessage
				rf.messageCh <- &Message{Type: MsgRequestDone, payload: appendReqID}
				looping = false
			case <-time.After(5 * time.Second):
				logger.Infof("recieve commit channel timedout")
				//doneMessage.payload.(*StartCommand).resIndex = -1
				//doneMessage.doneChan <- doneMessage
				rf.messageCh <- &Message{Type: MsgRequestDone, payload: appendReqID}
				looping = false
			}
		}
		close(commitCh)
	}(rf.logger)

	 */
}


func (rf *Raft) resendAppendEntriesMessage(entries []*raft.LogEntry, message *AppendEntriesMessage, prevIndex int, prevTerm int) {


	rf.logger.Infof("Resend to %d append Entries from prevIndex: %d, prevTerm: %d", message.toServerId, prevIndex, prevTerm)

	go func(i int, term int, leaderCommitIndex int, appendReqID int64) {
		reply := AppendEntryReply{}

		req := &AppendEntryRequest{RequestID: appendReqID, Term: term, LeaderId: rf.me, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, LeaderCommit: leaderCommitIndex, Entries: entries}
		ok := rf.sendAppendEntries(i, req, &reply)
		//fmt.Printf("Append entries sent successfully\n")
		if ok {
			payload := &AppendEntriesMessage{request: req, reply: &reply, toServerId: i}
			rf.messageCh <- &Message{Type: MsgAppendEntriesResp, payload: payload}
		}
	}(message.toServerId, message.request.Term, message.request.LeaderCommit, message.request.RequestID)

}

func (rf *Raft) sendInstallSnapshot(serverID int, leaderID int, term int, data []byte ) {

	rf.logger.Debugf("send install snapshot to %d", serverID)
	go func(i int, leaderID int, term int, data []byte) {

		req := &InstallSnapshotRequest{}
		reply := InstallSnapshotReply{}

		snapshot := NewSnapshot(data)


		req.LeaderID = leaderID
		req.Term = term
		req.LastIncludedIndex = snapshot.SnapshotIndex
		req.LastIncludedTerm = snapshot.SnapshotTerm
		req.Done = true
		req.Offset = 0
		req.Data = data


		ok := rf.installSnapshot(i, req, &reply)
		if ok {
			//rf.logger.Debugf("Install snapshot succeeded!!!")
			rf.messageCh <- &Message{Type: MsgInstallSnapshotResp, payload: &InstallSnapshotMessage{request: req, reply: &reply, toServerId: i, snapshotIndex: snapshot.SnapshotIndex}}
		} else {
			rf.logger.Debugf("Install snapshot rpc failed!")
		}
	}(serverID, leaderID, term, data)


}

func (rf *Raft) resetPeerIndices() {

	for i:= 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.logs.LastIndex() + 1
	}
	for i:= 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}

}

func (rf *Raft) Snapshot(snapShot []byte, index int, doneChan chan *Message) {

	go func() {
		rf.messageCh <- &Message{Type: MsgTakeSnapShot, payload: &SnapshotMsg{snapshot: &Snapshot{Snapshot: snapShot, SnapshotIndex: index} }, doneChan: doneChan}
	}()
}

