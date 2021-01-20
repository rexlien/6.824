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
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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

type RequestPair struct {
	request interface{}
	reply interface{}

}

type StartCommand struct {
	logEntry *LogEntry
	resIndex int


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

type LogEntry struct {

	Command interface{}
	Term int

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
	log         []interface{}
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
	commitCh chan interface{}

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
	//doneMessage *Message

	commitCh chan int
	//commitWaiter sync.WaitGroup
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

type AppendEntryRequest struct {

	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []*LogEntry
	LeaderCommit int

}

type AppendEntryReply struct {

	Term int
	Success bool
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

	// Your code here (2B).



	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.me == rf.currentLeader
	rf.mu.Unlock()

	waitChannel := make(chan *Message)

	if !isLeader {
		return -1, term, false
	} else {
		commandMsg := &Message{Type: MsgStartCommand, payload: &StartCommand{logEntry: &LogEntry{Command: command, Term: term}}, doneChan: waitChannel}
		rf.startCh <- commandMsg
	}

	select {
		case msg := <-waitChannel:
			index = msg.payload.(*StartCommand).resIndex
		case <- time.After(30* time.Second):
			return -1, term, isLeader
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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Println("Making...")

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = make([]interface{}, 0)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.quorum = len(peers)/2 + 1

	rf.state = FOLLOWER
	rf.ticker = time.NewTicker(100 * time.Millisecond)
	//rf.r = rand.New(rand.NewSource(0))
	rf.electionTimeout = 5
	rf.heartbeatTimeout = 1
	rf.messageCh = make(chan *Message)
	rf.receiveChan = make(chan *Message)
	rf.startCh = make(chan* Message)
	rf.commitCh = make(chan interface{})
	rf.applyCh = applyCh
	rf.votedFor = -1
	rf.currentLeader = -1

	rf.resetElectionTimeout()
	rf.resetPeerIndices()

	rf.rootLoggerContext = CreateLogContext(zap.Int("server", rf.me))

	//ptr := uintptr(unsafe.Pointer(&rf.currentTerm))
	//_, rf.contextLogger = WithFields(zap.Int("server", rf.me))
	rf.logger = WithLogContext(rf.rootLoggerContext, []zapcore.Field{}...).GetSugarLogger() //rf.contextLogger

	//rf.logger.Info("test")
	//rf.logger.With(zap.)
	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {
			select {
			case msg := <- rf.startCh:
				if rf.state == LEADER {

					rf.logger.Infof("Start a new aggrement")
					logEntry := msg.payload.(*StartCommand).logEntry
					rf.log = append(rf.log, logEntry)
					msg.payload.(*StartCommand).resIndex = len(rf.log)
					rf.syncAppendEntries(msg)
					//msg.doneChan <- msg

				} else {

					rf.logger.Infof("Can't start: not a leader")
					msg.payload.(*StartCommand).resIndex = -1
					go func() {
						msg.doneChan <- msg
					}()
				}
			case _ = <-rf.ticker.C:
				if rf.state == FOLLOWER {
					rf.elapsedElectionTime++
					//rf.elapsedHeartbeatTime++

					if rf.elapsedElectionTime >= rf.randomElectionTimeout {
						rf.logger.Infof("Election timeout..Becoming candidate")

						rf.state = CANDIDATE
						//voteCount := 0

						//rf.currentTerm++
						rf.setTerm(rf.currentTerm + 1)
						///rf.currentLeader = -1
						rf.setLeader(-1)
						rf.currentVoteCount = 1
						rf.setVotedFor(rf.me)

						for i := 0; i < len(peers); i++ {
							if i != rf.me {

								go func(i int, term int) {
									reply := RequestVoteReply{}
									req := &RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: 0, LastLogTerm: 0}
									rf.sendRequestVote(i, req, &reply)
									rf.messageCh <- &Message{Type: MsgSentRequestVote, payload: &reply}
								}(i, rf.currentTerm)
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
						rf.setVotedFor(-1)

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
				if msg.Type == MsgSentRequestVote {
					//if rf.state == CANDIDATE {
					voteReply := msg.payload.(*RequestVoteReply)
					rf.logger.Infof("RequestVoteResp receive %t", voteReply.VoteGranted)
					if voteReply.Term > rf.currentTerm {
						rf.logger.Infof("Larger term : %d vote received", voteReply.Term)
						rf.state = FOLLOWER
						//rf.currentTerm = voteReply.Term
						rf.setTerm(voteReply.Term)
						rf.currentVoteCount = 0
						//rf.votedFor = -1
						rf.setVotedFor(-1)
						rf.resetElectionTimeout()
						break
					} else if voteReply.Term == rf.currentTerm {
						//fmt.Println("same term vote received")
						if voteReply.VoteGranted {
							rf.logger.Infof("Got Vote granted")
							rf.currentVoteCount++
							if rf.state == CANDIDATE {
								if rf.currentVoteCount >= rf.quorum {
									rf.logger.Infof("Became Leader")
									rf.setLeader(rf.me)
									rf.state = LEADER
									rf.sendHeartbeat()
									rf.elapsedHeartbeatTime = 0
								}
							}
						}
					}
					//}

				} else if msg.Type == MsgHeartbeatResp || msg.Type == MsgAppendEntriesResp{

					appendReply, ok := msg.payload.(*AppendEntryReply)
					if !ok {
						appendReply = msg.payload.(*AppendEntriesMessage).reply
					}

					if appendReply.Term > rf.currentTerm {

						rf.logger.Infof("Become follower from Appendentries")
						rf.setTerm(appendReply.Term)
						rf.setVotedFor(-1)
						rf.toFollower()
						rf.resetElectionTimeout()

					} else {

						if msg.Type == MsgAppendEntriesResp {

							rf.logger.Infof("Got MsgAppendEntriesResp")
							appendEntriesMsg := msg.payload.(*AppendEntriesMessage)

							if appendReply.Success {

								newCommitIndex := appendEntriesMsg.request.PrevLogIndex + 1
								if rf.matchIndex[appendEntriesMsg.toServerId] < newCommitIndex {
									rf.matchIndex[appendEntriesMsg.toServerId] = newCommitIndex

									if appendReply.Term == rf.currentTerm {

										countReplica := 0
										for i := 0; i < len(rf.matchIndex); i++ {

											if rf.matchIndex[i] >= newCommitIndex {
												countReplica++
											}
										}
										if countReplica == rf.quorum - 1 {
											if rf.commitIndex < newCommitIndex {
												rf.commitIndex = newCommitIndex
											}
											go func(logger *zap.SugaredLogger) {
												/*
												select {
													case appendEntriesMsg.commitCh <- newCommitIndex:
													case <- time.After(time.Second * 15):
														logger.Infof("send commitCh timeout")
												}
*/
												logger.Infof("send commit chennel")
												rf.commitCh <- appendEntriesMsg

											}(rf.logger)
										}
									}
								}

								if rf.nextIndex[appendEntriesMsg.toServerId] < newCommitIndex {
									rf.nextIndex[appendEntriesMsg.toServerId] = newCommitIndex
								}

							} else {
								rf.logger.Infof("append entry false")
							}

						}
					}
				}
				case msg := <-rf.commitCh:
					rf.logger.Infof("Committed received")
					appendEntriesMessage, ok := msg.(*AppendEntriesMessage)
					if ok {
						rf.applyCommitted()
						appendEntriesMessage.commitCh <- 1
					} else {
						rf.applyCommitted()
					}

					//appendEntriesMessage.commitCh

				case msg := <-rf.receiveChan:
				if msg.Type == MsgReceiveRequest {
					reqPair := msg.payload.(*RequestPair)
					if requestVoteArg, _ := reqPair.request.(*RequestVoteArgs); requestVoteArg != nil {

						rf.logger.Infof("Request Vote Received")
						requestVoteReply := reqPair.reply.(*RequestVoteReply)

						requestVoteReply.Term = rf.currentTerm
						requestVoteReply.VoteGranted = false

						if requestVoteArg.Term < rf.currentTerm {
							msg.doneChan <- msg
							rf.logger.Infof("Vote Not Granted because requets Term: %d is less than current: %d", requestVoteArg.Term, rf.currentTerm)
							break
						}

						if requestVoteArg.Term != rf.currentTerm {
							rf.setVotedFor(-1)
							//rf.currentTerm = requestVoteArg.Term
							rf.setTerm(requestVoteArg.Term)
						}
						//newer Term
						requestVoteReply.Term = rf.currentTerm
						//TODO: check index
						if rf.votedFor == -1 || rf.votedFor == requestVoteArg.CandidateId {
							//rf.votedFor = requestVoteArg.CandidateId
							rf.setVotedFor(requestVoteArg.CandidateId)
							rf.logger.Infof("Voted:%d\n", rf.votedFor)
							requestVoteReply.VoteGranted = true
							rf.resetElectionTimeout()
							msg.doneChan <- msg
							break
						} else {
							rf.logger.Infof("Server: %d already voted %d", rf.me, rf.votedFor)
						}

						msg.doneChan <- msg

					} else if appendEntriesArg, _:= reqPair.request.(*AppendEntryRequest); appendEntriesArg != nil {

						appendEntriesReply := reqPair.reply.(*AppendEntryReply)

						//if rf.currentTerm == appendEntriesArg.Term {

						appendEntriesReply.Term = rf.currentTerm
						//the sender is has less term
						if appendEntriesArg.Term < rf.currentTerm {

							rf.logger.Infof("Got ex-term AppendEntries from Term: %d leader: %d", appendEntriesArg.Term, appendEntriesArg.LeaderId)
							appendEntriesReply.Success = false
						} else {

							if rf.currentTerm != appendEntriesArg.Term {

								rf.setTerm(appendEntriesArg.Term)
								rf.setVotedFor(appendEntriesArg.LeaderId)
							}

							if appendEntriesArg.Entries != nil {
								rf.logger.Infof("Receive append entries")

								if appendEntriesArg.PrevLogIndex > 0 {

									arrayIndex := appendEntriesArg.PrevLogIndex - 1
									if arrayIndex < len(rf.log) {
										if appendEntriesArg.PrevLogTerm != rf.log[arrayIndex].(*LogEntry).Term {

											appendEntriesReply.Success = false
											//delete all follows it

										} else {
											//prevLogEntry := rf.log[appendEntriesArg.PrevLogIndex]
											rf.logger.Infof("sync entries")
											for i:= 0; i < len(appendEntriesArg.Entries); i++ {
												rf.log = append(rf.log, appendEntriesArg.Entries[i])
											}
											appendEntriesReply.Success = true
										}

										//appendEntriesReply.Success = false
									} else {

										//prev index is empty
										appendEntriesReply.Success = false

									}
								} else {
									//just copy
									rf.logger.Infof("sync entries")
									for i:= 0; i < len(appendEntriesArg.Entries); i++ {
										rf.log = append(rf.log, appendEntriesArg.Entries[i])
									}
									//rf.commitIndex = appendEntriesArg.LeaderCommit
									appendEntriesReply.Success = true
								}

							} else {
								appendEntriesReply.Success = true
							}

							lastIndex, _ := rf.getLastLog()

							if appendEntriesArg.LeaderCommit > rf.commitIndex && lastIndex >= appendEntriesArg.LeaderCommit {
								rf.commitIndex = appendEntriesArg.LeaderCommit

								go func() {
									rf.commitCh <- 1
								}()


								//rf.applyCommitted()
							}
							//rf.commitIndex

							//rf.currentLeader = appendEntriesArg.LeaderId
							rf.setLeader(appendEntriesArg.LeaderId)
							rf.toFollower()
							rf.resetElectionTimeout()
						}



						msg.doneChan <- msg

					}
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) setTerm(term int) {

	defer func() {
		rf.mu.Unlock()
	}()

	rf.mu.Lock()
	rf.currentTerm = term
	rf.logger = WithLogContext(rf.rootLoggerContext, zap.Int("term", rf.currentTerm)).GetSugarLogger()

}

func (rf *Raft) setLeader(id int) {
	defer func() {
		rf.mu.Unlock()
	}()

	rf.mu.Lock()
	rf.currentLeader = id
}

func (rf *Raft) applyCommitted() {

	if rf.lastApplied < rf.commitIndex {

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			command := rf.log[i-1].(*LogEntry).Command
			applyMsg := ApplyMsg{Command: command, CommandValid: true, CommandIndex: i}
			rf.applyCh <- applyMsg

		}
		rf.logger.Infof("End apply from %d to %d", rf.lastApplied, rf.commitIndex)
		rf.lastApplied = rf.commitIndex
	}
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

func (rf *Raft) setVotedFor(votedFor int) {

	rf.votedFor = votedFor
}

func (rf *Raft) sendHeartbeat() {

	//if rf.elapsedHeartbeatTime >= rf.heartbeatTimeout {
		//rf.logger.Infof("Server %d Send Heartbeat\n", rf.me)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int, term int, leaderCommitIndex int) {
					reply := AppendEntryReply{}
					req := &AppendEntryRequest{Term: term, LeaderId: rf.me, PrevLogIndex: 0, PrevLogTerm: -1, LeaderCommit: leaderCommitIndex}
					rf.sendAppendEntries(i, req, &reply)
					rf.messageCh <- &Message{Type: MsgHeartbeatResp, payload: &reply}
				}(i, rf.currentTerm, rf.commitIndex)
			}
		}
	//}
}

func (rf *Raft) syncAppendEntries(doneMessage  *Message) {

	lastIndex, logEntry := rf.getLastLog()//len(rf.log)


	//leaderCommit := rf.commitIndex
	commitCh := make(chan int)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if lastIndex >= rf.nextIndex[i]  {

				go func(i int, term int, index int, leaderCommitIndex int) {
					reply := AppendEntryReply{}
					entries := make([]*LogEntry, 1)
					entries[0] = logEntry
					req := &AppendEntryRequest{Term: term, LeaderId: rf.me, PrevLogIndex: lastIndex - 1, PrevLogTerm: logEntry.Term, LeaderCommit: leaderCommitIndex, Entries: entries}
					//req := &AppendEntryRequest{Term: term, LeaderId: rf.me, PrevLogIndex: 0, PrevLogTerm: -1, LeaderCommit: 0}
					//fmt.Printf("Append entries sent...\n")
					rf.sendAppendEntries(i, req, &reply)
					//fmt.Printf("Append entries sent successfully\n")
					payload:= &AppendEntriesMessage{request: req, reply: &reply, commitCh: commitCh, toServerId: i}
					rf.messageCh <- &Message{Type: MsgAppendEntriesResp, payload: payload}
				}(i, rf.currentTerm, rf.nextIndex[i], rf.commitIndex)
			}
		}
	}

	go func(logger* zap.SugaredLogger) {

		looping:= true
		for looping {
			select {
			case _ = <-commitCh:
				doneMessage.doneChan <- doneMessage
				looping = false
			case <-time.After(15 * time.Second):
				logger.Infof("recieve commit channel timedout")
				doneMessage.payload.(*StartCommand).resIndex = -1
				doneMessage.doneChan <- doneMessage
				looping = false
			}
		}
		close(commitCh)
	}(rf.logger)
}

func (rf *Raft) resetPeerIndices() {

	for i:= 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
	}
	for i:= 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}

}

func (rf *Raft) getLastLog() (int, *LogEntry) {

	len := len(rf.log)
	var last *LogEntry = nil

	if len > 0 {
		last = rf.log[len - 1].(*LogEntry)
	}
	return len, last

}
