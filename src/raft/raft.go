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
const MsgSentAppendEntries MessageType = 2

type RequestPair struct {
	request interface{}
	reply interface{}

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
	log         []interface{}
	votedFor    int

	state State

	tickCh    chan uint64
	messageCh chan *Message
	ticker    *time.Ticker

	receiveChan chan *Message

	electionTimeout       int
	heartbeatTimeout      int
	randomElectionTimeout int
	elapsedElectionTime   int
	elapsedHeartbeatTime  int
	r                     *rand.Rand

	commitIndex int
	quorum      int

	currentVoteCount int
	currentLeader int
}

type Message struct {
	Type    MessageType
	payload interface{}

	waitChan chan *Message
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
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
	req = <- req.waitChan

}

func (rf *Raft) AppendEntries(args *AppendEntryRequest, reply *AppendEntryReply) {
	// Your code here (2A, 2B).

	req := &Message{MsgReceiveRequest,&RequestPair{args, reply}, make(chan *Message)}
	rf.receiveChan <- req
	req = <- req.waitChan

}

type AppendEntryRequest struct {

	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries[] interface{}
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

	//fmt.Printf("Server: %d, Len: %d", server, len(rf.peers))

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term = rf.currentTerm
	isLeader = rf.me == rf.currentLeader

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

var globalR = rand.New(rand.NewSource(time.Now().Unix()))

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Println("Making...")

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.quorum = len(peers)/2 + 1

	rf.state = FOLLOWER
	rf.log = make([]interface{}, 8)
	rf.ticker = time.NewTicker(100 * time.Millisecond)
	//rf.r = rand.New(rand.NewSource(0))
	rf.electionTimeout = 20
	rf.heartbeatTimeout = 1
	rf.messageCh = make(chan *Message)
	rf.receiveChan = make(chan *Message)
	rf.votedFor = -1
	rf.currentLeader = -1
	rf.resetElectionTimeout()

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {
			select {
			case _ = <-rf.ticker.C:
				if rf.state == FOLLOWER {
					rf.elapsedElectionTime++
					//rf.elapsedHeartbeatTime++

					if rf.elapsedElectionTime >= rf.randomElectionTimeout {
						fmt.Println("Election timeout..Becoming candidate")
						rf.state = CANDIDATE
						//voteCount := 0

						rf.currentTerm++
						rf.currentVoteCount = 1
						rf.votedFor = rf.me

						for i := 0; i < len(peers); i++ {
							if i != rf.me {

								go func(i int) {
									reply := RequestVoteReply{}
									req := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: 0, LastLogTerm: 0}
									rf.sendRequestVote(i, req, &reply)
									rf.messageCh <- &Message{Type: MsgSentRequestVote, payload: &reply}
								}(i)
							}
						}

						rf.resetElectionTimeout()

					}
				}
				if rf.state == CANDIDATE {

					rf.elapsedElectionTime++
					if rf.elapsedElectionTime >= rf.randomElectionTimeout {

						fmt.Println("Election timeout, become follower")
						rf.state = FOLLOWER
						rf.currentVoteCount = 0
						rf.votedFor = -1

						rf.resetElectionTimeout()
					}

				}
				if rf.state == LEADER {

					rf.elapsedHeartbeatTime++
					//send heart if heartbeat timeout passed
					if rf.elapsedHeartbeatTime >= rf.heartbeatTimeout {
						fmt.Printf("Send Heartbeat\n")
						for i := 0; i < len(peers); i++ {
							if i != rf.me {
								go func(i int) {
									reply := AppendEntryReply{}
									req := &AppendEntryRequest{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex:0, PrevLogTerm:0, LeaderCommit: 0}
									rf.sendAppendEntries(i, req, &reply)
									rf.messageCh <- &Message{Type: MsgSentAppendEntries, payload: &reply}
								}(i)
							}
						}
						rf.elapsedHeartbeatTime = 0

					}

				}
			case msg := <-rf.messageCh:
				if msg.Type == MsgSentRequestVote {
					//if rf.state == CANDIDATE {
					fmt.Println("RequestVote Sent")
					voteReply := msg.payload.(*RequestVoteReply)
					if voteReply.Term > rf.currentTerm {
						fmt.Println("Larger term vote received")
						rf.state = FOLLOWER
						rf.currentTerm = voteReply.Term
						rf.currentVoteCount = 0
						rf.votedFor = -1
						rf.resetElectionTimeout()
						break
					} else if voteReply.Term == rf.currentTerm {
						//fmt.Println("same term vote received")
						if voteReply.VoteGranted {
							fmt.Println("Got Vote granted")
							rf.currentVoteCount++
							if rf.state == CANDIDATE {
								if rf.currentVoteCount >= rf.quorum {
									fmt.Printf("%d Become Leader\n", rf.me)
									rf.state = LEADER
								}
							}
						}
					}
					//}

				} else if msg.Type == MsgSentAppendEntries {

					appendReply := msg.payload.(*AppendEntryReply)
					//if appendReply.Success == false {

						if appendReply.Term > rf.currentTerm {

							rf.currentTerm = appendReply.Term
							rf.state = FOLLOWER
							rf.votedFor = -1
							rf.currentVoteCount = 0
							rf.resetElectionTimeout()

						}
					//}
				}
			case msg := <-rf.receiveChan:
				if msg.Type == MsgReceiveRequest {
					reqPair := msg.payload.(*RequestPair)
					if requestVoteArg, _ := reqPair.request.(*RequestVoteArgs); requestVoteArg != nil {

						fmt.Printf("Request Vote Received\n")
						requestVoteReply := reqPair.reply.(*RequestVoteReply)

						requestVoteReply.Term = rf.currentTerm
						requestVoteReply.VoteGranted = false

						if requestVoteArg.Term < rf.currentTerm {
							msg.waitChan <- msg
							break
						}

						//newer Term
						rf.currentTerm = requestVoteArg.Term
						requestVoteReply.Term = rf.currentTerm
						//TODO: check index
						if rf.votedFor == -1 {
							rf.votedFor = requestVoteArg.CandidateId
							fmt.Printf("Voted:%d\n", rf.votedFor)
							requestVoteReply.VoteGranted = true
							rf.resetElectionTimeout()
							msg.waitChan <- msg
							break
						}

						msg.waitChan <- msg

					} else if appendEntriesArg, _:= reqPair.request.(*AppendEntryRequest); appendEntriesArg != nil {
						appendEntriesReply := reqPair.reply.(*AppendEntryReply)

						//if rf.currentTerm == appendEntriesArg.Term {

						//	appendEntriesReply.Success = true
						//} else if appendEntriesArg.Term < rf.currentTerm {
						//	appendEntriesReply.Success = false
						//}
						if appendEntriesArg.Term >= rf.currentTerm {

							rf.currentTerm = appendEntriesArg.Term
							rf.state = FOLLOWER
							rf.votedFor = -1
							rf.currentVoteCount = 0
							rf.resetElectionTimeout()
						}
						appendEntriesReply.Term = rf.currentTerm
						appendEntriesReply.Success = true

						msg.waitChan <- msg

					}
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) resetElectionTimeout()  {

	rf.randomElectionTimeout = rf.electionTimeout + globalR.Intn(rf.electionTimeout)
	rf.elapsedElectionTime = 0
	fmt.Printf("Next Election timeout: %d\n",rf.randomElectionTimeout)

}
