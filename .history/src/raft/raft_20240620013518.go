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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	IsLeader    bool
	VotedFor    int        // candidateId that received vote in current term; null if none
	Log         []LogEntry // log entries;

	CommitIndex int // index of highest log entry known to be committed
	LastApplied int // index of highest log entry applied to state machine

	NextIndex  []int // for each server, index of the next log entry to send to that server
	MatchIndex []int // for each server, index of the highest log entry known to be replicated on server

	electionTimeout     time.Duration
	lastHeardFromLeader time.Time
	state       State
}

// A Go object holding information about each log entry
type LogEntry struct {
	Command string // command for state machine
	Term    int    // term when entry was received by leader (first index is 1)
	// Indicates the term in which the log entry was created. Terms are used to distinguish entries created in different election cycles.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// mutex is unlocked when the surrounding GetState() returns
	term = rf.CurrentTerm
	isleader = rf.IsLeader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// This is what the candidate sends to the followers
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// This is what followers respond to the candidate with
	Term        int  // current term of the server that received the vote request
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// rf: the voter / follower
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock() // unlock when the surrounding function returns

	currentTerm, isLeader := rf.GetState()
	reply.Term = currentTerm
	if args.Term < currentTerm {
		// reply false if the candidate has a lower term than the follower
		reply.VoteGranted = false
		return
	} 
	if args.Term > currentTerm {
		// if the candidate has a higher term than the follower
		// update follower term
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1 // haven't voted for anyone
		rf.state = Follower
		rf.resetElectionTimeout()
		return
	}

	// If votedFor is null or candidateId
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		// If candidate’s log is at least as up-to-date as receiver’s log, grant vote
		followerLastLogIndex := len(rf.Log) - 1
		followerLastLogTerm := rf.Log[followerLastLogIndex].Term
		if args.LastLogTerm >= followerLastLogTerm ||
			(args.LastLogTerm == followerLastLogTerm && args.LastLogIndex >= followerLastLogIndex) {
			rf.VotedFor = args.CandidateId
			rf.resetElectionTimeout()
			reply.VoteGranted = true
			return
		} else {
			reply.VoteGranted = false
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so followers can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndexEntry
	Entries      []LogEntry // new log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // leader's term
	Success bool // true if follower contained entry matching prevLogIndex and
}

// AppendEntries RPC handler
// invoked by leader to replicate log entries and used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.CurrentTerm {
		// REMEMBER TO SEND BACK THE MOST RECENT TERM!
		// So that the sender (leader) knows it's outdated
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		// rf.persist()
		rf.IsLeader = false
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if len(rf.Log) <= args.PrevLogIndex || (rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// this can happen if the follower has a shorter log than the leader
		// or there's a discrepancy in terms for the same index (which can happen when tehre's network partition etc)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	for i := 0; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.Log) {
			if rf.Log[index].Term != args.Entries[i].Term {
				// delete all conflicting log and those following it
				rf.Log = rf.Log[:index]
				break
			}
		}
	}

	// 4. Append any new entries not already in the log
	rf.Log = append(rf.Log[:args.PrevLogIndex+1], args.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, len(rf.Log)-1)
	}

	rf.resetElectionTimeout()

	reply.Term = rf.CurrentTerm
	reply.Success = true

	return
}

func (rf *Raft) runElectionTimer() {
	for {
		rf.mu.Lock()
		electionTimeout := rf.electionTimeout
		rf.mu.Unlock()

		time.Sleep(electionTimeout)

		rf.mu.Lock()
		if time.Since(rf.lastHeardFromLeader) >= rf.electionTimeout {
			rf.startElection()
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) resetElectionTimeout() {
	// all raft nodes have a randomised election timeout 150ms - 300ms
	// followers: reset election timeout when receiving heartbeats
	rf.electionTimeout = time.Duration(rand.Intn(200)+300) * time.Millisecond
	rf.lastHeardFromLeader = time.Now()

}

func (rf *Raft) startElection() {
	ff.mu.Lock()
	defer rf.mu.Unlock()
	// On conversion to candidate, start election:
	// Increment currentTerm
	rf.CurrentTerm++
	// Vote for self
	rf.VotedFor = rf.me
	rf.state = Candidate
	// Reset election timer
	rf.resetElectionTimeout()
	// rf.persist()
	// Send RequestVote RPCs to all other servers
	argsLastLogIndex := len(rf.Log) - 1
	argsLastLogTerm := rf.Log[len(rf.Log)-1].Term
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: argsLastLogIndex,
		LastLogTerm:  argsLastLogTerm,
	}
	votes := 1 // vote for self
	// sending request vote to all neighbours 
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// check results in reply 
					if reply.Term > args.Term {
						// if receiving a 
						rf.CurrentTerm = reply.Term
						rf.state = Follower
						rf.VotedFor = -1 
						rf.resetElectionTimeout()
						// rf.persist()
						return
					}

					if reply.VoteGranted && rf.state == Candidate {
						votes ++
						// If votes received from majority of servers: become leader
						if votes > len(rf.peers)/2 {
							rf.state = Leader
							rf.resetElectionTimeout()
							rf.startLeader()
						}
					}

				}
			} (i) // passing i to the go func as server int
		}
	}
	
	// If AppendEntries RPC received from new leader: convert to follower


	
	// If election timeout elapses: start new election
	go func() {
		time.Sleep(rf.electionTimeout)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Candidate && time.Since(rf.lastHeardFromLeader) >= rf.electionTimeout {
			rf.startElection()
		}
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// Modify Make() to create a background goroutine that will kick off leader election
	// periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	// This way a peer will learn who is the leader,
	// if there is already a leader, or become the leader itself.
	// Implement the RequestVote() RPC handler so that servers will vote for one another.

	go func() {
		time.Sleep(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// for loop:
// every raft sleep for a randomised time from 150 ms to 300 ms.
// if it receives heartbeat, reset sleep for a randomised amount
// else if there is an election timeout at the raft, call startElection
