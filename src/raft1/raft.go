package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	currentTerm     int
	votedFor        int
	electionTimeout int
	status          string
	lastContact     time.Time
	voteCount       int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = false
	if rf.status == LEADER {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.setToFollower()
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	rf.resetLastContact()

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	CurrentTerm int
}

type AppendEntriesReply struct {
	CurrentTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.CurrentTerm < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		return
	}

	if args.CurrentTerm > rf.currentTerm {
		rf.currentTerm = args.CurrentTerm
		rf.votedFor = -1
		rf.setToFollower()
	}

	// valid leader → reset timeout
	rf.resetLastContact()
	reply.CurrentTerm = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(me int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[me].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (3B).

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

func (rf *Raft) resetLastContact() {
	rf.lastContact = time.Now()
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = 300 + rand.Intn(201)
}

func (rf *Raft) isElectionTimeout() bool {
	if time.Since(rf.lastContact) > time.Duration(rf.electionTimeout)*time.Millisecond {
		return true
	}
	return false
}

func (rf *Raft) setToCandidate() {
	rf.status = CANDIDATE
}

func (rf *Raft) setToFollower() {
	rf.status = FOLLOWER
}

func (rf *Raft) setToLeader() {
	rf.status = LEADER
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// heartbeat every 100ms
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// check if curr node is leader - if yes then send heartbeat
		rf.mu.Lock()

		if rf.status == LEADER {
			// send heartbeats
			go rf.sendHeartBeats(rf.me)
			rf.mu.Unlock()
			continue
		}
		currStatus := rf.status
		rf.mu.Unlock()
		if rf.isElectionTimeout() && currStatus != LEADER {
			// start the election
			go rf.startElection(rf.me)
		}
	}
}

func (rf *Raft) startElection(me int) {
	rf.mu.Lock()

	rf.status = CANDIDATE
	rf.currentTerm++
	currentTerm := rf.currentTerm

	rf.votedFor = me
	rf.voteCount = 1
	totalPeers := len(rf.peers)

	rf.resetElectionTimeout()
	rf.resetLastContact()
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == me {
			continue
		}

		go func(idx int) {
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateId: me,
			}

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(idx, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.status = FOLLOWER
				rf.votedFor = -1
				return
			}

			if rf.status != CANDIDATE || rf.currentTerm != currentTerm {
				return
			}

			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount > totalPeers/2 {
					rf.status = LEADER
					// start heartbeats ideally here
					go rf.sendHeartBeats(rf.me)
				}
			}
		}(idx)
	}
}

func (rf *Raft) sendHeartBeats(me int) {
	for idx := range rf.peers {
		if idx == me {
			continue
		}

		go func(idx int) {
			rf.mu.Lock()
			if rf.status != LEADER {
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{rf.currentTerm}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(idx, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.CurrentTerm > rf.currentTerm {
				rf.setToFollower()
				rf.currentTerm = reply.CurrentTerm
				return
			}

			if rf.status != LEADER {
				return
			}
		}(idx)
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastContact = time.Now()
	rf.status = FOLLOWER
	rf.electionTimeout = 500 + rand.Intn(301)
	rf.voteCount = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
