package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
)

type Rlog struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm     int
	VotedFor        int
	status          string
	lastContact     time.Time
	electionTimeout int

	lastSnapshotIndex int
	commitIdx         int
	lastApplied       int
	Rlog              []Rlog
	nextIndex         []int
	matchIndex        []int

	pendingSnapshot      []byte
	pendingSnapshotIndex int
	pendingSnapshotTerm  int

	applyMsg  chan raftapi.ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.status == LEADER
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Rlog)
	e.Encode(rf.lastSnapshotIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Rlog
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		return
	}
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.Rlog = log

	var lastSnapShotIndex int
	if d.Decode(&lastSnapShotIndex) == nil {
		rf.lastSnapshotIndex = lastSnapShotIndex
	} else {
		rf.lastSnapshotIndex = 0
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) physicalIndex(logicalIndex int) int {
	return logicalIndex - rf.lastSnapshotIndex
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastSnapshotIndex + len(rf.Rlog) - 1
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastSnapshotIndex {
		return
	}

	if index > rf.lastLogIndex() {
		return
	}

	physIdx := rf.physicalIndex(index)
	newRlog := make([]Rlog, len(rf.Rlog[physIdx:]))
	copy(newRlog, rf.Rlog[physIdx:])
	rf.Rlog = newRlog
	rf.Rlog[0].Command = nil
	rf.lastSnapshotIndex = index

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Rlog)
	e.Encode(rf.lastSnapshotIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

func (rf *Raft) resetLastContact() {
	rf.lastContact = time.Now()
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = 300 + rand.Intn(200)
}

// becomeFollower updates term, resets votedFor, and sets status to follower.
// Must be called with rf.mu held.
func (rf *Raft) becomeFollower(term int) {
	rf.status = FOLLOWER
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.persist()
}

// lastLogIndexTerm returns the index and term of the last log entry.
// Must be called with rf.mu held.
func (rf *Raft) lastLogIndexTerm() (int, int) {
	idx := len(rf.Rlog) - 1
	return rf.lastSnapshotIndex + idx, rf.Rlog[idx].Term
}

// ─────────────────────────────────────────────
// RequestVote RPC
// ─────────────────────────────────────────────

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	defer rf.persist()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	// 1. Reject stale candidates immediately.
	if args.Term < rf.CurrentTerm {
		return
	}

	// 2. If we see a higher term, step down unconditionally BEFORE any
	//    other check (Figure 2: "If RPC request contains term T >
	//    currentTerm: set currentTerm = T, convert to follower").
	if args.Term > rf.CurrentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.CurrentTerm
	}

	// 3. Have we already voted for someone else this term?
	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
		return
	}

	// 4. Election restriction (§5.4.1): only vote for a candidate whose
	//    log is at least as up-to-date as ours.
	myLastIdx, myLastTerm := rf.lastLogIndexTerm()
	candidateUpToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIdx)

	if !candidateUpToDate {
		return
	}

	// Grant the vote.
	rf.VotedFor = args.CandidateId
	rf.resetLastContact()
	reply.VoteGranted = true
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

// ─────────────────────────────────────────────
// AppendEntries RPC
// ─────────────────────────────────────────────

type AppendEntriesArgs struct {
	CurrentTerm     int
	PrevLogIndx     int
	PrevLogTerm     int
	LeaderCommitIdx int
	Entries         []Rlog
}

type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
	// Fast backtracking fields (§5.3 optimization)
	// ConflictTerm  = -1 means the follower's log is too short
	// ConflictTerm >= 0 is the term of the conflicting entry
	// ConflictIndex is the first index of that term (or log length when too short)
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.CurrentTerm = rf.CurrentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	// 1. Reject stale leaders.
	if args.CurrentTerm < rf.CurrentTerm {
		return
	}

	// 2. Valid leader — update term / state and reset election timer.
	if args.CurrentTerm > rf.CurrentTerm {
		rf.becomeFollower(args.CurrentTerm)
	} else {
		// Same term: a candidate must revert to follower upon receiving a
		// valid AppendEntries from the current leader.
		rf.status = FOLLOWER
	}
	rf.resetLastContact()
	reply.CurrentTerm = rf.CurrentTerm

	// Handle overlap with snapshot
	if args.PrevLogIndx < rf.lastSnapshotIndex {
		if args.PrevLogIndx+len(args.Entries) <= rf.lastSnapshotIndex {
			reply.Success = true
			return
		}
		overlap := rf.lastSnapshotIndex - args.PrevLogIndx
		args.Entries = args.Entries[overlap:]
		args.PrevLogIndx = rf.lastSnapshotIndex
		args.PrevLogTerm = rf.Rlog[0].Term
	}

	// 3. Does PrevLogIndex exist in our log?
	if args.PrevLogIndx > rf.lastLogIndex() {
		// Log is too short.
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	// 4. Does the term at PrevLogIndex match?
	prevLogPhysIdx := rf.physicalIndex(args.PrevLogIndx)
	if args.PrevLogIndx >= rf.lastSnapshotIndex && rf.Rlog[prevLogPhysIdx].Term != args.PrevLogTerm {
		conflictTerm := rf.Rlog[prevLogPhysIdx].Term
		reply.ConflictTerm = conflictTerm
		// Walk back to find the first index in this conflicting term.
		idx := args.PrevLogIndx
		for idx > rf.lastSnapshotIndex && rf.Rlog[rf.physicalIndex(idx-1)].Term == conflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	// 5. Append new entries, handling conflicts.
	insertIdx := args.PrevLogIndx + 1
	for i, entry := range args.Entries {
		pos := insertIdx + i
		posPhys := rf.physicalIndex(pos)
		if pos <= rf.lastLogIndex() {
			if rf.Rlog[posPhys].Term != entry.Term {
				// Conflict: truncate the log here and append the rest.
				rf.Rlog = rf.Rlog[:posPhys]
				rf.Rlog = append(rf.Rlog, args.Entries[i:]...)
				break
			}
			// Entry already present and matching — nothing to do.
		} else {
			// Beyond current log end — append remaining entries.
			rf.Rlog = append(rf.Rlog, args.Entries[i:]...)
			break
		}
	}

	// 6. Advance commit index and wake the applier.
	if args.LeaderCommitIdx > rf.commitIdx {
		newCommit := args.LeaderCommitIdx
		if lastIdx := rf.lastLogIndex(); lastIdx < newCommit {
			newCommit = lastIdx
		}
		if newCommit > rf.commitIdx {
			rf.commitIdx = newCommit
			rf.applyCond.Signal()
		}
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ─────────────────────────────────────────────
// InstallSnapshot RPC
// ─────────────────────────────────────────────

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	// 2. Step down if term > currentTerm
	if args.Term > rf.CurrentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.CurrentTerm
	} else {
		rf.status = FOLLOWER
	}
	rf.resetLastContact()

	// 3. If lastIncludedIndex is <= our own lastSnapshotIndex, this snapshot is stale.
	if args.LastIncludedIndex <= rf.lastSnapshotIndex {
		return
	}

	// 4. Trim log.
	lastLogIdx := rf.lastLogIndex()
	if args.LastIncludedIndex < lastLogIdx &&
		rf.Rlog[rf.physicalIndex(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		physIdx := rf.physicalIndex(args.LastIncludedIndex)
		newRlog := make([]Rlog, len(rf.Rlog[physIdx:]))
		copy(newRlog, rf.Rlog[physIdx:])
		rf.Rlog = newRlog
		rf.Rlog[0].Command = nil
	} else {
		rf.Rlog = []Rlog{{Command: nil, Term: args.LastIncludedTerm}}
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex

	// Update commitIdx and lastApplied if they lag behind.
	if args.LastIncludedIndex > rf.commitIdx {
		rf.commitIdx = args.LastIncludedIndex
	}

	if args.LastIncludedIndex > rf.lastApplied {
		rf.pendingSnapshot = args.Data
		rf.pendingSnapshotIndex = args.LastIncludedIndex
		rf.pendingSnapshotTerm = args.LastIncludedTerm
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Rlog)
	e.Encode(rf.lastSnapshotIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)

	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Your code here (3B).
	if rf.status != LEADER {
		return -1, rf.CurrentTerm, false
	}

	index := rf.lastLogIndex() + 1
	term := rf.CurrentTerm
	rf.Rlog = append(rf.Rlog, Rlog{Command: command, Term: term})

	// Update leader's own tracking state.
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	// Kick off replication to all peers without holding the lock.
	go rf.broadcastAppendEntries(term)

	return index, term, true
}

// ─────────────────────────────────────────────
// Replication helpers
// ─────────────────────────────────────────────

// broadcastAppendEntries sends one round of AppendEntries (heartbeat or
// replication) to every peer in parallel.  It is the single code path used
// for both heartbeats and log replication — peers with a stale log will
// receive real entries, not an artificially empty slice.
func (rf *Raft) broadcastAppendEntries(term int) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendEntriesToPeer(peer, term)
	}
}

// sendEntriesToPeer sends AppendEntries to a single peer, handles the reply,
// and loops on conflict (fast backtracking) until either success or the peer
// is up-to-date.  Network failures cause an immediate return; the next
// heartbeat will retry.
func (rf *Raft) sendSnapshotToPeer(peer int, term int) {
	rf.mu.Lock()
	if rf.status != LEADER || rf.CurrentTerm != term {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.Rlog[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.CurrentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
		return
	}

	if rf.status != LEADER || rf.CurrentTerm != term {
		return
	}

	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
}

func (rf *Raft) sendEntriesToPeer(peer int, term int) {
	for {
		rf.mu.Lock()

		// Bail out if we are no longer the leader for this term.
		if rf.status != LEADER || rf.CurrentTerm != term {
			rf.mu.Unlock()
			return
		}

		nextIdx := rf.nextIndex[peer]
		if nextIdx <= rf.lastSnapshotIndex {
			go rf.sendSnapshotToPeer(peer, term)
			rf.mu.Unlock()
			return
		}

		prevLogIdx := nextIdx - 1
		prevLogPhysIdx := rf.physicalIndex(prevLogIdx)
		prevLogTerm := rf.Rlog[prevLogPhysIdx].Term

		// Always send real entries from nextIndex onward (never force-empty).
		nextPhysIdx := rf.physicalIndex(nextIdx)
		entries := make([]Rlog, len(rf.Rlog[nextPhysIdx:]))
		copy(entries, rf.Rlog[nextPhysIdx:])

		args := AppendEntriesArgs{
			CurrentTerm:     rf.CurrentTerm,
			PrevLogIndx:     prevLogIdx,
			PrevLogTerm:     prevLogTerm,
			Entries:         entries,
			LeaderCommitIdx: rf.commitIdx,
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if !ok {
			// Network failure — give up; the next heartbeat will retry.
			return
		}

		rf.mu.Lock()

		// Step down if we discover a higher term.
		if reply.CurrentTerm > rf.CurrentTerm {
			rf.becomeFollower(reply.CurrentTerm)
			rf.persist()
			rf.mu.Unlock()
			return
		}

		// Stale reply (we moved to a new term or lost leadership).
		if rf.status != LEADER || rf.CurrentTerm != term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// Advance matchIndex / nextIndex (guard against regression from
			// out-of-order replies).
			newMatch := prevLogIdx + len(entries)
			if newMatch > rf.matchIndex[peer] {
				rf.matchIndex[peer] = newMatch
				rf.nextIndex[peer] = newMatch + 1
			}
			// See if we can now commit more entries.
			rf.advanceCommitIndex(term)
			rf.persist()
			rf.mu.Unlock()
			return
		}

		// ── Fast backtracking (§5.3 optimization) ────────────────────────
		// The follower told us where the conflict is; jump back an entire
		// term at a time instead of decrementing by one.
		if reply.ConflictTerm == -1 {
			// Follower's log is shorter than PrevLogIndex.
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			// Search our own log for the last entry in ConflictTerm.
			// If we have it, start just after it; otherwise use the
			// follower's ConflictIndex as the hint.
			newNext := reply.ConflictIndex
			lastLogIdx := rf.lastLogIndex()
			for i := lastLogIdx; i > rf.lastSnapshotIndex; i-- {
				if rf.Rlog[rf.physicalIndex(i)].Term == reply.ConflictTerm {
					newNext = i + 1
					break
				}
			}
			rf.nextIndex[peer] = newNext
		}

		// Never go below 1 (since index 0 is the sentinel/snapshot index).
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
		rf.persist()
		rf.mu.Unlock()
		// Loop back and retry with the corrected nextIndex.
	}
}

// advanceCommitIndex checks whether any log index N > commitIdx has been
// replicated on a majority and, if so, advances commitIdx.
// IMPORTANT: only entries from the *current* term may be directly committed
// (Figure 8 / §5.4.2 safety rule).
// Must be called with rf.mu held.
func (rf *Raft) advanceCommitIndex(term int) {
	lastLogIdx := rf.lastLogIndex()
	for N := lastLogIdx; N > rf.commitIdx; N-- {
		physIdx := rf.physicalIndex(N)
		if physIdx < 0 || physIdx >= len(rf.Rlog) {
			continue
		}
		// Only commit entries stamped with the current term.
		if rf.Rlog[physIdx].Term != term {
			continue
		}
		count := 1 // leader always counts itself
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIdx = N
			rf.applyCond.Signal()
			break
		}
	}
}

// ─────────────────────────────────────────────
// Apply goroutine
// ─────────────────────────────────────────────

// applier runs in its own goroutine and sends committed log entries to the
// service layer via applyCh.  It uses a condition variable so it wakes
// immediately when commitIdx advances rather than polling.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.pendingSnapshotIndex > rf.lastApplied {
			snap := rf.pendingSnapshot
			snapIdx := rf.pendingSnapshotIndex
			snapTerm := rf.pendingSnapshotTerm
			rf.pendingSnapshot = nil

			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snap,
				SnapshotTerm:  snapTerm,
				SnapshotIndex: snapIdx,
			}
			rf.lastApplied = snapIdx
			rf.mu.Unlock()
			rf.applyMsg <- msg
			rf.mu.Lock()
		} else if rf.lastApplied < rf.commitIdx {
			rf.lastApplied++
			idx := rf.lastApplied
			physIdx := rf.physicalIndex(idx)
			if physIdx >= 0 && physIdx < len(rf.Rlog) {
				msg := raftapi.ApplyMsg{
					CommandValid: true,
					CommandIndex: idx,
					Command:      rf.Rlog[physIdx].Command,
				}
				rf.mu.Unlock()
				rf.applyMsg <- msg
				rf.mu.Lock()
			}
		} else {
			// Nothing to apply right now — sleep until signalled.
			rf.applyCond.Wait()
		}
	}
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
	// Wake the applier so it can notice it has been killed.
	rf.applyCond.Signal()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ─────────────────────────────────────────────
// Election
// ─────────────────────────────────────────────

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.status = CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.resetLastContact()
	rf.resetElectionTimeout()

	term := rf.CurrentTerm
	me := rf.me
	lastIdx, lastTerm := rf.lastLogIndexTerm()
	rf.persist()
	rf.mu.Unlock()

	votes := int32(1)
	majority := int32(len(rf.peers)/2 + 1)

	for peer := range rf.peers {
		if peer == me {
			continue
		}

		go func(peer int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := RequestVoteReply{}

			if !rf.sendRequestVote(peer, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Step down if we see a higher term.
			if reply.Term > rf.CurrentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			// Ignore stale replies (our term or role changed).
			if rf.status != CANDIDATE || rf.CurrentTerm != term {
				return
			}

			if reply.VoteGranted {
				if atomic.AddInt32(&votes, 1) >= majority {
					rf.status = LEADER
					rf.initLeaderState()
					// Assert leadership immediately with a broadcast.
					go rf.broadcastAppendEntries(rf.CurrentTerm)
				}
			}
		}(peer)
	}
}

// initLeaderState resets per-leader volatile state (nextIndex, matchIndex).
// Must be called with rf.mu held.
func (rf *Raft) initLeaderState() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIdx := rf.lastLogIndex()
	for i := range rf.peers {
		// Optimistic: assume follower is up-to-date.
		rf.nextIndex[i] = lastLogIdx + 1
		rf.matchIndex[i] = 0
	}
	// Leader has already replicated its entire log to itself.
	rf.matchIndex[rf.me] = lastLogIdx
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// Sleep for a fixed heartbeat interval then decide what to do. (if the election is already taking place this helps in waiting for that to end)
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		status := rf.status
		term := rf.CurrentTerm
		timedOut := time.Since(rf.lastContact) >
			time.Duration(rf.electionTimeout)*time.Millisecond
		rf.mu.Unlock()

		switch status {
		case LEADER:
			// Send heartbeats / replicate any pending entries.
			go rf.broadcastAppendEntries(term)

		case FOLLOWER, CANDIDATE:
			if timedOut {
				go rf.startElection()
			}
		}
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
	rf.applyMsg = applyCh
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.status = FOLLOWER
	rf.lastContact = time.Now()
	rf.commitIdx = 0
	rf.lastApplied = 0
	rf.lastSnapshotIndex = 0

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimeout()

	// Index-0 sentinel entry (term 0, nil command).  Keeps all real log
	// indices ≥ 1 and makes the very first AppendEntries trivially valid
	// with PrevLogIndex=0 / PrevLogTerm=0.
	rf.Rlog = []Rlog{{Command: nil, Term: 0}}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range peers {
		rf.nextIndex[i] = 1 // first real entry will be at index 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.lastSnapshotIndex > 0 {
		rf.commitIdx = rf.lastSnapshotIndex
		rf.lastApplied = rf.lastSnapshotIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
