package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	currentTerm     int
	votedFor        int
	status          string
	lastContact     time.Time
	electionTimeout int

	commitIdx   int
	lastApplied int
	rlog        []Rlog
	nextIndex   []int
	matchIndex  []int

	applyMsg  chan raftapi.ApplyMsg
	applyCond *sync.Cond
}

// ─────────────────────────────────────────────
// Basic helpers
// ─────────────────────────────────────────────

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == LEADER
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persist() {}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Signal()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) resetLastContact() {
	rf.lastContact = time.Now()
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = 300 + rand.Intn(200)
}

func (rf *Raft) isElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastContact) > time.Duration(rf.electionTimeout)*time.Millisecond
}

func (rf *Raft) becomeFollower(term int) {
	rf.status = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) lastLogIndexTerm() (int, int) {
	idx := len(rf.rlog) - 1
	return idx, rf.rlog[idx].Term
}

// ─────────────────────────────────────────────
// RequestVote RPC
// ─────────────────────────────────────────────

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Reject stale terms immediately
	if args.Term < rf.currentTerm {
		return
	}

	// Step down if we see a higher term — must happen before any other check
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	// Already voted for someone else this term
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// Election restriction: candidate log must be at least as up-to-date
	myLastIdx, myLastTerm := rf.lastLogIndexTerm()
	candidateUpToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIdx)

	if !candidateUpToDate {
		return
	}

	// Grant vote
	rf.votedFor = args.CandidateId
	rf.resetLastContact()
	reply.VoteGranted = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	CurrentTerm   int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.CurrentTerm = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	// 1. Reject stale leader
	if args.CurrentTerm < rf.currentTerm {
		return
	}

	// 2. Valid leader — step down if needed and reset election timer
	if args.CurrentTerm > rf.currentTerm {
		rf.becomeFollower(args.CurrentTerm)
	} else {
		// Same term: if we were a candidate, revert to follower
		rf.status = FOLLOWER
	}
	rf.resetLastContact()
	reply.CurrentTerm = rf.currentTerm

	// 3. Log consistency check: does PrevLogIndex exist?
	if args.PrevLogIndx >= len(rf.rlog) {
		// Log is too short
		reply.ConflictIndex = len(rf.rlog)
		reply.ConflictTerm = -1
		return
	}

	// 4. Does the term at PrevLogIndex match?
	if args.PrevLogIndx >= 0 && rf.rlog[args.PrevLogIndx].Term != args.PrevLogTerm {
		conflictTerm := rf.rlog[args.PrevLogIndx].Term
		reply.ConflictTerm = conflictTerm
		// Find the first index in this conflicting term
		idx := args.PrevLogIndx
		for idx > 0 && rf.rlog[idx-1].Term == conflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	// 5. Append entries, resolving conflicts
	insertIdx := args.PrevLogIndx + 1
	for i, entry := range args.Entries {
		pos := insertIdx + i
		if pos < len(rf.rlog) {
			if rf.rlog[pos].Term != entry.Term {
				// Conflict: truncate and append remainder
				rf.rlog = rf.rlog[:pos]
				rf.rlog = append(rf.rlog, args.Entries[i:]...)
				break
			}
			// Entry already matches — skip
		} else {
			// Extend the log
			rf.rlog = append(rf.rlog, args.Entries[i:]...)
			break
		}
	}

	// 6. Advance commit index
	if args.LeaderCommitIdx > rf.commitIdx {
		newCommit := args.LeaderCommitIdx
		if lastIdx := len(rf.rlog) - 1; lastIdx < newCommit {
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
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// ─────────────────────────────────────────────
// Start — called by upper layer to submit a command
// ─────────────────────────────────────────────

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != LEADER {
		return -1, rf.currentTerm, false
	}

	index := len(rf.rlog)
	term := rf.currentTerm
	rf.rlog = append(rf.rlog, Rlog{Command: command, Term: term})
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	// Wake replication goroutines
	go rf.broadcastAppendEntries(term)

	return index, term, true
}

// ─────────────────────────────────────────────
// Replication
// ─────────────────────────────────────────────

// broadcastAppendEntries sends AppendEntries to every peer in parallel.
// Called both from Start() and from the heartbeat ticker.
func (rf *Raft) broadcastAppendEntries(term int) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendEntriesToPeer(peer, term)
	}
}

// sendEntriesToPeer sends one round of AppendEntries to a single peer,
// handles the reply (including fast backtracking), and advances commitIdx.
// It loops only on conflict (backtracking); on success or step-down it returns.
func (rf *Raft) sendEntriesToPeer(peer int, term int) {
	for {
		rf.mu.Lock()

		// Stop if we are no longer the leader for this term
		if rf.status != LEADER || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		nextIdx := rf.nextIndex[peer]
		prevLogIdx := nextIdx - 1
		prevLogTerm := rf.rlog[prevLogIdx].Term

		// Copy the slice of entries to send
		entries := make([]Rlog, len(rf.rlog[nextIdx:]))
		copy(entries, rf.rlog[nextIdx:])

		args := AppendEntriesArgs{
			CurrentTerm:     rf.currentTerm,
			PrevLogIndx:     prevLogIdx,
			PrevLogTerm:     prevLogTerm,
			Entries:         entries,
			LeaderCommitIdx: rf.commitIdx,
		}

		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if !ok {
			// Network failure — give up; heartbeat will retry later
			return
		}

		rf.mu.Lock()

		// Step down if we see a higher term
		if reply.CurrentTerm > rf.currentTerm {
			rf.becomeFollower(reply.CurrentTerm)
			rf.mu.Unlock()
			return
		}

		// Guard: still leader for the same term?
		if rf.status != LEADER || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// Update matchIndex and nextIndex
			newMatch := prevLogIdx + len(entries)
			if newMatch > rf.matchIndex[peer] {
				rf.matchIndex[peer] = newMatch
				rf.nextIndex[peer] = newMatch + 1
			}

			// Try to advance commitIdx
			rf.advanceCommitIndex(term)
			rf.mu.Unlock()
			return
		}

		// ── Fast backtracking ──────────────────────────────────────────
		if reply.ConflictTerm == -1 {
			// Follower's log is too short
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			// Search our log for the last entry with ConflictTerm
			newNext := reply.ConflictIndex
			for i := len(rf.rlog) - 1; i >= 1; i-- {
				if rf.rlog[i].Term == reply.ConflictTerm {
					newNext = i + 1
					break
				}
			}
			rf.nextIndex[peer] = newNext
		}

		// Clamp to at least 1 (index 0 is the sentinel)
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}

		rf.mu.Unlock()
		// Loop back and retry with the new nextIndex
	}
}

// advanceCommitIndex checks whether a new log index can be committed
// (replicated on a majority) and advances rf.commitIdx if so.
// Must be called with rf.mu held.
func (rf *Raft) advanceCommitIndex(term int) {
	for N := len(rf.rlog) - 1; N > rf.commitIdx; N-- {
		// Only commit entries from the current term (Figure 8 safety rule)
		if rf.rlog[N].Term != term {
			continue
		}
		count := 1
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

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.lastApplied < rf.commitIdx {
			rf.lastApplied++
			idx := rf.lastApplied
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				CommandIndex: idx,
				Command:      rf.rlog[idx].Command,
			}
			rf.mu.Unlock()
			rf.applyMsg <- msg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

// ─────────────────────────────────────────────
// Election
// ─────────────────────────────────────────────

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.status = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetLastContact()
	rf.resetElectionTimeout()

	term := rf.currentTerm
	me := rf.me
	lastIdx, lastTerm := rf.lastLogIndexTerm()

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

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			// Ignore stale replies
			if rf.status != CANDIDATE || rf.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				if atomic.AddInt32(&votes, 1) >= majority {
					rf.status = LEADER
					rf.initLeaderState()
					// Immediately send heartbeats to assert leadership
					go rf.broadcastAppendEntries(rf.currentTerm)
				}
			}
		}(peer)
	}
}

// ─────────────────────────────────────────────
// Leader state initialisation
// ─────────────────────────────────────────────

func (rf *Raft) initLeaderState() {
	// Called with rf.mu held
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.rlog) // optimistic
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.rlog) - 1
}

// ─────────────────────────────────────────────
// Ticker: drives elections and heartbeats
// ─────────────────────────────────────────────

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		status := rf.status
		term := rf.currentTerm
		timedOut := time.Since(rf.lastContact) >
			time.Duration(rf.electionTimeout)*time.Millisecond
		rf.mu.Unlock()

		switch status {
		case LEADER:
			// Send heartbeats / replicate entries every 50 ms
			go rf.broadcastAppendEntries(term)

		case FOLLOWER, CANDIDATE:
			if timedOut {
				go rf.startElection()
			}
		}
	}
}

// ─────────────────────────────────────────────
// Make
// ─────────────────────────────────────────────

func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *tester.Persister,
	applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		applyMsg:    applyCh,
		currentTerm: 0,
		votedFor:    -1,
		status:      FOLLOWER,
		lastContact: time.Now(),
		commitIdx:   0,
		lastApplied: 0,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimeout()

	// Sentinel entry at index 0 (term 0, no command)
	rf.rlog = []Rlog{{Command: nil, Term: 0}}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range peers {
		rf.nextIndex[i] = 1
	}

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}
