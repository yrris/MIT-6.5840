package raft

// Raft consensus, following Figure 2 of the extended paper. Covers
// 3A (leader election), 3B (log replication), 3C (persistence) and
// 3D (snapshots).

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	heartbeatInterval = 100 * time.Millisecond
	electionMin       = 300 * time.Millisecond
	electionMax       = 600 * time.Millisecond
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	// persistent state on all servers (Figure 2 + 3D additions)
	currentTerm       int
	votedFor          int
	log               []LogEntry // log[0] is a dummy holding the snapshot boundary
	lastIncludedIndex int
	lastIncludedTerm  int

	// volatile state on all servers
	commitIndex      int
	lastApplied      int
	role             Role
	electionDeadline time.Time

	// volatile leader state
	nextIndex  []int
	matchIndex []int

	// apply pipeline
	applyCh         chan raftapi.ApplyMsg
	applyCond       *sync.Cond
	snapshotPending bool
	pendingSnap     []byte
}

// ---------- index helpers (logical -> slice translation) ----------

func (rf *Raft) lastLogIndex() int { return rf.lastIncludedIndex + len(rf.log) - 1 }

func (rf *Raft) termAt(idx int) int {
	return rf.log[idx-rf.lastIncludedIndex].Term
}

func (rf *Raft) entryAt(idx int) LogEntry {
	return rf.log[idx-rf.lastIncludedIndex]
}

// ---------- public API ----------

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	idx := rf.lastLogIndex()
	rf.matchIndex[rf.me] = idx
	rf.nextIndex[rf.me] = idx + 1
	go rf.broadcastAppendEntries()
	return idx, rf.currentTerm, true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.lastLogIndex() {
		return
	}
	sliceIdx := index - rf.lastIncludedIndex
	newLog := make([]LogEntry, 0, len(rf.log)-sliceIdx)
	newLog = append(newLog, LogEntry{Term: rf.log[sliceIdx].Term})
	newLog = append(newLog, rf.log[sliceIdx+1:]...)
	rf.lastIncludedTerm = rf.log[sliceIdx].Term
	rf.lastIncludedIndex = index
	rf.log = newLog
	rf.persistWithSnapshot(snapshot)
}

// ---------- persistence ----------

func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.Save(rf.encodeState(), snapshot)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, voted int
	var log []LogEntry
	var lii, lit int
	if d.Decode(&term) != nil ||
		d.Decode(&voted) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lii) != nil ||
		d.Decode(&lit) != nil {
		return
	}
	rf.currentTerm = term
	rf.votedFor = voted
	rf.log = log
	rf.lastIncludedIndex = lii
	rf.lastIncludedTerm = lit
	rf.commitIndex = lii
	rf.lastApplied = lii
}

// ---------- role transitions ----------

func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
}

func (rf *Raft) resetElectionTimer() {
	d := electionMin + time.Duration(rand.Int63n(int64(electionMax-electionMin)))
	rf.electionDeadline = time.Now().Add(d)
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	last := rf.lastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = last + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = last
	go rf.broadcastAppendEntries()
}

// ---------- RequestVote ----------

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

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	upToDate := func() bool {
		myLastIdx := rf.lastLogIndex()
		myLastTerm := rf.termAt(myLastIdx)
		if args.LastLogTerm != myLastTerm {
			return args.LastLogTerm > myLastTerm
		}
		return args.LastLogIndex >= myLastIdx
	}()

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.resetElectionTimer()
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.termAt(rf.lastLogIndex()),
	}
	term := rf.currentTerm
	votes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			reply := &RequestVoteReply{}
			if !rf.peers[p].Call("Raft.RequestVote", args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != term || rf.role != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

// ---------- AppendEntries ----------

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term of conflicting entry (-1 if none)
	XIndex  int // first index in follower's log with XTerm
	XLen    int // follower's log length (lastLogIndex+1)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.lastLogIndex() + 1

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}
	rf.role = Follower
	rf.resetElectionTimer()

	// PrevLogIndex predates our snapshot: ask the leader to skip ahead.
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.XLen = rf.lastIncludedIndex + 1
		return
	}

	// PrevLogIndex past our log end: tell leader our true length.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	// PrevLogIndex sits in our log; check term match.
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.XTerm = rf.termAt(args.PrevLogIndex)
		idx := args.PrevLogIndex
		for idx > rf.lastIncludedIndex+1 && rf.termAt(idx-1) == reply.XTerm {
			idx--
		}
		reply.XIndex = idx
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	// Sync entries: keep matching prefix, replace from first conflict.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastLogIndex() {
			if rf.termAt(idx) == entry.Term {
				continue
			}
			rf.log = rf.log[:idx-rf.lastIncludedIndex]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}
	rf.persist()
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if last := rf.lastLogIndex(); newCommit > last {
			newCommit = last
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateTo(peer)
	}
}

func (rf *Raft) replicateTo(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer] <= rf.lastIncludedIndex {
		rf.sendInstallSnapshot(peer)
		return
	}

	prevIdx := rf.nextIndex[peer] - 1
	if prevIdx < rf.lastIncludedIndex {
		prevIdx = rf.lastIncludedIndex
	}
	prevTerm := rf.termAt(prevIdx)
	entries := make([]LogEntry, rf.lastLogIndex()-prevIdx)
	copy(entries, rf.log[prevIdx+1-rf.lastIncludedIndex:])
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !rf.peers[peer].Call("Raft.AppendEntries", args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != term || rf.role != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
			rf.nextIndex[peer] = newMatch + 1
			rf.maybeAdvanceCommit()
		}
		return
	}

	// Backtrack via XTerm/XIndex/XLen.
	if reply.XTerm == -1 {
		rf.nextIndex[peer] = reply.XLen
	} else {
		idx := -1
		for i := rf.lastLogIndex(); i > rf.lastIncludedIndex; i-- {
			if rf.termAt(i) == reply.XTerm {
				idx = i
				break
			}
		}
		if idx >= 0 {
			rf.nextIndex[peer] = idx + 1
		} else {
			rf.nextIndex[peer] = reply.XIndex
		}
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

func (rf *Raft) maybeAdvanceCommit() {
	n := len(rf.peers)
	matches := make([]int, n)
	for i := range rf.peers {
		matches[i] = rf.matchIndex[i]
	}
	matches[rf.me] = rf.lastLogIndex()
	sort.Ints(matches)
	majority := n/2 + 1
	N := matches[n-majority]
	if N > rf.commitIndex && N > rf.lastIncludedIndex && rf.termAt(N) == rf.currentTerm {
		rf.commitIndex = N
		rf.applyCond.Broadcast()
	}
}

// ---------- InstallSnapshot ----------

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
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}
	rf.role = Follower
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// Preserve any local entries that match the snapshot's last entry; otherwise drop the lot.
	newLog := []LogEntry{{Term: args.LastIncludedTerm}}
	if args.LastIncludedIndex < rf.lastLogIndex() &&
		rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		startSlice := args.LastIncludedIndex - rf.lastIncludedIndex + 1
		newLog = append(newLog, rf.log[startSlice:]...)
	}
	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	rf.persistWithSnapshot(args.Data)

	rf.snapshotPending = true
	rf.pendingSnap = args.Data
	rf.applyCond.Broadcast()
}

// caller holds rf.mu
func (rf *Raft) sendInstallSnapshot(peer int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	if !rf.peers[peer].Call("Raft.InstallSnapshot", args, reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != term || rf.role != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	rf.maybeAdvanceCommit()
}

// ---------- background loops ----------

func (rf *Raft) ticker() {
	for !rf.killed() {
		ms := 30 + rand.Int63n(40)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.role != Leader && time.Now().After(rf.electionDeadline) {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.role == Leader
		rf.mu.Unlock()
		if isLeader {
			rf.broadcastAppendEntries()
		}
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.snapshotPending {
			snap := rf.pendingSnap
			sIdx := rf.lastIncludedIndex
			sTerm := rf.lastIncludedTerm
			rf.snapshotPending = false
			rf.pendingSnap = nil
			rf.mu.Unlock()
			rf.applyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snap,
				SnapshotTerm:  sTerm,
				SnapshotIndex: sIdx,
			}
			rf.mu.Lock()
			if rf.lastApplied < sIdx {
				rf.lastApplied = sIdx
			}
			continue
		}
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
			continue
		}
		if rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
			continue
		}
		idx := rf.lastApplied + 1
		cmd := rf.entryAt(idx).Command
		rf.lastApplied = idx
		rf.mu.Unlock()
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: idx,
		}
		rf.mu.Lock()
	}
}

// ---------- lifecycle ----------

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,
		votedFor:  -1,
		role:      Follower,
		log:       []LogEntry{{Term: 0}},
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionTimer()

	go rf.ticker()
	go rf.heartbeatLoop()
	go rf.applier()
	return rf
}
