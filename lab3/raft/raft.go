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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// follower, candidate, leader
	state int

	// channels
	applyCh       chan ApplyMsg
	winElectionCh chan struct{}
	demoteCh      chan struct{}
	voteCh        chan struct{}
	heartbeatCh   chan struct{}

	// vote
	voteCount int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (3C).
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

// send to channel but do not block
func (rf *Raft) nonblockingSend(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// assumes lock is held
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// assumes lock is held
func (rf *Raft) resetChannels() {
	rf.winElectionCh = make(chan struct{})
	rf.demoteCh = make(chan struct{})
	rf.voteCh = make(chan struct{})
	rf.heartbeatCh = make(chan struct{})
}

// assumes lock is held
func (rf *Raft) demote(term int) {
	state := rf.state
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	if state != FOLLOWER {
		rf.nonblockingSend(rf.demoteCh)
	}
}

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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.demote(args.Term)
		reply.Term = rf.currentTerm
	}

	var logUpToDate bool
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.log[lastLogIndex].Term
	if args.LastLogTerm == lastLogTerm {
		logUpToDate = args.LastLogIndex >= lastLogIndex
	} else {
		logUpToDate = args.LastLogTerm > lastLogTerm
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.nonblockingSend(rf.voteCh)
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

func (rf *Raft) requestVoteAndHandleReply(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != CANDIDATE || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.demote(args.Term)
		return
	}
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount == len(rf.peers)/2+1 {
			rf.nonblockingSend(rf.winElectionCh)
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	if rf.state != CANDIDATE {
		return
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.log[lastLogIndex].Term

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for server := range rf.peers {
		if server != rf.me {
			go func() {
				rf.requestVoteAndHandleReply(server, &args)
			}()
		}
	}
}

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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.demote(args.Term)
		reply.Term = rf.currentTerm
	}

	rf.nonblockingSend(rf.heartbeatCh)

	lastLogIndex := rf.getLastLogIndex()
	if args.PrevLogIndex > lastLogIndex {
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	argIndex, logIndex := 0, args.PrevLogIndex+1
	for argIndex < len(args.Entries) && logIndex < len(rf.log) {
		if rf.log[logIndex].Term != args.Entries[argIndex].Term {
			break
		}
		argIndex++
		logIndex++
	}
	rf.log = append(rf.log[:logIndex], args.Entries[argIndex:]...)

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		go rf.applyLog()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendEntriesAndHandleReply(server int, args *AppendEntriesArgs) bool {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return true
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return true
	}
	if reply.Term > rf.currentTerm {
		rf.demote(args.Term)
		return true
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = newMatchIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server]--
		return false
	}

	// if there exists N > commitIndex
	//                 a majority of matchIndex[i] >= N
	//                 and log[N].term == currentTerm
	// then set commit index = N
	for n := rf.getLastLogIndex(); n > rf.commitIndex; n-- {
		count := 1
		if rf.log[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLog()
			break
		}
	}
	return true
}

func (rf *Raft) getAppendEntriesArgs(server int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.LeaderCommit = rf.commitIndex
	if len(rf.log) >= rf.nextIndex[server] {
		entries := rf.log[rf.nextIndex[server]:]
		args.Entries = make([]LogEntry, len(entries))
		copy(args.Entries, entries)
	}
	return &args
}

func (rf *Raft) broadcastAppendEntries() {
	if rf.state != LEADER {
		return
	}
	for server := range rf.peers {
		if server != rf.me {
			go func() {
				for {
					args := rf.getAppendEntriesArgs(server)
					if ok := rf.appendEntriesAndHandleReply(server, args); ok {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}()
		}
	}

}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
	}
}

func (rf *Raft) startElection(fromState int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != fromState {
		return
	}

	rf.resetChannels()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1

	rf.broadcastRequestVote()
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != CANDIDATE {
		return
	}

	rf.resetChannels()
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex := rf.getLastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
	}

	rf.broadcastAppendEntries()
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, rf.currentTerm, false
	}

	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	return rf.getLastLogIndex(), term, true
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

func (rf *Raft) electionTimeout() time.Duration {
	return time.Duration(350+rand.Intn(200)) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case LEADER:
			select { // wait until either demoted or 100ms timeout
			case <-rf.demoteCh:
			case <-time.After(100 * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		case CANDIDATE:
			select { // wait until either demoted, win election, or election timeout
			case <-rf.demoteCh:
			case <-rf.winElectionCh:
				rf.becomeLeader()
			case <-time.After(rf.electionTimeout()):
				rf.startElection(state)
			}
		case FOLLOWER:
			select { // wait until either a vote happens, heartbeat received, or election timeout
			case <-rf.voteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.electionTimeout()):
				rf.startElection(state)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.voteCount = 0
	rf.log = []LogEntry{{Term: 0}}
	rf.resetChannels()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
