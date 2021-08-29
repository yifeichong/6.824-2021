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
	"log"
	"time"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	_ = iota
	FOLLOWER
	CANDIDATE
	LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill() (int32 because of using atomic)

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	applyCh     chan ApplyMsg

	// Log stat
	commitIndex int
	lastApplied int

	// Leaders state (reset after election)
	nextIndexes  []int // index of the next log entry to send
	matchIndexes []int // highest log entry index commited

	state    int // follower, condidate or leader
	leaderID int

	// Timeouts in ms
	heartsbeatsTimeout int64
	minElectionTimeout int32
	maxElectionTimeout int32
	timerCanceled      chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term := rf.currentTerm
	isleader := rf.state == LEADER
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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

	// log.Println("VOTE_START", "ME", rf.me)

	rf.resetElectionTimer()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.state == CANDIDATE) {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.leaderID = -1
	}
	recieverLastLogIndex := len(rf.log)
	var recieverLastLogTerm int
	if recieverLastLogIndex > 0 {
		recieverLastLogTerm = rf.log[recieverLastLogIndex-1].Term
	}
	logUpToDate := args.LastLogIndex >= recieverLastLogIndex && args.LastLogTerm <= recieverLastLogTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && logUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
	log.Println(
		"VOTE_END",
		"ME", rf.me,
		"CANDIDATE", args.CandidateID,
		"VOTE", rf.votedFor,
		"REPLY_VOTE", reply.VoteGranted,
		"TERM", args.Term,
		"CURR_TERM", rf.currentTerm,
		"LOG UP TO DATE", logUpToDate,
	)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// "Custom" variables
	SelfID                int
	LeaderID              int
	LastLogIndex          int
	PrevValidLogTerm      int
	PrevValidLogTermIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.resetElectionTimer()
	rf.mu.Lock()
	defer func() {
		// Apply all commands between leaderCommit and commitIndex
		if args.LeaderCommit > rf.commitIndex {
			commitIndex := args.LeaderCommit
			lastNewEntryIndex := len(rf.log)
			if lastNewEntryIndex < args.LeaderCommit {
				commitIndex = lastNewEntryIndex
			}
			rf.commitIndex = commitIndex
		}
		rf.mu.Unlock()
		rf.applyChanges()
	}()

	log.Println(
		"APND_REQ",
		"FOLLOWER_LOG", rf.log,
		"ME", rf.me,
		"STATE", rf.state,
		"TERM", rf.currentTerm,
		"LEADER", rf.leaderID,
		"COMMIT_INDEX", rf.commitIndex,
		"LAST_APPLIED", rf.lastApplied,
		"INPUT_TERM", args.Term,
		"INPUT_LEADER", args.LeaderID,
		"LEADER_COMMIT", args.LeaderCommit,
		"PREV_LOG_INDEX", args.PrevLogIndex,
		"PREV_LOG_TERM", args.PrevLogTerm,
		"ENTRIES", args.Entries,
	)

	reply.SelfID = rf.me
	if rf.currentTerm < args.Term || rf.state == CANDIDATE {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.leaderID = args.LeaderID
	}
	reply.Term = rf.currentTerm
	reply.LeaderID = rf.leaderID

	// Can't trust the leader with the lower term
	if rf.currentTerm > args.Term && args.LeaderID != rf.leaderID {
		return
	}
	rf.leaderID = args.LeaderID
	// Skip further logic if here is a simple heartsbeat
	if args.PrevLogIndex == -1 || args.PrevLogTerm == -1 {
		return
	}

	// 2B
	lastLogIndex := len(rf.log)
	lastLogTerm := rf.currentTerm
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	if args.PrevLogIndex > lastLogIndex {
		reply.PrevValidLogTerm = lastLogTerm
		reply.PrevValidLogTermIndex = 1
		for idx := lastLogIndex; idx > 0; idx-- {
			logTerm := rf.log[idx-1].Term
			if logTerm < lastLogTerm {
				reply.PrevValidLogTermIndex = lastLogIndex - idx
				break
			}
		}
	} else {
		prevLogTerm := rf.currentTerm
		if args.PrevLogIndex > 0 {
			prevLogTerm = rf.log[args.PrevLogIndex-1].Term
		}

		if prevLogTerm != args.PrevLogTerm {
			reply.PrevValidLogTerm = prevLogTerm
			reply.PrevValidLogTermIndex = 1
			for idx := args.PrevLogIndex; idx > 0; idx-- {
				logTerm := rf.log[idx-1].Term
				if logTerm < prevLogTerm {
					rf.log = rf.log[:idx]
					break
				}
			}
		} else {
			// If everything is good - add the new entries to the log
			for _, entry := range args.Entries {
				rf.log = append(rf.log, entry)
			}
			reply.Success = true
		}
	}
	reply.LastLogIndex = len(rf.log)
}

func (rf *Raft) applyChanges() {
	rf.mu.RLock()
	if rf.commitIndex <= rf.lastApplied {
		rf.mu.RUnlock()
		return
	}

	applyMsgs := make([]ApplyMsg, rf.commitIndex-rf.lastApplied)
	for i := 0; i < len(applyMsgs); i++ {
		idx := i + rf.lastApplied + 1
		applyMsgs[i] = ApplyMsg{
			CommandValid: true,
			Command:      rf.log[idx-1].Command,
			CommandIndex: idx,
		}
	}
	rf.mu.RUnlock()

	for _, msg := range applyMsgs {
		rf.mu.Lock()
		rf.lastApplied++
		rf.mu.Unlock()
		rf.applyCh <- msg
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) runElection() {
	// Increases the current term, votes for
	// itself and changes the node state
	rf.mu.Lock()
	rf.resetElectionTimer()
	if rf.state == LEADER {
		rf.mu.Unlock()
		return
	}
	if rf.state == FOLLOWER {
		rf.currentTerm++
	}
	rf.votedFor = rf.me
	rf.state = CANDIDATE
	currentTerm := rf.currentTerm
	nPeers := len(rf.peers)
	me := rf.me
	lastLogIndex := len(rf.log)
	var lastLogTerm int
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	rf.mu.Unlock()

	log.Println("ELECTION START", "ME", me, "TERM", currentTerm)

	votesCh := make(chan bool, nPeers-1)
	rf.resetElectionTimer()
	for srv := 0; srv < nPeers; srv++ {
		if srv == me {
			continue
		}
		go func(srv int) {
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			// log.Println("VOTE_SEND", "ME", me, "SRV", srv, "TERM", args.Term)

			ok := rf.sendRequestVote(
				srv,
				args,
				reply,
			)

			if !ok {
				return
			}
			votesCh <- reply.VoteGranted

			// log.Println(
			// 	"VOTE_REPLY",
			// 	"ME", me,
			// 	"SRV", srv,
			// 	"RESP", ok,
			// 	"TERM", reply.Term,
			// )
		}(srv)
	}

	var votes int = 1
	halfPeers := nPeers / 2
	for {
		select {
		case vote := <-votesCh:
			if vote {
				votes++
			}
			if votes > halfPeers {
				rf.mu.Lock()

				log.Println("BEFORE ELECTION END:", me, "TERM:", rf.currentTerm)

				// Check if the term and state has been already changed
				// while handling one of the incoming RequestVote RPCs
				if rf.currentTerm > currentTerm || rf.state == FOLLOWER {
					rf.mu.Unlock()
					return
				}
				rf.state = LEADER
				rf.nextIndexes = make([]int, len(rf.peers))
				initNextIndex := len(rf.log) + 1
				for srv := 0; srv < len(rf.peers); srv++ {
					if srv == me {
						continue
					}
					rf.nextIndexes[srv] = initNextIndex
				}
				rf.matchIndexes = make([]int, len(rf.peers))
				rf.mu.Unlock()

				log.Println("ELECTION END", "ME", me, "VOTES", votes, "PEERS", halfPeers, "TERM", currentTerm)

				go rf.sendHeartsbeats()
				return
			}
		case <-rf.timerCanceled:
			log.Println("ELECTION CANCELED", "ME", me, "TERM", currentTerm)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartsbeats() {
	rf.mu.RLock()
	heartsbeatsTimeout := rf.heartsbeatsTimeout
	currentTerm := rf.currentTerm
	me := rf.me
	nPeers := len(rf.peers)
	rf.mu.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(nPeers - 1)
	for srv := 0; srv < nPeers; srv++ {
		if srv == me {
			continue
		}
		go func(srv int) {
			defer wg.Done()
			for !rf.killed() {
				term, isLeader := rf.GetState()
				if term > currentTerm || !isLeader {
					break
				}
				go rf.handleHeartbeat(srv, currentTerm, me)
				time.Sleep(time.Duration(heartsbeatsTimeout) * time.Millisecond)

				// log.Println("INSIDE HEARTSBEATS SEND", "ME", me, "SRV", srv, "TERM", term, "INITIAL_TERM", currentTerm)
			}
		}(srv)
	}
	wg.Wait()
}

func (rf *Raft) handleHeartbeat(srv, currentTerm, me int) {
	rf.mu.RLock()
	if rf.state != LEADER {
		rf.mu.RUnlock()
		return
	}
	prevLogTerm := currentTerm
	entries := make([]LogEntry, 0)
	lastLogIndex := len(rf.log)
	prevLogIndex := lastLogIndex - 1
	nextIndex := rf.nextIndexes[srv]
	if lastLogIndex >= nextIndex {
		prevLogIndex = nextIndex - 1
		if prevLogIndex > 0 {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}
		entries = rf.log[nextIndex-1:]
	}
	rf.mu.RUnlock()

	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderID:     me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(srv, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		rf.applyChanges()
	}()

	log.Println(
		"APND_REPLY",
		"ME", me,
		"TERM", rf.currentTerm,
		"LEADER_LOG", rf.log,
		"TERM", reply.Term,
		"SUCCESS", reply.Success,
		"FROM", reply.SelfID,
		"PREV_VALID_TERM", reply.PrevValidLogTerm,
		"PREV_VALID_IDX", reply.PrevValidLogTermIndex,
		"LAST_LOG_IDX", reply.LastLogIndex,
	)

	// If self state has been changed in between - break
	if rf.state != LEADER || rf.currentTerm > currentTerm {
		return
	}

	// For the replies on old requests from the this leader after recovering
	if reply.Term > currentTerm && reply.LeaderID == me {
		return
	}

	if reply.Term > currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		return
	}

	// Break if we deal with just regular heartsbeat
	if prevLogIndex == -1 || prevLogTerm == -1 {
		return
	}

	// 2B
	if !reply.Success {
		var nextIndex int
		for idx := len(rf.log); idx > 0; idx-- {
			if rf.log[idx-1].Term < reply.PrevValidLogTerm {
				nextIndex = idx + reply.PrevValidLogTermIndex
				break
			}
		}
		if reply.LastLogIndex < nextIndex {
			nextIndex = reply.LastLogIndex + 1
		}
		if nextIndex > 0 {
			rf.nextIndexes[reply.SelfID] = nextIndex
		}
	} else {
		rf.nextIndexes[reply.SelfID] = reply.LastLogIndex + 1
		rf.matchIndexes[reply.SelfID] = reply.LastLogIndex

		// Calculate commitIndex
		votes := make(map[int]int)
		for srv, idx := range rf.matchIndexes {
			if srv == rf.me {
				continue
			}
			votes[idx]++
		}
		halfPeers := len(rf.peers)/2 - 1
		for N, c := range votes {
			if c > halfPeers && N > rf.commitIndex &&
				N <= len(rf.log) &&
				rf.log[N-1].Term == rf.currentTerm {
				rf.commitIndex = N
				break
			}
		}

		log.Println(
			votes,
			"APND_REPLY_APPLY", me,
			"COMMIT_INDEX", rf.commitIndex,
			"LAST_APPLIED", rf.lastApplied,
		)
	}
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
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	index = len(rf.log)

	log.Println("START; INDEX", index)

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

func (rf *Raft) getRandElectionTimeout() int32 {
	min := atomic.LoadInt32(&rf.minElectionTimeout)
	max := atomic.LoadInt32(&rf.maxElectionTimeout)
	return rand.Int31n(max-min+1) + min
}

func (rf *Raft) resetElectionTimer() {
	select {
	case rf.timerCanceled <- true:
	default:
		return
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	rf.mu.RLock()
	me := rf.me
	rf.mu.RUnlock()

	for rf.killed() == false {
		// (2A) Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		timeout := rf.getRandElectionTimeout()
		select {
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			if _, isLeader := rf.GetState(); !isLeader {
				log.Println(">>>>>>> ELECTION SCHEDULED", me, timeout)
				go rf.runElection()
			}
		case <-rf.timerCanceled:
			continue
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())
	rf.votedFor = -1
	rf.leaderID = -1
	rf.state = FOLLOWER
	rf.heartsbeatsTimeout = 150
	rf.minElectionTimeout = 350
	rf.maxElectionTimeout = 450
	rf.log = make([]LogEntry, 0)
	rf.timerCanceled = make(chan bool, 1)

	// Leaders volatile state
	rf.nextIndexes = make([]int, len(rf.peers))
	rf.matchIndexes = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
