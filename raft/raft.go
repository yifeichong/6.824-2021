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
	lastApplied int32

	// Leaders state (reset after election)
	nextIndexes  []int // index of the next log entry to send
	matchIndexes []int // highest log entry index commited

	state                int // follower, condidate or leader
	leaderID             int
	heartsbeatsRepliesCh chan AppendEntriesReply

	// Timeouts in ms
	heartsbeatsTimeout int32
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
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.leaderID = -1
		reply.Term = args.Term
	}
	recieverLastLogIndex := len(rf.log)
	var recieverLastLogTerm int
	if recieverLastLogIndex > 0 {
		recieverLastLogTerm = rf.log[recieverLastLogIndex-1].Term
	}
	logUpToDate := args.LastLogIndex >= recieverLastLogIndex && args.LastLogTerm >= recieverLastLogTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && logUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
	// log.Println(
	// 	"VOTE_END",
	// 	"ME", rf.me,
	// 	"CANDIDATE", args.CandidateID,
	// 	"VOTE", rf.votedFor,
	// 	"REPLY_VOTE", reply.VoteGranted,
	// 	"TERM", args.Term,
	// 	"CURR_TERM", rf.currentTerm,
	// )
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
	LastLogIndex          int
	PrevValidLogTerm      int
	PrevValidLogTermIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.resetElectionTimer()
	rf.mu.Lock()

	log.Println("APP REQ", "ME", rf.me, "STATE", rf.state, "TERM", rf.currentTerm, "NEW_TERM", args.Term, "LEADER", args.LeaderID)

	reply.SelfID = rf.me
	if rf.currentTerm < args.Term || rf.state == CANDIDATE {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		// Can't trust the leader with the lower term
		rf.mu.Unlock()
		return
	}
	rf.leaderID = args.LeaderID
	if args.PrevLogIndex < 0 {
		rf.mu.Unlock()
		return
	}

	// 2B
	lastLogIndex := len(rf.log)
	lastLogTerm := -1
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
		rf.mu.Unlock()
		return
	}
	prevLogTerm := rf.log[args.PrevLogIndex-1].Term
	if prevLogTerm != args.PrevLogTerm {
		reply.PrevValidLogTerm = prevLogTerm
		reply.PrevValidLogTermIndex = 1
		for idx := args.PrevLogIndex; idx > 0; idx-- {
			logTerm := rf.log[idx-1].Term
			if logTerm < prevLogTerm {
				rf.log = rf.log[:idx+1]
				break
			}
		}
		rf.mu.Unlock()
		return
	}

	// If everything is good - add the new entries to the log
	for _, entry := range args.Entries {
		rf.log = append(rf.log, entry)
	}
	reply.Success = true
	reply.LastLogIndex = len(rf.log)

	// Apply all commands between leaderCommit and commitIndex
	commitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		commitIndex = args.LeaderCommit
		lastNewEntryIndex := len(rf.log)
		if lastNewEntryIndex < commitIndex {
			commitIndex = lastNewEntryIndex
		}
		rf.commitIndex = commitIndex
	}
	lastApplied := int(rf.lastApplied)
	rf.mu.Unlock()

	if commitIndex > lastApplied {
		go rf.applyChanges(lastApplied, commitIndex)
	}
}

func (rf *Raft) applyChanges(lastApplied, commitIndex int) {
	applyMsgs := make([]ApplyMsg, commitIndex-lastApplied)
	rf.mu.RLock()
	for i := 0; i < len(applyMsgs); i++ {
		idx := i + lastApplied
		applyMsgs[i] = ApplyMsg{
			CommandValid: true,
			Command:      rf.log[idx-1],
			CommandIndex: idx,
		}
	}
	rf.mu.RUnlock()

	for _, msg := range applyMsgs {
		atomic.AddInt32(&rf.lastApplied, 1)
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
	rf.currentTerm++
	currentTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.state = CANDIDATE
	rf.mu.Unlock()

	// Request votes from all the peers
	rf.mu.RLock()
	nPeers := len(rf.peers)
	me := rf.me
	lastLogIndex := len(rf.log)
	var lastLogTerm int
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	rf.mu.RUnlock()

	log.Println("ELECTION START", "ME", me, "TERM", currentTerm)

	votesCh := make(chan bool, nPeers)
	rf.resetElectionTimer()
	for srv := 0; srv < nPeers; srv++ {
		if srv == me {
			votesCh <- true
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

			// log.Println("GO_SEND", "ME", me, "SRV", srv, "TERM", args.Term)

			ok := rf.sendRequestVote(
				srv,
				args,
				reply,
			)

			if !ok || args.Term != reply.Term {
				reply.VoteGranted = false
			}
			votesCh <- reply.VoteGranted

			// log.Println(
			// 	"GO_REPLY",
			// 	"ME", me,
			// 	"SRV", srv,
			// 	"RESP", ok,
			// 	"TERM", reply.Term,
			// )
		}(srv)
	}

	var votes int
	halfPeers := nPeers / 2
	for {
		select {
		case vote := <-votesCh:
			if vote {
				votes++
			}
			if votes > halfPeers {
				rf.mu.Lock()

				// log.Println("BEFORE ELECTION END:", me, "TERM:", rf.currentTerm)

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
	go rf.processHeartsbeatsReplies()
	for _, isLeader := rf.GetState(); isLeader && !rf.killed(); {
		wg.Add(nPeers - 1)
		for srv := 0; srv < nPeers; srv++ {
			if srv == me {
				continue
			}
			go func(srv int) {
				defer wg.Done()
				prevLogIndex := -1
				prevLogTerm := -1
				entries := make([]LogEntry, 0)

				rf.mu.RLock()
				lastLogIndex := len(rf.log)
				nextIndex := rf.nextIndexes[srv]
				if lastLogIndex >= nextIndex {
					matchIndex := rf.matchIndexes[srv]
					prevLogIndex = matchIndex - 1
					if prevLogIndex > 0 {
						prevLogTerm = rf.log[prevLogIndex-1].Term
						entries = rf.log[matchIndex-1 : nextIndex-1]
					}
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
				if ok {
					rf.heartsbeatsRepliesCh <- reply
				}
			}(srv)
		}
		// Heartsbeats will be sent *at least* every heartsbeatsTimeout ms
		time.Sleep(time.Duration(heartsbeatsTimeout) * time.Millisecond)
		wg.Wait()
	}
}

func (rf *Raft) handleHeartsbeatsReply(currentTerm int, reply AppendEntriesReply) {
	rf.mu.Lock()
	if reply.Term > currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.mu.Unlock()
		return
	}

	// TODO (2B):
	//   if fail:
	//     - decrement nextIndex for the server
	//   if success:
	//     - update nextIndex and matchIndex
	//     - calculate new commitIndex (see conditions in the table 2 for leaders)
	//     - apply changes locally (push to the rf.applyCh)
	if !reply.Success {
		for idx := len(rf.log); idx > 0; idx-- {
			if rf.log[idx-1].Term < reply.PrevValidLogTerm {
				rf.matchIndexes[reply.SelfID] = idx + 1
				rf.nextIndexes[reply.SelfID] = idx + reply.PrevValidLogTermIndex + 1
				break
			}
		}
		rf.mu.Unlock()
		return
	}

	rf.nextIndexes[reply.SelfID] = len(rf.log) + 1
	rf.matchIndexes[reply.SelfID] = reply.LastLogIndex

	// calculate commitIndex
	votes := make(map[int]int)
	for srv, idx := range rf.matchIndexes {
		if srv == rf.me {
			continue
		}
		votes[idx]++
	}
	halfPeers := len(rf.peers)/2 - 1
	for k, v := range votes {
		if v > halfPeers && k > rf.commitIndex &&
			rf.log[k-1].Term == rf.currentTerm {
			rf.commitIndex = k
		}
	}
	commitIndex := rf.commitIndex
	lastApplied := int(rf.lastApplied)
	rf.mu.Unlock()

	if commitIndex > lastApplied {
		go rf.applyChanges(lastApplied, commitIndex)
	}

}

func (rf *Raft) processHeartsbeatsReplies() {
	for reply := range rf.heartsbeatsRepliesCh {
		currentTerm, isLeader := rf.GetState()
		if !isLeader {
			return
		}

		// rf.mu.RLock()
		// log.Println("APP REPL: ", rf.me, "FROM:", reply.SelfID, "TERM:", reply.Term, "CURR_TERM", currentTerm, isLeader)
		// rf.mu.RUnlock()

		go rf.handleHeartsbeatsReply(currentTerm, reply)
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
	for rf.killed() == false {
		// (2A) Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		timeout := rf.getRandElectionTimeout()
		select {
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			if _, isLeader := rf.GetState(); !isLeader {
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
	rf.minElectionTimeout = 250
	rf.maxElectionTimeout = 350
	rf.log = make([]LogEntry, 0)
	rf.timerCanceled = make(chan bool, 1)
	rf.heartsbeatsRepliesCh = make(chan AppendEntriesReply, len(rf.peers)-1)

	// Leaders volatile state
	rf.nextIndexes = make([]int, len(rf.peers))
	rf.matchIndexes = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
