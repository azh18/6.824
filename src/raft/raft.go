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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	MinRandomElectionTimeoutMs = 300
	MaxRandomElectionTimeoutMs = 700
	BatchAppendEntriesNum      = 32
)

func MinInt(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// Raft State
//
const (
	RaftStateFollower = iota
	RaftStateCandidate
	RaftStateLeader
)

//
// LogEntry: Definition of Log Entry
//
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	voteLock          sync.Mutex
	appendEntriesLock sync.Mutex
	// public vars
	state       int
	currentTerm int
	voteFor     int
	// modify state/currentTerm/voteFor need to invoke SetState
	stateMutex         sync.RWMutex
	logs               []LogEntry
	applyCh            chan ApplyMsg
	lastLeaderActiveTS time.Time

	// vars for leader
	nextIndices  []int
	matchIndices []int

	// vars for follower
	commitIndex int
	lastApplied int

	logsMutex sync.RWMutex
}

//
// SetRaftState: return true if ok else False
//
func (rf *Raft) SetRaftState(term int, state int, voteFor int) bool {
	// only accept increase term
	// voteFor change with term
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	currentTerm := rf.currentTerm
	currentVoteFor := rf.voteFor
	currentState := rf.state
	if term < currentTerm {
		return false
	}
	if term == currentTerm {
		// do not accept follower -> leader, follower -> candidate, leader -> candidate
		if currentState == RaftStateFollower && state != RaftStateFollower {
			return false
		}
		if currentState == RaftStateLeader && state == RaftStateCandidate {
			return false
		}
		// do not accept duplicated voting in one term
		if currentVoteFor != 1 && currentVoteFor != voteFor {
			return false
		}
	}
	rf.currentTerm = term
	rf.state = state
	rf.voteFor = voteFor
	return true
}

//
// GetRaftState: get state, term and voteFor
//
func (rf *Raft) GetRaftState() (state int, term int, voteFor int) {
	rf.stateMutex.RLock()
	defer rf.stateMutex.RUnlock()
	return rf.state, rf.currentTerm, rf.voteFor
}

func (rf *Raft) Logging(format string, a ...interface{}) {
	newFormat := "[%d] " + format + "\n"
	vars := make([]interface{}, len(a)+1)
	vars[0] = rf.me
	for i := 0; i < len(a); i++ {
		vars[i+1] = a[i]
	}
	DPrintf(newFormat, vars...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	if rf.state == RaftStateLeader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.voteFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.logs)
	d.Decode(&rf.voteFor)
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

func (rf *Raft) isRequestVoteArgsUpToDate(args RequestVoteArgs) bool {
	peerLogsLen := len(rf.logs)
	if peerLogsLen == 0 {
		return true
	}
	rf.logsMutex.RLock()
	defer rf.logsMutex.RUnlock()
	lastTerm := rf.logs[peerLogsLen-1].Term
	lastIndex := rf.logs[peerLogsLen-1].Index
	rf.Logging("UpToDate check from %d. self:(%d, %d), candidate:(%d, %d)", args.CandidateID, lastTerm, lastIndex, args.LastLogTerm, args.LastLogIndex)
	if args.LastLogTerm < lastTerm {
		return false
	} else if args.LastLogTerm > lastTerm {
		return true
	} else if args.LastLogIndex >= lastIndex {
		return true
	} else {
		return false
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// ensure there is only one voting at a time
	rf.voteLock.Lock()
	defer rf.voteLock.Unlock()
	rf.Logging("Recv RPC: args=%v, reply=%v", args, *reply)
	currentState, currentTerm, voteFor := rf.GetRaftState()
	newTerm := currentTerm
	newVoteFor := voteFor
	newState := currentState
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		rf.Logging("Deny RequestVote from %d. term %d < currentTerm %d", args.CandidateID, args.Term, currentTerm)
		return
	}
	if args.Term > currentTerm {
		newTerm = args.Term
		newVoteFor = -1
		newState = RaftStateFollower
	}
	isUpToDate := rf.isRequestVoteArgsUpToDate(args)
	if (newVoteFor == args.CandidateID || newVoteFor == -1) && (isUpToDate) {
		reply.Term = newTerm
		reply.VoteGranted = true
		newVoteFor = args.CandidateID
		// rf.lastLeaderActiveTS = time.Now()
		rf.Logging("Vote for %d on term %d", args.CandidateID, newTerm)
	} else {
		reply.Term = newTerm
		reply.VoteGranted = false
		if isUpToDate {
			rf.Logging("Deny RequestVote from %d. have vote for %d. ", args.CandidateID, newVoteFor)
		} else {
			rf.Logging("Deny RequestVote from %d. log entries is not up-to-date", args.CandidateID)
		}

	}
	rf.SetRaftState(newTerm, newState, newVoteFor)
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PervLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
}

//
// AppendEntries handler
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.appendEntriesLock.Lock()
	defer rf.appendEntriesLock.Unlock()
	rf.lastLeaderActiveTS = time.Now()
	leaderTerm := args.Term
	_, currentTerm, _ := rf.GetRaftState()
	if leaderTerm < currentTerm {
		// failed because leader's term is smaller than self
		reply.CurrentTerm = currentTerm
		reply.Success = false
		return
	}
	rf.SetRaftState(leaderTerm, RaftStateFollower, args.LeaderId)
	// transfer state if state is not Follower
	prevLogIndex := args.PrevLogIndex
	prevTerm := args.PervLogTerm
	if prevLogIndex >= len(rf.logs) || (prevLogIndex >= 0 && rf.logs[prevLogIndex].Term != prevTerm) {
		// failed because prevLogIndex and Term is not consistent
		reply.CurrentTerm = rf.currentTerm
		reply.Success = false
	} else {
		// execute appendEntries and update commitIndex
		if len(args.Entries) != 0 {
			// not heartbeat package, overwrite log entries
			newLogSliceLen := prevLogIndex + 1 + len(args.Entries)
			newLogSlice := make([]LogEntry, newLogSliceLen)
			for i := 0; i < prevLogIndex+1; i++ {
				newLogSlice[i] = rf.logs[i]
			}
			for i := prevLogIndex + 1; i < newLogSliceLen; i++ {
				newLogSlice[i] = args.Entries[i-prevLogIndex-1]
			}
			rf.logs = newLogSlice
		}
		reply.CurrentTerm = rf.currentTerm
		reply.Success = true
		if args.LeaderCommitIndex > rf.commitIndex {
			rf.commitIndex = MinInt(args.LeaderCommitIndex, len(rf.logs)-1)
		}
	}
	return
}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) ApplyCommittedLogs() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{i, rf.logs[i].Command, false, nil}
		rf.lastApplied++
	}
}

func (rf *Raft) HandleRequestVoteResp(electionTerm int, respChan chan *RequestVoteReply) {
	nVoteReceived := 1
	nVotes := 1
	nVoters := rf.me
	for {
		select {
		case reply := <-respChan:
			nVoteReceived++
			if reply == nil {
				break
			}
			replyTerm := reply.Term
			replyIsVoted := reply.VoteGranted
			rf.Logging("Recv: term %d isVoted %v", replyTerm, replyIsVoted)
			if replyIsVoted {
				nVotes++
			}
			if nVotes > nVoters/2 {
				rf.SetRaftState(electionTerm, RaftStateLeader, rf.me)
				rf.lastLeaderActiveTS = time.Now()
			}
			if nVoteReceived >= nVoters {
				return
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (rf *Raft) sendRequestVoteWithRespChan(requestVoteArgs RequestVoteArgs, respChan chan *RequestVoteReply, target int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(target, requestVoteArgs, &reply)
	rf.Logging("Send RequestVote on term %d, ok=%v", requestVoteArgs.Term, ok)
	if ok {
		respChan <- &reply
	} else {
		respChan <- nil
	}
}

func (rf *Raft) RaiseElectionAsNeed() {
	delta := MaxRandomElectionTimeoutMs - MinRandomElectionTimeoutMs
	randomDelta := rand.Int31n(int32(delta))
	randomTimeout := MinRandomElectionTimeoutMs + randomDelta
	if rf.state == RaftStateLeader {
		return
	}
	if int32(time.Now().Sub(rf.lastLeaderActiveTS).Nanoseconds()/1e6) > randomTimeout {
		// timeout. Raise New Election
		_, currentTerm, _ := rf.GetRaftState()
		rf.Logging("Raise Election for term %d because of timeout", currentTerm+1)
		rf.SetRaftState(currentTerm+1, RaftStateCandidate, rf.me)
		// rf.currentTerm++
		// rf.voteFor = rf.me
		// rf.state = RaftStateCandidate
		nPeers := len(rf.peers)
		// nVotes := 1

		requestVoteArgs := RequestVoteArgs{
			Term:        currentTerm + 1,
			CandidateID: rf.me,
		}
		if len(rf.logs) == 0 {
			requestVoteArgs.LastLogTerm = -1
			requestVoteArgs.LastLogIndex = -1
		} else {
			requestVoteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
			requestVoteArgs.LastLogIndex = rf.logs[len(rf.logs)-1].Index
		}
		requestVoteRespChan := make(chan *RequestVoteReply)
		go rf.HandleRequestVoteResp(currentTerm+1, requestVoteRespChan)
		for i := 0; i < nPeers; i++ {
			if i != rf.me {
				go rf.sendRequestVoteWithRespChan(requestVoteArgs, requestVoteRespChan, i)
			}
		}
	}
	return
}

func (rf *Raft) MakeAppendEntriesArgs(isHeartBeat bool, target int, currentTerm int) (args AppendEntriesArgs) {
	args = AppendEntriesArgs{
		Term:              currentTerm,
		LeaderId:          rf.me,
		LeaderCommitIndex: rf.commitIndex,
	}
	prevLogIndex := rf.nextIndices[target] - 1
	if prevLogIndex == -1 {
		args.PrevLogIndex = -1
		args.PervLogTerm = -1
	} else {
		args.PrevLogIndex = prevLogIndex
		args.PervLogTerm = rf.logs[prevLogIndex].Term
	}
	if isHeartBeat {
		args.Entries = make([]LogEntry, 0)
	} else {
		// choose correct log entries to send
		nAppendEntries := MinInt(BatchAppendEntriesNum, len(rf.logs)-rf.nextIndices[target])
		args.Entries = make([]LogEntry, nAppendEntries)
		for j := 0; j < nAppendEntries; j++ {
			args.Entries[j] = rf.logs[rf.nextIndices[target]+j]
		}
	}
	return
}

func (rf *Raft) SendAppendEntriesAndHandleResp(target int, args AppendEntriesArgs, currentTerm int) {
	// send out RPC request
	reply := AppendEntriesReply{}
	ok := rf.SendAppendEntries(target, args, &reply)
	if ok {
		if !reply.Success {
			// check term
			if reply.CurrentTerm > currentTerm {
				// have higher term server, transfer to follower
				rf.SetRaftState(reply.CurrentTerm, RaftStateFollower, -1)
				return
			}
			// prevLog not match, decrease it
			rf.nextIndices[target]--
		} else {
			// success, increase matchIdx and nextIdx
			nAppendedEntries := len(args.Entries)
			rf.matchIndices[target] = args.PrevLogIndex + nAppendedEntries
			rf.nextIndices[target] = rf.matchIndices[target] + 1
		}
	}
}

func (rf *Raft) SendAppendEntriesPackage(isHeartBeat bool) {
	_, currentTerm, _ := rf.GetRaftState()
	nPeers := len(rf.peers)
	// make args in advance to make sure send RPC with right args
	// because the response handler may modify the states of RaftPeer
	argsSlice := make([]AppendEntriesArgs, nPeers)
	for i := 0; i < nPeers; i++ {
		if i != rf.me {
			argsSlice[i] = rf.MakeAppendEntriesArgs(isHeartBeat, i, currentTerm)
		}
	}
	// send AppendEntries in parallel
	// to prevent frequent timeout
	rf.Logging("Send AppendEntries...")
	for i := 0; i < nPeers; i++ {
		if i != rf.me {
			go rf.SendAppendEntriesAndHandleResp(i, argsSlice[i], currentTerm)
		}
	}
}

func countMatchPeers(matchIndices []int, logIndex int) int {
	n := 0
	for i := range matchIndices {
		if i >= logIndex {
			n++
		}
	}
	return n
}

func (rf *Raft) CommitLogEntriesAsNeed() {
	nPeers := len(rf.peers)
	nLogEntries := len(rf.logs)
	for i := nLogEntries - 1; i > rf.commitIndex; i++ {
		if rf.logs[i].Term == rf.currentTerm {
			nMatch := countMatchPeers(rf.matchIndices, i)
			if nMatch > nPeers/2 {
				rf.commitIndex = i
				break
			}
		}
	}
}

func (rf *Raft) Run() {
	for {
		// check whether leader is active. If not, transfer to candidate state and start election.
		rf.RaiseElectionAsNeed()
		// apply committed logs

		switch rf.state {
		case RaftStateFollower:
			// do nothing
		case RaftStateCandidate:
			// do nothing
		case RaftStateLeader:
			// send heartbeat package
			rf.SendAppendEntriesPackage(true)
			// commit log entries that have been replicated on the majority
			rf.CommitLogEntriesAsNeed()
		}
		time.Sleep(time.Millisecond * 5)
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

	// Your initialization code here.
	nPeers := len(peers)
	rf.applyCh = applyCh
	rf.state = RaftStateFollower
	rf.currentTerm = -1
	rf.voteFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.nextIndices = make([]int, nPeers)
	rf.matchIndices = make([]int, nPeers)
	for i := 0; i < nPeers; i++ {
		rf.nextIndices[i] = 0
		rf.matchIndices[i] = -1
	}
	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.Logging("Run")
	go rf.Run()

	return rf
}
