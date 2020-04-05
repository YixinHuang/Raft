// Version: 0.2
// Date:2020/4/4
// Memo: Main loop change ,refer to etct's raft code
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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "../labgob"

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
//Server states. Followers only respond to requests from other servers.
//If a follower receives no communication, it becomes a candidate and initiates an election.
//A candidate that receives votes from a majority of the full cluster becomes the new leader.
//Leaders typically operate until they fail.
//
// TODO(hyx): need complete the following statues change logical
//
// SC0 start up  -> follower  //Start of day                               --done
// SC1 follower  -> candidate //times out ,starts election                 --done
// SC2 candidate -> leader    //receives votes from majority of Servers    --done
// SC3 candidate -> candidate //times out, new elections                   --TODO(hyx)
// SC4 candidate -> follower  //discovers current leader or new term1      --TODO(hyx)
// SC5 leader    -> follower  //discovers server with higher term          --TODO(hyx)
//
type RaftNodeState int

const (
	follower RaftNodeState = iota
	candidate
	leader
)

var rnsmap = [...]string{
	"Follower",
	"Candidate",
	"Leader",
}

func (rns RaftNodeState) String() string {
	return rnsmap[rns]
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

	//============================State==========================================
	//Persistent state on all server：
	//(Updated on stable storage before responding to RPCs)
	currentTerm int                   //latest term server has seen.(initialized to 0 on first boot,increases monotonically)
	votedFor    int                   //CandidateId that reveived vore in current term (or null if none)
	logs        []map[int]interface{} //log entries;each entry contains command for state machine and term when entry was received by leader

	//Volatile state on all Servers：
	commitIndex int //index of highest log entry known to be committed(initialized to 0, increases monotonically)
	//A log entry is committed once the leader that created the entry has replicated it on a majority of the servers

	lastApplied int //index of highest log entry applied to state machine (initialized to 0,increases monotonically)
	//Rules for all servers:If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3) TBC

	//Volatile state on leaders:(Reinitialized after election)
	nextIndex  []int //for each server, index of the next log entry to send to that server(initialized to leader last log index +1)
	matchIndex []int //for each server,index of highest entry known to be replicated on server(initialized to 0,increases monotonically)
	//===========================================================================

	//internal value declare here
	nodeState RaftNodeState

	//Raft’s RPCs typically require the recipient to persist information to stable storage,
	//so the broadcast time may range from 0.5ms to 20ms, depending on storage technology.
	//As a result, the election timeout is likely to be somewhere between 10ms and 500ms.
	//broadcastTime ≪ electionTimeout ≪ MTBF
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	//temp values
	reStartHeartBeat bool

	leaderId int //the leader id
	tick     func()
	//Debug setting valuse
	enableDump bool
}

func (rf *Raft) StartOfDay() {
	DPrintf("[StartOfDay@raft.go][%d] StartOfDay Start", rf.me)

	rf.currentTerm = 0 //initialized to 0 on first boot,increases monotonically
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderId = -1

	//My initialization code
	rf.ChangeNodeState(follower)
	rf.reStartHeartBeat = false

	//debuging initialize values
	//rf.enableDump = false
	rf.enableDump = true

	DPrintf("[StartOfDay@raft.go][%d] StartOfDay Exit", rf.me)
}

func (rf *Raft) PlusCurrentTerm() {
	rf.currentTerm++
	DPrintf("[PlusCurrentTerm@raft.go][%d] rf.currentTerm:=[%d]", rf.me, rf.currentTerm)
}

func (rf *Raft) IsLeader() bool {
	return rf.nodeState == leader
}

func (rf *Raft) ChangeNodeState(currStatus RaftNodeState) {
	// SC0 start up  -> follower  //Start of day                               --done
	// SC1 follower  -> candidate //times out ,starts election                 --done
	// SC2 candidate -> leader    //receives votes from majority of Servers    --done
	// SC3 candidate -> candidate //times out, new elections                   --TODO(hyx)
	// SC4 candidate -> follower  //discovers current leader or new term1      --TODO(hyx) half
	// SC5 leader    -> follower  //discovers server with higher term          --TODO(hyx)
	switch rf.nodeState {
	case follower:
		switch currStatus {
		case candidate:
			DPrintf("[ChangeNodeState@raft.go][%d]SC1 ChangeNodeState from follower to candidate ,times out starts election", rf.me)
		case follower:
			DPrintf("[ChangeNodeState@raft.go][%d]SC0 ChangeNodeState from init to follower ,start of day", rf.me)
		default:
			DPrintf("[ChangeNodeState@raft.go][%d]XXX ChangeNodeState from follower to %v", rf.me, currStatus)
		}
		rf.nodeState = currStatus
	case candidate:
		switch currStatus {
		case leader:
			DPrintf("[ChangeNodeState@raft.go][%d]SC2 ChangeNodeState from candidate to leader,receives votes from majority of Servers", rf.me)
		case candidate:
			DPrintf("[ChangeNodeState@raft.go][%d]SC3 ChangeNodeState from candidate to candidate ,times out starts election", rf.me)
		case follower:
			DPrintf("[ChangeNodeState@raft.go][%d]SC4 ChangeNodeState from candidate to follower ,discovers current leader or new term1 ", rf.me)
		default:
			DPrintf("[ChangeNodeState@raft.go][%d]XXX ChangeNodeState from candidate to %v", rf.me, currStatus)
		}
		rf.nodeState = currStatus
	case leader:
		if currStatus == follower {
			DPrintf("[ChangeNodeState@raft.go][%d]SC5 ChangeNodeState From leader to follow ,discovers server with higher term   ", rf.me)
		} else {
			DPrintf("[ChangeNodeState@raft.go][%d]XXX ChangeNodeState from leader to %v", rf.me, currStatus)
		}
		rf.nodeState = currStatus
	default:
		DPrintf("[ChangeNodeState@raft.go][%d] rf.nodeState:= N/A", rf.me)
	}
}

func (rf *Raft) GetNodeState() string {
	return rnsmap[rf.nodeState]
}

func (rf *Raft) SetElectionTimeout(enableTimer bool) {
	if enableTimer {
		rand.Seed(time.Now().Unix() + int64(rf.me))
		rf.electionTimeout = time.Duration(1000-rand.Intn(300)) * time.Millisecond //config.go const RaftElectionTimeout = 1000 * time.Millisecond
		DPrintf("[SetElectionTimeout@raft.go][%d] Enable rf.ectionTimeout:= [%v]", rf.me, rf.electionTimeout)
	} else {
		rf.electionTimeout = -1
		DPrintf("[SetElectionTimeout@raft.go][%d] Disable electionTimeout", rf.me)
	}

}

func (rf *Raft) SetHeartBeatTimeout(enableTimer bool) {
	if enableTimer {
		rand.Seed(time.Now().Unix() + int64(rf.me))
		//rf.heartbeatTimeout = time.Duration(1000-rand.Intn(300)) * time.Millisecond //config.go const RaftElectionTimeout = 1000 * time.Millisecond
		rf.heartbeatTimeout = 100 * time.Millisecond //config.go const RaftElectionTimeout = 1000 * time.Millisecond
		DPrintf("[SetHeartBeatTimeout@raft.go][%d] Enable rf.ectionTimeout:= [%v]", rf.me, rf.electionTimeout)
	} else {
		rf.heartbeatTimeout = -1
		DPrintf("[SetHeartBeatTimeout@raft.go][%d] Disable electionTimeout", rf.me)
	}
}

func (rf *Raft) HasLeader() bool { return rf.leaderId != -1 }

func (rf *Raft) SetLeader(leaderID int) { rf.leaderId = leaderID }

func (rf *Raft) DumpRaft() {
	if !rf.enableDump {
		return
	}
	DPrintf("[DumpRaft@raft.go] -----------------------------Dump Raft------------------------------------------")
	DPrintf("[DumpRaft@raft.go] rf.me := [%d]", rf.me)
	DPrintf("[DumpRaft@raft.go] rf.dead := [%d]", rf.dead)
	DPrintf("[DumpRaft@raft.go] rf.currentTerm := [%d]", rf.currentTerm)
	DPrintf("[DumpRaft@raft.go] rf.votedFor := [%d]", rf.votedFor)
	DPrintf("[DumpRaft@raft.go] rf.leaderId := [%d]", rf.leaderId)
	DPrintf("[DumpRaft@raft.go] rf.commitIndex := [%d]", rf.commitIndex)
	DPrintf("[DumpRaft@raft.go] rf.lastApplied := [%d]", rf.lastApplied)
	DPrintf("[DumpRaft@raft.go] rf.electionTimeout := [%v]", rf.electionTimeout)
	DPrintf("[DumpRaft@raft.go] rf.heartbeatTimeout := [%v]", rf.heartbeatTimeout)
	DPrintf("[DumpRaft@raft.go] rf.reStartHeartBeat := [%t]", rf.reStartHeartBeat)

	for i, curPeer := range rf.peers {
		DPrintf("[DumpRaft@raft.go] rf.peers[%d] := [%s]", i, curPeer.GetName())
	}

	switch rf.nodeState {
	case follower:
		DPrintf("[DumpRaft@raft.go] rf.nodeState:= follower")
	case candidate:
		DPrintf("[DumpRaft@raft.go] rf.nodeState:= candidate")
	case leader:
		DPrintf("[DumpRaft@raft.go] rf.nodeState:= leader")
	default:
		DPrintf("[DumpRaft@raft.go] rf.nodeState:= N/A")

	}
	for i, nxtInd := range rf.nextIndex {
		DPrintf("[DumpRaft@raft.go] rf.nextIndex[%d] := [%d]", i, nxtInd)
	}

	for i, mchInd := range rf.matchIndex {
		DPrintf("[DumpRaft@raft.go] rf.matchIndex[%d] := [%d]", i, mchInd)
	}
	DPrintf("[DumpRaft@raft.go] ---------------------------------------------------------------------------------")
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	DPrintf("[GetState@raft.go][%d] GetState() Entry", rf.me)
	var term int
	var isleader bool

	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.IsLeader()

	//dump output values
	rf.DumpRaft()
	DPrintf("[GetState@raft.go][%d] GetState() Exit, return term:=[%d] isLeader:=[%t] ", rf.me, term, isleader)
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

//============================RequestVote RPC==================================

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term1
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log Entry
	LastLogTerm  int //term of candidate's last log entry
}

func (req *RequestVoteArgs) DumpRequestVoteArgs() {
	DPrintf("[DumpRequestVoteArgs@raft.go] req.Term := [%d] req.CandidateId :=[%d]  req.LastLogIndex :=[%d] req.LastLogTerm :=[%d]", req.Term, req.CandidateId, req.LastLogIndex, req.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm .for candidate to update itself
	VoteGranted bool //ture means candidate reveived vote
}

func (reply *RequestVoteReply) DumpRequestVoteReply() {
	DPrintf("[DumpRequestVoteReply@raft.go] reply.Term := [%d] reply.VoteGranted :=[%t]", reply.Term, reply.VoteGranted)
}

//
// example RequestVote RPC handler.
//
//-----------------------------------------------------------------------------
//Receiver implementation:
//1. Reply false if term < currentTerm (§5.1)
//2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date
//	 as receiver’s log, grant vote (§5.2, §5.4)
//-----------------------------------------------------------------------------
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[RequestVoteHandler@raft.go][%d][%s] RequestVote() Entry", rf.me, rf.GetNodeState())
	var vote bool
	vote = true

	//Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		DPrintf("[RequestVoteHandler@raft.go][%d] Reject Vote because request term:[%d] < currentTerm:[%d]", rf.me, args.Term, rf.currentTerm)
		vote = false
	}

	//If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogIndex < rf.lastApplied {
			DPrintf("[RequestVoteHandler@raft.go][%d] Reject Vote because request LastLogIndex:[%d] < lastApplied:[%d]", rf.me, args.LastLogIndex, rf.lastApplied)
			vote = false
		}
	}

	//Update current raft node value ,because note vote new leader
	if vote {
		DPrintf("[RequestVoteHandler@raft.go][%d] Update raft node value (From voteFor[%d] to args.CandidateId[%d]) using RequestVote parameter ", rf.me, rf.votedFor, args.CandidateId)
		rf.votedFor = args.CandidateId
		if rf.currentTerm != args.Term {
			DPrintf("[RequestVoteHandler@raft.go][%d] Update raft node value (From currentTerm[%d] to args.Term[%d] ) using RequestVote parameter ", rf.me, rf.currentTerm, args.Term)
			rf.currentTerm = args.Term
		}
		//change node states
		rf.ChangeNodeState(follower)

		//rf.reStartHeartBeat = false
		rf.reStartHeartBeat = true
	}

	//set return value
	reply.Term = rf.currentTerm
	reply.VoteGranted = vote
	DPrintf("[RequestVoteHandler@raft.go][%d] RequestVote() Exit", rf.me)
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
	DPrintf("[sendRequestVote@raft.go][%d] sendRequestVote() Entry", rf.me)
	args.DumpRequestVoteArgs()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	reply.DumpRequestVoteReply()
	DPrintf("[sendRequestVote@raft.go][%d] sendRequestVote() Exit", rf.me)
	return ok
}

//============================AppendEntries RPC================================
//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int //leader's term
	LeaderId     int //so follower can redirect clients
	PrevLogIndex int //index of logentry immediately preceding new ones
	PrevLogTerm  int //term of PrevLogIndex  entry
	//entries		 []int //log entries to store(empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int //leader's commitIndex
}

func (req *AppendEntriesArgs) DumpAppendEntriesArgs() {
	DPrintf("[DumpAppendEntriesArgs@raft.go] req.Term := [%d] req.LeaderId :=[%d]  req.PrevLogIndex :=[%d] req.PrevLogTerm :=[%d] req.LeaderCommit :=[%d]", req.Term, req.LeaderId, req.PrevLogIndex, req.PrevLogTerm, req.LeaderCommit)
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  //currentTerm .for leader to update itself
	Success bool //ture if follower contained entry matching prevLogIndex and prevLogTerm
}

func (reply *AppendEntriesReply) DumpAppendEntriesReply() {
	DPrintf("[DumpAppendEntriesReply@raft.go] reply.Term := [%d] reply.Success :=[%t]", reply.Term, reply.Success)
}

//
// example AppendEntries RPC handler.
//
//-----------------------------------------------------------------------------
//Receiver implementation:
//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
//-----------------------------------------------------------------------------

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	DPrintf("[AppendEntriesHandler@raft.go][%d][%s] AppendEntries() Entry", rf.me, rf.GetNodeState())
	//set return value

	rf.SetLeader(args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = true

	rf.reStartHeartBeat = true

	DPrintf("[AppendEntriesHandler@raft.go][%d][%s] AppendEntries() Exit", rf.me, rf.GetNodeState())
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[sendAppendEntries@raft.go][%d] sendAppendEntries() Entry", rf.me)
	args.DumpAppendEntriesArgs()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	reply.DumpAppendEntriesReply()
	DPrintf("[sendAppendEntries@raft.go][%d] sendAppendEntries() Exit", rf.me)
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
	isLeader := false

	// Your code here (2B).
	DPrintf("[Start@raft.go] Start() Entry")
	index = rf.commitIndex
	term = rf.currentTerm
	isLeader = rf.IsLeader()
	DPrintf("[Start@raft.go] Start() Exit. index:=[%d] term:=[%d] isLeader:=[%t]", index, term, isLeader)
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
	DPrintf("[Kill@raft.go][%d]Kill Entry", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("[Kill@raft.go][%d]Kill Exit", rf.me)
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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("[Make@raft.go][%d] StartOfDay initialized", rf.me)
	rf.StartOfDay()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//Modify Make() to create a background goroutine that will kick off leader election periodically by
	//sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	//This way a peer will learn who is the leader, if there is already a leader, or become the leader itself.
	//Implement the RequestVote() RPC handler so that servers will vote for one another.
	var command interface{}
	rf.Start(command)

	//Refre the etcd example code ,the newRaftNode() initiates a raftnode instance
	//The raftnode has a member named node (node.go),and the node had raft instance
	newRaftNode(rf)

	//kickoff is simple implementation for testing case
	//go rf.kickoff()

	return rf
}

//
// The kickoff is simple implementation, only tesing case TestInitialElection2A be passed
// TODO(hyx): Using mit suggestion to complete all testing cases in the future.
//
func (rf *Raft) kickoff() {
	DPrintf("[kickoff@raft.go][%d]kickoff Entry", rf.me)

	//The tester requires that the leader send heartbeat RPCs no more than ten times per second.
	hearBeatTicker := time.NewTicker(200 * time.Millisecond)
	defer hearBeatTicker.Stop()

	//const RaftElectionTimeout = 1000 * time.Millisecond
	rf.SetElectionTimeout(true)
	DPrintf("[kickoff@raft.go][%d]Start a election timer:[%v]", rf.me, rf.electionTimeout)
	electionticker := time.NewTicker(rf.electionTimeout)
	defer electionticker.Stop()

	for {

		select {
		case <-hearBeatTicker.C:
			DPrintf("[kickoff@raft.go][%d]HearBeatTicker timeout", rf.me)
			if rf.reStartHeartBeat {
				DPrintf("[kickoff@raft.go][%d]Break form hearBeatTicker ,bacause  reStartHeartBeat is true", rf.me)
				break
			}

			go func() {
				rf.SendAppendEntries2All()
			}()
			//Figure 4: Server states. Followers only respond to requests from other servers.
			//If a follower receives no communication, it becomes a candidate and initiates an election.
			if rf.nodeState == follower {
				DPrintf("[kickoff@raft.go][%d]current state is follower ,so becames a candidate and call initiates an election ", rf.me)
				rf.InitialElection()
				//electionticker.Stop()
				//electionticker:= time.NewTicker(rf.electionTimeout)
				//defer electionticker.Stop()
			} else {
				DPrintf("[kickoff@raft.go][%d]current node state is [%s], do noting", rf.me, rf.GetNodeState())
			}

		case <-electionticker.C:
			//rf.TestInitialElection()
			DPrintf("[kickoff@raft.go][%d]Electionticker Ticker timeout", rf.me)
			rf.SendReqVote2All()
			//DPrintf("[kickoff@raft.go][%d]kickoff Exit", rf.me)
			//return
		}

		if rf.killed() {
			DPrintf("[kickoff@raft.go][%d]kickoff Exit ,becase be killed", rf.me)
			break
		} else {
			DPrintf("[kickoff@raft.go][%d]kickoff killed is false", rf.me)
		}
		time.Sleep(5 * time.Millisecond)

	}
	DPrintf("[kickoff@raft.go][%d]kickoff Exit", rf.me)
}

func (rf *Raft) GetLastLogTerm() int {
	DPrintf("[GetLastLogTerm@raft.go][%d] GetLastLogTerm return 0 ,need implement in future", rf.me)
	return 0
}

//Rules for Servers
//Candidates (§5.2)
//——————————————————————————————————
//	• On conversion to candidate, start election:
//	• Increment currentTerm
//	• Vote for self
//	• Reset election timer
//• Send RequestVote RPCs to all other servers
//• If votes received from majority of servers: become leader
//• If AppendEntries RPC received from new leader: convert to follower
//• If election timeout elapses: start new election
func (rf *Raft) InitialElection() {
	DPrintf("[InitialElection@raft.go][%d] InitialElection  Entry", rf.me)
	//On conversion to candidate
	rf.ChangeNodeState(candidate)
	//Increment CurrentTerm
	rf.PlusCurrentTerm()
	//Vote for self
	rf.votedFor = rf.me
	//Reset election timer
	rf.SetElectionTimeout(true)
	//Send RequestVote RPCs to all other servers

	DPrintf("[InitialElection@raft.go][%d] Attempting an election at term [%d]  Entry", rf.me, rf.currentTerm)

	DPrintf("[InitialElection@raft.go][%d] InitialElection  Exit", rf.me)
}

//Send RequestVote RPCs to all other servers ,using NewCond
func (rf *Raft) SendReqVote2All() {
	DPrintf("[SendReqVote2All@raft.go][%d][%s] SendReqVote2All Entry  ", rf.me, rf.GetNodeState())
	if rf.nodeState != candidate {
		DPrintf("[SendReqVote2All@raft.go][%d][%s] SendReqVote2All Exit,because rf.nodeState != candidate  ", rf.me, rf.GetNodeState())
		return
	}

	var mu sync.Mutex
	//Use this condition variable for kinkd of coordinating
	//when a certain condition some property on that shared data
	//when that becomes true
	cond := sync.NewCond(&mu)
	//Some share data
	votesCount := 1
	finishedLoop := 1

	for i, curPeer := range rf.peers {
		//DPrintf("[SendReqVote2All@raft.go][%d] rf.peers[%d] := [%s]",rf.me,i,curPeer.GetName())
		if i == rf.me {
			continue
		}
		go func(server int) {
			DPrintf("[SendReqVote2All@raft.go][%d][%s] send RequestVote for  rf.peers[%d] := [%s]", rf.me, rf.GetNodeState(), server, curPeer.GetName())
			args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.GetLastLogTerm()}
			reply := RequestVoteReply{}
			DPrintf("[SendReqVote2All@raft.go][%d] Call sendRequestVote", rf.me)
			rf.sendRequestVote(server, &args, &reply)

			mu.Lock()
			defer mu.Unlock()

			if reply.VoteGranted {
				votesCount++
			}
			finishedLoop++
			cond.Broadcast()
		}(i)
	}
	// Need using peer total number to count
	majority := len(rf.peers)/2 + 1
	DPrintf("[SendReqVote2All@raft.go][%d] majority:=[%d]", rf.me, majority)

	mu.Lock()
	for votesCount < majority && finishedLoop != majority+1 {
		DPrintf("[SendReqVote2All@raft.go][%d] cond.Wait()  votesCount=[%d] finishedLoop=[%d]", rf.me, votesCount, finishedLoop)
		cond.Wait()
	}

	if votesCount >= majority {
		mu.Unlock()
		rf.ChangeNodeState(leader)
		DPrintf("[SendReqVote2All@raft.go][%d][%s] ChangeNodeState to leader because total votesCount:=[%d]", rf.me, rf.GetNodeState(), votesCount)
		rf.SendAppendEntries2All()
	} else {
		mu.Unlock()
		DPrintf("[SendReqVote2All@raft.go][%d][%s] Cannot change to leader because total votesCount:=[%d]", rf.me, rf.GetNodeState(), votesCount)
	}
}

//Send AppendEntries RPCs to all other servers
//Once a candidate wins an election, it becomes leader. It then sends heartbeat messages to all of the other servers to establish its authority and prevent new elections.
func (rf *Raft) SendAppendEntries2All() {
	DPrintf("[SendAppendEntries2All@raft.go][%d][%s] SendAppendEntries2All Entry  ", rf.me, rf.GetNodeState())
	if rf.nodeState != leader {
		DPrintf("[SendAppendEntries2All@raft.go][%d][%s] SendAppendEntries2All Exit,because rf.nodeState != leader  ", rf.me, rf.GetNodeState())
		return
	}

	for i, curPeer := range rf.peers {
		//DPrintf("[SendReqVote2All@raft.go][%d] rf.peers[%d] := [%s]",rf.me,i,curPeer.GetName())
		if i == rf.me {
			continue
		}

		go func(server int) {
			DPrintf("[SendAppendEntries2All@raft.go][%d][%s] send AppendEntries for  rf.peers[%d] := [%s]", rf.me, rf.GetNodeState(), server, curPeer.GetName())
			args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.GetLastLogTerm(), rf.commitIndex}
			reply := AppendEntriesReply{}
			//DPrintf("[SendAppendEntries2All@raft.go][%d] Call AppendEntries Entry", rf.me)
			rf.sendAppendEntries(server, &args, &reply)
		}(i)
	}
	DPrintf("[SendAppendEntries2All@raft.go][%d][%s] SendAppendEntries2All Exit  ", rf.me, rf.GetNodeState())
}

/*                    Rules for Servers

All Servers:
• If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
• If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

Followers (§5.2):
• Respond to RPCs from candidates and leaders
• If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate

Candidates (§5.2):
• On conversion to candidate, start election:
• Increment currentTerm
• Vote for self
• Reset election timer
• Send RequestVote RPCs to all other servers
• If votes received from majority of servers: become leader
• If AppendEntries RPC received from new leader: convert to follower
• If election timeout elapses: start new election

Leaders:
• Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
• If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
• If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
• If successful: update nextIndex and matchIndex for follower (§5.3)
• If AppendEntries fails because of log inconsistency:decrement nextIndex and retry (§5.3)
• If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).*/

//Send RequestVote RPCs to all other servers ,using WaitGroup
func (rf *Raft) SendReqVote2AllVerWaitGroup() {
	DPrintf("[SendReqVote2All@raft.go][%d][%s] SendReqVote2All Entry  ", rf.me, rf.GetNodeState())
	if rf.nodeState != candidate {
		DPrintf("[SendReqVote2All@raft.go][%d][%s] SendReqVote2All Exit,because rf.nodeState != candidate  ", rf.me, rf.GetNodeState())
		return
	}

	var wg sync.WaitGroup

	votesCount := 1
	for i, curPeer := range rf.peers {
		//DPrintf("[SendReqVote2All@raft.go][%d] rf.peers[%d] := [%s]",rf.me,i,curPeer.GetName())
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			DPrintf("[SendReqVote2All@raft.go][%d][%s] send RequestVote for  rf.peers[%d] := [%s]", rf.me, rf.GetNodeState(), server, curPeer.GetName())
			args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.GetLastLogTerm()}
			reply := RequestVoteReply{}
			DPrintf("[SendReqVote2All@raft.go][%d] Call sendRequestVote", rf.me)
			rf.sendRequestVote(server, &args, &reply)
			if reply.VoteGranted {
				votesCount++
			}
			wg.Done()
		}(i)
	}

	wg.Wait() //need update here

	// Need using peer total number to count
	majority := len(rf.peers)/2 + 1
	DPrintf("[SendReqVote2All@raft.go][%d] majority:=[%d]", rf.me, majority)

	if votesCount >= majority {
		rf.ChangeNodeState(leader)
		DPrintf("[SendReqVote2All@raft.go][%d][%s] ChangeNodeState to leader because total votesCount:=[%d]", rf.me, rf.GetNodeState(), votesCount)
		rf.SendAppendEntries2All()
	} else {
		DPrintf("[SendReqVote2All@raft.go][%d][%s] Cannot change to leader because total votesCount:=[%d]", rf.me, rf.GetNodeState(), votesCount)
	}
}
