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
//
//
type RaftNodeState int

const (
	follower RaftNodeState = iota
	candidate
	leader
)

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

	//Persistent state on all server
	currentTerm int                   //latest term server has seen.(initialized to 0 on first boot,increases monotonically)
	votedFor    int                   //CandidateId that reveived vore in current term (or null if none)
	logs        []map[int]interface{} // log entries;each entry contains command for state machine and term when entry was received by leader

	//Volatile state on all expectedServers
	commitIndex int //index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0,increases monotonically)

	//Volatile state on leaders:
	nextIndex  []int //for each server, index of the next log entry to send to that server(initialized to leader last log index +1)
	matchIndex []int //for each server,index of highest entry known to be replicated on server(initialized to 0,increases monotonically)

	//internal value declare here
	nodeState     RaftNodeState
	electionTimer time.Duration

	//temp values
	reStartHeartBeat bool
	//Debug setting valuse
	enableDump bool
}

func (rf *Raft) PlusCurrentTerm() {
	rf.currentTerm++
	DPrintf("[PlusCurrentTerm@raft.go][%d] rf.currentTerm:=[%d]", rf.me, rf.currentTerm)
}

func (rf *Raft) IsLeader() bool {
	return rf.nodeState == leader
}

func (rf *Raft) ChangeNodeState(currStatus RaftNodeState) {
	rf.nodeState = currStatus
	switch rf.nodeState {
	case follower:
		DPrintf("[ChangeNodeState@raft.go][%d] rf.nodeState:= follower", rf.me)
	case candidate:
		DPrintf("[ChangeNodeState@raft.go][%d] rf.nodeState:= candidate", rf.me)
	case leader:
		DPrintf("[ChangeNodeState@raft.go][%d] rf.nodeState:= leader", rf.me)
	default:
		DPrintf("[ChangeNodeState@raft.go][%d] rf.nodeState:= N/A", rf.me)
	}
}

func (rf *Raft) GetNodeState() string {
	switch rf.nodeState {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	default:
		return "N/A"
	}
}

func (rf *Raft) SetElectionTimer(enableTimer bool) {
	if enableTimer {
		rand.Seed(time.Now().Unix() + int64(rf.me))
		rf.electionTimer = time.Duration(1000-rand.Intn(300)) * time.Millisecond //config.go const RaftElectionTimeout = 1000 * time.Millisecond
		DPrintf("[SetElectionTimer@raft.go][%d] Enable rf.SetElectionTimer:= [%v]", rf.me, rf.electionTimer)
	} else {
		rf.electionTimer = -1
		DPrintf("[SetElectionTimer@raft.go][%d] Disable ElectionTimer", rf.me)
	}

}

func (rf *Raft) DumpRaft() {
	if !rf.enableDump {
		return
	}
	DPrintf("[DumpRaft@raft.go] -----------------------------Dump Raft------------------------------------------")
	DPrintf("[DumpRaft@raft.go] rf.me := [%d]", rf.me)
	DPrintf("[DumpRaft@raft.go] rf.dead := [%d]", rf.dead)
	DPrintf("[DumpRaft@raft.go] rf.currentTerm := [%d]", rf.currentTerm)
	DPrintf("[DumpRaft@raft.go] rf.votedFor := [%d]", rf.votedFor)
	DPrintf("[DumpRaft@raft.go] rf.commitIndex := [%d]", rf.commitIndex)
	DPrintf("[DumpRaft@raft.go] rf.lastApplied := [%d]", rf.lastApplied)
	DPrintf("[DumpRaft@raft.go] rf.electionTimer := [%v]", rf.electionTimer)
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

	var term int
	var isleader bool
	// Your code here (2A).

	/*if (rf.me == 0) {
		rf.ChangeNodeState(leader)
	}*/

	term = rf.currentTerm
	isleader = rf.IsLeader()

	//dump output values
	rf.DumpRaft()
	DPrintf("[GetState@raft.go][%d] GetState() Exit return term:=[%d] isLeader:=[%t] ", rf.me, term, isleader)
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
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	DPrintf("[AppendEntriesHandler@raft.go][%d][%s] RequestVote() Entry", rf.me, rf.GetNodeState())
	//set return value
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.reStartHeartBeat = true

	DPrintf("[AppendEntriesHandler@raft.go][%d][%s] RequestVote() Exit", rf.me, rf.GetNodeState())
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[sendAppendEntries@raft.go][%d] sendRequestVote() Entry", rf.me)
	args.DumpAppendEntriesArgs()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	reply.DumpAppendEntriesReply()
	DPrintf("[sendAppendEntries@raft.go][%d] sendRequestVote() Exit", rf.me)
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
	term = rf.currentTerm

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
	atomic.StoreInt32(&rf.dead, 1)
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
	DPrintf("[Make@raft.go][%d] initialized", rf.me)
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//My initialization code
	rf.nodeState = follower
	rf.reStartHeartBeat = false

	//debuging initialize values
	//rf.enableDump = false
	rf.enableDump = true

	//Modify Make() to create a background goroutine that will kick off leader election periodically by
	//sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	//This way a peer will learn who is the leader, if there is already a leader, or become the leader itself.
	//Implement the RequestVote() RPC handler so that servers will vote for one another.
	var command interface{}
	rf.Start(command)

	go rf.kickoff()

	return rf
}

func (rf *Raft) kickoff() {
	DPrintf("[kickoff@raft.go][%d]kickoff Entry", rf.me)

	hearBeatTicker := time.NewTicker(100 * time.Millisecond)
	defer hearBeatTicker.Stop()

	//const RaftElectionTimeout = 1000 * time.Millisecond
	rf.SetElectionTimer(true)
	DPrintf("[kickoff@raft.go][%d]Start a election timer:[%v]", rf.me, rf.electionTimer)
	electionticker := time.NewTicker(rf.electionTimer)
	defer electionticker.Stop()

	for {
		select {
		case <-hearBeatTicker.C:
			if rf.reStartHeartBeat {
				DPrintf("[kickoff@raft.go][%d]Break form hearBeatTicker ,bacause  reStartHeartBeat is true", rf.me)
				break
			}
			DPrintf("[kickoff@raft.go][%d]HearBeatTicker timeout", rf.me)
			//Figure 4: Server states. Followers only respond to requests from other servers.
			//If a follower receives no communication, it becomes a candidate and initiates an election.
			if rf.nodeState == follower {
				DPrintf("[kickoff@raft.go][%d]current state is follower ,so becames a candidate and initiates an election ", rf.me)
				rf.InitialElection()
				//electionticker.Stop()
				//electionticker:= time.NewTicker(rf.electionTimer)
				//defer electionticker.Stop()
			} else {
				DPrintf("[kickoff@raft.go][%d]current node state is [%s], do noting", rf.me, rf.GetNodeState())
			}

		case <-electionticker.C:
			//rf.TestInitialElection()
			DPrintf("[kickoff@raft.go][%d]Electionticker Ticker timeout", rf.me)
			rf.SendReqVote2All()
			DPrintf("[kickoff@raft.go][%d]kickoff Exit", rf.me)
			return
		}
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
	rf.SetElectionTimer(true)
	//Send RequestVote RPCs to all other servers

	DPrintf("[InitialElection@raft.go][%d] InitialElection  Exit", rf.me)
}

//Send RequestVote RPCs to all other servers
func (rf *Raft) SendReqVote2All() {
	DPrintf("[SendReqVote2All@raft.go][%d][%s] SendReqVote2All Entry  ", rf.me, rf.GetNodeState())
	if rf.nodeState != candidate {
		DPrintf("[SendReqVote2All@raft.go][%d][%s] SendReqVote2All Exit,because rf.nodeState != candidate  ", rf.me, rf.GetNodeState())
		return
	}

	votesCount := 1
	for i, curPeer := range rf.peers {
		//DPrintf("[SendReqVote2All@raft.go][%d] rf.peers[%d] := [%s]",rf.me,i,curPeer.GetName())
		if i != rf.me {
			DPrintf("[SendReqVote2All@raft.go][%d][%s] send RequestVote for  rf.peers[%d] := [%s]", rf.me, rf.GetNodeState(), i, curPeer.GetName())
			args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.GetLastLogTerm()}
			reply := RequestVoteReply{}
			DPrintf("[SendReqVote2All@raft.go][%d] Call sendRequestVote Entry", rf.me)
			rf.sendRequestVote(i, &args, &reply)
			if reply.VoteGranted {
				votesCount++
			}
			// Need using peer total number to count
			majority := len(rf.peers)/2 + 1
			DPrintf("[SendReqVote2All@raft.go][%d] majority:=[%d]", rf.me, majority)
			if votesCount >= majority {
				DPrintf("[SendReqVote2All@raft.go][%d] ChangeNodeState to leader because total votesCount:=[%d]", rf.me, votesCount)
				rf.ChangeNodeState(leader)
				rf.SendAppendEntries2All()
			}
		}
	}

	/*votesCount := 0

			//Send RequestVote RPCs to all other servers
			args  := RequestVoteArgs{rf.currentTerm,rf.me,rf.lastApplied,rf.GetLastLogTerm()}
			reply := RequestVoteReply{}

	    DPrintf("[SendReqVote2All@raft.go][%d] Call sendRequestVote Entry",rf.me )
			rf.sendRequestVote(0, &args, &reply )
			if (reply.VoteGranted){
						votesCount ++
			}

			reply2 := RequestVoteReply{}
			rf.sendRequestVote(1, &args, &reply2 )
			if (reply2.VoteGranted){
						votesCount ++
			}
			if (votesCount >= 2){
				DPrintf("[SendReqVote2All@raft.go][%d] ChangeNodeState to leader because total votesCount:=[%d]",rf.me,votesCount )
				rf.ChangeNodeState(leader)
			}
			DPrintf("[TestInitialElection@raft.go][%d] Call sendRequestVote Exit. votesCount:=[%d]",rf.me,votesCount)
	*/

	/*func (rf *Raft) InitialElection() {

			votesCount := 0

			if (rf.me == 2) {
					votesCount = 1
					//On conversion to candidate
					rf.ChangeNodeState(candidate)
					//Increment CurrentTerm
					rf.PlusCurrentTerm()
					//Vote for self
					rf.votedFor = rf.me
					//Reset election timer
					rf.SetElectionTimer(true)
					//Send RequestVote RPCs to all other servers
					args  := RequestVoteArgs{rf.currentTerm,rf.me,rf.lastApplied,rf.GetLastLogTerm()}
					reply := RequestVoteReply{}

	        DPrintf("[TestInitialElection@raft.go][%d] Call sendRequestVote Entry",rf.me )
					rf.sendRequestVote(0, &args, &reply )
					if (reply.VoteGranted){
								votesCount ++
					}

					reply2 := RequestVoteReply{}
					rf.sendRequestVote(1, &args, &reply2 )
					if (reply2.VoteGranted){
								votesCount ++
					}
					// Need using peer total number to count
					if (votesCount >= 2){
						DPrintf("[TestInitialElection@raft.go][%d] ChangeNodeState to leader because total votesCount:=[%d]",rf.me,votesCount )
						rf.ChangeNodeState(leader)
					}
					DPrintf("[TestInitialElection@raft.go][%d] Call sendRequestVote Exit. votesCount:=[%d]",rf.me,votesCount)
			} else {
				  DPrintf("[TestInitialElection@raft.go][%d] Call Nothing" ,rf.me)
			}
	}*/

}

//Send AppendEntries RPCs to all other servers
func (rf *Raft) SendAppendEntries2All() {
	DPrintf("[SendAppendEntries2All@raft.go][%d][%s] SendReqVote2All Entry  ", rf.me, rf.GetNodeState())
	/*if rf.nodeState != candidate {
		DPrintf("[SendReqVote2All@raft.go][%d][%s] SendReqVote2All Exit,because rf.nodeState != candidate  ", rf.me, rf.GetNodeState())
		return
	}*/

	for i, curPeer := range rf.peers {
		//DPrintf("[SendReqVote2All@raft.go][%d] rf.peers[%d] := [%s]",rf.me,i,curPeer.GetName())
		if i != rf.me {
			DPrintf("[SendReqVote2All@raft.go][%d][%s] send RequestVote for  rf.peers[%d] := [%s]", rf.me, rf.GetNodeState(), i, curPeer.GetName())
			args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.GetLastLogTerm(), rf.commitIndex}
			reply := AppendEntriesReply{}
			DPrintf("[SendReqVote2All@raft.go][%d] Call sendRequestVote Entry", rf.me)
			rf.sendAppendEntries(i, &args, &reply)
		}
	}
}
