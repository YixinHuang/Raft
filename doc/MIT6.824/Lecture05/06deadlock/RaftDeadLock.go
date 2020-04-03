package main

import (
	"log"
	"sync"
	"time"
)

type State string

const (
	Follower State = "follower"
	Candidate = "candidate"
	Leader = "leader"
)

type Raft struct {
	mu sync.Mutex
	me int
	pers []int
	currentTerm int
	votedFor int
}

func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	log.Printf("[%d] attempting an election at term %d",rf.me,rf.currentTerm)
	rf.mu.Unlock()
	for _, server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			voteGranted := rf.CallRequestVote(server)
			if !voteGranted {
				return 
			}
			// ... tally the votes
		}(server)
	}
}

func (rf *Raft) CallRequestVote(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%d] sending request vote to %d",rf.me,server)
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateID: rf.me,
	}
	var reply RequestVoteReply
	ok := rf.sendRequestVote(server,&args,&reply)
	log.Printf("[%d] finish sending request vote to %d",rf.me,server)
	if !ok {
		return false
	}

}

// s0.CallRequestVote, acquire the lock
// s0.CallRequestVote, send RPC to s1
// s1.CallRequestVote, acquire the lock
// s1.CallRequestVote, send RPC to s0
// so.Handler , s1.Handler trying to acquire lock
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs,reply *RequestVoteReply){
	log.Print("[%d] received request vote from %d",rf.me,args.CandidateID)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Print("[%d] handling request vore from %d",rf.me,args.CandidateID)
	//..
}
