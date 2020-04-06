// Version: 0.1
// Date:2020/4/5
// Memo: refer to etcd raft code :https://github.com/etcd-io/etcd/tree/master/raft/raft.go

package raft

//import "time"

//The etcd new a raft instance in newRaft function, we already have one in Make function.
//So we just using parameter reference raft that create by make before
func newRaft(rf *Raft) *Raft {
	DPrintf("[newRaft@etcd.go][%d][%s] newRaft Entry  ", rf.me, rf.GetNodeState())

	rf.becomeFollower(rf.currentTerm, None)

	DPrintf("[newRaft@etcd.go][%d][%s] newRaft Exit  ", rf.me, rf.GetNodeState())
	return rf
}

func (rf *Raft) becomeFollower(term int, lead int) {
	DPrintf("[becomeFollower@etcd.go][%d][%s] becomeFollower Entry,Term:=[%d] Leader:=[%d]  ", rf.me, rf.GetNodeState(), term, lead)
	rf.reset(term)
	rf.step = stepFollower
	rf.tick = rf.tickElection
	rf.SetLeader(lead)
	rf.ChangeNodeState(follower)
	DPrintf("[becomeFollower@etcd.go][%d][%s] becomeFollower Exit  ", rf.me, rf.GetNodeState())
}

// tickElection is run by followers and candidates after rf.electionTimeout.
func (rf *Raft) tickElection() {
	//DPrintf("[tickElection@etcd.go][%d][%s] tickElection Entry  ", rf.me, rf.GetNodeState())
	rf.mu.Lock()
	rf.elapsed++
	if rf.isElectionTimeout() {
		DPrintf("[tickElection@etcd.go][%d][%s] Call Step  because election  timeout. ", rf.me, rf.GetNodeState())
		rf.SetElapsed(0)
		rf.mu.Unlock()
		rf.Step(Message{From: rf.me, Type: MsgHup})
	} else {
		rf.mu.Unlock()
	}
	//DPrintf("[tickElection@etcd.go][%d][%s] tickElection Exit  ", rf.me, rf.GetNodeState())
}

func stepFollower(rf *Raft, m Message) error {
	DPrintf("[stepFollower@etcd.go][%d][%s][%s] stepFollower Entry  ", rf.me, rf.GetNodeState(), m.Type)
	switch m.Type {
	/*case pb.MsgProp:
		if rf.lead == None {
			log.Printf("raft: %x no leader at term %d; dropping proposal", rf.id, rf.Term)
			return
		}
		m.To = rf.lead
		rf.send(m)
	case pb.MsgApp:
		rf.elapsed = 0
		rf.lead = m.From
		rf.handleAppendEntries(m)*/
	case MsgHeartbeat:
		rf.SetElapsed(0)
		rf.SetLeader(m.From)
		DPrintf("[stepFollower@etcd.go][%d][%s][%s] call handleHeartbeat  ", rf.me, rf.GetNodeState(), m.Type)
		//rf.handleHeartbeat(m)
	/*case pb.MsgSnap:
	rf.elapsed = 0
	rf.handleSnapshot(m)*/
	case MsgVote:
		DPrintf("[stepFollower@etcd.go][%d][%s][%s] call MsgVote  ", rf.me, rf.GetNodeState(), m.Type)
		/*if (rf.Vote == None || rf.Vote == m.From) && rf.raftLog.isUpToDate(m.Index, m.LogTerm) {
			rf.elapsed = 0
			log.Printf("raft: %x [logterm: %d, index: %d, vote: %x] voted for %x [logterm: %d, index: %d] at term %d",
				rf.id, rf.raftLog.lastTerm(), rf.raftLog.lastIndex(), rf.Vote, m.From, m.LogTerm, m.Index, rf.Term)
			rf.Vote = m.From
			rf.send(pb.Message{To: m.From, Type: pb.MsgVoteResp})
		} else {
			log.Printf("raft: %x [logterm: %d, index: %d, vote: %x] rejected vote from %x [logterm: %d, index: %d] at term %d",
				rf.id, rf.raftLog.lastTerm(), rf.raftLog.lastIndex(), rf.Vote, m.From, m.LogTerm, m.Index, rf.Term)
			rf.send(pb.Message{To: m.From, Type: pb.MsgVoteResp, Reject: true})
		}*/
	}

	DPrintf("[stepFollower@etcd.go][%d][%s][%s] stepFollower Exit  ", rf.me, rf.GetNodeState(), m.Type)
	return nil
}

//
//Call by Tick function with a messsage, for example :tickElection
//Part1:process base on Term
//
//Part2:process base on message type
//
func (rf *Raft) Step(m Message) error {
	DPrintf("[Step@etcd.go][%d][%s] Step Entry,Message From:[%d],Type:[%s]  ", rf.me, rf.GetNodeState(), m.From, m.Type)

	switch m.Type {
	case MsgHup:
		rf.campaign(campaignElection)
		DPrintf("[Step@etcd.go][%d][%s] Step Exit  ", rf.me, rf.GetNodeState())
		return nil
	case MsgVote:
		DPrintf("[Step@etcd.go][%d][%s] Step revceive  MsgVote ", rf.me, rf.GetNodeState())
	case MsgBeat:
		DPrintf("[Step@etcd.go][%d][%s] Step revceive  MsgBeat ", rf.me, rf.GetNodeState())
	case MsgHeartbeat:
		DPrintf("[Step@etcd.go][%d][%s] Step revceive  MsgHeartbeat ", rf.me, rf.GetNodeState())
	default:
		DPrintf("[Step@etcd.go][%d][%s] Default Step call raft  ", rf.me, rf.GetNodeState())
		err := rf.step(rf, m)
		if err != nil {
			return err
		}
	}

	/*switch {
	case m.Term == 0:
		// local message
	case m.Term > rf.Term:
		lead := m.From
		if m.Type == pb.MsgVote {
			lead = None
		}
		log.Printf("raft: %x [term: %d] received a %s message with higher term from %x [term: %d]",
			rf.id, rf.Term, m.Type, m.From, m.Term)
		rf.becomeFollower(m.Term, lead)
	case m.Term < rf.Term:
		// ignore
		log.Printf("raft: %x [term: %d] ignored a %s message with lower term from %x [term: %d]",
			rf.id, rf.Term, m.Type, m.From, m.Term)
		return nil
	}*/

	DPrintf("[Step@etcd.go][%d][%s]Type:[%s] Step call raft  ", rf.me, rf.GetNodeState(), m.Type)
	rf.step(rf, m)

	DPrintf("[Step@etcd.go][%d][%s][%s] Step Exit  ", rf.me, rf.GetNodeState(), m.Type)
	return nil
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
func (rf *Raft) campaign(t CampaignType) {
	DPrintf("[campaign@etcd.go][%d][%s] campaign Entry  ", rf.me, rf.GetNodeState())
	/*if rf.HasLeader() {
		DPrintf("[campaign@etcd.go][%d][%s] campaign Exit ,because has leader ", rf.me, rf.GetNodeState())
		return
	}*/
	if rf.IsLeader() {
		DPrintf("[campaign@etcd.go][%d][%s] campaign Exit ,because is leader ", rf.me, rf.GetNodeState())
		rf.becomeLeader()
		return
	}

	rf.becomeCandidate()

	rf.Step(Message{From: rf.me, Type: MsgVote})
	DPrintf("[campaign@etcd.go][%d][%s] campaign Exit  ", rf.me, rf.GetNodeState())
}

func (rf *Raft) becomeCandidate() {
	DPrintf("[becomeCandidate@etcd.go][%d][%s] becomeCandidate Entry  ", rf.me, rf.GetNodeState())
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if rf.IsLeader() {
		panic("invalid transition [leader -> candidate]")
	}
	rf.step = stepCandidate
	rf.reset(rf.currentTerm + 1)
	rf.tick = rf.tickElection
	rf.votedFor = rf.me
	rf.ChangeNodeState(candidate)
	DPrintf("[becomeCandidate@etcd.go][%d][%s] becomeCandidate Exit  ", rf.me, rf.GetNodeState())
}

func stepCandidate(rf *Raft, m Message) error {
	DPrintf("[stepCandidate@etcd.go][%d][%s][%s] stepCandidate Entry  ", rf.me, rf.GetNodeState(), m.Type)
	DPrintf("[stepCandidate@etcd.go][%d][%s][%s] call SendReqVote2All ", rf.me, rf.GetNodeState(), m.Type)
	rf.SendReqVote2All()
	DPrintf("[stepCandidate@etcd.go][%d][%s][%s] stepCandidate Exit  ", rf.me, rf.GetNodeState(), m.Type)
	return nil
}

func (rf *Raft) becomeLeader() {
	DPrintf("[becomeLeader@etcd.go][%d][%s] becomeLeader Entry  ", rf.me, rf.GetNodeState())
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if rf.nodeState == follower {
		DPrintf("[becomeLeader@etcd.go][%d][%s] invalid transition [leader -> candidate]  ", rf.me, rf.GetNodeState())
		panic("invalid transition [leader -> candidate]")
	}
	rf.step = stepLeader
	rf.reset(rf.currentTerm)
	rf.tick = rf.tickHeartbeat
	rf.SetLeader(rf.me)
	rf.ChangeNodeState(leader)
	DPrintf("[becomeLeader@etcd.go][%d][%s] becomeLeader Exit  ", rf.me, rf.GetNodeState())
}

//rf.Step(Message{From: rf.me, Type: MsgVote})
func stepLeader(rf *Raft, m Message) error {
	DPrintf("[stepLeader@etcd.go][%d][%s][%s] stepLeader Entry  ", rf.me, rf.GetNodeState(), m.Type)
	rf.bcastHeartbeat()
	DPrintf("[stepLeader@etcd.go][%d][%s][%s] stepLeader Exit  ", rf.me, rf.GetNodeState(), m.Type)
	return nil
}

// bcastHeartbeat sends RRPC, without entries to all the peers.
func (rf *Raft) bcastHeartbeat() {
	DPrintf("[bcastHeartbeat@etcd.go][%d][%s] bcastHeartbeat Entry  ", rf.me, rf.GetNodeState())
	rf.SendAppendEntries2All()
	DPrintf("[bcastHeartbeat@etcd.go][%d][%s] bcastHeartbeat Exit  ", rf.me, rf.GetNodeState())
}

// tickHeartbeat is run by leaders to send a MsgBeat after rf.heartbeatTimeout.
func (rf *Raft) tickHeartbeat() {
	//DPrintf("[tickHeartbeat@etcd.go][%d][%s] tickHeartbeat Entry  ", rf.me, rf.GetNodeState())
	rf.mu.Lock()
	rf.elapsed++
	if rf.elapsed >= rf.heartbeatTimeout {
		rf.SetElapsed(0)
		rf.mu.Unlock()
		rf.Step(Message{From: rf.me, Type: MsgBeat})
	} else {
		rf.mu.Unlock()
	}
	//DPrintf("[tickHeartbeat@etcd.go][%d][%s] tickHeartbeat Exit  ", rf.me, rf.GetNodeState())
}
