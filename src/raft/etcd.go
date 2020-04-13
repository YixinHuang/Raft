// Version: 0.1
// Date:2020/4/5
// Memo: refer to etcd raft code :https://github.com/etcd-io/etcd/tree/master/raft/raft.go

package raft

import (
	"log"
	"sort"
	"strconv"
)

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
	rf.send(m)*/
	case MsgApp:
		DPrintf("[stepFollower@etcd2B.go][%d][%s][%s] stepFollower Entry  ", rf.me, rf.GetNodeState(), m.Type)
		rf.SetElapsed(0)
		rf.lead = m.From
		//rf.handleAppendEntries(m)
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
	/*if m.Type == MsgHup {
		DPrintf("[Step@etcd.go][%d][%s] %x is starting a new election at term %d", rf.me, rf.GetNodeState(), rf.me, rf.currentTerm)
		//r.campaign()
		//r.Commit = r.raftLog.committed
		return nil
	}*/
	switch m.Type {
	case MsgHup:
		rf.campaign(campaignElection)
		DPrintf("[Step@etcd.go][%d][%s] Step Exit  ", rf.me, rf.GetNodeState())
		return nil
	case MsgVote:
		DPrintf("[Step@etcd.go][%d][%s] Step revceive  MsgVote ", rf.me, rf.GetNodeState())
	case MsgBeat:
		DPrintf("[Step@etcd.go][%d][%s] Step revceive  MsgBeat ", rf.me, rf.GetNodeState())
	case MsgProp:
		DPrintf("[Step@etcd.go][%d][%s] Step revceive  MsgProp ", rf.me, rf.GetNodeState())
	case MsgApp:
		DPrintf("[Step@etcd.go][%d][%s] Step revceive  MsgApp ", rf.me, rf.GetNodeState())
	case MsgHeartbeat:
		DPrintf("[Step@etcd.go][%d][%s] Step revceive  MsgHeartbeat ", rf.me, rf.GetNodeState())
	case MsgAppResp:
		DPrintf("[Step@etcd.go][%d][%s] Step revceive  MsgAppResp ", rf.me, rf.GetNodeState())
	default:
		DPrintf("[Step@etcd.go][%d][%s] Default Step call raft  ", rf.me, rf.GetNodeState())
		err := rf.step(rf, m)
		if err != nil {
			return err
		}
	}

	switch {
	case m.Term == 0:
		// local message
	case m.Term > rf.currentTerm:
		lead := m.From
		if m.Type == MsgVote {
			lead = None
		}
		rf.becomeFollower(m.Term, lead)
	case m.Term < rf.currentTerm:
		// ignore
		return nil
	}

	DPrintf("[Step@etcd.go][%d][%s]Type:[%s] Step call raft  ", rf.me, rf.GetNodeState(), m.Type)
	rf.step(rf, m)
	//r.Commit = r.raftLog.committed

	DPrintf("[Step@etcd.go][%d][%s][%s] Step Exit  ", rf.me, rf.GetNodeState(), m.Type)
	return nil
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
// The ETCD using send Message to raft message array.
// The node run loop to create a Ready struct that includes message member ,
// The node send Ready to application via chan readyc
// The application received the Ready ,save to disk and use transport.Send(rd.Messages),at last call node.Advance()
// The transport will send message to node
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

	/*	r.becomeCandidate()
		if r.q() == r.poll(r.id, true) {
			r.becomeLeader()
			return
		}
		for i := range r.prs {
			if i == r.id {
				continue
			}
			log.Printf("raft: %x [logterm: %d, index: %d] sent vote request to %x at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), i, r.Term)
			r.send(pb.Message{To: i, Type: pb.MsgVote, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm()})
		}*/
}

func (rf *Raft) becomeCandidate() {
	DPrintf("[becomeCandidate@etcd.go][%d][%s] becomeCandidate Entry  ", rf.me, rf.GetNodeState())
	// (xiangli) remove the panic when the raft implementation is stable
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
	// (xiangli) remove the panic when the raft implementation is stable
	if rf.nodeState == follower {
		DPrintf("[becomeLeader@etcd.go][%d][%s] invalid transition [leader -> candidate]  ", rf.me, rf.GetNodeState())
		panic("invalid transition [leader -> candidate]")
	}
	rf.step = stepLeader
	rf.reset(rf.currentTerm)
	rf.tick = rf.tickHeartbeat
	rf.SetLeader(rf.me)
	rf.ChangeNodeState(leader)
	DPrintf("[becomeLeader@etcd.go][%d][%s] becomeLeader Exit, at term:=[%d]  ", rf.me, rf.GetNodeState(), rf.currentTerm)
}

//rf.Step(Message{From: rf.me, Type: MsgVote})
func stepLeader(rf *Raft, m Message) error {
	if !rf.IsLeader() {
		return nil
	}
	DPrintf("[stepLeader@etcd.go][%d][%s][%s] stepLeader Entry  ", rf.me, rf.GetNodeState(), m.Type)
	//rf.bcastHeartbeat()
	switch m.Type {
	case MsgBeat:
		DPrintf("[stepLeader@etcd.go][%d][%s][%s] MsgBeat  Received ", rf.me, rf.GetNodeState(), m.Type)
		rf.bcastHeartbeat()
	case MsgProp:
		DPrintf("[stepLeader@etcd.go2B][%d][%s][%s] MsgProp  Received ", rf.me, rf.GetNodeState(), m.Type)
		if len(m.Entries) == 0 {
			log.Panicf("raft: %x stepped empty MsgProp", rf.me)
		}
		rf.appendEntry(m.Entries...)
		rf.bcastAppend()
	case MsgAppResp:
		if m.Reject {
			DPrintf("[stepLeader@etcd.go2B]raft: %x received msgApp rejection(lastindex: %d) from %x for index %d", rf.me, m.RejectHint, m.From, m.Index)
			//TODO(hyx)
			/*if r.prs[m.From].maybeDecrTo(m.Index, m.RejectHint) {
				log.Printf("raft: %x decreased progress of %x to [%s]", r.id, m.From, r.prs[m.From])
				r.sendAppend(m.From)
			}*/
		} else {
			oldWait := rf.prs[m.From].shouldWait()
			DPrintf("[MsgAppRespstepLeader@etcd.go2B][%d][%s][%s] call update ,m.GetCommand()=[%d]  Messge:=[%s]", rf.me, rf.GetNodeState(), m.Type, m.GetCommand(), m.String())
			rf.prs[m.From].update(m.Index)
			for i, curprs := range rf.prs {
				DPrintf("[MsgAppRespstepLeader@etcd.go2B][%d][%s][%s] call update rf.prs[%d] := [%v]", rf.me, rf.GetNodeState(), m.Type, i, curprs)
			}
			DPrintf("[MsgAppRespstepLeader@etcd.go2B][%d][%s][%s] call maybeCommit ,m.GetCommand()=[%d]  Messge:=[%s]", rf.me, rf.GetNodeState(), m.Type, m.GetCommand(), m.String())
			if rf.maybeCommitMit() {
				DPrintf("[MsgAppRespstepLeader@etcd.go2B][%d][%s][%s] call rf.bcastAppend() ,rf.prs[%d]:=[%s]  oldWait:=[%t]", rf.me, rf.GetNodeState(), m.Type, m.From, rf.prs[m.From], oldWait)
				rf.bcastAppend()
				//Write back to tesing chan
				rf.sendApplyMsg(m.GetCommand())
			} else if oldWait {
				// update() reset the wait state on this node. If we had delayed sending
				// an update before, send it now.
				DPrintf("[MsgAppRespstepLeader@etcd.go2B][%d][%s][%s] call rf.sendAppend() ", rf.me, rf.GetNodeState(), m.Type)
				//rf.sendAppend(m.From)
			}
			DPrintf("[MsgAppRespstepLeader@etcd.go2B][%d][%s][%s] exit else rf.prs[%d]:=[%s]  oldWait:=[%t]", rf.me, rf.GetNodeState(), m.Type, m.From, rf.prs[m.From], oldWait)
		}
	}

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

//============================For 2B Testing Cases================================
// 1. Start(command interface{})
//	  Receive Command from testing code
//	  If this server isn't the leader, returns false. otherwise start the agreement and return immediately.
//	  the first return value is the index that the command will appear at
//    if it's ever committed. the second return value is the current
//    term. the third return value is true if this server believes it is the leader.
// 2. TestBasicAgree2B
//	  Call cfg.one(index*100, servers, false)
//	  index*100 is commnad need to save
//	  Servers=3 means will check 3 nodes to agree the command value
// 3. func (cfg *config) one(cmd interface{}, expectedServers int, retry bool)
//	  3.1 Loop in 10 second to call Start(command interface{})
//	  3.2 Loop in 2 second to call cfg.nCommitted(index) for check all node agree the vaule
// 4. applyCh chan ApplyMsg
//	  From Make parameter get the chan apply
//	  applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// 5. func (cfg *config) start1(i int)
//	  start or re-start a Raft. Start a go routin to listen to messages from Raft indicating newly committed messages.
// 6. Main flow:
//				6.1 save command to raftlog;
//				6.2 leader send AppendEntries to followers for saving command in follower's log
//				6.3 follower save command to raftlog and answer to leader
//				6.4 leader get majority answer , send commit to follower by using heartbeating AE message,
//				6.5 leader send msg to chan applyCh
//				6.6 follower send msg to chan applyCh
//=====================================================================================================================

//6.1 save command to raftlog. Using ETCT message
//Normally, the applicat send message via ch propc. The function saveCommand2Raftlog as application using Message MsgProp just for testing
//The ETCD impletation need not do this
func (rf *Raft) saveCommand2Raftlog(command interface{}) {
	DPrintf("[saveCommand2Raftlog@etcd.go2B][%d][%s] saveCommand2Raftlog Entry  ", rf.me, rf.GetNodeState())
	//buf := make([]byte, 1)
	//buf[0] = byte(command.(int))
	cmd := command.(int)
	str := strconv.Itoa(cmd)
	buf := []byte(str)
	DPrintf("[saveCommand2Raftlog@etcd.go2B][%d][%s] command to int :=[%d]  buf:=[%v] ", rf.me, rf.GetNodeState(), command.(int), buf)
	//rf.appendEntry(Entry{Data: buf})
	rf.appendMsgProp(Entry{Data: buf})
	DPrintf("[saveCommand2Raftlog@etcd.go2B][%d][%s] saveCommand2Raftlog Exit  ", rf.me, rf.GetNodeState())
}

func (rf *Raft) appendMsgProp(es ...Entry) {
	DPrintf("[appendMsgProp@etcd.go2B][%d][%s] appendMsgProp Entry  ", rf.me, rf.GetNodeState())
	li := rf.raftLog.lastIndex()
	for i := range es {
		es[i].Type = EntryNormal
		es[i].Term = rf.currentTerm
		es[i].Index = li + 1 + i
		DPrintf("[appendMsgProp@etcd.go2B][%d][%s] Entry[%d] Term:=[%d] Index:=[%d] Data:=[%v]", rf.me, rf.GetNodeState(), i, es[i].Term, es[i].Index, es[i].Data)
	}
	//TODO(hyx)
	rf.send(Message{From: rf.me, Type: MsgProp, Entries: es})

	DPrintf("[appendMsgProp@etcd.go2B][%d][%s] appendMsgProp Exit  ", rf.me, rf.GetNodeState())
}

// send persists state to stable storage and then sends to its mailbox.
func (rf *Raft) send(m Message) {
	DPrintf("[send@etcd.go2B][%d][%s] send Entry ", rf.me, rf.GetNodeState())

	if m.Type != MsgAppResp {
		m.From = rf.me
	}
	// do not attach term to MsgProp
	// proposals are a way to forward to the leader and
	// should be treated as local message.
	if m.Type != MsgProp {
		m.Term = rf.currentTerm
	}
	rf.msgs = append(rf.msgs, m)

	DPrintf("[send@etcd.go2B][%d][%s] send Exit  ", rf.me, rf.GetNodeState())
}

func (rf *Raft) appendEntry(es ...Entry) {
	DPrintf("[appendEntry@etcd.go2B][%d][%s] appendEntry Entry  ", rf.me, rf.GetNodeState())
	li := rf.raftLog.lastIndex()
	for i := range es {
		es[i].Type = EntryNormal
		es[i].Term = rf.currentTerm
		es[i].Index = li + 1 + i
		DPrintf("[appendEntry@etcd.go2B][%d][%s] Entry[%d] Term:=[%d] Index:=[%d] Date:=[%v]", rf.me, rf.GetNodeState(), i, es[i].Term, es[i].Index, es[i].Data)
	}
	rf.raftLog.append(es...)
	DPrintf("[appendEntry@etcd.go2B][%d][%s] rf.raftLog := [%v]", rf.me, rf.GetNodeState(), rf.raftLog)
	DPrintf("[appendEntry@etcd.go2B][%d][%s] rf.raftLog.lastIndex() := [%d]", rf.me, rf.GetNodeState(), rf.raftLog.lastIndex())
	DPrintf("[appendEntry@etcd.go2B][%d][%s] rf.prsp[%d] := [%s]", rf.me, rf.GetNodeState(), rf.me, rf.prs[rf.me].String())
	rf.prs[rf.me].update(rf.raftLog.lastIndex())

	rf.maybeCommit()
	DPrintf("[appendEntry@etcd.go2B][%d][%s] appendEntry Exit  ", rf.me, rf.GetNodeState())
}

func (rf *Raft) maybeCommit() bool {
	// (bmizerany): optimize.. Currently naive
	DPrintf("[maybeCommit@etcd.go2B][%d][%s] maybeCommit Entry  ", rf.me, rf.GetNodeState())
	mis := make(uintSlice, 0, len(rf.prs))
	for i := range rf.prs {
		mis = append(mis, rf.prs[i].Match)
	}
	sort.Sort(sort.Reverse(mis))
	mci := mis[rf.majority()-1]
	rlt := rf.raftLog.maybeCommit(mci, rf.currentTerm)
	DPrintf("[maybeCommit@etcd.go2B][%d][%s] maybeCommit Exit,return [%t] mci:=[%d]", rf.me, rf.GetNodeState(), rlt, mci)
	return rlt
}

func (rf *Raft) maybeCommitMit() bool {
	// (bmizerany): optimize.. Currently naive
	DPrintf("[maybeCommitMit@etcd.go2B][%d][%s] maybeCommitMit Entry  ", rf.me, rf.GetNodeState())
	mis := make(uintSlice, 0, len(rf.prs))
	for i := range rf.prs {
		mis = append(mis, rf.prs[i].Match)
	}
	sort.Sort(sort.Reverse(mis))
	mci := mis[rf.majority()]
	rlt := rf.raftLog.maybeCommit(mci, rf.currentTerm)
	DPrintf("[maybeCommitMit@etcd.go2B][%d][%s] maybeCommitMit Exit,return [%t] mci:=[%d]", rf.me, rf.GetNodeState(), rlt, mci)
	return rlt
}

//If command received from client: append entry to local log,
//respond after entry applied to state machine (§5.3)

// bcastAppend sends RRPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (rf *Raft) bcastAppend() {
	if !rf.IsLeader() {
		panic("[bcastAppend@etcd.go2B] unexpected node type (is not leader)")
	}
	DPrintf("[bcastAppend@etcd.go2B][%d][%s] bcastAppend Entry ", rf.me, rf.GetNodeState())
	for i := range rf.prs {
		if i == rf.me {
			continue
		}
		DPrintf("[bcastAppend@etcd.go2B][%d][%s] bcastAppend call  sendAppend i=[%d]", rf.me, rf.GetNodeState(), i)
		rf.sendAppend(i)
	}
	DPrintf("[bcastAppend@etcd.go2B][%d][%s] bcastAppend Exit", rf.me, rf.GetNodeState())
}

// sendAppend sends RPC, with entries to the given peer.
func (rf *Raft) sendAppend(to int) {
	if !rf.IsLeader() {
		panic("[sendAppend@etcd.go2B] unexpected node type (is not leader)")
	}
	DPrintf("[sendAppend@etcd.go2B][%d][%s] sendAppend Entry, to [%d] ", rf.me, rf.GetNodeState(), to)
	rf.mu.Lock()
	pr := rf.prs[to]

	AEargs := AppendEntriesArgs{}
	AEargs.Term = rf.currentTerm
	AEargs.LeaderId = rf.me
	AEargs.PrevLogIndex = pr.Next - 1
	AEargs.PrevLogTerm = rf.raftLog.term(pr.Next - 1)
	AEargs.LeaderCommit = rf.commitIndex
	AEargs.Entries = rf.raftLog.entries(pr.Next)

	for i, ens := range AEargs.Entries {
		DPrintf("[sendAppend@etcd.go2B][%d][%s] AEargs.Entries[%d] := [%s]", rf.me, rf.GetNodeState(), i, ens.String())
	}
	DPrintf("[sendAppend@etcd.go2B][%d][%s] pr := [%s]", rf.me, rf.GetNodeState(), pr.String())
	DPrintf("[sendAppend@etcd.go2B][%d][%s] rf.raftLog := [%v]", rf.me, rf.GetNodeState(), rf.raftLog)
	DPrintf("[sendAppend@etcd.go2B][%d][%s] Dump %s  ", rf.me, rf.GetNodeState(), AEargs.String())
	rf.mu.Unlock()
	go func(server int, args AppendEntriesArgs) {
		DPrintf("[sendAppend@etcd.go2B][%d][%s] send AppendEntries for  rf.peers[%d] ", rf.me, rf.GetNodeState(), server)
		reply := AppendEntriesReply{}
		//DPrintf("[SendAppendEntries2All@etcd.go2B][%d] Call AppendEntries Entry", rf.me)
		rf.sendAppendEntriesEx(server, &args, &reply)
	}(to, AEargs)
	DPrintf("[sendAppend@etcd.go2B][%d][%s] sendAppend Exit  ", rf.me, rf.GetNodeState())
}

func (rf *Raft) AppendEntriesEx(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[AppendEntriesExHandler@etcd.go2B][%d][%s] AppendEntriesExHandler() Entry,args.Term =[%d] args.LeaderId =[%d]", rf.me, rf.GetNodeState(), args.Term, args.LeaderId)

	DPrintf("[AppendEntriesExHandler@etcd.go2B][%d][%s] Dump %s", rf.me, rf.GetNodeState(), args.String())

	m := Message{}
	m.To = rf.me
	m.Type = MsgApp
	m.Index = args.PrevLogIndex
	m.LogTerm = args.PrevLogTerm
	m.Entries = args.Entries
	m.Commit = args.LeaderCommit

	//set return value
	success := rf.handleAppendEntries(m)

	reply.Term = rf.currentTerm
	reply.Success = success

	DPrintf("[AppendEntriesExHandler@etcd.go2B][%d][%s] AppendEntriesExHandler() Exit", rf.me, rf.GetNodeState())
}

func (rf *Raft) sendAppendEntriesEx(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[sendAppendEntriesEx@etcd.go2B][%d] sendAppendEntriesEx() Entry", rf.me)
	args.DumpAppendEntriesArgs()
	ok := rf.peers[server].Call("Raft.AppendEntriesEx", args, reply)
	reply.DumpAppendEntriesReply()
	DPrintf("[sendAppendEntriesEx@etcd.go2B][%d] sendAppendEntriesEx() Exit", rf.me)
	return ok
}

func (rf *Raft) handleAppendEntries(m Message) bool {
	DPrintf("[handleAppendEntries@etcd.go2B][%d][%s] handleAppendEntries() Entry,Message:=[%s]", rf.me, rf.GetNodeState(), m.String())
	rt := true
	rf.SetElapsed(0)
	if mlastIndex, ok := rf.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		//rf.send(Message{To: m.From, Type: MsgAppResp, Index: mlastIndex})
		DPrintf("[handleAppendEntries@etcd.go2B][%d][%s] rf.raftLog.maybeAppend return [%t], ", rf.me, rf.GetNodeState(), ok)
		cmd := m.GetCommand()
		DPrintf("[handleAppendEntries@etcd.go2B][%d][%s] m.GetCommand() return [%d], ", rf.me, rf.GetNodeState(), cmd)
		if cmd > 0 {
			ci := rf.raftLog.findConflict(m.Entries)

			DPrintf("[handleAppendEntries@etcd.go2B][%d][%s] call rf.answer2tesingSvr  ci:=[%d] entry:=[%s] mlastIndex:=[%d]", rf.me, rf.GetNodeState(), ci, m.Entries[0].String(), mlastIndex)
			rf.answer2tesingSvr(m.GetCommand(), m.Index)
			rf.sendTsfMessage(rf.lead, Message{To: m.From, Type: MsgAppResp, Index: mlastIndex, Entries: m.Entries})
		}
	} else {
		DPrintf("[handleAppendEntries@etcd.go2B] raft: %x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			rf.me, rf.raftLog.term(m.Index), m.Index, m.LogTerm, m.Index, m.From)
		rf.sendTsfMessage(rf.lead, Message{To: m.From, Type: MsgAppResp, Index: m.Index, Reject: true, RejectHint: rf.raftLog.lastIndex()})
		rt = false
	}
	DPrintf("[handleAppendEntries@etcd.go2B][%d][%s] handleAppendEntries() Exit, result :=[%t]", rf.me, rf.GetNodeState(), rt)
	return rt
}

func (rf *Raft) sendTsfMessage(server int, m Message) bool {
	DPrintf("[sendTsfMessage@etcd.go2B][%d][%s] sendTsfMessage() Entry,server:=[%d]", rf.me, rf.GetNodeState(), server)
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.lastApplied, PrevLogTerm: rf.GetLastLogTerm(), LeaderCommit: rf.commitIndex}
	args.Msg = m
	args.Msg.From = rf.me
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.SendTransferMessage", &args, &reply)
	DPrintf("[sendTsfMessage@etcd.go2B][%d][%s] sendTsfMessage() Exit", rf.me, rf.GetNodeState())
	return ok
}

func (rf *Raft) SendTransferMessage(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	DPrintf("[SendTransferMessage@etcd.go2B][%d][%s] SendTransferMessage() Entry,args.Term =[%d] args.LeaderId =[%d]", rf.me, rf.GetNodeState(), args.Term, args.LeaderId)
	DPrintf("[SendTransferMessage@etcd.go2B][%d][%s] Dump %s", rf.me, rf.GetNodeState(), args.Msg.String())

	m := args.Msg
	//m.From = rf.me
	if m.Type != MsgProp {
		m.Term = rf.currentTerm
	}
	rf.msgs = append(rf.msgs, m)
	for i, msg := range rf.msgs {
		DPrintf("SendTransferMessage@etcd.go2B][%d][%s] rf.msgs[%d] := [%s]", rf.me, rf.GetNodeState(), i, msg.String())
	}
	DPrintf("[SendTransferMessage@etcd.go2B][%d][%s] SendTransferMessage() Exit,msg length:=[%d]", rf.me, rf.GetNodeState(), len(rf.msgs))
}

/*func (rf *Raft) sendTsfMessage(server int, m Message) bool {
	DPrintf("[sendTsfMessage@etcd.go2B][%d] sendAppendEntriesEx() Entry", rf.me)
	ok := rf.peers[server].Call("Raft.SendTransferMessage", m, m)
	DPrintf("[sendTsfMessage@etcd.go2B][%d] sendAppendEntriesEx() Exit", rf.me)
	return ok
}

func (rf *Raft) SendTransferMessage(m Message, n Message) {
	DPrintf("[SendTransferMessage@etcd.go2B][%d][%s] send Entry ,server:=[]", rf.me, rf.GetNodeState())

	m.From = rf.me
	// do not attach term to MsgProp
	// proposals are a way to forward to the leader and
	// should be treated as local message.
	if m.Type != MsgProp {
		m.Term = rf.currentTerm
	}
	rf.msgs = append(rf.msgs, m)

	DPrintf("[SendTransferMessage@etcd.go2B][%d][%s] send Exit  ", rf.me, rf.GetNodeState())
}*/

//================================================================================
