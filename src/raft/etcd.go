// Version: 0.1
// Date:2020/4/5
// Memo: refer to etcd raft code :https://github.com/etcd-io/etcd/tree/master/raft/raft.go

package raft

//import "time"

////////////////////////////////////////////////////////////////////////////////////

type Message struct {
	Type string
	To   int
	From int
	Term int
	/*	LogTerm          uint64      `protobuf:"varint,5,opt,name=logTerm" json:"logTerm"`
		Index            uint64      `protobuf:"varint,6,opt,name=index" json:"index"`
		Entries          []Entry     `protobuf:"bytes,7,rep,name=entries" json:"entries"`
		Commit           uint64      `protobuf:"varint,8,opt,name=commit" json:"commit"`
		Snapshot         Snapshot    `protobuf:"bytes,9,opt,name=snapshot" json:"snapshot"`
		Reject           bool        `protobuf:"varint,10,opt,name=reject" json:"reject"`
		RejectHint       uint64      `protobuf:"varint,11,opt,name=rejectHint" json:"rejectHint"`
		Context          []byte      `protobuf:"bytes,12,opt,name=context" json:"context,omitempty"`*/
}

func newRaft(rf *Raft) *Raft {
	DPrintf("[newRaft@raft.go][%d][%s] newRaft Entry  ", rf.me, rf.GetNodeState())
	rf.becomeFollower(rf.currentTerm, rf.leaderId)
	DPrintf("[newRaft@raft.go][%d][%s] newRaft Exit  ", rf.me, rf.GetNodeState())
	return rf
}

func (rf *Raft) becomeFollower(term int, lead int) {
	DPrintf("[becomeFollower@raft.go][%d][%s] becomeFollower Entry  ", rf.me, rf.GetNodeState())
	rf.tick = rf.tickElection
	DPrintf("[becomeFollower@raft.go][%d][%s] becomeFollower Exit  ", rf.me, rf.GetNodeState())

	/*r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)*/
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (rf *Raft) tickElection() {
	DPrintf("[tickElection@raft.go][%d][%s] tickElection Entry  ", rf.me, rf.GetNodeState())
	rf.Step(Message{From: rf.me, Type: "MsgHup"})
	DPrintf("[tickElection@raft.go][%d][%s] tickElection Exit  ", rf.me, rf.GetNodeState())
	/*r.electionElapsed++

	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}*/
}

func (rf *Raft) Step(m Message) {
	DPrintf("[Step@raft.go][%d][%s] Step Entry  ", rf.me, rf.GetNodeState())
	DPrintf("[Step@raft.go][%d][%s] Message From:[%d],Type:[%s] ", rf.me, rf.GetNodeState(), m.From, m.Type)
	DPrintf("[Step@raft.go][%d][%s] Step Exit  ", rf.me, rf.GetNodeState())

}
