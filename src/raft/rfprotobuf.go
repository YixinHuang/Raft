// Version: 0.1
// Date:2020/4/5
// Memo: copy from etcd raft code :https://github.com/etcd-io/etcd/tree/master/raft/raftpb/raft.pb.go

package raft

//import (
//	"errors"
//)
//import "time"

type MessageType int

const (
	MsgHup            MessageType = 0
	MsgBeat           MessageType = 1
	MsgProp           MessageType = 2
	MsgApp            MessageType = 3
	MsgAppResp        MessageType = 4
	MsgVote           MessageType = 5
	MsgVoteResp       MessageType = 6
	MsgSnap           MessageType = 7
	MsgHeartbeat      MessageType = 8
	MsgHeartbeatResp  MessageType = 9
	MsgUnreachable    MessageType = 10
	MsgSnapStatus     MessageType = 11
	MsgCheckQuorum    MessageType = 12
	MsgTransferLeader MessageType = 13
	MsgTimeoutNow     MessageType = 14
	MsgReadIndex      MessageType = 15
	MsgReadIndexResp  MessageType = 16
	MsgPreVote        MessageType = 17
	MsgPreVoteResp    MessageType = 18
)

var MessageType_name = [...]string{
	"MsgHup",
	"MsgBeat",
	"MsgProp",
	"MsgApp",
	"MsgAppResp",
	"MsgVote",
	"MsgVoteResp",
	"MsgSnap",
	"MsgHeartbeat",
	"MsgHeartbeatResp",
	"MsgUnreachable",
	"MsgSnapStatus",
	"MsgCheckQuorum",
	"MsgTransferLeader",
	"MsgTimeoutNow",
	"MsgReadIndex",
	"MsgReadIndexResp",
	"MsgPreVote",
	"MsgPreVoteResp",
}

func (x MessageType) String() string {
	return MessageType_name[x]
}

type Message struct {
	Type MessageType
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

func (m *Message) Reset() { *m = Message{} }

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)
