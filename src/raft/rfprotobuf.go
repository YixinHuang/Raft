// Version: 0.1
// Date:2020/4/5
// Memo: copy from etcd raft code :https://github.com/etcd-io/etcd/tree/master/raft/raftpb/raft.pb.go

package raft

import (
	"fmt"
	"strconv"
)

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
	Type       MessageType
	From       int
	To         int
	Term       int
	Entries    []Entry
	Index      int
	LogTerm    int
	Commit     int
	Reject     bool
	RejectHint int
	Snapshot   Snapshot
	Context    []byte

	/*
		Snapshot         Snapshot    `protobuf:"bytes,9,opt,name=snapshot" json:"snapshot"`

		RejectHint       uint64      `protobuf:"varint,11,opt,name=rejectHint" json:"rejectHint"`
		Context          []byte      `protobuf:"bytes,12,opt,name=context" json:"context,omitempty"`*/
}

func (m *Message) Reset() { *m = Message{} }
func (m *Message) String() string {
	return fmt.Sprintf("Message:Type:=[%s] From:=[%d], To:=[%d], Term:=[%d] Entries:=[%v] Index:=[%d] LogTerm:=[%d] Commit:=[%d] Reject:=[%t]", MessageType_name[m.Type], m.From, m.To, m.Term, m.Entries, m.Index, m.LogTerm, m.Commit, m.Reject)
}
func (m *Message) GetCommand() int {
	cmd := 0
	if len(m.Entries) > 0 {
		str := string(m.Entries[0].Data[:])
		cmd, _ = strconv.Atoi(str)
	}
	return cmd
}

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

type EntryType int

const (
	EntryNormal EntryType = iota
	EntryConfChange
)

type Entry struct {
	Type             EntryType
	Term             int
	Index            int
	Data             []byte
	XXX_unrecognized []byte
}

func (m *Entry) Reset() { *m = Entry{} }
func (m *Entry) String() string {
	return fmt.Sprintf("Entry:Term = [%d], Type = [%d], Index = [%d]", m.Term, m.Type, m.Index)
}
func (*Entry) ProtoMessage() {}

type HardState struct {
	Term             int
	Vote             int
	Commit           int
	XXX_unrecognized []byte
}

func (m *HardState) Reset()      { *m = HardState{} }
func (*HardState) ProtoMessage() {}
func (m *HardState) String() string {
	return fmt.Sprintf("HardState:Term = [%d], Vote = [%d], Commit = [%d]", m.Term, m.Vote, m.Commit)
}

type ConfState struct {
	Nodes            []int
	XXX_unrecognized []byte
}

func (m *ConfState) Reset()         { *m = ConfState{} }
func (m *ConfState) String() string { return "confstate" }
func (*ConfState) ProtoMessage()    {}

type SnapshotMetadata struct {
	ConfState        ConfState
	Index            int
	Term             int
	XXX_unrecognized []byte
}

func (m *SnapshotMetadata) Reset() { *m = SnapshotMetadata{} }
func (m *SnapshotMetadata) String() string {
	return fmt.Sprintf("SnapshotMetadata:ConfState = [%v], Index = [%d], Term = [%d]", m.ConfState, m.Index, m.Term)
}
func (*SnapshotMetadata) ProtoMessage() {}

type Snapshot struct {
	Data             []byte
	Metadata         SnapshotMetadata
	XXX_unrecognized []byte
}

func (m *Snapshot) Reset() { *m = Snapshot{} }
func (m *Snapshot) String() string {
	return fmt.Sprintf("Snapshot:Metadata = [%v]", m.Metadata)
}
func (*Snapshot) ProtoMessage() {}
