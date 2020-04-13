// Version: 0.1
// Date:2020/4/4
// Memo: refer to etcd raft code :https://github.com/etcd-io/etcd/tree/master/raft/node.go

package raft

import (
	"errors"
	"time"
)

var (
	emptyState = HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      int
	RaftState RaftNodeState
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

type Node struct {

	/*
		recvc      chan pb.Message
		confc      chan pb.ConfChangeV2
		confstatec chan pb.ConfState
		readyc     chan Ready
		advancec   chan struct{}*/
	propc chan Message
	tickc chan struct{}
	done  chan struct{}
	stop  chan struct{}
	//status     chan chan Status
	rn *Raft
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
//func StartNode(c *Config, peers []Peer) Node {
func StartNode(rf *Raft) *Node {
	DPrintf("[StartNode@node.go] StartNode Entry")
	n := newNode(rf)
	newRaft(rf)
	go n.Run()
	DPrintf("[StartNode@node.go] StartNode Exit")
	return &n
}

func newNode(rn *Raft) Node {
	return Node{
		/*propc:      make(chan msgWithResult),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChangeV2),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),*/
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		propc: make(chan Message),
		tickc: make(chan struct{}, 128),
		done:  make(chan struct{}),
		stop:  make(chan struct{}),
		//status: make(chan chan Status),
		rn: rn,
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *Node) Tick() {
	//DPrintf("[Tick@node.go] Tick Entry")
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		//	n.rn.raft.logger.Warningf("%x (leader %v) A tick missed to fire. Node blocks too long!", n.rn.raft.id, n.rn.raft.id == n.rn.raft.lead)
	}
	//DPrintf("[Tick@node.go] Tick Exit")
}

func (n *Node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

//Just for mit tesing need this, the ETCD do not need following function
func (n *Node) ProcessMsgProp() {
	//DPrintf("[ProcessMsgProp@node.go][%d] ProcessMsgProp Entry", n.rn.me)
	mm := []Message{}
	if len(n.rn.msgs) < 1 || !n.rn.IsLeader() {
		//DPrintf("[ProcessMsgProp@node.go][%d] ProcessMsgProp Exit,because no message reviced by leader", n.rn.me)
		return
	}
	for j, msg := range n.rn.msgs {
		if msg.Type == MsgProp {
			n.rn.Step(msg)
			DPrintf("[ProcessMsgProp@node.go][%d] Receive MsgProp in msgs[%d]", n.rn.me, j)
		} else {
			mm = append(mm, msg)
		}
	}
	n.rn.msgs = mm
	DPrintf("[ProcessMsgProp@node.go][%d] ProcessMsgProp Exit", n.rn.me)
}

func (n *Node) ProcessMsgAppResp() {
	//DPrintf("[ProcessMsgAppResp@node.go][%d] ProcessMsgAppResp Entry", n.rn.me)
	mm := []Message{}
	if len(n.rn.msgs) < 1 {
		//DPrintf("[ProcessMsgAppResp@node.go][%d][%s] ProcessMsgAppResp Exit,because [%d]no message received", n.rn.me, n.rn.GetNodeState(), len(n.rn.msgs))
		return
	}
	for j, msg := range n.rn.msgs {
		if msg.Type == MsgAppResp {
			DPrintf("[ProcessMsgAppResp@node.go][%d] Receive MsgAppResp in msgs[%d]", n.rn.me, j)
			n.rn.Step(msg)

		} else {
			mm = append(mm, msg)
		}
	}
	n.rn.msgs = mm
	DPrintf("[ProcessMsgAppResp@node.go][%d] ProcessMsgAppResp Exit", n.rn.me)
}

func (n *Node) Run() {
	DPrintf("[Run@node.go][%d] Run Entry", n.rn.me)
	r := n.rn
	var propc chan Message

	prevSoftSt := r.softState()
	prevHardSt := emptyState

	lead := None
	if lead != r.lead {
		if r.HasLeader() {
			if lead == None {
				DPrintf("[Run@node.go][%d][%s] raft.node: %x elected leader %x at term %d", r.me, r.GetNodeState(), r.me, r.lead, r.currentTerm)
			} else {
				DPrintf("[Run@node.go][%d][%s] raft.node: %x changed leader from %x to %x at term %d", r.me, r.GetNodeState(), r.me, lead, r.lead, r.currentTerm)
			}
			propc = n.propc
		} else {
			DPrintf("[Run@node.go][%d][%s] raft.node: %x lost leader %x at term %d", r.me, r.GetNodeState(), r.me, lead, r.currentTerm)
			propc = nil
		}
		lead = r.lead
	}
	//r.DumpRaft()

	for {

		n.ProcessMsgProp()
		n.ProcessMsgAppResp()
		if IsReady(r, prevSoftSt, prevHardSt) {
			prevSoftSt = r.softState()
			prevHardSt = r.hardState()
		}

		select {
		case <-n.tickc:
			n.rn.tick()
			//DPrintf("[Run@node.go][%d][%s] ticker", n.rn.me, n.rn.GetNodeState())

		//  maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		case m := <-propc:
			m.From = r.me
			r.Step(m)
		case <-n.stop:
			close(n.done)
			break
		}
		if n.rn.killed() {
			DPrintf("[Run@node.go][%d] Node Run exit ,because be killed", n.rn.me)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	n.rn.DumpRaft()
	DPrintf("[Run@node.go][%d] Run Exit", n.rn.me)
}

func (n *Node) Propose(data []byte) error {
	return n.step(Message{Type: MsgProp, Entries: []Entry{{Data: data}}})
}

func (n *Node) SendMsg(m Message) {
	n.propc <- m
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *Node) step(m Message) error {
	//ch := n.recvc
	ch := n.propc

	if m.Type == MsgProp {
		ch = n.propc
	}
	select {
	case ch <- m:
		return nil

	}
}

func IsReady(r *Raft, prevSoftSt *SoftState, prevHardSt HardState) bool {
	//DPrintf("[IsReady@node.go] IsReady Entry")

	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		DPrintf("[IsReady@node.go][%d][%s] !softSt.equal(prevSoftSt) ", r.me, r.GetNodeState())
		return true
	}
	if !isHardStateEqual(r.hardState(), prevHardSt) {
		DPrintf("[IsReady@node.go][%d][%s] isHardStateEqual Change", r.me, r.GetNodeState())
		return true
		//prevHardSt = r.hardState()
	}
	if r.raftLog.unstable.snapshot != nil {
		DPrintf("[IsReady@node.go] softState Change")
		//rd.Snapshot = *r.raftLog.unstable.snapshot
	}

	//DPrintf("[IsReady@node.go]IsReady Exit")
	return false
}

func isHardStateEqual(a, b HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}
