// Version: 0.1
// Date:2020/4/4
// Memo: refer to etcd raft code :https://github.com/etcd-io/etcd/tree/master/raft/node.go

package raft

import "time"

type Node struct {
	/*propc      chan msgWithResult
	recvc      chan pb.Message
	confc      chan pb.ConfChangeV2
	confstatec chan pb.ConfState
	readyc     chan Ready
	advancec   chan struct{}*/
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
	DPrintf("[Tick@node.go] Tick Entry")
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		//	n.rn.raft.logger.Warningf("%x (leader %v) A tick missed to fire. Node blocks too long!", n.rn.raft.id, n.rn.raft.id == n.rn.raft.lead)
	}
	DPrintf("[Tick@node.go] Tick Exit")
}

func (n *Node) Run() {
	DPrintf("[Run@node.go][%d] Run Entry", n.rn.me)
	r := n.rn
	r.DumpRaft()

	for {
		select {
		case <-n.tickc:
			n.rn.tick()
			DPrintf("[Run@node.go][%d] ticker", n.rn.me)

		}
		if n.rn.killed() {
			DPrintf("[Run@node.go][%d] Node Run exit ,because be killed", n.rn.me)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	/*	var propc chan msgWithResult
		var readyc chan Ready
		var advancec chan struct{}
		var rd Ready

		r := n.rn.raft

		lead := None

		for {
			if advancec != nil {
				readyc = nil
			} else if n.rn.HasReady() {
				// Populate a Ready. Note that this Ready is not guaranteed to
				// actually be handled. We will arm readyc, but there's no guarantee
				// that we will actually send on it. It's possible that we will
				// service another channel instead, loop around, and then populate
				// the Ready again. We could instead force the previous Ready to be
				// handled first, but it's generally good to emit larger Readys plus
				// it simplifies testing (by emitting less frequently and more
				// predictably).
				rd = n.rn.readyWithoutAccept()
				readyc = n.readyc
			}

			if lead != r.lead {
				if r.hasLeader() {
					if lead == None {
						r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
					} else {
						r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
					}
					propc = n.propc
				} else {
					r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
					propc = nil
				}
				lead = r.lead
			}

			select {
			// TODO: maybe buffer the config propose if there exists one (the way
			// described in raft dissertation)
			// Currently it is dropped in Step silently.
			case pm := <-propc:
				m := pm.m
				m.From = r.id
				err := r.Step(m)
				if pm.result != nil {
					pm.result <- err
					close(pm.result)
				}
			case m := <-n.recvc:
				// filter out response message from unknown From.
				if pr := r.prs.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
					r.Step(m)
				}
			case cc := <-n.confc:
				_, okBefore := r.prs.Progress[r.id]
				cs := r.applyConfChange(cc)
				// If the node was removed, block incoming proposals. Note that we
				// only do this if the node was in the config before. Nodes may be
				// a member of the group without knowing this (when they're catching
				// up on the log and don't have the latest config) and we don't want
				// to block the proposal channel in that case.
				//
				// NB: propc is reset when the leader changes, which, if we learn
				// about it, sort of implies that we got readded, maybe? This isn't
				// very sound and likely has bugs.
				if _, okAfter := r.prs.Progress[r.id]; okBefore && !okAfter {
					var found bool
					for _, sl := range [][]uint64{cs.Voters, cs.VotersOutgoing} {
						for _, id := range sl {
							if id == r.id {
								found = true
							}
						}
					}
					if !found {
						propc = nil
					}
				}
				select {
				case n.confstatec <- cs:
				case <-n.done:
				}
			case <-n.tickc:
				n.rn.Tick()
			case readyc <- rd:
				n.rn.acceptReady(rd)
				advancec = n.advancec
			case <-advancec:
				n.rn.Advance(rd)
				rd = Ready{}
				advancec = nil
			case c := <-n.status:
				c <- getStatus(r)
			case <-n.stop:
				close(n.done)
				return
			}
		}*/
	DPrintf("[Run@node.go][%d] Run Exit", n.rn.me)
}
