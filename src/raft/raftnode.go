// Version: 0.1
// Date:2020/4/4
// Memo: refer to etcd raft code.   https://github.com/etcd-io/etcd/tree/master/contrib/raftexample/raft.go

package raft

import "time"

// A key-value stream backed by raft
type raftNode struct {
	id    int    // client ID for raft session
	peers string // raft peer URLs
	join  bool   // node is joining an existing cluster
	// raft backing for the commit/error channel
	node *Node
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(rf *Raft) {
	DPrintf("[newRaftNode@raftnode.go][%d] newRaftNode Entry", rf.me)
	rn := &raftNode{
		id:    rf.me,
		peers: "peers",
		join:  true,
	}
	rn.StartRaft(rf)
	DPrintf("[newRaftNode@raftnode.go][%d] newRaftNode Exit", rf.me)
}

//Refre the etcd node code (node.go)
//Firstly,the StartRaft() initiaes a node instance uisng StartNode()
//The node has a member named rf(raft.go)
//Secondly, the StartRaft() start goroutines for main loop
func (rn *raftNode) StartRaft(rf *Raft) {
	DPrintf("[StartRaft@raftnode.go][%d] StartRaft Entry", rn.id)
	rn.node = StartNode(rf)

	//curNode := StartNode(rf)
	//curNode.rn.DumpRaft()
	//go rc.serveRaft()
	go rn.ServeChannels(rf)
	DPrintf("[StartRaft@raftnode.go][%d] StartRaft Exit", rn.id)
}

func (rn *raftNode) ServeChannels(rf *Raft) {
	DPrintf("[ServeChannels@raftnode.go][%d] ServeChannels Entry", rn.id)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rn.node.Tick()
			DPrintf("[ServeChannels@raftnode.go][%d] ticker", rf.me)
			/*case <-rc.stopc:
			rc.stop()
			return*/
		}
		if rf.killed() {
			DPrintf("[ServeChannels@raftnode.go][%d] ServeChannels exit ,because be killed", rf.me)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	DPrintf("[ServeChannels@raftnode.go][%d] ServeChannels Exit", rn.id)
}
