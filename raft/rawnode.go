package raft

import (
	"errors"

	pb "github.com/Alorun/stonekv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft

	// The state recorded during the last Ready state is used for HasReady change detection
	prevSoftState *SoftState
	prevHardState pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	r := newRaft(config)
	rn := &RawNode{
		Raft: r,
		prevSoftState: &SoftState{
			Lead:		r.Lead,
			RaftState: 	r.State,
		},	
		prevHardState: pb.HardState{
			Term: 	r.Term,
			Vote: 	r.Vote,
			Commit: r.RaftLog.committed,
		},
	}
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// softState return the current SoftState
func (rn *RawNode) softState() *SoftState {
	return &SoftState{Lead: rn.Raft.Lead, RaftState: rn.Raft.State}
}


// hardState return the current HardState
func (rn *RawNode) hardState() pb.HardState {
	return pb.HardState{
		Term: 	rn.Raft.Term,
		Vote: 	rn.Raft.Vote,
		Commit:	rn.Raft.RaftLog.committed,
	}
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	r := rn.Raft
	if ss := rn.softState(); *ss != *rn.prevSoftState {
		return true
	}
	if hs := rn.hardState(); !IsEmptyHardState(hs) && !isHardStateEqual(hs, rn.prevHardState) {
		return true
	}
	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		return true
	}
	if len(r.msgs) > 0 {
		return true
	}
	if len(r.RaftLog.unstableEntries()) > 0 {
		return true
	}
	if len(r.RaftLog.nextEnts()) > 0 {
		return true
	}
	return false
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	r := rn.Raft
	rd := Ready {
		Messages: 			r.msgs,
		Entries: 			r.RaftLog.unstableEntries(),
		CommittedEntries: 	r.RaftLog.nextEnts(),
	}

	// SoftState having change
	if ss := rn.softState(); *ss != *rn.prevSoftState {
		rd.SoftState = ss
	}

	// HardState having change
	if hs := rn.hardState(); !isHardStateEqual(hs, rn.prevHardState) {
		rd.HardState = hs
	}

	// Snapshot to be applied
	if  !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		rd.Snapshot = *r.RaftLog.pendingSnapshot
	}

	return rd
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftState = rd.SoftState
	}
	if IsEmptyHardState(rd.HardState) {
		rn.prevHardState = rd.HardState
	}

	r := rn.Raft

	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries) - 1]
		r.RaftLog.stableTo(last.Index, last.Term)
	}

	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries) - 1]
		r.RaftLog.appliedTo(last.Index)
	}

	r.msgs = nil

	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.pendingSnapshot = nil
		r.RaftLog.maybeCompact()
	}
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
