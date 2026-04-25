package raft

import (
	"crypto/rand"
	"errors"

	pb "github.com/Alorun/stonekv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

//================================== Config =======================================

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}
	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}
	return nil
}

//================================== Progress =======================================

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// Match + 1 <= Next
	Match, Next uint64
}

//================================== Raft =======================================

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	// randomizedElectionTimeout is a timeout value that is randomized within 
	// the range of [electionTimeout, 2*electionTimeout) to avoid election conflicts
	randomizedElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this value.
	PendingConfIndex uint64
}

//================================== Construct =======================================

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	
	raftLog := newLog(c.Storage)

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	peers := c.peers
	if len(peers) == 0 {
		peers = confState.Nodes
	}

	prs := make(map[uint64]*Progress)
	for _, id := range peers {
		prs[id] = &Progress{Next: 1}
	}

	r := &Raft{
		id :				c.ID,
		Term: 				hardState.Term,
		Vote: 				hardState.Vote,
		RaftLog:	 		raftLog,
		Prs: 				prs,
		State: 				StateFollower,
		votes:		  		make(map[uint64]bool),
		msgs:       	    nil,
		Lead: 				None,
		heartbeatTimeout: 	c.HeartbeatTick,
		electionTimeout: 	c.ElectionTick,
	}

	// Restore applied pointer
	if c.Applied > 0 {
		raftLog.appliedTo(c.Applied)
	}

	// Override committed
	raftLog.committed = hardState.Commit

	r.resetRandomizedElectionTimeout()
	return r
}

//================================== Tool Method =======================================

// send push the message into the send queue, automatically populating From and Term
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

// quorum return the minimum number of nodes required to form a majority
func (r *Raft) quorum() int {
	return (len(r.Prs) / 2) + 1
}

// Randomized election timeout within the interval to avoid multiple nodes initiating elections at the same time
func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// Check whether the randomization election timeout has been exceeted
func (r *Raft) pastElectinoTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

// softState return the volatile state of the current node
func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

// hardState return the state that needs to be persisted
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term: 	r.Term,
		Vote:	r.Vote,
		Commit: r.RaftLog.committed,
	}
}

//================================== Role Reversal =======================================

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
	r.leadTransferee = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.State = StateCandidate
	r.Term++;
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.State = StateLeader
	r.Lead = r.id
	r.leadTransferee = None
	r.heartbeatElapsed = 0

	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id] = &Progress{Match: lastIndex + 1, Next: lastIndex + 2}
		} else {
			r.Prs[id] = &Progress{Match: 0, Next: lastIndex + 1}
		}
	}

	noop := &pb.Entry{Term: r.Term, Index: lastIndex + 1}
	r.RaftLog.append(*noop)

	r.broadcastAppend()

	r.maybeCommit()
}

//================================== Tick =======================================

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.tickHeartbeat()
	case StateFollower, StateCandidate:
		r.tickElection()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	// The leader will automatically step down if it does not receive a response from
	// the majority for a long time
	if r.pastElectinoTimeout() {
		r.electionElapsed = 0
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		// MsgBeat is an internal message that dirces the leader to broadcast heartbeats
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.pastElectinoTimeout() {
		r.electionElapsed = 0
		// MsgHup drives follower/candidate to initiate elections
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

//================================== Step - Message Processing Entry=======================================

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Global term check
	switch {
	case m.Term == 0:
		// Local messages(MsgHup/MsgBeat/MsgPropose), no term check
	case m.Term > r.Term:
		// After reveiving a higher term, regardless of current role, immediately reduce to follower
		lead := m.From
		// The voting request cannot conclude that From is the Leader
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		// Outdated news, ignore it directly
		return nil
	}

	// Distribute by role
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

//================================== Each Role Step Function =======================================

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()

	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)

	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)

	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)

	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)

	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()

	case pb.MessageType_MsgPropose:
		return r.handlePropose(m)

	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)

	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)

	case pb.MessageType_MsgRequestVote:
		// Same Term, high Term already is dealed with
		r.handleRequestVote(m)
	
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
	return nil
}

//================================== Send Primitive =======================================

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	if pr == nil {
		return false
	}

	prevLogIndex := pr.Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		return r.sendSnapshot(to)
	}

	entries, err := r.RaftLog.slice(pr.Next, r.RaftLog.LastIndex() + 1)
	if err != nil {
		return r.sendSnapshot(to)
	}

	ents := make([]*pb.Entry, len(entries))
	for i := range entries {
		ents[i] = &entries[i]
	}

	r.send(pb.Message{
		MsgType: 	pb.MessageType_MsgAppend,
		To: 		to,
		Index: 		prevLogIndex,
		LogTerm: 	prevLogTerm,
		Entries: 	ents,
		Commit: 	r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	r.send(pb.Message{
		MsgType: 	pb.MessageType_MsgHeartbeat,
		To: 		to,
		Commit: 	commit,
	})

}

func (r *Raft) sendSnapshot(to uint64) bool {
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return false
	}
	r.send(pb.Message{
		MsgType: 	pb.MessageType_MsgSnapshot,
		To: 		to,
		Snapshot: 	&snap,
	})
	r.Prs[to].Next = snap.Metadata.Index + 1
	return true
}

func (r *Raft) broadcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) broadcastHeartbeat() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

//================================== Election =======================================

func (r *Raft) startElection() {
	r. becomeCandidate()

	// Single node cluster, directly elected
	if r.quorum() == 1 {
		r.becomeLeader()
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			MsgType: 	pb.MessageType_MsgRequestVote,
			To: 		id,
			LogTerm: 	lastTerm,
			Index: 		lastIndex,
		})
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// up-to-date
	// 1. Term of candidate is longer than 
	// 2. Term of candidate si equal, log up-to-date
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	logUpToDate := m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex)

	// Conditions for voting
	// 1. This term has not yet been voted for, or has been voted for the same candidate.
	// 2. Candidate log up-to-date
	canGrant := (r.Vote == None || r.Vote == m.From) && logUpToDate

	if canGrant {
		r.Vote = m.From
		r.electionElapsed = 0
		r.resetRandomizedElectionTimeout()
	}

	r.send(pb.Message{
		MsgType: 	pb.MessageType_MsgRequestVoteResponse,
		To: 		m.From,
		Reject: 	!canGrant,
	})
}

// For candidate to count the votes
func (r *Raft) handleVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject

	granded, rejected := 0, 0
	for _, v := range r.votes {
		if v {
			granded++
		} else {
			rejected++
		}
	}

	q := r.quorum()
	switch {
	case granded >= q:
		r.becomeLeader()
	case rejected >= q:
		r.becomeFollower(r.Term, None)
	}
}

//================================== AppendEntries =======================================

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// The append log has been commited
	if m.Index < r.RaftLog.committed {
		// Reply to leader regarding current progress
		r.send(pb.Message{
			MsgType: 	pb.MessageType_MsgAppendResponse,
			To: 		m.From,
			Index: 		r.RaftLog.committed,
			Reject: 	false,
		})
		return
	}

	// Consistency check: the terms of prevLog must match
	if prevTerm, err := r.RaftLog.Term(m.Index); err != nil || prevTerm != m.LogTerm {
		// Tell the leader our LastIndex for quick rollback
		r.send(pb.Message{
			MsgType: 	pb.MessageType_MsgAppendResponse,
			To: 		m.From,
			Index: 		r.RaftLog.LastIndex(),
			Reject: 	true,
		})
	}

	// Append/overwrite log entries(handle confliciting truncation)
	for i, ent := range m.Entries {
		idx := ent.Index
		if idx <= r.RaftLog.LastIndex() {
			// The index already exists, check whether the terms conflict
			exisTerm, err := r.RaftLog.Term(idx)
			if err != nil || exisTerm != ent.Term {
				newEnts := make([]pb.Entry, len(m.Entries) - i)
				for j, e := range m.Entries[i:] {
					newEnts[j] = *e
				}
				r.RaftLog.append(newEnts...)
				break
			}
		} else {
			// The index exceeds the end of the current log and appends all remaining entries
			newEnts := make([]pb.Entry, len(m.Entries) - i)
			for j, e := range m.Entries[i:] {
				newEnts[j] = *e
			}
			r.RaftLog.append(newEnts...)
			break
		}
	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index + uint64(len(m.Entries)))
	}

	// Tell the leader our LastIndex
	r.send(pb.Message{
		MsgType: 	pb.MessageType_MsgAppendResponse,
		To:			m.From,
		Index: 		r.RaftLog.LastIndex(),
		Reject: 	false,
	})
}

// For leader to deal with follower's appendentries
func (r *Raft) handleAppendResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr == nil {
		return
	}

	if m.Reject {
		// Quick rewind
		if m.Index + 1 < pr.Next {
			pr.Next = m.Index + 1
		}
		r.sendAppend(m.From)
		return
	}

	// Success: Update Match/Next
	if m.Index > pr.Match {
		pr.Match = m.Index
		pr.Next = m.Index + 1
		r.maybeCommit()
		if r.leadTransferee == m.From && pr.Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(m.From)
		}
	}
}

// maybeCommit attempts to advance leader's commitIndex
func (r *Raft) maybeCommit() {
	for n := r.RaftLog.LastIndex(); n > r.RaftLog.committed; n-- {
		t, err := r.RaftLog.Term(n)
		if err != nil || t != r.Term {
			continue
		}
		matched := 0
		for _, pr := range r.Prs {
			if pr.Match >= n {
				matched++
			}
		}
		if matched >= r.quorum() {
			r.RaftLog.committed = n
			r.broadcastAppend()
			break
		}
	}
}

//================================== Heartbeat =======================================

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// The heartbeat also carries commitIndex and the follower follows up
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.send(pb.Message{
		MsgType: 	pb.MessageType_MsgHeartbeatResponse,
		To: 		m.From,
	})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr == nil {
		return
	}
	// The follower log si lagging behind and AppendEntries will be reissued
	if pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

//================================== Propose(Leader Writes To Log) =======================================

func (r *Raft) handlePropose(m pb.Message) error {
	if r.leadTransferee != None {
		// Leader transfer is in progress, new proposals are rejected
		return ErrProposalDropped
	}

	lastIndex := r.RaftLog.LastIndex()
	ents := make([]pb.Entry, len(m.Entries))
	for i, e := range m.Entries {
		ents[i] = pb.Entry{
			EntryType: 	e.EntryType,
			Term: 		r.Term,
			Index: 		lastIndex + uint64(i) + 1,
			Data: 		e.Data,
		}
	}
	r.RaftLog.append(ents...)

	// Update itself
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	if r.quorum() == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	} else {
		r.broadcastAppend()
	}
	return nil
}

//================================== Snapshot =======================================

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	snap := m.Snapshot
	if snap == nil || IsEmptySnap(snap) {
		return
	}

	snapIndex := snap.Metadata.Index
	snapTerm := snap.Metadata.Term

	if snapIndex <= r.RaftLog.committed {
		r.send(pb.Message{
			MsgType: 	pb.MessageType_MsgAppendResponse,
			To: 		m.From,
			Index: 		r.RaftLog.committed,
		})
		return
	}

	r.RaftLog.pendingSnapshot = snap
	r.RaftLog.entries = nil
	r.RaftLog.stabled = snapIndex
	r.RaftLog.committed = snapIndex
	r.RaftLog.applied = snapIndex

	if snap.Metadata.ConfState != nil {
		r.Prs = make(map[uint64]*Progress)
		for _, id := range snap.Metadata.ConfState.Nodes {
			r.Prs[id] = &Progress{Next: snapIndex + 1}
		}
	}

	r.send(pb.Message{
		MsgType: 	pb.MessageType_MsgAppendResponse,
		To: 		m.From,
		Index: 		snapIndex,
		LogTerm: 	snapTerm,
	})
}

//================================== Leader Transfer =======================================

// handle TransferLeader handles leader transfer request
func (r *Raft) handleTransferLeader(m pb.Message) {
	transferee := m.From
	if _, ok := r.Prs[transferee]; !ok {
		return
	}
	if transferee ==r.id {
		return
	}

	r.leadTransferee = transferee
	pr := r.Prs[transferee]

	if pr.Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(transferee)
	} else {
		r.sendAppend(transferee)
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{
		MsgType: 	pb.MessageType_MsgTimeoutNow,
		To: 		to,
	})
}

//================================== Member Change =======================================

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	delete(r.Prs, id)
	// After the node is removed, the majority condition may have been met, Try to advance the commit
	if r.State == StateLeader {
		r.maybeCommit()
	}
}