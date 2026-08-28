package raft

import (
	"bytes"
	"reflect"
	"testing"

	pb "github.com/Alorun/stonekv/proto/pkg/eraftpb"
	"github.com/stretchr/testify/require"
)

func TestRaftIsolatedLeaderCannotCommitAndItsLogIsOverwritten(t *testing.T) {
	network := newNetwork(nil, nil, nil)
	network.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	oldLeader := network.peers[1].(*Raft)
	require.Equal(t, StateLeader, oldLeader.State)
	committedBeforePartition := oldLeader.RaftLog.committed

	network.isolate(1)
	network.send(pb.Message{
		From: 1, To: 1, MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{Data: []byte("stale-minority-write")}},
	})
	require.Equal(t, committedBeforePartition, oldLeader.RaftLog.committed)

	network.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})
	newLeader := network.peers[2].(*Raft)
	require.Equal(t, StateLeader, newLeader.State)
	network.send(pb.Message{
		From: 2, To: 2, MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{Data: []byte("majority-write")}},
	})
	require.Equal(t, newLeader.RaftLog.LastIndex(), newLeader.RaftLog.committed)

	network.recover()
	network.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgBeat})
	network.send(pb.Message{
		From: 2, To: 2, MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{Data: []byte("post-recovery-barrier")}},
	})

	wantEntries := newLeader.RaftLog.allEntries()
	for id, peer := range network.peers {
		raft := peer.(*Raft)
		require.True(t, reflect.DeepEqual(wantEntries, raft.RaftLog.allEntries()), "peer %d log diverged", id)
		require.Equal(t, newLeader.RaftLog.committed, raft.RaftLog.committed, "peer %d commit diverged", id)
		for _, entry := range raft.RaftLog.allEntries() {
			require.False(t, bytes.Equal(entry.Data, []byte("stale-minority-write")), "peer %d retained stale write", id)
		}
	}
}

func TestRaftAllowsOnlyOnePendingConfigurationChange(t *testing.T) {
	raft := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	require.NoError(t, raft.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup}))
	require.Equal(t, StateLeader, raft.State)

	first, err := (&pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 2}).Marshal()
	require.NoError(t, err)
	second, err := (&pb.ConfChange{ChangeType: pb.ConfChangeType_AddNode, NodeId: 3}).Marshal()
	require.NoError(t, err)

	require.NoError(t, raft.Step(pb.Message{
		From: 1, To: 1, MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{
			{EntryType: pb.EntryType_EntryConfChange, Data: first},
			{EntryType: pb.EntryType_EntryConfChange, Data: second},
		},
	}))

	entries := raft.RaftLog.allEntries()
	require.Len(t, entries, 3)
	for i, entry := range entries {
		require.Equal(t, uint64(i+1), entry.Index)
		require.Equal(t, raft.Term, entry.Term)
	}
	require.Equal(t, pb.EntryType_EntryConfChange, entries[1].EntryType)
	require.Equal(t, first, entries[1].Data)
	require.Equal(t, pb.EntryType_EntryNormal, entries[2].EntryType)
	require.Empty(t, entries[2].Data)
	require.Equal(t, entries[1].Index, raft.PendingConfIndex)
}

func TestRaftRejectsWritesWhileLeadershipTransferIsPending(t *testing.T) {
	network := newNetwork(nil, nil, nil)
	network.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	leader := network.peers[1].(*Raft)
	require.Equal(t, StateLeader, leader.State)

	network.isolate(3)
	network.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	require.Equal(t, uint64(3), leader.leadTransferee)
	lastIndex := leader.RaftLog.LastIndex()

	err := leader.Step(pb.Message{
		From: 1, To: 1, MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{Data: []byte("must-not-be-appended")}},
	})
	require.ErrorIs(t, err, ErrProposalDropped)
	require.Equal(t, lastIndex, leader.RaftLog.LastIndex())

	network.send(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	require.Equal(t, StateLeader, network.peers[2].(*Raft).State)
	network.recover()
	network.send(pb.Message{
		From: 2, To: 2, MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{Data: []byte("accepted-by-new-leader")}},
	})
	require.Equal(t, network.peers[2].(*Raft).RaftLog.LastIndex(), network.peers[2].(*Raft).RaftLog.committed)
}
