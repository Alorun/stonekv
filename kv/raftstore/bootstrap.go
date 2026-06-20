package raftstore

import (
	"bytes"

	"github.com/Alorun/stonekv/kv/raftstore/meta"
	"github.com/Alorun/stonekv/kv/util/engine_util"
	"github.com/Alorun/stonekv/kv/util/rocketdb"
	"github.com/Alorun/stonekv/proto/pkg/eraftpb"
	"github.com/Alorun/stonekv/proto/pkg/metapb"
	rspb "github.com/Alorun/stonekv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
)

const (
	InitEpochVer     uint64 = 1
	InitEpochConfVer uint64 = 1
)

func isRangeEmpty(engine *rocketdb.DB, startKey, endKey []byte) (bool, error) {
	var hasData bool
	txn := engine_util.NewTxn(engine)
	defer txn.Discard()
	it := txn.NewRawIterator()
	defer it.Close()
	if len(startKey) == 0 {
		it.SeekToFirst()
	} else {
		it.Seek(startKey)
	}
	if it.Valid() {
		if bytes.Compare(it.Key(), endKey) < 0 {
			hasData = true
		}
	}
	return !hasData, nil
}

func BootstrapStore(engines *engine_util.Engines, clusterID, storeID uint64) error {
	ident := new(rspb.StoreIdent)
	empty, err := isRangeEmpty(engines.Kv, meta.MinKey, meta.MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("kv store is not empty and has already had data.")
	}
	empty, err = isRangeEmpty(engines.Raft, meta.MinKey, meta.MaxKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("raft store is not empty and has already had data.")
	}
	ident.ClusterId = clusterID
	ident.StoreId = storeID
	err = engine_util.PutMeta(engines.Kv, meta.StoreIdentKey, ident)
	if err != nil {
		return err
	}
	return nil
}

func PrepareBootstrap(engines *engine_util.Engines, storeID, regionID, peerID uint64) (*metapb.Region, error) {
	region := &metapb.Region{
		Id:       regionID,
		StartKey: []byte{},
		EndKey:   []byte{},
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochVer,
			ConfVer: InitEpochConfVer,
		},
		Peers: []*metapb.Peer{
			{
				Id:      peerID,
				StoreId: storeID,
			},
		},
	}
	err := PrepareBootstrapCluster(engines, region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

func PrepareBootstrapCluster(engines *engine_util.Engines, region *metapb.Region) error {
	state := new(rspb.RegionLocalState)
	state.Region = region
	kvWB := new(engine_util.WriteBatch)
	kvWB.SetMeta(meta.PrepareBootstrapKey, state)
	kvWB.SetMeta(meta.RegionStateKey(region.Id), state)
	writeInitialApplyState(kvWB, region.Id)
	err := engines.WriteKV(kvWB)
	if err != nil {
		return err
	}
	raftWB := new(engine_util.WriteBatch)
	writeInitialRaftState(raftWB, region.Id)
	err = engines.WriteRaft(raftWB)
	if err != nil {
		return err
	}
	return nil
}

func writeInitialApplyState(kvWB *engine_util.WriteBatch, regionID uint64) {
	applyState := &rspb.RaftApplyState{
		AppliedIndex: meta.RaftInitLogIndex,
		TruncatedState: &rspb.RaftTruncatedState{
			Index: meta.RaftInitLogIndex,
			Term:  meta.RaftInitLogTerm,
		},
	}
	kvWB.SetMeta(meta.ApplyStateKey(regionID), applyState)
}

func writeInitialRaftState(raftWB *engine_util.WriteBatch, regionID uint64) {
	raftState := &rspb.RaftLocalState{
		HardState: &eraftpb.HardState{
			Term:   meta.RaftInitLogTerm,
			Commit: meta.RaftInitLogIndex,
		},
		LastIndex: meta.RaftInitLogIndex,
	}
	raftWB.SetMeta(meta.RaftStateKey(regionID), raftState)
}

func ClearPrepareBootstrap(engines *engine_util.Engines, regionID uint64) error {
	raftWB := new(engine_util.WriteBatch)
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	if err := engines.WriteRaft(raftWB); err != nil {
		return errors.WithStack(err)
	}
	wb := new(engine_util.WriteBatch)
	wb.DeleteMeta(meta.PrepareBootstrapKey)
	// should clear raft initial state too.
	wb.DeleteMeta(meta.RegionStateKey(regionID))
	wb.DeleteMeta(meta.ApplyStateKey(regionID))
	if err := engines.WriteKV(wb); err != nil {
		return err
	}
	return nil
}

func ClearPrepareBootstrapState(engines *engine_util.Engines) error {
	kvWB := new(engine_util.WriteBatch)
	kvWB.DeleteMeta(meta.PrepareBootstrapKey)
	return errors.WithStack(engines.WriteKV(kvWB))
}
