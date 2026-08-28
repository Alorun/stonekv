package test_raftstore

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Alorun/stonekv/kv/config"
	"github.com/Alorun/stonekv/kv/util/engine_util"
	"github.com/Alorun/stonekv/proto/pkg/metapb"
	"github.com/Alorun/stonekv/proto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/require"
)

func waitForRaftStoreValue(t *testing.T, cluster *Cluster, storeID uint64, cf string, key, value []byte) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		got, err := engine_util.GetCF(cluster.engines[storeID].Kv, cf, key)
		if err == nil && bytes.Equal(got, value) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, err := engine_util.GetCF(cluster.engines[storeID].Kv, cf, key)
	t.Fatalf("store %d did not converge: value=%q err=%v, want=%q", storeID, got, err, value)
}

func waitForRaftStoreMissing(t *testing.T, cluster *Cluster, storeID uint64, cf string, key []byte) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		_, err := engine_util.GetCF(cluster.engines[storeID].Kv, cf, key)
		if err == engine_util.ErrKeyNotFound {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	value, err := engine_util.GetCF(cluster.engines[storeID].Kv, cf, key)
	t.Fatalf("store %d retained key %q: value=%q err=%v", storeID, key, value, err)
}

func waitForDifferentLeader(t *testing.T, cluster *Cluster, regionID, oldStoreID uint64) *metapb.Peer {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		_, leader, err := cluster.schedulerClient.GetRegionByID(context.Background(), regionID)
		if err == nil && leader != nil && leader.GetStoreId() != oldStoreID {
			return leader
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("region %d did not elect a leader after store %d stopped", regionID, oldStoreID)
	return nil
}

func TestRaftStoreEmptyValueSurvivesWholeClusterRestart(t *testing.T) {
	cluster := NewTestCluster(3, config.NewTestConfig())
	cluster.Start()
	defer cluster.Shutdown()

	key := []byte("empty/replicated")
	cluster.MustPut(key, []byte{})
	for storeID := uint64(1); storeID <= 3; storeID++ {
		waitForRaftStoreValue(t, cluster, storeID, engine_util.CfDefault, key, []byte{})
	}

	for storeID := uint64(1); storeID <= 3; storeID++ {
		cluster.StopServer(storeID)
	}
	for storeID := uint64(1); storeID <= 3; storeID++ {
		cluster.StartServer(storeID)
	}
	cluster.MustPut([]byte("restart/barrier"), []byte("committed"))

	for storeID := uint64(1); storeID <= 3; storeID++ {
		waitForRaftStoreValue(t, cluster, storeID, engine_util.CfDefault, key, []byte{})
		waitForRaftStoreValue(t, cluster, storeID, engine_util.CfDefault, []byte("restart/barrier"), []byte("committed"))
	}
}

func TestRaftStoreWritesContinueAcrossLeaderTransfersAndFailure(t *testing.T) {
	cluster := NewTestCluster(3, config.NewTestConfig())
	cluster.Start()
	defer cluster.Shutdown()

	key := []byte("order/state")
	region := cluster.GetRegion(key)
	for round := 0; round < 4; round++ {
		leader := cluster.LeaderOfRegion(region.GetId())
		var target *metapb.Peer
		for _, peer := range region.GetPeers() {
			if peer.GetId() != leader.GetId() {
				target = peer
				break
			}
		}
		require.NotNil(t, target)
		cluster.MustTransferLeader(region.GetId(), target)
		cluster.MustPut(key, []byte(fmt.Sprintf("version-%d", round)))
	}

	leader := cluster.LeaderOfRegion(region.GetId())
	stoppedStore := leader.GetStoreId()
	cluster.StopServer(stoppedStore)
	waitForDifferentLeader(t, cluster, region.GetId(), stoppedStore)
	cluster.MustPut(key, []byte("written-after-leader-failure"))
	cluster.StartServer(stoppedStore)
	cluster.MustPut([]byte("leader-failure/barrier"), []byte("ok"))

	for storeID := uint64(1); storeID <= 3; storeID++ {
		waitForRaftStoreValue(t, cluster, storeID, engine_util.CfDefault, key, []byte("written-after-leader-failure"))
	}
}

func TestRaftStoreLaggingPeerCatchesUpAtomicMultiCFCommand(t *testing.T) {
	cluster := NewTestCluster(3, config.NewTestConfig())
	cluster.Start()
	defer cluster.Shutdown()

	key := []byte("txn/atomic-key")
	region := cluster.GetRegion(key)
	leader := cluster.LeaderOfRegion(region.GetId())
	var lagging *metapb.Peer
	for _, peer := range region.GetPeers() {
		if peer.GetId() != leader.GetId() {
			lagging = peer
			break
		}
	}
	require.NotNil(t, lagging)

	majority := make([]uint64, 0, 2)
	for _, peer := range region.GetPeers() {
		if peer.GetStoreId() != lagging.GetStoreId() {
			majority = append(majority, peer.GetStoreId())
		}
	}
	cluster.AddFilter(&PartitionFilter{s1: []uint64{lagging.GetStoreId()}, s2: majority})

	requests := []*raft_cmdpb.Request{
		NewPutCfCmd(engine_util.CfDefault, key, []byte("value-payload")),
		NewPutCfCmd(engine_util.CfLock, key, []byte("lock-payload")),
		NewPutCfCmd(engine_util.CfWrite, key, []byte("write-payload")),
	}
	resp, _ := cluster.Request(key, requests, 5*time.Second)
	require.Empty(t, resp.Header.GetError())
	require.Len(t, resp.Responses, len(requests))
	for _, cf := range []string{engine_util.CfDefault, engine_util.CfLock, engine_util.CfWrite} {
		_, err := engine_util.GetCF(cluster.engines[lagging.GetStoreId()].Kv, cf, key)
		require.ErrorIs(t, err, engine_util.ErrKeyNotFound)
	}

	cluster.ClearFilters()
	cluster.MustTransferLeader(region.GetId(), lagging)
	values := map[string][]byte{
		engine_util.CfDefault: []byte("value-payload"),
		engine_util.CfLock:    []byte("lock-payload"),
		engine_util.CfWrite:   []byte("write-payload"),
	}
	for cf, value := range values {
		require.Equal(t, value, cluster.GetCF(cf, key))
	}

	cluster.StopServer(lagging.GetStoreId())
	waitForDifferentLeader(t, cluster, region.GetId(), lagging.GetStoreId())
	cluster.StartServer(lagging.GetStoreId())
	cluster.MustPut([]byte("multi-cf/barrier"), []byte("ok"))
	for storeID := uint64(1); storeID <= 3; storeID++ {
		for cf, value := range values {
			waitForRaftStoreValue(t, cluster, storeID, cf, key, value)
		}
	}
}

func TestRaftStoreDeleteRemainsDeletedAfterFollowerRecovery(t *testing.T) {
	cfg := config.NewTestConfig()
	cfg.RaftLogGcCountLimit = 10
	cluster := NewTestCluster(3, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	key := []byte("session/to-delete")
	cluster.MustPut(key, []byte("active"))
	region := cluster.GetRegion(key)
	leader := cluster.LeaderOfRegion(region.GetId())
	var laggingStore uint64
	for _, peer := range region.GetPeers() {
		if peer.GetId() != leader.GetId() {
			laggingStore = peer.GetStoreId()
			break
		}
	}
	majority := make([]uint64, 0, 2)
	for _, peer := range region.GetPeers() {
		if peer.GetStoreId() != laggingStore {
			majority = append(majority, peer.GetStoreId())
		}
	}
	cluster.AddFilter(&PartitionFilter{s1: []uint64{laggingStore}, s2: majority})
	cluster.MustDelete(key)
	for i := 0; i < 25; i++ {
		cluster.MustPut([]byte(fmt.Sprintf("snapshot/filler/%02d", i)), []byte("payload"))
	}
	cluster.ClearFilters()
	cluster.MustPut([]byte("snapshot/catch-up"), []byte("barrier"))

	waitForRaftStoreMissing(t, cluster, laggingStore, engine_util.CfDefault, key)
	cluster.StopServer(laggingStore)
	cluster.StartServer(laggingStore)
	cluster.MustPut([]byte("snapshot/restart-barrier"), []byte("ok"))
	waitForRaftStoreMissing(t, cluster, laggingStore, engine_util.CfDefault, key)
}
