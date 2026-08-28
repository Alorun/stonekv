package server

import (
	"github.com/Alorun/stonekv/proto/pkg/metapb"
	"github.com/Alorun/stonekv/scheduler/server/core"
	. "github.com/pingcap/check"
)

func (s *testCoordinatorSuite) TestPrepareCheckerCountsUniqueRegionHeartbeats(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestCluster(opt)

	for storeID := uint64(1); storeID <= 3; storeID++ {
		c.Assert(cluster.addLeaderStore(storeID, 0), IsNil)
	}
	for regionID := uint64(1); regionID <= 10; regionID++ {
		c.Assert(cluster.LoadRegion(regionID, 1, 2, 3), IsNil)
	}

	region := cluster.GetRegion(1)
	heartbeat := region.Clone(core.WithLeader(region.GetPeers()[0]))
	for i := 0; i < 8; i++ {
		c.Assert(cluster.processRegionHeartbeat(heartbeat), IsNil)
	}
	c.Assert(cluster.isPrepared(), IsFalse,
		Commentf("duplicate heartbeats for one region must not make the cluster ready"))

	for regionID := uint64(2); regionID <= 10; regionID++ {
		region = cluster.GetRegion(regionID)
		heartbeat = region.Clone(core.WithLeader(region.GetPeers()[0]))
		c.Assert(cluster.processRegionHeartbeat(heartbeat), IsNil)
	}
	c.Assert(cluster.isPrepared(), IsTrue)
}

func (s *testCoordinatorSuite) TestPrepareCheckerTracksLatestRegionPeers(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestCluster(opt)

	for storeID := uint64(1); storeID <= 4; storeID++ {
		c.Assert(cluster.addLeaderStore(storeID, 0), IsNil)
	}
	c.Assert(cluster.LoadRegion(1, 1, 2, 3), IsNil)
	region := cluster.GetRegion(1)
	c.Assert(cluster.processRegionHeartbeat(region.Clone(core.WithLeader(region.GetPeers()[0]))), IsNil)

	peer4, err := cluster.AllocPeer(4)
	c.Assert(err, IsNil)
	updatedPeers := []*metapb.Peer{region.GetPeers()[0], region.GetPeers()[1], peer4}
	updated := region.Clone(
		core.WithIncConfVer(),
		core.SetPeers(updatedPeers),
		core.WithLeader(updatedPeers[0]),
	)
	c.Assert(cluster.processRegionHeartbeat(updated), IsNil)

	c.Assert(cluster.prepareChecker.sum, Equals, 1)
	c.Assert(cluster.prepareChecker.reactiveRegions[1], Equals, 1)
	c.Assert(cluster.prepareChecker.reactiveRegions[2], Equals, 1)
	c.Assert(cluster.prepareChecker.reactiveRegions[3], Equals, 0)
	c.Assert(cluster.prepareChecker.reactiveRegions[4], Equals, 1)
}

func (s *testCoordinatorSuite) TestPrepareCheckerRejectsStaleHeartbeatWithoutCountingIt(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	cluster := newTestCluster(opt)
	for storeID := uint64(1); storeID <= 3; storeID++ {
		c.Assert(cluster.addLeaderStore(storeID, 0), IsNil)
	}
	c.Assert(cluster.LoadRegion(1, 1, 2, 3), IsNil)

	original := cluster.GetRegion(1)
	fresh := original.Clone(core.WithIncVersion(), core.WithLeader(original.GetPeers()[0]))
	c.Assert(cluster.processRegionHeartbeat(fresh), IsNil)
	sum := cluster.prepareChecker.sum
	perStore := map[uint64]int{
		1: cluster.prepareChecker.reactiveRegions[1],
		2: cluster.prepareChecker.reactiveRegions[2],
		3: cluster.prepareChecker.reactiveRegions[3],
	}

	c.Assert(cluster.processRegionHeartbeat(original), NotNil)
	c.Assert(cluster.prepareChecker.sum, Equals, sum)
	for storeID, count := range perStore {
		c.Assert(cluster.prepareChecker.reactiveRegions[storeID], Equals, count)
	}
}
