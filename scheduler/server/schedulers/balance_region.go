// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/Alorun/stonekv/scheduler/server/core"
	"github.com/Alorun/stonekv/scheduler/server/schedule"
	"github.com/Alorun/stonekv/scheduler/server/schedule/operator"
	"github.com/Alorun/stonekv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	suitableStores := make([]*core.StoreInfo, 0)
	// Exclude failed or long-disconnected nodes.
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}

	if len(suitableStores) < 2 {
		return nil
	}

	// Sort by every size of region (ascending).
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})

	// Get the source stores to migrate from.
	// Priority: pending -> follower -> leader.
	var sourecStore *core.StoreInfo
	var region *core.RegionInfo
	for _, store := range suitableStores {
		id := store.GetID()

		cluster.GetPendingRegionsWithLock(id, func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		})
		if region == nil {
			cluster.GetFollowersWithLock(id, func(rc core.RegionsContainer) {
				region = rc.RandomRegion(nil, nil)
			})
		}
		if region == nil {
			cluster.GetLeadersWithLock(id, func(rc core.RegionsContainer) {
				region = rc.RandomRegion(nil, nil)
			})
		}

		if region != nil {
			sourecStore = store
			break
		}
	}

	if region == nil {
		return nil
	}

	if len(region.GetStoreIds()) != cluster.GetMaxReplicas() {
		return nil
	}

	// Select the target store.
	regionStoreIds := region.GetStoreIds()
	var targetStore *core.StoreInfo
	for i := len(suitableStores) - 1; i >= 0; i-- {
		if _, ok := regionStoreIds[suitableStores[i].GetID()]; !ok {
			targetStore = suitableStores[i]
			break
		}
	}

	if targetStore == nil {
		return nil
	}

	// Check whether the load difference between the source and target store justifies migration to prevent oscillation.
	if sourecStore.GetRegionSize() - targetStore.GetRegionSize() <= 2 * region.GetApproximateSize() {
		return nil
	}

	// Allocate a new peer on the target store and generate a MovePeer operator.
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}
	op, err := operator.CreateMovePeerOperator(
		"balance-region",
		cluster,
		region,
		operator.OpBalance,
		sourecStore.GetID(),
		targetStore.GetID(),
		newPeer.GetId(),
	)
	if err != nil {
		return nil
	}
	return op
}