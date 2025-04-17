// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.epack.warehouse;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.BaseSlotTracker;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.QueryQueueOptions;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.qe.scheduler.slot.SlotSelectionStrategyV2;
import com.starrocks.server.GlobalStateMgr;

import java.util.Optional;

public class WarehouseSlotTracker extends BaseSlotTracker {

    public WarehouseSlotTracker(ResourceUsageMonitor resourceUsageMonitor, long warehouseId) {
        super(resourceUsageMonitor, warehouseId);
        this.slotSelectionStrategy = new SlotSelectionStrategyV2(this.warehouseId);
        this.listeners = ImmutableList.of(slotSelectionStrategy,
                new SlotListenerForPipelineDriverAllocator());
    }

    @Override
    public Optional<Integer> getMaxSlots() {
        return getOptsV2().map(QueryQueueOptions.V2::getTotalSlots);
    }

    public Optional<QueryQueueOptions.V2> getOptsV2() {
        Preconditions.checkArgument(slotSelectionStrategy instanceof SlotSelectionStrategyV2,
                "Slot selection strategy is not SlotSelectionStrategyV2");
        SlotSelectionStrategyV2 strategyV2 = (SlotSelectionStrategyV2) slotSelectionStrategy;
        strategyV2.updateOptionsPeriodically();
        return Optional.ofNullable(strategyV2.getOpts()).map(QueryQueueOptions::v2);
    }

    public SlotSelectionStrategyV2 getSlotSelectionStrategy() {
        Preconditions.checkArgument(slotSelectionStrategy instanceof SlotSelectionStrategyV2,
                "Slot selection strategy is not SlotSelectionStrategyV2");
        return (SlotSelectionStrategyV2) slotSelectionStrategy;
    }

    @Override
    public boolean requireSlot(LogicalSlot slot) {
        final BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        if (pendingSlots.size() >= slotManager.getQueryQueueMaxQueuedQueries(slot.getWarehouseId())) {
            return false;
        }

        if (slots.containsKey(slot.getSlotId())) {
            return true;
        }

        slots.put(slot.getSlotId(), slot);
        slotsOrderByExpiredTime.add(slot);
        pendingSlots.put(slot.getSlotId(), slot);

        MetricRepo.COUNTER_QUERY_QUEUE_SLOT_PENDING.increase((long) slot.getNumPhysicalSlots());

        listeners.forEach(listener -> listener.onRequireSlot(slot));
        slot.onRequire();

        return true;
    }
}
