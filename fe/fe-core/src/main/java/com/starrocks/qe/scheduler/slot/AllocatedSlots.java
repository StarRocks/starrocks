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

package com.starrocks.qe.scheduler.slot;

import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class AllocatedSlots {

    private final Map<TUniqueId, LogicalSlot> slots = new LinkedHashMap<>();
    private final Set<LogicalSlot> slotsOrderByExpiredTime = new TreeSet<>(
            Comparator.comparingLong(LogicalSlot::getExpiredAllocatedTimeMs).thenComparing(LogicalSlot::getSlotId));
    private int totalSlotCount = 0;
    private final Map<Long, Integer> groupIdToSlotCount = new HashMap<>();
    private final PipelineDriverAllocator pipelineDriverAllocator = new PipelineDriverAllocator();

    public int getNumSlots() {
        return totalSlotCount;
    }

    public int getNumSlotsOfGroup(long groupId) {
        return groupIdToSlotCount.getOrDefault(groupId, 0);
    }

    public void allocateSlot(LogicalSlot slot) {
        slots.put(slot.getSlotId(), slot);
        slotsOrderByExpiredTime.add(slot);

        pipelineDriverAllocator.allocate(slot);
        totalSlotCount += slot.getNumPhysicalSlots();
        groupIdToSlotCount.compute(slot.getGroupId(),
                (k, prevCount) -> prevCount == null ? slot.getNumPhysicalSlots() : prevCount + slot.getNumPhysicalSlots());
    }

    public LogicalSlot releaseSlot(TUniqueId slotId) {
        LogicalSlot slot = slots.remove(slotId);
        if (slot == null) {
            return null;
        }

        slotsOrderByExpiredTime.remove(slot);

        totalSlotCount -= slot.getNumPhysicalSlots();
        pipelineDriverAllocator.release(slot);
        groupIdToSlotCount.computeIfPresent(slot.getGroupId(), (k, v) -> v - slot.getNumPhysicalSlots());

        return slot;
    }

    public List<LogicalSlot> peakExpiredSlots() {
        long nowMs = System.currentTimeMillis();
        List<LogicalSlot> expiredSlots = new ArrayList<>();
        for (LogicalSlot slot : slotsOrderByExpiredTime) {
            if (!slot.isAllocatedExpired(nowMs)) {
                break;
            }
            expiredSlots.add(slot);
        }
        return expiredSlots;
    }

    public long getMinExpiredTimeMs() {
        if (slotsOrderByExpiredTime.isEmpty()) {
            return 0;
        }
        return slotsOrderByExpiredTime.iterator().next().getExpiredAllocatedTimeMs();
    }
}
