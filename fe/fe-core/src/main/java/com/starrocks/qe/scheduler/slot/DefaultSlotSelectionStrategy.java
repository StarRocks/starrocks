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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.compress.utils.Lists;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

public class DefaultSlotSelectionStrategy implements SlotSelectionStrategy {
    private static final long SWEEP_EMPTY_GROUP_INTERVAL_MS = 1000L;

    private final Map<Long, LinkedHashMap<TUniqueId, LogicalSlot>> requiringGroupIdToSubQueue = new LinkedHashMap<>();
    private final Map<Long, Integer> allocatedGroupIdToSlotCount = new HashMap<>();
    private int nextGroupIndex = 0;

    private final BooleanSupplier isGlobalResourceOverloaded;
    private final Function<Long, Boolean> isGroupResourceOverloaded;

    private long lastSweepEmptyGroupTimeMs = 0;

    public DefaultSlotSelectionStrategy(BooleanSupplier isGlobalResourceOverloaded,
                                        Function<Long, Boolean> isGroupResourceOverloaded) {
        this.isGlobalResourceOverloaded = isGlobalResourceOverloaded;
        this.isGroupResourceOverloaded = isGroupResourceOverloaded;
    }

    @Override
    public List<LogicalSlot> peakSlotsToAllocate(SlotTracker slotTracker) {
        sweepEmptyGroups();

        List<LogicalSlot> slotsToAllocate = Lists.newArrayList();

        if (requiringGroupIdToSubQueue.isEmpty()) {
            return slotsToAllocate;
        }

        int numAllocatedSlots = slotTracker.getNumAllocatedSlots();
        if (!isGlobalSlotAvailable(numAllocatedSlots) || isGlobalResourceOverloaded.getAsBoolean()) {
            return slotsToAllocate;
        }

        // Traverse groups round-robin from nextGroupIndex.
        int localNextGroupIndex = nextGroupIndex;
        Iterator<Map.Entry<Long, LinkedHashMap<TUniqueId, LogicalSlot>>> groupIterator =
                requiringGroupIdToSubQueue.entrySet().iterator();
        for (int i = 0; i < localNextGroupIndex && groupIterator.hasNext(); i++) {
            groupIterator.next();
        }

        for (int i = 0; i < requiringGroupIdToSubQueue.size(); i++) {
            if (!isGlobalSlotAvailable(numAllocatedSlots)) {
                break;
            }

            localNextGroupIndex = (localNextGroupIndex + 1) % requiringGroupIdToSubQueue.size();
            if (!groupIterator.hasNext()) {
                groupIterator = requiringGroupIdToSubQueue.entrySet().iterator();
            }
            Map.Entry<Long, LinkedHashMap<TUniqueId, LogicalSlot>> entry = groupIterator.next();
            Long groupId = entry.getKey();
            LinkedHashMap<TUniqueId, LogicalSlot> subQueue = entry.getValue();

            ResourceGroup group = GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(groupId);
            int numAllocatedSlotsOfGroup = allocatedGroupIdToSlotCount.getOrDefault(groupId, 0);
            int numSlotsToAllocate = peakSlotsToAllocateFromSubQueue(
                    subQueue, group, numAllocatedSlots, numAllocatedSlotsOfGroup, slotsToAllocate);
            numAllocatedSlots += numSlotsToAllocate;

            // If the group of the current index peaks slots to allocate, update nextGroupIndex to make the next turn starts
            // from the next group index.
            if (numSlotsToAllocate > 0) {
                nextGroupIndex = localNextGroupIndex;
            }
        }

        return slotsToAllocate;
    }

    private boolean isGlobalSlotAvailable(int numAllocatedSlots) {
        return !GlobalVariable.isQueryQueueConcurrencyLimitEffective() ||
                numAllocatedSlots < GlobalVariable.getQueryQueueConcurrencyLimit();
    }

    private boolean isGroupSlotAvailable(ResourceGroup group, int numAllocatedSlotsOfGroup) {
        if (group == null) {
            return true;
        }

        if (!GlobalVariable.isEnableGroupLevelQueryQueue()) {
            return true;
        }

        if (group.isConcurrencyLimitEffective() && numAllocatedSlotsOfGroup >= group.getConcurrencyLimit()) {
            return false;
        }

        return !isGroupResourceOverloaded.apply(group.getId());
    }

    private int peakSlotsToAllocateFromSubQueue(LinkedHashMap<TUniqueId, LogicalSlot> subQueue,
                                                ResourceGroup group,
                                                final int numAllocatedSlots,
                                                final int numAllocatedSlotsOfGroup,
                                                List<LogicalSlot> slotsToAllocate) {
        int numSlotsToAllocate = 0;
        for (LogicalSlot slot : subQueue.values()) {
            if (!isGlobalSlotAvailable(numAllocatedSlots + numSlotsToAllocate)) {
                break;
            }

            if (!isGroupSlotAvailable(group, numAllocatedSlotsOfGroup + numSlotsToAllocate)) {
                break;
            }

            slotsToAllocate.add(slot);
            numSlotsToAllocate += slot.getNumPhysicalSlots();
        }

        return numSlotsToAllocate;
    }

    @Override
    public void onRequireSlot(LogicalSlot slot) {
        requiringGroupIdToSubQueue
                .computeIfAbsent(slot.getGroupId(), k -> new LinkedHashMap<>())
                .put(slot.getSlotId(), slot);

        sweepEmptyGroups();
    }

    @Override
    public void onAllocateSlot(LogicalSlot slot) {
        LinkedHashMap<TUniqueId, LogicalSlot> subQueue = requiringGroupIdToSubQueue.get(slot.getGroupId());
        if (subQueue != null) {
            subQueue.remove(slot.getSlotId());
        }

        allocatedGroupIdToSlotCount.compute(slot.getGroupId(),
                (k, prevCount) -> prevCount == null ? slot.getNumPhysicalSlots() : prevCount + slot.getNumPhysicalSlots());

        sweepEmptyGroups();
    }

    @Override
    public void onReleaseSlot(LogicalSlot slot) {
        LinkedHashMap<TUniqueId, LogicalSlot> subQueue = requiringGroupIdToSubQueue.get(slot.getGroupId());
        boolean isRequiredSlot = subQueue != null && subQueue.remove(slot.getSlotId()) != null;
        if (!isRequiredSlot) {
            allocatedGroupIdToSlotCount.computeIfPresent(slot.getGroupId(), (k, v) -> v - slot.getNumPhysicalSlots());
        }

        sweepEmptyGroups();
    }

    @VisibleForTesting
    int getNumAllocatedSlotsOfGroup(long groupId) {
        return allocatedGroupIdToSlotCount.getOrDefault(groupId, 0);
    }

    private void sweepEmptyGroups() {
        long nowMs = System.currentTimeMillis();
        if (lastSweepEmptyGroupTimeMs + SWEEP_EMPTY_GROUP_INTERVAL_MS > nowMs) {
            return;
        }

        lastSweepEmptyGroupTimeMs = nowMs;
        requiringGroupIdToSubQueue.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        allocatedGroupIdToSlotCount.entrySet().removeIf(entry -> entry.getValue() == 0);
    }
}
