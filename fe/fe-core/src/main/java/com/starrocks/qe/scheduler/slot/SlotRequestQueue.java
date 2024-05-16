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

import com.starrocks.catalog.ResourceGroup;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.compress.utils.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

public class SlotRequestQueue {
    private static final Logger LOG = LogManager.getLogger(SlotRequestQueue.class);

    private final Map<TUniqueId, LogicalSlot> slots = new HashMap<>();
    private final Set<LogicalSlot> slotsOrderByExpiredTime = new TreeSet<>(
            Comparator.comparingLong(LogicalSlot::getExpiredPendingTimeMs).thenComparing(LogicalSlot::getSlotId));

    private final Map<Long, LinkedHashMap<TUniqueId, LogicalSlot>> groupIdToSubQueue = new LinkedHashMap<>();
    private int nextGroupIndex = 0;

    private final BooleanSupplier isGlobalResourceOverloaded;
    private final Function<Long, Boolean> isGroupResourceOverloaded;

    public SlotRequestQueue(BooleanSupplier isGlobalResourceOverloaded, Function<Long, Boolean> isGroupResourceOverloaded) {
        this.isGlobalResourceOverloaded = isGlobalResourceOverloaded;
        this.isGroupResourceOverloaded = isGroupResourceOverloaded;
    }

    public boolean addPendingSlot(LogicalSlot slot) {
        if (GlobalVariable.isQueryQueueMaxQueuedQueriesEffective() &&
                slots.size() >= GlobalVariable.getQueryQueueMaxQueuedQueries()) {
            return false;
        }

        slots.put(slot.getSlotId(), slot);
        slotsOrderByExpiredTime.add(slot);
        groupIdToSubQueue.computeIfAbsent(slot.getGroupId(), k -> new LinkedHashMap<>())
                .put(slot.getSlotId(), slot);
        return true;
    }

    public LogicalSlot removePendingSlot(TUniqueId slotId) {
        LogicalSlot slot = slots.remove(slotId);
        if (slot == null) {
            return null;
        }

        slotsOrderByExpiredTime.remove(slot);

        LinkedHashMap<TUniqueId, LogicalSlot> subQueue = groupIdToSubQueue.get(slot.getGroupId());
        subQueue.remove(slotId);

        return slot;
    }

    public List<LogicalSlot> peakExpiredSlots() {
        long nowMs = System.currentTimeMillis();
        List<LogicalSlot> expiredSlots = new ArrayList<>();
        for (LogicalSlot slot : slotsOrderByExpiredTime) {
            if (!slot.isPendingExpired(nowMs)) {
                break;
            }
            expiredSlots.add(slot);
        }
        return expiredSlots;
    }

    public List<LogicalSlot> peakSlotsToAllocate(AllocatedSlots allocatedSlots) {
        List<LogicalSlot> slotsToAllocate = Lists.newArrayList();

        if (groupIdToSubQueue.isEmpty()) {
            return slotsToAllocate;
        }

        int numAllocatedSlots = allocatedSlots.getNumSlots();
        if (!isGlobalSlotAvailable(numAllocatedSlots) || isGlobalResourceOverloaded.getAsBoolean()) {
            return slotsToAllocate;
        }

        // Traverse groups round-robin from nextGroupIndex.
        int localNextGroupIndex = nextGroupIndex;
        Iterator<Map.Entry<Long, LinkedHashMap<TUniqueId, LogicalSlot>>> groupIterator = groupIdToSubQueue.entrySet().iterator();
        for (int i = 0; i < localNextGroupIndex && groupIterator.hasNext(); i++) {
            groupIterator.next();
        }

        for (int i = 0; i < groupIdToSubQueue.size(); i++) {
            if (!isGlobalSlotAvailable(numAllocatedSlots)) {
                break;
            }

            localNextGroupIndex = (localNextGroupIndex + 1) % groupIdToSubQueue.size();
            if (!groupIterator.hasNext()) {
                groupIterator = groupIdToSubQueue.entrySet().iterator();
            }
            Map.Entry<Long, LinkedHashMap<TUniqueId, LogicalSlot>> entry = groupIterator.next();
            Long groupId = entry.getKey();
            LinkedHashMap<TUniqueId, LogicalSlot> subQueue = entry.getValue();

            ResourceGroup group = GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(groupId);
            int numAllocatedSlotsOfGroup = allocatedSlots.getNumSlotsOfGroup(groupId);
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
}
