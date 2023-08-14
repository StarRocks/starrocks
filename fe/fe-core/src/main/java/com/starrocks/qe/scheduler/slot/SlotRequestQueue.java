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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

public class SlotRequestQueue {
    private final Map<TUniqueId, Slot> slots = new HashMap<>();
    private final Set<Slot> slotsOrderByExpiredTime = new TreeSet<>(
            Comparator.comparingLong(Slot::getExpiredPendingTimeMs).thenComparing(Slot::getSlotId));

    private final Map<Long, LinkedHashMap<TUniqueId, Slot>> groupIdToSubQueue = new LinkedHashMap<>();
    private int nextGroupIndex = 0;

    private final Supplier<Boolean> isResourceOverloaded;

    public SlotRequestQueue(Supplier<Boolean> isResourceOverloaded) {
        this.isResourceOverloaded = isResourceOverloaded;
    }

    public boolean addPendingSlot(Slot slot) {
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

    public Slot removePendingSlot(TUniqueId slotId) {
        Slot slot = slots.remove(slotId);
        if (slot == null) {
            return null;
        }

        slotsOrderByExpiredTime.remove(slot);

        LinkedHashMap<TUniqueId, Slot> subQueue = groupIdToSubQueue.get(slot.getGroupId());
        subQueue.remove(slotId);

        return slot;
    }

    public List<Slot> peakExpiredSlots() {
        long nowMs = System.currentTimeMillis();
        List<Slot> expiredSlots = new ArrayList<>();
        for (Slot slot : slotsOrderByExpiredTime) {
            if (!slot.isPendingExpired(nowMs)) {
                break;
            }
            expiredSlots.add(slot);
        }
        return expiredSlots;
    }

    public List<Slot> peakSlotsToAllocate(AllocatedSlots allocatedSlots) {
        List<Slot> slotsToAllocate = Lists.newArrayList();

        if (groupIdToSubQueue.isEmpty()) {
            return slotsToAllocate;
        }

        int numAllocatedSlots = allocatedSlots.getNumSlots();
        if (!isGlobalSlotAvailable(numAllocatedSlots) || Boolean.TRUE.equals(isResourceOverloaded.get())) {
            return slotsToAllocate;
        }

        // Traverse groups round-robin from nextGroupIndex.
        int localNextGroupIndex = nextGroupIndex;
        Iterator<Map.Entry<Long, LinkedHashMap<TUniqueId, Slot>>> groupIterator = groupIdToSubQueue.entrySet().iterator();
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
            Map.Entry<Long, LinkedHashMap<TUniqueId, Slot>> entry = groupIterator.next();
            Long groupId = entry.getKey();
            LinkedHashMap<TUniqueId, Slot> subQueue = entry.getValue();

            ResourceGroup group = GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(groupId);
            int numAllocatedSlotsOfGroup = allocatedSlots.getNumSlotsOfGroup(groupId);
            int numSlotsToAllocateOfGroup = peakSlotsToAllocateFromSubQueue(
                    subQueue, group, numAllocatedSlots, numAllocatedSlotsOfGroup, slotsToAllocate);
            numAllocatedSlots += numSlotsToAllocateOfGroup;

            // If the group of the current index peaks slots to allocate, update nextGroupIndex to make the next turn starts
            // from the next group index.
            if (numSlotsToAllocateOfGroup > 0) {
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

        int numTotalSlots = group.getConcurrencyLimit();
        return numTotalSlots <= 0 || numAllocatedSlotsOfGroup < numTotalSlots;
    }

    private int peakSlotsToAllocateFromSubQueue(LinkedHashMap<TUniqueId, Slot> subQueue,
                                                ResourceGroup group,
                                                final int numAllocatedSlots,
                                                final int numAllocatedSlotsOfGroup,
                                                List<Slot> slotsToAllocate) {
        int numSlotsToAllocate = 0;
        for (Slot slot : subQueue.values()) {
            if (!isGlobalSlotAvailable(numAllocatedSlots + numSlotsToAllocate)) {
                break;
            }

            if (!isGroupSlotAvailable(group, numAllocatedSlotsOfGroup + numSlotsToAllocate)) {
                break;
            }

            slotsToAllocate.add(slot);
            numSlotsToAllocate += slot.getNumSlots();
        }

        return numSlotsToAllocate;
    }
}
