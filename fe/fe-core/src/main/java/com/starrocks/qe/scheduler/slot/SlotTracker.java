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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.Warehouse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * SlotTracker is used to track the status of slots.
 * It is responsible for managing the life cycle of slots, including requiring, allocating, and releasing slots.
 *
 * <p> All the methods except {@link #getSlots()} are not thread-safe.
 */
public class SlotTracker {
    private final ConcurrentMap<TUniqueId, LogicalSlot> slots = new ConcurrentHashMap<>();

    private final Set<LogicalSlot> slotsOrderByExpiredTime = new TreeSet<>(
            Comparator.comparingLong(LogicalSlot::getExpiredPendingTimeMs)
                    .thenComparing(LogicalSlot::getSlotId));

    private final Map<TUniqueId, LogicalSlot> pendingSlots = new HashMap<>();
    private final Map<TUniqueId, LogicalSlot> allocatedSlots = new HashMap<>();
    private final long warehouseId;
    private final List<Listener> listeners;
    private final SlotSelectionStrategy slotSelectionStrategy;
    private int numAllocatedSlots = 0;
    private Optional<String> warehouseName = Optional.empty();

    public SlotTracker(ResourceUsageMonitor resourceUsageMonitor, long warehouseId) {
        this.warehouseId = warehouseId;
        this.slotSelectionStrategy = createSlotSelectionStrategy(resourceUsageMonitor);
        this.listeners = ImmutableList.of(slotSelectionStrategy,
                new SlotListenerForPipelineDriverAllocator());
    }

    @VisibleForTesting
    public SlotTracker(List<Listener> listeners) {
        this.warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        this.listeners = listeners;
        if (listeners.isEmpty()) {
            this.slotSelectionStrategy = createSlotSelectionStrategy(GlobalStateMgr.getCurrentState().getResourceUsageMonitor());
        } else {
            Preconditions.checkArgument(!listeners.isEmpty());
            Preconditions.checkArgument(listeners.get(0) instanceof SlotSelectionStrategy);
            this.slotSelectionStrategy = (SlotSelectionStrategy) listeners.get(0);
        }
    }

    private SlotSelectionStrategy createSlotSelectionStrategy(ResourceUsageMonitor resourceUsageMonitor) {
        final boolean enableQueryQueue = QueryQueueOptions.isEnableQueryQueue(warehouseId);
        if (enableQueryQueue) {
            return new SlotSelectionStrategyV2(this.warehouseId);
        } else {
            return new DefaultSlotSelectionStrategy(
                    resourceUsageMonitor::isGlobalResourceOverloaded, resourceUsageMonitor::isGroupResourceOverloaded);
        }
    }

    public String getWarehouseName() {
        if (warehouseName.isEmpty()) {
            Warehouse warehouse = QueryQueueOptions.getWarehouse(this.warehouseId);
            this.warehouseName = Optional.of(warehouse.getName());
        }
        return warehouseName.orElse("");
    }

    public long getQueuePendingLength() {
        return pendingSlots.size();
    }

    public long getAllocatedLength() {
        return allocatedSlots.size();
    }

    public Optional<Integer> getMaxRequiredSlots() {
        return pendingSlots.values().stream()
                .map(LogicalSlot::getNumPhysicalSlots)
                .max(Integer::compareTo);
    }

    public Optional<Integer> getSumRequiredSlots() {
        return pendingSlots.values().stream()
                .map(LogicalSlot::getNumPhysicalSlots)
                .reduce(Integer::sum);
    }

    public Optional<Integer> getMaxSlots() {
        return getOptsV2().map(QueryQueueOptions.V2::getTotalSlots);
    }

    public Optional<Integer> getRemainSlots() {
        return getMaxSlots().map(s -> Math.max(0, s - numAllocatedSlots));
    }

    public long getMaxQueueQueueLength() {
        return QueryQueueOptions.getQueryQueueMaxQueuedQueries(warehouseId);
    }

    public long getMaxQueuePendingTimeSecond() {
        return QueryQueueOptions.getQueryQueuePendingTimeoutSecond(warehouseId);
    }

    private Optional<QueryQueueOptions.V2> getOptsV2() {
        if (slotSelectionStrategy == null || !(slotSelectionStrategy instanceof SlotSelectionStrategyV2)) {
            return Optional.empty();
        }
        SlotSelectionStrategyV2 strategyV2 = (SlotSelectionStrategyV2) slotSelectionStrategy;
        strategyV2.updateOptionsPeriodically();
        return Optional.ofNullable(strategyV2.getOpts()).map(QueryQueueOptions::v2);
    }

    /**
     * Add a slot requirement.
     * @param slot The required slot.
     * @return True if the slot is required successfully or already required , false if the query queue is full.
     */
    public boolean requireSlot(LogicalSlot slot) {
        if (GlobalVariable.isQueryQueueMaxQueuedQueriesEffective() &&
                pendingSlots.size() >= QueryQueueOptions.getQueryQueueMaxQueuedQueries(slot.getWarehouseId())) {
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

    /**
     * Allocate a slot which has already been required.
     * If the slot has not been required, this method has no effect.
     * @param slot The slot to be allocated.
     */
    public void allocateSlot(LogicalSlot slot) {
        TUniqueId slotId = slot.getSlotId();
        if (!slots.containsKey(slotId)) {
            return;
        }

        if (pendingSlots.remove(slotId) == null) {
            return;
        }
        MetricRepo.COUNTER_QUERY_QUEUE_SLOT_PENDING.increase((long) -slot.getNumPhysicalSlots());

        if (allocatedSlots.put(slotId, slot) != null) {
            return;
        }

        MetricRepo.COUNTER_QUERY_QUEUE_SLOT_RUNNING.increase((long) slot.getNumPhysicalSlots());
        numAllocatedSlots += slot.getNumPhysicalSlots();

        listeners.forEach(listener -> listener.onAllocateSlot(slot));
        slot.onAllocate();
    }

    /**
     * Release a slot which has already been allocated or required.
     * @param slotId The slot id to be released.
     * @return The released slot, or null if the slot has not been required or allocated.
     */
    public LogicalSlot releaseSlot(TUniqueId slotId) {
        LogicalSlot slot = slots.remove(slotId);
        if (slot == null) {
            return null;
        }

        slotsOrderByExpiredTime.remove(slot);

        if (allocatedSlots.remove(slotId) != null) {
            numAllocatedSlots -= slot.getNumPhysicalSlots();
            MetricRepo.COUNTER_QUERY_QUEUE_SLOT_RUNNING.increase((long) -slot.getNumPhysicalSlots());
        } else {
            if (pendingSlots.remove(slotId) != null) {
                MetricRepo.COUNTER_QUERY_QUEUE_SLOT_PENDING.increase((long) -slot.getNumPhysicalSlots());
            }
        }

        listeners.forEach(listener -> listener.onReleaseSlot(slot));
        slot.onRelease();

        return slot;
    }

    public Collection<LogicalSlot> peakSlotsToAllocate() {
        return slotSelectionStrategy.peakSlotsToAllocate(this);
    }

    /**
     * Peak all the expired slots.
     *
     * <p> Note that this method does not remove the expired slots from the tracker,
     * and {@link #releaseSlot} should be called to release these slots after peaking.
     * @return The expired slots.
     */
    public List<LogicalSlot> peakExpiredSlots() {
        final long nowMs = System.currentTimeMillis();
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
        return slotsOrderByExpiredTime.iterator().next().getExpiredPendingTimeMs();
    }

    public double getEarliestQueryWaitTimeSecond() {
        return slots.values().stream().map(LogicalSlot::getStartTimeMs).min(Long::compareTo)
                .map(t -> (System.currentTimeMillis() - t) / 1000.0).orElse(0.0);
    }

    public Collection<LogicalSlot> getSlots() {
        return slots.values();
    }

    public LogicalSlot getSlot(TUniqueId slotId) {
        return slots.get(slotId);
    }

    public int getNumAllocatedSlots() {
        return numAllocatedSlots;
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public interface Listener {
        void onRequireSlot(LogicalSlot slot);

        void onAllocateSlot(LogicalSlot slot);

        void onReleaseSlot(LogicalSlot slot);
    }

    private static class SlotListenerForPipelineDriverAllocator implements SlotTracker.Listener {
        private final PipelineDriverAllocator pipelineDriverAllocator = new PipelineDriverAllocator();

        @Override
        public void onRequireSlot(LogicalSlot slot) {
            // Do nothing.
        }

        @Override
        public void onAllocateSlot(LogicalSlot slot) {
            pipelineDriverAllocator.allocate(slot);
        }

        @Override
        public void onReleaseSlot(LogicalSlot slot) {
            pipelineDriverAllocator.release(slot);
        }
    }
}
