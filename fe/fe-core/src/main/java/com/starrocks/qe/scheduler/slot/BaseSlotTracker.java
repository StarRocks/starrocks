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

import com.google.api.client.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * SlotTracker is used to track the status of slots.
 * It is responsible for managing the life cycle of slots, including requiring, allocating, and releasing slots.
 *
 * <p> All the methods except {@link #getSlots()} are not thread-safe.
 */
public abstract class BaseSlotTracker {
    private static final Logger LOG = LogManager.getLogger(BaseSlotTracker.class);

    protected final ConcurrentMap<TUniqueId, LogicalSlot> slots = new ConcurrentHashMap<>();
    protected final Set<LogicalSlot> slotsOrderByExpiredTime = new TreeSet<>(
            Comparator.comparingLong(LogicalSlot::getExpiredPendingTimeMs)
                    .thenComparing(LogicalSlot::getSlotId));

    protected final Map<TUniqueId, LogicalSlot> pendingSlots = new HashMap<>();
    protected final Map<TUniqueId, LogicalSlot> allocatedSlots = new HashMap<>();
    protected final ResourceUsageMonitor resourceUsageMonitor;
    protected final long warehouseId;

    protected int numAllocatedSlots = 0;
    protected Optional<String> warehouseName = Optional.empty();

    protected List<BaseSlotTracker.Listener> listeners;
    protected SlotSelectionStrategy slotSelectionStrategy;
    // history slots to record the slots which have been released for monitoring
    private final Queue<LogicalSlot> historySlots = new ConcurrentLinkedQueue();

    public BaseSlotTracker(ResourceUsageMonitor resourceUsageMonitor, long warehouseId) {
        this.resourceUsageMonitor = resourceUsageMonitor;
        this.warehouseId = warehouseId;
    }

    public String getWarehouseName() {
        try {
            if (warehouseName.isEmpty()) {
                WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                Warehouse warehouse = warehouseManager.getWarehouse(this.warehouseId);
                this.warehouseName = Optional.of(warehouse.getName());
            }
            return warehouseName.orElse("");
        } catch (Exception e) {
            LOG.warn("Failed to get warehouse name for warehouseId: {}", warehouseId, e);
            return "";
        }
    }

    public long getQueuePendingLength() {
        return pendingSlots.size();
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


    public Optional<Integer> getRemainSlots() {
        return getMaxSlots().map(s -> Math.max(0, s - numAllocatedSlots));
    }

    public long getMaxQueueQueueLength() {
        final BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        return slotManager.getQueryQueueMaxQueuedQueries(warehouseId);
    }

    public long getMaxQueuePendingTimeSecond() {
        final BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        return slotManager.getQueryQueuePendingTimeoutSecond(warehouseId);
    }

    /**
     * Return the max slots of the slot tracker if query queuen is supported.
     */
    public abstract Optional<Integer> getMaxSlots();

    /**
     * Check if the slot tracker is beyond the capacity limit.
     * @param slot : The slot to be checked.
     * @return True if the slot tracker is beyond the capacity limit, false otherwise.
     */
    protected boolean isResourceCapacityEnough(LogicalSlot slot) {
        final BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        if (GlobalVariable.isQueryQueueMaxQueuedQueriesEffective() &&
                pendingSlots.size() >= slotManager.getQueryQueueMaxQueuedQueries(slot.getWarehouseId())) {
            return false;
        }
        return true;
    }

    /**
     * Get the extra message of the slot tracker.
     */
    public Optional<ExtraMessage> getExtraMessage() {
        return Optional.empty();
    }

    /**
     * Add a slot requirement.
     * @param slot The required slot.
     * @return True if the slot is required successfully or already required , false if the query queue is full.
     */
    public boolean requireSlot(LogicalSlot slot) {
        if (!isResourceCapacityEnough(slot)) {
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

        // try to register the slot to the connected context
        tryRegisterConnectContext(slot);

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
        LogicalSlot slot = releaseSlotImpl(slotId);
        if (slot != null) {
            tryAddIntoHistorySlots(slot);
        }
        return slot;
    }

    protected void tryAddIntoHistorySlots(LogicalSlot slot) {
        if (Config.max_query_queue_history_slots_number <= 0) {
            return;
        }
        while (historySlots.size() > Config.max_query_queue_history_slots_number) {
            historySlots.poll();
        }
        historySlots.add(slot);
    }

    /**
     * Try to register the slot to the connected context for more observing information.
     * @param slot: The slot to be registered.
     */
    protected void tryRegisterConnectContext(LogicalSlot slot) {
        // ignore the slot is null or slot's extra message has been set
        if (Config.max_query_queue_history_slots_number <= 0 ||
                slot == null || (slot.getExtraMessage() != null && slot.getExtraMessage().isPresent())) {
            return;
        }
        // find the connected context and register the logical slot
        try {
            TUniqueId slotId = slot.getSlotId();
            UUID queryId = UUIDUtil.fromTUniqueid(slotId);
            ConnectContext ctx = ExecuteEnv.getInstance().getScheduler().findContextByQueryId(queryId.toString());
            if (ctx == null) {
                LOG.debug("Failed to find the context for queryId: {}", queryId);
                return;
            }
            LOG.debug("Registering the slot {} to context {}", slot, ctx);
            ctx.registerListener(new LogicalSlot.ConnectContextListener(slot));
        } catch (Exception e) {
            LOG.warn("Failed to register the slot to context", e);
        }
    }

    protected LogicalSlot releaseSlotImpl(TUniqueId slotId) {
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

    /**
     * Peak the slots to be allocated from slot manager.
     */
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
        if (Config.max_query_queue_history_slots_number > 0) {
            List<LogicalSlot> result = Lists.newArrayList();
            // the newest slots are in the front
            result.addAll(slots.values());
            List<LogicalSlot> histories = Lists.newArrayList(historySlots);
            if (!histories.isEmpty()) {
                Collections.reverse(histories);
                result.addAll(histories);
            }
            return result;
        } else {
            return slots.values();
        }
    }

    @VisibleForTesting
    public Collection<LogicalSlot> getHistorySlots() {
        return historySlots;
    }

    public LogicalSlot getSlot(TUniqueId slotId) {
        return slots.get(slotId);
    }

    // allocated slots that mean the slots which have been allocated and not released.
    public int getNumAllocatedSlots() {
        return numAllocatedSlots;
    }

    // return the number of allocated slots which is the current concurrency in the query queue
    public int getCurrentCurrency() {
        return allocatedSlots.size();
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public interface Listener {
        void onRequireSlot(LogicalSlot slot);

        void onAllocateSlot(LogicalSlot slot);

        void onReleaseSlot(LogicalSlot slot);
    }

    public static class SlotListenerForPipelineDriverAllocator implements BaseSlotTracker.Listener {
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

    /**
     * Extra message for the slot tracker.
     */
    public static class ExtraMessage {
        @SerializedName("Concurrency")
        private final long concurrency;
        @SerializedName("QueryQueueOption")
        private final QueryQueueOptions.V2 v2;

        public ExtraMessage(long concurrency, QueryQueueOptions.V2 v2) {
            this.concurrency = concurrency;
            this.v2 = v2;
        }

        public long getConcurrency() {
            return concurrency;
        }

        public QueryQueueOptions.V2 getV2() {
            return v2;
        }
    }
}
