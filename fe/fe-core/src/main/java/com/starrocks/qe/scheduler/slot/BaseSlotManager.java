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

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.Config;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TFinishSlotRequirementRequest;
import com.starrocks.thrift.TFinishSlotRequirementResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Manage all the slots in the leader FE. It queues, allocates or releases each slot requirement.
 * <p> A query is related to a slot requirement. There are a total of {@link GlobalVariable#getQueryQueueConcurrencyLimit()} slots
 * and {@link ResourceGroup#getConcurrencyLimit()} slots for a group. If there are not free slots
 * or the resource usage (CPU and Memory) exceeds the limit, the coming query will be queued.
 * <p> The allocated slot to a query will be released, if any following condition occurs:
 * <ul>
 *     <li> The query is finished or cancelled and sends the release RPC to the slot manager.
 *     <li> The slot manager finds that the query is timeout.
 *     <li> The slot manager finds that the frontend where the query is started is dead or restarted.
 * </ul>
 * <p> The slot manager is only running in the leader FE. The following diagram indicates the control flow.
 * <pre>{@code
 *                         ┌─────────────────────────────────────┐
 *                         │            SlotManager              │
 *                         └──────▲─┬───────────────────▲────────┘
 *  Leader FE                     │ │Notify requirement │
 *                  Require slots │ │finished           │Release slot
 *  ------------------------------│-│-------------------│---------------------
 *                         ┌──────┴─▼───────────────────┴────────┐
 *                         │            SlotProvider             │
 *                         └──────▲─┬───────────────────▲────────┘
 *                                │ │Notify requirement │
 *  Follower FE     Require slots │ │finished           │Release slots
 *                                │ │                   │
 *                         ┌──────┴─▼───────────────────┴────────┐
 *                         │            Coordinator              │
 *                         └─────────────────────────────────────┘
 *
 *
 * }</pre>
 *
 * @see SlotProvider
 * @see ResourceUsageMonitor
 */
public abstract class BaseSlotManager {
    private static final Logger LOG = LogManager.getLogger(BaseSlotManager.class);

    private static final int MAX_PENDING_REQUESTS = 1_000_000;

    /**
     * All the data members except {@code requests} and {@link BaseSlotTracker#getSlots()} are only accessed
     * by the RequestWorker.
     * Others outside can do nothing, but add a request to {@code requests} or retrieve a view of all the running and queued
     * slots.
     */
    protected final BlockingQueue<Runnable> requests = Queues.newLinkedBlockingDeque(MAX_PENDING_REQUESTS);

    private final AtomicBoolean started = new AtomicBoolean();
    private final Executor responseExecutor = Executors.newFixedThreadPool(Config.slot_manager_response_thread_pool_size,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("slot-mgr-res-%d").build());

    private final Map<String, Set<LogicalSlot>> requestFeNameToSlots = new HashMap<>();

    /**
     * The lifecycle of a slot is managed by the slot tracker.
     *
     * <pre>{@code
     * CREATED -(1)-> REQUIRED -(2)-> ALLOCATED -(3)-> RELEASED
     *                  │                                  ▲
     *                  └─────────────────(3)──────────────┘
     * }</pre>
     *
     * <ul>
     * <li> (1) {@link BaseSlotTracker#requireSlot}: the slot is into required state and is waiting for allocation.
     * <li> (2) {@link SlotSelectionStrategy#peakSlotsToAllocate}: select proper slots to allocate,
     * {@link BaseSlotTracker#allocateSlot}: the slot is into allocated state and the related query is notified to be started.
     * <li> (3) {@link BaseSlotTracker#releaseSlot}: the slot is released and will be removed from the slot tracker.
     * </ul>
     */

    public BaseSlotManager(ResourceUsageMonitor resourceUsageMonitor) {
        resourceUsageMonitor.registerResourceAvailableListener(this::notifyResourceUsageAvailable);
    }

    /**
     * Get all the slots in the slot manager.
     */
    public abstract List<LogicalSlot> getSlots();

    /**
     * Get the slot tracker by the warehouse id.
     */
    public abstract BaseSlotTracker getSlotTracker(long warehouseId);

    /**
     * Start the slot manager.
     */
    public abstract void doStart();

    public void start() {
        if (started.compareAndSet(false, true)) {
            doStart();
        }
    }

    public void requireSlotAsync(LogicalSlot slot) {
        requests.add(() -> handleRequireSlotTask(slot));
    }

    public void releaseSlotAsync(long warehouseId, TUniqueId slotId) {
        requests.add(() -> handleReleaseSlotTask(warehouseId, slotId));
    }

    public void notifyFrontendDeadAsync(String feName) {
        requests.add(() -> handleFrontendDeadTask(feName));
    }

    public void notifyFrontendRestartAsync(String feName, long startMs) {
        requests.add(() -> handleFrontendRestart(feName, startMs));
    }

    public void notifyResourceUsageAvailable() {
        // The request does nothing but wake up the request worker to check whether resource usage becomes available.
        requests.add(() -> {
        });
    }

    private void handleRequireSlotTask(LogicalSlot slot) {
        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getFeByName(slot.getRequestFeName());
        if (frontend == null) {
            slot.onCancel();
            LOG.warn("[Slot] SlotManager receives a slot requirement with unknown FE [slot={}]", slot);
            return;
        }
        if (slot.getFeStartTimeMs() < frontend.getStartTime()) {
            slot.onCancel();
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Collections.singletonList(String.format("FeStartTime is not the latest [val=%s] [latest=%s]",
                    slot.getFeStartTimeMs(), frontend.getStartTime())));
            finishSlotRequirementToEndpoint(slot, status);
            LOG.warn("[Slot] SlotManager receives a slot requirement with old FeStartTime [slot={}] [newFeStartMs={}]",
                    slot, frontend.getStartTime());
            return;
        }
        final long warehouseId = slot.getWarehouseId();
        final BaseSlotTracker slotTracker = getSlotTracker(warehouseId);
        if (slotTracker.requireSlot(slot)) {
            requestFeNameToSlots.computeIfAbsent(slot.getRequestFeName(), k -> new HashSet<>())
                    .add(slot);
        } else {
            slot.onCancel();
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            String errMsg = String.format("Resource is not enough and the number of pending queries exceeds capacity [%d], " +
                            "you could modify the session variable [%s] to make more query can be queued",
                    QueryQueueOptions.getQueryQueueMaxQueuedQueries(warehouseId), GlobalVariable.QUERY_QUEUE_MAX_QUEUED_QUERIES);
            status.setError_msgs(Collections.singletonList(errMsg));
            finishSlotRequirementToEndpoint(slot, status);
        }
    }

    private void handleReleaseSlotTask(long warehouseId, TUniqueId slotId) {
        BaseSlotTracker slotTracker = getSlotTracker(warehouseId);
        if (slotTracker == null) {
            LOG.warn("[Slot] The warehouse [{}] is not found, and the slot [{}] will be released", warehouseId, slotId);
            return;
        }
        LogicalSlot slot = slotTracker.releaseSlot(slotId);
        if (slot != null) {
            Set<LogicalSlot> slots = requestFeNameToSlots.get(slot.getRequestFeName());
            if (slots != null) {
                slots.remove(slot);
            }
        }
    }

    protected void handleReleaseSlotTask(LogicalSlot input) {
        handleReleaseSlotTask(input.getWarehouseId(), input.getSlotId());
    }

    private void handleFrontendDeadTask(String feName) {
        Set<LogicalSlot> slots = requestFeNameToSlots.get(feName);
        if (slots == null) {
            return;
        }

        LOG.warn("[Slot] The frontend [{}] becomes dead, and its pending and allocated slots will be released", feName);
        List<LogicalSlot> copiedSlots = new ArrayList<>(slots);
        copiedSlots.forEach(this::handleReleaseSlotTask);
    }

    private void handleFrontendRestart(String feName, long startMs) {
        Set<LogicalSlot> slots = requestFeNameToSlots.get(feName);
        if (slots == null) {
            return;
        }

        LOG.warn("[Slot] The frontend [{}] restarts [startMs={}], " +
                "and its pending and allocated slots with less startMs will be released", feName, startMs);
        slots.stream().filter(slot -> {
            if (slot == null) {
                return false;
            }
            return slot.getFeStartTimeMs() < startMs;
        }).collect(Collectors.toList()).forEach(this::handleReleaseSlotTask);
    }

    protected void finishSlotRequirementToEndpoint(LogicalSlot slot, TStatus status) {
        responseExecutor.execute(() -> {
            TFinishSlotRequirementRequest request = new TFinishSlotRequirementRequest();
            request.setStatus(status);
            request.setSlot_id(slot.getSlotId());
            request.setPipeline_dop(slot.getPipelineDop());

            Frontend fe = GlobalStateMgr.getCurrentState().getNodeMgr().getFeByName(slot.getRequestFeName());
            final long warehouseId = slot.getWarehouseId();
            if (fe == null) {
                LOG.warn("[Slot] try to send finishSlotRequirement RPC to the unknown frontend [slot={}]", slot);
                releaseSlotAsync(warehouseId, slot.getSlotId());
                return;
            }

            TNetworkAddress feEndpoint = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
            try {
                TFinishSlotRequirementResponse res = ThriftRPCRequestExecutor.call(
                        ThriftConnectionPool.frontendPool,
                        feEndpoint,
                        client -> client.finishSlotRequirement(request));
                TStatus resStatus = res.getStatus();
                if (resStatus.getStatus_code() != TStatusCode.OK) {
                    LOG.warn("[Slot] failed to finish slot requirement [slot={}] [err={}]", slot, resStatus);
                    if (status.getStatus_code() == TStatusCode.OK) {
                        releaseSlotAsync(warehouseId, slot.getSlotId());
                    }
                }
            } catch (Exception e) {
                LOG.warn("[Slot] failed to finish slot requirement [slot={}]:", slot, e);
                if (status.getStatus_code() == TStatusCode.OK) {
                    releaseSlotAsync(warehouseId, slot.getSlotId());
                }
            }
        });
    }
}
