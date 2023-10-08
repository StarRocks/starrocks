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
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TFinishSlotRequirementRequest;
import com.starrocks.thrift.TFinishSlotRequirementResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.compress.utils.Lists;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
public class SlotManager {
    private static final Logger LOG = LogManager.getLogger(SlotManager.class);

    private static final int MAX_PENDING_REQUESTS = 1_000_000;

    /**
     * All the data members except {@code requests} and {@link #slots} are only accessed by the thread {@link #requestWorker}.
     * Others outside can do nothing, but add a request to {@code requests} or retrieve a view of all the running and queued
     * slots.
     */
    private final BlockingQueue<Runnable> requests = Queues.newLinkedBlockingDeque(MAX_PENDING_REQUESTS);
    private final RequestWorker requestWorker = new RequestWorker();
    private final AtomicBoolean started = new AtomicBoolean();

    private final Executor responseExecutor = Executors.newFixedThreadPool(Config.slot_manager_response_thread_pool_size,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("slot-mgr-res-%d").build());

    private final ConcurrentMap<TUniqueId, LogicalSlot> slots = new ConcurrentHashMap<>();
    private final Map<String, Set<TUniqueId>> requestFeNameToSlotIds = new HashMap<>();

    private final SlotRequestQueue slotRequestQueue;
    private final AllocatedSlots allocatedSlots;

    public SlotManager(ResourceUsageMonitor resourceUsageMonitor) {
        resourceUsageMonitor.registerResourceAvailableListener(this::notifyResourceUsageAvailable);
        this.slotRequestQueue = new SlotRequestQueue(resourceUsageMonitor::isGlobalResourceOverloaded,
                resourceUsageMonitor::isGroupResourceOverloaded);
        this.allocatedSlots = new AllocatedSlots();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            requestWorker.start();
        }
    }

    public void requireSlotAsync(LogicalSlot slot) {
        requests.add(() -> handleRequireSlotTask(slot));
    }

    public void releaseSlotAsync(TUniqueId slotId) {
        requests.add(() -> handleReleaseSlotTask(slotId));
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

    public List<LogicalSlot> getSlots() {
        return new ArrayList<>(slots.values());
    }

    private void handleRequireSlotTask(LogicalSlot slot) {
        Frontend frontend = GlobalStateMgr.getCurrentState().getFeByName(slot.getRequestFeName());
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

        boolean ok = slotRequestQueue.addPendingSlot(slot);
        if (ok) {
            slot.onRequire();
            slots.put(slot.getSlotId(), slot);
            requestFeNameToSlotIds.computeIfAbsent(slot.getRequestFeName(), k -> new HashSet<>())
                    .add(slot.getSlotId());
        } else {
            slot.onCancel();
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            String errMsg = String.format("Resource is not enough and the number of pending queries exceeds capacity [%d], " +
                            "you could modify the session variable [%s] to make more query can be queued",
                    GlobalVariable.getQueryQueueMaxQueuedQueries(), GlobalVariable.QUERY_QUEUE_MAX_QUEUED_QUERIES);
            status.setError_msgs(Collections.singletonList(errMsg));
            finishSlotRequirementToEndpoint(slot, status);
        }
    }

    private void handleReleaseSlotTask(TUniqueId slotId) {
        LogicalSlot slot = slotRequestQueue.removePendingSlot(slotId);
        if (slot == null) {
            slot = allocatedSlots.releaseSlot(slotId);
        }
        if (slot != null) {
            slot.onRelease();
            slots.remove(slot.getSlotId());
            Set<TUniqueId> slotIds = requestFeNameToSlotIds.get(slot.getRequestFeName());
            if (slotIds != null) {
                slotIds.remove(slotId);
            }
        }
    }

    private void handleFrontendDeadTask(String feName) {
        Set<TUniqueId> slotIds = requestFeNameToSlotIds.get(feName);
        if (slotIds == null) {
            return;
        }

        LOG.warn("[Slot] The frontend [{}] becomes dead, and its pending and allocated slots will be released", feName);
        List<TUniqueId> copiedSlotIds = new ArrayList<>(slotIds);
        copiedSlotIds.forEach(this::handleReleaseSlotTask);
    }

    private void handleFrontendRestart(String feName, long startMs) {
        Set<TUniqueId> slotIds = requestFeNameToSlotIds.get(feName);
        if (slotIds == null) {
            return;
        }

        LOG.warn("[Slot] The frontend [{}] restarts [startMs={}], " +
                "and its pending and allocated slots with less startMs will be released", feName, startMs);

        slotIds.stream().filter(slotId -> {
            LogicalSlot slot = slots.get(slotId);
            if (slot == null) {
                return false;
            }
            return slot.getFeStartTimeMs() < startMs;
        }).collect(Collectors.toList()).forEach(this::handleReleaseSlotTask);
    }

    private void finishSlotRequirementToEndpoint(LogicalSlot slot, TStatus status) {
        responseExecutor.execute(() -> {
            TFinishSlotRequirementRequest request = new TFinishSlotRequirementRequest();
            request.setStatus(status);
            request.setSlot_id(slot.getSlotId());
            request.setPipeline_dop(slot.getPipelineDop());

            Frontend fe = GlobalStateMgr.getCurrentState().getFeByName(slot.getRequestFeName());
            if (fe == null) {
                LOG.warn("[Slot] try to send finishSlotRequirement RPC to the unknown frontend [slot={}]", slot);
                releaseSlotAsync(slot.getSlotId());
                return;
            }

            TNetworkAddress feEndpoint = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
            try {
                TFinishSlotRequirementResponse res =
                        FrontendServiceProxy.call(feEndpoint, Config.thrift_rpc_timeout_ms,
                                Config.thrift_rpc_retry_times, client -> client.finishSlotRequirement(request));
                TStatus resStatus = res.getStatus();
                if (resStatus.getStatus_code() != TStatusCode.OK) {
                    LOG.warn("[Slot] failed to finish slot requirement [slot={}] [err={}]", slot, resStatus);
                    if (status.getStatus_code() == TStatusCode.OK) {
                        releaseSlotAsync(slot.getSlotId());
                    }
                }
            } catch (Exception e) {
                LOG.warn("[Slot] failed to finish slot requirement [slot={}]:", slot, e);
                if (status.getStatus_code() == TStatusCode.OK) {
                    releaseSlotAsync(slot.getSlotId());
                }
            }
        });
    }

    private class RequestWorker extends Thread {
        public RequestWorker() {
            super("slot-mgr-req");
        }

        private boolean schedule() {
            List<LogicalSlot> expiredSlots = allocatedSlots.peakExpiredSlots();
            if (!expiredSlots.isEmpty()) {
                LOG.warn("[Slot] expired allocated slots [{}]", expiredSlots);
            }
            expiredSlots.forEach(slot -> handleReleaseSlotTask(slot.getSlotId()));

            expiredSlots = slotRequestQueue.peakExpiredSlots();
            if (!expiredSlots.isEmpty()) {
                LOG.warn("[Slot] expired pending slots [{}]", expiredSlots);
            }
            expiredSlots.forEach(slot -> handleReleaseSlotTask(slot.getSlotId()));

            return tryAllocateSlots();
        }

        private boolean tryAllocateSlots() {
            List<LogicalSlot> slotsToAllocate = slotRequestQueue.peakSlotsToAllocate(allocatedSlots);
            slotsToAllocate.forEach(this::allocateSlot);
            return !slotsToAllocate.isEmpty();
        }

        private void allocateSlot(LogicalSlot slot) {
            slot.onAllocate();
            slotRequestQueue.removePendingSlot(slot.getSlotId());
            allocatedSlots.allocateSlot(slot);
            finishSlotRequirementToEndpoint(slot, new TStatus(TStatusCode.OK));
        }

        @Override
        public void run() {
            List<Runnable> newTasks = Lists.newArrayList();
            Runnable newTask;
            for (; ; ) {

                try {
                    newTask = null;
                    long minExpiredTimeMs = allocatedSlots.getMinExpiredTimeMs();
                    long nowMs = System.currentTimeMillis();
                    try {
                        if (minExpiredTimeMs == 0) {
                            newTask = requests.take();
                        } else if (nowMs < minExpiredTimeMs) {
                            newTask = requests.poll(minExpiredTimeMs - nowMs, TimeUnit.MILLISECONDS);
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("[Slot] RequestWorker is interrupted", e);
                        Thread.currentThread().interrupt();
                        return;
                    }

                    if (newTask != null) {
                        newTasks.add(newTask);
                    }

                    while ((newTask = requests.poll()) != null) {
                        newTasks.add(newTask);
                    }

                    newTasks.forEach(Runnable::run);
                    newTasks.clear();

                    boolean isAllocatedSlots = true;
                    while (isAllocatedSlots) {
                        isAllocatedSlots = schedule();
                    }
                } catch (Exception e) {
                    LOG.warn("[Slot] RequestWorker throws unexpected error", e);
                }

            }
        }
    }

}
