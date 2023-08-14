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
import com.starrocks.common.Config;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.thrift.TFinishSlotRequirementRequest;
import com.starrocks.thrift.TFinishSlotRequirementResponse;
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

public class SlotManager {
    private static final Logger LOG = LogManager.getLogger(SlotManager.class);

    private static final int MAX_PENDING_REQUESTS = 1_000_000;

    private final BlockingQueue<Runnable> requests = Queues.newLinkedBlockingDeque(MAX_PENDING_REQUESTS);
    private final RequestWorker requestWorker = new RequestWorker();
    private final AtomicBoolean started = new AtomicBoolean();

    private final Executor responseExecutor = Executors.newFixedThreadPool(
            Config.slot_manager_response_thread_pool_size,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("slot-mgr-res-%d").build());

    private final ConcurrentMap<TUniqueId, Slot> slots = new ConcurrentHashMap<>();
    private final Map<String, Set<TUniqueId>> requestHostToSlotIds = new HashMap<>();

    private final SlotRequestQueue slotRequestQueue;
    private final AllocatedSlots allocatedSlots;

    public SlotManager(ResourceUsageMonitor resourceUsageMonitor) {
        resourceUsageMonitor.registerResourceAvailableListener(this::notifyResourceUsageAvailable);
        this.slotRequestQueue = new SlotRequestQueue(resourceUsageMonitor::isResourceOverloaded);
        this.allocatedSlots = new AllocatedSlots();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            requestWorker.start();
        }
    }

    public void requireSlotAsync(Slot slot) {
        requests.add(() -> handleRequireSlotTask(slot));
    }

    public void releaseSlotAsync(TUniqueId slotId) {
        requests.add(() -> handleReleaseSlotTask(slotId));
    }

    public void notifyFrontendDeadAsync(String feHost) {
        requests.add(() -> handleFrontendDeadTask(feHost));
    }

    public void notifyResourceUsageAvailable() {
        requests.add(() -> {
        });
    }

    public List<Slot> getSlots() {
        return new ArrayList<>(slots.values());
    }

    private void handleRequireSlotTask(Slot slot) {
        boolean ok = slotRequestQueue.addPendingSlot(slot);
        if (ok) {
            slot.onRequire();
            slots.put(slot.getSlotId(), slot);
            requestHostToSlotIds.computeIfAbsent(slot.getRequestEndpoint().getHostname(), k -> new HashSet<>())
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
        Slot slot = slotRequestQueue.removePendingSlot(slotId);
        if (slot == null) {
            slot = allocatedSlots.releaseSlot(slotId);
        }
        if (slot != null) {
            slot.onRelease();
            slots.remove(slot.getSlotId());
            Set<TUniqueId> slotIds = requestHostToSlotIds.get(slot.getRequestEndpoint().getHostname());
            if (slotIds != null) {
                slotIds.remove(slotId);
            }
        }
    }

    private void handleFrontendDeadTask(String feHost) {
        Set<TUniqueId> slotIds = requestHostToSlotIds.get(feHost);
        if (slotIds == null) {
            return;
        }

        LOG.warn("[Slot] The frontend [{}] becomes dead and its allocated slots are released [{}]", feHost, slotIds);
        List<TUniqueId> copiedSlotIds = new ArrayList<>(slotIds);
        copiedSlotIds.forEach(this::handleReleaseSlotTask);
    }

    private boolean tryAllocateSlots() {
        List<Slot> slotsToAllocate = slotRequestQueue.peakSlotsToAllocate(allocatedSlots);
        slotsToAllocate.forEach(this::allocateSlot);
        return !slotsToAllocate.isEmpty();
    }

    private void allocateSlot(Slot slot) {
        slot.onAllocate();
        slotRequestQueue.removePendingSlot(slot.getSlotId());
        allocatedSlots.allocateSlot(slot);
        finishSlotRequirementToEndpoint(slot, new TStatus(TStatusCode.OK));
    }

    private void finishSlotRequirementToEndpoint(Slot slot, TStatus status) {
        responseExecutor.execute(() -> {
            TFinishSlotRequirementRequest request = new TFinishSlotRequirementRequest();
            request.setStatus(status);
            request.setSlot_id(slot.getSlotId());

            try {
                TFinishSlotRequirementResponse res = FrontendServiceProxy.call(slot.getRequestEndpoint(),
                        Config.thrift_rpc_timeout_ms,
                        Config.thrift_rpc_retry_times,
                        client -> client.finishSlotRequirement(request));
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
            List<Slot> expiredSlots = allocatedSlots.peakExpiredSlots();
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

                    boolean allocatedSlots = true;
                    while (allocatedSlots) {
                        allocatedSlots = schedule();
                    }
                } catch (Throwable e) {
                    LOG.warn("[Slot] RequestWorker throws unexpected error", e);
                }

            }
        }
    }

}
