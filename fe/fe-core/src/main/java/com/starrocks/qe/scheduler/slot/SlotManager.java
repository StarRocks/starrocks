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

import com.google.common.base.Preconditions;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.metric.MetricVisitor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.commons.compress.utils.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SlotManager extends BaseSlotManager {
    private static final Logger LOG = LogManager.getLogger(SlotManager.class);

    private final RequestWorker requestWorker = new RequestWorker();
    private final SlotTracker slotTracker;

    public SlotManager(ResourceUsageMonitor resourceUsageMonitor) {
        super(resourceUsageMonitor);
        this.slotTracker = new SlotTracker(this, resourceUsageMonitor);
    }

    @Override
    public List<LogicalSlot> getSlots() {
        return slotTracker.getSlots().stream().collect(Collectors.toList());
    }

    @Override
    public SlotTracker getSlotTracker(long warehouseId) {
        return slotTracker;
    }

    @Override
    public void doStart() {
        requestWorker.start();
    }

    @Override
    public void collectWarehouseMetrics(MetricVisitor visitor) {
        // do nothing
    }

    @Override
    public void onQueryFinished(LogicalSlot slot, ConnectContext context) {
        // do nothing
    }

    // It works only if's the leader node
    public String getExecStateByQueryId(String queryId) {
        Preconditions.checkState(GlobalStateMgr.getCurrentState().isLeader());
        return getSlots().stream()
                .filter(slot -> queryId.equals(DebugUtil.printId(slot.getSlotId())))
                .map(slot -> slot.getState().toQueryStateString())
                .findFirst()
                .orElse("");
    }

    private class RequestWorker extends Thread {
        public RequestWorker() {
            super("slot-mgr-req");
        }

        private boolean schedule() {
            List<LogicalSlot> expiredSlots = slotTracker.peakExpiredSlots();
            if (!expiredSlots.isEmpty()) {
                LOG.warn("[Slot] expired slots [{}]", expiredSlots);
            }
            expiredSlots.forEach(slot -> handleReleaseSlotTask(slot));

            return tryAllocateSlots();
        }

        private boolean tryAllocateSlots() {
            Collection<LogicalSlot> slotsToAllocate = slotTracker.peakSlotsToAllocate();
            slotsToAllocate.forEach(this::allocateSlot);
            return !slotsToAllocate.isEmpty();
        }

        private void allocateSlot(LogicalSlot slot) {
            slot.onAllocate();
            slotTracker.allocateSlot(slot);
            finishSlotRequirementToEndpoint(slot, new TStatus(TStatusCode.OK));
        }

        @Override
        public void run() {
            List<Runnable> newTasks = Lists.newArrayList();
            Runnable newTask;
            for (; ; ) {
                try {
                    newTask = null;
                    long minExpiredTimeMs = slotTracker.getMinExpiredTimeMs();
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
