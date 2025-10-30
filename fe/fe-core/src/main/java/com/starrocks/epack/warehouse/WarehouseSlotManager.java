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

package com.starrocks.epack.warehouse;

import com.google.common.collect.Maps;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricVisitor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.BaseSlotTracker;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.qe.scheduler.warehouse.WarehouseMetricEntity;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.compress.utils.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class WarehouseSlotManager extends BaseSlotManager {
    private static final Logger LOG = LogManager.getLogger(WarehouseSlotManager.class);

    private final ResourceUsageMonitor resourceUsageMonitor;
    private final WarehouseRequestWorker requestWorker = new WarehouseRequestWorker();
    private final Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = Maps.newConcurrentMap();
    private final Map<Long, WarehouseMetricEntity> warehouseMetrics = Maps.newConcurrentMap();

    public WarehouseSlotManager(ResourceUsageMonitor resourceUsageMonitor) {
        super(resourceUsageMonitor);
        this.resourceUsageMonitor = resourceUsageMonitor;
        resourceUsageMonitor.registerResourceAvailableListener(this::notifyResourceUsageAvailable);
    }

    @Override
    public Map<Long, BaseSlotTracker> getWarehouseIdToSlotTracker() {
        Map<Long, BaseSlotTracker> result = Maps.newHashMap();
        for (Map.Entry<Long, BaseSlotTracker> e : warehouseIdToSlotTracker.entrySet()) {
            // filter out warehouses which disable query queue
            if (!isEnableQueryQueue(e.getKey())) {
                continue;
            }
            result.put(e.getKey(), e.getValue());
        }
        return result;
    }

    public Map<Long, WarehouseMetricEntity> getWarehouseMetrics() {
        Map<Long, WarehouseMetricEntity> result = Maps.newHashMap();
        for (Map.Entry<Long, WarehouseMetricEntity> e : warehouseMetrics.entrySet()) {
            // filter out warehouses which disable query queue
            if (!isEnableQueryQueue(e.getKey())) {
                continue;
            }
            result.put(e.getKey(), e.getValue());
        }
        return result;
    }

    private boolean isEnableQueryQueue(long warehouseId) {
        Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
        if (wh == null || !(wh instanceof LocalWarehouse)) {
            return false;
        }
        LocalWarehouse localWarehouse = (LocalWarehouse) wh;
        WarehouseProperty warehouseProperty = localWarehouse.getProperty();
        return warehouseProperty.isEnableQueryQueue();
    }

    public void registerWarehouse(long warehouseId) {
        BaseSlotTracker slotTracker = warehouseIdToSlotTracker.computeIfAbsent(warehouseId,
                ignored -> new WarehouseSlotTracker(this, resourceUsageMonitor, warehouseId));
        warehouseMetrics.computeIfAbsent(warehouseId, ignored -> new WarehouseMetricEntity(slotTracker));
    }

    public void unregisterWarehouse(long warehouseId) {
        warehouseIdToSlotTracker.remove(warehouseId);
        warehouseMetrics.remove(warehouseId);
    }

    @Override
    public BaseSlotTracker getSlotTracker(long warehouseId) {
        return warehouseIdToSlotTracker.computeIfAbsent(warehouseId,
                k -> new WarehouseSlotTracker(this, resourceUsageMonitor, warehouseId));
    }

    @Override
    public void doStart() {
        requestWorker.start();
    }

    @Override
    public List<LogicalSlot> getSlots() {
        return warehouseIdToSlotTracker.values()
                .stream()
                .flatMap(slotTracker -> slotTracker.getSlots().stream())
                .collect(Collectors.toList());
    }

    private class WarehouseRequestWorker extends Thread {
        public WarehouseRequestWorker() {
            super("warehouse-slot-mgr-req");
        }

        private boolean schedule() {
            for (Map.Entry<Long, BaseSlotTracker> e : warehouseIdToSlotTracker.entrySet()) {
                BaseSlotTracker slotTracker = e.getValue();
                if (slotTracker.getSlots().isEmpty()) {
                    continue;
                }
                List<LogicalSlot> expiredSlots = slotTracker.peakExpiredSlots();
                if (!expiredSlots.isEmpty()) {
                    LOG.warn("[Slot] expired slots [{}]", expiredSlots);
                    expiredSlots.forEach(slot -> handleReleaseSlotTask(slot));
                }
            }
            return tryAllocateSlots();
        }

        private boolean tryAllocateSlots() {
            boolean isAllocated = false;
            for (Map.Entry<Long, BaseSlotTracker> e : warehouseIdToSlotTracker.entrySet()) {
                BaseSlotTracker slotTracker = e.getValue();
                if (slotTracker.getSlots().isEmpty()) {
                    continue;
                }
                Collection<LogicalSlot> slotsToAllocate = slotTracker.peakSlotsToAllocate();
                slotsToAllocate.forEach(slot -> allocateSlot(slotTracker, slot));
                isAllocated |= !slotsToAllocate.isEmpty();
            }
            return isAllocated;
        }

        private void allocateSlot(BaseSlotTracker slotTracker, LogicalSlot slot) {
            slot.onAllocate();
            slotTracker.allocateSlot(slot);
            finishSlotRequirementToEndpoint(slot, new TStatus(TStatusCode.OK));
        }

        @Override
        public void run() {
            List<Runnable> newTasks = Lists.newArrayList();
            Runnable newTask;
            for (; ;) {
                try {
                    newTask = null;
                    if (requests.isEmpty()) {
                        // wait until there is a new task
                        newTask = requests.take();
                    } else {
                        final long minExpiredTimeMs = warehouseIdToSlotTracker.values().stream()
                                .map(tracker -> tracker.getMinExpiredTimeMs())
                                .min(Long::compareTo)
                                .orElse(0L);
                        final long nowMs = System.currentTimeMillis();
                        try {
                            if (minExpiredTimeMs == 0) {
                                newTask = requests.take();
                            } else if (nowMs < minExpiredTimeMs) {
                                // poll until the first expired time
                                newTask = requests.poll(minExpiredTimeMs - nowMs, TimeUnit.MILLISECONDS);
                            }
                        } catch (InterruptedException e) {
                            LOG.warn("[Slot] RequestWorker is interrupted", e);
                            Thread.currentThread().interrupt();
                            return;
                        }
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

    /**
     * Collect warehouse metrics for all warehouses.
     */
    @Override
    public void collectWarehouseMetrics(MetricVisitor visitor) {
        try {
            Map<Long, WarehouseMetricEntity> warehouseMetrics = getWarehouseMetrics();
            for (Map.Entry<Long, WarehouseMetricEntity> entry : warehouseMetrics.entrySet()) {
                try {
                    WarehouseMetricEntity entity = entry.getValue();
                    for  (Metric metric : entity.getMetrics()) {
                        metric.addLabel(new MetricLabel("warehouse_id", String.valueOf(entity.getWarehouseId())));
                        metric.addLabel(new MetricLabel("warehouse_name", entity.getWarehouseName()));
                        visitor.visit(metric);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to collect warehouse metrics for warehouse {}: {}",
                            entry.getKey(), e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to collect warehouse metrics: {}", e.getMessage(), e);
        }
    }

    /**
     * In shared-data mode, use warehouse's property rather than global variables to determine whether to enable query queue.
     */
    @Override
    public boolean isEnableQueryQueue(ConnectContext connectContext, JobSpec jobSpec) {
        if (RunMode.isSharedNothingMode()) {
            return super.isEnableQueryQueue(connectContext, jobSpec);
        } else {
            // in shared-data mode, multi-warehouse is supported, so use warehouse's property rather global variables.
            long warehouseId = connectContext.getCurrentWarehouseId();
            if (!isEnableQueryQueueV2(warehouseId)) {
                return false;
            }
            Warehouse warehouse = getWarehouse(warehouseId);
            WarehouseProperty warehouseProperty = ((LocalWarehouse) warehouse).getProperty();
            if (jobSpec.isStatisticsJob()) {
                return warehouseProperty.isEnableQueryQueueStatistic();
            }
            if (jobSpec.isLoadType()) {
                return warehouseProperty.isEnableQueryQueueLoad();
            }
            return true;
        }
    }

    @Override
    public int getQueryQueuePendingTimeoutSecond(long warehouseId) {
        if (RunMode.isSharedNothingMode()) {
            return super.getQueryQueuePendingTimeoutSecond(warehouseId);
        } else {
            Warehouse warehouse = getWarehouse(warehouseId);
            if (warehouse == null || !(warehouse instanceof LocalWarehouse)) {
                return GlobalVariable.getQueryQueuePendingTimeoutSecond();
            }
            return ((LocalWarehouse) warehouse).getProperty().getQueryQueuePendingTimeoutSecond();
        }
    }

    @Override
    public int getQueryQueueMaxQueuedQueries(long warehouseId) {
        if (RunMode.isSharedNothingMode()) {
            return super.getQueryQueueMaxQueuedQueries(warehouseId);
        } else {
            Warehouse warehouse = getWarehouse(warehouseId);
            if (warehouse == null || !(warehouse instanceof LocalWarehouse)) {
                return GlobalVariable.getQueryQueueMaxQueuedQueries();
            }
            return ((LocalWarehouse) warehouse).getProperty().getQueryQueueMaxQueuedQueries();
        }
    }

    @Override
    public boolean isEnableQueryQueueV2(long warehouseId) {
        if (RunMode.isSharedNothingMode()) {
            return super.isEnableQueryQueueV2(warehouseId);
        } else {
            Warehouse warehouse = getWarehouse(warehouseId);
            if (warehouse == null || !(warehouse instanceof LocalWarehouse)) {
                return false;
            }
            return ((LocalWarehouse) warehouse).getProperty().isEnableQueryQueue();
        }
    }

    @Override
    public int getQueryQueueConcurrencyLimit(long warehouseId) {
        if (RunMode.isSharedNothingMode()) {
            return super.getQueryQueueConcurrencyLimit(warehouseId);
        } else {
            Warehouse warehouse = getWarehouse(warehouseId);
            if (warehouse == null || !(warehouse instanceof LocalWarehouse)) {
                return -1;
            }
            return ((LocalWarehouse) warehouse).getProperty().getQueryQueueConcurrencyLimit();
        }
    }
}
