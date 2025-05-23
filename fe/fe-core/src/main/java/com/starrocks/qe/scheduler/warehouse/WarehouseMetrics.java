// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe.scheduler.warehouse;

import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.scheduler.slot.BaseSlotTracker;
import com.starrocks.qe.scheduler.slot.QueryQueueOptions;
import com.starrocks.thrift.TGetWarehouseMetricsResponeItem;

import java.util.Optional;

public class WarehouseMetrics {
    private final long warehouseId;
    private final String warehouseName;
    private final long queuePendingLength;
    private final long queueRunningLength;
    private final long maxQueueQueueLength;
    private final double earliestQueryWaitTime;
    private final long maxQueuePendingTimeSecond;
    private final int maxRequiredSlots;
    private final int sumRequiredSlots;
    private final long remainSlots;
    private final long maxSlots;
    private final Optional<BaseSlotTracker.ExtraMessage> extraMessage;

    public WarehouseMetrics(long warehouseId, String warehouseName, long queuePendingLength, long queueRunningLength,
                            long maxQueueQueueLength, double earliestQueryWaitTime, long maxQueuePendingTimeSecond,
                            int maxRequiredSlots, int sumRequiredSlots, long remainSlots, long maxSlots,
                            Optional<BaseSlotTracker.ExtraMessage> extraMessage) {
        this.warehouseId = warehouseId;
        this.warehouseName = warehouseName;
        this.queuePendingLength = queuePendingLength;
        this.queueRunningLength = queueRunningLength;
        this.maxQueueQueueLength = maxQueueQueueLength;
        this.maxQueuePendingTimeSecond = maxQueuePendingTimeSecond;
        this.earliestQueryWaitTime = earliestQueryWaitTime;
        this.maxRequiredSlots = maxRequiredSlots;
        this.sumRequiredSlots = sumRequiredSlots;
        this.remainSlots = remainSlots;
        this.maxSlots = maxSlots;
        this.extraMessage = extraMessage;
    }

    public static WarehouseMetrics empty() {
        return new WarehouseMetrics(0, "", 0, 0, 0, 0, 0, 0, 0, 0, 0,
                Optional.empty());
    }

    public static WarehouseMetrics create(BaseSlotTracker tracker) {
        int maxRequestSlots = tracker.getMaxRequiredSlots().map(s -> QueryQueueOptions.correctSlotNum(s)).orElse(0);
        int sumRequestSlots = tracker.getSumRequiredSlots().map(s -> QueryQueueOptions.correctSlotNum(s)).orElse(0);
        // to avoid negative remain slots
        long remainSlots = QueryQueueOptions.correctSlotNum(tracker.getRemainSlots().orElse(0));
        long maxSlots = tracker.getMaxSlots().map(s -> QueryQueueOptions.correctSlotNum(s)).orElse(0);
        final Optional<BaseSlotTracker.ExtraMessage> extraMessage = tracker.getExtraMessage();
        return new WarehouseMetrics(tracker.getWarehouseId(), tracker.getWarehouseName(),
                tracker.getQueuePendingLength(), tracker.getCurrentCurrency(), tracker.getMaxQueueQueueLength(),
                tracker.getEarliestQueryWaitTimeSecond(), tracker.getMaxQueuePendingTimeSecond(),
                maxRequestSlots, sumRequestSlots, remainSlots, maxSlots, extraMessage);
    }

    public TGetWarehouseMetricsResponeItem toThrift() {
        TGetWarehouseMetricsResponeItem item = new TGetWarehouseMetricsResponeItem();
        item.setWarehouse_id(String.valueOf(warehouseId));
        item.setWarehouse_name(warehouseName);
        item.setQueue_pending_length(String.valueOf(queuePendingLength));
        item.setQueue_running_length(String.valueOf(queueRunningLength));
        item.setMax_pending_length(String.valueOf(maxQueueQueueLength));
        item.setMax_pending_time_second(String.valueOf(maxQueuePendingTimeSecond));
        item.setEarliest_query_wait_time(String.valueOf(earliestQueryWaitTime));
        item.setMax_required_slots(String.valueOf(maxRequiredSlots));
        item.setSum_required_slots(String.valueOf(sumRequiredSlots));
        item.setRemain_slots(String.valueOf(remainSlots));
        item.setMax_slots(String.valueOf(maxSlots));
        if (extraMessage != null && extraMessage.isPresent()) {
            item.setExtra_message(GsonUtils.GSON.toJson(extraMessage.get()));
        }
        return item;
    }
}
