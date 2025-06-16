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

import com.google.api.client.util.Lists;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.BaseSlotTracker;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetWarehouseMetricsRequest;
import com.starrocks.thrift.TGetWarehouseMetricsRespone;
import com.starrocks.thrift.TGetWarehouseMetricsResponeItem;
import com.starrocks.thrift.TGetWarehouseQueriesRequest;
import com.starrocks.thrift.TGetWarehouseQueriesResponse;
import com.starrocks.thrift.TGetWarehouseQueriesResponseItem;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.thrift.TException;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class WarehouseQueryQueueMetrics {
    public static TGetWarehouseMetricsRespone build(TGetWarehouseMetricsRequest request) {
        final BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        TGetWarehouseMetricsRespone response = new TGetWarehouseMetricsRespone();
        Map<Long, BaseSlotTracker> warehouseTrackers = slotManager.getWarehouseIdToSlotTracker();
        List<TGetWarehouseMetricsResponeItem> items = Lists.newArrayList();
        if (CollectionUtils.sizeIsEmpty(warehouseTrackers)) {
            response.setMetrics(items);
            return response;
        }
        for (BaseSlotTracker tracker : warehouseTrackers.values()) {
            response.addToMetrics(WarehouseMetrics.create(tracker).toThrift());
        }
        return response;
    }

    public static TGetWarehouseQueriesResponse build(TGetWarehouseQueriesRequest request) throws TException {
        List<LogicalSlot> slots = GlobalStateMgr.getCurrentState().getSlotManager().getSlots();
        TGetWarehouseQueriesResponse response = new TGetWarehouseQueriesResponse();
        List<TGetWarehouseQueriesResponseItem> items = Lists.newArrayList();
        if (CollectionUtils.isEmpty(slots)) {
            response.setQueries(items);
            return response;
        }
        slots.sort(Comparator.comparingLong(LogicalSlot::getWarehouseId)
                .thenComparingLong(LogicalSlot::getStartTimeMs)
                .thenComparingLong(LogicalSlot::getExpiredAllocatedTimeMs));
        for (LogicalSlot slot : slots) {
            response.addToQueries(WarehouseQueryMetrics.create(slot).toThrift());
        }
        return response;
    }
}