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

import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.QueryQueueOptions;
import com.starrocks.thrift.TGetWarehouseQueriesResponseItem;
import com.starrocks.thrift.TUniqueId;

public class WarehouseQueryMetrics {
    private final long warehouseId;
    private final String warehouseName;
    private final TUniqueId queryId;
    private final LogicalSlot.State state;
    private final long estCostsSlots;
    private final long allocateSlots;
    private final double queuedWaitSeconds;

    public WarehouseQueryMetrics(long warehouseId, String warehouseName, TUniqueId queryId, LogicalSlot.State state,
                                 long estCostsSlots, long allocateSlots, double queuedWaitSeconds) {
        this.warehouseId = warehouseId;
        this.warehouseName = warehouseName;
        this.queryId = queryId;
        this.state = state;
        this.estCostsSlots = estCostsSlots;
        this.allocateSlots = allocateSlots;
        this.queuedWaitSeconds = queuedWaitSeconds;
    }

    public static WarehouseQueryMetrics empty() {
        return new WarehouseQueryMetrics(0, "", new TUniqueId(),
                LogicalSlot.State.CREATED, 0, 0, 0);
    }

    public static WarehouseQueryMetrics create(LogicalSlot slot) {
        long estCostsSlots = QueryQueueOptions.correctSlotNum(slot.getNumPhysicalSlots());
        long allocateSlots = slot.getAllocatedNumPhysicalSlots().map(s -> QueryQueueOptions.correctSlotNum(s)).orElse(0);
        return new WarehouseQueryMetrics(slot.getWarehouseId(), slot.getWarehouseName(),
                slot.getSlotId(), slot.getState(), estCostsSlots, allocateSlots, slot.getQueuedWaitSeconds());
    }

    public TGetWarehouseQueriesResponseItem toThrift() {
        TGetWarehouseQueriesResponseItem item = new TGetWarehouseQueriesResponseItem();
        item.setWarehouse_id(String.valueOf(warehouseId));
        item.setWarehouse_name(warehouseName);
        item.setQuery_id(DebugUtil.printId(queryId));
        item.setState(state.name());
        item.setEst_costs_slots(String.valueOf(estCostsSlots));
        item.setAllocate_slots(String.valueOf(allocateSlots));
        item.setQueued_wait_seconds(String.valueOf(queuedWaitSeconds));
        return item;
    }
}

