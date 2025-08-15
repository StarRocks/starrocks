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

import com.google.common.collect.Lists;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.QueryQueueOptions;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TGetWarehouseQueriesResponseItem;
import com.starrocks.thrift.TUniqueId;

import java.util.List;
import java.util.Optional;

public class WarehouseQueryMetrics {
    private final long warehouseId;
    private final String warehouseName;
    private final TUniqueId queryId;
    private final LogicalSlot.State state;
    private final long estCostsSlots;
    private final long allocateSlots;
    private final double queuedWaitSeconds;
    private final String query;
    private final Optional<LogicalSlot.ExtraMessage> extraMessage;
    public WarehouseQueryMetrics(long warehouseId, String warehouseName, TUniqueId queryId, LogicalSlot.State state,
                                 long estCostsSlots, long allocateSlots, double queuedWaitSeconds, String query,
                                 Optional<LogicalSlot.ExtraMessage> extraMessage) {
        this.warehouseId = warehouseId;
        this.warehouseName = warehouseName;
        this.queryId = queryId;
        this.state = state;
        this.estCostsSlots = estCostsSlots;
        this.allocateSlots = allocateSlots;
        this.queuedWaitSeconds = queuedWaitSeconds;
        this.query = query;
        this.extraMessage = extraMessage;
    }

    public static WarehouseQueryMetrics empty() {
        return new WarehouseQueryMetrics(0, "", new TUniqueId(),
                LogicalSlot.State.CREATED, 0, 0, 0, "",
                Optional.empty());
    }

    public static WarehouseQueryMetrics create(LogicalSlot slot) {
        long estCostsSlots = QueryQueueOptions.correctSlotNum(slot.getNumPhysicalSlots());
        long allocateSlots = slot.getAllocatedNumPhysicalSlots().map(s -> QueryQueueOptions.correctSlotNum(s)).orElse(0);

        Optional<LogicalSlot.ExtraMessage> extraMessage = slot.getExtraMessage();
        return new WarehouseQueryMetrics(slot.getWarehouseId(), slot.getWarehouseName(),
                slot.getSlotId(), slot.getState(), estCostsSlots, allocateSlots,
                slot.getQueuedWaitSeconds(), extraMessage.map(e -> e.getQuery()).orElse(""), extraMessage);
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
        item.setQuery(query);
        // extra message
        if (extraMessage != null && extraMessage.isPresent()) {
            LogicalSlot.ExtraMessage extra = extraMessage.get();
            item.setQuery_start_time(TimeUtils.longToTimeString(extra.getQueryStartTime()));
            item.setQuery_end_time(TimeUtils.longToTimeString(extra.getQueryEndTime()));
            item.setQuery_duration(String.valueOf(extra.getQueryDuration()));
            item.setExtra_message(GsonUtils.GSON.toJson(extra));
        }
        return item;
    }

    public List<ScalarOperator> toConstantOperators() {
        List<ScalarOperator> result = Lists.newArrayList();
        result.add(ConstantOperator.createVarchar(String.valueOf(warehouseId)));
        result.add(ConstantOperator.createVarchar(warehouseName));
        result.add(ConstantOperator.createVarchar(DebugUtil.printId(queryId)));
        result.add(ConstantOperator.createVarchar(state.name()));
        result.add(ConstantOperator.createVarchar(String.valueOf(estCostsSlots)));
        result.add(ConstantOperator.createVarchar(String.valueOf(allocateSlots)));
        result.add(ConstantOperator.createDouble(queuedWaitSeconds));
        result.add(ConstantOperator.createVarchar(query));
        if (extraMessage.isPresent()) {
            LogicalSlot.ExtraMessage extra = extraMessage.get();
            result.add(ConstantOperator.createVarchar(TimeUtils.longToTimeString(extra.getQueryStartTime())));
            result.add(ConstantOperator.createVarchar(TimeUtils.longToTimeString(extra.getQueryEndTime())));
            result.add(ConstantOperator.createVarchar(String.valueOf(extra.getQueryDuration())));
            result.add(ConstantOperator.createVarchar(GsonUtils.GSON.toJson(extra)));
        } else {
            result.add(ConstantOperator.createVarchar(""));
            result.add(ConstantOperator.createVarchar(""));
            result.add(ConstantOperator.createVarchar(""));
            result.add(ConstantOperator.createVarchar(""));
        }
        return result;
    }
}

