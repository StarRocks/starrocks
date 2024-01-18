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

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.structure.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Show all the unfinished queries retrieving from the slot manager.
 * <p> Note that it only shows the queries managed by the slot manager, that is, enabling query queue.
 */
public class ShowRunningQueriesStmt extends ShowStmt {
    private static final List<Pair<Column, Function<LogicalSlot, String>>> META_DATA = ImmutableList.of(
            Pair.create(new Column("QueryId", ScalarType.createVarchar(64)),
                    slot -> DebugUtil.printId(slot.getSlotId())),
            Pair.create(new Column("ResourceGroupId", ScalarType.createVarchar(64)),
                    slot -> slot.getGroupId() == LogicalSlot.ABSENT_GROUP_ID ? "-" : Long.toString(slot.getGroupId())),
            Pair.create(new Column("StartTime", ScalarType.createVarchar(64)),
                    slot -> TimeUtils.longToTimeString(slot.getStartTimeMs())),
            Pair.create(new Column("PendingTimeout", ScalarType.createVarchar(64)),
                    slot -> TimeUtils.longToTimeString(slot.getExpiredPendingTimeMs())),
            Pair.create(new Column("QueryTimeout", ScalarType.createVarchar(64)),
                    slot -> TimeUtils.longToTimeString(slot.getExpiredAllocatedTimeMs())),
            Pair.create(new Column("State", ScalarType.createVarchar(64)),
                    slot -> slot.getState().toQueryStateString()),
            Pair.create(new Column("Slots", ScalarType.createVarchar(64)),
                    slot -> Integer.toString(slot.getNumPhysicalSlots())),
            Pair.create(new Column("Fragments", ScalarType.createVarchar(64)),
                    slot -> Integer.toString(slot.getNumFragments())),
            Pair.create(new Column("DOP", ScalarType.createVarchar(64)),
                    slot -> Integer.toString(slot.getPipelineDop())),
            Pair.create(new Column("Frontend", ScalarType.createVarchar(64)),
                    LogicalSlot::getRequestFeName),
            Pair.create(new Column("FeStartTime", ScalarType.createVarchar(64)),
                    slot -> TimeUtils.longToTimeString(slot.getFeStartTimeMs()))
    );

    private static final ShowResultSetMetaData COLUMN_META_DATA;

    private static final List<Function<LogicalSlot, String>> COLUMN_SUPPLIERS = META_DATA.stream()
            .map(item -> item.second).collect(Collectors.toList());

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        META_DATA.forEach(item -> builder.addColumn(item.first));
        COLUMN_META_DATA = builder.build();
    }

    private final int limit;

    public ShowRunningQueriesStmt(int limit, NodePosition pos) {
        super(pos);
        this.limit = limit;
    }

    public static List<Function<LogicalSlot, String>> getColumnSuppliers() {
        return COLUMN_SUPPLIERS;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowRunningQueriesStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return COLUMN_META_DATA;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    public int getLimit() {
        return limit;
    }
}
