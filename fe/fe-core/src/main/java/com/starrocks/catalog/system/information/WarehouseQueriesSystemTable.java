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
package com.starrocks.catalog.system.information;

import com.google.api.client.util.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.warehouse.WarehouseQueryMetrics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class WarehouseQueriesSystemTable extends SystemTable {
    public static final String NAME = "warehouse_queries";
    public WarehouseQueriesSystemTable() {
        super(
                SystemId.WAREHOUSE_QUERIES_METRICS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("WAREHOUSE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("WAREHOUSE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("QUERY_ID", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("STATE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("EST_COSTS_SLOTS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ALLOCATE_SLOTS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("QUEUED_WAIT_SECONDS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("QUERY", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("QUERY_START_TIME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("QUERY_END_TIME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("QUERY_DURATION", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("EXTRA_MESSAGE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_WAREHOUSE_QUERIES);
    }

    public static SystemTable create() {
        return new WarehouseQueriesSystemTable();
    }

    private static final Set<String> SUPPORTED_EQUAL_COLUMNS =
            Collections.unmodifiableSet(new TreeSet<>(String.CASE_INSENSITIVE_ORDER) {
                {
                    add("WAREHOUSE_ID");
                    add("WAREHOUSE_NAME");
                }
            });

    @Override
    public boolean supportFeEvaluation(ScalarOperator predicate) {
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        if (conjuncts.isEmpty()) {
            return true;
        }
        if (!isEmptyOrOnlyEqualConstantOps(conjuncts) || conjuncts.size() != 1) {
            return false;
        }
        return isSupportedEqualPredicateColumn(conjuncts, SUPPORTED_EQUAL_COLUMNS);
    }

    @Override
    public List<List<ScalarOperator>> evaluate(ScalarOperator predicate) {
        final BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        if (CollectionUtils.isEmpty(conjuncts)) {
            final List<LogicalSlot> slots = slotManager.getSlots();
            return slots.stream()
                    .map(tracker -> WarehouseQueryMetrics.create(tracker).toConstantOperators())
                    .collect(Collectors.toUnmodifiableList());
        } else {
            List<List<ScalarOperator>> result = Lists.newArrayList();
            ScalarOperator conjunct = conjuncts.get(0);
            BinaryPredicateOperator binary = (BinaryPredicateOperator) conjunct;
            ColumnRefOperator columnRef = binary.getChild(0).cast();
            String name = columnRef.getName();
            ConstantOperator value = binary.getChild(1).cast();

            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Warehouse warehouse;
            switch (name.toUpperCase()) {
                case "WAREHOUSE_ID": {
                    long warehouseId = value.getBigint();
                    warehouse = warehouseManager.getWarehouse(warehouseId);
                    break;
                }
                case "WAREHOUSE_NAME":
                    String warehouseName = value.getVarchar();
                    warehouse = warehouseManager.getWarehouse(warehouseName);
                    break;
                default:
                    throw new NotImplementedException("unsupported column: " + name);
            }
            if (warehouse == null) {
                return result; // empty result if warehouse not found
            } else {
                final List<LogicalSlot> slots = slotManager.getSlots();
                return slots.stream()
                        .filter(slot -> slot.getWarehouseId() == warehouse.getId())
                        .map(slot -> WarehouseQueryMetrics.create(slot))
                        .map(WarehouseQueryMetrics::toConstantOperators)
                        .collect(Collectors.toUnmodifiableList());
            }
        }
    }
}
