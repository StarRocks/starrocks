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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.BaseSlotTracker;
import com.starrocks.qe.scheduler.warehouse.WarehouseMetrics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class WarehouseMetricsSystemTable extends SystemTable {
    public static final String NAME = "warehouse_metrics";

    public WarehouseMetricsSystemTable() {
        super(
                SystemId.WAREHOUSE_METRICS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("WAREHOUSE_ID", IntegerType.BIGINT)
                        .column("WAREHOUSE_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("QUEUE_PENDING_LENGTH", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("QUEUE_RUNNING_LENGTH", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("MAX_PENDING_LENGTH", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("MAX_PENDING_TIME_SECOND", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("EARLIEST_QUERY_WAIT_TIME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("MAX_REQUIRED_SLOTS", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("SUM_REQUIRED_SLOTS", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("REMAIN_SLOTS", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("MAX_SLOTS", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("EXTRA_MESSAGE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_WAREHOUSE_METRICS);
    }

    public static SystemTable create() {
        return new WarehouseMetricsSystemTable();
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
            final Map<Long, BaseSlotTracker> warehouseTrackers = slotManager.getWarehouseIdToSlotTracker();
            return warehouseTrackers.values().stream()
                    .map(tracker -> WarehouseMetrics.create(tracker).toConstantOperators())
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
                BaseSlotTracker tracker = slotManager.getWarehouseIdToSlotTracker().get(warehouse.getId());
                if (tracker != null) {
                    result.add(WarehouseMetrics.create(tracker).toConstantOperators());
                }
                return result;
            }
        }
    }
}
