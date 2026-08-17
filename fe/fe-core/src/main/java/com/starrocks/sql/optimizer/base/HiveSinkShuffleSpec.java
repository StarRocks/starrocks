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

package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

/**
 * Hive {@link ConnectorSinkShuffleSpec}. Hive supports only identity partition
 * columns (no transforms), so {@link #prepare} computes hash key IDs directly
 * from {@code outputFullSchema.indexOf(column)} and never rewrites the plan.
 */
public class HiveSinkShuffleSpec implements ConnectorSinkShuffleSpec {

    private final HiveTable hiveTable;

    public HiveSinkShuffleSpec(HiveTable hiveTable) {
        this.hiveTable = hiveTable;
    }

    @Override
    public Table table() {
        return hiveTable;
    }

    @Override
    public PreOptimizeResult prepare(InsertStmt insertStmt,
                                     List<ColumnRefOperator> outputColumns,
                                     ColumnRefFactory columnRefFactory,
                                     OptExpression root,
                                     ColumnRefSet requiredColumns) {
        List<Column> outputFullSchema = insertStmt.getTargetTable().getBaseSchema();
        List<Integer> partitionColumnIds = Lists.newArrayList();
        for (Column column : hiveTable.getPartitionColumns()) {
            int index = outputFullSchema.indexOf(column);
            Preconditions.checkState(index >= 0,
                    "Hive partition column not found in output schema: " + column.getName());
            partitionColumnIds.add(outputColumns.get(index).getId());
        }
        // No plan rewrite for Hive (identity-only partition columns).
        return new PreOptimizeResult(root, requiredColumns, partitionColumnIds);
    }
}
