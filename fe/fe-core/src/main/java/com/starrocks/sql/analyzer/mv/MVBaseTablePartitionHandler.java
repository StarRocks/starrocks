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

package com.starrocks.sql.analyzer.mv;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.PrimitiveType;

/**
 * Strategy interface for handling base-table-type-specific partition column validation
 * in materialized view analysis.
 * <p>
 * Each supported base table type (OlapTable, Hive/Hudi/ODPS, Iceberg, JDBC, Paimon) provides
 * its own implementation. This eliminates if-else branching by table type in
 * {@code MaterializedViewAnalyzer}.
 */
public interface MVBaseTablePartitionHandler {

    /**
     * Validate that the MV partition column references a valid partition column in the base table.
     */
    void checkPartitionColumn(MVPartitionCheckContext context);

    /**
     * Validate that the MV partition expression references a valid partition column in the base table.
     * <p>
     * Checks performed:
     * <ol>
     *   <li>The base table must be partitioned.</li>
     *   <li>The column referenced by {@code mvPartitionSlotRef} must exist in the base table's partition columns.</li>
     *   <li>The matched partition column's type must be one of: fixed-point integer, date/datetime, or string.</li>
     * </ol>
     *
     * @param mvPartitionSlotRef the slot reference from the MV's PARTITION BY expression
     * @param baseTable          the base table that the MV is built on
     * @throws SemanticException if the base table is unpartitioned, the referenced column is not a partition column,
     *                           or the partition column type is not supported
     */
    static void checkPartitionColumnWithBaseTable(SlotRef mvPartitionSlotRef, Table baseTable) {
        if (baseTable.isUnPartitioned()) {
            throw new SemanticException("Materialized view partition column in partition exp " +
                    "must be base table partition column");
        }
        boolean found = false;
        for (Column partitionColumn : baseTable.getPartitionColumns()) {
            if (partitionColumn.getName().equalsIgnoreCase(mvPartitionSlotRef.getColumnName())) {
                PrimitiveType type = partitionColumn.getPrimitiveType();
                if (!type.isFixedPointType() && !type.isDateType() && !type.isStringType()) {
                    throw new SemanticException("Materialized view partition exp column:"
                            + partitionColumn.getName() + " with type " + type + " not supported");
                }
                found = true;
                break;
            }
        }
        if (!found) {
            throw new SemanticException("Materialized view partition column in partition exp " +
                    "must be base table partition column");
        }
    }
}
