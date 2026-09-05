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

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.Expr;

import java.util.Collections;
import java.util.List;

/**
 * Collects all inputs needed for resolving external base table partitions to MV partition keys.
 * Built once per mapping call, shared across all partition names in that batch.
 */
public class ExternalPartitionMappingContext {
    // The external base table (e.g., Hive, Iceberg, JDBC) whose partitions are being mapped.
    private final Table baseTable;
    // All partition columns of the external base table, in their original declaration order.
    private final List<Column> baseTablePartitionColumns;
    // The subset of partition columns that the MV actually references (i.e., the MV's partition-by columns).
    private final List<Column> mvRefBasePartitionColumns;
    // Positional indexes of mvRefBasePartitionColumns within baseTablePartitionColumns,
    // e.g., if baseTablePartitionColumns = [a, b, c] and mvRefBasePartitionColumns = [b],
    // then mvRefBasePartitionColumnIndexes = [1].
    private final List<Integer> mvRefBasePartitionColumnIndexes;
    // Optional partition expression (e.g., date_trunc('month', col)) used for range partition mapping;
    // null when doing multi-column list partition mapping.
    private final Expr mvPartitionExpr;

    private ExternalPartitionMappingContext(Table baseTable,
                                            List<Column> baseTablePartitionColumns,
                                            List<Column> mvRefBasePartitionColumns,
                                            List<Integer> mvRefBasePartitionColumnIndexes,
                                            Expr mvPartitionExpr) {
        this.baseTable = baseTable;
        this.baseTablePartitionColumns = baseTablePartitionColumns;
        this.mvRefBasePartitionColumns = mvRefBasePartitionColumns;
        this.mvRefBasePartitionColumnIndexes = mvRefBasePartitionColumnIndexes;
        this.mvPartitionExpr = mvPartitionExpr;
    }

    /**
     * Create context for multi-column mapping (used by getMVPartitionNameMapOfExternalTable).
     */
    public static ExternalPartitionMappingContext create(Table baseTable,
                                                         List<Column> mvRefBasePartitionColumns)
            throws AnalysisException {
        return create(baseTable, mvRefBasePartitionColumns, null);
    }

    /**
     * Create context for single-column range mapping (used by getRangePartitionMapOfExternalTable).
     */
    public static ExternalPartitionMappingContext create(Table baseTable,
                                                         Column mvRefBasePartitionColumn,
                                                         Expr mvPartitionExpr)
            throws AnalysisException {
        return create(baseTable, Collections.singletonList(mvRefBasePartitionColumn), mvPartitionExpr);
    }

    /**
     * Full factory method.
     */
    public static ExternalPartitionMappingContext create(Table baseTable,
                                                         List<Column> mvRefBasePartitionColumns,
                                                         Expr mvPartitionExpr)
            throws AnalysisException {
        List<Column> baseTablePartitionColumns = PartitionUtil.getPartitionColumns(baseTable);
        List<Integer> mvRefBasePartitionColumnIndexes =
                Lists.newArrayListWithCapacity(mvRefBasePartitionColumns.size());
        for (Column mvRefBasePartitionColumn : mvRefBasePartitionColumns) {
            mvRefBasePartitionColumnIndexes.add(
                    PartitionUtil.checkAndGetPartitionColumnIndex(
                            baseTablePartitionColumns, mvRefBasePartitionColumn));
        }
        return new ExternalPartitionMappingContext(
                baseTable,
                baseTablePartitionColumns,
                mvRefBasePartitionColumns,
                mvRefBasePartitionColumnIndexes,
                mvPartitionExpr);
    }

    public Table getBaseTable() {
        return baseTable;
    }

    public List<Column> getBaseTablePartitionColumns() {
        return baseTablePartitionColumns;
    }

    public List<Column> getMvRefBasePartitionColumns() {
        return mvRefBasePartitionColumns;
    }

    public List<Integer> getMvRefBasePartitionColumnIndexes() {
        return mvRefBasePartitionColumnIndexes;
    }

    public Expr getMvPartitionExpr() {
        return mvPartitionExpr;
    }

    /**
     * Get the actual base-table partition column at a given MV-ref position.
     * E.g., if mvRefBasePartitionColumnIndexes = [2], then
     * getBaseTablePartitionColumnAtRefPosition(0) returns baseTablePartitionColumns.get(2).
     */
    public Column getBaseTablePartitionColumnAtRefPosition(int mvRefPosition) {
        return baseTablePartitionColumns.get(mvRefBasePartitionColumnIndexes.get(mvRefPosition));
    }
}
