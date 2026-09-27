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

package com.starrocks.sql.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

// Delta Lake's split enumeration is lazy (Delta Kernel), so unlike Hive/Iceberg's plan tests, exercising the
// partition-filter-required check via a full SQL/mock-catalog query would require a working Delta Kernel
// snapshot. This test instead drives OptExternalPartitionPruner directly against a hand-built DeltaLakeTable +
// LogicalDeltaLakeScanOperator, which is all the check itself actually needs (predicate shape + static
// partition-column metadata, no file enumeration).
public class DeltaLakePartitionFilterRequiredTest extends ConnectorPlanTestBase {
    private static final String CATALOG_NAME = "delta_catalog";
    private static final String DB_NAME = "delta_db";

    private ColumnRefOperator partitionColumnRef;
    private Column partitionColumn;

    @BeforeEach
    public void setUp() {
        super.setUp();
        connectContext.getSessionVariable().setAllowLakeWithoutPartitionFilter(false);
        partitionColumn = new Column("date", StringType.STRING, true);
        partitionColumnRef = new ColumnRefOperator(1, StringType.STRING, "date", true);
    }

    private LogicalDeltaLakeScanOperator buildScanOperator(boolean partitioned, ScalarOperator predicate) {
        Column idColumn = new Column("id", IntegerType.INT, true);
        List<Column> columns = partitioned
                ? ImmutableList.of(idColumn, partitionColumn)
                : ImmutableList.of(idColumn);
        List<String> partitionNames = partitioned ? ImmutableList.of("date") : Collections.emptyList();

        DeltaLakeTable table = new DeltaLakeTable(1L, CATALOG_NAME, DB_NAME, "t1", columns, partitionNames,
                null, null, new MetastoreTable(DB_NAME, "t1", "file:///tmp/delta/t1", 0));

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = Maps.newHashMap();
        colRefToColumnMetaMap.put(partitionColumnRef, partitionColumn);
        columnMetaToColRefMap.put(partitionColumn, partitionColumnRef);

        return new LogicalDeltaLakeScanOperator(table, colRefToColumnMetaMap, columnMetaToColRefMap,
                Operator.DEFAULT_LIMIT, predicate);
    }

    private void prunePartitions(LogicalScanOperator operator) {
        OptExternalPartitionPruner.prunePartitions(new OptimizerContext(connectContext), operator);
    }

    @Test
    public void testValidPartitionFilterSucceeds() {
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, partitionColumnRef,
                ConstantOperator.createVarchar("2020-01-01"));
        Assertions.assertDoesNotThrow(() -> prunePartitions(buildScanOperator(true, predicate)));
    }

    @Test
    public void testMissingPartitionFilterThrows() {
        Assertions.assertThrows(StarRocksPlannerException.class,
                () -> prunePartitions(buildScanOperator(true, null)));
    }

    @Test
    public void testFunctionWrappedPartitionFilterThrows() {
        CallOperator upper = new CallOperator("upper", StringType.STRING, List.of(partitionColumnRef));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, upper,
                ConstantOperator.createVarchar("2020-01-01"));
        Assertions.assertThrows(StarRocksPlannerException.class,
                () -> prunePartitions(buildScanOperator(true, predicate)));
    }

    @Test
    public void testLikePartitionFilterThrows() {
        ScalarOperator predicate = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE,
                partitionColumnRef, ConstantOperator.createVarchar("2020%"));
        Assertions.assertThrows(StarRocksPlannerException.class,
                () -> prunePartitions(buildScanOperator(true, predicate)));
    }

    @Test
    public void testUnpartitionedTableAlwaysSucceeds() {
        Assertions.assertDoesNotThrow(() -> prunePartitions(buildScanOperator(false, null)));
    }
}
