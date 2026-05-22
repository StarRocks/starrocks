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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test for StatisticsCalcUtils, especially the fix for NPE when partitions are
 * dropped concurrently during query planning.
 */
public class StatisticsCalcUtilsTest {
    private static ConnectContext connectContext;
    private static OptimizerContext optimizerContext;
    private static ColumnRefFactory columnRefFactory;
    private static StarRocksAssert starRocksAssert;
    private static boolean originalRunningUnitTest;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = OptimizerFactory.mockContext(connectContext, columnRefFactory);

        starRocksAssert = new StarRocksAssert(connectContext);

        String dbName = "statistics_calc_utils_test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        // Save the original value so we can restore it after the test class finishes,
        // to avoid leaking state into other tests.
        originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
    }

    @AfterAll
    public static void afterClass() {
        // Restore the original value to avoid polluting global state for other tests.
        FeConstants.runningUnitTest = originalRunningUnitTest;
    }

    @BeforeEach
    public void before() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `test_partition_table` (\n" +
                "  `k1` int(11) NULL COMMENT \"\",\n" +
                "  `k2` varchar(20) NULL COMMENT \"\",\n" +
                "  `v1` bigint(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"10\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"20\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"30\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @AfterEach
    public void after() throws Exception {
        starRocksAssert.dropTable("test_partition_table");
    }

    /**
     * Test that getTableRowCount does not throw NPE when selectedPartitionIds
     * contains a partition ID that no longer exists in the table (simulating
     * concurrent partition drop).
     */
    @Test
    public void testGetTableRowCountWithDroppedPartition() throws Exception {
        OlapTable table = (OlapTable) connectContext.getGlobalStateMgr()
                .getLocalMetastore().getDb("statistics_calc_utils_test")
                .getTable("test_partition_table");

        Collection<Partition> partitions = table.getPartitions();
        // Get real partition IDs and add a non-existent one to simulate concurrent drop
        List<Long> partitionIds = partitions.stream()
                .mapToLong(Partition::getId).boxed().collect(Collectors.toList());
        // Derive an id guaranteed to be absent from the current table, instead of hard-coding
        // a magic number that could theoretically collide with a real partition id.
        long nonExistentPartitionId = partitions.stream()
                .mapToLong(Partition::getId).max().orElse(0L) + 1L;
        Assertions.assertFalse(partitionIds.contains(nonExistentPartitionId),
                "derived partition id should not exist in current table");
        partitionIds.add(nonExistentPartitionId);

        for (Partition partition : partitions) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000);
        }

        List<Column> columns = table.getColumns();
        Column column = columns.get(0);
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        ColumnRefOperator ref = new ColumnRefOperator(0, column.getType(), column.getName(), true);
        refToColumn.put(ref, column);
        columnToRef.put(column, ref);

        LogicalOlapScanOperator olapScanOperator = new LogicalOlapScanOperator(table,
                refToColumn, columnToRef,
                null, -1, null,
                table.getBaseIndexMetaId(),
                partitionIds,
                null,
                false,
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                false);

        // This should NOT throw NPE even though one partition ID doesn't exist
        long rowCount = StatisticsCalcUtils.getTableRowCount(table, olapScanOperator, optimizerContext);

        // Row count should only include existing partitions (3 partitions * 1000 rows each)
        Assertions.assertEquals(3000, rowCount);
    }

    /**
     * Test that getTableRowCount works correctly when all partitions exist.
     */
    @Test
    public void testGetTableRowCountWithAllPartitionsExist() throws Exception {
        OlapTable table = (OlapTable) connectContext.getGlobalStateMgr()
                .getLocalMetastore().getDb("statistics_calc_utils_test")
                .getTable("test_partition_table");

        Collection<Partition> partitions = table.getPartitions();
        List<Long> partitionIds = partitions.stream()
                .mapToLong(Partition::getId).boxed().collect(Collectors.toList());

        for (Partition partition : partitions) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(500);
        }

        List<Column> columns = table.getColumns();
        Column column = columns.get(0);
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        ColumnRefOperator ref = new ColumnRefOperator(0, column.getType(), column.getName(), true);
        refToColumn.put(ref, column);
        columnToRef.put(column, ref);

        LogicalOlapScanOperator olapScanOperator = new LogicalOlapScanOperator(table,
                refToColumn, columnToRef,
                null, -1, null,
                table.getBaseIndexMetaId(),
                partitionIds,
                null,
                false,
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                false);

        long rowCount = StatisticsCalcUtils.getTableRowCount(table, olapScanOperator, optimizerContext);

        // Row count should be 3 partitions * 500 rows each = 1500
        Assertions.assertEquals(1500, rowCount);
    }

    /**
     * Test that getTableRowCount does not throw NPE when ALL selected partitions
     * have been dropped (edge case).
     */
    @Test
    public void testGetTableRowCountWithAllPartitionsDropped() throws Exception {
        OlapTable table = (OlapTable) connectContext.getGlobalStateMgr()
                .getLocalMetastore().getDb("statistics_calc_utils_test")
                .getTable("test_partition_table");

        // Derive ids guaranteed to be absent from the current table, instead of hard-coding
        // magic numbers that could theoretically collide with real partition ids.
        long maxExistingId = table.getPartitions().stream()
                .mapToLong(Partition::getId).max().orElse(0L);
        long nonExistentId1 = maxExistingId + 1L;
        long nonExistentId2 = maxExistingId + 2L;
        List<Long> existingIds = table.getPartitions().stream()
                .mapToLong(Partition::getId).boxed().collect(Collectors.toList());
        Assertions.assertFalse(existingIds.contains(nonExistentId1),
                "derived partition id should not exist in current table");
        Assertions.assertFalse(existingIds.contains(nonExistentId2),
                "derived partition id should not exist in current table");
        List<Long> partitionIds = Lists.newArrayList(nonExistentId1, nonExistentId2);

        List<Column> columns = table.getColumns();
        Column column = columns.get(0);
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        ColumnRefOperator ref = new ColumnRefOperator(0, column.getType(), column.getName(), true);
        refToColumn.put(ref, column);
        columnToRef.put(column, ref);

        LogicalOlapScanOperator olapScanOperator = new LogicalOlapScanOperator(table,
                refToColumn, columnToRef,
                null, -1, null,
                table.getBaseIndexMetaId(),
                partitionIds,
                null,
                false,
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                false);

        // Should not throw NPE, should return minimum row count (1)
        long rowCount = StatisticsCalcUtils.getTableRowCount(table, olapScanOperator, optimizerContext);

        // When all partitions are dropped, selectedPartitions is empty, row count should be 1 (minimum)
        Assertions.assertEquals(1, rowCount);
    }
}
