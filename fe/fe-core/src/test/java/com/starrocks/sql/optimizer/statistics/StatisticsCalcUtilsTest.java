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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.TabletStatMgr;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    @Test
    public void testProvenBaseRowCount() {
        MaterializedIndex baseIndex = new MaterializedIndex(10L);
        Partition partition = new Partition(1L, 3L, "p", baseIndex, null);
        MaterializedIndex rollup = new MaterializedIndex(11L, MaterializedIndex.IndexState.NORMAL);
        partition.getDefaultPhysicalPartition().createRollupIndex(rollup);
        long visibleVersion = partition.getDefaultPhysicalPartition().getVisibleVersion();

        // nothing proven
        baseIndex.setRowCount(100L);
        Assertions.assertEquals(-1L, StatisticsCalcUtils.provenBaseRowCount(partition));

        // proven at the visible version: the base index count, without the rollup Partition#getRowCount adds
        baseIndex.setRowCount(100L, visibleVersion);
        rollup.setRowCount(100L);
        Assertions.assertEquals(100L, StatisticsCalcUtils.provenBaseRowCount(partition));
        Assertions.assertEquals(200L, partition.getRowCount());

        // a newer version became visible without a proven count for it
        partition.getDefaultPhysicalPartition().updateVisibleVersion(visibleVersion + 1);
        Assertions.assertEquals(-1L, StatisticsCalcUtils.provenBaseRowCount(partition));
    }

    @Test
    public void testProvenBaseRowCountRejectsVersionPublishedWhileReading() {
        MaterializedIndex baseIndex = new MaterializedIndex(10L);
        Partition partition = new Partition(1L, 3L, "p", baseIndex, null);
        long visibleVersion = partition.getDefaultPhysicalPartition().getVisibleVersion();
        baseIndex.setRowCount(100L, visibleVersion);

        // a publish makes the next version visible between reading the version and reading its proven count
        new MockUp<PhysicalPartition>() {
            private int reads = 0;

            @Mock
            public long getVisibleVersion() {
                return reads++ == 0 ? visibleVersion : visibleVersion + 1;
            }
        };
        Assertions.assertEquals(-1L, StatisticsCalcUtils.provenBaseRowCount(partition));
    }

    private long rowCountOfAllPartitions(OlapTable table) {
        return rowCountOfPartitions(table, Lists.newArrayList(table.getPartitions()));
    }

    // The scan selects these partitions, in this order, which is also the order the estimate reads them.
    private long rowCountOfPartitions(OlapTable table, List<Partition> partitions) {
        Column column = table.getColumns().get(0);
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        ColumnRefOperator ref = new ColumnRefOperator(0, column.getType(), column.getName(), true);
        refToColumn.put(ref, column);
        columnToRef.put(column, ref);
        List<Long> partitionIds = partitions.stream()
                .mapToLong(Partition::getId).boxed().collect(Collectors.toList());
        LogicalOlapScanOperator scan = new LogicalOlapScanOperator(table, refToColumn, columnToRef, null, -1, null,
                table.getBaseIndexMetaId(), partitionIds, null, false, Lists.newArrayList(), Lists.newArrayList(),
                Lists.newArrayList(), false);
        return StatisticsCalcUtils.getTableRowCount(table, scan, optimizerContext);
    }

    // Proves each partition's base index count at its visible version.
    private static void proveBaseRowCounts(OlapTable table, long provenRowCount) {
        for (Partition partition : table.getPartitions()) {
            PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
            physicalPartition.getLatestBaseIndex().setRowCount(provenRowCount, physicalPartition.getVisibleVersion());
        }
    }

    private static void setBasicStats(OlapTable table, StatsConstants.AnalyzeType type, long totalRows) {
        BasicStatsMeta meta = new BasicStatsMeta(table.getId() + 1, table.getId(), Lists.newArrayList(), type,
                LocalDateTime.now(), StatsConstants.buildInitStatsProp(), totalRows);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().put(table.getId(), meta);
    }

    private static void setSampleStats(OlapTable table, long totalRows) {
        setBasicStats(table, StatsConstants.AnalyzeType.SAMPLE, totalRows);
    }

    // No TabletStatMgr round has finished yet, so the stats and every partition's last load are newer than it,
    // whatever the background daemon does while the test runs.
    private static void pinLastTabletStatRound() {
        new MockUp<TabletStatMgr>() {
            @Mock
            public LocalDateTime getLastWorkTimestamp() {
                return LocalDateTime.MIN;
            }
        };
    }

    // No partition has an analyzed row count, whatever the asynchronously loaded statistics cache holds. Mocks the
    // class of the storage actually installed, so it holds even if another test installed a different one.
    private static <T extends StatisticStorage> void noAnalyzedRowCounts() {
        @SuppressWarnings("unchecked")
        Class<T> storageClass = (Class<T>) GlobalStateMgr.getCurrentState().getStatisticStorage().getClass();
        new MockUp<T>(storageClass) {
            @Mock
            public Map<Long, Optional<Long>> getTableStatistics(Long tableId, Collection<Partition> partitions) {
                return partitions.stream().collect(Collectors.toMap(Partition::getId, p -> Optional.empty()));
            }
        };
    }

    @Test
    public void testProvenRowCountsForBaseIndexOnlyTable() throws Exception {
        OlapTable table = (OlapTable) connectContext.getGlobalStateMgr()
                .getLocalMetastore().getDb("statistics_calc_utils_test")
                .getTable("test_partition_table");
        Assertions.assertTrue(StatisticsCalcUtils.isBaseIndexOnly(table));
        pinLastTabletStatRound();
        try {
            // SAMPLE stats whose totalRows counts rows written: ignored while every partition is proven
            proveBaseRowCounts(table, 1000L);
            setSampleStats(table, 90000L);
            Assertions.assertEquals(3000, rowCountOfAllPartitions(table));

            // not proven any more: the previous estimate, totalRows adjustment included, comes back
            for (Partition partition : table.getPartitions()) {
                partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000L);
            }
            Assertions.assertEquals(90000, rowCountOfAllPartitions(table));
        } finally {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().remove(table.getId());
        }
    }

    @Test
    public void testProvenRowCountsNotUsedWithRollup() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `test_rollup_table` (\n" +
                "  `k1` int(11) NULL,\n" +
                "  `k2` int(11) NULL,\n" +
                "  `v1` bigint(20) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "ROLLUP (r1 (k2, k1))\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
        try {
            OlapTable table = (OlapTable) connectContext.getGlobalStateMgr()
                    .getLocalMetastore().getDb("statistics_calc_utils_test")
                    .getTable("test_rollup_table");
            Assertions.assertFalse(StatisticsCalcUtils.isBaseIndexOnly(table));
            pinLastTabletStatRound();
            proveBaseRowCounts(table, 1000L);
            for (Partition partition : table.getPartitions()) {
                for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                        .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                    if (index != partition.getDefaultPhysicalPartition().getLatestBaseIndex()) {
                        index.setRowCount(1000L);
                    }
                }
            }
            // Partition#getRowCount adds the rollup, so its totalRows adjustment isn't skipped for the proven count
            setSampleStats(table, 90000L);
            Assertions.assertEquals(90000, rowCountOfAllPartitions(table));
        } finally {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().remove(
                    ((OlapTable) connectContext.getGlobalStateMgr().getLocalMetastore()
                            .getDb("statistics_calc_utils_test").getTable("test_rollup_table")).getId());
            starRocksAssert.dropTable("test_rollup_table");
        }
    }

    @Test
    public void testProvenRowCountSkipsDeltaForUnanalyzedPartitions() throws Exception {
        OlapTable table = (OlapTable) connectContext.getGlobalStateMgr()
                .getLocalMetastore().getDb("statistics_calc_utils_test")
                .getTable("test_partition_table");
        pinLastTabletStatRound();
        noAnalyzedRowCounts();
        try {
            // FULL stats whose totalRows is far above the table's partitions, none of them analyzed: every one
            // changed since the last tablet stat round, so each gets a share of the difference as an estimated delta
            setBasicStats(table, StatsConstants.AnalyzeType.FULL, 90000L);
            for (Partition partition : table.getPartitions()) {
                partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000L);
            }
            Assertions.assertEquals(90000, rowCountOfAllPartitions(table));

            // proven at their visible version: no change to estimate
            proveBaseRowCounts(table, 1000L);
            Assertions.assertEquals(3000, rowCountOfAllPartitions(table));
        } finally {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().remove(table.getId());
        }
    }

    @Test
    public void testSampleAdjustmentWithSomePartitionsUnproven() throws Exception {
        OlapTable table = (OlapTable) connectContext.getGlobalStateMgr()
                .getLocalMetastore().getDb("statistics_calc_utils_test")
                .getTable("test_partition_table");
        pinLastTabletStatRound();
        try {
            setSampleStats(table, 90000L);
            proveBaseRowCounts(table, 1000L);
            // one partition's count is not proven: the totalRows adjustment applies to the whole estimate. It is put
            // last in the scan's partition list, which fixes the order the estimate reads them in, so that the
            // proven checks of the other partitions all run before it.
            List<Partition> partitions = Lists.newArrayList(table.getPartitions());
            Partition unproven = partitions.remove(0);
            partitions.add(unproven);
            unproven.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000L);
            Assertions.assertEquals(90000, rowCountOfPartitions(table, partitions));

            // A publish proving that partition lands right after its count was read for the sum. The estimate must
            // still treat it as unproven: the sum holds the count read before the publish. A second read for the
            // proven check would see it proven, skip the adjustment and leave the older sum, 3000.
            long unprovenId = unproven.getId();
            new MockUp<Partition>() {
                private boolean published = false;

                @Mock
                public long getRowCount(Invocation invocation) {
                    Partition partition = invocation.getInvokedInstance();
                    long rowCount = invocation.proceed();
                    if (partition.getId() == unprovenId && !published) {
                        published = true;
                        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
                        physicalPartition.getLatestBaseIndex().setRowCount(rowCount,
                                physicalPartition.getVisibleVersion());
                    }
                    return rowCount;
                }
            };
            Assertions.assertEquals(90000, rowCountOfPartitions(table, partitions));
        } finally {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().remove(table.getId());
        }
    }
}

