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

package com.starrocks.connector.starrocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.StarRocksExternalTable;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the connector statistics path: snapshot aggregation,
 * partition-pruned row counts, partition-column narrowing, per-partition
 * refinement, and the degradation ladder. All remote interaction is through
 * injected loaders; tests also assert the zero-extra-call contract.
 */
public class StarRocksMetadataStatisticsTest {

    private static final Column K_COL = new Column("k", IntegerType.INT, true);
    private static final Column V_COL = new Column("v", IntegerType.BIGINT, true);

    private static StarRocksRemoteTableStats.Snapshot rangeSnapshot() {
        StarRocksRemoteTableStats.Snapshot snapshot = new StarRocksRemoteTableStats.Snapshot();
        snapshot.status = 200;
        snapshot.epochs = new StarRocksRemoteTableStats.Epochs("l1", "d1", "a1");
        snapshot.partitionType = StarRocksRemoteTableStats.PARTITION_TYPE_RANGE;
        snapshot.partitionColumns = ImmutableList.of("k");
        snapshot.analyzeType = "FULL";
        snapshot.tableRowCount = 300;
        snapshot.partitions = new ArrayList<>();
        snapshot.partitions.add(rangePartition(1, "p1", 100, "0", "100"));
        snapshot.partitions.add(rangePartition(2, "p2", 100, "100", "200"));
        snapshot.partitions.add(rangePartition(3, "p3", 100, "200", "300"));
        StarRocksRemoteTableStats.ColumnStats kStats = columnStats("k", 300, 1200, 300, 0, "299", "0");
        StarRocksRemoteTableStats.ColumnStats vStats = columnStats("v", 300, 2400, 50, 30, "1000", "1");
        snapshot.columnStats = ImmutableList.of(kStats, vStats);
        return snapshot;
    }

    private static StarRocksRemoteTableStats.PartitionMeta rangePartition(long id, String name, long rowCount,
                                                                          String lower, String upper) {
        StarRocksRemoteTableStats.PartitionMeta partition = new StarRocksRemoteTableStats.PartitionMeta();
        partition.id = id;
        partition.name = name;
        partition.rowCount = rowCount;
        partition.rangeLower = new StarRocksRemoteTableStats.RangeBound();
        partition.rangeLower.values = ImmutableList.of(lower);
        partition.rangeUpper = new StarRocksRemoteTableStats.RangeBound();
        partition.rangeUpper.values = ImmutableList.of(upper);
        return partition;
    }

    private static StarRocksRemoteTableStats.ColumnStats columnStats(String name, long rowCount, double dataSize,
                                                                     long ndv, long nullCount, String max, String min) {
        StarRocksRemoteTableStats.ColumnStats stats = new StarRocksRemoteTableStats.ColumnStats();
        stats.column = name;
        stats.rowCount = rowCount;
        stats.dataSize = dataSize;
        stats.ndv = ndv;
        stats.nullCount = nullCount;
        stats.max = max;
        stats.min = min;
        return stats;
    }

    private static StarRocksExternalTable table(StarRocksRemoteTableStats.Snapshot snapshot, long tableRowCount) {
        return new StarRocksExternalTable(1, "sr_catalog", "db1", "tbl1",
                ImmutableList.of(K_COL, V_COL), 0, 1700000000L,
                snapshot == null ? ImmutableList.of() : snapshot.partitionColumns, tableRowCount,
                () -> snapshot);
    }

    private static Map<ColumnRefOperator, Column> columnRefs() {
        ColumnRefOperator kRef = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        ColumnRefOperator vRef = new ColumnRefOperator(2, IntegerType.BIGINT, "v", true);
        return ImmutableMap.of(kRef, K_COL, vRef, V_COL);
    }

    private static ColumnStatistic statOf(Statistics statistics, String name) {
        return statistics.getColumnStatistics().entrySet().stream()
                .filter(entry -> entry.getKey().getName().equals(name))
                .map(Map.Entry::getValue)
                .findFirst().orElseThrow();
    }

    @Test
    public void testDegradesToTableRowCountWithoutSnapshot() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        StarRocksMetadata metadata = new StarRocksMetadata("sr_catalog", feClient,
                new StarRocksMetadataCache(feClient, defaultOptions(), (d, t, e) -> null, null));
        StarRocksExternalTable table = table(null, 4096);

        Statistics statistics = metadata.getTableStatistics(null, table, columnRefs(), null, null, -1, null);

        Assertions.assertEquals(4096, statistics.getOutputRowCount(), 0.1);
        Assertions.assertTrue(statOf(statistics, "k").isUnknown());
        Assertions.assertTrue(statOf(statistics, "v").isUnknown());
    }

    @Test
    public void testUnprunedScanUsesAllPartitionsAndTableLevelColumnStats() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        AtomicInteger partitionLoads = new AtomicInteger();
        StarRocksMetadata metadata = new StarRocksMetadata("sr_catalog", feClient,
                new StarRocksMetadataCache(feClient, defaultOptions(), (d, t, e) -> null,
                        (d, t, ids, cols) -> {
                            partitionLoads.incrementAndGet();
                            return null;
                        }));
        StarRocksRemoteTableStats.Snapshot snapshot = rangeSnapshot();
        StarRocksExternalTable table = table(snapshot, 0);

        Statistics statistics = metadata.getTableStatistics(null, table, columnRefs(), null, null, -1, null);

        Assertions.assertEquals(300, statistics.getOutputRowCount(), 0.1);
        ColumnStatistic vStat = statOf(statistics, "v");
        Assertions.assertFalse(vStat.isUnknown());
        Assertions.assertEquals(50, vStat.getDistinctValuesCount(), 0.1);
        Assertions.assertEquals(0.1, vStat.getNullsFraction(), 0.001);
        Assertions.assertEquals(2400.0 / 300, vStat.getAverageRowSize(), 0.001);
        ColumnStatistic kStat = statOf(statistics, "k");
        Assertions.assertEquals(0, kStat.getMinValue(), 0.1);
        Assertions.assertEquals(299, kStat.getMaxValue(), 0.1);
        // Unpruned scans never touch the partition-level loader.
        Assertions.assertEquals(0, partitionLoads.get());
    }

    @Test
    public void testPrunedScanSumsSelectedPartitionsAndNarrowsPartitionColumn() {
        StarRocksFeClient feClient = Mockito.mock(StarRocksFeClient.class);
        StarRocksRemoteTableStats.PartitionStatsResponse refine =
                new StarRocksRemoteTableStats.PartitionStatsResponse();
        refine.status = 200;
        refine.partitionStats = new ArrayList<>();
        StarRocksRemoteTableStats.PartitionColumnStats vPart = new StarRocksRemoteTableStats.PartitionColumnStats();
        vPart.partitionId = 1;
        vPart.column = "v";
        vPart.ndv = 9;
        vPart.nullCount = 10;
        vPart.rowCount = 100;
        refine.partitionStats.add(vPart);
        StarRocksRemoteTableStats.PartitionColumnStats kPart = new StarRocksRemoteTableStats.PartitionColumnStats();
        kPart.partitionId = 1;
        kPart.column = "k";
        kPart.ndv = 100;
        kPart.nullCount = 0;
        kPart.rowCount = 100;
        refine.partitionStats.add(kPart);

        StarRocksMetadata metadata = new StarRocksMetadata("sr_catalog", feClient,
                new StarRocksMetadataCache(feClient, defaultOptions(), (d, t, e) -> null,
                        (d, t, ids, cols) -> refine));
        StarRocksRemoteTableStats.Snapshot snapshot = rangeSnapshot();
        StarRocksExternalTable table = table(snapshot, 0);

        // Simulate the pruner: canonical key of partition p1 only.
        Map<Long, PartitionKey> canonicalKeys =
                StarRocksStatsUtils.buildCanonicalKeys(snapshot, table.getPartitionColumns());
        List<PartitionKey> selectedKeys = ImmutableList.of(canonicalKeys.get(1L));

        Statistics statistics = metadata.getTableStatistics(null, table, columnRefs(), selectedKeys, null, -1, null);

        Assertions.assertEquals(100, statistics.getOutputRowCount(), 0.1);
        ColumnStatistic kStat = statOf(statistics, "k");
        // Narrowed to p1's range [0, 100).
        Assertions.assertEquals(0, kStat.getMinValue(), 0.1);
        Assertions.assertEquals(100, kStat.getMaxValue(), 0.1);
        Assertions.assertTrue(kStat.getDistinctValuesCount() <= 100.5);
        // Refined by per-partition stats: v ndv min(9, ...), null fraction 10/100.
        ColumnStatistic vStat = statOf(statistics, "v");
        Assertions.assertEquals(9, vStat.getDistinctValuesCount(), 0.1);
        Assertions.assertEquals(0.1, vStat.getNullsFraction(), 0.001);
    }

    private static StarRocksMetadataCache.Options defaultOptions() {
        return new StarRocksMetadataCache.Options(3600, 300, 1000, 100000);
    }
}
