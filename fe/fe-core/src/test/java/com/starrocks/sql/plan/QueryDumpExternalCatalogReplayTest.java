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

package com.starrocks.sql.plan;

import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Replays real query dumps captured from a cluster (with {@code mock=false}) over external-catalog tables
 * referenced by their fully-qualified {@code catalog.db.table} name -- the shape modern iceberg and hive
 * catalogs produce, as opposed to the legacy resource-mapping form the older dump tests exercise.
 *
 * <p>Such a dump records the table only as a {@code CREATE EXTERNAL TABLE ... ENGINE=ICEBERG|HIVE
 * ("resource"=...)} statement in {@code table_meta}; the backing metastore is not part of the dump.
 * Replay must therefore synthesize the table from the declared schema entirely offline
 * (UtFrameUtils.registerReplayExternalCatalogTables via ReplayIcebergCatalogMetadata /
 * ReplayHiveCatalogMetadata) and register a real external catalog of the matching type so the
 * fully-qualified reference resolves. Before this, the query analyzer's catalog-name rewrite mapped every
 * catalog reference to a (nonexistent) {@code resource_mapping_inside_catalog_hive_*} catalog and the
 * replay failed at analysis; the hive side additionally NPE'd in the legacy resource-mapping replay when
 * the dump carried no per-column statistics.
 *
 * <p>These dumps were produced by:
 *   POST /api/query_dump?db=&lt;catalog&gt;.bench_starrocks&amp;mock=false -d "&lt;query&gt;"
 * against a cluster whose {@code iceberg_test_rd} / {@code hive_test_rd} catalogs expose the same
 * {@code lineorder_100g} table. The tables carry no analyzed column statistics, so replay falls back to
 * UNKNOWN per-column statistics -- exactly what the cluster itself would hand back.
 */
public class QueryDumpExternalCatalogReplayTest extends ReplayFromDumpTestBase {

    private String replay(String dumpResource) throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile(dumpResource));
        Assertions.assertNotNull(replayPair.second, "replay produced no plan for " + dumpResource);
        return replayPair.second;
    }

    @Test
    public void testReplayIcebergCatalogCount() throws Exception {
        // select count(*) from iceberg_test_rd.bench_starrocks.lineorder_100g
        String plan = replay("query_dump/iceberg_catalog_count");
        Assertions.assertTrue(plan.contains("IcebergScanNode"), "expected an iceberg scan, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("lineorder_100g"), "expected the iceberg table scanned, plan:\n" + plan);
        // The dump carries no table_row_count, so replay falls back to the default row count (100); the scan
        // cardinality must reflect that recovered value rather than collapsing to the empty-table default.
        Assertions.assertTrue(plan.contains("cardinality=100"),
                "expected the iceberg scan cardinality from the fallback row count, plan:\n" + plan);
    }

    @Test
    public void testReplayIcebergCatalogPredicate() throws Exception {
        // select lo_orderkey, count(*) ... where lo_orderkey > 1000 group by lo_orderkey -- exercises
        // column projection and a scan predicate over the synthesized iceberg table.
        String plan = replay("query_dump/iceberg_catalog_predicate");
        Assertions.assertTrue(plan.contains("IcebergScanNode"), "expected an iceberg scan, plan:\n" + plan);
        // The WHERE predicate must survive onto the synthesized iceberg scan (asserting only that the
        // projected column appears would pass even if the predicate were dropped, since lo_orderkey is also
        // the GROUP BY key/output slot).
        Assertions.assertTrue(plan.contains("lo_orderkey > 1000"),
                "expected the WHERE predicate pushed onto the iceberg scan, plan:\n" + plan);
    }

    @Test
    public void testReplayHiveCatalogCount() throws Exception {
        // select count(*) from hive_test_rd.bench_starrocks.lineorder_100g
        String plan = replay("query_dump/hive_catalog_count");
        Assertions.assertTrue(plan.contains("HdfsScanNode"), "expected a hive scan, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("lineorder_100g"), "expected the hive table scanned, plan:\n" + plan);
        // The dump's hms row count is 0, so replay falls back to the numRows table property (620031330):
        // the scan cardinality must reflect that real row count, not the empty-table default.
        Assertions.assertTrue(plan.contains("cardinality: 620031330"),
                "expected the scan cardinality recovered from the numRows property, plan:\n" + plan);
    }

    @Test
    public void testReplayHiveCatalogPredicate() throws Exception {
        // select lo_orderkey, count(*) ... where lo_orderkey > 1000 group by lo_orderkey over hive.
        String plan = replay("query_dump/hive_catalog_predicate");
        Assertions.assertTrue(plan.contains("HdfsScanNode"), "expected a hive scan, plan:\n" + plan);
        // The WHERE predicate must survive onto the synthesized hive scan (hive renders it as a
        // NON-PARTITION PREDICATES line, which still contains this substring).
        Assertions.assertTrue(plan.contains("lo_orderkey > 1000"),
                "expected the WHERE predicate pushed onto the hive scan, plan:\n" + plan);
    }

    // Legacy-format partitioned external tables: captured from a partitioned iceberg/hive table with a
    // partition-column predicate, but from an old dump that lacks the external_table_catalog and partition-name
    // sections. Replay recovers the catalog from the catalog.db.table references in the SQL (backward
    // compatibility) but cannot reproduce pruning without captured partition names, so this asserts only that
    // the external catalog resolves and the partition-column predicate is pushed onto the scan. New-format
    // dumps (which do reproduce pruning) are covered by their own dedicated tests.
    private void assertPartitionReplay(String dumpResource, boolean expectCatalogSection,
                                       String scanNode) throws Exception {
        String dumpString = getDumpInfoFromFile(dumpResource);
        boolean hasSection = !getDumpInfoFromJson(dumpString).getExternalTableCatalogMap().isEmpty();
        Assertions.assertEquals(expectCatalogSection, hasSection,
                "external_table_catalog section presence mismatch for " + dumpResource);
        String plan = getCostPlanFragment(dumpString).second;
        Assertions.assertNotNull(plan, "replay produced no plan for " + dumpResource);
        Assertions.assertTrue(plan.contains(scanNode), "expected " + scanNode + ", plan:\n" + plan);
        Assertions.assertTrue(plan.contains("lo_orderdate >= 19970101"),
                "expected the partition-column predicate pushed onto the scan, plan:\n" + plan);
    }

    @Test
    public void testReplayIcebergCatalogPartitionNewFormat() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/iceberg_catalog_partition");
        Assertions.assertFalse(getDumpInfoFromJson(dumpString).getExternalTableCatalogMap().isEmpty(),
                "new-format dump should carry the external_table_catalog section");
        String plan = getCostPlanFragment(dumpString).second;
        Assertions.assertNotNull(plan);
        Assertions.assertTrue(plan.contains("IcebergScanNode"), "expected an iceberg scan, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("lo_orderdate >= 19970101"),
                "expected the partition predicate pushed onto the scan, plan:\n" + plan);
        // The dump captured the partition spec (identity lo_orderdate) plus all 2406 partition names, so replay
        // rebuilds the native iceberg table with one DataFile per partition and reproduces the cluster's
        // partition pruning (579/2406) via native planFiles, instead of the un-pruned scan it produced before.
        Assertions.assertTrue(plan.contains("partitions=579/2406"),
                "expected iceberg partition pruning reproduced (579/2406), plan:\n" + plan);
    }

    @Test
    public void testReplayIcebergCatalogPartitionLegacyFormat() throws Exception {
        assertPartitionReplay("query_dump/iceberg_catalog_partition_legacy", false, "IcebergScanNode");
    }

    // A small, deterministic single-column-partitioned iceberg table (iceberg_test_rd.qd_test.part_skew): 3
    // identity partitions on dt with DELIBERATELY uneven row counts (dt=20240101:100, 20240102:200,
    // 20240103:700), ANALYZEd so real column statistics are captured. The dump was taken for
    //   select k, dt from ... where dt >= 20240102
    // Where the lineorder test above is a coarse 579/2406 pruning check, this pins down the axes it cannot:
    // the real per-partition counts (captured into the existing table_row_count section, NOT a
    // total/partitionCount even split) and the non-UNKNOWN ANALYZEd column statistics reaching the planner.
    @Test
    public void testReplayIcebergCatalogPartitionSkew() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/iceberg_catalog_partition_skew");
        QueryDumpInfo dump = getDumpInfoFromJson(dumpString);

        // (1) Real total row count, not the "1" fallback of an un-analyzed dump.
        Assertions.assertEquals(1000L,
                dump.getExternalTableRowCountMap().getOrDefault("qd_test.part_skew", 0L).longValue(),
                "expected the real total row count captured after ANALYZE");

        // (2) Per-partition counts are the REAL uneven counts, NOT an even total/partitionCount split -- the
        // exact defect this change fixes. The three counts differ (100/200/700), so an even split would fail.
        Map<String, Long> perPartition = dump.getPartitionRowCountMap().get("qd_test.part_skew");
        Assertions.assertNotNull(perPartition, "expected per-partition row counts in the dump");
        Assertions.assertEquals(100L, perPartition.get("dt=20240101").longValue(), "dt=20240101 count");
        Assertions.assertEquals(200L, perPartition.get("dt=20240102").longValue(), "dt=20240102 count");
        Assertions.assertEquals(700L, perPartition.get("dt=20240103").longValue(), "dt=20240103 count");

        String plan = getCostPlanFragment(dumpString).second;
        Assertions.assertNotNull(plan);
        Assertions.assertTrue(plan.contains("IcebergScanNode"), "expected an iceberg scan, plan:\n" + plan);
        // (3) Partition pruning: 2 of the 3 partitions survive dt >= 20240102 (native planFiles on the rebuilt
        // identity spec), unaffected by the per-partition record counts.
        Assertions.assertTrue(plan.contains("partitions=2/3"),
                "expected partition pruning 2/3, plan:\n" + plan);
        // (4) The ANALYZEd column statistics round-trip to the planner UNCLAMPED -- the heart of the fix. Both
        // columns show their real captured statistic (NDV 3, full ranges), not the [-inf,inf]/UNKNOWN the dump
        // fed before the row count + stats were captured.
        Assertions.assertTrue(plan.contains("dt-->[2.0240101E7, 2.0240103E7, 0.0, 4.0, 3.0]"),
                "expected the captured dt statistic on the scan, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("k-->[1.0, 3.0, 0.0, 4.0, 3.0]"),
                "expected the captured k statistic on the scan, plan:\n" + plan);
        // (5) The scan cardinality is the real captured total (1000), not the old "1" fallback that clamped
        // everything. NOTE: the cluster itself estimates 500 here, by applying the dt range-predicate
        // selectivity (0.5) on top of the total. Replay serves external stats through the connector path (as
        // all external-table replay does -- FeConstants.runningUnitTest disables the internal-statistics
        // path), so StatisticsCalculator.removePartitionPredicate drops the partition-column predicate before
        // selectivity and the scan keeps the full total. That divergence is a pre-existing property of
        // external-table replay, independent of this change; what this test locks down is that the real total
        // (1000), not 1, now reaches the planner.
        Assertions.assertTrue(plan.contains("cardinality=1000"),
                "expected the captured total row count (1000) as the scan cardinality, plan:\n" + plan);
    }

    // A TWO-column-partitioned iceberg table (iceberg_test_rd.qd_test.part_multi): identity partitions on
    // (dt, city) -> 5 concrete partitions with uneven counts, ANALYZEd. Dump taken for
    //   select k, dt, city from ... where dt >= 20240102
    // Exercises a multi-column partition spec end to end: replay rebuilds the 2-field PartitionSpec, appends a
    // DataFile per "dt=.../city=..." partition, and native planFiles prunes on the leading column -> 3 of 5
    // partitions survive.
    @Test
    public void testReplayIcebergCatalogPartitionMultiColumn() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/iceberg_catalog_partition_multi");
        QueryDumpInfo dump = getDumpInfoFromJson(dumpString);

        Assertions.assertEquals(1000L,
                dump.getExternalTableRowCountMap().getOrDefault("qd_test.part_multi", 0L).longValue(),
                "expected the real total row count");
        // Real per-partition counts, keyed by the composite "dt=.../city=..." partition name.
        Map<String, Long> perPartition = dump.getPartitionRowCountMap().get("qd_test.part_multi");
        Assertions.assertNotNull(perPartition, "expected per-partition row counts in the dump");
        Assertions.assertEquals(5, perPartition.size(), "expected 5 composite partitions");
        Assertions.assertEquals(100L, perPartition.get("dt=20240101/city=bj").longValue());
        Assertions.assertEquals(250L, perPartition.get("dt=20240102/city=sh").longValue());
        Assertions.assertEquals(300L, perPartition.get("dt=20240103/city=bj").longValue());

        String plan = getCostPlanFragment(dumpString).second;
        Assertions.assertNotNull(plan);
        Assertions.assertTrue(plan.contains("IcebergScanNode"), "expected an iceberg scan, plan:\n" + plan);
        // 3 of 5 partitions survive dt >= 20240102: (20240102,bj),(20240102,sh),(20240103,bj).
        Assertions.assertTrue(plan.contains("partitions=3/5"),
                "expected multi-column partition pruning 3/5, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("dt-->[2.0240101E7, 2.0240103E7, 0.0, 4.0, 3.0]"),
                "expected the captured dt statistic on the scan, plan:\n" + plan);
        // Cardinality is the full captured total (see the single-column test for why the partition-column
        // predicate is not selectivized in the replay env).
        Assertions.assertTrue(plan.contains("cardinality=1000"),
                "expected the captured total row count (1000) as the scan cardinality, plan:\n" + plan);
    }

    // A day()-TRANSFORM-partitioned iceberg table (iceberg_test_rd.qd_test.part_tf): partitioned by day(ts)
    // over 4 days with uneven counts (100/200/300/400), ANALYZEd. Dump taken for
    //   select k, ts from ... where ts >= '2024-01-03 00:00:00'
    // Unlike identity partitioning, a transform-partitioned iceberg table cannot be populated by a static
    // insert, only a dynamic one (partition value from the data) -- hence the day-bucketed row counts. Replay
    // rebuilds the real day() PartitionSpec from the captured "day(`ts`)" transform string, appends a DataFile
    // per "ts_day=YYYY-MM-DD" partition, and native planFiles prunes on the source-column predicate: 2 of the
    // 4 day partitions survive. This is the transform counterpart to the identity pruning tests above.
    @Test
    public void testReplayIcebergCatalogPartitionTransform() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/iceberg_catalog_partition_transform");
        QueryDumpInfo dump = getDumpInfoFromJson(dumpString);

        // The captured partition spec is the day() transform, not an identity column.
        Assertions.assertEquals(List.of("day(`ts`)"),
                dump.getExternalTablePartitionSpecMap().get("qd_test.part_tf"),
                "expected the day() transform spec captured");
        Assertions.assertEquals(1000L,
                dump.getExternalTableRowCountMap().getOrDefault("qd_test.part_tf", 0L).longValue());
        // Real per-partition counts keyed by the transform partition name "ts_day=YYYY-MM-DD".
        Map<String, Long> perPartition = dump.getPartitionRowCountMap().get("qd_test.part_tf");
        Assertions.assertNotNull(perPartition, "expected per-partition row counts in the dump");
        Assertions.assertEquals(100L, perPartition.get("ts_day=2024-01-01").longValue());
        Assertions.assertEquals(300L, perPartition.get("ts_day=2024-01-03").longValue());
        Assertions.assertEquals(400L, perPartition.get("ts_day=2024-01-04").longValue());

        String plan = getCostPlanFragment(dumpString).second;
        Assertions.assertNotNull(plan);
        Assertions.assertTrue(plan.contains("IcebergScanNode"), "expected an iceberg scan, plan:\n" + plan);
        // day() transform pruning: 2 of 4 day partitions survive ts >= 2024-01-03 (native planFiles on the
        // rebuilt day() spec projects the source-column predicate onto the ts_day partition).
        Assertions.assertTrue(plan.contains("partitions=2/4"),
                "expected day() transform partition pruning 2/4, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("k-->[1.0, 4.0, 0.0, 4.0, 4.0]"),
                "expected the captured k statistic on the scan, plan:\n" + plan);
        // Cardinality is the full captured total (see the single-column identity test for why the
        // partition-column predicate is not selectivized in the replay env).
        Assertions.assertTrue(plan.contains("cardinality=1000"),
                "expected the captured total row count (1000) as the scan cardinality, plan:\n" + plan);
    }

    @Test
    public void testReplayHiveCatalogPartitionNewFormat() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/hive_catalog_partition");
        Assertions.assertFalse(getDumpInfoFromJson(dumpString).getExternalTableCatalogMap().isEmpty(),
                "new-format dump should carry the external_table_catalog section");
        String plan = getCostPlanFragment(dumpString).second;
        Assertions.assertNotNull(plan);
        Assertions.assertTrue(plan.contains("HdfsScanNode"), "expected a hive scan, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("lo_orderdate >= 19970101"),
                "expected the partition predicate pushed onto the scan, plan:\n" + plan);
        // The dump captured all 2406 partition names, so replay reproduces the cluster's partition pruning
        // (579/2406) exactly, instead of collapsing to 0/0 as it did before partition names were captured.
        Assertions.assertTrue(plan.contains("partitions=579/2406"),
                "expected hive partition pruning reproduced (579/2406), plan:\n" + plan);
    }

    // A small, deterministic single-column-partitioned hive table (hive_test_rd.qd_test.hpart_skew): partition
    // column dt with uneven counts (100/200/700), ANALYZEd. Dump taken for
    //   select k, dt from ... where dt >= 20240102
    // The hive replay reproduces pruning (2/3) from the captured partition names via listPartitionNamesByValue
    // and feeds the ANALYZEd column stats. (Unlike iceberg, the hive scan is not covered by
    // removePartitionPredicate, so the dt range predicate is selectivized and the dt statistic shows the
    // pruned [20240102, 20240103] range.)
    @Test
    public void testReplayHiveCatalogPartitionSkew() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/hive_catalog_partition_skew");
        QueryDumpInfo dump = getDumpInfoFromJson(dumpString);
        Assertions.assertEquals("hive_test_rd",
                dump.getExternalTableCatalogMap().get("qd_test.hpart_skew"), "expected the hive catalog captured");
        Assertions.assertEquals(1000L,
                dump.getExternalTableRowCountMap().getOrDefault("qd_test.hpart_skew", 0L).longValue(),
                "expected the real total row count captured after ANALYZE");

        String plan = getCostPlanFragment(dumpString).second;
        Assertions.assertNotNull(plan);
        Assertions.assertTrue(plan.contains("HdfsScanNode"), "expected a hive scan, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("partitions=2/3"),
                "expected hive partition pruning 2/3, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("dt-->[2.0240102E7, 2.0240103E7, 0.0, 4.0, 3.0]"),
                "expected the captured dt statistic (selectivized to the pruned range), plan:\n" + plan);
        Assertions.assertTrue(plan.contains("k-->[1.0, 3.0, 0.0, 4.0, 3.0]"),
                "expected the captured k statistic on the scan, plan:\n" + plan);
    }

    // A TWO-column-partitioned hive table (hive_test_rd.qd_test.hpart_multi): partition columns (dt, city) ->
    // 5 concrete partitions. Dump taken for
    //   select k, dt, city from ... where dt >= 20240102
    // Exercises multi-column hive partition pruning (3 of 5 survive) plus the ANALYZEd stats on both a numeric
    // and a string partition column.
    @Test
    public void testReplayHiveCatalogPartitionMultiColumn() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/hive_catalog_partition_multi");
        QueryDumpInfo dump = getDumpInfoFromJson(dumpString);
        Assertions.assertEquals("hive_test_rd",
                dump.getExternalTableCatalogMap().get("qd_test.hpart_multi"), "expected the hive catalog captured");
        Assertions.assertEquals(1000L,
                dump.getExternalTableRowCountMap().getOrDefault("qd_test.hpart_multi", 0L).longValue(),
                "expected the real total row count");

        String plan = getCostPlanFragment(dumpString).second;
        Assertions.assertNotNull(plan);
        Assertions.assertTrue(plan.contains("HdfsScanNode"), "expected a hive scan, plan:\n" + plan);
        // 3 of 5 (dt, city) partitions survive dt >= 20240102.
        Assertions.assertTrue(plan.contains("partitions=3/5"),
                "expected multi-column hive partition pruning 3/5, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("dt-->[2.0240102E7, 2.0240103E7, 0.0, 4.0, 3.0]"),
                "expected the captured dt statistic, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("city-->[-Infinity, Infinity, 0.0, 2.0, 2.0]"),
                "expected the captured string city statistic (NDV 2), plan:\n" + plan);
    }

    @Test
    public void testReplayHiveCatalogPartitionLegacyFormat() throws Exception {
        assertPartitionReplay("query_dump/hive_catalog_partition_legacy", false, "HdfsScanNode");
    }

    // Column statistics: after ANALYZE on the cluster, the dump carries real column_statistics for the
    // external table, and replay must feed them back to the planner instead of the empty/UNKNOWN it served
    // before. (Iceberg is the pointed case: its stats were never even captured into the dump before, because
    // the iceberg scan-stats path bypassed the dump hook.)
    @Test
    public void testReplayIcebergCatalogColumnStatistics() throws Exception {
        String dumpString = getDumpInfoFromFile("query_dump/iceberg_catalog_stats");
        // The dump must carry the external table row count; iceberg has no hms scanRowCount to hold it, and
        // without it replay falls back to a tiny row count that clamps every NDV/cardinality.
        Assertions.assertTrue(getDumpInfoFromJson(dumpString).getExternalTableRowCountMap()
                        .getOrDefault("bench_starrocks.lineorder_100g", 0L) > 100_000_000L,
                "new-format dump should carry the external table row count");
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(dumpString);
        String plan = replayPair.second;
        // The captured column statistic must reach the planner UNCLAMPED: the iceberg scan shows the real
        // NDV (~3.8M) and a cardinality driven by the captured row count (~6E8), not the fallback default.
        Assertions.assertTrue(plan.contains("lo_orderkey-->[1000.0, 5.99999776E8, 0.0, 4.0, 3869052.0]"),
                "expected the captured lo_orderkey statistic on the iceberg scan, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("cardinality=600036939"),
                "expected the iceberg scan cardinality from the captured row count, plan:\n" + plan);
    }

    @Test
    public void testReplayHiveCatalogColumnStatistics() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/hive_catalog_stats"));
        ColumnStatistic stat = replayPair.first.getTableStatisticsMap()
                .get("hive_ssb100g_partition_orc.supplier").get("s_region");
        Assertions.assertNotNull(stat, "s_region statistic should be present after replay");
        Assertions.assertFalse(stat.isUnknown(), "hive column stat must be fed from the dump, not UNKNOWN");
        // SSB has 5 regions; assert the captured NDV round-trips.
        Assertions.assertEquals(5.0, stat.getDistinctValuesCount(), 0.5, "expected 5 regions, got: " + stat);
    }

    @Test
    public void testMVBasedIceberg() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/mv_base_iceberg"));
        Assertions.assertTrue(replayPair.second.contains("OlapScanNode"));
        Assertions.assertTrue(replayPair.second.contains("table: mv_denorm_issue_cd_info_complete"));
    }
}
