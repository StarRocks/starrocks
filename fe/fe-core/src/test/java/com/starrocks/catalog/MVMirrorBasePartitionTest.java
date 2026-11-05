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

package com.starrocks.catalog;

import com.google.common.collect.Sets;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mv.pct.BaseToMVPartitionMapping;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PartitionDiffResult;
import com.starrocks.sql.common.RangePartitionDiffer;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

/**
 * End-to-end tests for the mv "mirror base partition" mapper ({@code enable_mv_mirror_base_partition})
 * covering the multi-base-table path that the mapper-level unit tests don't exercise:
 * <ul>
 *   <li>multiple base tables merged by {@code mergeRBTPartitionKeyMap} keep their unified boundaries,</li>
 *   <li>{@code RangePartitionDiffer.generateBaseRefMap}/{@code generateMvRefMap} converge to 1:1,</li>
 *   <li>the refresh task's {@code mvPartitionsToRefresh}/{@code refBasePartitionsToRefreshMap}/
 *       {@code basePartitionsToRefreshMap} converge to one month partition per base table.</li>
 * </ul>
 */
public class MVMirrorBasePartitionTest extends MVTestBase {

    private static final String MAY_RANGE = "PARTITION p202105 values [('2021-05-01'),('2021-06-01'))";
    private static final String JUNE_RANGE = "PARTITION p202106 values [('2021-06-01'),('2021-07-01'))";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        // three base tables that share the exact same (merged) month boundary
        for (String t : new String[] {"mirror_a1", "mirror_a2", "mirror_a3"}) {
            starRocksAssert.withTable(createTableSql(t, MAY_RANGE));
        }
        // base tables with non-overlapping boundaries: May + June
        starRocksAssert.withTable(createTableSql("mirror_b1", MAY_RANGE));
        starRocksAssert.withTable(createTableSql("mirror_b2", JUNE_RANGE));
        starRocksAssert.withTable(createTableSql("mirror_b3", MAY_RANGE));
    }

    private static String createTableSql(String name, String... partitions) {
        return "CREATE TABLE test." + name + "\n" +
                "(k1 date, k2 int, v1 int)\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(" + String.join(", ", partitions) + ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num'='1');";
    }

    private static String createMvSql(String mvName, String t1, String t2, String t3) {
        return "create materialized view test." + mvName + "\n" +
                "partition by date_trunc('day', k1)\n" +
                "distributed by hash(k2) buckets 3\n" +
                "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                "refresh deferred manual\n" +
                "as select a.k1, a.k2, a.v1 + b.v1 + c.v1 as v1\n" +
                "from test." + t1 + " a\n" +
                "join test." + t2 + " b on a.k1 = b.k1\n" +
                "join test." + t3 + " c on a.k1 = c.k1;";
    }

    @Test
    public void testMirrorThreeBaseTablesConvergeToOneMonthPartition() {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = true;
        try {
            starRocksAssert.withMaterializedView(
                    createMvSql("mv_mirror_aligned", "mirror_a1", "mirror_a2", "mirror_a3"),
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);

                        // 1) merge + diff: 3 base tables with the same month boundary -> one month mv partition
                        RangePartitionDiffer differ = new RangePartitionDiffer(
                                mv, MVTimelinessArbiter.QueryRewriteParams.ofRefresh(), null);
                        PartitionDiffResult result = differ.computePartitionDiff(null);
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                result.diff.getAdds().getPartitionNames());
                        Assertions.assertTrue(result.diff.getDeletes().isEmpty());
                        Assertions.assertEquals(3, result.refBaseTablePartitionMap.size());
                        for (BaseToMVPartitionMapping mapping : result.refBaseTablePartitionMap.values()) {
                            Assertions.assertEquals(Sets.newHashSet("p202105"),
                                    mapping.cells().getPartitionNames());
                        }

                        // 2) generateBaseRefMap: each base table's month partition maps to exactly one mv partition
                        Map<Table, PCellSetMapping> baseRefMap = differ.generateBaseRefMap(
                                BaseToMVPartitionMapping.extractCells(result.refBaseTablePartitionMap),
                                result.diff.getAdds());
                        Assertions.assertEquals(3, baseRefMap.size());
                        for (Table baseTable : baseRefMap.keySet()) {
                            Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                    baseRefMap.get(baseTable).get("p202105").getPartitionNames());
                        }

                        // 3) generateMvRefMap: the mv month partition maps back to one base partition per table
                        Map<String, Map<Table, PCellSortedSet>> mvRefMap = differ.generateMvRefMap(
                                result.diff.getAdds(),
                                BaseToMVPartitionMapping.extractCells(result.refBaseTablePartitionMap));
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"), mvRefMap.keySet());
                        Map<Table, PCellSortedSet> mvToBase = mvRefMap.get("p20210501_20210601");
                        Assertions.assertEquals(3, mvToBase.size());
                        for (PCellSortedSet cells : mvToBase.values()) {
                            Assertions.assertEquals(Sets.newHashSet("p202105"), cells.getPartitionNames());
                        }

                        // 4) refresh task extra_message converges as well
                        // initial refresh to materialize the mv partitions
                        TaskRun initTaskRun = buildMVTaskRun(mv, DB_NAME);
                        initTaskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        initTaskRun.executeTaskRun();

                        // change one day inside the merged month partition: still only one mv partition to refresh
                        executeInsertSql(connectContext,
                                "insert into mirror_a1 partition(p202105) values('2021-05-06', 1, 1);");

                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        taskRun.executeTaskRun();
                        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
                        MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                extraMessage.getMvPartitionsToRefresh());
                        Assertions.assertEquals(
                                Sets.newHashSet("mirror_a1", "mirror_a2", "mirror_a3"),
                                extraMessage.getRefBasePartitionsToRefreshMap().keySet());
                        Assertions.assertEquals(
                                Sets.newHashSet("mirror_a1", "mirror_a2", "mirror_a3"),
                                extraMessage.getBasePartitionsToRefreshMap().keySet());
                        for (String expected : Sets.newHashSet("mirror_a1", "mirror_a2", "mirror_a3")) {
                            Assertions.assertEquals(Sets.newHashSet("p202105"),
                                    extraMessage.getRefBasePartitionsToRefreshMap().get(expected));
                            Assertions.assertEquals(Sets.newHashSet("p202105"),
                                    extraMessage.getBasePartitionsToRefreshMap().get(expected));
                        }
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    @Test
    public void testEagerSplitsAlignedMergedMonthIntoSingleDays() {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = false;
        try {
            starRocksAssert.withMaterializedView(
                    createMvSql("mv_eager_aligned", "mirror_a1", "mirror_a2", "mirror_a3"),
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);
                        RangePartitionDiffer differ = new RangePartitionDiffer(
                                mv, MVTimelinessArbiter.QueryRewriteParams.ofRefresh(), null);
                        PartitionDiffResult result = differ.computePartitionDiff(null);

                        // Even when all base tables' month partitions are aligned, eager still unrolls each
                        // merged month into one mv partition per day, and never coalesces them back.
                        Set<String> adds = result.diff.getAdds().getPartitionNames();
                        Assertions.assertEquals(31, adds.size(), "eager mv partitions: " + adds);
                        Assertions.assertTrue(adds.contains("p20210501_20210502"), "eager mv partitions: " + adds);
                        Assertions.assertTrue(adds.contains("p20210531_20210601"), "eager mv partitions: " + adds);
                        Assertions.assertFalse(adds.contains("p20210501_20210601"), "eager mv partitions: " + adds);
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    @Test
    public void testMirrorNonOverlappingBaseTablesAreNotMerged() {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = true;
        try {
            starRocksAssert.withMaterializedView(
                    createMvSql("mv_mirror_nonoverlap", "mirror_b1", "mirror_b2", "mirror_b3"),
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);
                        RangePartitionDiffer differ = new RangePartitionDiffer(
                                mv, MVTimelinessArbiter.QueryRewriteParams.ofRefresh(), null);
                        PartitionDiffResult result = differ.computePartitionDiff(null);

                        // boundaries of the ref base tables must NOT be merged into one wide partition
                        Assertions.assertEquals(
                                Sets.newHashSet("p20210501_20210601", "p20210601_20210701"),
                                result.diff.getAdds().getPartitionNames());

                        TaskRun initTaskRun = buildMVTaskRun(mv, DB_NAME);
                        initTaskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        initTaskRun.executeTaskRun();

                        executeInsertSql(connectContext,
                                "insert into mirror_b1 partition(p202105) values('2021-05-06', 1, 1);");
                        executeInsertSql(connectContext,
                                "insert into mirror_b2 partition(p202106) values('2021-06-06', 1, 1);");

                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        taskRun.executeTaskRun();
                        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
                        MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();

                        // exactly two mv partitions, one per non-overlapping month boundary
                        Assertions.assertEquals(
                                Sets.newHashSet("p20210501_20210601", "p20210601_20210701"),
                                extraMessage.getMvPartitionsToRefresh());

                        // the two changed base tables resolve to their own source partition, nothing extra
                        Map<String, Set<String>> refBaseMap = extraMessage.getRefBasePartitionsToRefreshMap();
                        Assertions.assertEquals(
                                Sets.newHashSet("mirror_b1", "mirror_b2", "mirror_b3"), refBaseMap.keySet());
                        Assertions.assertEquals(Sets.newHashSet("p202105"), refBaseMap.get("mirror_b1"));
                        Assertions.assertEquals(Sets.newHashSet("p202106"), refBaseMap.get("mirror_b2"));
                        Assertions.assertEquals(Sets.newHashSet("p202105"), refBaseMap.get("mirror_b3"));

                        Map<String, Set<String>> baseMap = extraMessage.getBasePartitionsToRefreshMap();
                        Assertions.assertEquals(
                                Sets.newHashSet("mirror_b1", "mirror_b2", "mirror_b3"), baseMap.keySet());
                        Assertions.assertEquals(Sets.newHashSet("p202105"), baseMap.get("mirror_b1"));
                        Assertions.assertEquals(Sets.newHashSet("p202106"), baseMap.get("mirror_b2"));
                        Assertions.assertEquals(Sets.newHashSet("p202105"), baseMap.get("mirror_b3"));
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * End-to-end regression for the bounded-refresh data loss: with the flag enabled for an MV that was
     * built while the flag was off (eager day partitions) and a merged base month, a refresh restricted to
     * a range outside that month must not drop the month's legacy day partitions.
     */
    @Test
    public void testBoundedRefreshKeepsOutOfRangeMirrorHistory() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = false;
        try {
            // base table: a merged May month partition plus recent June day partitions
            starRocksAssert.withTable(createTableSql("mirror_c1", MAY_RANGE,
                    "PARTITION p20210601 values [('2021-06-01'),('2021-06-02'))",
                    "PARTITION p20210602 values [('2021-06-02'),('2021-06-03'))"));
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_bounded\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_c1 group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);

                        // initial full refresh with mirror disabled -> eager keeps every day partition
                        refreshMV(DB_NAME, mv);
                        Set<String> before = mv.getRangePartitionMap().getPartitionNames();
                        Assertions.assertTrue(before.contains("p20210501_20210502"), "before: " + before);
                        Assertions.assertFalse(before.contains("p20210501_20210601"), "before: " + before);

                        // enable mirror and refresh only June: the out-of-range May history must survive
                        Config.enable_mv_mirror_base_partition = true;
                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.getProperties().put(TaskRun.PARTITION_START, "2021-06-01");
                        taskRun.getProperties().put(TaskRun.PARTITION_END, "2021-06-03");
                        taskRun.getProperties().put(TaskRun.FORCE, "true");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Constants.TaskRunState state = taskRun.executeTaskRun();
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, state,
                                "bounded refresh did not succeed");

                        // the refresh must have been scoped to the in-range June partitions only
                        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
                        MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                        Set<String> refreshed = extraMessage.getMvPartitionsToRefresh();
                        Assertions.assertTrue(refreshed.contains("p20210601_20210602"), "refreshed: " + refreshed);
                        Assertions.assertTrue(refreshed.contains("p20210602_20210603"), "refreshed: " + refreshed);
                        Assertions.assertFalse(refreshed.contains("p20210501_20210502"), "refreshed: " + refreshed);

                        Set<String> after = mv.getRangePartitionMap().getPartitionNames();
                        // May must still be day-partitioned history: not dropped, not coalesced into a month
                        Assertions.assertTrue(after.contains("p20210501_20210502"), "after: " + after);
                        Assertions.assertFalse(after.contains("p20210501_20210601"), "after: " + after);
                        // in-range June partitions are intact
                        Assertions.assertTrue(after.contains("p20210601_20210602"), "after: " + after);
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * Reverse migration with an unbounded full refresh: the coarse month is replaced by day partitions.
     * The dropped month must leave the published topology; otherwise potential-partition expansion adds
     * it back to the insert targets and the refresh fails with "Unknown partition".
     */
    @Test
    public void testFullRefreshAfterDisablingMirrorDropsCoarseFromTopology() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = true;
        try {
            starRocksAssert.withTable(createTableSql("mirror_c3", MAY_RANGE));
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_full_reverse\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_c3 group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);
                        refreshMV(DB_NAME, mv);
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                mv.getRangePartitionMap().getPartitionNames());

                        Config.enable_mv_mirror_base_partition = false;
                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.getProperties().put(TaskRun.FORCE, "true");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Constants.TaskRunState state = taskRun.executeTaskRun();
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, state,
                                "full refresh after disabling mirror did not succeed");

                        Set<String> after = mv.getRangePartitionMap().getPartitionNames();
                        Assertions.assertEquals(31, after.size(), "after: " + after);
                        Assertions.assertFalse(after.contains("p20210501_20210601"), "after: " + after);

                        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
                        MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                        Assertions.assertFalse(
                                extraMessage.getMvPartitionsToRefresh().contains("p20210501_20210601"),
                                "refreshed: " + extraMessage.getMvPartitionsToRefresh());
                        Assertions.assertFalse(
                                processor.getMvContext().getPartitionTopology().getMvToCellMap()
                                        .containsName("p20210501_20210601"),
                                "topology still contains the dropped coarse partition");
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * Reverse migration: an MV refreshed while the flag was on holds a coarse month partition.
     * After disabling the flag, a bounded refresh inside that month must not drop the month while
     * only creating the in-range day; the coarse partition and its history must survive.
     */
    @Test
    public void testBoundedRefreshKeepsCoarseHistoryAfterDisablingMirror() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = true;
        try {
            starRocksAssert.withTable(createTableSql("mirror_c2", MAY_RANGE));
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_reverse\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_c2 group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);

                        // initial full refresh with mirror enabled -> one coarse month partition
                        refreshMV(DB_NAME, mv);
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                mv.getRangePartitionMap().getPartitionNames());

                        // disable mirror and refresh only one day inside the month
                        Config.enable_mv_mirror_base_partition = false;
                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.getProperties().put(TaskRun.PARTITION_START, "2021-05-15");
                        taskRun.getProperties().put(TaskRun.PARTITION_END, "2021-05-16");
                        taskRun.getProperties().put(TaskRun.FORCE, "true");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Constants.TaskRunState state = taskRun.executeTaskRun();
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, state,
                                "bounded refresh did not succeed");

                        // the coarse month cannot be replaced atomically inside a one-day range, so it
                        // must be kept and refreshed as a whole instead of being dropped
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                mv.getRangePartitionMap().getPartitionNames());
                        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
                        MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                extraMessage.getMvPartitionsToRefresh());
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * With auto_refresh_partitions_limit=1, a complete refresh only keeps the newest partition.
     * Enabling mirror on a populated daily MV must migrate the in-window month but leave the
     * out-of-window month's day partitions (and their history) untouched.
     */
    @Test
    public void testAutoRefreshLimitKeepsMirrorHistory() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = false;
        try {
            starRocksAssert.withTable(createTableSql("mirror_limit1", MAY_RANGE, JUNE_RANGE));
            executeInsertSql(connectContext,
                    "insert into mirror_limit1 partition(p202105) values('2021-05-06', 1, 1);");
            executeInsertSql(connectContext,
                    "insert into mirror_limit1 partition(p202106) values('2021-06-06', 1, 2);");
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_limit\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_limit1 group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);
                        refreshMV(DB_NAME, mv);
                        Set<String> before = mv.getRangePartitionMap().getPartitionNames();
                        Assertions.assertTrue(before.contains("p20210501_20210502"), "before: " + before);
                        Assertions.assertFalse(before.contains("p20210501_20210601"), "before: " + before);

                        Config.enable_mv_mirror_base_partition = true;
                        mv.getTableProperty().setAutoRefreshPartitionsLimit(1);
                        // make the June base partition newer so this run has a change to process
                        executeInsertSql(connectContext,
                                "insert into mirror_limit1 partition(p202106) values('2021-06-15', 2, 3);");

                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Constants.TaskRunState state = taskRun.executeTaskRun();
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, state,
                                "complete refresh did not succeed");

                        Set<String> after = mv.getRangePartitionMap().getPartitionNames();
                        // May is outside the newest-1 window: its day partitions must survive
                        Assertions.assertTrue(after.contains("p20210501_20210502"), "after: " + after);
                        Assertions.assertTrue(after.contains("p20210531_20210601"), "after: " + after);
                        Assertions.assertFalse(after.contains("p20210501_20210601"), "after: " + after);
                        // June is inside the window: it is migrated to the mirror month partition
                        Assertions.assertTrue(after.contains("p20210601_20210701"), "after: " + after);

                        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
                        MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                        Assertions.assertTrue(
                                extraMessage.getMvPartitionsToRefresh().contains("p20210601_20210701"),
                                "refreshed: " + extraMessage.getMvPartitionsToRefresh());
                        Assertions.assertFalse(
                                extraMessage.getMvPartitionsToRefresh().contains("p20210501_20210502"),
                                "refreshed: " + extraMessage.getMvPartitionsToRefresh());
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * Only toggling the mirror flag (no base data change) still creates a replacement partition.
     * That partition must be materialized instead of being left empty because change detection
     * cannot see it.
     */
    @Test
    public void testMirrorToggleWithoutBaseChangeMaterializesReplacement() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = false;
        try {
            starRocksAssert.withTable(createTableSql("mirror_toggle1", MAY_RANGE));
            executeInsertSql(connectContext,
                    "insert into mirror_toggle1 partition(p202105) values('2021-05-06', 1, 1);");
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_toggle\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_toggle1 group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);
                        refreshMV(DB_NAME, mv);
                        Assertions.assertTrue(mv.getRangePartitionMap().getPartitionNames()
                                .contains("p20210501_20210502"));

                        Config.enable_mv_mirror_base_partition = true;
                        // No base table change at all: only the partition mapping changes.
                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Constants.TaskRunState state = taskRun.executeTaskRun();
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, state,
                                "refresh skipped even though the shape change needed materialization");

                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                mv.getRangePartitionMap().getPartitionNames());
                        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
                        MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                        Assertions.assertTrue(
                                extraMessage.getMvPartitionsToRefresh().contains("p20210501_20210601"),
                                "refreshed: " + extraMessage.getMvPartitionsToRefresh());
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * Reverse migration with a small limit: the month needs 31 day replacements, which cannot fit
     * in the newest-1 window, so the coarse month must be kept instead of being partially split.
     */
    @Test
    public void testAutoRefreshLimitKeepsCoarseHistoryAfterDisablingMirror() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = true;
        try {
            starRocksAssert.withTable(createTableSql("mirror_reverse_limit1", MAY_RANGE));
            executeInsertSql(connectContext,
                    "insert into mirror_reverse_limit1 partition(p202105) values('2021-05-06', 1, 1);");
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_reverse_limit\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_reverse_limit1 group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);
                        refreshMV(DB_NAME, mv);
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                mv.getRangePartitionMap().getPartitionNames());

                        Config.enable_mv_mirror_base_partition = false;
                        mv.getTableProperty().setAutoRefreshPartitionsLimit(1);
                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.getProperties().put(TaskRun.FORCE, "true");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Constants.TaskRunState state = taskRun.executeTaskRun();
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, state,
                                "complete refresh did not succeed");

                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                mv.getRangePartitionMap().getPartitionNames(),
                                "coarse month must be kept when the day replacements exceed the limit");
                        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
                        MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                        Assertions.assertTrue(
                                extraMessage.getMvPartitionsToRefresh().contains("p20210501_20210601"),
                                "refreshed: " + extraMessage.getMvPartitionsToRefresh());
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * When the auto refresh limit allows the whole coarse-to-fine component, the day replacements
     * must still be refreshed together even though partition_refresh_number defaults to 1. Deferring
     * 30 of them would leave empty partitions after their month was already dropped.
     */
    @Test
    public void testAutoRefreshLimitKeepsCoarseHistoryWithPartitionRefreshNumberOne() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = true;
        try {
            starRocksAssert.withTable(createTableSql("mirror_reverse_batch1", MAY_RANGE));
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_reverse_batch\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_reverse_batch1 group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);
                        refreshMV(DB_NAME, mv);
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                mv.getRangePartitionMap().getPartitionNames());

                        Config.enable_mv_mirror_base_partition = false;
                        mv.getTableProperty().setAutoRefreshPartitionsLimit(31);
                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.getProperties().put(TaskRun.FORCE, "true");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Constants.TaskRunState state = taskRun.executeTaskRun();
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, state,
                                "complete refresh did not succeed");

                        Set<String> after = mv.getRangePartitionMap().getPartitionNames();
                        Assertions.assertEquals(31, after.size(), "after: " + after);
                        Assertions.assertFalse(after.contains("p20210501_20210601"), "after: " + after);

                        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
                        // The task-run extra message is truncated for display
                        // (Config.max_mv_task_run_meta_message_values_length), so assert on the
                        // internal refresh scope instead.
                        Assertions.assertEquals(31, processor.getMvContext().getRefreshScope()
                                        .getMvPartitionsToRefresh().size(),
                                "all replacement day partitions must be refreshed together");
                        Assertions.assertNull(processor.getMvContext().getNextPartitionStart(),
                                "replacement partitions must not be deferred to a next batch");
                        Assertions.assertNull(processor.getMvContext().getNextPartitionEnd(),
                                "replacement partitions must not be deferred to a next batch");
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * EXPLAIN REFRESH synchronizes partitions against the live MV, so it must not apply a shape change:
     * dropping the populated coarse month for empty day partitions without running the insert that fills
     * them would leave MV reads without their materialized history. A following real refresh still applies
     * the replacement.
     */
    @Test
    public void testExplainRefreshDoesNotApplyShapeChange() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = true;
        try {
            starRocksAssert.withTable(createTableSql("mirror_explain1", MAY_RANGE));
            executeInsertSql(connectContext,
                    "insert into mirror_explain1 partition(p202105) values('2021-05-06', 1, 1);");
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_explain\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_explain1 group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);
                        refreshMV(DB_NAME, mv);
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                mv.getRangePartitionMap().getPartitionNames());

                        // Disabling mirror makes the next refresh replace the coarse month with day partitions.
                        Config.enable_mv_mirror_base_partition = false;
                        TaskRun explainRun = buildMVTaskRun(mv, DB_NAME);
                        ExecuteOption executeOption = new ExecuteOption(explainRun.getTask());
                        Map<String, String> explainProps = executeOption.getTaskRunProperties();
                        explainProps.put(TaskRun.FORCE, "true");
                        explainProps.put(TaskRun.PARTITION_START, "2021-05-01");
                        explainProps.put(TaskRun.PARTITION_END, "2021-06-01");
                        String plan = explainMVRefreshExecPlan(mv, executeOption,
                                "explain refresh materialized view test.mv_mirror_explain " +
                                        "partition start (\"2021-05-01\") end (\"2021-06-01\") force;");
                        Assertions.assertNotNull(plan);
                        Assertions.assertEquals(Sets.newHashSet("p20210501_20210601"),
                                mv.getRangePartitionMap().getPartitionNames(),
                                "EXPLAIN reshaped the partitions: "
                                        + mv.getRangePartitionMap().getPartitionNames());

                        // The same range executed for real still applies the replacement.
                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.getProperties().put(TaskRun.FORCE, "true");
                        taskRun.getProperties().put(TaskRun.PARTITION_START, "2021-05-01");
                        taskRun.getProperties().put(TaskRun.PARTITION_END, "2021-06-01");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, taskRun.executeTaskRun(),
                                "bounded refresh of the whole component did not succeed");
                        Set<String> after = mv.getRangePartitionMap().getPartitionNames();
                        Assertions.assertEquals(31, after.size(), "after: " + after);
                        Assertions.assertFalse(after.contains("p20210501_20210601"), "after: " + after);
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * A bounded refresh whose range only partially covers a pending shape change cannot be satisfied
     * atomically. The retained day partitions do not cover the requested date either, so the refresh must
     * report the conflict instead of claiming the requested range is already up to date.
     */
    @Test
    public void testBoundedRefreshInsideDeferredShapeChangeIsReported() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = false;
        try {
            starRocksAssert.withTable(createTableSql("mirror_deferred1", MAY_RANGE));
            executeInsertSql(connectContext,
                    "insert into mirror_deferred1 partition(p202105) values('2021-05-06', 1, 1);");
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_deferred\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_deferred1 group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);

                        // a bounded eager refresh only materializes the requested days
                        TaskRun eagerRun = buildMVTaskRun(mv, DB_NAME);
                        eagerRun.getProperties().put(TaskRun.FORCE, "true");
                        eagerRun.getProperties().put(TaskRun.PARTITION_START, "2021-05-01");
                        eagerRun.getProperties().put(TaskRun.PARTITION_END, "2021-05-03");
                        eagerRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, eagerRun.executeTaskRun(),
                                "bounded eager refresh did not succeed");
                        Set<String> eagerDays = Sets.newHashSet(
                                "p20210501_20210502", "p20210502_20210503");
                        Assertions.assertEquals(eagerDays, mv.getRangePartitionMap().getPartitionNames());

                        // Enabling mirror maps the whole base month to one coarse partition, so the pending shape
                        // change spans the month while the retained days only cover May 1-2.
                        Config.enable_mv_mirror_base_partition = true;

                        TaskRun explainRun = buildMVTaskRun(mv, DB_NAME);
                        ExecuteOption executeOption = new ExecuteOption(explainRun.getTask());
                        Map<String, String> explainProps = executeOption.getTaskRunProperties();
                        explainProps.put(TaskRun.FORCE, "true");
                        explainProps.put(TaskRun.PARTITION_START, "2021-05-15");
                        explainProps.put(TaskRun.PARTITION_END, "2021-05-16");
                        String plan = explainMVRefreshExecPlan(mv, executeOption,
                                "explain refresh materialized view test.mv_mirror_deferred " +
                                        "partition start (\"2021-05-15\") end (\"2021-05-16\") force;");
                        Assertions.assertTrue(
                                plan.contains("only partially covers a pending partition shape change"), plan);
                        Assertions.assertEquals(eagerDays, mv.getRangePartitionMap().getPartitionNames(),
                                "a rejected range must not reshape partitions");

                        // The real refresh rejects the request instead of reporting a successful no-op.
                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.getProperties().put(TaskRun.FORCE, "true");
                        taskRun.getProperties().put(TaskRun.PARTITION_START, "2021-05-15");
                        taskRun.getProperties().put(TaskRun.PARTITION_END, "2021-05-16");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        DmlException rejected = Assertions.assertThrows(DmlException.class,
                                taskRun::executeTaskRun,
                                "an unsatisfiable range must not end as a successful no-op");
                        Assertions.assertTrue(rejected.getMessage().contains(
                                        "only partially covers the pending partition shape change"),
                                rejected.getMessage());
                        Assertions.assertEquals(eagerDays, mv.getRangePartitionMap().getPartitionNames(),
                                "a rejected range must not reshape partitions");
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }
}
