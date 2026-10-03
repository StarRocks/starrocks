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
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
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
}
