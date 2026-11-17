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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Partition;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A partitioned MV joined with a non-ref base table, refreshed in batches of one partition
 * ({@code partition_refresh_number = 1}). A change of the non-ref table invalidates every MV partition, so the batches
 * of the refresh it triggers must together refresh all of them, and the MV is fresh only for the non-ref table
 * version every batch read.
 */
public class MVBatchRefreshNonRefTableTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE f_list (k1 varchar(10) not null, v1 int)\n"
                + "PARTITION BY LIST(k1) (\n"
                + "  PARTITION pa VALUES IN ('a'),\n"
                + "  PARTITION pb VALUES IN ('b'),\n"
                + "  PARTITION pc VALUES IN ('c'))\n"
                + "DISTRIBUTED BY HASH(v1) BUCKETS 1 PROPERTIES('replication_num' = '1');");
        starRocksAssert.withTable("CREATE TABLE f_range (k1 date, v1 int)\n"
                + "PARTITION BY RANGE(k1) (\n"
                + "  PARTITION p1 VALUES [('2024-01-01'), ('2024-01-02')),\n"
                + "  PARTITION p2 VALUES [('2024-01-02'), ('2024-01-03')),\n"
                + "  PARTITION p3 VALUES [('2024-01-03'), ('2024-01-04')))\n"
                + "DISTRIBUTED BY HASH(v1) BUCKETS 1 PROPERTIES('replication_num' = '1');");
        starRocksAssert.withTable("CREATE TABLE d_dim (v1 int, w int)\n"
                + "DISTRIBUTED BY HASH(v1) BUCKETS 1 PROPERTIES('replication_num' = '1');");
        executeInsertSql("INSERT INTO f_list VALUES ('a', 1), ('b', 2), ('c', 3);");
        executeInsertSql("INSERT INTO f_range VALUES ('2024-01-01', 1), ('2024-01-02', 2), ('2024-01-03', 3);");
        executeInsertSql("INSERT INTO d_dim VALUES (1, 10), (2, 20), (3, 30);");
    }

    private static final String LIST_MV = "CREATE MATERIALIZED VIEW %s\n"
            + "PARTITION BY k1\n"
            + "DISTRIBUTED BY RANDOM\n"
            + "REFRESH DEFERRED MANUAL\n"
            + "PROPERTIES ('partition_refresh_number' = '1')\n"
            + "AS SELECT f.k1, f.v1, d.w FROM f_list f JOIN d_dim d ON f.v1 = d.v1;";

    private static final String RANGE_MV = "CREATE MATERIALIZED VIEW %s\n"
            + "PARTITION BY k1\n"
            + "DISTRIBUTED BY RANDOM\n"
            + "REFRESH DEFERRED MANUAL\n"
            + "PROPERTIES ('partition_refresh_number' = '1')\n"
            + "AS SELECT f.k1, f.v1, d.w FROM f_range f JOIN d_dim d ON f.v1 = d.v1;";

    /** Run a refresh and every batch it spawns, the way the task manager would. */
    private void refreshAllBatches(MaterializedView mv, Runnable betweenFirstAndSecondBatch) throws Exception {
        TaskRun taskRun = withMVRefreshTaskRun(DB_NAME, mv);
        for (int batch = 1; batch < 10; batch++) {
            MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
            TaskRun next = processor.getNextTaskRun();
            if (next == null) {
                return;
            }
            if (batch == 1 && betweenFirstAndSecondBatch != null) {
                betweenFirstAndSecondBatch.run();
            }
            next.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
            next.executeTaskRun();
            taskRun = next;
        }
        Assertions.fail("the refresh did not finish in 10 batches");
    }

    private static Map<String, Long> visibleVersions(MaterializedView mv) {
        return mv.getPartitions().stream().collect(Collectors.toMap(Partition::getName,
                p -> p.getDefaultPhysicalPartition().getVisibleVersion()));
    }

    private static Set<String> notRefreshedSince(MaterializedView mv, Map<String, Long> before) {
        Map<String, Long> after = visibleVersions(mv);
        return after.keySet().stream()
                .filter(name -> after.get(name).equals(before.get(name)))
                .collect(Collectors.toSet());
    }

    private void assertNonRefChangeRefreshesEveryPartition(String createMv, String mvName) throws Exception {
        starRocksAssert.withMaterializedView(String.format(createMv, mvName));
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            refreshAllBatches(mv, null);
            Assertions.assertEquals(3, mv.getPartitions().size());
            Map<String, Long> before = visibleVersions(mv);

            executeInsertSql("INSERT INTO d_dim VALUES (1, 11), (2, 21), (3, 31);");
            refreshAllBatches(mv, null);
            Assertions.assertEquals(Set.of(), notRefreshedSince(mv, before),
                    "a change of the non-ref table left these mv partitions unrefreshed");
            // Every batch read the same non-ref state, so the last one records it and the mv is fresh.
            MvUpdateInfo updateInfo = getMvUpdateInfo(mv);
            Assertions.assertNotEquals(MvUpdateInfo.MvToRefreshType.FULL, updateInfo.getMVToRefreshType());
            Assertions.assertEquals(Set.of(), updateInfo.getMVToRefreshPCells().getPartitionNames());
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    /** F-1: a list-partitioned MV refreshes only the first batch's partitions after a non-ref table change. */
    @Test
    public void testListMVBatchesRefreshEveryPartitionOnNonRefChange() throws Exception {
        assertNonRefChangeRefreshesEveryPartition(LIST_MV, "mv_batch_list");
    }

    /** The range partitioner's isAutoRefresh branch (#37876), the behavior the list one should match. */
    @Test
    public void testRangeMVBatchesRefreshEveryPartitionOnNonRefChange() throws Exception {
        assertNonRefChangeRefreshesEveryPartition(RANGE_MV, "mv_batch_range");
    }

    private void assertNonRefChangeBetweenBatchesKeepsEarlyBatchesStale(String createMv, String mvName)
            throws Exception {
        starRocksAssert.withMaterializedView(String.format(createMv, mvName));
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            refreshAllBatches(mv, null);

            executeInsertSql("INSERT INTO d_dim VALUES (1, 12), (2, 22), (3, 32);");
            // The first batch read the non-ref table before this second change, the later ones after it.
            refreshAllBatches(mv, () -> {
                try {
                    executeInsertSql("INSERT INTO d_dim VALUES (1, 13), (2, 23), (3, 33);");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            // A non-ref change reports FULL with an empty partition set, so the type is what tells stale from fresh.
            MvUpdateInfo updateInfo = getMvUpdateInfo(mv);
            Assertions.assertTrue(updateInfo.getMVToRefreshType() == MvUpdateInfo.MvToRefreshType.FULL
                            || !updateInfo.getMVToRefreshPCells().isEmpty(),
                    "the partition the first batch refreshed lacks the second change, but the mv counts as fresh: "
                            + updateInfo.getMVToRefreshType());
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    /** F-2: the non-ref table version recorded at the last batch also covers the partitions of earlier ones. */
    @Test
    public void testListMVNonRefChangeBetweenBatchesKeepsEarlyBatchesStale() throws Exception {
        assertNonRefChangeBetweenBatchesKeepsEarlyBatchesStale(LIST_MV, "mv_batch_list_between");
    }

    @Test
    public void testRangeMVNonRefChangeBetweenBatchesKeepsEarlyBatchesStale() throws Exception {
        assertNonRefChangeBetweenBatchesKeepsEarlyBatchesStale(RANGE_MV, "mv_batch_range_between");
    }

    /** Two pending batches that read different non-ref states must not merge: the survivor's fingerprint would win. */
    @Test
    public void testBatchesWithDifferentNonRefFingerprintsDoNotMerge() {
        Map<String, String> base = Map.of(TaskRun.MV_ID, "1", TaskRun.PARTITION_VALUES, "[[\"a\"]]");
        Map<String, String> first = new HashMap<>(base);
        first.put(TaskRun.MV_NON_REF_TABLE_FINGERPRINT, "10:aaaa");
        Map<String, String> other = new HashMap<>(base);
        other.put(TaskRun.MV_NON_REF_TABLE_FINGERPRINT, "10:bbbb");
        int priority = Constants.TaskRunPriority.HIGHER.value();
        ExecuteOption firstOption = new ExecuteOption(priority, true, first);
        Assertions.assertFalse(firstOption.isMergeableWith(new ExecuteOption(priority, true, other)));
        Assertions.assertFalse(firstOption.isMergeableWith(new ExecuteOption(priority, true, new HashMap<>(base))));
        Assertions.assertTrue(firstOption.isMergeableWith(new ExecuteOption(priority, true, new HashMap<>(first))));
    }
}
