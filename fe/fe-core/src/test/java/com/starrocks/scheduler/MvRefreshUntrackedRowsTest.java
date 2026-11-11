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

package com.starrocks.scheduler;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;
import java.util.Set;

/**
 * A base partition emptied by TRUNCATE sits at its initial version under a new id. While the version map tracks the old
 * id the change is seen; once the map is reset (an inactivation, a refresh-scheme change, a recreated base table), the
 * partition reads as new and empty, and the mv partition still holding its rows has to be found from the mv side.
 */
@TestMethodOrder(MethodName.class)
public class MvRefreshUntrackedRowsTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    @Test
    public void testPartitionEmptiedBeforeVersionMapResetIsRecomputed() throws Exception {
        createDailyTable("tbl_emptied");
        starRocksAssert.withMaterializedView(dailyMvSql("mv_emptied", "tbl_emptied", ""));
        try {
            MaterializedView mv = getMv("mv_emptied");
            TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
            initAndExecuteTaskRun(taskRun);
            long versionBefore = visibleVersion(mv, "p20220103");

            truncatePartition("tbl_emptied", "p20220103");
            mv.resetVisibleVersionMap();
            Assertions.assertTrue(getPartitionNamesToRefreshForMv(mv).contains("p20220103"));

            initAndExecuteTaskRun(taskRun);
            Assertions.assertEquals(Set.of("p20220101", "p20220102", "p20220103"), refreshedMvPartitions(taskRun));
            Assertions.assertTrue(visibleVersion(mv, "p20220103") > versionBefore);
            assertSettled(mv, taskRun);
        } finally {
            starRocksAssert.dropMaterializedView("mv_emptied");
            starRocksAssert.dropTable("tbl_emptied");
        }
    }

    @Test
    public void testNewEmptyPartitionIsNotRefreshedAfterVersionMapReset() throws Exception {
        createDailyTable("tbl_new_empty");
        starRocksAssert.withMaterializedView(dailyMvSql("mv_new_empty", "tbl_new_empty", ""));
        try {
            MaterializedView mv = getMv("mv_new_empty");
            TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
            initAndExecuteTaskRun(taskRun);

            starRocksAssert.alterTable("ALTER TABLE tbl_new_empty ADD PARTITION p20220104 " +
                    "VALUES [('2022-01-04'),('2022-01-05'))");
            mv.resetVisibleVersionMap();

            initAndExecuteTaskRun(taskRun);
            Assertions.assertEquals(Set.of("p20220101", "p20220102", "p20220103"), refreshedMvPartitions(taskRun));
            Assertions.assertEquals(Partition.PARTITION_INIT_VERSION, visibleVersion(mv, "p20220104"));
            assertSettled(mv, taskRun);
        } finally {
            starRocksAssert.dropMaterializedView("mv_new_empty");
            starRocksAssert.dropTable("tbl_new_empty");
        }
    }

    /**
     * The first batch puts its partitions back into the version map, so a later batch must not rely on the map being
     * empty to find the emptied partitions left to it.
     */
    @Test
    public void testBatchedRefreshRecomputesEmptiedPartitionsOfLaterBatches() throws Exception {
        createDailyTable("tbl_batched");
        starRocksAssert.withMaterializedView(dailyMvSql("mv_batched", "tbl_batched",
                ", 'partition_refresh_number' = '1', 'partition_refresh_strategy' = 'strict'"));
        try {
            MaterializedView mv = getMv("mv_batched");
            Task task = TaskBuilder.buildMvTask(mv, DB_NAME);
            task.getProperties().put(TaskRun.IS_TEST, "true");
            executeAllBatches(TaskRunBuilder.newBuilder(task).build());
            long firstVersionBefore = visibleVersion(mv, "p20220101");
            long lastVersionBefore = visibleVersion(mv, "p20220103");

            truncatePartition("tbl_batched", "p20220101");
            truncatePartition("tbl_batched", "p20220103");
            mv.resetVisibleVersionMap();

            TaskRun firstBatch = TaskRunBuilder.newBuilder(task).build();
            initAndExecuteTaskRun(firstBatch);
            TaskRun nextBatch = getPartitionBasedRefreshProcessor(firstBatch).getNextTaskRun();
            Assertions.assertNotNull(nextBatch);
            executeAllBatches(nextBatch);

            Assertions.assertTrue(visibleVersion(mv, "p20220101") > firstVersionBefore);
            Assertions.assertTrue(visibleVersion(mv, "p20220103") > lastVersionBefore);
            assertSettled(mv, TaskRunBuilder.newBuilder(task).build());
        } finally {
            starRocksAssert.dropMaterializedView("mv_batched");
            starRocksAssert.dropTable("tbl_batched");
        }
    }

    /**
     * Only the ref base table maps rows to mv partitions. The non-ref table is emptied too, so it cannot force a full
     * refresh and hide whether the emptied ref partition is found.
     */
    @Test
    public void testEmptiedRefPartitionRecomputedWithEmptiedNonRefTable() throws Exception {
        createDailyTable("tbl_fact");
        createSingleTable("tbl_dim");
        starRocksAssert.withMaterializedView("create materialized view test.mv_join\n" +
                "partition by k1\n" +
                "distributed by hash(k1) buckets 3\n" +
                "refresh deferred manual\n" +
                "properties('replication_num' = '1')\n" +
                "as select f.k1, sum(f.v1) as v1, max(d.v1) as dim_v1 from test.tbl_fact f\n" +
                "left join test.tbl_dim d on f.v1 = d.k1 group by f.k1;");
        try {
            MaterializedView mv = getMv("mv_join");
            TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
            initAndExecuteTaskRun(taskRun);
            long versionBefore = visibleVersion(mv, "p20220103");

            truncatePartition("tbl_fact", "p20220103");
            starRocksAssert.getCtx().executeSql("TRUNCATE TABLE test.tbl_dim");
            mv.resetVisibleVersionMap();
            Assertions.assertTrue(getPartitionNamesToRefreshForMv(mv).contains("p20220103"));

            initAndExecuteTaskRun(taskRun);
            Assertions.assertTrue(refreshedMvPartitions(taskRun).contains("p20220103"));
            Assertions.assertTrue(visibleVersion(mv, "p20220103") > versionBefore);
            assertSettled(mv, taskRun);
        } finally {
            starRocksAssert.dropMaterializedView("mv_join");
            starRocksAssert.dropTable("tbl_fact");
            starRocksAssert.dropTable("tbl_dim");
        }
    }

    @Test
    public void testListPartitionEmptiedBeforeVersionMapResetIsRecomputed() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.tbl_list (province string, v1 int)\n" +
                "PARTITION BY LIST(province) (\n" +
                "  PARTITION p_beijing VALUES IN ('beijing'),\n" +
                "  PARTITION p_shanghai VALUES IN ('shanghai')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(province) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        executeInsertSql("insert into test.tbl_list partition(p_beijing) values('beijing', 1);");
        executeInsertSql("insert into test.tbl_list partition(p_shanghai) values('shanghai', 1);");
        starRocksAssert.withMaterializedView("create materialized view test.mv_list\n" +
                "partition by province\n" +
                "distributed by hash(province) buckets 3\n" +
                "refresh deferred manual\n" +
                "properties('replication_num' = '1')\n" +
                "as select province, sum(v1) as v1 from test.tbl_list group by province;");
        try {
            MaterializedView mv = getMv("mv_list");
            TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
            initAndExecuteTaskRun(taskRun);
            String shanghai = mv.getVisiblePartitionNames().stream()
                    .filter(name -> name.contains("shanghai")).findFirst().orElseThrow();
            long versionBefore = visibleVersion(mv, shanghai);

            truncatePartition("tbl_list", "p_shanghai");
            mv.resetVisibleVersionMap();
            Assertions.assertTrue(getPartitionNamesToRefreshForMv(mv).contains(shanghai));

            initAndExecuteTaskRun(taskRun);
            Assertions.assertTrue(refreshedMvPartitions(taskRun).contains(shanghai));
            Assertions.assertTrue(visibleVersion(mv, shanghai) > versionBefore);
            assertSettled(mv, taskRun);
        } finally {
            starRocksAssert.dropMaterializedView("mv_list");
            starRocksAssert.dropTable("tbl_list");
        }
    }

    @Test
    public void testNonPartitionedMvRecomputedAfterItsBaseTableWasEmptied() throws Exception {
        createSingleTable("tbl_single");
        starRocksAssert.withMaterializedView(singleMvSql("mv_single", "tbl_single", "refresh deferred manual"));
        try {
            MaterializedView mv = getMv("mv_single");
            TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
            initAndExecuteTaskRun(taskRun);
            long versionBefore = visibleVersion(mv, "mv_single");

            starRocksAssert.getCtx().executeSql("TRUNCATE TABLE test.tbl_single");
            mv.resetVisibleVersionMap();
            Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.FULL, getMvUpdateInfo(mv).getMVToRefreshType());

            Assertions.assertNotNull(getMVRefreshExecPlan(taskRun));
            Assertions.assertTrue(visibleVersion(mv, "mv_single") > versionBefore);
            Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, getMvUpdateInfo(mv).getMVToRefreshType());
            Assertions.assertNull(getMVRefreshExecPlan(taskRun));
        } finally {
            starRocksAssert.dropMaterializedView("mv_single");
            starRocksAssert.dropTable("tbl_single");
        }
    }

    @Test
    public void testTableExcludedFromRefreshLeavesNoUntrackedRows() throws Exception {
        createSingleTable("tbl_excluded");
        // excluded_refresh_tables only applies to async mvs; the start keeps the schedule from running here.
        starRocksAssert.withMaterializedView(singleMvSql("mv_excluded", "tbl_excluded",
                "refresh async start('2099-01-01 00:00:00') every (interval 1 day)"));
        try {
            MaterializedView mv = getMv("mv_excluded");
            initAndExecuteTaskRun(buildMVTaskRun(mv, DB_NAME));
            starRocksAssert.getCtx().executeSql("TRUNCATE TABLE test.tbl_excluded");
            mv.resetVisibleVersionMap();
            OlapTable baseTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(DB_NAME, "tbl_excluded");
            Assertions.assertTrue(MvRefreshArbiter.hasUntrackedRows(mv, baseTable));

            starRocksAssert.alterMvProperties("ALTER MATERIALIZED VIEW mv_excluded " +
                    "SET ('excluded_refresh_tables' = 'test.tbl_excluded')");
            Assertions.assertFalse(MvRefreshArbiter.hasUntrackedRows(mv, baseTable));
        } finally {
            starRocksAssert.dropMaterializedView("mv_excluded");
            starRocksAssert.dropTable("tbl_excluded");
        }
    }

    private static void createDailyTable(String name) throws Exception {
        starRocksAssert.withTable("CREATE TABLE test." + name + " (k1 date, v1 int)\n" +
                "PARTITION BY RANGE(k1) (\n" +
                "  PARTITION p20220101 VALUES [('2022-01-01'),('2022-01-02')),\n" +
                "  PARTITION p20220102 VALUES [('2022-01-02'),('2022-01-03')),\n" +
                "  PARTITION p20220103 VALUES [('2022-01-03'),('2022-01-04'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        for (String day : List.of("2022-01-01", "2022-01-02", "2022-01-03")) {
            executeInsertSql(String.format("insert into test.%s partition(p%s) values('%s', 1);",
                    name, day.replace("-", ""), day));
        }
    }

    private static String dailyMvSql(String mvName, String tableName, String extraProperties) {
        return "create materialized view test." + mvName + "\n" +
                "partition by k1\n" +
                "distributed by hash(k1) buckets 3\n" +
                "refresh deferred manual\n" +
                "properties('replication_num' = '1'" + extraProperties + ")\n" +
                "as select k1, sum(v1) as v1 from test." + tableName + " group by k1;";
    }

    private static void createSingleTable(String name) throws Exception {
        starRocksAssert.withTable("CREATE TABLE test." + name + " (k1 int, v1 int)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        executeInsertSql("insert into test." + name + " values(1, 1), (2, 2);");
    }

    private static String singleMvSql(String mvName, String tableName, String refreshScheme) {
        return "create materialized view test." + mvName + "\n" +
                "distributed by hash(k1) buckets 3\n" +
                refreshScheme + "\n" +
                "properties('replication_num' = '1')\n" +
                "as select k1, sum(v1) as v1 from test." + tableName + " group by k1;";
    }

    private static void truncatePartition(String tableName, String partitionName) throws Exception {
        starRocksAssert.getCtx().executeSql(
                String.format("TRUNCATE TABLE test.%s PARTITION (%s)", tableName, partitionName));
    }

    private static long visibleVersion(MaterializedView mv, String partitionName) {
        return mv.getPartition(partitionName).getDefaultPhysicalPartition().getVisibleVersion();
    }

    private static Set<String> refreshedMvPartitions(TaskRun taskRun) {
        return getPartitionBasedRefreshProcessor(taskRun).getMvContext().getRefreshScope()
                .getMvPartitionsToRefresh().getPartitionNames();
    }

    private static void assertSettled(MaterializedView mv, TaskRun taskRun) throws Exception {
        Assertions.assertTrue(getPartitionNamesToRefreshForMv(mv).isEmpty(), getPartitionNamesToRefreshForMv(mv)::toString);
        Assertions.assertNull(getMVRefreshExecPlan(taskRun));
    }
}
