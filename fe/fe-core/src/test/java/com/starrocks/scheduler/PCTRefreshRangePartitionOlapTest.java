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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@TestMethodOrder(MethodName.class)
public class PCTRefreshRangePartitionOlapTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    @Test
    public void testMVForceRefresh() throws Exception {
        String partitionTable = "CREATE TABLE range_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_t1", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_t1", "p2", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_t1 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_t1 partition(p2) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv1 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"partition_refresh_number\"=\"-1\")\n" +
                "AS SELECT dt1,sum(int1) from range_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv1");

        TaskRun taskRun = buildMVTaskRun(mv, "test");
        ExecPlan execPlan;
        // explain without force
        {
            execPlan = getMVRefreshExecPlan(taskRun);
            Assertions.assertNotNull(execPlan);

            refreshMV("test", mv);
            execPlan = getMVRefreshExecPlan(taskRun);
            Assertions.assertNull(execPlan);

            String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized " +
                    "view test_mv1;");
            Assertions.assertTrue(plan.contains("PLAN NOT AVAILABLE"));
        }

        // refresh with force
        Map<String, String> props = taskRun.getProperties();
        props.put(TaskRun.FORCE, "true");
        // explain with refresh
        {
            ExecuteOption executeOption = new ExecuteOption(taskRun.getTask());
            Map<String, String> explainProps = executeOption.getTaskRunProperties();
            explainProps.put(TaskRun.FORCE, "true");

            execPlan =
                    getMVRefreshExecPlan(mv, "explain refresh materialized view test_mv1 " +
                            "force;");
            Assertions.assertNotNull(execPlan);

            String plan = explainMVRefreshExecPlan(mv, executeOption, "explain refresh materialized view test_mv1 " +
                    "force;");
            Assertions.assertTrue(plan.contains("MVToRefreshedPartitions: [p20240104_20240105, p20240105_20240106]"));

            // after refresh, still can refresh with force
            execPlan = getMVRefreshExecPlan(taskRun, true);
            execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=2/2");
            Assertions.assertNotNull(execPlan);

            refreshMV("test", mv);

            // after refresh, still can refresh with force
            execPlan = getMVRefreshExecPlan(taskRun, true);
            execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=2/2");
            Assertions.assertNotNull(execPlan);
        }
    }

    @Test
    public void testExplainRefreshOnFreshPartitionRange() throws Exception {
        String partitionTable = "CREATE TABLE range_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_t1", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_t1", "p2", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_t1 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_t1 partition(p2) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv1 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"partition_refresh_number\"=\"-1\")\n" +
                "AS SELECT dt1,sum(int1) from range_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv1");
        refreshMV("test", mv);

        // only p2 goes stale, so the p1 range is fresh while the mv as a whole is not
        executeInsertSql("INSERT INTO range_t1 partition(p2) VALUES (\"2024-01-05\",2);");

        Map<String, String> props = new HashMap<>();
        props.put(TaskRun.PARTITION_START, "2024-01-04");
        props.put(TaskRun.PARTITION_END, "2024-01-05");
        String scopedPlan = explainMVRefreshExecPlan(mv, new ExecuteOption(70, false, props),
                "explain refresh materialized view test_mv1 partition start (\"2024-01-04\") " +
                        "end (\"2024-01-05\");");
        Assertions.assertTrue(scopedPlan.contains("PLAN NOT AVAILABLE"), scopedPlan);
        Assertions.assertTrue(
                scopedPlan.contains("NO REFRESH NEEDED: the requested partitions are already up to date"),
                scopedPlan);
        Assertions.assertFalse(scopedPlan.contains("the materialized view is already up to date"), scopedPlan);

        String wholePlan = explainMVRefreshExecPlan(mv, "explain refresh materialized view test_mv1;");
        Assertions.assertFalse(wholePlan.contains("NO REFRESH NEEDED"), wholePlan);
    }

    @Test
    public void testMVForceRefreshPropagateForce() throws Exception {
        String partitionTable = "CREATE TABLE range_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_t1", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_t1", "p2", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_t1 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_t1 partition(p2) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv1 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"partition_refresh_number\"=\"1\")\n" +
                "AS SELECT dt1,sum(int1) from range_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv1");

        TaskRun taskRun = buildMVTaskRun(mv, "test");

        // refresh with force
        ExecPlan execPlan = getMVRefreshExecPlan(taskRun, true);
        Assertions.assertNotNull(execPlan);

        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
        TaskRun nextTaskRun = processor.getNextTaskRun();
        String v = nextTaskRun.getProperties().get(TaskRun.FORCE);
        Assertions.assertEquals("true", v);

    }

    @Test
    public void testMVBatchRefreshSeedsFreshnessBaseline() throws Exception {
        String partitionTable = "CREATE TABLE range_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_t1", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_t1", "p2", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_t1 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_t1 partition(p2) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv1 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"partition_refresh_number\"=\"1\")\n" +
                "AS SELECT dt1,sum(int1) from range_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv1");

        TaskRun taskRun = buildMVTaskRun(mv, "test");
        taskRun.getProperties().put(TaskRun.FORCE, "true");
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        long leaderStartTime = 1718000000123L;
        taskRun.getStatus().setProcessStartTime(leaderStartTime);
        taskRun.executeTaskRun();

        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
        TaskRun nextTaskRun = processor.getNextTaskRun();
        Assertions.assertNotNull(nextTaskRun);
        Assertions.assertEquals(String.valueOf(leaderStartTime),
                nextTaskRun.getProperties().get(TaskRun.MV_FRESHNESS_BASELINE_TIME));
        String leaderSubmitUser = taskRun.getStatus().getSubmitUser();
        Assertions.assertNotNull(leaderSubmitUser);
        Assertions.assertEquals(leaderSubmitUser, nextTaskRun.getExecuteOption().getSubmitUser());
    }

    @Test
    public void testMVPartialRefreshDoesNotConfirmFreshness() throws Exception {
        String partitionTable = "CREATE TABLE range_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_t1", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_t1", "p2", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_t1 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_t1 partition(p2) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv1 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL\n" +
                "AS SELECT dt1,sum(int1) from range_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv1");

        TaskRun taskRun = buildMVTaskRun(mv, "test");
        taskRun.getProperties().put(TaskRun.PARTITION_START, "2024-01-04");
        taskRun.getProperties().put(TaskRun.PARTITION_END, "2024-01-05");
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.getStatus().setProcessStartTime(1718000000123L);
        taskRun.executeTaskRun();

        Assertions.assertEquals(0L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
    }

    @Test
    public void testMVBatchRefreshConfirmsFreshnessOnlyOnFinalRun() throws Exception {
        String partitionTable = "CREATE TABLE range_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_t1", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_t1", "p2", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_t1 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_t1 partition(p2) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv1 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"partition_refresh_number\"=\"1\")\n" +
                "AS SELECT dt1,sum(int1) from range_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv1");
        long previousConfirmedAt = 1717000000000L;
        mv.getRefreshScheme().setLastFreshnessConfirmedAt(previousConfirmedAt);

        TaskRun taskRun = buildMVTaskRun(mv, "test");
        taskRun.getProperties().put(TaskRun.FORCE, "true");
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        long leaderStartTime = 1718000000123L;
        taskRun.getStatus().setProcessStartTime(leaderStartTime);
        taskRun.executeTaskRun();

        // intermediate batch run keeps the previous confirmation untouched
        Assertions.assertEquals(previousConfirmedAt, mv.getRefreshScheme().getLastFreshnessConfirmedAt());

        TaskRun nextTaskRun = getPartitionBasedRefreshProcessor(taskRun).getNextTaskRun();
        Assertions.assertNotNull(nextTaskRun);
        nextTaskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        nextTaskRun.getStatus().setProcessStartTime(leaderStartTime + 5000);
        nextTaskRun.executeTaskRun();

        // the final batch run confirms freshness at the batch's first-run start
        Assertions.assertEquals(leaderStartTime, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
    }

    @Test
    public void testMVForcePartialRefresh() throws Exception {
        String partitionTable = "CREATE TABLE range_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_t1", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_t1", "p2", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_t1 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_t1 partition(p2) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv1 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"partition_refresh_number\"=\"-1\")\n" +
                "AS SELECT dt1,sum(int1) from range_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv1");

        TaskRun taskRun = buildMVTaskRun(mv, "test");
        Map<String, String> props = taskRun.getProperties();
        props.put(TaskRun.PARTITION_START, "2024-01-04");
        props.put(TaskRun.PARTITION_END, "2024-01-05");

        ExecPlan execPlan;
        // explain without force
        {
            execPlan = getMVRefreshExecPlan(taskRun);
            Assertions.assertNotNull(execPlan);

            refreshMV("test", mv);
            execPlan = getMVRefreshExecPlan(taskRun);
            Assertions.assertNull(execPlan);

            String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized " +
                    "view test_mv1;");
            Assertions.assertTrue(plan.contains("PLAN NOT AVAILABLE"));
        }

        // refresh with force
        props.put(TaskRun.FORCE, "true");
        // explain with refresh
        {
            ExecuteOption executeOption = new ExecuteOption(taskRun.getTask());
            Map<String, String> explainProps = executeOption.getTaskRunProperties();
            explainProps.put(TaskRun.FORCE, "true");
            explainProps.put(TaskRun.PARTITION_START, "2024-01-04");
            explainProps.put(TaskRun.PARTITION_END, "2024-01-05");
            execPlan =
                    getMVRefreshExecPlan(mv, "explain refresh materialized view test_mv1 " +
                            "force;");
            Assertions.assertNotNull(execPlan);

            String plan = explainMVRefreshExecPlan(mv, executeOption, "explain refresh materialized view test_mv1 " +
                    "force;");
            Assertions.assertTrue(plan.contains("MVToRefreshedPartitions: [p20240104_20240105]"));

            // after refresh, still can refresh with force
            execPlan = getMVRefreshExecPlan(taskRun, true);
            execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/2");
            Assertions.assertNotNull(execPlan);

            refreshMV("test", mv);

            // after refresh, still can refresh with force
            execPlan = getMVRefreshExecPlan(taskRun, true);
            execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/2");
            Assertions.assertNotNull(execPlan);
        }
    }

    @Test
    public void testAutoRefreshPartitionsLimitExcludingPartitionsDoesNotConfirmFreshness() throws Exception {
        String partitionTable = "CREATE TABLE range_limit_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_limit_t1", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_limit_t1", "p2", "2024-01-05", "2024-01-06");
        addRangePartition("range_limit_t1", "p3", "2024-01-06", "2024-01-07");
        String[] sqls = {
                "INSERT INTO range_limit_t1 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_limit_t1 partition(p2) VALUES (\"2024-01-05\",1);",
                "INSERT INTO range_limit_t1 partition(p3) VALUES (\"2024-01-06\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv_limit_excl " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL\n" +
                "AS SELECT dt1,sum(int1) from range_limit_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv_limit_excl");
        // auto_refresh_partitions_limit is rejected at CREATE time for MANUAL-refresh MVs
        // (PropertyAnalyzer#analyzeAutoRefreshPartitionsLimit), so set it directly on the table
        // property afterwards, mirroring PartitionBasedMvRefreshProcessorHiveTest
        // #testAutoRefreshPartitionLimitWithHiveTable's precedent for exercising this property
        // deterministically in tests (also sidesteps any background auto-refresh scheduling).
        mv.getTableProperty().setAutoRefreshPartitionsLimit(1);

        Assertions.assertEquals(0L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());

        // Build and run the task run directly (like testMVBatchRefreshConfirmsFreshnessOnlyOnFinalRun)
        // so we control processStartTime: refreshMV()/withMVRefreshTaskRun() never call
        // setProcessStartTime, which would leave freshnessBaselineTime()'s fallback at 0 regardless
        // of the bug and make this assertion pass vacuously.
        TaskRun taskRun = buildMVTaskRun(mv, "test");
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        long processStartTime = 1718000000123L;
        taskRun.getStatus().setProcessStartTime(processStartTime);
        // A plain refresh with no explicit partition range is a "complete refresh" by
        // MVRefreshParams' definition, but auto_refresh_partitions_limit=1 silently drops the
        // two oldest changed partitions (p1, p2) from this batch; only p3 (the newest) actually
        // gets refreshed.
        taskRun.executeTaskRun();

        // p1/p2 are still stale (never refreshed), so this refresh must NOT confirm whole-MV
        // freshness. Without the fix, lastFreshnessConfirmedAt wrongly advances to this run's
        // process start time because MVRefreshParams.isCompleteRefresh() only looks at the
        // (absent) explicit range/list request, not at whether the limit excluded partitions
        // from the batch.
        Assertions.assertEquals(0L, mv.getRefreshScheme().getLastFreshnessConfirmedAt(),
                "lastFreshnessConfirmedAt must not advance when auto_refresh_partitions_limit " +
                        "excluded changed partitions from a 'complete' refresh");
    }

    @Test
    public void testAutoRefreshPartitionsLimitWithinBoundsConfirmsFreshness() throws Exception {
        String partitionTable = "CREATE TABLE range_limit_t2 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_limit_t2", "p1", "2024-01-04", "2024-01-05");
        addRangePartition("range_limit_t2", "p2", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_limit_t2 partition(p1) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_limit_t2 partition(p2) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_mv_limit_ok " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL\n" +
                "AS SELECT dt1,sum(int1) from range_limit_t2 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_mv_limit_ok");
        // Control: the limit is well above the 2 changed partitions, so it never trims anything.
        // This batch really is complete, and freshness confirmation must still work.
        mv.getTableProperty().setAutoRefreshPartitionsLimit(10);

        Assertions.assertEquals(0L, mv.getRefreshScheme().getLastFreshnessConfirmedAt());

        TaskRun taskRun = buildMVTaskRun(mv, "test");
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        long processStartTime = 1718000000456L;
        taskRun.getStatus().setProcessStartTime(processStartTime);
        taskRun.executeTaskRun();

        Assertions.assertEquals(processStartTime, mv.getRefreshScheme().getLastFreshnessConfirmedAt(),
                "lastFreshnessConfirmedAt must advance to this run's start time for a complete " +
                        "refresh that did not exclude any partitions");
    }
    @Test
    public void testLeaderCatchUpUsesPeriodicRefreshScope() throws Exception {
        checkPeriodicCatchUpScope(false);
    }

    @Test
    public void testManualPeriodicMvRefreshRetainsExplicitHistoricalScope() throws Exception {
        checkPeriodicCatchUpScope(true);
    }

    private void checkPeriodicCatchUpScope(boolean manual) throws Exception {
        String tableName = manual ? "manual_scope_base" : "catch_up_scope_base";
        String mvName = manual ? "manual_scope_mv" : "catch_up_scope_mv";
        starRocksAssert.withTable("CREATE TABLE " + tableName + " (dt date, v int) " +
                "PARTITION BY date_trunc('day', dt)");
        try {
            addRangePartition(tableName, "p1", "2024-01-04", "2024-01-05");
            addRangePartition(tableName, "p2", "2024-01-05", "2024-01-06");
            addRangePartition(tableName, "p3", "2024-01-06", "2024-01-07");
            for (int day = 4; day <= 6; day++) {
                executeInsertSql("INSERT INTO " + tableName + " VALUES ('2024-01-0" + day + "', 1)");
            }
            // Real ASYNC metadata, with the global timer far in the future. Only the local controlled timer fires.
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName +
                    " PARTITION BY date_trunc('day', dt) " +
                    "REFRESH DEFERRED ASYNC START ('2122-01-01 00:00:00') EVERY (INTERVAL 60 SECOND) " +
                    "PROPERTIES ('partition_refresh_number'='-1', 'partition_refresh_strategy'='strict') " +
                    "AS SELECT dt, sum(v) AS total FROM " + tableName + " GROUP BY dt");
            try {
                MaterializedView mv = getMv(mvName);
                TaskRun baseline = buildMVTaskRun(mv, "test");
                baseline.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                executeDirectRun(baseline, 0);
                Assertions.assertNull(getPartitionBasedRefreshProcessor(baseline).getNextTaskRun());
                OlapTable base = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable("test", tableName);
                Map<String, MaterializedView.BasePartitionInfo> versions = mv.getRefreshScheme().getAsyncRefreshContext()
                        .getBaseTableVisibleVersionMap().get(base.getId());
                Assertions.assertEquals(3, versions.size());
                long oldVersion = versions.get("p1").getVersion();
                long secondOldVersion = versions.get("p2").getVersion();
                long confirmedBefore = mv.getRefreshScheme().getLastFreshnessConfirmedAt();
                Assertions.assertTrue(confirmedBefore > 0);
                mv.getTableProperty().setAutoRefreshPartitionsLimit(1);
                mv.getTableProperty().setPartitionRefreshNumber(1);
                for (int day = 4; day <= 6; day++) {
                    executeInsertSql("INSERT INTO " + tableName + " VALUES ('2024-01-0" + day + "', 2)");
                }
                Assertions.assertTrue(base.getPartition("p1").getLatestPhysicalPartition().getVisibleVersion() > oldVersion);

                Task registered = GlobalStateMgr.getCurrentState().getTaskManager().getTask(mv);
                Assertions.assertEquals(Constants.TaskType.PERIODICAL, registered.getType());
                Task task = TaskBuilder.buildMvTask(mv, "test");
                task.setId(registered.getId());
                task.setType(registered.getType());
                task.setState(Constants.TaskState.ACTIVE);
                long now = TimeUtils.getEpochSeconds();
                task.setSchedule(new TaskSchedule(now - 3600, 60, TimeUnit.SECONDS));
                task.setLastScheduleTime(now - 180);
                task.getProperties().put(TaskRun.IS_TEST, "true");
                try (TaskManagerTest.PeriodicTaskFixture fixture = new TaskManagerTest.PeriodicTaskFixture(task)) {
                    if (manual) {
                        fixture.manager.executeTask(task.getName());
                    } else {
                        fixture.manager.registerScheduler(task);
                    }
                    TaskRun run = fixture.takeRun();
                    if (manual) {
                        Assertions.assertEquals(Constants.TaskType.MANUAL, run.getType());
                        Assertions.assertTrue(run.getExecuteOption().isManual());
                    }
                    // The scope oracle is independent of the type assertion in TaskManagerTest (including in red).
                    executeCapturedRun(fixture, run, confirmedBefore);
                    Assertions.assertEquals(Set.of("p20240106_20240107"),
                            run.getStatus().getMvTaskRunExtraMessage().getMvPartitionsToRefresh());
                    Assertions.assertNull(getPartitionBasedRefreshProcessor(run).getNextTaskRun());
                    Assertions.assertEquals(oldVersion, versions.get("p1").getVersion());
                    Assertions.assertEquals(secondOldVersion, versions.get("p2").getVersion());
                    Assertions.assertEquals(confirmedBefore, mv.getRefreshScheme().getLastFreshnessConfirmedAt());

                    if (manual) {
                        ExecuteOption range = new ExecuteOption(task);
                        range.setManual(true);
                        range.getTaskRunProperties().put(TaskRun.PARTITION_START, "2024-01-05");
                        range.getTaskRunProperties().put(TaskRun.PARTITION_END, "2024-01-06");
                        fixture.manager.executeTask(task.getName(), range);
                        TaskRun historical = fixture.takeRun();
                        executeCapturedRun(fixture, historical, confirmedBefore);
                        Assertions.assertEquals(Set.of("p20240105_20240106"),
                                historical.getStatus().getMvTaskRunExtraMessage().getMvPartitionsToRefresh());
                        Assertions.assertEquals(Constants.TaskType.MANUAL, historical.getType());
                        Assertions.assertTrue(versions.get("p2").getVersion() > secondOldVersion);
                    } else {
                        // Re-dirty the latest partition and compare the actual normal tick against catch-up.
                        executeInsertSql("INSERT INTO " + tableName + " VALUES ('2024-01-06', 3)");
                        fixture.tick.run();
                        TaskRun periodic = fixture.takeRun();
                        Assertions.assertEquals(Constants.TaskType.PERIODICAL, periodic.getType());
                        Assertions.assertFalse(periodic.getExecuteOption().isManual());
                        executeCapturedRun(fixture, periodic, confirmedBefore);
                        Assertions.assertEquals(run.getStatus().getMvTaskRunExtraMessage().getMvPartitionsToRefresh(),
                                periodic.getStatus().getMvTaskRunExtraMessage().getMvPartitionsToRefresh());
                        Assertions.assertNull(getPartitionBasedRefreshProcessor(periodic).getNextTaskRun());
                    }
                }
            } finally {
                starRocksAssert.dropMaterializedView(mvName);
            }
        } finally {
            starRocksAssert.dropTable(tableName);
        }
    }

    private static void executeCapturedRun(TaskManagerTest.PeriodicTaskFixture fixture, TaskRun run,
                                            long confirmedBefore) throws Exception {
        try {
            executeDirectRun(run, confirmedBefore);
        } finally {
            fixture.manager.getTaskRunScheduler().removeRunningTask(run.getTaskId());
        }
    }

    private static void executeDirectRun(TaskRun run, long confirmedBefore) throws Exception {
        ConnectContext previous = ConnectContext.get();
        run.getStatus().setState(Constants.TaskRunState.RUNNING);
        run.getStatus().setProcessStartTime(Math.max(System.currentTimeMillis(), confirmedBefore + 1));
        try {
            Constants.TaskRunState state = run.executeTaskRun();
            run.getStatus().setState(state);
            run.getFuture().complete(state);
            Assertions.assertEquals(Constants.TaskRunState.SUCCESS, state);
        } finally {
            if (!run.getFuture().isDone()) {
                run.getFuture().cancel(true);
            }
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

}
