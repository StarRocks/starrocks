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
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
        String result = "";
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
            result = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=2/2");
            Assertions.assertNotNull(execPlan);

            refreshMV("test", mv);

            // after refresh, still can refresh with force
            execPlan = getMVRefreshExecPlan(taskRun, true);
            result = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=2/2");
            Assertions.assertNotNull(execPlan);
        }
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
        String result = "";
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
            result = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/2");
            Assertions.assertNotNull(execPlan);

            refreshMV("test", mv);

            // after refresh, still can refresh with force
            execPlan = getMVRefreshExecPlan(taskRun, true);
            result = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: range_t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/2");
            Assertions.assertNotNull(execPlan);
        }
    }

    /**
     * Regression for https://github.com/StarRocks/starrocks/issues/71974:
     * a single sync task run must
     * <ol>
     *   <li>honor {@code partition_refresh_number} (scanned partition count capped to N),</li>
     *   <li>publish the next-batch cursor to {@link MVTaskRunExtraMessage} so
     *       {@link TaskManager#executeTaskSync} can drive follow-up batches, and</li>
     *   <li>NOT auto-spawn an async follow-up task run (that would race the caller's sync loop).</li>
     * </ol>
     */
    @Test
    public void testSyncRefreshSingleBatchInternalContract() throws Exception {
        String partitionTable = "CREATE TABLE range_sync_t1 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_sync_t1", "p1", "2024-01-01", "2024-01-02");
        addRangePartition("range_sync_t1", "p2", "2024-01-02", "2024-01-03");
        addRangePartition("range_sync_t1", "p3", "2024-01-03", "2024-01-04");
        addRangePartition("range_sync_t1", "p4", "2024-01-04", "2024-01-05");
        addRangePartition("range_sync_t1", "p5", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_sync_t1 partition(p1) VALUES (\"2024-01-01\",1);",
                "INSERT INTO range_sync_t1 partition(p2) VALUES (\"2024-01-02\",1);",
                "INSERT INTO range_sync_t1 partition(p3) VALUES (\"2024-01-03\",1);",
                "INSERT INTO range_sync_t1 partition(p4) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_sync_t1 partition(p5) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_sync_mv1 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"partition_refresh_number\"=\"2\")\n" +
                "AS SELECT dt1,sum(int1) from range_sync_t1 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_sync_mv1");

        Task task = TaskBuilder.buildMvTask(mv, "test");
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");

        ExecuteOption syncOption = new ExecuteOption(task);
        syncOption.setManual(true);
        syncOption.setSync(true);
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).setExecuteOption(syncOption).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
        MvTaskRunContext mvContext = processor.getMvContext();

        // (1) Batch scan must be bounded by partition_refresh_number
        ExecPlan execPlan = mvContext.getExecPlan();
        Assertions.assertNotNull(execPlan);
        PlanTestBase.assertContains(execPlan.getExplainString(TExplainLevel.NORMAL),
                "     TABLE: range_sync_t1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=2/5");

        // (2) Next-batch cursor must be published both on the in-memory context
        //     and on the persisted TaskRunStatus extra message.
        Assertions.assertTrue(mvContext.hasNextBatchPartition(),
                "nextPartitionStart/End must be set on mvContext");
        MVTaskRunExtraMessage extra = taskRun.getStatus().getMvTaskRunExtraMessage();
        Assertions.assertNotNull(extra, "MVTaskRunExtraMessage must be present");
        boolean hasRangeCursor = extra.getNextPartitionStart() != null && extra.getNextPartitionEnd() != null;
        boolean hasListCursor = extra.getNextPartitionValues() != null;
        Assertions.assertTrue(hasRangeCursor || hasListCursor,
                "extra message must publish the next partition cursor for TaskManager.executeTaskSync loop");

        // (3) Sync mode must not auto-spawn an async follow-up — that is the caller's job.
        Assertions.assertNull(processor.getNextTaskRun(),
                "sync refresh must not spawn an async next task run");
    }

    /**
     * End-to-end regression for https://github.com/StarRocks/starrocks/issues/71974:
     * simulate the {@link TaskManager#executeTaskSync} continuation loop to confirm that a single
     * {@code REFRESH ... WITH SYNC MODE} drives all stale partitions to completion, internally
     * fanning out into multiple task runs each bounded by {@code partition_refresh_number}.
     */
    @Test
    public void testSyncRefreshDrivesAllPartitionsAcrossBatches() throws Exception {
        String partitionTable = "CREATE TABLE range_sync_t2 (dt1 date, int1 int)\n" +
                "PARTITION BY date_trunc('day', dt1)";
        starRocksAssert.withTable(partitionTable);
        addRangePartition("range_sync_t2", "p1", "2024-01-01", "2024-01-02");
        addRangePartition("range_sync_t2", "p2", "2024-01-02", "2024-01-03");
        addRangePartition("range_sync_t2", "p3", "2024-01-03", "2024-01-04");
        addRangePartition("range_sync_t2", "p4", "2024-01-04", "2024-01-05");
        addRangePartition("range_sync_t2", "p5", "2024-01-05", "2024-01-06");
        String[] sqls = {
                "INSERT INTO range_sync_t2 partition(p1) VALUES (\"2024-01-01\",1);",
                "INSERT INTO range_sync_t2 partition(p2) VALUES (\"2024-01-02\",1);",
                "INSERT INTO range_sync_t2 partition(p3) VALUES (\"2024-01-03\",1);",
                "INSERT INTO range_sync_t2 partition(p4) VALUES (\"2024-01-04\",1);",
                "INSERT INTO range_sync_t2 partition(p5) VALUES (\"2024-01-05\",1);"
        };
        for (String sql : sqls) {
            executeInsertSql(sql);
        }

        String mvQuery = "CREATE MATERIALIZED VIEW test_sync_mv2 " +
                "PARTITION BY date_trunc('day', dt1) " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"partition_refresh_number\"=\"2\")\n" +
                "AS SELECT dt1,sum(int1) from range_sync_t2 group by dt1";
        starRocksAssert.withMaterializedView(mvQuery);

        MaterializedView mv = getMv("test_sync_mv2");
        OlapTable baseTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(starRocksAssert.getDb("test").getFullName(), "range_sync_t2");

        Task task = TaskBuilder.buildMvTask(mv, "test");
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");

        // Simulate the batch-continuation loop of TaskManager.executeTaskSync:
        // execute one task run, read nextPartition cursor from the extra message, build the next
        // option, and loop until no more partitions need refreshing.
        ExecuteOption currentOption = new ExecuteOption(task);
        currentOption.setManual(true);
        currentOption.setSync(true);

        int batches = 0;
        Set<String> scannedPartitions = new HashSet<>();
        int safety = 10;
        while (safety-- > 0) {
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).setExecuteOption(currentOption).build();
            taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
            taskRun.executeTaskRun();
            batches++;

            MVPCTRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);
            MVTaskRunExtraMessage extra = taskRun.getStatus().getMvTaskRunExtraMessage();
            Assertions.assertNotNull(extra);
            if (extra.getBasePartitionsToRefreshMap() != null) {
                extra.getBasePartitionsToRefreshMap().values().forEach(scannedPartitions::addAll);
            }
            Assertions.assertNull(processor.getNextTaskRun(),
                    "sync mode must never stash an async next task run");

            String nextStart = extra.getNextPartitionStart();
            String nextEnd = extra.getNextPartitionEnd();
            String nextValues = extra.getNextPartitionValues();
            boolean hasMore = (nextStart != null && nextEnd != null) || nextValues != null;
            if (!hasMore) {
                break;
            }

            Map<String, String> nextProps = new HashMap<>();
            for (Map.Entry<String, String> e : currentOption.getTaskRunProperties().entrySet()) {
                if (e.getKey() == null || e.getValue() == null
                        || TaskRun.MV_UNCOPYABLE_PROPERTIES.contains(e.getKey())) {
                    continue;
                }
                nextProps.put(e.getKey(), e.getValue());
            }
            if (nextValues != null) {
                nextProps.put(TaskRun.PARTITION_VALUES, nextValues);
            } else {
                nextProps.put(TaskRun.PARTITION_START, nextStart);
                nextProps.put(TaskRun.PARTITION_END, nextEnd);
            }
            ExecuteOption nextOption = new ExecuteOption(currentOption.getPriority(), true, nextProps);
            nextOption.setSync(true);
            nextOption.setManual(true);
            currentOption = nextOption;
        }

        // With 5 stale partitions and partition_refresh_number=2, we expect 3 internal batches
        // (2 + 2 + 1) to cover all partitions within one conceptual sync call.
        Assertions.assertEquals(3, batches,
                "expected 3 internal batches of partition_refresh_number=2 for 5 stale partitions");

        // All 5 base-table partitions should have been pulled into MV refreshes across the batches.
        Assertions.assertEquals(
                Set.of("p1", "p2", "p3", "p4", "p5"),
                scannedPartitions,
                "every stale base-table partition must be covered by the internal batch loop");

        // After the last sync batch, the MV tracks every base-table partition as refreshed.
        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                mv.getRefreshScheme().getAsyncRefreshContext();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseVisibleMap =
                asyncRefreshContext.getBaseTableVisibleVersionMap();
        Assertions.assertTrue(baseVisibleMap.containsKey(baseTable.getId()),
                "base-table refresh map must exist after sync refresh completes");
        Set<String> refreshedBasePartitions = baseVisibleMap.get(baseTable.getId()).keySet();
        Assertions.assertEquals(
                Set.of("p1", "p2", "p3", "p4", "p5"),
                refreshedBasePartitions,
                "every base-table partition must be recorded as refreshed after the sync loop");
    }
}
