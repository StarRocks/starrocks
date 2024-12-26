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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TGetTasksParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.scheduler.TaskRun.MV_ID;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionBasedMvRefreshProcessorOlapPart2Test extends MVTestBase {
    private static final Logger LOG = LogManager.getLogger(PartitionBasedMvRefreshProcessorOlapPart2Test.class);

    private static String R1;
    private static String R2;
    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        // range partition table
        R1 = "CREATE TABLE r1 \n" +
                "(\n" +
                "    dt date,\n" +
                "    k2 int,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY RANGE(dt)\n" +
                "(\n" +
                "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                "    PARTITION p3 values [('2022-03-01'),('2022-04-01'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');";
        R2 = "CREATE TABLE r2 \n" +
                "(\n" +
                "    dt date,\n" +
                "    k2 int,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY date_trunc('day', dt)\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');";
    }

    @Test
    public void testMVRefreshWithTheSameTables1() {
        starRocksAssert.withMTables(List.of(
                        new MTable("tt1", "k1",
                                List.of(
                                        "k1 int",
                                        "k2 int",
                                        "k3 string",
                                        "dt date"
                                ),
                                "dt",
                                List.of(
                                        "PARTITION p0 values [('2021-12-01'),('2021-12-02'))",
                                        "PARTITION p1 values [('2021-12-02'),('2021-12-03'))",
                                        "PARTITION p2 values [('2021-12-03'),('2021-12-04'))"
                                )
                        )
                ),
                () -> {
                    String[] mvSqls = {
                            "create materialized view test_mv1 \n" +
                                    "partition by dt \n" +
                                    "distributed by RANDOM\n" +
                                    "refresh deferred manual\n" +
                                    "as select a.dt, b.k2 from tt1 a " +
                                    "   join tt1 b on a.dt = substr(date_sub(b.dt, interval dayofyear(a.dt) day), 1, 10)" +
                                    "   join tt1 c on a.dt = substr(date_add(c.dt, interval dayofyear(a.dt) day), 1, 10)" +
                                    " where a.k1 > 1 and b.k1 > 2 and c.k1 > 3;",
                            "create materialized view test_mv1 \n" +
                                    "partition by dt \n" +
                                    "distributed by RANDOM\n" +
                                    "refresh deferred manual\n" +
                                    "as select a.dt, b.k2 from tt1 a " +
                                    "   join tt1 b on a.dt = date_sub(b.dt, interval dayofyear(a.dt) day)" +
                                    "   join tt1 c on a.dt = date_add(c.dt, interval dayofyear(a.dt) day)" +
                                    " where a.k1 > 1 and b.k1 > 2 and c.k1 > 3;",
                    };
                    for (String mvSql : mvSqls) {
                        starRocksAssert.withMaterializedView(mvSql, (obj) -> {
                            String mvName = (String) obj;
                            assertPlanWithoutPushdownBelowScan(mvName);
                        });
                    }
                    ;
                });
    }

    private void assertPlanWithoutPushdownBelowScan(String mvName) throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), mvName));
        Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");

        String insertSql = "insert into tt1 partition(p0) values(1, 1, 1, '2021-12-01');";
        executeInsertSql(connectContext, insertSql);

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor =
                (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        ExecPlan execPlan = processor.getMvContext().getExecPlan();
        Assert.assertTrue(execPlan != null);
        String plan = execPlan.getExplainString(StatementBase.ExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, "     TABLE: tt1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: k1 > 1\n" +
                "     partitions=1/3");
        PlanTestBase.assertContains(plan, "     TABLE: tt1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: k1 > 2\n" +
                "     partitions=3/3");
        PlanTestBase.assertContains(plan, "     TABLE: tt1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 9: k1 > 3\n" +
                "     partitions=3/3");
    }

    @Test
    public void testMVRefreshWithTheSameTables22() {
        starRocksAssert.withMTables(List.of(
                        new MTable("tt1", "k1",
                                List.of(
                                        "k1 int",
                                        "k2 int",
                                        "k3 string",
                                        "dt date"
                                ),
                                "dt",
                                List.of(
                                        "PARTITION p0 values [('2021-12-01'),('2021-12-02'))",
                                        "PARTITION p1 values [('2021-12-02'),('2021-12-03'))",
                                        "PARTITION p2 values [('2021-12-03'),('2021-12-04'))"
                                )
                        )
                ),
                () -> {
                    String[] mvSqls = {
                            "create materialized view test_mv1 \n" +
                                    "partition by dt \n" +
                                    "distributed by RANDOM\n" +
                                    "refresh deferred manual\n" +
                                    "as select a.dt, b.k2 from tt1 a " +
                                    "   join tt1 b on a.dt = b.dt" +
                                    "   join tt1 c on a.dt = c.dt" +
                                    " where a.k1 > 1 and b.k1 > 2 and c.k1 > 3;",
                            "create materialized view test_mv1 \n" +
                                    "partition by dt \n" +
                                    "distributed by RANDOM\n" +
                                    "refresh deferred manual\n" +
                                    "as select a.dt, b.k2 from tt1 a " +
                                    "   join tt1 b on a.dt = date_trunc('day', b.dt)" +
                                    "   join tt1 c on a.dt = date_trunc('day', c.dt)" +
                                    " where a.k1 > 1 and b.k1 > 2 and c.k1 > 3;",
                    };
                    for (String mvSql : mvSqls) {
                        starRocksAssert.withMaterializedView(mvSql, (obj) -> {
                            String mvName = (String) obj;
                            assertPlanWithPushdownBelowScan(mvName);
                        });
                    }
                    ;
                });
    }

    private void assertPlanWithPushdownBelowScan(String mvName) throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), mvName));
        Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        Map<String, String> testProperties = task.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");

        String insertSql = "insert into tt1 partition(p0) values(1, 1, 1, '2021-12-01');";
        executeInsertSql(connectContext, insertSql);

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor =
                (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        ExecPlan execPlan = processor.getMvContext().getExecPlan();
        Assert.assertTrue(execPlan != null);
        String plan = execPlan.getExplainString(StatementBase.ExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, "     TABLE: tt1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: k1 > 1\n" +
                "     partitions=1/3");
        PlanTestBase.assertContains(plan, "     TABLE: tt1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: k1 > 2\n" +
                "     partitions=1/3");
        PlanTestBase.assertContains(plan, "     TABLE: tt1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 9: k1 > 3\n" +
                "     partitions=1/3");
    }

    @Test
    public void testRefreshWithCachePartitionTraits() throws Exception {
        starRocksAssert.withTable("CREATE TABLE tbl1 \n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withMaterializedView("create materialized view test_mv1 \n" +
                        "partition by (k1) \n" +
                        "distributed by random \n" +
                        "refresh deferred manual\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;",
                () -> {
                    executeInsertSql(connectContext, "insert into tbl1 values(\"2022-02-20\", 1, 10)");
                    OlapTable table = (OlapTable) getTable("test", "tbl1");
                    MaterializedView mv = getMv("test", "test_mv1");
                    PartitionBasedMvRefreshProcessor processor = refreshMV("test", mv);
                    QueryMaterializationContext queryMVContext = connectContext.getQueryMVContext();
                    Assert.assertTrue(queryMVContext == null);

                    {
                        RuntimeProfile runtimeProfile = processor.getRuntimeProfile();
                        QueryMaterializationContext.QueryCacheStats queryCacheStats = getQueryCacheStats(runtimeProfile);
                        String key = String.format("cache_getUpdatedPartitionNames_%s_%s", mv.getId(), table.getId());
                        Assert.assertTrue(queryCacheStats != null);
                        Assert.assertTrue(queryCacheStats.getCounter().containsKey(key));
                        Assert.assertTrue(queryCacheStats.getCounter().get(key) == 1);
                    }

                    Set<String> partitionsToRefresh1 = getPartitionNamesToRefreshForMv(mv);
                    Assert.assertTrue(partitionsToRefresh1.isEmpty());

                    executeInsertSql(connectContext, "insert into tbl1 values(\"2022-02-20\", 2, 10)");
                    Partition p2 = table.getPartition("p2");
                    while (p2.getDefaultPhysicalPartition().getVisibleVersion() != 3) {
                        Thread.sleep(1000);
                    }
                    MvUpdateInfo mvUpdateInfo = getMvUpdateInfo(mv);
                    Assert.assertTrue(mvUpdateInfo.getMvToRefreshType() == MvUpdateInfo.MvToRefreshType.PARTIAL);
                    Assert.assertTrue(mvUpdateInfo.isValidRewrite());
                    partitionsToRefresh1 = getPartitionNamesToRefreshForMv(mv);
                    Assert.assertFalse(partitionsToRefresh1.isEmpty());

                    {
                        processor = refreshMV("test", mv);
                        RuntimeProfile runtimeProfile = processor.getRuntimeProfile();
                        QueryMaterializationContext.QueryCacheStats queryCacheStats = getQueryCacheStats(runtimeProfile);
                        String key = String.format("cache_getUpdatedPartitionNames_%s_%s", mv.getId(), table.getId());
                        Assert.assertTrue(queryCacheStats != null);
                        Assert.assertTrue(queryCacheStats.getCounter().containsKey(key));
                        Assert.assertTrue(queryCacheStats.getCounter().get(key) >= 1);
                    }
                });
        starRocksAssert.dropTable("tbl1");
    }

    @Test
    public void testTaskRun() {
        starRocksAssert.withTable(new MTable("tbl6", "k2",
                        List.of(
                                "k1 date",
                                "k2 int",
                                "v1 int"
                        ),
                        "k1",
                        List.of(
                                "PARTITION p0 values [('2021-12-01'),('2022-01-01'))",
                                "PARTITION p1 values [('2022-01-01'),('2022-02-01'))",
                                "PARTITION p2 values [('2022-02-01'),('2022-03-01'))",
                                "PARTITION p3 values [('2022-03-01'),('2022-04-01'))",
                                "PARTITION p4 values [('2022-04-01'),('2022-05-01'))"
                        )
                ),
                () -> {
                    starRocksAssert
                            .withMaterializedView("create materialized view test_task_run \n" +
                                    "partition by date_trunc('month',k1) \n" +
                                    "distributed by hash(k2) buckets 10\n" +
                                    "refresh deferred manual\n" +
                                    "properties('replication_num' = '1', 'partition_refresh_number'='1')\n" +
                                    "as select k1, k2 from tbl6;");
                    String mvName = "test_task_run";
                    Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
                    MaterializedView mv = ((MaterializedView) testDb.getTable(mvName));
                    TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();

                    executeInsertSql(connectContext, "insert into tbl6 partition(p1) values('2022-01-02',2,10);");
                    executeInsertSql(connectContext, "insert into tbl6 partition(p2) values('2022-02-02',2,10);");

                    long taskId = tm.getTask(TaskBuilder.getMvTaskName(mv.getId())).getId();
                    TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();

                    // refresh materialized view
                    HashMap<String, String> taskRunProperties = new HashMap<>();
                    taskRunProperties.put(TaskRun.FORCE, Boolean.toString(true));
                    Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    taskRun.setTaskId(taskId);
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());

                    Assert.assertTrue(taskRunScheduler.addPendingTaskRun(taskRun));
                    Assert.assertNotNull(taskRunScheduler.getRunnableTaskRun(taskId));

                    // without db name
                    Assert.assertFalse(tm.getMatchedTaskRunStatus(null).isEmpty());
                    Assert.assertFalse(tm.filterTasks(null).isEmpty());
                    Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(null, null).isEmpty());

                    // specific db
                    TGetTasksParams getTasksParams = new TGetTasksParams();
                    getTasksParams.setDb(DB_NAME);
                    Assert.assertFalse(tm.getMatchedTaskRunStatus(getTasksParams).isEmpty());
                    Assert.assertFalse(tm.filterTasks(getTasksParams).isEmpty());
                    Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(DB_NAME, null).isEmpty());

                    while (taskRunScheduler.getRunningTaskCount() > 0) {
                        Thread.sleep(100);
                    }
                    starRocksAssert.dropMaterializedView("test_task_run");
                }
        );
    }

    @Test
    public void testRefreshPriority() {
        starRocksAssert.withTable(new MTable("tbl6", "k2",
                        List.of(
                                "k1 date",
                                "k2 int",
                                "v1 int"
                        ),
                        "k1",
                        List.of(
                                "PARTITION p0 values [('2021-12-01'),('2022-01-01'))",
                                "PARTITION p1 values [('2022-01-01'),('2022-02-01'))",
                                "PARTITION p2 values [('2022-02-01'),('2022-03-01'))",
                                "PARTITION p3 values [('2022-03-01'),('2022-04-01'))",
                                "PARTITION p4 values [('2022-04-01'),('2022-05-01'))"
                        )
                ),
                () -> {
                    String mvName = "mv_refresh_priority";
                    starRocksAssert.withMaterializedView("create materialized view test.mv_refresh_priority\n" +
                            "partition by date_trunc('month',k1) \n" +
                            "distributed by hash(k2) buckets 10\n" +
                            "refresh deferred manual\n" +
                            "properties('replication_num' = '1', 'partition_refresh_number'='1')\n" +
                            "as select k1, k2 from tbl6;");
                    Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                    MaterializedView mv = ((MaterializedView) testDb.getTable(mvName));
                    TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
                    long taskId = tm.getTask(TaskBuilder.getMvTaskName(mv.getId())).getId();
                    TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();

                    executeInsertSql(connectContext, "insert into tbl6 partition(p1) values('2022-01-02',2,10);");
                    executeInsertSql(connectContext, "insert into tbl6 partition(p2) values('2022-02-02',2,10);");

                    HashMap<String, String> taskRunProperties = new HashMap<>();
                    taskRunProperties.put(TaskRun.FORCE, Boolean.toString(true));
                    Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    initAndExecuteTaskRun(taskRun);
                    TGetTasksParams params = new TGetTasksParams();
                    params.setTask_name(task.getName());
                    List<TaskRunStatus> statuses = tm.getMatchedTaskRunStatus(params);
                    while (statuses.size() != 1) {
                        statuses = tm.getMatchedTaskRunStatus(params);
                        Thread.sleep(100);
                    }
                    Assert.assertEquals(1, statuses.size());
                    TaskRunStatus status = statuses.get(0);
                    // default priority for next refresh batch is Constants.TaskRunPriority.HIGHER.value()
                    Assert.assertEquals(Constants.TaskRunPriority.HIGHER.value(), status.getPriority());
                    starRocksAssert.dropMaterializedView("mv_refresh_priority");
                }
        );
    }

    @Test
    public void testMVRefreshProperties() {
        starRocksAssert.withTable(new MTable("tbl6", "k2",
                        List.of(
                                "k1 date",
                                "k2 int",
                                "v1 int"
                        ),
                        "k1",
                        List.of(
                                "PARTITION p0 values [('2021-12-01'),('2022-01-01'))",
                                "PARTITION p1 values [('2022-01-01'),('2022-02-01'))",
                                "PARTITION p2 values [('2022-02-01'),('2022-03-01'))",
                                "PARTITION p3 values [('2022-03-01'),('2022-04-01'))",
                                "PARTITION p4 values [('2022-04-01'),('2022-05-01'))"
                        )
                ),
                () -> {
                    String mvName = "test_mv1";
                    starRocksAssert.withMaterializedView("create materialized view test_mv1 \n" +
                            "partition by date_trunc('month',k1) \n" +
                            "distributed by hash(k2) buckets 10\n" +
                            "refresh deferred manual\n" +
                            "properties(" +
                            "   'replication_num' = '1', " +
                            "   'session.enable_materialized_view_rewrite' = 'true', \n" +
                            "   'session.enable_materialized_view_for_insert' = 'true',  \n" +
                            "   'partition_refresh_number'='1'" +
                            ")\n" +
                            "as select k1, k2 from tbl6;",
                            () -> {
                                Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                                MaterializedView mv = ((MaterializedView) testDb.getTable(mvName));
                                executeInsertSql(connectContext,
                                        "insert into tbl6 partition(p1) values('2022-01-02',2,10);");
                                executeInsertSql(connectContext, "insert into tbl6 partition(p2) values('2022-02-02',2,10);");

                                HashMap<String, String> taskRunProperties = new HashMap<>();
                                taskRunProperties.put(TaskRun.FORCE, Boolean.toString(true));
                                Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
                                ExecuteOption executeOption = new ExecuteOption(70, false, new HashMap<>());
                                TaskRun taskRun = TaskRunBuilder.newBuilder(task).setExecuteOption(executeOption).build();
                                initAndExecuteTaskRun(taskRun);
                                TGetTasksParams params = new TGetTasksParams();
                                params.setTask_name(task.getName());
                                TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
                                List<TaskRunStatus> statuses = tm.getMatchedTaskRunStatus(params);
                                while (statuses.size() != 1) {
                                    statuses = tm.getMatchedTaskRunStatus(params);
                                    Thread.sleep(100);
                                }
                                Assert.assertEquals(1, statuses.size());
                                TaskRunStatus status = statuses.get(0);
                                // the priority for next refresh batch is 70 which is specified in executeOption
                                Assert.assertEquals(70, status.getPriority());

                                PartitionBasedMvRefreshProcessor processor =
                                        (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                MvTaskRunContext mvTaskRunContext = processor.getMvContext();
                                Assert.assertEquals(70, mvTaskRunContext.getExecuteOption().getPriority());
                                Map<String, String> properties = mvTaskRunContext.getProperties();
                                Assert.assertEquals(1, properties.size());
                                Assert.assertTrue(properties.containsKey(MV_ID));
                                // Ensure that table properties are not passed to the task run
                                Assert.assertFalse(properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));

                                ConnectContext context = mvTaskRunContext.getCtx();
                                SessionVariable sessionVariable = context.getSessionVariable();
                                // Ensure that session properties are set
                                Assert.assertTrue(sessionVariable.isEnableMaterializedViewRewrite());
                                Assert.assertTrue(sessionVariable.isEnableMaterializedViewRewriteForInsert());
                                starRocksAssert.dropMaterializedView(mvName);
                            });
                }
        );
    }
}