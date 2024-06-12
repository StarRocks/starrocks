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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowMaterializedViewStatus;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TUniqueId;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.starrocks.sql.plan.PlanTestBase.cleanupEphemeralMVs;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionBasedMvRefreshProcessorOlapTest extends MVRefreshTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVRefreshTestBase.beforeClass();

        starRocksAssert.withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-01-01'), ('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'), ('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl4\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl5\n" +
                        "(\n" +
                        "    dt date,\n" +
                        "    k1 datetime,\n" +
                        "    k2 int,\n" +
                        "    k3 bigint\n" +
                        ")\n" +
                        "PARTITION BY RANGE(dt)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl15\n" +
                        "(\n" +
                        "    k1 datetime,\n" +
                        "    k2 int,\n" +
                        "    v1 int\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p20220101 values [('2022-01-01 00:00:00'),('2022-01-02 00:00:00')),\n" +
                        "    PARTITION p20220102 values [('2022-01-02 00:00:00'),('2022-01-03 00:00:00')),\n" +
                        "    PARTITION p20220103 values [('2022-01-03 00:00:00'),('2022-01-04 00:00:00')),\n" +
                        "    PARTITION p20220201 values [('2022-02-01 00:00:00'),('2022-02-02 00:00:00')),\n" +
                        "    PARTITION p20220202 values [('2022-02-02 00:00:00'),('2022-02-03 00:00:00'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl16\n" +
                        "(\n" +
                        "    k1 datetime,\n" +
                        "    k2 int,\n" +
                        "    v1 int\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p20220101 values [('2022-01-01 00:00:00'),('2022-01-02 00:00:00')),\n" +
                        "    PARTITION p20220102 values [('2022-01-02 00:00:00'),('2022-01-03 00:00:00')),\n" +
                        "    PARTITION p20220103 values [('2022-01-03 00:00:00'),('2022-01-04 00:00:00')),\n" +
                        "    PARTITION p20220201 values [('2022-02-01 00:00:00'),('2022-02-02 00:00:00')),\n" +
                        "    PARTITION p20220202 values [('2022-02-02 00:00:00'),('2022-02-03 00:00:00'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view test.union_all_mv\n" +
                        "partition by dt \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select dt, -1 as k2 from tbl5 where k2 is null union all select dt, k2 from tbl5;")
                .withMaterializedView("create materialized view test.mv1\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;")
                .withMaterializedView("create materialized view test.mv2\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl4.k1, tbl4.k2 from tbl4;")
                .withMaterializedView("create materialized view test.mv_inactive\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;")
                .withMaterializedView("create materialized view test.mv_without_partition\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select k2, sum(v1) as total_sum from tbl3 group by k2;")
                .withTable("CREATE TABLE test.base\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view test.mv_with_test_refresh\n" +
                        "partition by k1\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "as select k1, k2, sum(v1) as total_sum from base group by k1, k2;");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        cleanupEphemeralMVs(starRocksAssert, startSuiteTime);
    }

    @Before
    public void before() {
        startCaseTime = Instant.now().getEpochSecond();
    }

    @After
    public void after() throws Exception {
        cleanupEphemeralMVs(starRocksAssert, startCaseTime);
    }

    protected void assertPlanContains(ExecPlan execPlan, String... explain) throws Exception {
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    private static void initAndExecuteTaskRun(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
    }

    @Test
    public void testUnionAllMvWithPartition() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("union_all_mv"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        try {
            // base table partition insert data
            String insertSql = "insert into tbl5 partition(p4) values('2022-04-01', '2021-04-01 00:02:11', 3, 10);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            // TODO(fixme): for self join, forbid pushing down filter, but there are some cases to optimize.
            PlanTestBase.assertContains(plan, "partitions=5/5");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testWithPartition() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv1"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        try {
            // first sync partition
            initAndExecuteTaskRun(taskRun);
            Collection<Partition> partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // add tbl1 partition p5
            String addPartitionSql = "ALTER TABLE test.tbl1 ADD\n" +
                    "PARTITION p5 VALUES [('2022-05-01'),('2022-06-01'))";
            new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                    addPartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
            initAndExecuteTaskRun(taskRun);
            partitions = materializedView.getPartitions();
            Assert.assertEquals(6, partitions.size());
            // drop tbl2 partition p5
            String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p5\n";
            new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                    dropPartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
            initAndExecuteTaskRun(taskRun);
            partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // add tbl2 partition p3
            addPartitionSql = "ALTER TABLE test.tbl2 ADD PARTITION p3 values less than('2022-04-01')";
            new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                    addPartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
            initAndExecuteTaskRun(taskRun);
            partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // drop tbl2 partition p3
            dropPartitionSql = "ALTER TABLE test.tbl2 DROP PARTITION p3";
            new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                    dropPartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
            initAndExecuteTaskRun(taskRun);
            partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());

            // base table partition insert data
            testBaseTablePartitionInsertData(testDb, materializedView, taskRun);

            // base table partition replace
            testBaseTablePartitionReplace(testDb, materializedView, taskRun);

            // base table add partition
            testBaseTableAddPartitionWhileSync(testDb, materializedView, taskRun);
            testBaseTableAddPartitionWhileRefresh(testDb, materializedView, taskRun);

            // base table drop partition
            testBaseTableDropPartitionWhileSync(testDb, materializedView, taskRun);
            testBaseTableDropPartitionWhileRefresh(testDb, materializedView, taskRun);

            // base table partition rename
            testBaseTablePartitionRename(taskRun);

            testRefreshWithFailure(testDb, materializedView, taskRun);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMvWithoutPartition() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_without_partition"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        try {
            initAndExecuteTaskRun(taskRun);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("refresh failed");
        }
    }

    @Test
    public void testRangePartitionRefresh() throws Exception {
        MaterializedView materializedView = refreshMaterializedView("mv2", "2022-01-03", "2022-02-05");

        String insertSql = "insert into tbl4 partition(p1) values('2022-01-02',2,10);";
        executeInsertSql(connectContext, insertSql);

        refreshMVRange(materializedView.getName(), null, null, false);
        Assert.assertEquals(1, materializedView.getPartition("p202112_202201").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202201_202202").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202202_202203").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202203_202204").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202204_202205").getVisibleVersion());

        refreshMVRange(materializedView.getName(), "2021-12-03", "2022-04-05", false);
        Assert.assertEquals(1, materializedView.getPartition("p202112_202201").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202201_202202").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202202_202203").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202203_202204").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202204_202205").getVisibleVersion());

        insertSql = "insert into tbl4 partition(p3) values('2022-03-02',21,102);";
        executeInsertSql(connectContext, insertSql);
        insertSql = "insert into tbl4 partition(p0) values('2021-12-02',81,182);";
        executeInsertSql(connectContext, insertSql);

        refreshMVRange(materializedView.getName(), "2021-12-03", "2022-03-01", false);
        Assert.assertEquals(2, materializedView.getPartition("p202112_202201").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202201_202202").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202202_202203").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202203_202204").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202204_202205").getVisibleVersion());

        refreshMVRange(materializedView.getName(), "2021-12-03", "2022-05-06", true);
        Assert.assertEquals(3, materializedView.getPartition("p202112_202201").getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p202201_202202").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202202_202203").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202203_202204").getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p202204_202205").getVisibleVersion());
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
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
                    MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                    TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
                    TaskRunManager trm = tm.getTaskRunManager();

                    executeInsertSql(connectContext, "insert into tbl6 partition(p1) values('2022-01-02',2,10);");
                    executeInsertSql(connectContext, "insert into tbl6 partition(p2) values('2022-02-02',2,10);");

                    HashMap<String, String> taskRunProperties = new HashMap<>();
                    taskRunProperties.put(TaskRun.FORCE, Boolean.toString(true));
                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    initAndExecuteTaskRun(taskRun);

                    long taskId = tm.getTask(TaskBuilder.getMvTaskName(materializedView.getId())).getId();
                    TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
                    TaskRun run = taskRunScheduler.getRunnableTaskRun(taskId);
                    Assert.assertEquals(Constants.TaskRunPriority.HIGHEST.value(), run.getStatus().getPriority());

                    while (taskRunScheduler.getRunningTaskCount() > 0) {
                        Thread.sleep(100);
                    }
                    starRocksAssert.dropMaterializedView("mv_refresh_priority");
                }
        );
    }

    @Test
    public void testRewriteNonPartitionedMVForOlapTable() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("create materialized view mv_single_for_olap " +
                        "refresh async " +
                        "as select * from tbl1");
        String mvName = "mv_single_for_olap";
        refreshMVRange(mvName, false);
        String querySql = "select * from tbl1";
        starRocksAssert.query(querySql).explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @NotNull
    private MaterializedView refreshMaterializedView(String materializedViewName, String start, String end) throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable(materializedViewName));
        refreshMVRange(materializedView.getName(), start, end, false);
        return materializedView;
    }

    @Test
    public void testMvWithoutPartitionRefreshTwice() throws Exception {
        final AtomicInteger taskRunCounter = new AtomicInteger();
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                taskRunCounter.incrementAndGet();
            }
        };
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_without_partition"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        String insertSql = "insert into tbl3 values('2021-12-01', 2, 10);";
        executeInsertSql(connectContext, insertSql);

        try {
            for (int i = 0; i < 2; i++) {
                initAndExecuteTaskRun(taskRun);
            }
            Assert.assertEquals(1, taskRunCounter.get());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("refresh failed");
        }
    }

    @Test
    public void testMVClearQueryInfo() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_without_partition"));
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                UUID uuid = UUID.randomUUID();
                TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                TUniqueId queryId = UUIDUtil.toTUniqueId(execPlan.getConnectContext().getQueryId());
                System.out.println("register query id: " + DebugUtil.printId(queryId));
                LoadPlanner loadPlanner = new LoadPlanner(1, loadId, 1, 1, materializedView,
                        false, "UTC", 10, System.currentTimeMillis(),
                        false, connectContext, null, 10,
                        10, null, null, null, 1);
                DefaultCoordinator coordinator =
                        new DefaultCoordinator.Factory().createBrokerLoadScheduler(loadPlanner);
                QeProcessorImpl.INSTANCE.registerQuery(queryId, coordinator);
            }
        };
        String insertSql = "insert into tbl3 values('2021-12-01', 2, 10);";
        executeInsertSql(connectContext, insertSql);
        refreshMVRange(materializedView.getName(), false);
        System.out.println("unregister query id: " + DebugUtil.printId(connectContext.getExecutionId()));
        Assert.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(connectContext.getExecutionId()));
    }

    private void testBaseTablePartitionInsertData(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        String insertSql = "insert into tbl1 partition(p0) values('2021-12-01', 2, 10);";
        executeInsertSql(connectContext, insertSql);
        insertSql = "insert into tbl1 partition(p1) values('2022-01-01', 2, 10);";
        executeInsertSql(connectContext, insertSql);

        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        initAndExecuteTaskRun(taskRun);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo basePartitionInfo = baseTableVisibleVersionMap.get(tbl1.getId()).get("p0");
        Assert.assertEquals(2, basePartitionInfo.getVersion());
        // insert new data into tbl1's p0 partition
        // update base table tbl1's p0 version to 2
        insertSql = "insert into tbl1 partition(p0) values('2021-12-01', 2, 10);";
        executeInsertSql(connectContext, insertSql);

        initAndExecuteTaskRun(taskRun);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap2 =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo newP0PartitionInfo = baseTableVisibleVersionMap2.get(tbl1.getId()).get("p0");
        Assert.assertEquals(3, newP0PartitionInfo.getVersion());

        MaterializedView.BasePartitionInfo p1PartitionInfo = baseTableVisibleVersionMap2.get(tbl1.getId()).get("p1");
        Assert.assertEquals(2, p1PartitionInfo.getVersion());
    }

    private void testRefreshWithFailure(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        // insert new data into tbl1's p0 partition
        // update base table tbl1's p0 version to 3
        String insertSql = "insert into tbl1 partition(p0) values('2021-12-01', 2, 10);";
        executeInsertSql(connectContext, insertSql);

        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public void processTaskRun(TaskRunContext context) throws Exception {
                throw new RuntimeException("new exception");
            }
        };
        try {
            initAndExecuteTaskRun(taskRun);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap2 =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo newP0PartitionInfo = baseTableVisibleVersionMap2.get(tbl1.getId()).get("p0");
        Assert.assertEquals(3, newP0PartitionInfo.getVersion());
    }

    public void testBaseTablePartitionRename(TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p1, p1 renamed with p10 after collect and before insert overwrite
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            private Map<Long, TableSnapshotInfo> collectBaseTableSnapshotInfos(MaterializedView materializedView) {
                Map<Long, TableSnapshotInfo> olapTables = Maps.newHashMap();
                List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();

                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    Optional<Table> tableOptional = MvUtils.getTableWithIdentifier(baseTableInfo);
                    if (tableOptional.isEmpty() || !tableOptional.get().isOlapTable()) {
                        continue;
                    }
                    Database baseDb = GlobalStateMgr.getCurrentState().getDb(baseTableInfo.getDbId());
                    if (baseDb == null) {
                        throw new SemanticException("Materialized view base db: " +
                                baseTableInfo.getDbId() + " not exist.");
                    }
                    OlapTable olapTable = (OlapTable) baseDb.getTable(baseTableInfo.getTableId());
                    if (olapTable == null) {
                        throw new SemanticException("Materialized view base table: " +
                                baseTableInfo.getTableId() + " not exist.");
                    }
                    OlapTable copied = new OlapTable();
                    if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                        throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                    }
                    olapTables.put(olapTable.getId(), new TableSnapshotInfo(baseTableInfo, copied));
                }

                String renamePartitionSql = "ALTER TABLE test.tbl1 RENAME PARTITION p1 p1_1";
                try {
                    // will fail when retry in second times
                    new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                            renamePartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };
        // insert new data into tbl1's p1 partition
        // update base table tbl1's p1 version to 2
        String insertSql = "insert into tbl1 partition(p1) values('2022-01-01', 2, 10);";
        executeInsertSql(connectContext, insertSql);
        try {
            initAndExecuteTaskRun(taskRun);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is not active, skip sync partition and data with base tables"));
        }
    }

    private void testBaseTablePartitionReplace(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p2, p2 replace with tp2 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public Map<Long, TableSnapshotInfo> collectBaseTableSnapshotInfos(
                    MaterializedView materializedView) {
                Map<Long, TableSnapshotInfo> olapTables = Maps.newHashMap();
                List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTableChecked(baseTableInfo);
                    if (!table.isOlapTable()) {
                        continue;
                    }
                    Database baseDb = GlobalStateMgr.getCurrentState().getDb(baseTableInfo.getDbId());
                    if (baseDb == null) {
                        throw new SemanticException("Materialized view base db: " +
                                baseTableInfo.getDbId() + " not exist.");
                    }
                    OlapTable olapTable = (OlapTable) baseDb.getTable(baseTableInfo.getTableId());
                    if (olapTable == null) {
                        throw new SemanticException("Materialized view base table: " +
                                baseTableInfo.getTableId() + " not exist.");
                    }
                    OlapTable copied = new OlapTable();
                    if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                        throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                    }
                    olapTables.put(olapTable.getId(), new TableSnapshotInfo(baseTableInfo, olapTable));
                }

                try {
                    String replacePartitionSql =
                            "ALTER TABLE test.tbl1 REPLACE PARTITION (p3) WITH TEMPORARY PARTITION (tp3)\n" +
                                    "PROPERTIES (\n" +
                                    "    \"strict_range\" = \"false\",\n" +
                                    "    \"use_temp_partition_name\" = \"false\"\n" +
                                    ");";
                    new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                            replacePartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
                    String insertSql = "insert into tbl1 partition(p3) values('2021-03-01', 2, 10);";
                    executeInsertSql(connectContext, insertSql);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };
        // change partition and replica versions
        Partition partition = tbl1.getPartition("p3");
        String createTempPartitionSql =
                "ALTER TABLE test.tbl1 ADD TEMPORARY PARTITION tp3 values [('2022-03-01'),('2022-04-01'))";
        new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                createTempPartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
        String insertSql = "insert into tbl1 partition(p3) values('2021-03-01', 2, 10);";
        executeInsertSql(connectContext, insertSql);
        initAndExecuteTaskRun(taskRun);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo basePartitionInfo = baseTableVisibleVersionMap.get(tbl1.getId()).get("p3");
        Assert.assertNotEquals(partition.getId(), basePartitionInfo.getId());
    }

    public void testBaseTableAddPartitionWhileSync(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p3, add partition p99 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public Map<Long, TableSnapshotInfo> collectBaseTableSnapshotInfos(MaterializedView materializedView) {
                Map<Long, TableSnapshotInfo> olapTables = Maps.newHashMap();
                List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    Optional<Table> tableOptional = MvUtils.getTableWithIdentifier(baseTableInfo);
                    if (tableOptional.isEmpty()) {
                        continue;
                    }
                    if (!tableOptional.get().isOlapTable()) {
                        continue;
                    }
                    Database baseDb = GlobalStateMgr.getCurrentState().getDb(baseTableInfo.getDbId());
                    if (baseDb == null) {
                        throw new SemanticException("Materialized view base db: " +
                                baseTableInfo.getDbId() + " not exist.");
                    }
                    OlapTable olapTable = (OlapTable) baseDb.getTable(baseTableInfo.getTableId());
                    if (olapTable == null) {
                        throw new SemanticException("Materialized view base table: " +
                                baseTableInfo.getTableId() + " not exist.");
                    }
                    OlapTable copied = new OlapTable();
                    if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                        throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                    }
                    olapTables.put(olapTable.getId(), new TableSnapshotInfo(baseTableInfo, copied));
                }

                String addPartitionSql =
                        "ALTER TABLE test.tbl1 ADD PARTITION p99 VALUES [('9999-03-01'),('9999-04-01'))";
                try {
                    new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                            addPartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                String insertSql = "insert into tbl1 partition(p99) values('9999-03-01', 2, 10);";
                try {
                    executeInsertSql(connectContext, insertSql);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 2, 10);";
        executeInsertSql(connectContext, insertSql);
        initAndExecuteTaskRun(taskRun);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(3, baseTableVisibleVersionMap.get(tbl1.getId()).get("p3").getVersion());
        Assert.assertNotNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p99"));
        Assert.assertEquals(2, baseTableVisibleVersionMap.get(tbl1.getId()).get("p99").getVersion());
    }

    public void testBaseTableAddPartitionWhileRefresh(Database testDb, MaterializedView materializedView,
                                                      TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p3, add partition p99 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan,
                                                InsertStmt insertStmt) throws Exception {
                String addPartitionSql =
                        "ALTER TABLE test.tbl1 ADD PARTITION p100 VALUES [('9999-04-01'),('9999-05-01'))";
                String insertSql = "insert into tbl1 partition(p100) values('9999-04-01', 3, 10);";
                try {
                    new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                            addPartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
                    executeInsertSql(connectContext, insertSql);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ConnectContext ctx = mvContext.getCtx();
                StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
                ctx.setExecutor(executor);
                ctx.setThreadLocalInfo();
                ctx.setStmtId(new AtomicInteger().incrementAndGet());
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
                executor.handleDMLStmt(execPlan, insertStmt);
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        executeInsertSql(connectContext, insertSql);

        initAndExecuteTaskRun(taskRun);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(4, baseTableVisibleVersionMap.get(tbl1.getId()).get("p3").getVersion());
        Assert.assertNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p100"));
    }

    public void testBaseTableDropPartitionWhileSync(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p4, drop partition p4 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            private Map<Long, TableSnapshotInfo> collectBaseTableSnapshotInfos(MaterializedView materializedView) {
                Map<Long, TableSnapshotInfo> olapTables = Maps.newHashMap();
                List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    Table table = MvUtils.getTableChecked(baseTableInfo);
                    if (!table.isOlapTable()) {
                        continue;
                    }
                    Database baseDb = GlobalStateMgr.getCurrentState().getDb(baseTableInfo.getDbId());
                    if (baseDb == null) {
                        throw new SemanticException("Materialized view base db: " +
                                baseTableInfo.getDbId() + " not exist.");
                    }
                    OlapTable olapTable = (OlapTable) baseDb.getTable(baseTableInfo.getTableId());
                    if (olapTable == null) {
                        throw new SemanticException("Materialized view base table: " +
                                baseTableInfo.getTableId() + " not exist.");
                    }
                    OlapTable copied = new OlapTable();
                    if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                        throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                    }
                    olapTables.put(olapTable.getId(), new TableSnapshotInfo(baseTableInfo, copied));
                }

                String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p4";
                try {
                    new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                            dropPartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        executeInsertSql(connectContext, insertSql);

        initAndExecuteTaskRun(taskRun);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p4"));
    }

    public void testBaseTableDropPartitionWhileRefresh(Database testDb, MaterializedView materializedView,
                                                       TaskRun taskRun)
            throws Exception {
        // drop partition p4 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan,
                                                InsertStmt insertStmt) throws Exception {
                String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p100";
                try {
                    new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                            dropPartitionSql, connectContext.getSessionVariable().getSqlMode())).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ConnectContext ctx = mvContext.getCtx();
                StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
                ctx.setExecutor(executor);
                ctx.setThreadLocalInfo();
                ctx.setStmtId(new AtomicInteger().incrementAndGet());
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
                executor.handleDMLStmt(execPlan, insertStmt);
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        executeInsertSql(connectContext, insertSql);

        initAndExecuteTaskRun(taskRun);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertNotNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p100"));
        Assert.assertEquals(3, baseTableVisibleVersionMap.get(tbl1.getId()).get("p100").getVersion());
    }

    @Test
    public void testFilterPartitionByRefreshNumber() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_with_test_refresh"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        materializedView.getTableProperty().setPartitionRefreshNumber(3);
        PartitionBasedMvRefreshProcessor processor = new PartitionBasedMvRefreshProcessor();

        MvTaskRunContext mvContext = new MvTaskRunContext(new TaskRunContext());

        processor.setMvContext(mvContext);
        processor.filterPartitionByRefreshNumber(materializedView.getPartitionNames(), Sets.newHashSet(), materializedView);

        mvContext = processor.getMvContext();
        Assert.assertEquals("2022-03-01", mvContext.getNextPartitionStart());
        Assert.assertEquals("2022-05-01", mvContext.getNextPartitionEnd());

        initAndExecuteTaskRun(taskRun);

        processor.filterPartitionByRefreshNumber(Sets.newHashSet(), Sets.newHashSet(), materializedView);
        mvContext = processor.getMvContext();
        Assert.assertNull(mvContext.getNextPartitionStart());
        Assert.assertNull(mvContext.getNextPartitionEnd());
    }

    @Test
    public void testRefreshMaterializedViewDefaultConfig1() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("create materialized view test.mv_config1\n" +
                "partition by date_trunc('month',k1) \n" +
                "distributed by hash(k2) buckets 10\n" +
                "refresh deferred manual\n" +
                "properties(" +
                "'replication_num' = '1',\n" +
                "'storage_medium' = 'SSD'" +
                ")\n" +
                "as select k1, k2 from tbl1;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_config1"));

        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        executeInsertSql(connectContext, insertSql);

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        // by default, enable spill
        {
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            Assert.assertTrue(execPlan.getConnectContext().getSessionVariable().isEnableSpill());
        }

        {
            // change global config
            Config.enable_materialized_view_spill = false;

            // insert again.
            executeInsertSql(connectContext, insertSql);
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            Assert.assertFalse(execPlan.getConnectContext().getSessionVariable().isEnableSpill());

            Config.enable_materialized_view_spill = true;
        }
    }

    @Test
    public void testRefreshMaterializedViewDefaultConfig2() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("create materialized view test.mv_config2\n" +
                "partition by date_trunc('month',k1) \n" +
                "distributed by hash(k2) buckets 10\n" +
                "refresh deferred manual\n" +
                "properties(" +
                "'replication_num' = '1',\n" +
                "'session.enable_spill' = 'false'" +
                ")\n" +
                "as select k1, k2 from tbl1;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_config2"));

        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        executeInsertSql(connectContext, insertSql);

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();
        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        Assert.assertFalse(execPlan.getConnectContext().getSessionVariable().isEnableSpill());
    }

    @Test
    public void testSyncPartitionWithSsdStorage() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("create materialized view test.mv_with_ssd\n" +
                "partition by date_trunc('month',k1) \n" +
                "distributed by hash(k2) buckets 10\n" +
                "refresh deferred manual\n" +
                "properties('replication_num' = '1',\n" +
                "'storage_medium' = 'SSD')\n" +
                "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_with_ssd"));

        refreshMVRange(materializedView.getName(), false);
        refreshMVRange(materializedView.getName(), false);
    }

    @Test
    public void testSyncPartitionWithSsdStorageAndCooldownTime() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_use_ssd_and_cooldown\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1',\n" +
                        "'storage_medium' = 'SSD',\n" +
                        "'storage_cooldown_time' = '2222-04-21 20:45:11')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_use_ssd_and_cooldown"));
        refreshMVRange(materializedView.getName(), false);
        refreshMVRange(materializedView.getName(), false);
    }

    @Test
    public void testMVOnListPartitionTables1() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        String createSQL = "CREATE TABLE test.list_partition_tbl1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";
        starRocksAssert.withTable(createSQL);

        String sql = "create materialized view list_partition_mv1 " +
                "refresh deferred manual\n" +
                "distributed by hash(dt, province) buckets 10 " +
                "as select dt, province, avg(age) from list_partition_tbl1 group by dt, province;";
        starRocksAssert.withMaterializedView(sql);
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("list_partition_mv1"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        // run 1
        {
            // just refresh to avoid dirty data
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            Assert.assertTrue(execPlan == null);
        }

        // run 2
        {
            String insertSql = "INSERT INTO list_partition_tbl1 VALUES (1, 1, '2023-08-15', 'beijing');";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            Assert.assertTrue(plan.contains("partitions=2/2\n" +
                    "     rollup: list_partition_tbl1"));
        }

        // run 3
        {
            // just refresh to avoid dirty data
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            Assert.assertTrue(execPlan == null);
        }

        starRocksAssert.dropMaterializedView("list_partition_mv1");
        starRocksAssert.dropTable("list_partition_tbl1");
    }

    @Test
    public void testPartitionPruneNonRefBaseTable1() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`partition_prune_non_ref_tables1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`k1`)\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT t1.k1, sum(t1.k2) as sum1, avg(t2.k2) as avg1 FROM tbl4 as t1 join " +
                        "tbl5 t2 on t1.k1=t2.dt group by t1.k1");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("partition_prune_non_ref_tables1"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        // run 1
        {
            // just refresh to avoid dirty data
            initAndExecuteTaskRun(taskRun);
        }

        // run 2
        {
            // base table partition insert data
            String insertSql = "insert into tbl4 partition(p4) values('2022-04-01', 3, 10);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            Assert.assertFalse(plan.contains("partitions=5/5"));
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl5"));
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl4"));
        }

        // run 3
        {
            String insertSql = "insert into tbl5 partition(p4) values('2022-04-01', '2021-04-01 00:02:11', 3, 10);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl5"));
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl4"));
        }
        starRocksAssert.dropMaterializedView("partition_prune_non_ref_tables1");
    }

    @Test
    public void testPartitionPruneNonRefBaseTable2() throws Exception {
        // partition column with alias
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`partition_prune_non_ref_tables2`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`k11`)\n" +
                        "DISTRIBUTED BY HASH(`k11`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT t1.k1 as k11, sum(t1.k2) as sum1, avg(t2.k2) as avg1 FROM tbl4 as t1 join " +
                        "tbl5 t2 on t1.k1=t2.dt group by t1.k1");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("partition_prune_non_ref_tables2"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        // run 1
        {
            // just refresh to avoid dirty data
            initAndExecuteTaskRun(taskRun);
        }

        // run 2
        {
            // base table partition insert data
            String insertSql = "insert into tbl4 partition(p4) values('2022-04-01', 3, 10);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            Assert.assertFalse(plan.contains("partitions=5/5"));
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl5"));
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl4"));
        }

        // run 3
        {
            String insertSql = "insert into tbl5 partition(p4) values('2022-04-01', '2021-04-01 00:02:11', 3, 10);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl5"));
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl4"));
        }
        starRocksAssert.dropMaterializedView("partition_prune_non_ref_tables2");
    }

    @Test
    public void testPartitionPruneNonRefBaseTable3() throws Exception {
        // mv with predicates
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`partition_prune_non_ref_tables1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`k1`)\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT t1.k1, sum(t1.k2) as sum1, avg(t2.k2) as avg1 FROM tbl4 as t1 join " +
                        "tbl5 t2 on t1.k1=t2.dt where t1.k1>'2022-01-01' and t1.k2>0 group by t1.k1");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("partition_prune_non_ref_tables1"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        // run 1
        {
            // just refresh to avoid dirty data
            initAndExecuteTaskRun(taskRun);
        }

        // run 2
        {
            // base table partition insert data
            String insertSql = "insert into tbl4 partition(p4) values('2022-04-01', 3, 10);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            Assert.assertFalse(plan.contains("partitions=5/5"));
            Assert.assertTrue(plan.contains("PREDICATES: 2: k2 > 0\n" +
                    "     partitions=1/5\n" +
                    "     rollup: tbl4"));
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl5"));
        }

        // run 3
        {
            String insertSql = "insert into tbl5 partition(p4) values('2022-04-01', '2021-04-01 00:02:11', 3, 10);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            Assert.assertTrue(plan.contains("PREDICATES: 2: k2 > 0\n" +
                    "     partitions=1/5\n" +
                    "     rollup: tbl4"));
            Assert.assertTrue(plan.contains("partitions=1/5\n" +
                    "     rollup: tbl5"));
        }
        starRocksAssert.dropMaterializedView("partition_prune_non_ref_tables1");
    }

    @Test
    public void testMVDropBaseVersionMetaOfOlapTable() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view test_drop_partition_mv1\n" +
                "PARTITION BY k1\n" +
                "distributed by hash(k2) buckets 3\n" +
                "refresh async \n" +
                "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;");

        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        Table tbl1 = testDb.getTable("tbl1");
        MaterializedView mv = ((MaterializedView) testDb.getTable("test_drop_partition_mv1"));
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getMvPartitionNameRefBaseTablePartitionMap();
        Map<String, MaterializedView.BasePartitionInfo> tableMap = Maps.newHashMap();
        // case1: version map cannot decide whether it's safe to drop p1, drop the table from version map.
        {
            tableMap.put("p1", new MaterializedView.BasePartitionInfo(1, 2, -1));
            tableMap.put("p2", new MaterializedView.BasePartitionInfo(3, 4, -1));
            versionMap.put(tbl1.getId(), tableMap);

            SyncPartitionUtils.dropBaseVersionMeta(mv, "p1", null);
            Assert.assertFalse(versionMap.containsKey(tbl1.getId()));
        }
        {
            tableMap.put("p1", new MaterializedView.BasePartitionInfo(1, 2, -1));
            tableMap.put("p2", new MaterializedView.BasePartitionInfo(3, 4, -1));
            versionMap.put(tbl1.getId(), tableMap);

            mvPartitionNameRefBaseTablePartitionMap.put("p1", Sets.newHashSet("p1"));
            mvPartitionNameRefBaseTablePartitionMap.put("p2", Sets.newHashSet("p2"));

            SyncPartitionUtils.dropBaseVersionMeta(mv, "p1", null);
            Assert.assertTrue(versionMap.containsKey(tbl1.getId()));
            Assert.assertTrue(tableMap.containsKey("p2"));
        }
        {
            tableMap.put("p1", new MaterializedView.BasePartitionInfo(1, 2, -1));
            tableMap.put("p2", new MaterializedView.BasePartitionInfo(3, 4, -1));
            versionMap.put(tbl1.getId(), tableMap);

            mvPartitionNameRefBaseTablePartitionMap.put("p1", Sets.newHashSet("p1"));
            mvPartitionNameRefBaseTablePartitionMap.put("p2", Sets.newHashSet("p2"));

            SyncPartitionUtils.dropBaseVersionMeta(mv, "p3", null);
            Assert.assertTrue(versionMap.containsKey(tbl1.getId()));
            Assert.assertTrue(tableMap.containsKey("p2"));
        }
        starRocksAssert.dropMaterializedView("test_drop_partition_mv1");
    }

    private Map<Table, Set<String>> getRefTableRefreshedPartitions(PartitionBasedMvRefreshProcessor processor) {
        Map<TableSnapshotInfo, Set<String>> baseTables = processor
                .getRefTableRefreshPartitions(Sets.newHashSet("p20220101"));
        Assert.assertEquals(2, baseTables.size());
        return baseTables.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getBaseTable(), x -> x.getValue()));
    }

    @Test
    public void testFilterPartitionByJoinPredicate1() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        //normal case, join predicate is partition column
        // a.k1 = date_trunc(month, b.k1)
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1 \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '10000')" +
                        "refresh deferred manual\n" +
                        "as select a.k1, b.k2 from test.tbl15 as a join test.tbl16 as b " +
                        "on a.k1 = date_trunc('month', b.k1);");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_join_predicate"));
        Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        {
            taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
            taskRun.executeTaskRun();
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
            Map<Table, Set<String>> baseTables = getRefTableRefreshedPartitions(processor);
            Assert.assertEquals(2, baseTables.size());
            Assert.assertEquals(Sets.newHashSet("p20220101"), baseTables.get(testDb.getTable("tbl15")));
            Assert.assertEquals(Sets.newHashSet("p20220101", "p20220102", "p20220103"),
                    baseTables.get(testDb.getTable("tbl16")));
            Assert.assertTrue(processor.getNextTaskRun() == null);
        }

        {
            // insert new data into tbl16's p20220202 partition
            String insertSql = "insert into tbl16 partition(p20220202) values('2022-02-02', 3, 10);";
            new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                    insertSql, connectContext.getSessionVariable().getSqlMode())).execute();
            taskRun.executeTaskRun();
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
            // 1. updated partition of tbl16 is p20220202
            // 2. date_trunc('month', p20220202) is '2022-02'
            // 3. tbl15's associated partitions are p20220201 and p20220202
            Assert.assertEquals(Sets.newHashSet("p20220202", "p20220201"),
                    processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
            Assert.assertEquals("{tbl15=[p20220202, p20220201], tbl16=[p20220202, p20220201]}",
                    processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
        }
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_join_predicate");
    }

    @Test
    public void testFilterPartitionByJoinPredicate3() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        // join predicate has no equal condition
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1 \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '10000')" +
                        "refresh deferred manual\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1  join tbl2 on tbl1.k1 = tbl2.k1 or tbl1.k2 = tbl2.k2;");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_join_predicate"));
        Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        {
            executeInsertSql(connectContext, "insert into tbl1 partition(p2) values('2022-02-02', 3, 10);");
            taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
            taskRun.executeTaskRun();
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
            Assert.assertEquals(Sets.newHashSet("p2"), processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
            ExecPlan execPlan = processor.getMvContext().getExecPlan();
            Assert.assertTrue(execPlan != null);
            Assert.assertEquals("{tbl1=[p2]}",
                    processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
        }

        {
            executeInsertSql(connectContext, "insert into tbl2 partition(p2) values('2022-02-02', 3, 10);");
            taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
            taskRun.executeTaskRun();
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
            ExecPlan execPlan = processor.getMvContext().getExecPlan();
            Assert.assertTrue(execPlan != null);
            assertPlanContains(execPlan, "partitions=5/5\n     rollup: tbl1", "partitions=2/2\n     rollup: tbl2");
            starRocksAssert.useDatabase("test").dropMaterializedView("mv_join_predicate");
        }
    }

    @Test
    public void testFilterPartitionByJoinPredicate31() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        // join predicate is not mv partition expr
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '100')" +
                        "refresh deferred manual\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1  join tbl2 using(k1);");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_join_predicate"));
        Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_join_predicate");
    }

    @Test
    public void testFilterPartitionByJoinPredicate4() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        // nest table alias join
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by date_trunc('month', k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "as select a.k1, b.k2 from test.tbl15 as a join test.tbl16 as b " +
                        "on date_trunc('month', a.k1) = b.k1;");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_join_predicate"));
        Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                "insert into tbl15 partition(p20220202) values('2022-02-02', 3, 10);",
                connectContext.getSessionVariable().getSqlMode())).execute();
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        Assert.assertEquals(Sets.newHashSet("p202202_202203"), processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
        Assert.assertEquals("{tbl15=[p20220202, p20220201], tbl16=[p20220202, p20220201]}",
                processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_join_predicate");
    }

    @Test
    public void testFilterPartitionByJoinPredicate5() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        // nest table alias join
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '100')" +
                        "refresh deferred manual\n" +
                        "as select a.ds as k1, a.k2 from" +
                        "(select date_trunc('DAY', k1) as ds, k1, k2 from (select k1, k2 from " +
                        "(select * from tbl1)t1 )t2 ) a left join " +
                        "(select date_trunc('DAY', k1) as ds, k2 from (select * from tbl2)t ) b " +
                        "on date_trunc('DAY', a.k1) = b.ds and a.k2 = b.k2");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_join_predicate"));
        Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        executeInsertSql(connectContext, "insert into tbl2 partition(p1) values('2022-01-02', 3, 10);");
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        Assert.assertEquals(Sets.newHashSet("p20220101_20220201"),
                processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
        Map<String, Set<String>> refBasePartitionsToRefreshMap =
                processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap();
        Map<String, String> expect = ImmutableMap.of(
                "tbl1", "[p1]",
                "tbl2", "[p1]",
                "tbl15", "[p20220101, p20220102, p20220103]"
        );
        for (Map.Entry<String, Set<String>> e : refBasePartitionsToRefreshMap.entrySet()) {
            String k = e.getKey();
            Set<String> v = Sets.newTreeSet(e.getValue());
            Assert.assertEquals(expect.get(k), v.toString());
        }
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_join_predicate");
    }

    @Test
    public void testFilterPartitionByJoinPredicate6() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        // nest function in join predicate
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '100')" +
                        "refresh deferred manual\n" +
                        "as select a.ds as k1, a.k2 from" +
                        "(select date_trunc('day', k1) as ds, k2 from tbl1) a " +
                        "left join " +
                        "(select k1 as ds, k2 from tbl2) b " +
                        "on date_trunc('day', a.ds) = b.ds and a.k2 = b.k2 ;");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_join_predicate"));
        Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_join_predicate");
    }

    @Test
    public void testFilterPartitionByJoinPredicate7() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        // duplicate table join
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '100')" +
                        "refresh deferred manual\n" +
                        "as select a.ds as k1, a.k2 from" +
                        "(select k1 as ds, k2 from tbl1) a left join " +
                        "(select k1 as ds, k2 from tbl2) b " +
                        "on a.ds = b.ds and a.k2 = b.k2 left join " +
                        "(select k1 as ds, k2 from tbl2) c " +
                        "on a.ds = c.ds and a.k2 = c.k2;");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_join_predicate"));
        Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_join_predicate");
    }

    @Test
    public void testFilterPartitionByJoinPredicate8() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '100')" +
                        "refresh deferred manual\n" +
                        "as select a.ds as k1, a.k2 from" +
                        "(select date_trunc('day', k1) as ds, k2 from tbl1) a " +
                        "left join " +
                        "(select date_trunc('day', k1) as ds, k2 from tbl1) b " +
                        "on a.ds = b.ds and a.k2 = b.k2 " +
                        "left join " +
                        "(select date_trunc('day', k1) as ds, k2 from tbl1) c " +
                        "on a.ds = c.ds;");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_join_predicate"));
        Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_join_predicate");
    }

    @Test
    public void testFilterPartitionByJoinPredicate9() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        // unsupported function in join predicate
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '100')" +
                        "refresh deferred manual\n" +
                        "as select a.ds as k1, a.k2 from" +
                        "(select k1 as ds, k2 from tbl1) a left join " +
                        "(select k1 as ds, k2 from tbl2) b " +
                        "on a.ds = b.ds and a.k2 = b.k2 left join " +
                        "(select date_add(k1, INTERVAL 1 DAY) as ds, k2 from tbl15) c " +
                        "on a.ds = c.ds and a.k2 = c.k2;");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_join_predicate"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        executeInsertSql(connectContext, "insert into tbl2 partition(p1) values('2022-01-02', 3, 10);");
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        Assert.assertEquals(Sets.newHashSet("p1"), processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
        Assert.assertEquals("{tbl2=[p1], tbl1=[p1]}",
                processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_join_predicate");
    }

    @Test
    public void testFilterPartitionByUnion() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_union_filter\n" +
                        "partition by k1 \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '10000')\n" +
                        "refresh deferred manual\n" +
                        "as select k1, k2 from test.tbl15 \n" +
                        "union " +
                        "select k1, k2 from test.tbl16;");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_union_filter"));
        Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        Map<Table, Set<String>> baseTables = getRefTableRefreshedPartitions(processor);
        Assert.assertEquals(2, baseTables.size());
        Assert.assertEquals(Sets.newHashSet("p20220101"), baseTables.get(testDb.getTable("tbl15")));
        Assert.assertEquals(Sets.newHashSet("p20220101"), baseTables.get(testDb.getTable("tbl16")));

        // insert new data into tbl16's p20220202 partition
        String insertSql = "insert into tbl16 partition(p20220202) values('2022-02-02', 3, 10);";
        new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                insertSql, connectContext.getSessionVariable().getSqlMode())).execute();
        taskRun.executeTaskRun();
        Assert.assertEquals(Sets.newHashSet("p20220202"), processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
        Assert.assertEquals("{tbl15=[p20220202], tbl16=[p20220202]}",
                processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_union_filter");

        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_union_filter\n" +
                        "partition by k1 \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '10000')\n" +
                        "refresh deferred manual\n" +
                        "as " +
                        "select date_trunc('month', k1) as k1, k2 from test.tbl16\n" +
                        "union " +
                        "select k1, k2 from test.tbl15;");
        materializedView = ((MaterializedView) testDb.getTable("mv_union_filter"));
        Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());

        task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        // insert new data into tbl16's p20220202 partition
        insertSql = "insert into tbl16 partition(p20220202) values('2022-02-02', 3, 10);";
        new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                insertSql, connectContext.getSessionVariable().getSqlMode())).execute();
        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        Assert.assertEquals(Sets.newHashSet("p202202_202203"), processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
        Assert.assertEquals("{tbl15=[p20220202, p20220201], tbl16=[p20220202, p20220201]}",
                processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
        starRocksAssert.useDatabase("test").dropMaterializedView("mv_union_filter");
    }

    @Test
    public void testFilterPartitionByJoinPredicate_RefreshPartitionNum() {
        starRocksAssert.withTables(List.of(
                        new MTable("tt1", "k1",
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
                        new MTable("tt2", "k1",
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
                        )
                ),
                () -> {
                    starRocksAssert
                            .withMaterializedView("create materialized view mv_with_join0\n" +
                                    "partition by k1\n" +
                                    "distributed by hash(k2) buckets 10\n" +
                                    "PROPERTIES('partition_refresh_number' = '1')" +
                                    "refresh deferred manual\n" +
                                    "as select a.k1, b.k2 from tt1 a join tt2 b on a.k1=b.k1;");
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

                    MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_with_join0"));
                    Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());
                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    Map<String, String> testProperties = task.getProperties();
                    testProperties.put(TaskRun.IS_TEST, "true");

                    OlapTable tbl1 = (OlapTable) testDb.getTable("tt1");
                    OlapTable tbl2 = (OlapTable) testDb.getTable("tt2");
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                    taskRun.executeTaskRun();

                    executeInsertSql(connectContext, "insert into tt1 partition(p1) values('2022-01-02', 3, 10);");
                    executeInsertSql(connectContext, "insert into tt1 partition(p2) values('2022-02-03', 3, 10);");
                    {
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        taskRun.executeTaskRun();
                        MvTaskRunContext mvContext = ((PartitionBasedMvRefreshProcessor) taskRun.getProcessor()).getMvContext();
                        Assert.assertTrue(mvContext.hasNextBatchPartition());
                        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                        System.out.println(processor.getMVTaskRunExtraMessage());
                        Assert.assertEquals(Sets.newHashSet("p1"),
                                processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());

                        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                                materializedView.getRefreshScheme().getAsyncRefreshContext();
                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                                asyncRefreshContext.getBaseTableVisibleVersionMap();
                        System.out.println(baseTableVisibleVersionMap);

                        Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl1.getId()));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl1.getId()).containsKey("p1"));
                        Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl2.getId()));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl2.getId()).containsKey("p1"));
                        taskRun = processor.getNextTaskRun();
                        Assert.assertTrue(taskRun != null);
                    }

                    {
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        taskRun.executeTaskRun();
                        MvTaskRunContext mvContext = ((PartitionBasedMvRefreshProcessor) taskRun.getProcessor()).getMvContext();
                        Assert.assertTrue(!mvContext.hasNextBatchPartition());
                        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                        System.out.println(processor.getMVTaskRunExtraMessage());
                        Assert.assertEquals(Sets.newHashSet("p2"),
                                processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());

                        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                                materializedView.getRefreshScheme().getAsyncRefreshContext();
                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                                asyncRefreshContext.getBaseTableVisibleVersionMap();
                        System.out.println(baseTableVisibleVersionMap);

                        Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl1.getId()));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl1.getId()).containsKey("p1"));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl1.getId()).containsKey("p2"));
                        Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl2.getId()));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl2.getId()).containsKey("p1"));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl1.getId()).containsKey("p2"));
                        taskRun = processor.getNextTaskRun();
                        Assert.assertTrue(taskRun == null);
                    }
                    starRocksAssert.dropMaterializedView("mv_with_join0");
                }
        );
    }

    @Test
    public void testFilterPartitionByJoinPredicateWithNonPartitionTable() {
        starRocksAssert.withTables(List.of(
                        new MTable("tt1", "k1",
                                List.of(
                                        "k1 date",
                                        "k2 int",
                                        "v1 int"
                                ),

                                "k1",
                                List.of(
                                        "PARTITION p0 values [('2021-12-01'),('2022-01-01'))",
                                        "PARTITION p1 values [('2022-01-01'),('2022-02-01'))",
                                        "PARTITION p2 values [('2022-02-01'),('2022-03-01'))"
                                )
                        ),
                        new MTable("tt2", "k1",
                                List.of(
                                        "k1 date",
                                        "k2 int",
                                        "v1 int"
                                )
                        )
                ),
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view mv_with_join0\n" +
                                    "partition by k1\n" +
                                    "distributed by hash(k2) buckets 3\n" +
                                    "PROPERTIES('partition_refresh_number' = '1')" +
                                    "refresh deferred manual\n" +
                                    "as select a.k1, b.k2 from tt1 a join tt2 b on a.k1=b.k1;",
                            (name) -> {
                                String mvName = (String) name;
                                Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
                                MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                                Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());

                                OlapTable tbl1 = (OlapTable) testDb.getTable("tt1");
                                OlapTable tbl2 = (OlapTable) testDb.getTable("tt2");
                                TaskRun taskRun = buildMVTaskRun(materializedView, TEST_DB_NAME);
                                taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                taskRun.executeTaskRun();

                                executeInsertSql(connectContext, "insert into tt1 partition(p1) values('2022-01-02', 3, 10);");
                                executeInsertSql(connectContext, "insert into tt1 partition(p2) values('2022-02-02', 3, 10);");
                                executeInsertSql(connectContext, "insert into tt2 values('2022-02-02', 3, 10);");

                                List<String> tt1Partitions = ImmutableList.of("p0", "p1", "p2");
                                for (int i = 0; i < tt1Partitions.size(); i++) {
                                    boolean isEnd = (i == tt1Partitions.size() - 1);
                                    String tt1Partition = tt1Partitions.get(i);
                                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                    taskRun.executeTaskRun();
                                    MvTaskRunContext mvContext =
                                            ((PartitionBasedMvRefreshProcessor) taskRun.getProcessor()).getMvContext();
                                    Assert.assertTrue(isEnd ? !mvContext.hasNextBatchPartition()
                                            : mvContext.hasNextBatchPartition());
                                    PartitionBasedMvRefreshProcessor processor =
                                            (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                    System.out.println(processor.getMVTaskRunExtraMessage());
                                    Assert.assertEquals(Sets.newHashSet(tt1Partition),
                                            processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());

                                    MaterializedView.AsyncRefreshContext asyncRefreshContext =
                                            materializedView.getRefreshScheme().getAsyncRefreshContext();
                                    Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                                            asyncRefreshContext.getBaseTableVisibleVersionMap();
                                    System.out.println(baseTableVisibleVersionMap);

                                    Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl1.getId()));
                                    Assert.assertTrue(baseTableVisibleVersionMap.get(tbl1.getId()).containsKey(tt1Partition));
                                    Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl2.getId()));
                                    taskRun = processor.getNextTaskRun();
                                    Assert.assertTrue(isEnd ? taskRun == null : taskRun != null);
                                }
                            }
                    );
                }
        );
    }

    @Test
    public void testFilterPartitionByJoinPredicateWithNonPartition_RefreshPartitionNum() {
        starRocksAssert.withTables(List.of(
                        new MTable("tt1", "k1",
                                List.of(
                                        "k1 date",
                                        "k2 int",
                                        "v1 int"
                                ),

                                "k1",
                                List.of(
                                        "PARTITION p0 values [('2021-12-01'),('2022-01-01'))",
                                        "PARTITION p1 values [('2022-01-01'),('2022-02-01'))"
                                )
                        ),
                        new MTable("tt2", "k1",
                                List.of(
                                        "k1 date",
                                        "k2 int",
                                        "v1 int"
                                )
                        )
                ),
                () -> {
                    starRocksAssert
                            .withMaterializedView("create materialized view mv_with_join0\n" +
                                    "partition by k1\n" +
                                    "distributed by hash(k2) buckets 10\n" +
                                    "PROPERTIES('partition_refresh_number' = '1')" +
                                    "refresh deferred manual\n" +
                                    "as select a.k1, b.k2 from tt1 a join tt2 b on a.k1=b.k1;");
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

                    MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_with_join0"));
                    Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());
                    TaskRun taskRun = buildMVTaskRun(materializedView, TEST_DB_NAME);

                    OlapTable tbl1 = (OlapTable) testDb.getTable("tt1");
                    OlapTable tbl2 = (OlapTable) testDb.getTable("tt2");
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                    taskRun.executeTaskRun();

                    executeInsertSql(connectContext, "insert into tt1 partition(p1) values('2022-01-02', 3, 10);");
                    executeInsertSql(connectContext, "insert into tt1 partition(p2) values('2022-02-02', 3, 10);");
                    executeInsertSql(connectContext, "insert into tt2 values('2022-02-02', 3, 10);");
                    {
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        taskRun.executeTaskRun();
                        MvTaskRunContext mvContext = ((PartitionBasedMvRefreshProcessor) taskRun.getProcessor()).getMvContext();
                        Assert.assertTrue(mvContext.hasNextBatchPartition());
                        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                        System.out.println(processor.getMVTaskRunExtraMessage());
                        Assert.assertEquals(Sets.newHashSet("p0"),
                                processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());

                        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                                materializedView.getRefreshScheme().getAsyncRefreshContext();
                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                                asyncRefreshContext.getBaseTableVisibleVersionMap();
                        System.out.println(baseTableVisibleVersionMap);

                        Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl1.getId()));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl1.getId()).containsKey("p0"));
                        Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl2.getId()));
                        // assert not contain the non-partition table in the 1th task run
                        Assert.assertFalse(baseTableVisibleVersionMap.get(tbl2.getId()).containsKey("tt2"));
                        taskRun = processor.getNextTaskRun();
                        Assert.assertTrue(taskRun != null);
                    }

                    {
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        taskRun.executeTaskRun();
                        MvTaskRunContext mvContext = ((PartitionBasedMvRefreshProcessor) taskRun.getProcessor()).getMvContext();
                        Assert.assertTrue(!mvContext.hasNextBatchPartition());
                        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                        System.out.println(processor.getMVTaskRunExtraMessage());
                        Assert.assertEquals(Sets.newHashSet("p1"),
                                processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());

                        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                                materializedView.getRefreshScheme().getAsyncRefreshContext();
                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                                asyncRefreshContext.getBaseTableVisibleVersionMap();
                        System.out.println(baseTableVisibleVersionMap);

                        Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl1.getId()));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl1.getId()).containsKey("p0"));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl1.getId()).containsKey("p1"));
                        // assert contain the non-partition table in the 1th task run
                        Assert.assertTrue(baseTableVisibleVersionMap.containsKey(tbl2.getId()));
                        Assert.assertTrue(baseTableVisibleVersionMap.get(tbl2.getId()).containsKey("tt2"));
                        taskRun = processor.getNextTaskRun();
                        Assert.assertTrue(taskRun == null);
                    }
                    starRocksAssert.dropMaterializedView("mv_with_join0");
                }
        );
    }

    @Test
    public void testQueryDebugOptions() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        QueryDebugOptions debugOptions = sessionVariable.getQueryDebugOptions();
        Assert.assertEquals(debugOptions.getMaxRefreshMaterializedViewRetryNum(), 3);
        Assert.assertEquals(debugOptions.isEnableNormalizePredicateAfterMVRewrite(), false);
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
                            .withMaterializedView("create materialized view mv_refresh_priority\n" +
                                    "partition by date_trunc('month',k1) \n" +
                                    "distributed by hash(k2) buckets 10\n" +
                                    "refresh deferred manual\n" +
                                    "properties('replication_num' = '1', 'partition_refresh_number'='1')\n" +
                                    "as select k1, k2 from tbl6;");
                    String mvName = "mv_refresh_priority";
                    Database testDb = GlobalStateMgr.getCurrentState().getDb(TEST_DB_NAME);
                    MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                    TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
                    TaskRunManager trm = tm.getTaskRunManager();

                    executeInsertSql(connectContext, "insert into tbl6 partition(p1) values('2022-01-02',2,10);");
                    executeInsertSql(connectContext, "insert into tbl6 partition(p2) values('2022-02-02',2,10);");

                    // refresh materialized view
                    HashMap<String, String> taskRunProperties = new HashMap<>();
                    taskRunProperties.put(TaskRun.FORCE, Boolean.toString(true));
                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                    taskRun.executeTaskRun();

                    // without db name
                    Assert.assertFalse(tm.getMatchedTaskRunStatus(null).isEmpty());
                    Assert.assertFalse(tm.filterTasks(null).isEmpty());
                    Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(null, null).isEmpty());

                    // specific db
                    TGetTasksParams getTasksParams = new TGetTasksParams();
                    getTasksParams.setDb(TEST_DB_NAME);
                    Assert.assertFalse(tm.getMatchedTaskRunStatus(getTasksParams).isEmpty());
                    Assert.assertFalse(tm.filterTasks(getTasksParams).isEmpty());
                    Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(TEST_DB_NAME, null).isEmpty());

                    long taskId = tm.getTask(TaskBuilder.getMvTaskName(materializedView.getId())).getId();
                    TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
                    Assert.assertNotNull(taskRunScheduler.getRunnableTaskRun(taskId));

                    while (taskRunScheduler.getRunningTaskCount() > 0) {
                        Thread.sleep(100);
                    }
                    starRocksAssert.dropMaterializedView("mv_refresh_priority");
                }
        );
    }

    @Test
    public void testMVPartitionMappingWithManyToMany() {
        starRocksAssert.withTable(new MTable("mock_tbl", "k2",
                        List.of(
                                "k1 date",
                                "k2 int",
                                "v1 int"
                        ),
                        "k1",
                        List.of(
                                "PARTITION p0 values [('2021-07-23'),('2021-07-26'))",
                                "PARTITION p1 values [('2021-07-26'),('2021-07-29'))",
                                "PARTITION p2 values [('2021-07-29'),('2021-08-02'))",
                                "PARTITION p3 values [('2021-08-02'),('2021-08-04'))"
                        )
                ),
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view test_mv_with_many_to_many \n" +
                                    "partition by date_trunc('month',k1) \n" +
                                    "distributed by hash(k2) buckets 3 \n" +
                                    "refresh deferred manual\n" +
                                    "properties('replication_num' = '1', 'partition_refresh_number'='1')\n" +
                                    "as select k1, k2, v1 from mock_tbl;",
                            (mvName) -> {
                                Database testDb = GlobalStateMgr.getCurrentState().getDb(TEST_DB_NAME);
                                MaterializedView materializedView = ((MaterializedView) testDb.getTable((String) mvName));
                                Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());

                                // initial refresh
                                {
                                    TaskRun taskRun = buildMVTaskRun(materializedView, TEST_DB_NAME);
                                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                    taskRun.executeTaskRun();
                                }

                                {
                                    executeInsertSql(connectContext, "insert into mock_tbl partition(p0) " +
                                            "values('2021-07-23',2,10);");
                                    TaskRun taskRun = buildMVTaskRun(materializedView, TEST_DB_NAME);

                                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                    taskRun.executeTaskRun();
                                    PartitionBasedMvRefreshProcessor processor =
                                            (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                    Map<Long, TableSnapshotInfo> snapshotInfoMap = processor.getSnapshotBaseTables();
                                    Assert.assertEquals(1, snapshotInfoMap.size());
                                    TableSnapshotInfo tableSnapshotInfo =
                                            snapshotInfoMap.get(testDb.getTable("mock_tbl").getId());
                                    Assert.assertEquals(Sets.newHashSet("p0", "p1", "p2"),
                                            tableSnapshotInfo.getRefreshedPartitionInfos().keySet());

                                    MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                                    System.out.println(processor.getMVTaskRunExtraMessage());
                                    Assert.assertEquals(Sets.newHashSet("p202107_202108"),
                                            extraMessage.getMvPartitionsToRefresh());
                                    Assert.assertEquals(Sets.newHashSet("p0", "p1", "p2"),
                                            extraMessage.getBasePartitionsToRefreshMap().get("mock_tbl"));
                                    Assert.assertTrue(processor.getNextTaskRun() == null);

                                }

                                {
                                    executeInsertSql(connectContext, "insert into mock_tbl partition(p1) " +
                                            "values('2021-07-27',2,10);");
                                    executeInsertSql(connectContext, "insert into mock_tbl partition(p2) " +
                                            "values('2021-07-29',2,10);");
                                    executeInsertSql(connectContext, "insert into mock_tbl partition(p3) " +
                                            "values('2021-08-02',2,10);");
                                    TaskRun taskRun = buildMVTaskRun(materializedView, TEST_DB_NAME);

                                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                    taskRun.executeTaskRun();
                                    PartitionBasedMvRefreshProcessor processor =
                                            (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                    Map<Long, TableSnapshotInfo> snapshotInfoMap = processor.getSnapshotBaseTables();
                                    Assert.assertEquals(1, snapshotInfoMap.size());
                                    TableSnapshotInfo tableSnapshotInfo =
                                            snapshotInfoMap.get(testDb.getTable("mock_tbl").getId());
                                    System.out.println(processor.getMVTaskRunExtraMessage());
                                    Assert.assertEquals(Sets.newHashSet("p0", "p1", "p2", "p3"),
                                            tableSnapshotInfo.getRefreshedPartitionInfos().keySet());

                                    MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                                    System.out.println(processor.getMVTaskRunExtraMessage());
                                    Assert.assertEquals(Sets.newHashSet("p202107_202108", "p202108_202109"),
                                            extraMessage.getMvPartitionsToRefresh());
                                    Assert.assertEquals(Sets.newHashSet("p0", "p1", "p2", "p3"),
                                            extraMessage.getBasePartitionsToRefreshMap().get("mock_tbl"));
                                    Assert.assertTrue(processor.getNextTaskRun() == null);
                                }
                            });
                }
        );
    }

    @Test
    public void testMVPartitionMappingWithOneToMany() {
        starRocksAssert.withTable(new MTable("mock_tbl", "k2",
                        List.of(
                                "k1 date",
                                "k2 int",
                                "v1 int"
                        ),
                        "k1",
                        List.of(
                                "PARTITION p0 values [('2021-07-01'),('2021-08-01'))",
                                "PARTITION p1 values [('2021-08-01'),('2021-09-01'))",
                                "PARTITION p2 values [('2021-09-01'),('2021-10-01'))"
                        )
                ),
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view test_mv_with_one_to_many \n" +
                                    "partition by date_trunc('day',k1) \n" +
                                    "distributed by hash(k2) buckets 3 \n" +
                                    "refresh deferred manual\n" +
                                    "properties('replication_num' = '1', 'partition_refresh_number'='1')\n" +
                                    "as select k1, k2, v1 from mock_tbl;",
                            (mvName) -> {
                                Database testDb = GlobalStateMgr.getCurrentState().getDb(TEST_DB_NAME);
                                MaterializedView materializedView = ((MaterializedView) testDb.getTable((String) mvName));
                                Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());

                                // initial refresh
                                {
                                    TaskRun taskRun = buildMVTaskRun(materializedView, TEST_DB_NAME);
                                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                    taskRun.executeTaskRun();
                                }

                                {
                                    executeInsertSql(connectContext, "insert into mock_tbl partition(p0) " +
                                            "values('2021-07-23',2,10);");
                                    TaskRun taskRun = buildMVTaskRun(materializedView, TEST_DB_NAME);

                                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                    taskRun.executeTaskRun();
                                    PartitionBasedMvRefreshProcessor processor =
                                            (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                    Map<Long, TableSnapshotInfo> snapshotInfoMap = processor.getSnapshotBaseTables();
                                    Assert.assertEquals(1, snapshotInfoMap.size());
                                    TableSnapshotInfo tableSnapshotInfo =
                                            snapshotInfoMap.get(testDb.getTable("mock_tbl").getId());
                                    Assert.assertEquals(Sets.newHashSet("p0"),
                                            tableSnapshotInfo.getRefreshedPartitionInfos().keySet());

                                    MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                                    System.out.println(processor.getMVTaskRunExtraMessage());
                                    Assert.assertEquals(Sets.newHashSet("p20210701_20210801"),
                                            extraMessage.getMvPartitionsToRefresh());
                                    Assert.assertEquals(Sets.newHashSet("p0"),
                                            extraMessage.getBasePartitionsToRefreshMap().get("mock_tbl"));
                                    Assert.assertTrue(processor.getNextTaskRun() == null);

                                }

                                {
                                    executeInsertSql(connectContext, "insert into mock_tbl partition(p1) " +
                                            "values('2021-08-27',2,10);");
                                    executeInsertSql(connectContext, "insert into mock_tbl partition(p2) " +
                                            "values('2021-09-29',2,10);");
                                    TaskRun taskRun = buildMVTaskRun(materializedView, TEST_DB_NAME);

                                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                    taskRun.executeTaskRun();
                                    PartitionBasedMvRefreshProcessor processor =
                                            (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                    Map<Long, TableSnapshotInfo> snapshotInfoMap = processor.getSnapshotBaseTables();
                                    Assert.assertEquals(1, snapshotInfoMap.size());
                                    TableSnapshotInfo tableSnapshotInfo =
                                            snapshotInfoMap.get(testDb.getTable("mock_tbl").getId());
                                    System.out.println(processor.getMVTaskRunExtraMessage());
                                    Assert.assertEquals(Sets.newHashSet("p1"),
                                            tableSnapshotInfo.getRefreshedPartitionInfos().keySet());

                                    MVTaskRunExtraMessage extraMessage = processor.getMVTaskRunExtraMessage();
                                    System.out.println(processor.getMVTaskRunExtraMessage());
                                    Assert.assertEquals(Sets.newHashSet("p20210801_20210901"),
                                            extraMessage.getMvPartitionsToRefresh());
                                    Assert.assertEquals(Sets.newHashSet("p1"),
                                            extraMessage.getBasePartitionsToRefreshMap().get("mock_tbl"));
                                    Assert.assertTrue(processor.getNextTaskRun() != null);

                                    {
                                        taskRun = processor.getNextTaskRun();
                                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                        taskRun.executeTaskRun();
                                        processor =
                                                (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                        extraMessage = processor.getMVTaskRunExtraMessage();
                                        System.out.println(processor.getMVTaskRunExtraMessage());
                                        Assert.assertEquals(Sets.newHashSet("p20210901_20211001"),
                                                extraMessage.getMvPartitionsToRefresh());
                                        Assert.assertEquals(Sets.newHashSet("p2"),
                                                extraMessage.getBasePartitionsToRefreshMap().get("mock_tbl"));
                                        Assert.assertTrue(processor.getNextTaskRun() == null);
                                    }
                                }
                            });
                }
        );
    }

    @Test
    public void testShowMaterializedViewsWithNonForce() {
        MTable mTable = new MTable("mockTbl", "k2",
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
                        "PARTITION p3 values [('2022-03-01'),('2022-04-01'))"
                )
        ).withValues(List.of(
                "('2021-12-02',2,10)",
                "('2022-01-02',2,10)",
                "('2022-02-02',2,10)"
        ));
        starRocksAssert.withTable(mTable,
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view mock_mv0 \n" +
                                    "partition by k1 \n" +
                                    "distributed by hash(k2) buckets 10\n" +
                                    "refresh deferred manual\n" +
                                    "properties(" +
                                    "   'replication_num' = '1', " +
                                    "   'partition_refresh_number'='1'" +
                                    ")\n" +
                                    "as select k1, k2 from mockTbl;",
                            () -> {
                                String mvName = "mock_mv0";
                                Database testDb = GlobalStateMgr.getCurrentState().getDb(TEST_DB_NAME);
                                MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                                TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();

                                executeInsertSql(connectContext, mTable.getGenerateDataSQL());

                                // refresh materialized view(non force)
                                starRocksAssert.refreshMV(String.format("REFRESH MATERIALIZED VIEW %s", mvName));

                                // without db name
                                Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(null, null).isEmpty());

                                // specific db
                                Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(TEST_DB_NAME, null).isEmpty());

                                String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
                                Map<String, List<TaskRunStatus>> taskNameJobStatusMap =
                                        tm.listMVRefreshedTaskRunStatus(TEST_DB_NAME, Set.of(mvTaskName));
                                System.out.println(taskNameJobStatusMap);
                                Assert.assertFalse(taskNameJobStatusMap.isEmpty());
                                Assert.assertEquals(1, taskNameJobStatusMap.size());
                                // refresh 4 times
                                Assert.assertEquals(4, taskNameJobStatusMap.get(mvTaskName).size());

                                ShowMaterializedViewStatus status =
                                        new ShowMaterializedViewStatus(materializedView.getId(), TEST_DB_NAME,
                                                materializedView.getName());
                                status.setLastJobTaskRunStatus(taskNameJobStatusMap.get(mvTaskName));
                                ShowMaterializedViewStatus.RefreshJobStatus refreshJobStatus = status.getRefreshJobStatus();
                                System.out.println(refreshJobStatus);
                                Assert.assertEquals(refreshJobStatus.isForce(), false);
                                Assert.assertEquals(refreshJobStatus.isRefreshFinished(), true);
                                Assert.assertEquals(refreshJobStatus.getRefreshState(), Constants.TaskRunState.SUCCESS);
                                Assert.assertEquals(refreshJobStatus.getErrorCode(), "0");
                                Assert.assertEquals(refreshJobStatus.getErrorMsg(), "");
                                Assert.assertEquals("[NULL, 2022-01-01, 2022-02-01, 2022-03-01]",
                                        refreshJobStatus.getRefreshedPartitionStarts().toString());
                                Assert.assertEquals("[NULL, 2022-04-01, 2022-04-01, 2022-04-01]",
                                        refreshJobStatus.getRefreshedPartitionEnds().toString());
                                Assert.assertEquals("[{mockTbl=[p0]}, {mockTbl=[p1]}, {mockTbl=[p2]}, {mockTbl=[p3]}]",
                                        refreshJobStatus.getRefreshedBasePartitionsToRefreshMaps().toString());
                                Assert.assertEquals("[[p0], [p1], [p2], [p3]]",
                                        refreshJobStatus.getRefreshedMvPartitionsToRefreshs().toString());
                            });
                }
        );
    }

    @Test
    public void testShowMaterializedViewsWithPartialRefresh() {
        MTable mTable = new MTable("mockTbl", "k2",
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
                        "PARTITION p3 values [('2022-03-01'),('2022-04-01'))"
                )
        ).withValues(List.of(
                "('2021-12-02',2,10)",
                "('2022-01-02',2,10)",
                "('2022-02-02',2,10)"
        ));
        starRocksAssert.withTable(mTable,
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view mock_mv0 \n" +
                                    "partition by k1 \n" +
                                    "distributed by hash(k2) buckets 10\n" +
                                    "refresh deferred manual\n" +
                                    "properties(" +
                                    "   'replication_num' = '1', " +
                                    "   'partition_refresh_number'='1'" +
                                    ")\n" +
                                    "as select k1, k2 from mockTbl;",
                            () -> {
                                String mvName = "mock_mv0";
                                Database testDb = GlobalStateMgr.getCurrentState().getDb(TEST_DB_NAME);
                                MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                                TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();

                                executeInsertSql(connectContext, mTable.getGenerateDataSQL());

                                // refresh materialized view(non force)
                                starRocksAssert.refreshMV(String.format("REFRESH MATERIALIZED VIEW %s\n" +
                                        "PARTITION START (\"%s\") END (\"%s\")", mvName, "2021-12-01", "2022-02-01"));

                                // without db name
                                Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(null, null).isEmpty());

                                // specific db
                                Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(TEST_DB_NAME, null).isEmpty());

                                String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
                                Map<String, List<TaskRunStatus>> taskNameJobStatusMap =
                                        tm.listMVRefreshedTaskRunStatus(TEST_DB_NAME, Set.of(mvTaskName));
                                System.out.println(taskNameJobStatusMap);
                                Assert.assertFalse(taskNameJobStatusMap.isEmpty());
                                Assert.assertEquals(1, taskNameJobStatusMap.size());
                                // refresh 4 times
                                Assert.assertEquals(2, taskNameJobStatusMap.get(mvTaskName).size());

                                ShowMaterializedViewStatus status =
                                        new ShowMaterializedViewStatus(materializedView.getId(), TEST_DB_NAME,
                                                materializedView.getName());
                                status.setLastJobTaskRunStatus(taskNameJobStatusMap.get(mvTaskName));
                                ShowMaterializedViewStatus.RefreshJobStatus refreshJobStatus = status.getRefreshJobStatus();
                                System.out.println(refreshJobStatus);
                                Assert.assertEquals(refreshJobStatus.isForce(), false);
                                Assert.assertEquals(refreshJobStatus.isRefreshFinished(), true);
                                Assert.assertEquals(refreshJobStatus.getRefreshState(), Constants.TaskRunState.SUCCESS);
                                Assert.assertEquals(refreshJobStatus.getErrorCode(), "0");
                                Assert.assertEquals(refreshJobStatus.getErrorMsg(), "");
                                Assert.assertEquals("[2021-12-01, 2022-01-01]",
                                        refreshJobStatus.getRefreshedPartitionStarts().toString());
                                Assert.assertEquals("[2022-02-01, 2022-02-01]",
                                        refreshJobStatus.getRefreshedPartitionEnds().toString());
                                Assert.assertEquals("[{mockTbl=[p0]}, {mockTbl=[p1]}]",
                                        refreshJobStatus.getRefreshedBasePartitionsToRefreshMaps().toString());
                                Assert.assertEquals("[[p0], [p1]]",
                                        refreshJobStatus.getRefreshedMvPartitionsToRefreshs().toString());
                            });
                }
        );
    }

    @Test
    public void testShowMaterializedViewsWithForce() {
        starRocksAssert.withTable(new MTable("mockTbl", "k2",
                        List.of(
                                "k1 date",
                                "k2 int",
                                "v1 int"
                        ),
                        "k1",
                        List.of(
                                "PARTITION p0 values [('2021-12-01'),('2022-01-01'))",
                                "PARTITION p1 values [('2022-01-01'),('2022-02-01'))",
                                "PARTITION p2 values [('2022-02-01'),('2022-03-01'))"
                        )
                ).withValues(List.of(
                        "'2021-12-02',2,10",
                        "'2022-01-02',2,10",
                        "'2022-02-02',2,10"
                )),
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view mock_mv0 \n" +
                                    "partition by k1 \n" +
                                    "distributed by hash(k2) buckets 10\n" +
                                    "refresh deferred manual\n" +
                                    "properties(" +
                                    "   'replication_num' = '1', " +
                                    "   'partition_refresh_number'='1'" +
                                    ")\n" +
                                    "as select k1, k2 from mockTbl;",
                            () -> {
                                String mvName = "mock_mv0";
                                Database testDb = GlobalStateMgr.getCurrentState().getDb(TEST_DB_NAME);
                                MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                                TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();

                                // refresh materialized view(force)
                                refreshMVRange(mvName, "2021-12-01", "2022-03-01", true);

                                // without db name
                                Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(null, null).isEmpty());

                                // specific db
                                Assert.assertFalse(tm.listMVRefreshedTaskRunStatus(TEST_DB_NAME, null).isEmpty());

                                String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
                                Map<String, List<TaskRunStatus>> taskNameJobStatusMap =
                                        tm.listMVRefreshedTaskRunStatus(TEST_DB_NAME, Set.of(mvTaskName));
                                System.out.println(taskNameJobStatusMap);
                                Assert.assertFalse(taskNameJobStatusMap.isEmpty());
                                Assert.assertEquals(1, taskNameJobStatusMap.size());

                                ShowMaterializedViewStatus status =
                                        new ShowMaterializedViewStatus(materializedView.getId(), TEST_DB_NAME,
                                                materializedView.getName());
                                status.setLastJobTaskRunStatus(taskNameJobStatusMap.get(mvTaskName));
                                ShowMaterializedViewStatus.RefreshJobStatus refreshJobStatus = status.getRefreshJobStatus();
                                System.out.println(refreshJobStatus);
                                Assert.assertEquals(refreshJobStatus.isForce(), true);
                                Assert.assertEquals(refreshJobStatus.isRefreshFinished(), true);
                                Assert.assertEquals(refreshJobStatus.getRefreshState(), Constants.TaskRunState.SUCCESS);
                                Assert.assertEquals(refreshJobStatus.getErrorCode(), "0");
                                Assert.assertEquals(refreshJobStatus.getErrorMsg(), "");
                            });
                }
        );
    }

    @Test
    public void testMVRefreshStatus() {
        starRocksAssert.withTables(List.of(
                        new MTable("tt1", "k1",
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
                        new MTable("tt2", "k1",
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
                        )
                ),
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view mv_with_join0\n" +
                                    "partition by k1\n" +
                                    "distributed by hash(k2) buckets 10\n" +
                                    "PROPERTIES('partition_refresh_number' = '1')" +
                                    "refresh deferred manual\n" +
                                    "as select a.k1, b.k2 from tt1 a join tt2 b on a.k1=b.k1;",
                            () -> {
                                Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

                                MaterializedView materializedView =
                                        ((MaterializedView) testDb.getTable("mv_with_join0"));
                                Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());
                                Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                                Map<String, String> testProperties = task.getProperties();
                                testProperties.put(TaskRun.IS_TEST, "true");

                                TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                                taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                taskRun.executeTaskRun();

                                executeInsertSql(connectContext,
                                        "insert into tt1 partition(p1) values('2022-01-02', 3, 10);");
                                executeInsertSql(connectContext,
                                        "insert into tt1 partition(p2) values('2022-02-03', 3, 10);");

                                String jobID = "";
                                {
                                    TaskRunStatus taskRunStatus =
                                            taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                    taskRun.executeTaskRun();
                                    taskRunStatus.setState(Constants.TaskRunState.SUCCESS);

                                    taskRunStatus.setProcessStartTime(System.currentTimeMillis());
                                    MvTaskRunContext mvContext =
                                            ((PartitionBasedMvRefreshProcessor) taskRun.getProcessor()).getMvContext();
                                    Assert.assertTrue(mvContext.hasNextBatchPartition());
                                    PartitionBasedMvRefreshProcessor processor =
                                            (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                    Assert.assertEquals(Sets.newHashSet("p1"),
                                            processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());

                                    Assert.assertEquals(taskRunStatus.getStartTaskRunId(), taskRun.getTaskRunId());

                                    jobID = taskRunStatus.getStartTaskRunId();
                                    {
                                        MVTaskRunExtraMessage extraMessage = taskRunStatus.getMvTaskRunExtraMessage();
                                        System.out.println(extraMessage);
                                        Assert.assertTrue(extraMessage != null);
                                        Assert.assertTrue(extraMessage.getPartitionStart() == null);
                                        Assert.assertTrue(extraMessage.getPartitionEnd() == null);
                                        Assert.assertEquals(extraMessage.getNextPartitionStart(), "2022-02-01");
                                        Assert.assertEquals(extraMessage.getNextPartitionEnd(), "2022-03-01");

                                        Assert.assertTrue(extraMessage.getExecuteOption() != null);
                                        Assert.assertFalse(extraMessage.getExecuteOption().isMergeRedundant());
                                        Assert.assertFalse(extraMessage.getExecuteOption().isReplay());
                                    }

                                    Assert.assertFalse(taskRunStatus.isRefreshFinished());
                                    Assert.assertEquals(taskRunStatus.getLastRefreshState(),
                                            Constants.TaskRunState.RUNNING);

                                    taskRun = processor.getNextTaskRun();
                                    Assert.assertTrue(taskRun != null);
                                }

                                {
                                    TaskRunStatus taskRunStatus =
                                            taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                    taskRun.executeTaskRun();
                                    taskRunStatus.setState(Constants.TaskRunState.SUCCESS);

                                    MvTaskRunContext mvContext =
                                            ((PartitionBasedMvRefreshProcessor) taskRun.getProcessor()).getMvContext();
                                    Assert.assertTrue(!mvContext.hasNextBatchPartition());
                                    PartitionBasedMvRefreshProcessor processor =
                                            (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                    Assert.assertEquals(Sets.newHashSet("p2"),
                                            processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());

                                    Assert.assertEquals(jobID, taskRunStatus.getStartTaskRunId());
                                    {
                                        MVTaskRunExtraMessage extraMessage = taskRunStatus.getMvTaskRunExtraMessage();
                                        System.out.println(extraMessage);
                                        Assert.assertTrue(extraMessage != null);
                                        Assert.assertEquals(extraMessage.getPartitionStart(), "2022-02-01");
                                        Assert.assertEquals(extraMessage.getPartitionEnd(), "2022-03-01");
                                        Assert.assertTrue(extraMessage.getNextPartitionStart() == null);
                                        Assert.assertTrue(extraMessage.getNextPartitionEnd() == null);
                                        Assert.assertTrue(extraMessage.getExecuteOption() != null);
                                        Assert.assertTrue(extraMessage.getExecuteOption().isMergeRedundant());
                                        Assert.assertFalse(extraMessage.getExecuteOption().isReplay());
                                    }

                                    Assert.assertTrue(taskRunStatus.isRefreshFinished());
                                    Assert.assertEquals(taskRunStatus.getLastRefreshState(),
                                            Constants.TaskRunState.SUCCESS);
                                    taskRun = processor.getNextTaskRun();
                                    Assert.assertTrue(taskRun == null);
                                }
                            });
                }
        );
    }

    @Test
    public void testMVRefreshWithFailedStatus() {
        starRocksAssert.withTables(List.of(
                        new MTable("tt1", "k1",
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
                        new MTable("tt2", "k1",
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
                        )
                ),
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view mv_with_join0\n" +
                                    "partition by k1\n" +
                                    "distributed by hash(k2) buckets 10\n" +
                                    "PROPERTIES('partition_refresh_number' = '1')" +
                                    "refresh deferred manual\n" +
                                    "as select a.k1, b.k2 from tt1 a join tt2 b on a.k1=b.k1;",
                            () -> {
                                Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

                                MaterializedView materializedView =
                                        ((MaterializedView) testDb.getTable("mv_with_join0"));
                                Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());
                                Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                                Map<String, String> testProperties = task.getProperties();
                                testProperties.put(TaskRun.IS_TEST, "true");

                                TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                                taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                taskRun.executeTaskRun();

                                executeInsertSql(connectContext,
                                        "insert into tt1 partition(p1) values('2022-01-02', 3, 10);");
                                executeInsertSql(connectContext,
                                        "insert into tt1 partition(p2) values('2022-02-03', 3, 10);");

                                TaskRunStatus taskRunStatus =
                                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                                taskRun.executeTaskRun();
                                taskRunStatus.setState(Constants.TaskRunState.SUCCESS);

                                taskRunStatus.setProcessStartTime(System.currentTimeMillis());
                                MvTaskRunContext mvContext =
                                        ((PartitionBasedMvRefreshProcessor) taskRun.getProcessor()).getMvContext();
                                Assert.assertTrue(mvContext.hasNextBatchPartition());
                                PartitionBasedMvRefreshProcessor processor =
                                        (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                                Assert.assertEquals(Sets.newHashSet("p1"),
                                        processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());

                                Assert.assertEquals(taskRunStatus.getStartTaskRunId(), taskRun.getTaskRunId());

                                // mock: set its state to FAILED
                                taskRunStatus.setState(Constants.TaskRunState.FAILED);

                                MVTaskRunExtraMessage extraMessage = taskRunStatus.getMvTaskRunExtraMessage();
                                System.out.println(extraMessage);
                                Assert.assertTrue(extraMessage != null);
                                Assert.assertTrue(extraMessage.getPartitionStart() == null);
                                Assert.assertTrue(extraMessage.getPartitionEnd() == null);
                                Assert.assertEquals(extraMessage.getNextPartitionStart(), "2022-02-01");
                                Assert.assertEquals(extraMessage.getNextPartitionEnd(), "2022-03-01");

                                Assert.assertTrue(extraMessage.getExecuteOption() != null);
                                Assert.assertFalse(extraMessage.getExecuteOption().isMergeRedundant());
                                Assert.assertFalse(extraMessage.getExecuteOption().isReplay());

                                Assert.assertTrue(taskRunStatus.isRefreshFinished());
                                Assert.assertEquals(taskRunStatus.getLastRefreshState(),
                                        Constants.TaskRunState.FAILED);
                            });
                });
    }
}