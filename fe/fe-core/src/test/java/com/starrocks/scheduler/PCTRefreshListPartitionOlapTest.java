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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.apache.commons.lang3.StringUtils;
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

import static com.starrocks.sql.plan.PlanTestBase.cleanupEphemeralMVs;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PCTRefreshListPartitionOlapTest extends MVRefreshTestBase {
    private static String T1;
    private static String T2;
    private static String T3;
    private static String T4;
    private static String T5;

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVRefreshTestBase.beforeClass();
        // table whose partitions have multiple values
        T1 = "CREATE TABLE t1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";
        // table whose partitions have only single values
        T2 = "CREATE TABLE t2 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";
        // table whose partitions have multi columns
        T3 = "CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\"))  ,\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\"))  ,\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";
        // table with partition expression whose partitions have multiple values
        T4 = "CREATE TABLE t4 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY (province) \n" +
                "DISTRIBUTED BY RANDOM\n";
        // table with partition expression whose partitions have multi columns
        T5 = "CREATE TABLE t5 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY (province, dt) \n" +
                "DISTRIBUTED BY RANDOM\n";
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

    private ExecPlan getExecPlan(TaskRun taskRun) {
        try {
            initAndExecuteTaskRun(taskRun);

            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            return execPlan;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
            return null;
        }
    }

    private ExecPlan getExecPlanAfterInsert(TaskRun taskRun, String insertSql) {
        try {
            executeInsertSql(connectContext, insertSql);
        } catch (Exception e) {
            Assert.fail();
            return null;
        }
        ExecPlan execPlan = getExecPlan(taskRun);
        Assert.assertTrue(execPlan != null);
        return execPlan;
    }

    private void addListPartition(String tbl, String pName, String pVal) {
        String addPartitionSql = String.format("ALTER TABLE %s ADD PARTITION %s VALUES IN ('%s')", tbl, pName, pVal);
        StatementBase stmt = SqlParser.parseSingleStatement(addPartitionSql, connectContext.getSessionVariable().getSqlMode());
        try {
            new StmtExecutor(connectContext, stmt).execute();
        } catch (Exception e) {
            Assert.fail("add partition failed:" + e);
        }
    }

    private void addListPartition(String tbl, String pName, String pVal1, String pVal2) {
        String addPartitionSql = String.format("ALTER TABLE %s ADD PARTITION %s VALUES IN (('%s', '%s'))", tbl, pName, pVal1,
                pVal2);
        StatementBase stmt = SqlParser.parseSingleStatement(addPartitionSql, connectContext.getSessionVariable().getSqlMode());
        try {
            new StmtExecutor(connectContext, stmt).execute();
        } catch (Exception e) {
            Assert.fail("add partition failed:" + e);
        }
    }

    @Test
    public void testRefreshNonPartitionedMV() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTable(T2, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "as select dt, province, sum(age) from t2 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=2/2");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(1, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t2", "p3", "hangzhou");

                            String insertSql = "INSERT INTO t2 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=3/3");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(1, partitions.size());
                        }
                    });
        });
    }

    @Test
    public void testRefreshSingleColumnMVWithSingleValues() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTable(T2, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1')" +
                            "as select dt, province, sum(age) from t2 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'beijing'\n" +
                                    "     partitions=1/2");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t2", "p3", "hangzhou");

                            String insertSql = "INSERT INTO t2 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'hangzhou'\n" +
                                    "     partitions=1/3");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(3, partitions.size());
                        }
                    });
        });
    }

    @Test
    public void testRefreshSingleColumnWithMultiValues() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTable(T1, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1')" +
                            "as select dt, province, sum(age) from t1 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            String insertSql = "insert into t1 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province IN ('beijing', 'chongqing')\n" +
                                    "     partitions=1/2");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t1", "p3", "hangzhou");

                            String insertSql = "INSERT INTO t1 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'hangzhou'\n" +
                                    "     partitions=1/3");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(3, partitions.size());
                        }
                    });
        });
    }

    @Test
    public void testRefreshMultiColumnsMV1() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTable(T3, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1')" +
                            "as select dt, province, sum(age) from t3 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            String insertSql = "insert into t3 partition(p1) values(1, 1, '2024-01-01', 'beijing');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);

                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                    "     TABLE: t3\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'beijing'\n" +
                                    "     partitions=2/4");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            String insertSql = "insert into t3 partition(p1) values(1, 1, '2024-01-01', 'beijing');";
                            executeInsertSql(connectContext, insertSql);
                            insertSql = "insert into t3 partition(p3) values(1, 1, '2024-01-02', 'beijing');";
                            executeInsertSql(connectContext, insertSql);

                            ExecPlan execPlan = getExecPlan(taskRun);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t3\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'beijing'\n" +
                                    "     partitions=2/4");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t3", "p5", "hangzhou", "2022-01-01");
                            String insertSql = "INSERT INTO t3 partition(p5) values(1, 1, '2022-01-01', 'hangzhou')";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t3\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'hangzhou'\n" +
                                    "     partitions=1/5");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(3, partitions.size());
                        }
                    });
        });
    }

    @Test
    public void testRefreshMultiColumnsMV2() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTable(T3, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by dt \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1')" +
                            "as select dt, province, sum(age) from t3 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            String insertSql = "insert into t3 values (1, 1, '2024-01-01', 'beijing')," +
                                    "(2, 20, '2024-01-01', 'guangdong'), (3, 30, '2024-01-02', 'guangdong');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);

                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t3\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 3: dt IN ('2024-01-01', '2024-01-02')\n" +
                                    "     partitions=4/4");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            String insertSql = "insert into t3 partition(p1) values(1, 1, '2024-01-01', 'beijing');";
                            executeInsertSql(connectContext, insertSql);
                            insertSql = "insert into t3 partition(p3) values(1, 1, '2024-01-02', 'beijing');";
                            executeInsertSql(connectContext, insertSql);

                            ExecPlan execPlan = getExecPlan(taskRun);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t3\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 3: dt IN ('2024-01-01', '2024-01-02')\n" +
                                    "     partitions=4/4");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t3", "p5", "hangzhou", "2024-01-01");
                            String insertSql = "INSERT INTO t3 partition(p5) values(1, 1, '2024-01-01', 'hangzhou')";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t3\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 3: dt = '2024-01-01'\n" +
                                    "     partitions=3/5");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                        }
                    });
        });
    }

    @Test
    public void testRefreshSingleColumnMVWithPartitionExpr() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTable(T4, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1')" +
                            "as select dt, province, sum(age) from t4 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            addListPartition("t4", "p1", "beijing");
                            String insertSql = "insert into t4 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t4\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'beijing'\n" +
                                    "     partitions=1/2");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(1, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t4", "p3", "hangzhou");

                            String insertSql = "INSERT INTO t4 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t4\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'hangzhou'\n" +
                                    "     partitions=1/3");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                        }
                    });
        });
    }

    @Test
    public void testRefreshMultiColumnsMVWithPartitionExpr() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTable(T5, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1')" +
                            "as select dt, province, sum(age) from t5 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t5", "p1", "beijing", "2022-01-01");
                            String insertSql = "insert into t5 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);

                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t5\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'beijing'\n" +
                                    "     partitions=1/2");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(1, partitions.size());
                            Assert.assertTrue(partitions.iterator().next().getName().equals("p1"));
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            String insertSql = "insert into t5 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            executeInsertSql(connectContext, insertSql);
                            addListPartition("t5", "p2", "beijing", "2022-01-02");
                            insertSql = "insert into t5 partition(p2) values(1, 1, '2021-12-02', 'beijing');";
                            executeInsertSql(connectContext, insertSql);

                            ExecPlan execPlan = getExecPlan(taskRun);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t5\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'beijing'\n" +
                                    "     partitions=2/3");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(1, partitions.size());
                            Assert.assertTrue(partitions.iterator().next().getName().equals("p1"));
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t5", "p5", "hangzhou", "2022-01-01");
                            String insertSql = "INSERT INTO t5 partition(p5) values(1, 1, '2022-01-01', 'hangzhou')";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t5\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'hangzhou'\n" +
                                    "     partitions=1/4");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                        }
                    });
        });
    }

    @Test
    public void testRefreshMultiBaseTablesWithSingleColumn() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTables(ImmutableList.of(T2, T4), () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1')" +
                            "as " +
                            "   select dt, province, sum(age) from t2 group by dt, province " +
                            " union all " +
                            "   select dt, province, sum(age) from t4 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // only one table has updated
                            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "  1:OlapScanNode\n" +
                                    "     TABLE: t2\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/2\n" +
                                    "     rollup: t2");
                            PlanTestBase.assertContains(plan, "     TABLE: t4\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/1\n" +
                                    "     rollup: t4");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // t2 add a new partition
                            addListPartition("t2", "p3", "hangzhou");
                            String insertSql = "INSERT INTO t2 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            executeInsertSql(connectContext, insertSql);

                            // t4 add a new partition
                            addListPartition("t4", "p3", "hangzhou");
                            insertSql = "INSERT INTO t4 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            executeInsertSql(connectContext, insertSql);

                            ExecPlan execPlan = getExecPlan(taskRun);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/3");
                            PlanTestBase.assertContains(plan, "     TABLE: t4\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/2");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(3, partitions.size());
                        }
                    });
        });
    }

    @Test
    public void testRefreshMultiBaseTablesWithMultiColumns() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTables(ImmutableList.of(T1, T5), () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1') \n" +
                            "as select dt, province, sum(age) from t1 group by dt, province \n" +
                            " union all\n" +
                            " select dt, province, sum(age) from t5 group by dt, province",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // only t1 has updated
                            String insertSql = "insert into t1 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/2\n" +
                                    "     rollup: t1");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t1", "p3", "hangzhou");

                            String insertSql = "INSERT INTO t1 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/3");
                            PlanTestBase.assertContains(plan, "     TABLE: t5\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/1\n" +
                                    "     rollup: t5");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(3, partitions.size());
                        }

                        {
                            // t1 add a new partition
                            String insertSql = "INSERT INTO t1 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            executeInsertSql(connectContext, insertSql);
                            // t5 add a new partition
                            addListPartition("t5", "p1", "beijing", "2022-01-01");
                            insertSql = "insert into t5 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            executeInsertSql(connectContext, insertSql);

                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=2/3\n" +
                                    "     rollup: t1");
                            PlanTestBase.assertContains(plan, "     TABLE: t5\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/2");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(3, partitions.size());
                        }
                    });
        });
    }

    @Test
    public void testRefreshJoinWithMultiColumns1() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        starRocksAssert.withTables(ImmutableList.of(T1, T5), () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1') \n" +
                            "as select t1.dt, t5.province, sum(t5.age) from t1 join t5 on t1.province=t5.province " +
                            "group by t1.dt, t5.province \n",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                        {
                            // no partition has changed, no need to refresh
                            ExecPlan execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // only t1 has updated
                            String insertSql = "insert into t1 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/2\n" +
                                    "     rollup: t1");

                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(2, partitions.size());
                            // refresh again, refreshed partitions should not be refreshed again.
                            execPlan = getExecPlan(taskRun);
                            Assert.assertTrue(execPlan == null);
                        }

                        {
                            // add a new partition
                            addListPartition("t1", "p3", "hangzhou");

                            String insertSql = "INSERT INTO t1 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/3");
                            PlanTestBase.assertContains(plan, "     TABLE: t5\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/1\n" +
                                    "     rollup: t5");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(3, partitions.size());
                        }

                        {
                            // t1 add a new partition
                            String insertSql = "INSERT INTO t1 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            executeInsertSql(connectContext, insertSql);
                            // t5 add a new partition
                            addListPartition("t5", "p1", "beijing", "2022-01-01");
                            insertSql = "insert into t5 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            executeInsertSql(connectContext, insertSql);

                            ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 4: province = 'beijing'\n" +
                                    "     partitions=1/3");
                            PlanTestBase.assertContains(plan, "     TABLE: t5\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     PREDICATES: 8: province = 'beijing'\n" +
                                    "     partitions=1/2");
                            Collection<Partition> partitions = materializedView.getPartitions();
                            Assert.assertEquals(3, partitions.size());
                        }
                    });
        });
    }
}