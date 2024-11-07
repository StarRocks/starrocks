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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.plan.PlanTestBase.cleanupEphemeralMVs;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PCTRefreshListPartitionOlapTest extends MVRefreshTestBase {
    private static String T1;
    private static String T2;
    private static String T3;
    private static String T4;
    private static String T5;
    private static String T6;
    private static String S2;

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
        // table whose partitions have only single values
        S2 = "CREATE TABLE s2 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (dt) (\n" +
                "     PARTITION p1 VALUES IN (\"20240101\") ,\n" +
                "     PARTITION p2 VALUES IN (\"20240102\") ,\n" +
                "     PARTITION p3 VALUES IN (\"20240103\") \n" +
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
                    "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\")),\n" +
                    "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\")), \n" +
                    "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\")),\n" +
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
        // table with partition expression whose partitions have multi columns(nullable partition columns)
        T6 = "CREATE TABLE t6 (\n" +
                    "      id BIGINT,\n" +
                    "      age SMALLINT,\n" +
                    "      dt VARCHAR(10),\n" +
                    "      province VARCHAR(64)\n" +
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

    private ExecPlan getExecPlan(TaskRun taskRun) {
        try {
            PartitionBasedMvRefreshProcessor processor = getProcessor(taskRun);
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            return execPlan;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
            return null;
        }
    }

    private PartitionBasedMvRefreshProcessor getProcessor(TaskRun taskRun) {
        try {
            initAndExecuteTaskRun(taskRun);
            return (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
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

    private String toPartitionVal(String val) {
        return val == null ? "NULL" : String.format("'%s'", val);
    }

    private void addListPartition(String tbl, String pName, String pVal1, String pVal2) {
        String addPartitionSql = String.format("ALTER TABLE %s ADD PARTITION %s VALUES IN ((%s, %s))", tbl, pName,
                    toPartitionVal(pVal1), toPartitionVal(pVal2));
        StatementBase stmt = SqlParser.parseSingleStatement(addPartitionSql, connectContext.getSessionVariable().getSqlMode());
        try {
            new StmtExecutor(connectContext, stmt).execute();
        } catch (Exception e) {
            Assert.fail("add partition failed:" + e);
        }
    }

    @Test
    public void testRefreshNonPartitionedMV() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T2, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                                    "distributed by random \n" +
                                    "REFRESH DEFERRED MANUAL \n" +
                                    "as select dt, province, sum(age) from t2 group by dt, province;",
                        (obj) -> {
                            String mvName = (String) obj;
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T2, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                                    "partition by province \n" +
                                    "distributed by random \n" +
                                    "REFRESH DEFERRED MANUAL \n" +
                                    "properties ('partition_refresh_number' = '-1')" +
                                    "as select dt, province, sum(age) from t2 group by dt, province;",
                        (obj) -> {
                            String mvName = (String) obj;
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
                                            "     partitions=1/3");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(3, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testRefreshSingleColumnWithMultiValues() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T1, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                                    "partition by province \n" +
                                    "distributed by random \n" +
                                    "REFRESH DEFERRED MANUAL \n" +
                                    "properties ('partition_refresh_number' = '-1')" +
                                    "as select dt, province, sum(age) from t1 group by dt, province;",
                        (obj) -> {
                            String mvName = (String) obj;
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
                                            "     partitions=1/3");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(3, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testRefreshMultiColumnsMV1() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T3, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                                    "partition by province \n" +
                                    "distributed by random \n" +
                                    "REFRESH DEFERRED MANUAL \n" +
                                    "properties ('partition_refresh_number' = '-1')" +
                                    "as select dt, province, sum(age) from t3 group by dt, province;",
                        (obj) -> {
                            String mvName = (String) obj;
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
                                            "     partitions=1/5");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(3, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testRefreshMultiColumnsMV2() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T3, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                                    "partition by dt \n" +
                                    "distributed by random \n" +
                                    "REFRESH DEFERRED MANUAL \n" +
                                    "properties ('partition_refresh_number' = '-1')" +
                                    "as select dt, province, sum(age) from t3 group by dt, province;",
                        (obj) -> {
                            String mvName = (String) obj;
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
                                            "     partitions=3/5");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(2, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testRefreshSingleColumnMVWithPartitionExpr() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T4, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                                    "partition by province \n" +
                                    "distributed by random \n" +
                                    "REFRESH DEFERRED MANUAL \n" +
                                    "properties ('partition_refresh_number' = '-1')" +
                                    "as select dt, province, sum(age) from t4 group by dt, province;",
                        (obj) -> {
                            String mvName = (String) obj;
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
                                            "     partitions=1/1");

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
                                            "     partitions=1/2");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(2, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testRefreshMultiColumnsMVWithPartitionExpr() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T5, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                                    "partition by province \n" +
                                    "distributed by random \n" +
                                    "REFRESH DEFERRED MANUAL \n" +
                                    "properties ('partition_refresh_number' = '-1')" +
                                    "as select dt, province, sum(age) from t5 group by dt, province;",
                        (obj) -> {
                            String mvName = (String) obj;
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
                                            "     partitions=1/1");

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
                                            "     partitions=2/2");

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
                                            "     partitions=1/3");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(2, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testRefreshMultiBaseTablesWithSingleColumn() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
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
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
                                            "     partitions=0/0\n" +
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
                                            "     partitions=1/1");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(3, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testRefreshMultiBaseTablesWithMultiColumns() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
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
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
                                            "     partitions=0/0\n" +
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
                                            "     partitions=1/1");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(3, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testRefreshJoinWithMultiColumns1() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
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
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
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
                                            "     partitions=0/0\n" +
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
                                insertSql = "insert into t5 partition(p1) values(1, 1, '2022-01-01', 'beijing');";
                                executeInsertSql(connectContext, insertSql);

                                ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                                String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                                PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                            "     PREAGGREGATION: ON\n" +
                                            "     partitions=2/3");
                                PlanTestBase.assertContains(plan, "     TABLE: t5\n" +
                                            "     PREAGGREGATION: ON\n" +
                                            "     partitions=1/1");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(3, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testRefreshMVWithMultiNulllalbeColumns() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T6, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                                    "partition by province \n" +
                                    "distributed by random \n" +
                                    "REFRESH DEFERRED MANUAL \n" +
                                    "properties ('partition_refresh_number' = '-1')" +
                                    "as select dt, province, sum(age) from t6 group by dt, province;",
                        (obj) -> {
                            String mvName = (String) obj;
                            MaterializedView materializedView =
                                        ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                                    .getTable(testDb.getFullName(), mvName));
                            Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                            TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

                            {
                                // no partition has changed, no need to refresh
                                ExecPlan execPlan = getExecPlan(taskRun);
                                Assert.assertTrue(execPlan == null);
                            }

                            {
                                // add a new partition
                                addListPartition("t6", "p1", "beijing", "2022-01-01");
                                String insertSql = "insert into t6 partition(p1) values(1, 1, '2021-01-01', 'beijing');";
                                ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);

                                String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                                PlanTestBase.assertContains(plan, "     TABLE: t6\n" +
                                            "     PREAGGREGATION: ON\n" +
                                            "     partitions=1/1");

                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(1, partitions.size());
                                Assert.assertTrue(partitions.iterator().next().getName().equals("p1"));
                                // refresh again, refreshed partitions should not be refreshed again.
                                execPlan = getExecPlan(taskRun);
                                Assert.assertTrue(execPlan == null);
                            }

                            {
                                String insertSql = "insert into t6 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                                executeInsertSql(connectContext, insertSql);
                                addListPartition("t6", "p2", "beijing", "2022-01-02");
                                insertSql = "insert into t6 partition(p2) values(1, 1, '2021-12-02', 'beijing');";
                                executeInsertSql(connectContext, insertSql);

                                ExecPlan execPlan = getExecPlan(taskRun);
                                String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                                PlanTestBase.assertContains(plan, "     TABLE: t6\n" +
                                            "     PREAGGREGATION: ON\n" +
                                            "     partitions=2/2");

                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(1, partitions.size());
                                Assert.assertTrue(partitions.iterator().next().getName().equals("p1"));
                                // refresh again, refreshed partitions should not be refreshed again.
                                execPlan = getExecPlan(taskRun);
                                Assert.assertTrue(execPlan == null);
                            }

                            {
                                // add a new partition
                                addListPartition("t6", "p5", "hangzhou", "2022-01-01");
                                String insertSql = "INSERT INTO t6 partition(p5) values(1, 1, '2022-01-01', 'hangzhou')";
                                ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                                String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                                PlanTestBase.assertContains(plan, "     TABLE: t6\n" +
                                            "     PREAGGREGATION: ON\n" +
                                            "     partitions=1/3");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(2, partitions.size());
                            }

                            {
                                // add a null partition
                                addListPartition("t6", "p6", null, null);
                                String insertSql = "INSERT INTO t6 partition(p6) values(1, 1, NULL, NULL)";
                                ExecPlan execPlan = getExecPlanAfterInsert(taskRun, insertSql);
                                String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                                PlanTestBase.assertContains(plan, "    TABLE: t6\n" +
                                            "     PREAGGREGATION: ON\n" +
                                            "     partitions=1/4");
                                Collection<Partition> partitions = materializedView.getPartitions();
                                Assert.assertEquals(3, partitions.size());
                            }
                        });
        });
    }

    @Test
    public void testPartialRefreshSingleColumnMVWithSingleValues1() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(S2, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by dt \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '1')" +
                            "as select dt, province, sum(age) from s2 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState()
                                .getLocalMetastore().getTable(testDb.getFullName(), mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                        ExecPlan execPlan = getExecPlan(taskRun);
                        Assert.assertEquals(null, execPlan);
                        List<String> partitions =
                                materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                        .collect(Collectors.toList());
                        Assert.assertEquals("[p1, p2, p3]", partitions.toString());
                    });
        });
    }

    @Test
    public void testPartialRefreshSingleColumnMVWithSingleValues2() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(S2, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by dt \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties (" +
                            "   'partition_refresh_number' = '1'," +
                            "   'partition_ttl_number' = '1'" +
                            ")" +
                            "as select dt, province, sum(age) from s2 group by dt, province;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState()
                                .getLocalMetastore().getTable(testDb.getFullName(), mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                        ExecPlan execPlan = getExecPlan(taskRun);
                        Assert.assertEquals(null, execPlan);
                        List<String> partitions =
                                materializedView.getPartitions().stream().map(Partition::getName).sorted()
                                        .collect(Collectors.toList());
                        // If mv has partition_ttl_number, ensure only create ttl number partitions.
                        Assert.assertEquals("[p3]", partitions.toString());
                    });
        });
    }

    @Test
    public void testPartialRefreshSingleColumnMVWithSingleValues3() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(S2, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by dt \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties (" +
                            "   'partition_refresh_number' = '1'" +
                            ")" +
                            "as select dt, province, sum(age) from s2 group by dt, province;",
                    (obj) -> {
                        // update base table
                        {
                            String insertSQL = "INSERT INTO s2 partition(p1) values(1, 1, '20240101', 'beijing')";
                            executeInsertSql(insertSQL);
                            insertSQL = "INSERT INTO s2 partition(p2) values(2, 2, '20240102', 'nanjing')";
                            executeInsertSql(insertSQL);
                        }
                        String mvName = (String) obj;
                        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState()
                                .getLocalMetastore().getTable(testDb.getFullName(), mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        Map<String, String> props = Maps.newHashMap();
                        PListCell partitionValues = new PListCell("20240102");
                        props.put(TaskRun.PARTITION_VALUES, PListCell.batchSerialize(ImmutableSet.of(partitionValues)));
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task)
                                .properties(props)
                                .build();
                        PartitionBasedMvRefreshProcessor processor = getProcessor(taskRun);
                        MvTaskRunContext mvTaskRunContext = processor.getMvContext();
                        Assert.assertNull(mvTaskRunContext.getNextPartitionValues());
                        MVTaskRunExtraMessage message = mvTaskRunContext.status.getMvTaskRunExtraMessage();
                        Assert.assertEquals("p2", message.getMvPartitionsToRefreshString());
                        Assert.assertEquals("{s2=[p2]}", message.getBasePartitionsToRefreshMapString());
                        ExecPlan execPlan = mvTaskRunContext.getExecPlan();
                        Assert.assertNotEquals(null, execPlan);
                        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     TABLE: s2\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     partitions=1/3");
                    });
        });
    }

    @Test
    public void testPartialRefreshSingleColumnMVWithSingleValuesMultiValues1() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T1, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '1')" +
                            "as select dt, province, sum(age) from t1 group by dt, province;",
                    (obj) -> {
                        {
                            String insertSql = "insert into t1 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
                            executeInsertSql(insertSql);
                            insertSql = "INSERT INTO t1 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            executeInsertSql(insertSql);
                        }

                        String mvName = (String) obj;
                        MaterializedView materializedView =
                                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getTable(testDb.getFullName(), mvName));

                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        Map<String, String> props = Maps.newHashMap();
                        PListCell partitionValues = new PListCell("beijing");
                        props.put(TaskRun.PARTITION_VALUES, PListCell.batchSerialize(ImmutableSet.of(partitionValues)));
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task)
                                .properties(props)
                                .build();
                        PartitionBasedMvRefreshProcessor processor = getProcessor(taskRun);
                        MvTaskRunContext mvTaskRunContext = processor.getMvContext();
                        Assert.assertNull(mvTaskRunContext.getNextPartitionValues());
                        MVTaskRunExtraMessage message = mvTaskRunContext.status.getMvTaskRunExtraMessage();
                        Assert.assertEquals("p1", message.getMvPartitionsToRefreshString());
                        Assert.assertEquals("{t1=[p1]}", message.getBasePartitionsToRefreshMapString());
                        ExecPlan execPlan = mvTaskRunContext.getExecPlan();
                        Assert.assertNotEquals(null, execPlan);
                        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     TABLE: t1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     partitions=1/2");
                    });
        });
    }

    @Test
    public void testPartialRefreshMultiColumnMV1() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable(T3, () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '1')" +
                            "as select dt, province, sum(age) from t3 group by dt, province;",
                    (obj) -> {
                        {
                            String insertSql = "insert into t3 partition(p1) values(1, 1, '2024-01-01', 'beijing');";
                            executeInsertSql(insertSql);
                            insertSql = "insert into t3 partition(p3) values(1, 1, '2024-01-02', 'beijing');";
                            executeInsertSql(insertSql);
                        }
                        String mvName = (String) obj;
                        MaterializedView materializedView =
                                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getTable(testDb.getFullName(), mvName));
                        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                        Map<String, String> props = Maps.newHashMap();
                        // even base table has multi partition columns, mv only contain one column
                        PListCell partitionValues = new PListCell(ImmutableList.of(ImmutableList.of("beijing")));
                        props.put(TaskRun.PARTITION_VALUES, PListCell.batchSerialize(ImmutableSet.of(partitionValues)));
                        TaskRun taskRun = TaskRunBuilder.newBuilder(task)
                                .properties(props)
                                .build();
                        PartitionBasedMvRefreshProcessor processor = getProcessor(taskRun);
                        MvTaskRunContext mvTaskRunContext = processor.getMvContext();
                        Assert.assertNull(mvTaskRunContext.getNextPartitionValues());
                        MVTaskRunExtraMessage message = mvTaskRunContext.status.getMvTaskRunExtraMessage();
                        Assert.assertEquals("p1", message.getMvPartitionsToRefreshString());
                        Assert.assertEquals("{t3=[p1, p3]}", message.getBasePartitionsToRefreshMapString());
                        ExecPlan execPlan = mvTaskRunContext.getExecPlan();
                        Assert.assertNotEquals(null, execPlan);
                        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                        PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                "     TABLE: t3\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     partitions=2/4");
                    });
        });
    }

    @Test
    public void testRefreshWithDuplicatedPartitions() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t1 (\n" +
                "    dt varchar(20),\n" +
                "    province string,\n" +
                "    num int\n" +
                ")\n" +
                "DUPLICATE KEY(dt)\n" +
                "PARTITION BY LIST(`dt`, `province`)\n" +
                "(\n" +
                "    PARTITION `p1` VALUES IN ((\"2020-07-01\", \"beijing\"), (\"2020-07-02\", \"beijing\")),\n" +
                "    PARTITION `p2` VALUES IN ((\"2020-07-01\", \"chengdu\"), (\"2020-07-03\", \"chengdu\")),\n" +
                "    PARTITION `p3` VALUES IN ((\"2020-07-02\", \"hangzhou\"), (\"2020-07-04\", \"hangzhou\"))\n" +
                ");");
        executeInsertSql("INSERT INTO t1 VALUES \n" +
                "    (\"2020-07-01\", \"beijing\",  1), (\"2020-07-01\", \"chengdu\",  2),\n" +
                "    (\"2020-07-02\", \"beijing\",  3), (\"2020-07-02\", \"hangzhou\", 4),\n" +
                "    (\"2020-07-03\", \"chengdu\",  1),\n" +
                "    (\"2020-07-04\", \"hangzhou\", 1)\n" +
                ";");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv1 \n" +
                "    PARTITION BY dt\n" +
                "    REFRESH DEFERRED MANUAL \n" +
                "    PROPERTIES (\n" +
                "        'partition_refresh_number' = '-1',\n" +
                "        \"replication_num\" = \"1\"\n" +
                "    )\n" +
                "    AS SELECT dt,province,sum(num) FROM t1 GROUP BY dt,province;\n");
        try {
            starRocksAssert.refreshMV("REFRESH MATERIALIZED VIEW mv1 WITH SYNC MODE;");
            MaterializedView mv =
                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable("test", "mv1"));
            Assert.assertEquals(3, mv.getPartitions().size());
            PartitionInfo partitionInfo = mv.getPartitionInfo();
            Assert.assertTrue(partitionInfo instanceof ListPartitionInfo);
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            Map<Long, List<List<String>>> idToMultiValues = listPartitionInfo.getIdToMultiValues();
            Assert.assertEquals(3, idToMultiValues.size());
            Partition p1 = mv.getPartition("p1");
            Partition p2 = mv.getPartition("p2");
            Partition p3 = mv.getPartition("p3");
            List<List<String>> p1Values = ImmutableList.of(ImmutableList.of("2020-07-01"), ImmutableList.of("2020-07-02"));
            List<List<String>> p2Values = ImmutableList.of(ImmutableList.of("2020-07-03"));
            List<List<String>> p3Values = ImmutableList.of(ImmutableList.of("2020-07-04"));
            Assert.assertEquals(p1Values, idToMultiValues.get(p1.getId()));
            Assert.assertEquals(p2Values, idToMultiValues.get(p2.getId()));
            Assert.assertEquals(p3Values, idToMultiValues.get(p3.getId()));

            // should not have any partitions to refresh after complete refresh
            Task task = TaskBuilder.buildMvTask(mv, "test");
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
            ExecPlan execPlan = getExecPlan(taskRun);
            Assert.assertEquals(null, execPlan);

            // update old partitions of base table
            executeInsertSql("INSERT INTO t1 VALUES \n" +
                    "    (\"2020-07-01\", \"beijing\",  1), (\"2020-07-01\", \"chengdu\",  2),\n" +
                    "    (\"2020-07-02\", \"beijing\",  3), (\"2020-07-02\", \"hangzhou\", 4);\n");
            PartitionBasedMvRefreshProcessor processor = getProcessor(taskRun);
            MvTaskRunContext mvTaskRunContext = processor.getMvContext();
            Assert.assertNull(mvTaskRunContext.getNextPartitionValues());
            MVTaskRunExtraMessage message = mvTaskRunContext.status.getMvTaskRunExtraMessage();
            Assert.assertEquals("p1,p2,p3", message.getMvPartitionsToRefreshString());
            Assert.assertEquals("{t1=[p1, p2, p3]}", message.getBasePartitionsToRefreshMapString());
            starRocksAssert.dropTable("t1");
            starRocksAssert.dropMaterializedView("mv1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}