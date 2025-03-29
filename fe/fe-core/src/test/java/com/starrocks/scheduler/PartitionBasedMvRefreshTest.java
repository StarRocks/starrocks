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
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionBasedMvRefreshTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, temp.newFolder().toURI().toString());
        starRocksAssert
                    .withTable("CREATE TABLE `t1` (\n" +
                                "    `k1`  date not null, \n" +
                                "    `k2`  datetime not null, \n" +
                                "    `k3`  char(20), \n" +
                                "    `k4`  varchar(20), \n" +
                                "    `k5`  boolean, \n" +
                                "    `k6`  tinyint, \n" +
                                "    `k7`  smallint, \n" +
                                "    `k8`  int, \n" +
                                "    `k9`  bigint, \n" +
                                "    `k10` largeint, \n" +
                                "    `k11` float, \n" +
                                "    `k12` double, \n" +
                                "    `k13` decimal(27,9) ) \n" +
                                "DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) \n" +
                                "PARTITION BY RANGE(`k1`) \n" +
                                "(\n" +
                                "PARTITION p20201022 VALUES [(\"2020-10-22\"), (\"2020-10-23\")), \n" +
                                "PARTITION p20201023 VALUES [(\"2020-10-23\"), (\"2020-10-24\")), \n" +
                                "PARTITION p20201024 VALUES [(\"2020-10-24\"), (\"2020-10-25\"))\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3;")
                    .withTable("CREATE TABLE `t2` (\n" +
                                "    `k1`  date not null, \n" +
                                "    `k2`  datetime not null, \n" +
                                "    `k3`  char(20), \n" +
                                "    `k4`  varchar(20), \n" +
                                "    `k5`  boolean, \n" +
                                "    `k6`  tinyint, \n" +
                                "    `k7`  smallint, \n" +
                                "    `k8`  int, \n" +
                                "    `k9`  bigint, \n" +
                                "    `k10` largeint, \n" +
                                "    `k11` float, \n" +
                                "    `k12` double, \n" +
                                "    `k13` decimal(27,9) ) \n" +
                                "DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) \n" +
                                "PARTITION BY RANGE(`k1`) \n" +
                                "(\n" +
                                "PARTITION p20201010 VALUES [(\"2020-10-10\"), (\"2020-10-11\")), \n" +
                                "PARTITION p20201011 VALUES [(\"2020-10-11\"), (\"2020-10-12\")), \n" +
                                "PARTITION p20201012 VALUES [(\"2020-10-12\"), (\"2020-10-13\")), \n" +
                                "PARTITION p20201021 VALUES [(\"2020-10-21\"), (\"2020-10-22\")), \n" +
                                "PARTITION p20201022 VALUES [(\"2020-10-22\"), (\"2020-10-23\"))\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3;");
        executeInsertSql(connectContext, "INSERT INTO t1 VALUES\n" +
                    "    ('2020-10-22','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889),\n" +
                    "    ('2020-10-23','2020-10-24 12:12:12','k3','k4',0,0,2,3,4,5,1.1,1.12,2.889),\n" +
                    "    ('2020-10-24','2020-10-25 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889);");
        executeInsertSql(connectContext, "INSERT INTO t2 VALUES\n" +
                    "    ('2020-10-10','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889),\n" +
                    "    ('2020-10-11','2020-10-24 12:12:12','k3','k4',0,0,2,3,4,5,1.1,1.12,2.889),\n" +
                    "    ('2020-10-12','2020-10-25 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889),\n" +
                    "    ('2020-10-21','2020-10-24 12:12:12','k3','k4',0,0,2,3,4,5,1.1,1.12,2.889),\n" +
                    "    ('2020-10-22','2020-10-25 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889)");
    }

    protected void assertPlanContains(ExecPlan execPlan, String... explain) throws Exception {
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                        StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    private static void initAndExecuteTaskRun(TaskRun taskRun,
                                              String startPartition,
                                              String endPartition) throws Exception {
        Map<String, String> testProperties = taskRun.getProperties();
        testProperties.put(TaskRun.IS_TEST, "true");
        if (startPartition != null) {
            testProperties.put(TaskRun.PARTITION_START, startPartition);
        }
        if (endPartition != null) {
            testProperties.put(TaskRun.PARTITION_END, endPartition);
        }
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
    }

    protected static void initAndExecuteTaskRun(TaskRun taskRun) throws Exception {
        initAndExecuteTaskRun(taskRun, null, null);
    }

    private void testRefreshUnionAllWithDefaultRefreshNumber(String mvSql,
                                                             List<Integer> t1PartitionNums,
                                                             List<Integer> t2PartitionNums) {
        Assert.assertTrue(t1PartitionNums.size() == t2PartitionNums.size());
        int mvRefreshTimes = t1PartitionNums.size();
        starRocksAssert.withMaterializedView(mvSql,
                    () -> {
                        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "test_mv0"));
                        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());

                        TaskRun taskRun = null;
                        for (int i = 0; i < mvRefreshTimes; i++) {
                            if (i == 0) {
                                taskRun = TaskRunBuilder.newBuilder(task).build();
                            }
                            System.out.println("start to execute task run:" + i);
                            Assert.assertTrue(taskRun != null);
                            initAndExecuteTaskRun(taskRun);
                            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                                        taskRun.getProcessor();
                            MvTaskRunContext mvContext = processor.getMvContext();
                            ExecPlan execPlan = mvContext.getExecPlan();
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            Assert.assertTrue(plan != null);
                            taskRun = processor.getNextTaskRun();
                            PlanTestBase.assertContains(plan, String.format("     TABLE: t1\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     partitions=%s/3", t1PartitionNums.get(i)));
                            PlanTestBase.assertContains(plan, String.format("     TABLE: t2\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     partitions=%s/5", t2PartitionNums.get(i)));
                            if (i == mvRefreshTimes - 1) {
                                Assert.assertTrue(taskRun == null);
                            }
                        }
                    });
    }

    @Test
    public void testUnionAllMvWithPartition1() {
        String sql = "create materialized view test_mv0 \n" +
                    "partition by k1 \n" +
                    "distributed by random \n" +
                    "refresh async \n" +
                    "properties(" +
                    "\"partition_refresh_number\" = \"1\"" +
                    ")" +
                    "as " +
                    " select * from t1 union all select * from t2;";
        List<Integer> t1PartitionNums = ImmutableList.of(0, 0, 0, 0, 1, 1, 1);
        List<Integer> t2PartitionNums = ImmutableList.of(1, 1, 1, 1, 1, 0, 0);
        testRefreshUnionAllWithDefaultRefreshNumber(sql, t1PartitionNums, t2PartitionNums);
    }

    @Test
    public void testUnionAllMvWithPartition2() {
        String sql = "create materialized view test_mv0 \n" +
                    "partition by k1 \n" +
                    "distributed by random \n" +
                    "refresh async \n" +
                    "properties(" +
                    "\"partition_refresh_number\" = \"1\"" +
                    ")" +
                    "as " +
                    " select * from t2 union all select * from t1;";
        List<Integer> t1PartitionNums = ImmutableList.of(0, 0, 0, 0, 1, 1, 1);
        List<Integer> t2PartitionNums = ImmutableList.of(1, 1, 1, 1, 1, 0, 0);
        testRefreshUnionAllWithDefaultRefreshNumber(sql, t1PartitionNums, t2PartitionNums);
    }

    @Test
    public void testUnionAllMvWithPartitionForceMode() {
        String sql = "create materialized view test_mv0 \n" +
                    "partition by k1 \n" +
                    "distributed by random \n" +
                    "refresh async \n" +
                    "as " +
                    " select * from t1 union all select * from t2;";
        starRocksAssert.withMaterializedView(sql,
                    () -> {
                        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "test_mv0"));
                        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());

                        TaskRun taskRun = null;
                        int refreshTimes = 1;
                        for (int i = 0; i < refreshTimes; i++) {
                            if (i == 0) {
                                taskRun = TaskRunBuilder.newBuilder(task).build();
                            }
                            System.out.println("start to execute task run:" + i);
                            Assert.assertTrue(taskRun != null);
                            initAndExecuteTaskRun(taskRun);
                            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                                        taskRun.getProcessor();
                            MvTaskRunContext mvContext = processor.getMvContext();
                            ExecPlan execPlan = mvContext.getExecPlan();
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            taskRun = processor.getNextTaskRun();

                            PlanTestBase.assertContains(plan, "     TABLE: t1\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     partitions=3/3");
                            PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     partitions=5/5");
                            if (i == refreshTimes - 1) {
                                Assert.assertTrue(taskRun == null);
                            }
                        }
                    });
    }

    @Test
    public void testUnionAllMvWithPartitionWithPartitionStartAndEnd() {
        String sql = "create materialized view test_mv0 \n" +
                    "partition by k1 \n" +
                    "distributed by random \n" +
                    "refresh async \n" +
                    "properties(" +
                    "\"partition_refresh_number\" = \"1\"" +
                    ")" +
                    "as " +
                    " select * from t2 union all select * from t1;";
        starRocksAssert.withMaterializedView(sql,
                    () -> {
                        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "test_mv0"));

                        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
                        int mvRefreshTimes = 3;
                        List<Integer> t1PartitionNums = ImmutableList.of(0, 0, 1);
                        List<Integer> t2PartitionNums = ImmutableList.of(1, 1, 1);
                        TaskRun taskRun = null;
                        for (int i = 0; i < mvRefreshTimes; i++) {
                            System.out.println("start to execute task run:" + i);
                            if (i == 0) {
                                taskRun = TaskRunBuilder.newBuilder(task).build();
                                initAndExecuteTaskRun(taskRun, "2020-10-12", "2020-10-23");
                            } else {
                                ExecuteOption executeOption = taskRun.getExecuteOption();
                                String partitionStart = executeOption.getTaskRunProperties().get(TaskRun.PARTITION_START);
                                String partitionEnd = executeOption.getTaskRunProperties().get(TaskRun.PARTITION_END);
                                initAndExecuteTaskRun(taskRun, partitionStart, partitionEnd);
                            }
                            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                                        taskRun.getProcessor();
                            MvTaskRunContext mvContext = processor.getMvContext();
                            ExecPlan execPlan = mvContext.getExecPlan();
                            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
                            Assert.assertTrue(plan != null);
                            taskRun = processor.getNextTaskRun();
                            PlanTestBase.assertContains(plan, String.format("     TABLE: t1\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     partitions=%s/3", t1PartitionNums.get(i)));
                            PlanTestBase.assertContains(plan, String.format("     TABLE: t2\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     partitions=%s/5", t2PartitionNums.get(i)));
                            if (i == mvRefreshTimes - 1) {
                                Assert.assertTrue(taskRun == null);
                            }
                        }
                    });
    }

    @Test
    public void testJoinMV_SlotRef() throws Exception {
        starRocksAssert.withTable("CREATE TABLE join_base_t1 (dt1 date, int1 int)\n" +
                    "                    PARTITION BY RANGE(dt1)\n" +
                    "                    (\n" +
                    "                    PARTITION p1 VALUES LESS THAN (\"2020-07-01\"),\n" +
                    "                    PARTITION p2 VALUES LESS THAN (\"2020-08-01\"),\n" +
                    "                    PARTITION p3 VALUES LESS THAN (\"2020-09-01\")\n" +
                    "                    );");
        starRocksAssert.withTable("CREATE TABLE join_base_t2 (dt2 date, int2 int)\n" +
                    "                    PARTITION BY RANGE(dt2)\n" +
                    "                    (\n" +
                    "                    PARTITION p4 VALUES LESS THAN (\"2020-07-01\"),\n" +
                    "                    PARTITION p5 VALUES LESS THAN (\"2020-08-01\"),\n" +
                    "                    PARTITION p6 VALUES LESS THAN (\"2020-09-01\")\n" +
                    "                    );");
        starRocksAssert.withRefreshedMaterializedView("CREATE MATERIALIZED VIEW join_mv1 " +
                    "PARTITION BY dt1 " +
                    "REFRESH MANUAL " +
                    "PROPERTIES (\"partition_refresh_number\"=\"3\") AS " +
                    "SELECT dt1,dt2,sum(int1) " +
                    "FROM join_base_t1 t1 " +
                    "JOIN join_base_t2 t2 ON t1.dt1=t2.dt2 GROUP BY dt1,dt2;");

        MaterializedView mv = starRocksAssert.getMv("test", "join_mv1");
        Assert.assertEquals(3, mv.getPartitionNames().size());
        Set<Range<PartitionKey>> ranges =
                mv.getRangePartitionMap().values().stream().collect(Collectors.toSet());
        Assert.assertEquals(3, ranges.size());
        PartitionKey p0 = new PartitionKey(ImmutableList.of(new DateLiteral(0, 1, 1)),
                ImmutableList.of(PrimitiveType.DATE));
        PartitionKey p1 = new PartitionKey(ImmutableList.of(new DateLiteral(2020, 7, 1)),
                ImmutableList.of(PrimitiveType.DATE));
        PartitionKey p2 = new PartitionKey(ImmutableList.of(new DateLiteral(2020, 8, 1)),
                ImmutableList.of(PrimitiveType.DATE));
        PartitionKey p3 = new PartitionKey(ImmutableList.of(new DateLiteral(2020, 9, 1)),
                ImmutableList.of(PrimitiveType.DATE));
        Assert.assertTrue(ranges.contains(Range.closedOpen(p0, p1)));
        Assert.assertTrue(ranges.contains(Range.closedOpen(p1, p2)));
        Assert.assertTrue(ranges.contains(Range.closedOpen(p2, p3)));
        starRocksAssert.dropTable("join_base_t1");
        starRocksAssert.dropTable("join_base_t2");
        starRocksAssert.dropMaterializedView("join_mv1");
    }

    @Test
    public void testJoinMV_ListPartition() throws Exception {
        starRocksAssert.withTable("CREATE TABLE join_base_t1 (dt1 date, int1 int)\n" +
                    "PARTITION BY RANGE(dt1)\n" +
                    "(\n" +
                    "    PARTITION p202006 VALUES LESS THAN (\"2020-07-01\"),\n" +
                    "    PARTITION p202007 VALUES LESS THAN (\"2020-08-01\"),\n" +
                    "    PARTITION p202008 VALUES LESS THAN (\"2020-09-01\")\n" +
                    ")");
        starRocksAssert.withTable("CREATE TABLE join_base_t2 (dt2 date not null, int2 int)\n" +
                    "PARTITION BY LIST(dt2)\n" +
                    "(\n" +
                    "    PARTITION p202006 VALUES in (\"2020-06-23\"),\n" +
                    "    PARTITION p202007 VALUES in (\"2020-07-23\"),\n" +
                    "    PARTITION p202008 VALUES in (\"2020-08-23\")\n" +
                    ");");
        Exception e = Assert.assertThrows(DmlException.class, () ->
                    starRocksAssert.withRefreshedMaterializedView("CREATE MATERIALIZED VIEW join_mv1 " +
                                "PARTITION BY dt1 REFRESH MANUAL PROPERTIES (\"partition_refresh_number\"=\"3\") AS \n" +
                                "SELECT dt1,dt2,sum(int1) " +
                                "FROM join_base_t1 t1 " +
                                "JOIN join_base_t2 t2 ON t1.dt1=t2.dt2 GROUP BY dt1,dt2")
        );
        // TODO(fix me): throw a better stack
        System.out.println(e.getMessage());
        Assert.assertTrue(e.getMessage().contains("Must be range partitioned table"));

        starRocksAssert.dropTable("join_base_t1");
        starRocksAssert.dropTable("join_base_t2");
        starRocksAssert.dropMaterializedView("join_mv1");
    }
}