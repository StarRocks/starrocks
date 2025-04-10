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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Instant;
import java.util.Set;

import static com.starrocks.sql.plan.PlanTestBase.cleanupEphemeralMVs;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionBasedMvRefreshProcessorOlapPart2Test extends MVRefreshTestBase {

    @AfterClass
    public static void tearDown() throws Exception {
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

    private static void initAndExecuteTaskRun(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
    }

    @Test
    public void testMVRefreshWithTheSameTables1() {
        starRocksAssert.withTables(ImmutableList.of(
                        new MTable("tt1", "k1",
                                ImmutableList.of(
                                        "k1 int",
                                        "k2 int",
                                        "k3 string",
                                        "dt date"
                                ),
                                "dt",
                                ImmutableList.of(
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
                    };
                });
    }

    private void assertPlanWithoutPushdownBelowScan(String mvName) throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

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
        System.out.println(plan);
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
        starRocksAssert.withTables(ImmutableList.of(
                        new MTable("tt1", "k1",
                                ImmutableList.of(
                                        "k1 int",
                                        "k2 int",
                                        "k3 string",
                                        "dt date"
                                ),
                                "dt",
                                ImmutableList.of(
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
                    };
                });
    }

    private void assertPlanWithPushdownBelowScan(String mvName) throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

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
        System.out.println(plan);
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
                    while (p2.getVisibleVersion()  != 3) {
                        System.out.println("waiting for partition p2 to be visible:" + p2.getVisibleVersion());
                        Thread.sleep(1000);
                    }
                    partitionsToRefresh1 = getPartitionNamesToRefreshForMv(mv);
                    Assert.assertFalse(partitionsToRefresh1.isEmpty());

                    {
                        processor = refreshMV("test", mv);
                        RuntimeProfile runtimeProfile = processor.getRuntimeProfile();
                        QueryMaterializationContext.QueryCacheStats queryCacheStats = getQueryCacheStats(runtimeProfile);
                        String key = String.format("cache_getUpdatedPartitionNames_%s_%s", mv.getId(), table.getId());
                        Assert.assertTrue(queryCacheStats != null);
                        Assert.assertTrue(queryCacheStats.getCounter().containsKey(key));
                        Assert.assertTrue(queryCacheStats.getCounter().get(key) == 1);
                    }
                });
        starRocksAssert.dropTable("tbl1");
    }
}