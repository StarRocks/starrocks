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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mv.analyzer.MVPartitionExpr;
import com.starrocks.mv.analyzer.MVPartitionExprResolver;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.PartitionBasedMvRefreshProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class MVPartitionExprResolverTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        // create connect context
        MVTestBase.beforeClass();
        starRocksAssert.withDatabase("test").useDatabase("test");
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
        ;
    }

    @Test
    public void testGetSupportMvPartitionExpr() {
        // Set up test data for SlotRef
        SlotRef slotRef = new SlotRef(new TableName("db", "table"), "column");

        // Execute the method under test
        MVPartitionExpr result = MVPartitionExpr.getSupportMvPartitionExpr(slotRef);

        // Assertions
        Assert.assertNotNull(result);
        Assert.assertEquals(slotRef, result.getExpr());
        Assert.assertEquals(slotRef, result.getSlotRef());

        // Set up test data for FunctionCallExpr
        slotRef = new SlotRef(new TableName("db", "table"), "column");
        StringLiteral day = new StringLiteral("day");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_trunc", Lists.newArrayList(day, slotRef));

        // Execute the method under test
        result = MVPartitionExpr.getSupportMvPartitionExpr(functionCallExpr);

        // Assertions
        Assert.assertNotNull(result);
        Assert.assertEquals(functionCallExpr, result.getExpr());
        Assert.assertEquals(slotRef, result.getSlotRef());
    }

    private SlotRef makeMvSlotRef(String tableName, String columnName) {
        SlotRef slotRef = new SlotRef(new TableName("test", tableName), columnName, columnName);
        slotRef.getTblNameWithoutAnalyzed().normalization(connectContext);
        slotRef.setType(Type.DATE);
        return slotRef;
    }

    @Test
    public void testMVMultiPartitionResolverBasic() {
        SlotRef slot1 = makeMvSlotRef("tbl1", "k1");
        SlotRef slot2 = makeMvSlotRef("tbl2", "k1");

        Map<Expr, SlotRef> result;
        {
            SlotRef slot = makeMvSlotRef("t1", "k1");
            String sql = "select t1.k1, t2.k2 from tbl1 t1 join tbl2 t2 on t1.k1 = t2.k1 and t1.v1 = t2.v1;";
            result = checkMVPartitionExprs(sql, slot, 2);
            Assert.assertTrue(result.containsKey(slot1));
            Assert.assertTrue(result.containsKey(slot2));
        }

        {
            String sql = "select k1, k2 from tbl1 union select k1, k2 from tbl2";
            checkMVPartitionExprs(sql, slot1, 2);
        }
    }

    @Test
    public void testMVMultiPartitionResolverWithDateTruc() {
        String sql = "select t1.k1, t2.k1, t2.k2 from tbl1 t1 join tbl2 t2 on t1.k1 = date_trunc('day', t2.k1);";

        {
            StringLiteral day = new StringLiteral("day");
            SlotRef slot = makeMvSlotRef("t2", "k1");
            FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_trunc", Lists.newArrayList(day, slot));
            checkMVPartitionExprs(sql, functionCallExpr, 2);
        }

        {
            SlotRef slot = makeMvSlotRef("t1", "k1");
            checkMVPartitionExprs(sql, slot, 2);
        }
    }

    @Test
    public void testMVMultiPartitionResolverWithAlias() {
        SlotRef slot = makeMvSlotRef("t", "k1");
        String[] sqls = {
                "select k1 from (select k1, k2 from tbl1 union select k1, k2 from tbl2) t group by k1",
                "select k1 from " +
                        "(" +
                        "   select k1, k2 from tbl1 " +
                        " union " +
                        "   select k1, k2 from tbl2 " +
                        " union all " +
                        "   select k1, k2 from tbl1" +
                        ") t group by k1",
                "select k1 from " +
                        "(" +
                        "   select t1.k1, t2.k2 from tbl1 t1 join tbl2 t2 on t1.k1=t2.k1" +
                        ") t group by k1",
        };
        for (String sql : sqls) {
            System.out.println(sql);
            checkMVPartitionExprs(sql, slot, 2);
        }
    }

    @Test
    public void testMVMultiPartitionResolverWithNestedUnion() {
        SlotRef slot = makeMvSlotRef("t", "k1");
        String sql = "select k1 from " +
                " (" +
                " select k1, k2 from tbl1 union all select k1, k2 from tbl2 " +
                "   union all " +
                " select k1, k2 from tbl1 union all select k1, k2 from tbl2 " +
                " ) t group by k1";
        checkMVPartitionExprs(sql, slot, 2);
    }

    private QueryStatement getQueryStatement(String sql) {
        try {
            return (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        return null;
    }

    private Map<Expr, SlotRef> checkMVPartitionExprs(String sql, Expr slot, int expect) {
        QueryStatement query = getQueryStatement(sql);
        Map<Expr, SlotRef> result = MVPartitionExprResolver.getMVPartitionExprsChecked(
                Lists.newArrayList(slot), query, null);
        Assert.assertEquals(expect, result.size());
        return result;
    }

    @Test
    public void testFilterPartitionByUnion() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        starRocksAssert.withMaterializedView(
                "create materialized view test.mv_union_filter\n" +
                        "partition by k1 \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '10000')\n" +
                        "refresh deferred manual\n" +
                        "as select k1, k2 from test.tbl15 \n" +
                        "union " +
                        "select k1, k2 from test.tbl16;", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_union_filter"));
                    Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());

                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                    taskRun.executeTaskRun();
                    PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                    Map<Table, Set<String>> baseTables = getRefTableRefreshedPartitions(processor);
                    Assert.assertEquals(2, baseTables.size());
                    Assert.assertEquals(Sets.newHashSet("p20220101"), baseTables.get(
                            GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "tbl15")));
                    Assert.assertEquals(Sets.newHashSet("p20220101"), baseTables.get(
                            GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "tbl16")));

                    // insert new data into tbl16's p20220202 partition
                    String insertSql = "insert into tbl16 partition(p20220202) values('2022-02-02', 3, 10);";
                    new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                            insertSql, connectContext.getSessionVariable().getSqlMode())).execute();
                    taskRun.executeTaskRun();
                    Assert.assertEquals(Sets.newHashSet("p20220202"),
                            processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
                    Assert.assertEquals("{tbl15=[p20220202], tbl16=[p20220202]}",
                            processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
                });

        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_union_filter\n" +
                        "partition by k1 \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '10000')\n" +
                        "refresh deferred manual\n" +
                        "as " +
                        "select date_trunc('month', k1) as k1, k2 from test.tbl16\n" +
                        "union " +
                        "select k1, k2 from test.tbl15;", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_union_filter"));
                    Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());

                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                    taskRun.executeTaskRun();

                    // insert new data into tbl16's p20220202 partition
                    String insertSql = "insert into tbl16 partition(p20220202) values('2022-02-02', 3, 10);";
                    new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                            insertSql, connectContext.getSessionVariable().getSqlMode())).execute();
                    taskRun.executeTaskRun();
                    PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                    Assert.assertEquals(Sets.newHashSet("p202202_202203"),
                            processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
                    Assert.assertEquals("{tbl15=[p20220202, p20220201], tbl16=[p20220202, p20220201]}",
                            processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
                });
    }

    @Test
    public void testFilterPartitionByJoinPredicate1() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        //normal case, join predicate is partition column
        // a.k1 = date_trunc(month, b.k1)
        starRocksAssert.withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1 \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '10000')" +
                        "refresh deferred manual\n" +
                        "as select a.k1, b.k2 from test.tbl15 as a join test.tbl16 as b " +
                        "on a.k1 = date_trunc('month', b.k1);", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_join_predicate"));
                    Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());

                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    {
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        taskRun.executeTaskRun();
                        PartitionBasedMvRefreshProcessor processor =
                                (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                        Map<Table, Set<String>> baseTables = getRefTableRefreshedPartitions(processor);
                        Assert.assertEquals(2, baseTables.size());
                        Assert.assertEquals(Sets.newHashSet("p20220101"),
                                baseTables.get(GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getTable(testDb.getFullName(), "tbl15")));
                        Assert.assertEquals(Sets.newHashSet("p20220101", "p20220102", "p20220103"),
                                baseTables.get(GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getTable(testDb.getFullName(), "tbl16")));
                        Assert.assertTrue(processor.getNextTaskRun() == null);
                    }

                    {
                        // insert new data into tbl16's p20220202 partition
                        String insertSql = "insert into tbl16 partition(p20220202) values('2022-02-02', 3, 10);";
                        new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                                insertSql, connectContext.getSessionVariable().getSqlMode())).execute();
                        taskRun.executeTaskRun();
                        PartitionBasedMvRefreshProcessor processor =
                                (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                        // 1. updated partition of tbl16 is p20220202
                        // 2. date_trunc('month', p20220202) is '2022-02'
                        // 3. tbl15's associated partitions are p20220201 and p20220202
                        Assert.assertEquals(Sets.newHashSet("p20220202", "p20220201"),
                                processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
                        Assert.assertEquals("{tbl15=[p20220202, p20220201], tbl16=[p20220202, p20220201]}",
                                processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
                    }
                });
    }

    @Test
    public void testFilterPartitionByJoinPredicate3() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        // join predicate has no equal condition
        starRocksAssert.withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1 \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '10000')" +
                        "refresh deferred manual\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k1 = tbl2.k1 or tbl1.k2 = tbl2.k2;",
                () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_join_predicate"));
                    Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                    taskRun.executeTaskRun();

                    {
                        executeInsertSql(connectContext, "insert into tbl1 partition(p2) values('2022-02-02', 3, 10);");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        taskRun.executeTaskRun();
                        PartitionBasedMvRefreshProcessor processor =
                                (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                        Assert.assertEquals(Sets.newHashSet("p2"),
                                processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                        Assert.assertTrue(execPlan != null);
                        Assert.assertEquals("{tbl1=[p2]}",
                                processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
                    }

                    {
                        executeInsertSql(connectContext, "insert into tbl2 partition(p2) values('2022-02-02', 3, 10);");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        taskRun.executeTaskRun();
                        PartitionBasedMvRefreshProcessor processor =
                                (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                        Assert.assertTrue(execPlan != null);
                        assertPlanContains(execPlan, "partitions=5/5\n     rollup: tbl1",
                                "partitions=2/2\n     rollup: tbl2");
                    }
                });
    }

    @Test
    public void testFilterPartitionByJoinPredicate31() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        // join predicate is not mv partition expr
        starRocksAssert.withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by date_trunc('month', k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '100')" +
                        "refresh deferred manual\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 using(k1);", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_join_predicate"));
                    Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());
                });
    }

    @Test
    public void testFilterPartitionByJoinPredicate4() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        // nest table alias join
        starRocksAssert.withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by date_trunc('month', k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "as select a.k1, b.k2 from test.tbl15 as a join test.tbl16 as b " +
                        "on date_trunc('month', a.k1) = b.k1;", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_join_predicate"));
                    Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                    taskRun.executeTaskRun();

                    new StmtExecutor(connectContext, SqlParser.parseSingleStatement(
                            "insert into tbl15 partition(p20220202) values('2022-02-02', 3, 10);",
                            connectContext.getSessionVariable().getSqlMode())).execute();
                    taskRun.executeTaskRun();
                    PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                    Assert.assertEquals(Sets.newHashSet("p202201_202202", "p202202_202203"),
                            processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
                    Assert.assertEquals("{tbl15=[p20220103, p20220202, p20220102, p20220201, p20220101]}",
                            processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
                });
    }

    @Test
    public void testFilterPartitionByJoinPredicate5() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        // nest table alias join
        starRocksAssert.withMaterializedView(
                "create materialized view test.mv_join_predicate\n" +
                        "partition by k1\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "PROPERTIES('partition_refresh_number' = '100')" +
                        "refresh deferred manual\n" +
                        "as select a.ds as k1, a.k2 from" +
                        "(" +
                        "select date_trunc('DAY', k1) as ds, k1, k2 from " +
                        "  (select k1, k2 from (select * from tbl1)t1 )t2 ) a " +
                        "left join " +
                        "  (select date_trunc('DAY', k1) as ds, k2 from (select * from tbl2)t ) b " +
                        "on date_trunc('DAY', a.k1) = b.ds and a.k2 = b.k2", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_join_predicate"));
                    Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());
                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                    taskRun.executeTaskRun();

                    executeInsertSql(connectContext, "insert into tbl2 partition(p1) values('2022-01-02', 3, 10);");
                    taskRun.executeTaskRun();
                    PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                    Set<String> mvPartitionsToRefresh = processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh();
                    System.out.println(mvPartitionsToRefresh);
                    Assert.assertTrue(mvPartitionsToRefresh.contains("p20220101_20220102"));
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
                });
    }

    @Test
    public void testFilterPartitionByJoinPredicate6() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
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
                        "on date_trunc('day', a.ds) = b.ds and a.k2 = b.k2 ;", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_join_predicate"));
                    Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
                });
    }

    @Test
    public void testFilterPartitionByJoinPredicate7() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        // duplicate table join
        starRocksAssert.withMaterializedView(
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
                        "on a.ds = c.ds and a.k2 = c.k2;", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_join_predicate"));
                    Assert.assertEquals(2, materializedView.getPartitionExprMaps().size());
                });
    }

    @Test
    public void testFilterPartitionByJoinPredicate8() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withMaterializedView(
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
                        "on a.ds = c.ds;", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_join_predicate"));
                    Assert.assertEquals(1, materializedView.getPartitionExprMaps().size());
                });
    }

    @Test
    public void testFilterPartitionByJoinPredicate9() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        // unsupported function in join predicate
        starRocksAssert.withMaterializedView(
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
                        "on a.ds = c.ds and a.k2 = c.k2;", () -> {

                    MaterializedView materializedView =
                            ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), "mv_join_predicate"));
                    Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
                    TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
                    taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                    taskRun.executeTaskRun();

                    executeInsertSql(connectContext, "insert into tbl2 partition(p1) values('2022-01-02', 3, 10);");
                    taskRun.executeTaskRun();
                    PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
                    Assert.assertEquals(Sets.newHashSet("p1"),
                            processor.getMVTaskRunExtraMessage().getMvPartitionsToRefresh());
                    Assert.assertEquals("{tbl2=[p1], tbl1=[p1]}",
                            processor.getMVTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().toString());
                });
    }
}