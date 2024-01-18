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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.implementation.HashJoinImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.OlapScanImplementationRule;
import com.starrocks.sql.optimizer.rule.tree.PredicateReorderRule;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;
import static org.junit.Assert.assertEquals;

public class PredicateReorderRuleTest {

    private static ColumnRefFactory columnRefFactory;

    private static ColumnRefOperator v1;
    private static ColumnRefOperator v2;
    private static ColumnRefOperator v3;
    private static ColumnRefOperator v4;
    private static ColumnRefOperator v5;
    private static ColumnRefOperator v6;
    private static ColumnRefOperator v7;

    private static Statistics statistics;
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static SessionVariable sessionVariable;

    @BeforeClass
    public static void beforeClass() throws Exception {

        Config.alter_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setEnableReplicationJoin(false);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                        "  `v1` int NULL COMMENT \"\",\n" +
                        "  `v3` decimal(5,2) NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`v1`, `v3`)\n" +
                        "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");")
                .withTable("CREATE TABLE `t1` (\n" +
                        "  `v1` int NULL COMMENT \"\",\n" +
                        "  `v3` decimal(5,2) NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`v1`, `v3`)\n" +
                        "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");");
        CreateDbStmt dbStmt = new CreateDbStmt(false, StatsConstants.STATISTICS_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().createDb(dbStmt.getFullDbName());
        } catch (DdlException e) {
            return;
        }
        starRocksAssert.useDatabase(StatsConstants.STATISTICS_DB_NAME);
        starRocksAssert.withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        FeConstants.runningUnitTest = true;

        columnRefFactory = new ColumnRefFactory();

        v1 = columnRefFactory.create("v1", Type.INT, true);
        v2 = columnRefFactory.create("v2", Type.INT, true);
        v3 = columnRefFactory.create("v3", Type.DECIMALV2, true);
        v4 = columnRefFactory.create("v4", Type.BOOLEAN, true);
        v5 = columnRefFactory.create("v5", Type.BOOLEAN, false);
        v6 = columnRefFactory.create("v6", Type.BOOLEAN, false);
        v7 = columnRefFactory.create("v7", Type.DATETIME, false);

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(10000);
        builder.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(10, 21, 0, 10, 100)));
        builder.addColumnStatistics(ImmutableMap.of(v3, new ColumnStatistic(10.1, 21.2, 0, 10, 200)));
        builder.addColumnStatistics(ImmutableMap.of(v4, new ColumnStatistic(1, 1, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v5, new ColumnStatistic(0, 0, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v6, new ColumnStatistic(0, 1, 0, 10, 50)));
        //2003-10-11 - 2003-10-12
        builder.addColumnStatistics(ImmutableMap.of(v7, new ColumnStatistic(1065801600, 1065888000, 0, 10, 200)));
        statistics = builder.build();

        GlobalStateMgr catalog = GlobalStateMgr.getCurrentState();
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        OlapTable t0 = (OlapTable) catalog.getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) catalog.getDb("test").getTable("t1");
        cachedStatisticStorage.addColumnStatistic(t0, v1.getName(), statistics.getColumnStatistic(v1));
        cachedStatisticStorage.addColumnStatistic(t0, v2.getName(), statistics.getColumnStatistic(v2));
        cachedStatisticStorage.addColumnStatistic(t1, v1.getName(), statistics.getColumnStatistic(v1));
        cachedStatisticStorage.addColumnStatistic(t1, v2.getName(), statistics.getColumnStatistic(v2));
        catalog.setStatisticStorage(cachedStatisticStorage);

        sessionVariable = new SessionVariable();

    }

    @Test
    public void testOlapScanPredicateReorder() {

        sessionVariable.enablePredicateReorder();

        OlapTable t0 = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test").getTable("t0");

        PredicateReorderRule predicateReorderRule = new PredicateReorderRule(sessionVariable);

        ConstantOperator constantOperatorInt0 = ConstantOperator.createInt(15);
        ConstantOperator constantOperatorInt1 = ConstantOperator.createDouble(23);

        // v1 < 15
        BinaryPredicateOperator intLt0 = BinaryPredicateOperator.lt(v1, constantOperatorInt0);
        // v2 > 23
        BinaryPredicateOperator intGt0 = BinaryPredicateOperator.gt(v2, constantOperatorInt1);

        ScalarOperator and = CompoundPredicateOperator.and(intLt0, intGt0);
        ScalarOperator or = CompoundPredicateOperator.or(intLt0, intGt0);
        ScalarOperator notLt = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                intLt0);
        ScalarOperator notGt = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                intGt0);

        OlapScanImplementationRule olapScanImplementationRule = new OlapScanImplementationRule();
        OptExpression optExpression1 = new OptExpression(
                new LogicalOlapScanOperator(t0, Maps.newHashMap(), Maps.newHashMap(), null, -1, and)
        );
        optExpression1 = olapScanImplementationRule.transform(optExpression1, null).get(0);
        optExpression1.setStatistics(statistics);

        OptExpression optExpression2 = new OptExpression(
                new LogicalOlapScanOperator(t0, Maps.newHashMap(), Maps.newHashMap(), null, -1, or)
        );
        optExpression2 = olapScanImplementationRule.transform(optExpression2, null).get(0);
        optExpression2.setStatistics(statistics);

        OptExpression optExpression3 = new OptExpression(
                new LogicalOlapScanOperator(t0, Maps.newHashMap(), Maps.newHashMap(), null, -1, notLt)
        );
        optExpression3 = olapScanImplementationRule.transform(optExpression3, null).get(0);
        optExpression3.setStatistics(statistics);

        OptExpression optExpression4 = new OptExpression(
                new LogicalOlapScanOperator(t0, Maps.newHashMap(), Maps.newHashMap(), null, -1, notGt)
        );
        optExpression4 = olapScanImplementationRule.transform(optExpression4, null).get(0);
        optExpression4.setStatistics(statistics);

        optExpression1 = predicateReorderRule.rewrite(optExpression1, null);
        assertEquals(optExpression1.getOp().getPredicate().getChild(0), intGt0);
        assertEquals(optExpression1.getOp().getPredicate().getChild(1), intLt0);

        optExpression2 = predicateReorderRule.rewrite(optExpression2, null);
        assertEquals(optExpression2.getOp().getPredicate().getChild(0), intLt0);
        assertEquals(optExpression2.getOp().getPredicate().getChild(1), intGt0);

        optExpression3 = predicateReorderRule.rewrite(optExpression3, null);
        assertEquals(optExpression3.getOp().getPredicate().getChild(0), intLt0);

        optExpression4 = predicateReorderRule.rewrite(optExpression4, null);
        assertEquals(optExpression4.getOp().getPredicate().getChild(0), intGt0);

        //test disable
        sessionVariable.disablePredicateReorder();
        optExpression1 = new OptExpression(
                new LogicalOlapScanOperator(t0, Maps.newHashMap(), Maps.newHashMap(), null, -1, and)
        );
        optExpression1 = olapScanImplementationRule.transform(optExpression1, null).get(0);
        optExpression1.setStatistics(statistics);
        optExpression1 = predicateReorderRule.rewrite(optExpression1, null);
        assertEquals(optExpression1.getOp().getPredicate().getChild(0), intLt0);
        assertEquals(optExpression1.getOp().getPredicate().getChild(1), intGt0);
    }

    @Test
    public void testHashJoinPredicateReorder() {
        sessionVariable.enablePredicateReorder();

        OlapTable t0 = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test").getTable("t1");

        PredicateReorderRule predicateReorderRule = new PredicateReorderRule(sessionVariable);

        ConstantOperator constantOperatorInt0 = ConstantOperator.createInt(15);
        ConstantOperator constantOperatorDouble0 = ConstantOperator.createDouble(150);

        // t0.v1 + t1.v1 = 15
        CastOperator castOperatorV10 = new CastOperator(Type.BIGINT, v1);
        CastOperator castOperatorV11 = new CastOperator(Type.BIGINT, v1);
        CallOperator add0 = new CallOperator("add", Type.BIGINT, Lists.newArrayList(castOperatorV10, castOperatorV11));
        BinaryPredicateOperator intEq0 = BinaryPredicateOperator.eq(add0, constantOperatorInt0);
        // t0.v2 + t1.v2 = 150
        CastOperator castOperatorV20 = new CastOperator(Type.BIGINT, v2);
        CastOperator castOperatorV21 = new CastOperator(Type.BIGINT, v2);
        CallOperator add1 = new CallOperator("add", Type.BIGINT, Lists.newArrayList(castOperatorV20, castOperatorV21));
        BinaryPredicateOperator intEq1 = BinaryPredicateOperator.eq(add1, constantOperatorDouble0);

        ScalarOperator and = CompoundPredicateOperator.and(intEq0, intEq1);
        ScalarOperator or = CompoundPredicateOperator.or(intEq0, intEq1);

        OlapScanImplementationRule olapScanImplementationRule = new OlapScanImplementationRule();
        OptExpression optExpressionT0 = new OptExpression(
                new LogicalOlapScanOperator(t0, Maps.newHashMap(), Maps.newHashMap(), null, -1, null)
        );
        optExpressionT0 = olapScanImplementationRule.transform(optExpressionT0, null).get(0);
        optExpressionT0.setStatistics(statistics);
        OptExpression optExpressionT1 = new OptExpression(
                new LogicalOlapScanOperator(t1, Maps.newHashMap(), Maps.newHashMap(), null, -1, null)
        );
        optExpressionT1 = olapScanImplementationRule.transform(optExpressionT1, null).get(0);
        optExpressionT1.setStatistics(statistics);

        HashJoinImplementationRule hashJoinImplementationRule = HashJoinImplementationRule.getInstance();

        LogicalJoinOperator.Builder builder1 = new LogicalJoinOperator.Builder();
        builder1.setPredicate(and);
        LogicalJoinOperator logicalJoinOperator1 = builder1.build();
        OptExpression optExpression1 =
                hashJoinImplementationRule.transform(new OptExpression(logicalJoinOperator1), null).get(0);
        optExpression1.getInputs().add(optExpressionT0);
        optExpression1.getInputs().add(optExpressionT1);

        LogicalJoinOperator.Builder builder2 = new LogicalJoinOperator.Builder();
        builder2.setPredicate(or);
        LogicalJoinOperator logicalJoinOperator2 = builder2.build();
        OptExpression optExpression2 =
                hashJoinImplementationRule.transform(new OptExpression(logicalJoinOperator2), null).get(0);
        optExpression2.getInputs().add(optExpressionT0);
        optExpression2.getInputs().add(optExpressionT1);
        //
        optExpression1 = predicateReorderRule.rewrite(optExpression1, null);
        assertEquals(optExpression1.getOp().getPredicate().getChild(0), intEq1);
        assertEquals(optExpression1.getOp().getPredicate().getChild(1), intEq0);

        optExpression2 = predicateReorderRule.rewrite(optExpression2, null);
        assertEquals(optExpression2.getOp().getPredicate().getChild(0), intEq0);
        assertEquals(optExpression2.getOp().getPredicate().getChild(1), intEq1);

        sessionVariable.disablePredicateReorder();
    }
}
