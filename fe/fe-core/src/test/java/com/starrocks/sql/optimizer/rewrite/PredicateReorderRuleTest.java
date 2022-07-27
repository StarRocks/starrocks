package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.physical.PredicateReorderRule;
import com.starrocks.sql.optimizer.rule.implementation.HashJoinImplementationRule;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PredicateReorderRuleTest {

    private static ColumnRefOperator v1;
    private static ColumnRefOperator v2;

    private static Statistics statistics;
    private static TaskContext taskContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        String DB_NAME = "test";
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        taskContext = new TaskContext(new OptimizerContext(null, null, connectContext),
                null, null, Double.MAX_VALUE);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                        "  `v1` int NULL COMMENT \"\",\n" +
                        "  `v3` decimal(5,2) NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`v1`, `v3`)\n" +
                        "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");")
                .withTable("CREATE TABLE `t1` (\n" +
                        "  `v1` int NULL COMMENT \"\",\n" +
                        "  `v3` decimal(5,2) NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`v1`, `v3`)\n" +
                        "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");");
        FeConstants.runningUnitTest = true;
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();

        v1 = columnRefFactory.create("v1", Type.INT, true);
        v2 = columnRefFactory.create("v2", Type.INT, true);

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(10000);
        builder.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(10, 21, 0, 10, 100)));
        statistics = builder.build();

        GlobalStateMgr catalog = GlobalStateMgr.getCurrentState();
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        OlapTable t1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t1");
        cachedStatisticStorage.addColumnStatistic(t0, v1.getName(), statistics.getColumnStatistic(v1));
        cachedStatisticStorage.addColumnStatistic(t0, v2.getName(), statistics.getColumnStatistic(v2));
        cachedStatisticStorage.addColumnStatistic(t1, v1.getName(), statistics.getColumnStatistic(v1));
        cachedStatisticStorage.addColumnStatistic(t1, v2.getName(), statistics.getColumnStatistic(v2));
        catalog.setStatisticStorage(cachedStatisticStorage);
    }

    @Test
    public void testOlapScanPredicateReorder() {
        taskContext.getOptimizerContext().getSessionVariable().enablePredicateReorder();
        OlapTable t0 = (OlapTable) GlobalStateMgr.getCurrentState().getDb("default_cluster:test").getTable("t0");
        PredicateReorderRule predicateReorderRule = new PredicateReorderRule();

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

        OptExpression optExpression1 = new OptExpression(
                new PhysicalOlapScanOperator(t0, Maps.newHashMap(), null, -1, and, 0, Lists.newArrayList(), Lists.newArrayList(), null)
        );
        optExpression1.setStatistics(statistics);

        OptExpression optExpression2 = new OptExpression(
                new PhysicalOlapScanOperator(t0, Maps.newHashMap(), null, -1, or, 0, Lists.newArrayList(), Lists.newArrayList(), null)
        );
        optExpression2.setStatistics(statistics);

        OptExpression optExpression3 = new OptExpression(
                new PhysicalOlapScanOperator(t0, Maps.newHashMap(), null, -1, notLt, 0, Lists.newArrayList(), Lists.newArrayList(), null)
        );
        optExpression3.setStatistics(statistics);

        OptExpression optExpression4 = new OptExpression(
                new PhysicalOlapScanOperator(t0, Maps.newHashMap(), null, -1, notGt, 0, Lists.newArrayList(), Lists.newArrayList(), null)
        );
        optExpression4.setStatistics(statistics);

        optExpression1 = predicateReorderRule.rewrite(optExpression1, taskContext);
        assertEquals(optExpression1.getOp().getPredicate().getChild(0), intGt0);
        assertEquals(optExpression1.getOp().getPredicate().getChild(1), intLt0);

        optExpression2 = predicateReorderRule.rewrite(optExpression2, taskContext);
        assertEquals(optExpression2.getOp().getPredicate().getChild(0), intLt0);
        assertEquals(optExpression2.getOp().getPredicate().getChild(1), intGt0);

        optExpression3 = predicateReorderRule.rewrite(optExpression3, taskContext);
        assertEquals(optExpression3.getOp().getPredicate().getChild(0), intLt0);

        optExpression4 = predicateReorderRule.rewrite(optExpression4, taskContext);
        assertEquals(optExpression4.getOp().getPredicate().getChild(0), intGt0);

        //test disable
        taskContext.getOptimizerContext().getSessionVariable().disablePredicateReorder();
        optExpression1 = new OptExpression(
                new PhysicalOlapScanOperator(t0, Maps.newHashMap(), null, -1, and, 0, Lists.newArrayList(), Lists.newArrayList(), null)
        );
        optExpression1.setStatistics(statistics);
        optExpression1 = predicateReorderRule.rewrite(optExpression1, taskContext);
        assertEquals(optExpression1.getOp().getPredicate().getChild(0), intLt0);
        assertEquals(optExpression1.getOp().getPredicate().getChild(1), intGt0);
    }

    @Test
    public void testHashJoinPredicateReorder() {
        taskContext.getOptimizerContext().getSessionVariable().enablePredicateReorder();

        OlapTable t0 = (OlapTable) GlobalStateMgr.getCurrentState().getDb("default_cluster:test").getTable("t0");
        OlapTable t1 = (OlapTable) GlobalStateMgr.getCurrentState().getDb("default_cluster:test").getTable("t1");

        PredicateReorderRule predicateReorderRule = new PredicateReorderRule();

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

        OptExpression optExpressionT0 = new OptExpression(
                new PhysicalOlapScanOperator(t0, Maps.newHashMap(), null, -1, null, 0, Lists.newArrayList(), Lists.newArrayList(), null)
        );
        optExpressionT0.setStatistics(statistics);
        OptExpression optExpressionT1 = new OptExpression(
                new PhysicalOlapScanOperator(t1, Maps.newHashMap(), null, -1, null, 0, Lists.newArrayList(), Lists.newArrayList(), null)
        );
        optExpressionT1.setStatistics(statistics);

        HashJoinImplementationRule hashJoinImplementationRule = HashJoinImplementationRule.getInstance();

        LogicalJoinOperator.Builder builder1 = new LogicalJoinOperator.Builder();
        builder1.setPredicate(and);
        LogicalJoinOperator logicalJoinOperator1 = builder1.build();
        OptExpression optExpression1 =
                hashJoinImplementationRule.transform(new OptExpression(logicalJoinOperator1), null).get(0);
        optExpression1.getInputs().add(optExpressionT0);
        optExpression1.getInputs().add(optExpressionT1);
        optExpression1.setStatistics(statistics);

        LogicalJoinOperator.Builder builder2 = new LogicalJoinOperator.Builder();
        builder2.setPredicate(or);
        LogicalJoinOperator logicalJoinOperator2 = builder2.build();
        OptExpression optExpression2 =
                hashJoinImplementationRule.transform(new OptExpression(logicalJoinOperator2), null).get(0);
        optExpression2.getInputs().add(optExpressionT0);
        optExpression2.getInputs().add(optExpressionT1);
        optExpression2.setStatistics(statistics);

        optExpression1 = predicateReorderRule.rewrite(optExpression1, taskContext);
        assertEquals(optExpression1.getOp().getPredicate().getChild(0), intEq1);
        assertEquals(optExpression1.getOp().getPredicate().getChild(1), intEq0);

        optExpression2 = predicateReorderRule.rewrite(optExpression2, taskContext);
        assertEquals(optExpression2.getOp().getPredicate().getChild(0), intEq0);
        assertEquals(optExpression2.getOp().getPredicate().getChild(1), intEq1);

        taskContext.getOptimizerContext().getSessionVariable().disablePredicateReorder();
    }
}
