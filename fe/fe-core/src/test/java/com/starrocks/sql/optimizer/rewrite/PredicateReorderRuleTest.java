package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import mockit.Mocked;
import org.junit.BeforeClass;
import org.junit.Test;

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

    @BeforeClass
    public static void beforeClass() throws Exception {
        columnRefFactory = new ColumnRefFactory();

        v1 = columnRefFactory.create("v1", Type.INT, true);
        v2 = columnRefFactory.create("v2", Type.DOUBLE, true);
        v3 = columnRefFactory.create("v3", Type.DECIMALV2, true);
        v4 = columnRefFactory.create("v4", Type.BOOLEAN, true);
        v5 = columnRefFactory.create("v5", Type.BOOLEAN, false);
        v6 = columnRefFactory.create("v6", Type.BOOLEAN, false);
        v7 = columnRefFactory.create("v7", Type.DATETIME, false);

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(10000);
        builder.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(10, 21, 0, 10, 100)));
        builder.addColumnStatistics(ImmutableMap.of(v3, new ColumnStatistic(15.23, 300.12, 0, 10, 200)));
        builder.addColumnStatistics(ImmutableMap.of(v4, new ColumnStatistic(1, 1, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v5, new ColumnStatistic(0, 0, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v6, new ColumnStatistic(0, 1, 0, 10, 50)));
        //2003-10-11 - 2003-10-12
        builder.addColumnStatistics(ImmutableMap.of(v7, new ColumnStatistic(1065801600, 1065888000, 0, 10, 200)));
        statistics = builder.build();
    }

    @Test
    public void testOlapScanPredicateReorder(@Mocked OlapTable olapTable1) {

        PredicateReorderRule predicateReorderRule = new PredicateReorderRule();

        ConstantOperator constantOperatorInt0 = ConstantOperator.createInt(15);
        ConstantOperator constantOperatorDouble0 = ConstantOperator.createDouble(23);

        // v1 < 15
        BinaryPredicateOperator intLt0 = BinaryPredicateOperator.lt(v1, constantOperatorInt0);
        // v2 > 23
        BinaryPredicateOperator doubleGt0 = BinaryPredicateOperator.gt(v2, constantOperatorDouble0);

        ScalarOperator and = CompoundPredicateOperator.and(intLt0, doubleGt0);
        ScalarOperator or = CompoundPredicateOperator.or(intLt0, doubleGt0);
        ScalarOperator notLt = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                intLt0);
        ScalarOperator notGt = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                doubleGt0);

        OptExpression optExpression1 = new OptExpression(
                new LogicalOlapScanOperator(olapTable1, Maps.newHashMap(), Maps.newHashMap(), null, -1, and)
        );
        optExpression1.setStatistics(statistics);
        OptExpression optExpression2 = new OptExpression(
                new LogicalOlapScanOperator(olapTable1, Maps.newHashMap(), Maps.newHashMap(), null, -1, or)
        );
        optExpression2.setStatistics(statistics);
        OptExpression optExpression3 = new OptExpression(
                new LogicalOlapScanOperator(olapTable1, Maps.newHashMap(), Maps.newHashMap(), null, -1, notLt)
        );
        optExpression3.setStatistics(statistics);
        OptExpression optExpression4 = new OptExpression(
                new LogicalOlapScanOperator(olapTable1, Maps.newHashMap(), Maps.newHashMap(), null, -1, notGt)
        );
        optExpression4.setStatistics(statistics);


        optExpression1 = predicateReorderRule.rewrite(optExpression1, null);
        assertEquals(optExpression1.getOp().getPredicate().getChild(0), doubleGt0);
        assertEquals(optExpression1.getOp().getPredicate().getChild(1), intLt0);

        optExpression2 = predicateReorderRule.rewrite(optExpression2, null);
        assertEquals(optExpression2.getOp().getPredicate().getChild(0), intLt0);
        assertEquals(optExpression2.getOp().getPredicate().getChild(1), doubleGt0);

        optExpression3 = predicateReorderRule.rewrite(optExpression3, null);
        assertEquals(optExpression3.getOp().getPredicate().getChild(0), intLt0);

        optExpression4 = predicateReorderRule.rewrite(optExpression4, null);
        assertEquals(optExpression4.getOp().getPredicate().getChild(0), doubleGt0);

    }
}
