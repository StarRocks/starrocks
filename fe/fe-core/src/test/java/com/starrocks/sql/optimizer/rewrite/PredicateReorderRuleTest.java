package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.implementation.OlapScanImplementationRule;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.Constants;
import com.starrocks.system.SystemInfoService;
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

        FeConstants.default_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        String DB_NAME = "test";
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(10000000000L);
        connectContext.getSessionVariable().setEnableReplicationJoin(false);

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
                ");");
        CreateDbStmt dbStmt = new CreateDbStmt(false, Constants.StatisticsDBName);
        dbStmt.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        try {
            Catalog.getCurrentCatalog().createDb(dbStmt);
        } catch (DdlException e) {
            return;
        }
        starRocksAssert.useDatabase(Constants.StatisticsDBName);
        starRocksAssert.withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        FeConstants.runningUnitTest = true;

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
        builder.addColumnStatistics(ImmutableMap.of(v3, new ColumnStatistic(10.1, 21.2, 0, 10, 200)));
        builder.addColumnStatistics(ImmutableMap.of(v4, new ColumnStatistic(1, 1, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v5, new ColumnStatistic(0, 0, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v6, new ColumnStatistic(0, 1, 0, 10, 50)));
        //2003-10-11 - 2003-10-12
        builder.addColumnStatistics(ImmutableMap.of(v7, new ColumnStatistic(1065801600, 1065888000, 0, 10, 200)));
        statistics = builder.build();

        Catalog catalog = Catalog.getCurrentCatalog();
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        cachedStatisticStorage.addColumnStatistic(t0, v1.getName(), statistics.getColumnStatistic(v1));
        cachedStatisticStorage.addColumnStatistic(t0, v3.getName(), statistics.getColumnStatistic(v3));
        catalog.setStatisticStorage(cachedStatisticStorage);

        sessionVariable = new SessionVariable();
        sessionVariable.enablePredicateReorder();

    }

    @Test
    public void testOlapScanPredicateReorder() throws Exception {

        OlapTable olapTable1 = (OlapTable) Catalog.getCurrentCatalog().getDb("default_cluster:test").getTable("t0");

        PredicateReorderRule predicateReorderRule = new PredicateReorderRule(sessionVariable);

        ConstantOperator constantOperatorInt0 = ConstantOperator.createInt(15);
        ConstantOperator constantOperatorDouble0 = ConstantOperator.createDouble(23);

        // v1 < 15
        BinaryPredicateOperator intLt0 = BinaryPredicateOperator.lt(v1, constantOperatorInt0);
        // v2 > 23
        BinaryPredicateOperator doubleGt0 = BinaryPredicateOperator.gt(v3, constantOperatorDouble0);

        ScalarOperator and = CompoundPredicateOperator.and(intLt0, doubleGt0);
        ScalarOperator or = CompoundPredicateOperator.or(intLt0, doubleGt0);
        ScalarOperator notLt = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                intLt0);
        ScalarOperator notGt = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                doubleGt0);

        OlapScanImplementationRule olapScanImplementationRule = new OlapScanImplementationRule();
        OptExpression optExpression1 = new OptExpression(
                new LogicalOlapScanOperator(olapTable1, Maps.newHashMap(), Maps.newHashMap(), null, -1, and)
        );
        optExpression1 = olapScanImplementationRule.transform(optExpression1,null).get(0);
        optExpression1.setStatistics(statistics);

        OptExpression optExpression2 = new OptExpression(
                new LogicalOlapScanOperator(olapTable1, Maps.newHashMap(), Maps.newHashMap(), null, -1, or)
        );
        optExpression2 = olapScanImplementationRule.transform(optExpression2,null).get(0);
        optExpression2.setStatistics(statistics);

        OptExpression optExpression3 = new OptExpression(
                new LogicalOlapScanOperator(olapTable1, Maps.newHashMap(), Maps.newHashMap(), null, -1, notLt)
        );
        optExpression3 = olapScanImplementationRule.transform(optExpression3,null).get(0);
        optExpression3.setStatistics(statistics);

        OptExpression optExpression4 = new OptExpression(
                new LogicalOlapScanOperator(olapTable1, Maps.newHashMap(), Maps.newHashMap(), null, -1, notGt)
        );
        optExpression4 = olapScanImplementationRule.transform(optExpression4,null).get(0);
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
