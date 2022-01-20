// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CTEPlanTest extends PlanTestBase {
    private static class TestStorage extends EmptyStatisticStorage {
        @Override
        public ColumnStatistic getColumnStatistic(Table table, String column) {
            return new ColumnStatistic(0, 2000000, 0, 8, 2000000);
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setCboCteReuse(true);
        connectContext.getSessionVariable().setEnablePipelineEngine(true);

        Catalog catalog = connectContext.getCatalog();
        catalog.setStatisticStorage(new TestStorage());

        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(t0, 20000000);

        OlapTable t1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t1");
        setTableStatistics(t1, 2000000);
    }

    @Test
    public void testMultiFlatCTE() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from t1) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM"));
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    RANDOM\n" +
                "\n" +
                "  5:OlapScanNode\n" +
                "     TABLE: t1"));
    }

    @Test
    public void testMultiContainsCTE() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from x0) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    RANDOM"));
    }

    @Test
    public void testFromUseCte() throws Exception {
        String sql = "with x0 as (select * from t0) " +
                "select * from (with x1 as (select * from t1) select * from x1 join x0 on x1.v4 = x0.v1) tt";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v4 = 10: v1"));
        Assert.assertFalse(plan.contains("MultiCastDataSinks"));
    }

    @Test
    public void testSubqueryUserSameCTE() throws Exception {
        String sql = "with x0 as (select * from t0) " +
                "select * from x0 x,t1 y where v1 in (select v2 from x0 z where z.v1 = x.v1)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED"));

        sql = "with x0 as (select * from t0) " +
                "select * from x0 t,t1 where v1 in (select v2 from x0 where t.v1 = v1)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED"));
    }

    @Test
    public void testCTEJoinReorderLoseStatistics() throws Exception {
        connectContext.getSessionVariable().setMaxTransformReorderJoins(1);

        String sql = "with xx as (select * from t0) select * from xx as x0 join xx as x1 on x0.v1 = x1.v1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    UNPARTITIONED\n"));

        connectContext.getSessionVariable().setMaxTransformReorderJoins(4);
    }

    @Test
    public void testOneCTEInline() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from t1) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM"));
    }

    @Test
    public void testOneCTEInlineComplex() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from x0) " +
                "select * from x1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));
    }

    @Test
    public void testCTEPredicate() throws Exception {
        StatisticsEstimateCoefficient.DEFAULT_CTE_INLINE_COST_RATE = 0;
        String sql = "explain with xx as (select * from t0) " +
                "select x1.v1 from xx x1 join xx x2 on x1.v2=x2.v3 where x1.v3 = 4 and x2.v2=3;";
        String plan = getFragmentPlan(sql);
        StatisticsEstimateCoefficient.DEFAULT_CTE_INLINE_COST_RATE = 1.5;
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (2: v2 = 3) OR (3: v3 = 4)"));
    }

    @Test
    public void testCTELimit() throws Exception {
        StatisticsEstimateCoefficient.DEFAULT_CTE_INLINE_COST_RATE = 0;
        String sql = "with xx as (select * from t0) " +
                "select x1.v1 from (select * from xx limit 1) x1 " +
                "join (select * from xx limit 3) x2 on x1.v2=x2.v3;";
        String plan = getFragmentPlan(sql);
        StatisticsEstimateCoefficient.DEFAULT_CTE_INLINE_COST_RATE = 1.5;
        Assert.assertTrue(plan.contains("MultiCastDataSinks"));
        Assert.assertTrue(plan.contains("cardinality=1\n" +
                "     avgRowSize=24.0\n" +
                "     numNodes=0\n" +
                "     limit: 3"));
    }

    @Test
    public void testCTEPredicateLimit() throws Exception {
        StatisticsEstimateCoefficient.DEFAULT_CTE_INLINE_COST_RATE = 0;
        String sql = "with xx as (select * from t0) " +
                "select x1.v1 from " +
                "(select * from xx where xx.v2 = 2 limit 1) x1 join " +
                "(select * from xx where xx.v3 = 4 limit 3) x2 on x1.v2=x2.v3;";
        String plan = getFragmentPlan(sql);
        StatisticsEstimateCoefficient.DEFAULT_CTE_INLINE_COST_RATE = 1.5;
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (3: v3 = 4) OR (2: v2 = 2)\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=0\n" +
                "     avgRowSize=24.0\n" +
                "     numNodes=0\n" +
                "     limit: 3"));
    }

    @Test
    public void testCTEPruneColumns() throws Exception {
        StatisticsEstimateCoefficient.DEFAULT_CTE_INLINE_COST_RATE = 0;
        String sql = "with xx as (select * from t0) select v1 from xx union all select v2 from xx;";
        String plan = getFragmentPlan(sql);
        StatisticsEstimateCoefficient.DEFAULT_CTE_INLINE_COST_RATE = 1.5;
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    RANDOM"));
    }

    @Test
    public void testComplexCTE() throws Exception {
        String sql = "WITH s AS (select * from t0), \n" +
                "  a AS (select * from s), \n" +
                "  a2 AS (select * from s), \n" +
                "  b AS (select v3, v1, v2 from s\n" +
                "    UNION\n" +
                "    select v3 + 1, v1 + 2, v2 + 3 from s)\n" +
                "  select * from b;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    HASH_PARTITIONED: <slot 24>, <slot 22>, <slot 23>\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    HASH_PARTITIONED: <slot 28>, <slot 29>, <slot 30>"));
    }
}
