// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

    @Before
    public void alwaysCTEReuse() {
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @After
    public void defaultCTEReuse() {
        connectContext.getSessionVariable().setCboCTERuseRatio(1.5);
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
        Assert.assertTrue(plan, plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM"));

        sql = "with x0 as (select * from t0) " +
                "select * from x0 t,t1 where v1 in (select v2 from x0 where t.v1 = v1)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM"));
    }

    @Test
    public void testCTEJoinReorderLoseStatistics() throws Exception {
        connectContext.getSessionVariable().setMaxTransformReorderJoins(1);

        String sql = "with xx as (select * from t0) select * from xx as x0 join xx as x1 on x0.v1 = x1.v1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM"));

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
    public void testOneCTEInlineComplex2() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from x0), x2 as (select * from x1), " +
                "x3 as (select * from x2) " +
                "select * from x3;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));
    }

    @Test
    public void testCTEPredicate() throws Exception {
        String sql = "with xx as (select * from t0) " +
                "select x1.v1 from xx x1 join xx x2 on x1.v2=x2.v3 where x1.v3 = 4 and x2.v2=3;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (2: v2 = 3) OR (3: v3 = 4)"));
    }

    @Test
    public void testCTELimit() throws Exception {
        String sql = "with xx as (select * from t0) " +
                "select x1.v1 from (select * from xx limit 1) x1 " +
                "join (select * from xx limit 3) x2 on x1.v2=x2.v3;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks"));
        Assert.assertTrue(plan.contains("cardinality=1\n" +
                "     avgRowSize=24.0\n" +
                "     numNodes=0\n" +
                "     limit: 3"));
    }

    @Test
    public void testCTEPredicateLimit() throws Exception {
        String sql = "with xx as (select * from t0) " +
                "select x1.v1 from " +
                "(select * from xx where xx.v2 = 2 limit 1) x1 join " +
                "(select * from xx where xx.v3 = 4 limit 3) x2 on x1.v2=x2.v3;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (3: v3 = 4) OR (2: v2 = 2)\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=24.0\n" +
                "     numNodes=0\n" +
                "     limit: 3"));
    }

    @Test
    public void testCTEPruneColumns() throws Exception {
        String sql = "with xx as (select * from t0) select v1 from xx union all select v2 from xx;";
        String plan = getFragmentPlan(sql);
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
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    RANDOM"));
    }

    @Test
    public void testComplexCTEAllCostInline() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(Double.MAX_VALUE);

        String sql = "WITH x1 AS (select * from t0), \n" +
                " x2 AS (select * from x1) \n" +
                " select * " +
                " from (select x2.* from x1 join x2 on x1.v2 = x2.v2) as s1" +
                " join (select x1.* from x1 join x2 on x1.v3 = x2.v3) as s2 on s1.v2 = s2.v2;";
        String plan = getFragmentPlan(sql);
        defaultCTEReuse();
        Assert.assertFalse(plan.contains("MultiCastDataSinks"));
    }

    @Test
    public void testSubqueryWithPushPredicate() throws Exception {
        String sql = "select * from " +
                "(with xx as (select * from t0) select x1.* from xx x1 join xx x2 on x1.v2 = x2.v2) s " +
                "where s.v1 = 2;";

        String plan = getFragmentPlan(sql);
        defaultCTEReuse();
        Assert.assertTrue(plan, plan.contains("3:SELECT\n" +
                "  |  predicates: 4: v1 = 2\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 4> : 1: v1\n" +
                "  |  <slot 5> : 2: v2\n" +
                "  |  <slot 6> : 3: v3"));
    }

    @Test
    public void testSubqueryWithPushLimit() throws Exception {
        String sql = "select * from " +
                "(with xx as (select * from t0) " +
                "select x1.* from xx x1 left outer join[broadcast] xx x2 on x1.v2 = x2.v2) s " +
                "where s.v1 = 2 limit 10;";

        String plan = getFragmentPlan(sql);
        defaultCTEReuse();
        Assert.assertTrue(plan.contains("  3:SELECT\n" +
                "  |  predicates: 4: v1 = 2\n" +
                "  |  limit: 10"));
    }

    @Test
    public void testLeftJoinCTEWithConstOnPredicates() throws Exception {
        String sql1 = "WITH \n" +
                "    w_t0 as (SELECT * FROM t0) \n" +
                "SELECT * \n" +
                "FROM t1 LEFT JOIN w_t0 \n" +
                "ON t1.v4 = w_t0.v1 \n" +
                "AND false;\n";

        getFragmentPlan(sql1);

        String sql2 = "WITH \n" +
                "    w_t0 as (SELECT * FROM t0) \n" +
                "SELECT * \n" +
                "FROM w_t0 LEFT JOIN t1 \n" +
                "ON t1.v4 = w_t0.v1 \n" +
                "AND true;\n";

        getFragmentPlan(sql2);
    }

    @Test
    public void testNullTypeHack() throws Exception {
        String sql = "WITH cte_1 AS (\n" +
                "  SELECT null v1\n" +
                ")\n" +
                "SELECT  \n" +
                "  CASE \n" +
                "    WHEN a.v1 = b.v1 THEN 1 \n" +
                "    ELSE -1 \n" +
                "  END IS_OK\n" +
                "FROM cte_1 a, cte_1 b";

        String plan = getThriftPlan(sql);
        Assert.assertFalse(plan.contains("NULL_TYPE"));
    }

    @Test
    public void testMergePushdownPredicate() throws Exception {
        String sql = "with with_t_0 as (select v1, v2, v4 from t0 join t1),\n" +
                "with_t_1 as (select v1, v2, v5 from t0 join t1)\n" +
                "select v5, 1 from with_t_1 join with_t_0 left semi join\n" +
                "(select v2 from with_t_0 where v4 = 123) subwith_t_0\n" +
                "on with_t_0.v1 = subwith_t_0.v2 and with_t_0.v1 > 0\n" +
                "where with_t_0.v4 < 100;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:SELECT\n" +
                "  |  predicates: 19: v1 > 0, 22: v4 < 100");
        assertContains(plan, "9:SELECT\n" +
                "  |  predicates: 26: v2 > 0, 28: v4 = 123");
    }
}
