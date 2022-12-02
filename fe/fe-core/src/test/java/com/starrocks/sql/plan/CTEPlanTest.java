// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CTEPlanTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setCboCteReuse(true);
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
    }

    //@Test
    public void testMultiFlatCTE() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from t1) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    RANDOM"));
    }

    //@Test
    public void testMultiContainsCTE() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from x0) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 10\n" +
                "    RANDOM"));
    }

    //@Test
    public void testFromUseCte() throws Exception {
        String sql = "with x0 as (select * from t0) " +
                "select * from (with x1 as (select * from t1) select * from x1 join x0 on x1.v4 = x0.v1) tt";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  7:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v4 = 10: v1\n" +
                "  |  \n" +
                "  |----6:EXCHANGE"));
    }

    @Test
    public void testSubqueryUserSameCTE() throws Exception {
        String sql = "with x0 as (select * from t0) " +
                "select * from x0 x,t1 y where v1 in (select v2 from x0 z where z.v1 = x.v1)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));

        sql = "with x0 as (select * from t0) " +
                "select * from x0 t,t1 where v1 in (select v2 from x0 where t.v1 = v1)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));
    }

    /*
    @Test
    public void testFromUseSameNameCte() throws Exception {
        String sql = "with x0 as (select * from t0) " +
                "select * from (with x0 as (select * from t1) select * from x0 as x1 join x0 on x1.v4 = x0.v5) as tt;";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 4: v1\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    HASH_PARTITIONED: 7: v1\n"));
    }
     */

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

<<<<<<< HEAD
=======
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
        assertNotContains(plan, "NULL_TYPE");
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
>>>>>>> d8a7daee4 ([BugFix] merge pushdown predicate to cte (#14472))
}
