// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import org.junit.Assert;
import org.junit.Test;

public class SubqueryTest extends PlanTestBase {

    @Test
    public void testCountConstantWithSubquery() throws Exception {
        String sql = "SELECT 1 FROM (SELECT COUNT(1) FROM t0 WHERE false) t;";
        String thriftPlan = getThriftPlan(sql);
        Assert.assertTrue(thriftPlan.contains("function_name:count"));
    }

    @Test
    public void testSubqueryGatherJoin() throws Exception {
        String sql = "select t1.v5 from (select * from t0 limit 1) as x inner join t1 on x.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:\n"
                + "  PARTITION: RANDOM\n"
                + "\n"
                + "  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 02\n"
                + "    UNPARTITIONED\n"
                + "\n"
                + "  1:OlapScanNode\n"
                + "     TABLE: t0"));
    }

    @Test
    public void testSubqueryBroadJoin() throws Exception {
        String sql = "select t1.v5 from t0 inner join[broadcast] t1 on cast(t0.v1 as int) = cast(t1.v4 as int)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  equal join conjunct: 7: cast = 8: cast\n"));
        Assert.assertTrue(plan.contains("<slot 7> : CAST(1: v1 AS INT)"));
        Assert.assertTrue(plan.contains("<slot 8> : CAST(4: v4 AS INT)"));
    }

    @Test
    public void testMultiScalarSubquery() throws Exception {
        String sql = "SELECT CASE \n"
                + "    WHEN (SELECT count(*) FROM t1 WHERE v4 BETWEEN 1 AND 20) > 74219\n"
                + "    THEN ( \n"
                + "        SELECT avg(v7) FROM t2 WHERE v7 BETWEEN 1 AND 20\n"
                + "        )\n"
                + "    ELSE (\n"
                + "        SELECT avg(v8) FROM t2 WHERE v8 BETWEEN 1 AND 20\n"
                + "        ) END AS bucket1\n"
                + "FROM t0\n"
                + "WHERE v1 = 1;";
        String plan = getFragmentPlan(sql);
        Assert.assertNotNull(plan);
    }

    @Test
    public void testSubqueryLimit() throws Exception {
        String sql = "select * from t0 where 2 = (select v4 from t1 limit 1);";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  4:ASSERT NUMBER OF ROWS\n" +
                "  |  assert number of rows: LE 1"));
    }

    @Test
    public void testUnionSubqueryDefaultLimit() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(2);
        String sql = "select * from (select * from t0 union all select * from t0) xx limit 10;";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |       limit: 10\n" +
                "  |    \n" +
                "  3:EXCHANGE\n" +
                "     limit: 10"));
    }

    @Test
    public void testExistsRewrite() throws Exception {
        String sql =
                "select count(*) FROM  test.join1 WHERE  EXISTS (select max(id) from test.join2 where join2.id = join1.id)";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("LEFT SEMI JOIN"));
    }

    @Test
    public void testMultiNotExistPredicatePushDown() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.setDatabase("default_cluster:test");

        String sql =
                "select * from join1 where join1.dt > 1 and NOT EXISTS (select * from join1 as a where join1.dt = 1 and a.id = join1.id)" +
                        "and NOT EXISTS (select * from join1 as a where join1.dt = 2 and a.id = join1.id);";
        String explainString = getFragmentPlan(sql);

        Assert.assertTrue(explainString.contains("  5:HASH JOIN\n" +
                "  |  join op: RIGHT ANTI JOIN (COLOCATE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 9: id = 2: id\n" +
                "  |  other join predicates: 1: dt = 2"));
        Assert.assertTrue(explainString.contains("  |    3:HASH JOIN\n" +
                "  |    |  join op: LEFT ANTI JOIN (COLOCATE)\n" +
                "  |    |  hash predicates:\n" +
                "  |    |  colocate: true\n" +
                "  |    |  equal join conjunct: 2: id = 5: id\n" +
                "  |    |  other join predicates: 1: dt = 1"));
        Assert.assertTrue(explainString.contains("  |    1:OlapScanNode\n" +
                "  |       TABLE: join1\n" +
                "  |       PREAGGREGATION: ON\n" +
                "  |       PREDICATES: 1: dt > 1"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testAssertWithJoin() throws Exception {
        String sql = "SELECT max(1) FROM t0 WHERE 1 = (SELECT t1.v4 FROM t0, t1 WHERE t1.v4 IN (SELECT t1.v4 FROM  t1))";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  8:Project\n" +
                "  |  <slot 7> : 7: v4\n" +
                "  |  \n" +
                "  7:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)"));
    }

}
