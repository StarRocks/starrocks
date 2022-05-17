// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import org.junit.Assert;
import org.junit.Test;

public class GroupingSetTest extends PlanTestBase {
    @Test
    public void testGroupByCube() throws Exception {
        String sql = "select grouping_id(v1, v3), grouping(v2) from t0 group by cube(v1, v2, v3);";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("REPEAT_NODE"));
    }

    @Test
    public void testPredicateOnRepeatNode() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 is null;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 IS NULL"));
        Assert.assertFalse(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 IS NULL"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 is not null;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 IS NOT NULL"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 IS NOT NULL"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 = 1;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 = 1"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 1"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 = 1 + 2;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 = 3"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 3"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 = v2;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 = 2: v2"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 2: v2"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 <=> v2;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 <=> 2: v2"));
        Assert.assertFalse(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON" +
                "     PREDICATES: 1: v1 <=> 2: v2"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testGroupingFunctions() throws Exception {
        String sql = "select GROUPING(k10) from baseall;";
        starRocksAssert.query(sql).analysisError("cannot use GROUPING functions without");

        sql = "select k10 from baseall group by k10, GROUPING(1193275260000);";
        starRocksAssert.query(sql).analysisError("grouping functions only support column");

        sql = "select k10 from baseall group by k10 having GROUPING(1193275260000) > 2;";
        starRocksAssert.query(sql).analysisError("HAVING clause cannot contain grouping");

        sql = "select k10, GROUPING(k10) from baseall group by GROUPING SETS (  (k10), ( ) );";
        starRocksAssert.query(sql).explainContains("group by: 7: k10, 12: GROUPING_ID, 13: GROUPING");
    }

    @Test
    public void testPushDownOverRepeatNode() throws Exception {
        String sql1 = "SELECT\n" +
                "    IF(k2 IS NULL, 'ALL', k2) AS k2,\n" +
                "    IF(k3 IS NULL, 'ALL', k3) AS k3,\n" +
                "    k4\n" +
                "FROM\n" +
                "(\n" +
                "    SELECT\n" +
                "        k1,\n" +
                "        k2,\n" +
                "        k3,\n" +
                "        SUM(k4) AS k4\n" +
                "    FROM  db1.tbl6\n" +
                "    WHERE k1 = 0\n" +
                "        AND k4 = 1\n" +
                "        AND k3 = 'foo'\n" +
                "    GROUP BY \n" +
                "    GROUPING SETS (\n" +
                "        (k1),\n" +
                "        (k1, k2),\n" +
                "        (k1, k3),\n" +
                "        (k1, k2, k3)\n" +
                "    )\n" +
                ") t\n" +
                "WHERE IF(k2 IS NULL, 'ALL', k2) = 'ALL'";
        String plan = getFragmentPlan(sql1);
        Assert.assertTrue(plan.contains("  5:Project\n" +
                "  |  <slot 5> : 5: sum\n" +
                "  |  <slot 7> : if(2: k2 IS NULL, 'ALL', 2: k2)\n" +
                "  |  <slot 8> : if(3: k3 IS NULL, 'ALL', 3: k3)"));
        Assert.assertTrue(plan.contains("2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(4: k4)\n" +
                "  |  group by: 1: k1, 2: k2, 3: k3, 6: GROUPING_ID"));
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 3 lines [[1], [1, 2], [1, 3], [1, 2, 3]]\n" +
                "  |  PREDICATES: if(2: k2 IS NULL, 'ALL', 2: k2) = 'ALL'"));

        String sql2 =
                "SELECT\n" +
                        "    IF(k2 IS NULL, 'ALL', k2) AS k2,\n" +
                        "    IF(k3 IS NULL, 'ALL', k3) AS k3,\n" +
                        "    k4\n" +
                        "FROM\n" +
                        "(\n" +
                        "    SELECT\n" +
                        "        k1,\n" +
                        "        k2,\n" +
                        "        k3,\n" +
                        "        SUM(k4) AS k4\n" +
                        "    FROM  db1.tbl6\n" +
                        "    WHERE k1 = 0\n" +
                        "        AND k4 = 1\n" +
                        "        AND k3 = 'foo'\n" +
                        "    GROUP BY k1, k2, k3\n" +
                        ") t\n" +
                        "WHERE IF(k2 IS NULL, 'ALL', k2) = 'ALL'";
        plan = getFragmentPlan(sql2);
        Assert.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 5> : 5: sum\n" +
                "  |  <slot 6> : if(2: k2 IS NULL, 'ALL', 2: k2)\n" +
                "  |  <slot 7> : if(3: k3 IS NULL, 'ALL', 3: k3)"));
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: tbl6\n" +
                "     PREAGGREGATION: OFF. Reason: The key column don't support aggregate function: SUM\n" +
                "     PREDICATES: if(2: k2 IS NULL, 'ALL', 2: k2) = 'ALL', 1: k1 = '0', 4: k4 = 1, 3: k3 = 'foo'"));
    }

    @Test
    public void testSameGroupingAggColumn() throws Exception {
        String sql = "select v1, max(v2), sum(v3) from t0 group by rollup(v1, v2, v3);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: max(4: expr), sum(5: expr)\n" +
                "  |  group by: 1: v1, 2: v2, 3: v3, 8: GROUPING_ID\n" +
                "  |  \n" +
                "  2:REPEAT_NODE\n" +
                "  |  repeat: repeat 3 lines [[], [1], [1, 2], [1, 2, 3]]\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : clone(2: v2)\n" +
                "  |  <slot 5> : clone(3: v3)");
    }

    @Test
    public void testSameGroupingAggColumn2() throws Exception {
        String sql = "select v1, max(v2 + 1) from t0 group by rollup(v1, v2 + 1, v3);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: max(5: expr)\n" +
                "  |  group by: 1: v1, 4: expr, 3: v3, 7: GROUPING_ID\n" +
                "  |  \n" +
                "  2:REPEAT_NODE\n" +
                "  |  repeat: repeat 3 lines [[], [1], [1, 4], [1, 3, 4]]\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 8: add\n" +
                "  |  <slot 5> : clone(8: add)\n" +
                "  |  common expressions:\n" +
                "  |  <slot 8> : 2: v2 + 1");
    }

    @Test
    public void testSameGroupingAggColumn3() throws Exception {
        String sql = "select v1, max(v2), sum(v2) from t0 group by rollup(v1, v2, v3);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: max(5: expr), sum(5: expr)\n" +
                "  |  group by: 1: v1, 2: v2, 3: v3, 8: GROUPING_ID\n" +
                "  |  \n" +
                "  2:REPEAT_NODE\n" +
                "  |  repeat: repeat 3 lines [[], [1], [1, 2], [1, 2, 3]]\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 5> : clone(2: v2)");
    }
}
