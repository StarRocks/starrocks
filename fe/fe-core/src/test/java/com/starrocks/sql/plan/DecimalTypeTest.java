// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.Test;

public class DecimalTypeTest extends PlanTestBase {

    @Test
    public void testDecimalCast() throws Exception {
        String sql = "select * from baseall where cast(k5 as decimal32(4,3)) = 1.234";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: CAST(5: k5 AS DECIMAL32(4,3)) = 1.234"));

        sql = "SELECT k5 FROM baseall WHERE (CAST(k5 AS DECIMAL32 ) ) IN (0.006) " +
                "GROUP BY k5 HAVING (k5) IN (0.005, 0.006)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: 5: k5 IN (0.005, 0.006), CAST(5: k5 AS DECIMAL32(9,9)) = 0.006"));
    }

    @Test
    public void testDecimalConstRewrite() throws Exception {
        String sql = "select * from t0 WHERE CAST( - 8 AS DECIMAL ) * + 52 + 87 < - 86";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: TRUE"));
    }

    @Test
    public void testCountDecimalV3Literal() throws Exception {
        String sql = "select count( - - cast(89 AS DECIMAL )) from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: count(89)"));

        sql = "select max( - - cast(89 AS DECIMAL )) from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: max(89)"));

        sql = "select min( - - cast(89 AS DECIMAL )) from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: min(89)"));

        sql = "select sum( - - cast(89 AS DECIMAL )) from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: sum(89)"));

        sql = "select avg( - - cast(89 AS DECIMAL )) from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: avg(89)"));
    }

    @Test
    public void testDecimalV3Distinct() throws Exception {
        String sql = "select avg(t1c), count(distinct id_decimal) from test_all_type;";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "multi_distinct_count[([10: id_decimal, DECIMAL64(10,2), true]); args: DECIMAL64; result: BIGINT; args nullable: true; result nullable: false]"));
    }

    @Test
    public void testArithmeticDecimalReuse() throws Exception {
        String sql = "select t1a, sum(id_decimal * t1f), sum(id_decimal * t1f)" +
                "from test_all_type group by t1a";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("OUTPUT EXPRS:1: t1a | 12: sum | 12: sum"));
    }

    @Test
    public void testDecimalV3LiteralCast() throws Exception {
        String sql =
                "select id_datetime from test_all_type WHERE CAST(IF(true, 0.38542880072101215, '-Inf')  AS BOOLEAN )";
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("string_literal:TStringLiteral(value:0.38542880072101215)"));
    }

    @Test
    public void testJoinDecimalAndBool() throws Exception {
        String sql =
                "select t3.v1 from t3 inner join test_all_type on t3.v2 = test_all_type.id_decimal and t3.v2 > true";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n"
                + "     TABLE: t3\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: 2: v2 > 1"));

        Assert.assertTrue(plan.contains("  2:OlapScanNode\n"
                + "     TABLE: test_all_type\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=0/1\n"
                + "     rollup: test_all_type\n"));
    }

}
