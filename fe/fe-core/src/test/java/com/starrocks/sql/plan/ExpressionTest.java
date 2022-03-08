// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class ExpressionTest extends PlanTestBase {

    @Test
    public void testExpression() throws Exception {
        String sql = "select v1 + v2, v1 + v2 + v3, v1 + v2 + v3 + 1 from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  common expressions:\n"
                + "  |  <slot 7> : 1: v1 + 2: v2\n"
                + "  |  <slot 8> : 7: add + 3: v3\n"));
    }

    @Test
    public void testReduceCast() throws Exception {
        String sql = "select t1a, t1b from test_all_type where t1c > 2000 + 1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: test_all_type\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 3: t1c > 2001"));
    }

    @Test
    public void testExpression1() throws Exception {
        String sql = "select sum(v1 + v2) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n"
                + "  |  <slot 4> : 1: v1 + 2: v2"));
    }

    @Test
    public void testExpression2() throws Exception {
        String sql = "select sin(v1) + cos(v2) as a from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("sin(CAST(1: v1 AS DOUBLE)) + cos(CAST(2: v2 AS DOUBLE))"));

        sql = "select * from test_all_type where id_date = 20200202";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 9: id_date = '2020-02-02'"));
    }

    @Test
    public void testExpression3() throws Exception {
        String sql =
                "select cast (v1 as boolean), cast (v1 as tinyint), cast (v1 as smallint), cast (v1 as int), cast (v1"
                        + " as bigint), cast (v1 as largeint), cast (v1 as float), cast (v1 as double), cast(v1 as "
                        + "date), cast(v1 as datetime), cast(v1 as decimalv2), cast(v1 as varchar) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n"
                + "  |  <slot 1> : 1: v1\n"
                + "  |  <slot 4> : CAST(1: v1 AS BOOLEAN)\n"
                + "  |  <slot 5> : CAST(1: v1 AS TINYINT)\n"
                + "  |  <slot 6> : CAST(1: v1 AS SMALLINT)\n"
                + "  |  <slot 7> : CAST(1: v1 AS INT)\n"
                + "  |  <slot 8> : CAST(1: v1 AS LARGEINT)\n"
                + "  |  <slot 9> : CAST(1: v1 AS FLOAT)\n"
                + "  |  <slot 10> : CAST(1: v1 AS DOUBLE)\n"
                + "  |  <slot 11> : CAST(1: v1 AS DATE)\n"
                + "  |  <slot 12> : CAST(1: v1 AS DATETIME)\n"
                + "  |  <slot 13> : CAST(1: v1 AS DECIMAL(9,0))\n"
                + "  |  <slot 14> : CAST(1: v1 AS VARCHAR)\n"));
    }

    @Test
    public void testExpression4() throws Exception {
        String sql =
                "select v1 * v1 / v1 % v1 + v1 - v1 DIV v1, v2&~v1|v3^1 from t0 where v1 >= 1 and v1 <=10 and v2 > 1 "
                        + "and v2 < 10 and v3 != 10 and v3 <=> 10 and !(v1 = 1 and v2 = 2 or v3 =3) and v1 between 1 "
                        + "and 2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n"
                + "  |  <slot 4> : CAST(1: v1 * 1: v1 AS DOUBLE) / 6: cast % 6: cast + 6: cast - CAST(1: v1 DIV 1: v1"
                + " AS DOUBLE)\n"
                + "  |  <slot 5> : 2: v2 & ~ 1: v1 | 3: v3 ^ 1\n"
                + "  |  common expressions:\n"
                + "  |  <slot 6> : CAST(1: v1 AS DOUBLE)\n"));
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 >= 1, 1: v1 <= 10, 2: v2 > 1, 2: v2 < 10, 3: "
                + "v3 != 10, 3: v3 <=> 10, (1: v1 != 1) OR (2: v2 != 2), 3: v3 != 3, 1: v1 <= 2\n"));
    }

    @Test
    public void testExpression5() throws Exception {
        String sql = "select v1+20, case v2 when v3 then 1 else 0 end from t0 where v1 is null";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n"
                + "  |  <slot 4> : 1: v1 + 20\n"
                + "  |  <slot 5> : if(2: v2 = 3: v3, 1, 0)"));

        sql = "select v1+20, case when true then v1 else v2 end from t0 where v1 is null";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 1: v1 + 20"));

        sql = "select v1+20, ifnull(null, v2) from t0 where v1 is null";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 1: v1 + 20\n"));

        sql = "select v1+20, if(true, v1, v2) from t0 where v1 is null";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 1: v1 + 20"));

        sql = "select v1+20, if(false, v1, NULL) from t0 where v1 is null";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : 1: v1 + 20\n" +
                "  |  <slot 5> : NULL"));
    }

    @Test
    public void testExpression6() throws Exception {
        String sql = "select cast(v1 as decimal64(7,2)) + cast(v2 as decimal64(9,3)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL64(7,2)) AS DECIMAL64(18,2)) + CAST(CAST(2: v2 AS DECIMAL64(9,3)) AS DECIMAL64(18,3))\n"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testExpression7() throws Exception {
        String sql = "select cast(v1 as decimal128(27,2)) - cast(v2 as decimal64(10,3)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL128(27,2)) AS DECIMAL128(38,2)) - CAST(CAST(2: v2 AS DECIMAL64(10,3)) AS DECIMAL128(38,3))\n"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testExpression8() throws Exception {
        String sql = "select cast(v1 as decimal128(10,5)) * cast(v2 as decimal64(9,7)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL128(10,5)) AS DECIMAL128(38,5)) * CAST(CAST(2: v2 AS DECIMAL64(9,7)) AS DECIMAL128(38,7))\n"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testExpression9() throws Exception {
        String sql = "select cast(v1 as decimal128(18,5)) / cast(v2 as decimal32(9,7)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL128(18,5)) AS DECIMAL128(38,5)) / CAST(CAST(2: v2 AS DECIMAL32(9,7)) AS DECIMAL128(38,7))\n"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testExpression10() throws Exception {
        String sql = "select cast(v1 as decimal64(18,5)) % cast(v2 as decimal32(9,7)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(1: v1 AS DECIMAL64(18,5)) % CAST(CAST(2: v2 AS DECIMAL32(9,7)) AS DECIMAL64(18,7))\n"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testTimestampArithmeticExpr() throws Exception {
        String sql = "select id_date + interval '3' month," +
                "id_date + interval '1' day," +
                "id_date + interval '2' year," +
                "id_date - interval '3' day from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  <slot 11> : months_add(15: cast, 3)\n"
                + "  |  <slot 12> : days_add(15: cast, 1)\n"
                + "  |  <slot 13> : years_add(15: cast, 2)\n"
                + "  |  <slot 14> : days_sub(15: cast, 3)\n"));
    }

    @Test
    public void testScalarOperatorToExpr() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(2, Type.INT, "e", true);
        ScalarOperator cast = new CastOperator(Type.DOUBLE, columnRefOperator);
        ColumnRefOperator castColumnRef = new ColumnRefOperator(1, Type.INT, "cast", true);

        HashMap<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        projectMap.put(castColumnRef, cast);
        projectMap.put(columnRefOperator, ConstantOperator.createInt(1));

        HashMap<ColumnRefOperator, Expr> variableToSlotRef = new HashMap<>();
        variableToSlotRef.put(columnRefOperator, new IntLiteral(1));

        ScalarOperatorToExpr.FormatterContext context =
                new ScalarOperatorToExpr.FormatterContext(variableToSlotRef, projectMap);

        Expr castExpression = ScalarOperatorToExpr.buildExecExpression(castColumnRef, context);

        Assert.assertTrue(castExpression instanceof CastExpr);
    }

    @Test
    public void testScalarRewrite() throws Exception {
        String sql = "select t0.v1, case when true then t0.v1 else t0.v1 end from t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:1: v1 | 1: v1\n"));
    }

    @Test
    public void testScalarRewrite2() throws Exception {
        String sql = "select j.x1, j.x2 from "
                + "(select t0.v1 as x1, case when true then t0.v1 else t0.v1 end as x2, t0.v3 as x3 from t0 limit 10)"
                + " as j "
                + "where j.x3 > 1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Project\n"
                + "  |  <slot 1> : 1: v1\n"
                + "  |  \n"
                + "  1:SELECT\n"
                + "  |  predicates: 3: v3 > 1\n"
                + "  |  \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n"));
    }

    @Test(expected = SemanticException.class)
    public void testArithCastCheck() throws Exception {
        String sql = "select v1 + h1 from test_object;";
        getFragmentPlan(sql);
    }

    @Test
    public void testLikeFunctionIdThrift() throws Exception {
        String sql = "select S_ADDRESS from supplier where S_ADDRESS " +
                "like '%Customer%Complaints%' ";
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("fid:60010"));
    }

    @Test
    public void testCaseWhen() throws Exception {
        String sql = "SELECT v1 FROM t0 WHERE CASE WHEN (v1 IS NOT NULL) THEN NULL END";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: if(1: v1 IS NOT NULL, NULL, NULL)"));
    }

    @Test
    public void testConstantTimeTNull() throws Exception {
        // check can get plan without exception
        String sql = "select TIMEDIFF(\"1969-12-30 21:44:11\", NULL) from t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 1:Project\n" +
                "  |  <slot 4> : NULL"));

        sql = "select timediff(cast(cast(null as DATETIME) as DATETIME), " +
                "cast(case when ((cast(null as DOUBLE) < cast(null as DOUBLE))) then cast(null as DATETIME) " +
                "else cast(null as DATETIME) end as DATETIME)) as c18 from t0 as ref_0;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 1:Project\n" +
                "  |  <slot 4> : NULL"));
    }

    @Test
    public void testIfTimediff() throws Exception {
        String sql = "SELECT COUNT(*) FROM t0 WHERE (CASE WHEN CAST(t0.v1 AS BOOLEAN ) THEN " +
                "TIMEDIFF(\"1970-01-08\", \"1970-01-12\") END) BETWEEN (1341067345) AND " +
                "(((CASE WHEN false THEN -843579223 ELSE -1859488192 END)+(((-406527105)+(540481936))))) ;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "PREDICATES: CAST(if(CAST(1: v1 AS BOOLEAN), -345600.0, NULL) AS DOUBLE) >= 1.341067345E9, CAST(if(CAST(1: v1 AS BOOLEAN), -345600.0, NULL) AS DOUBLE) <= -1.725533361E9"));
    }
}
