// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

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
    public void testDateVariableCast() throws Exception {
        String sql = "select t1a, t1b from test_all_type where id_date > 2000";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 9: id_date > CAST(2000 AS DATE)");

        sql = "select t1a, t1b from test_all_type where id_datetime > 2000";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 8: id_datetime > CAST(2000 AS DATETIME)");
    }

    @Test
    public void testExpression1() throws Exception {
        String sql = "select sum(v1 + v2) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1 + 2: v2)"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL64(7,2)) AS DECIMAL64(10,2)) + " +
                "CAST(CAST(2: v2 AS DECIMAL64(9,3)) AS DECIMAL64(10,3))\n"));
    }

    @Test
    public void testExpression7() throws Exception {
        String sql = "select cast(v1 as decimal128(27,2)) - cast(v2 as decimal64(10,3)) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL128(27,2)) AS DECIMAL128(38,2)) - " +
                "CAST(CAST(2: v2 AS DECIMAL64(10,3)) AS DECIMAL128(38,3))\n"));
    }

    @Test
    public void testExpression8() throws Exception {
        String sql = "select cast(v1 as decimal128(10,5)) * cast(v2 as decimal64(9,7)) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:Project\n" +
                "  |  <slot 4> : CAST(1: v1 AS DECIMAL128(10,5)) * " +
                "CAST(CAST(2: v2 AS DECIMAL64(9,7)) AS DECIMAL128(9,7))"));
    }

    @Test
    public void testExpression9() throws Exception {
        String sql = "select cast(v1 as decimal128(18,5)) / cast(v2 as decimal32(9,7)) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL128(18,5)) AS DECIMAL128(38,5)) / " +
                "CAST(CAST(2: v2 AS DECIMAL32(9,7)) AS DECIMAL128(38,7))\n"));
    }

    @Test
    public void testExpression10() throws Exception {
        String sql = "select cast(v1 as decimal64(18,5)) % cast(v2 as decimal32(9,7)) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(1: v1 AS DECIMAL64(18,5)) % CAST(CAST(2: v2 AS DECIMAL32(9,7)) AS DECIMAL64(18,7))\n"));
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

        // lambda functions
        ScalarOperator lambdaExpr = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(100000, Type.INT, "x", true),
                ConstantOperator.createInt(1));
        ColumnRefOperator colRef = new ColumnRefOperator(100000, Type.INT, "x", true);
        LambdaFunctionOperator lambda =
                new LambdaFunctionOperator(Lists.newArrayList(colRef), lambdaExpr, Type.BOOLEAN);
        variableToSlotRef.clear();
        projectMap.clear();
        context = new ScalarOperatorToExpr.FormatterContext(variableToSlotRef, projectMap);

        Expr lambdaFunc = ScalarOperatorToExpr.buildExecExpression(lambda, context);

        Assert.assertTrue(lambdaFunc instanceof LambdaFunctionExpr);
        Assert.assertEquals("<slot 100000> -> <slot 100000> = 1", lambdaFunc.toSql());

        LambdaFunctionExpr lexpr = ((LambdaFunctionExpr) lambdaFunc);
        Assert.assertTrue(lexpr.getChildren().size() == 2 && lexpr.getChild(1) instanceof SlotRef);

        SlotRef slotRef = ((SlotRef) lexpr.getChild(1));
        Assert.assertTrue(slotRef.isFromLambda());

        List<TupleId> tids = ImmutableList.of(new TupleId(111));
        Assert.assertTrue(lexpr.getChild(1).isBoundByTupleIds(tids));
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
        Assert.assertTrue(plan.contains("  2:SELECT\n" +
                "  |  predicates: 3: v3 > 1\n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 10\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  0:OlapScanNode"));
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
        Assert.assertTrue(plan.contains("0:EMPTYSET"));
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
                "PREDICATES: CAST(if(CAST(1: v1 AS BOOLEAN), -345600.0, NULL) AS DOUBLE) >= 1.341067345E9, " +
                        "CAST(if(CAST(1: v1 AS BOOLEAN), -345600.0, NULL) AS DOUBLE) <= -1.725533361E9"));
    }

    @Test
    public void testConnectionId() throws Exception {
        String queryStr = "select connection_id()";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:UNION"));

        queryStr = "select database();";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:UNION"));
    }

    @Test
    public void testBetweenDate() throws Exception {
        String sql = "select * from test_all_type where id_date between '2020-12-12' and '2021-12-12'";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: 9: id_date >= '2020-12-12', 9: id_date <= '2021-12-12'"));
    }

    @Test
    public void testNullAddNull() throws Exception {
        String sql = "select null+null as c3 from test.join2;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:4: expr"));
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 4> : NULL"));
    }

    @Test
    public void testDateDateTimeFunctionMatch() throws Exception {
        String sql = "select if(3, date('2021-01-12'), STR_TO_DATE('2020-11-02', '%Y-%m-%d %H:%i:%s'));";
        starRocksAssert.query(sql).explainContains("if(CAST(3 AS BOOLEAN), '2021-01-12 00:00:00', " +
                "str_to_date('2020-11-02', '%Y-%m-%d %H:%i:%s'))");

        sql = "select nullif(date('2021-01-12'), date('2021-01-11'));";
        starRocksAssert.query(sql).explainContains("nullif('2021-01-12', '2021-01-11')");

        sql = "select nullif(date('2021-01-12'), STR_TO_DATE('2020-11-02', '%Y-%m-%d %H:%i:%s'));";
        starRocksAssert.query(sql)
                .explainContains("nullif('2021-01-12 00:00:00', str_to_date('2020-11-02', '%Y-%m-%d %H:%i:%s'))");

        sql = "select if(3, 4, 5);";
        starRocksAssert.query(sql).explainContains("if(CAST(3 AS BOOLEAN), 4, 5)");

        sql = "select ifnull(date('2021-01-12'), 123);";
        starRocksAssert.query(sql).explainContains("20210112");

        sql = "select ifnull(date('2021-01-12'), 'kks');";
        starRocksAssert.query(sql).explainContains("'2021-01-12'");

        sql = "select ifnull(1234, 'kks');";
        starRocksAssert.query(sql).explainContains("'1234'");
    }

    @Test
    public void testCaseWhenOperatorReuse() throws Exception {
        String sql =
                "select max(case when STRLEFT(DATE_FORMAT('2020-09-02 23:59:59', '%Y-%m'), 6) > 0 then v1 else v2 end),"
                        +
                        "min(case when STRLEFT(DATE_FORMAT('2020-09-02 23:59:59', '%Y-%m'), 6) > 0 then v2 else v1 end),"
                        +
                        "count(case when STRLEFT(DATE_FORMAT('2020-09-02 23:59:59', '%Y-%m'), 6) > 0 then v3 else v2 "
                        + "end) from t0";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: max(if(12: expr, 1: v1, 2: v2)), min(if(12: expr, 2: v2, 1: v1)), " +
                "count(if(12: expr, 3: v3, 2: v2))");
        Assert.assertTrue(planFragment.contains("<slot 10> : strleft('2020-09', 6)"));
    }

    @Test
    public void testCastUnCompatibleType1() throws Exception {
        String sql = "select CAST(CAST(CAST(t1e AS DATE) AS BOOLEAN) AS BOOLEAN) from test_all_type;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(CAST(5: t1e AS DATE) AS BOOLEAN)"));
    }

    @Test
    public void testCastUnCompatibleType2() throws Exception {
        String sql = "SELECT COUNT(*) FROM test_all_type WHERE CAST(CAST(t1e AS DATE) AS BOOLEAN);";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(CAST(5: t1e AS DATE) AS BOOLEAN)"));
    }

    @Test
    public void testCastType() throws Exception {
        String sql = "select * from test_all_type where t1a = 123 AND t1b = 999999999 AND t1d = 999999999 "
                + "AND id_datetime = '2020-12-20 20:20:20' AND id_date = '2020-12-11' AND id_datetime = 'asdlfkja';";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1: t1a = '123', CAST(2: t1b AS INT) = 999999999, 4: t1d = 999999999, "
                + "8: id_datetime = '2020-12-20 20:20:20', 9: id_date = '2020-12-11', "
                + "8: id_datetime = CAST('asdlfkja' AS DATETIME)"));
    }

    @Test
    public void testDateTypeReduceCast() throws Exception {
        String sql = "select * from test_all_type_distributed_by_datetime " +
                "where cast(cast(id_datetime as date) as datetime) >= '1970-01-01 12:00:00' " +
                "and cast(cast(id_datetime as date) as datetime) <= '1970-01-02 18:00:00'";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(
                plan.contains("8: id_datetime >= '1970-01-02 00:00:00', 8: id_datetime < '1970-01-03 00:00:00'"));
    }

    @Test
    public void testEqStringCast() throws Exception {
        String sql = "select 'a' = v1 from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(1: v1 AS VARCHAR(1048576)) = 'a'\n"));
    }

    @Test
    public void testCharCompareWithVarchar() throws Exception {
        String sql = "select t2.tb from tall t1 join tall t2 " +
                "on t1.tc = t2.tb and t2.tt = 123 and (t2.tt != 'ax') = t2.td;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: 20: tt = '123', " +
                "CAST(20: tt != 'ax' AS BIGINT) = 14: td, 14: td = 1"));
    }

    @Test
    public void testFunctionNullable() throws Exception {
        String sql = "select UNIX_TIMESTAMP(\"2015-07-28 19:41:12\", \"22\");";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan, plan.contains("scalar_fn:TScalarFunction(symbol:), " +
                "id:50287, fid:50287, could_apply_dict_optimize:false, ignore_nulls:false), " +
                "has_nullable_child:false, is_nullable:true"));
    }

    @Test
    public void testEqDoubleCast() throws Exception {
        String sql = "select 'a' = t1e from test_all_type";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(5: t1e AS DOUBLE) = CAST('a' AS DOUBLE)\n"));
    }

    @Test
    public void testNotEqStringCast() throws Exception {
        String sql = "select 'a' != v1 from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(1: v1 AS VARCHAR(1048576)) != 'a'\n"));
    }

    @Test
    public void testConstantNullable() throws Exception {
        String sql = "SELECT MICROSECONDS_SUB(\"1969-12-25\", NULL) FROM t1";
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
        List<ColumnRefOperator> outColumns = plan.getOutputColumns();

        Assert.assertEquals(1, outColumns.size());
        Assert.assertEquals(Type.DATETIME, outColumns.get(0).getType());
        Assert.assertTrue(outColumns.get(0).isNullable());
    }

    @Test
    public void testMd5sum() throws Exception {
        String sql = "select 1 from t0 left outer join t1 on t0.v1= t1.v4 where md5sum(t1.v4) = 'a'";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: LEFT OUTER JOIN (BROADCAST)"));
        Assert.assertTrue(plan.contains("other predicates: md5sum(CAST(4: v4 AS VARCHAR)) = 'a'"));
    }

    @Test
    public void testIsNullPredicateFunctionThrift() throws Exception {
        String sql = "select v1 from t0 where v1 is null";
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("fn:TFunction(name:TFunctionName(function_name:is_null_pred)"));
    }

    @Test
    public void testBitmapHashRewrite() throws Exception {
        String sql = "select bitmap_hash(NULL)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("bitmap_hash(NULL)"));
    }

    public void testPlanContains(String sql, String content) throws Exception {
        String plan = getFragmentPlan(sql);
        Assert.assertTrue("plan is " + plan, plan.contains(content));
    }

    @Test
    public void testLambdaReduce() throws Exception {
        starRocksAssert.withTable("create table test_lambda" +
                "(c0 INT, c2 array<int>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        String sql = "select * from test_lambda where array_map(x -> x, c2) is not null";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("array_map"));

        sql = "select array_map(x -> x, c2) from test_lambda";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("array_map"));
    }

    @Test
    public void testLambdaPredicateOnScan() throws Exception {
        starRocksAssert.withTable("create table test_lambda_on_scan" +
                "(c0 INT, c2 array<int>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        String sql = "select * from test_lambda_on_scan where array_map(x -> x + 1, c2) is not null";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("array_map"));
    }

    @Test
    public void testLambdaReuseSubExpression() throws Exception {
        starRocksAssert.withTable("create table if not exists test_array" +
                "(c0 INT,c1 int, c2 array<int>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        // without lambda arguments
        String sql = "select b, array_map(x->x+b, arr) from (select array_map(x->x+1, [1,2]) as arr, 3*c1 as b " +
                "from test_array)T";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("common expressions"));
        Assert.assertTrue(plan.contains("array_map(<slot 7> -> CAST(<slot 7> AS BIGINT) + 10: multiply"));

        sql = "select b, array_map(x->x+ 3 *c1, arr) from (select array_map(x->x+1, [1,2]) as arr, 3*c1 as b,c1 " +
                "from test_array)T";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("common expressions"));
        Assert.assertTrue(plan.contains("array_map(<slot 7> -> CAST(<slot 7> AS BIGINT) + 10: multiply"));

        sql = "select 3*c1, array_map(x->x+ 3 *c1, c2) from test_array";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("common expressions"));
        Assert.assertTrue(plan.contains("array_map(<slot 5> -> CAST(<slot 5> AS BIGINT) + 8: multiply"));

        sql = "select arr,array_length(arr) from (select array_map(x->x+1, [1,2]) as arr)T";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("common expressions"));
        Assert.assertTrue(plan.contains("array_length(6: array_map)"));

        sql = "select array_map(x->x+ 3 *c1 + 3*c1, c2) from test_array";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("common expressions"));
        Assert.assertTrue(plan.contains(
                "array_map(<slot 4> -> CAST(<slot 4> AS BIGINT) + 7: multiply + 7: multiply"));

        // with lambda arguments
        sql = "select array_map(x -> x*2 + x*2, [1,3])";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("lambda common expressions:{<slot 5> <-> CAST(<slot 2> AS SMALLINT)}" +
                "{<slot 6> <-> <slot 5> * 2}{<slot 7> <-> CAST(<slot 6> AS INT)}"));

        sql = "select array_map((x,y) -> x*2 + y  + x*2, [1,3],[4,5])";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("lambda common expressions:{<slot 6> <-> CAST(<slot 2> AS SMALLINT)}" +
                "{<slot 7> <-> <slot 6> * 2}"));

        sql = "select array_map((x,y) -> x*2 + y  + x*2, [1,3],[4,5]) from test_array";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("lambda common expressions:{<slot 8> <-> CAST(<slot 4> AS SMALLINT)}{<slot 9>" +
                " <-> <slot 8> * 2}"));

        sql = "select array_map(x -> x*2  + abs(c1) + x*2+ abs(c1), c2) from test_array";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("lambda common expressions:{<slot 8> <-> CAST(<slot 4> AS BIGINT)}" +
                "{<slot 9> <-> <slot 8> * 2}"));

        sql = "select array_map(x -> x*2  + abs(c1) + (x*2+ abs(c1)), c2) from test_array";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("lambda common expressions:{<slot 8> <-> CAST(<slot 4> AS BIGINT)}{<slot 9> " +
                "<-> <slot 8> * 2}{<slot 10> <-> <slot 9> + 6: abs}"));
    }

    @Test
    public void testReuseNestedHighOrderLambdaFunctions() throws Exception {
        starRocksAssert.withTable("CREATE TABLE action1(uid int(11), event_type varchar(65533), time datetime)" +
                " duplicate key(uid, event_type) distributed by hash(uid) buckets 1 " +
                "properties('replication_num'='1');");
        starRocksAssert.withTable("create table test_array23(c0 INT, c1 array<varchar(65533)>, c2 array<int>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        String sql = "WITH `CASE_006` AS\n" +
                "  (SELECT array_map((arg_001) -> (arg_001), `c1`) AS `argument_003`,\n" +
                "          array_map((arg_002) -> (CAST(1 AS BIGINT)), `c1`) AS `argument_004`\n" +
                "   FROM test_array23)\n" +
                "\n" +
                "select argument_004, ARRAY_FILTER((x, y) -> y IS NOT NULL, " +
                "`argument_003`, `argument_004`) AS `source_target_005` from CASE_006;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  common expressions:\n" +
                "  |  <slot 14> : array_map(<slot 5> -> 1, 8: c1)");

        sql = "WITH `CASE_006` AS\n" +
                "  (SELECT array_map((arg_001) -> (arg_001), `c1`) AS `argument_003`,\n" +
                "          array_map((arg_002) -> (arg_002 + 1), `c1`) AS `argument_004`\n" +
                "   FROM test_array23)\n" +
                "\n" +
                "select argument_004, ARRAY_FILTER((x, y) -> y IS NOT NULL, " +
                "`argument_003`, `argument_004`) AS `source_target_005` from CASE_006;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  |  common expressions:\n" +
                "  |  <slot 14> : array_map(<slot 5> -> CAST(<slot 5> AS DOUBLE) + 1.0, 8: c1)");

        sql = "SELECT uid, times, actions, step0_time, step1_time, array_filter((x, y)->(y = '支付' AND (x BETWEEN " +
                "step1_time AND date_add(step1_time, INTERVAL 90 MINUTE)) AND (step1_time <> '2020-01-01 00:00:00')" +
                " ), times, actions)[1] AS step2_time FROM (SELECT uid, times, actions, step0_time, array_filter( " +
                "(x, y)-> (y = '下单' AND (x BETWEEN step0_time AND date_add(step0_time, INTERVAL 90 MINUTE)) AND " +
                "(step0_time <> '2020-01-01 00:00:00' ) ), times, actions)[1] AS step1_time FROM (SELECT uid, times" +
                ", actions, array_filter((x, y) -> (y= '浏览' AND x BETWEEN '2020-01-02 00:00:00' AND " +
                "'2020-01-02 23:59:59'), times, actions)[1] AS step0_time FROM (SELECT uid, " +
                "array_sort(array_agg(time)) AS times, array_sortby(array_agg(event_type), array_agg(time)) " +
                "AS actions FROM action1 GROUP BY uid) AS t ) AS t1) AS t2;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  |  common expressions:\n" +
                "  |  <slot 22> : array_sort(4: array_agg)\n" +
                "  |  <slot 23> : array_sortby(5: array_agg, 4: array_agg)\n" +
                "  |  <slot 24> : array_map((<slot 8>, <slot 9>) -> (<slot 9> = '浏览') " +
                "AND ((<slot 8> >= '2020-01-02 00:00:00') " +
                "AND (<slot 8> <= '2020-01-02 23:59:59')), 22: array_sort, 23: array_sortby)\n" +
                "  |  <slot 25> : array_filter(22: array_sort, 24: array_map)\n" +
                "  |  <slot 26> : 25: array_filter[1]\n" +
                "  |  <slot 27> : minutes_add(26: expr, 90)\n" +
                "  |  <slot 28> : 26: expr != '2020-01-01 00:00:00'\n" +
                "  |  <slot 29> : array_map((<slot 11>, <slot 12>) -> ((<slot 12> = '下单') " +
                "AND ((<slot 11> >= 26: expr) AND (<slot 11> <= 27: minutes_add))) " +
                "AND (28: expr), 22: array_sort, 23: array_sortby)\n" +
                "  |  <slot 30> : array_filter(22: array_sort, 29: array_map)\n" +
                "  |  <slot 31> : 30: array_filter[1]");
    }

    @Test
    public void testLambdaWithAggAndWindowFunctions() throws Exception {
        starRocksAssert.withTable("create table if not exists test_array12 " +
                "(c0 INT,c1 int, c2 array<int>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        // aggregations
        String sql = "select array_agg(array_length(array_map(x->x*2, c2))) from test_array12";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  output: array_agg(array_length(array_map(<slot 4> -> CAST(<slot 4> AS BIGINT) * 2, 3: c2)))"));
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 3> : 3: c2"));

        sql = "select array_map(x->x > count(c1), c2) from test_array12 group by c2";
        plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 6> : array_map(<slot 5> -> CAST(<slot 5> AS BIGINT) > 4: count, 3: c2)\n"));
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: c1)\n" +
                "  |  group by: 3: c2"));

        // window functions
        sql = "select count(c1) over (partition by array_sum(array_map(x->x+1, [1]))) from test_array12";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:ANALYTIC\n" +
                "  |  functions: [, count(6: c1), ]\n" +
                "  |  partition by: 8: array_sum"));
        Assert.assertTrue(plan.contains("  2:SORT\n" +
                "  |  order by: <slot 8> 8: array_sum ASC\n" +
                "  |  offset:"));
        Assert.assertTrue(plan, plan.contains("  1:Project\n" +
                "  |  <slot 6> : 2: c1\n" +
                "  |  <slot 8> : array_sum(array_map(<slot 4> -> " +
                "CAST(<slot 4> AS SMALLINT) + 1, [1]))"));
    }

    @Test
    public void testInPredicateNormalize() throws Exception {
        starRocksAssert.withTable("create table test_in_pred_norm" +
                "(c0 INT, c1 INT, c2 INT, c3 INT, c4 DATE, c5 DATE) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");

        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0) ", "c0 = 0");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0, 1) ", "c0 IN (0, 1)");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0, 1, 2) ", "c0 IN (0, 1, 2)");

        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (c1) ", "1: c0 = 2: c1");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (c1, c2) ", "(1: c0 = 2: c1) OR (1: c0 = 3: c2)");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (c1, c2, c3) ",
                "((1: c0 = 2: c1) OR (1: c0 = 3: c2)) OR (1: c0 = 4: c3)");

        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (c1) ", "1: c0 != 2: c1");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (c1, c2) ", "1: c0 != 2: c1, 1: c0 != 3: c2");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (c1, c2, c3) ",
                "1: c0 != 2: c1, 1: c0 != 3: c2, 1: c0 != 4: c3");

        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0, c1) ", "(1: c0 = 0) OR (1: c0 = 2: c1)");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0, c1, c2) ",
                "((1: c0 = 0) OR (1: c0 = 2: c1)) OR (1: c0 = 3: c2)");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (0, c1) ", "1: c0 != 0, 1: c0 != 2: c1");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (0, c1, c2) ",
                "1: c0 != 0, 1: c0 != 2: c1, 1: c0 != 3: c2");

        // contains cast expression
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c4 IN ('1970-01-01', '1970-01-01', '1970-02-01') ",
                "c4 IN ('1970-01-01', '1970-01-01', '1970-02-01')");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c4 IN ('292278994-08-17', '1970-01-01', '1970-02-01') ",
                "c4 IN (CAST('292278994-08-17' AS DATE), '1970-01-01', '1970-02-01')");

        // common expression
        testPlanContains("SELECT " +
                        "c4 IN ('292278994-08-17', '1970-02-01') AND " +
                        "c5 IN ('292278994-08-17', '1970-02-01') AND " +
                        "c5 IN ('292278994-08-17', '1970-02-01')  " +
                        " FROM test_in_pred_norm",
                "<slot 7> : ((5: c4 = '1970-02-01') OR (5: c4 = 8: cast)) AND ((6: c5 = '1970-02-01') OR (6: c5 = 8: cast))");

        String plan = getFragmentPlan("SELECT " +
                "c4 IN ('292278994-08-17', '1970-02-01') AND c4 IN ('292278994-08-18', '1970-02-01') AND " +
                "c5 IN ('292278994-08-17', '1970-02-01') AND c5 IN ('292278994-08-18', '1970-02-01') AND " +
                "c5 IN ('292278994-08-17', '1970-02-01') AND c5 IN ('292278994-08-17', '1970-02-01')  " +
                " FROM test_in_pred_norm");
        Assert.assertTrue("plan is " + plan, plan.contains("common expressions:"));
        Assert.assertTrue("plan is \n" + plan, plan.contains("<slot 8> "));
        Assert.assertTrue("plan is \n" + plan, plan.contains("<slot 9> "));
    }

    @Test
    public void testLargeIntMod() throws Exception {
        String sql = "select -123 % 100000000000000000000000000000000000";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 2> : -123\n" +
                "  |  "));
    }

    @Test
    public void testCastDecimalZero() throws Exception {
        String sql = "select (CASE WHEN CAST(t0.v1 AS BOOLEAN ) THEN 0.00 END) BETWEEN (0.07) AND (0.04) from t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 4> : (6: if >= 0.07) AND (6: if <= 0.04)\n" +
                "  |  common expressions:\n" +
                "  |  <slot 5> : CAST(1: v1 AS BOOLEAN)\n" +
                "  |  <slot 6> : if(5: cast, 0.00, NULL)"));
    }

    @Test
    public void testCountDistinctMultiColumns() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        String sql = "select count(distinct L_SHIPMODE,L_ORDERKEY) from lineitem";
        String plan = getFragmentPlan(sql);
        // check use 4 stage agg plan
        Assert.assertTrue(plan.contains("6:AGGREGATE (merge finalize)\n" +
                "  |  output: count(18: count)"));
        Assert.assertTrue(plan.contains("4:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(15: L_SHIPMODE IS NULL, NULL, 1: L_ORDERKEY))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: L_ORDERKEY, 15: L_SHIPMODE"));

        sql = "select count(distinct L_SHIPMODE,L_ORDERKEY) from lineitem group by L_PARTKEY";
        plan = getFragmentPlan(sql);
        // check use 3 stage agg plan
        Assert.assertTrue(plan.contains(" 4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(15: L_SHIPMODE IS NULL, NULL, 1: L_ORDERKEY))\n" +
                "  |  group by: 2: L_PARTKEY\n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: L_ORDERKEY, 2: L_PARTKEY, 15: L_SHIPMODE"));
        Assert.assertTrue(plan.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: L_ORDERKEY, 2: L_PARTKEY, 15: L_SHIPMODE"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testEmptyProjectCountStar() throws Exception {
        String sql = "select count(*) from test_all_type a, test_all_type b where a.t1a is not null";
        String plan = getCostExplain(sql);
        Assert.assertTrue(plan, plan.contains("6:Project\n" +
                "  |  output columns:\n" +
                "  |  25 <-> 1\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * auto_fill_col-->[1.0, 1.0, 0.0, 1.0, 1.0] ESTIMATE\n" +
                "  |  \n" +
                "  5:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * auto_fill_col-->[1.0, 1.0, 0.0, 1.0, 1.0] ESTIMATE\n" +
                "  |  * auto_fill_col-->[1.0, 1.0, 0.0, 1.0, 1.0] ESTIMATE"));
    }

    @Test
    public void testArithmeticCommutative() throws Exception {
        String sql = "select v1 from t0 where 2 / v1  > 3";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 2.0 / CAST(1: v1 AS DOUBLE) > 3.0"));

        sql = "select v1 from t0 where 2 * v1  > 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 2 * 1: v1 > 3"));

        sql = "select v1 from t0 where v1 + 2 > 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 > 1"));

        sql = "select v1 from t0 where  v1 / 2 <=> 3";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: v1 <=> 6");

        sql = "select v1 from t0 where  v1 / -2 > 3";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: v1 < -6");

        sql = "select v1 from t0 where  v1 / abs(-2) > 3";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: CAST(1: v1 AS DOUBLE) / CAST(abs(-2) AS DOUBLE) > 3.0");

        sql = "select v1 from t0 where  v1 / -2 != 3";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: v1 != -6");

        sql = "select v1 from t0 where  v1 / abs(-2) = 3";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: CAST(1: v1 AS DOUBLE) = 3.0 * CAST(abs(-2) AS DOUBLE)");

        sql = "select v1 from t0 where 2 + v1 <= 3";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: v1 <= 1");

        sql = "select v1 from t0 where 2 - v1 <= 3";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: v1 >= -1");

        sql = "select k5 from bigtable where k5 * 2 <= 3";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 5: k5 * 2 <= 3");

        sql = "select k5 from bigtable where 2 / k5 <= 3";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 2 / CAST(5: k5 AS DECIMAL128(38,3)) <= 3");

        sql = "select t1a from test_all_type where date_add(id_datetime, 2) = '2020-12-21'";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 8: id_datetime = '2020-12-19 00:00:00'");

        sql = "select t1a from test_all_type where date_sub(id_datetime, 2) = '2020-12-21'";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 8: id_datetime = '2020-12-23 00:00:00'");

        sql = "select t1a from test_all_type where years_sub(id_datetime, 2) = '2020-12-21'";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 8: id_datetime = '2022-12-21 00:00:00'");

        sql = "select t1a from test_all_type where years_add(id_datetime, 2) = '2020-12-21'";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 8: id_datetime = '2018-12-21 00:00:00'");
    }

    @Test
    public void testNotExpr() throws Exception {
        String sql = "select v1 from t0 where not (v1 in (1, 2))";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("     PREDICATES: 1: v1 NOT IN (1, 2)"));

        sql = "select v1 from t0 where not (v1 > 2)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 <= 2"));

        sql = "select v1 from t0 where not (v1 > 2)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 <= 2"));

        sql = "select v1 from t0 where not (v1 > 2 and v2 < 3)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: (1: v1 <= 2) OR (2: v2 >= 3)"));

        sql = "select v1 from t0 where not (v1 > 2 and v2 is null)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: (1: v1 <= 2) OR (2: v2 IS NOT NULL)"));

        sql = "select v1 from t0 where not (v1 > 2 and v2 is null and v3 < 6)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: ((1: v1 <= 2) OR (2: v2 IS NOT NULL)) OR (3: v3 >= 6)"));

        sql = "select v1 from t0 where not (v1 > 2 and if(v2 > 2, FALSE, TRUE))";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: (1: v1 <= 2) OR (NOT (if(2: v2 > 2, FALSE, TRUE)))"));

        sql = "select v1 from t0 where not (v1 > 2 or v2 is null or if(v3 > 2, FALSE, TRUE))";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(
                planFragment.contains("PREDICATES: 1: v1 <= 2, 2: v2 IS NOT NULL, NOT (if(3: v3 > 2, FALSE, TRUE))"));
    }

    @Test
    public void testCaseWhenType2() throws Exception {
        String sql =
                "select case '10000' when 10000 THEN 'TEST1' WHEN NULL THEN 'TEST2' WHEN 40000 THEN 'TEST4' END FROM t1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n"
                + "  |  <slot 4> : 'TEST1'"));
    }

    @Test
    public void testGISConstantConjunct() throws Exception {
        String sql = "select * from  test.join1 where ST_Contains(\"\", " +
                "APPEND_TRAILING_CHAR_IF_ABSENT(-1338745708, \"RDBLIQK\") )";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString
                .contains("PREDICATES: st_contains('', append_trailing_char_if_absent('-1338745708', 'RDBLIQK'))"));
    }

    @Test
    public void testOrCompoundPredicateFold() throws Exception {
        String queryStr = "select * from baseall where (k1 > 1) or (k1 > 1 and k2 < 1)";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: 1: k1 > 1"));

        queryStr = "select * from  baseall where (k1 > 1 and k2 < 1) or  (k1 > 1)";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: 1: k1 > 1\n"));

        queryStr = "select * from  baseall where (k1 > 1) or (k1 > 1)";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: 1: k1 > 1\n"));
    }

    @Test
    public void testConvertCaseWhenToConstant() throws Exception {
        // basic test
        String caseWhenSql = "select "
                + "case when date_format(now(),'%H%i')  < 123 then 1 else 0 end as col "
                + "from test.baseall "
                + "where k11 = case when date_format(now(),'%H%i') < 123 " +
                "then date_format(date_sub(now(),2),'%Y%m%d') else date_format(date_sub(now(),1),'%Y%m%d') end";
        Assert.assertFalse(StringUtils.containsIgnoreCase(getFragmentPlan(caseWhenSql), "CASE WHEN"));

        // test 1: case when then
        // 1.1 multi when in on `case when` and can be converted to constants
        String sql11 = "select case when false then 2 when true then 3 else 0 end as col11;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql11), "|  <slot 2> : 3"));

        // 1.2 multi `when expr` in on `case when` ,`when expr` can not be converted to constants
        String sql121 =
                "select case when false then 2 when substr(k7,2,1) then 3 else 0 end as col121 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql121),
                "if(CAST(substr(9: k7, 2, 1) AS BOOLEAN), 3, 0)"));

        // 1.2.2 when expr which can not be converted to constants in the first
        String sql122 =
                "select case when substr(k7,2,1) then 2 when false then 3 else 0 end as col122 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql122),
                "if(CAST(substr(9: k7, 2, 1) AS BOOLEAN), 2, 0)"));

        // 1.2.3 test return `then expr` in the middle
        String sql124 = "select case when false then 1 when true then 2 when false then 3 else 'other' end as col124";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql124), "'2'"));

        // 1.3 test return null
        String sql3 = "select case when false then 2 end as col3";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql3), "NULL"));

        // 1.3.1 test return else expr
        String sql131 = "select case when false then 2 when false then 3 else 4 end as col131";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql131), "4"));

        // 1.4 nest `case when` and can be converted to constants
        String sql14 =
                "select case when (case when true then true else false end) then 2 when false then 3 else 0 end as col";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql14), "2"));

        // 1.5 nest `case when` and can not be converted to constants
        String sql15 = "select case when case when substr(k7,2,1) then true else false end " +
                "then 2 when false then 3 else 0 end as col from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql15),
                "if(if(CAST(substr(9: k7, 2, 1) AS BOOLEAN), TRUE, FALSE), 2, 0)"));

        // 1.6 test when expr is null
        String sql16 = "select case when null then 1 else 2 end as col16;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql16), "2"));

        // 1.7 test when true in first return directly
        String sql17 = "select case when true then 1 when substr(k7,2,1) then 3 else 2 end as col16 from test.baseall;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql17), "<slot 12> : 1"));

        // 1.8 test when true in the middle not return directly
        String sql18 = "select case when substr(k7,2,1) then 3 when true then 1 else 2 end as col16 from test.baseall;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql18),
                "CASE WHEN CAST(substr(9: k7, 2, 1) AS BOOLEAN) THEN 3 WHEN TRUE THEN 1 ELSE 2 END"));

        // 1.9 test remove when clause when is false/null
        String sql19 = "select case when substr(k7,2,1) then 3 " +
                "when false then 1 when null then 5 else 2 end as col16 from test.baseall;";
        Assert.assertTrue(StringUtils
                .containsIgnoreCase(getFragmentPlan(sql19), "if(CAST(substr(9: k7, 2, 1) AS BOOLEAN), 3, 2)"));

        // test 2: case xxx when then
        // 2.1 test equal
        String sql2 = "select case 1 when 1 then 'a' when 2 then 'b' else 'other' end as col2;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql2), "'a'"));

        // FIXME(yan): following cases are correct, we have to fix for them.
        // 2.1.2 test not equal
        String sql212 = "select case 'a' when 1 then 'a' when 'a' then 'b' else 'other' end as col212;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql212), "'b'"));

        // 2.2 test return null
        String sql22 = "select case 'a' when 1 then 'a' when 'b' then 'b' end as col22;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql22), "NULL"));

        // 2.2.2 test return else
        String sql222 = "select case 1 when 2 then 'a' when 3 then 'b' else 'other' end as col222;";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql222), "'other'"));

        // 2.3 test can not convert to constant,middle when expr is not constant
        String sql23 = "select case 'a' when 'b' then 'a' " +
                "when substr(k7,2,1) then 2 when false then 3 else 0 end as col23 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql23),
                "if(substr(9: k7, 2, 1) = 'a', '2', '0')"));

        // 2.3.1  first when expr is not constant
        String sql231 = "select case 'a' when substr(k7,2,1) then 2 " +
                "when 1 then 'a' when false then 3 else 0 end as col231 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql231),
                "if(substr(9: k7, 2, 1) = 'a', '2', '0')"));

        // 2.3.2 case expr is not constant
        String sql232 = "select case k1 when substr(k7,2,1) then 2 when 1 " +
                "then 'a' when false then 3 else 0 end as col232 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql232),
                "CASE CAST(1: k1 AS VARCHAR) " +
                        "WHEN substr(9: k7, 2, 1) THEN '2' WHEN '1' THEN 'a' WHEN '0' THEN '3' ELSE '0' END"));

        // 2.4 when expr has true but not equals case expr
        String sql24 = "select case 10 when true then 'a' when 2 then 'b' else 'other' end as col2;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql24),
                "'other'"));

        // 2.5 when expr has true but equals case expr
        String sql25 = "select case 1 when true then 'a' when 2 then 'b' else 'other' end as col2;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql25),
                "'a'"));

        // 2.6 when expr equals case expr in middle
        String sql26 = "select case 'a' when substr(k7,2,1) then 2 " +
                "when 'a' then 'b' else 'other' end as col2 from test.baseall;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql26),
                "CASE 'a' WHEN substr(9: k7, 2, 1) THEN '2' WHEN 'a' THEN 'b' ELSE 'other' END"));

        // 2.7 test remove when clause not equals case expr
        String sql27 = "select case 'a' when substr(k7,2,1) then 3 " +
                "when false then 1 when null then 5 else 2 end as col16 from test.baseall;";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql27), "if(substr(9: k7, 2, 1) = 'a', 3, 2)"));

        // 3.1 test float,float in case expr
        String sql31 = "select case cast(100 as float) when 1 then 'a' when 2 then 'b' else 'other' end as col31;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql31), "'other'"));

        // 4.1 test null in case expr return else
        String sql41 = "select case null when 1 then 'a' when 2 then 'b' else 'other' end as col41";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql41), "'other'"));

        // 4.1.2 test null in case expr return null
        String sql412 = "select case null when 1 then 'a' when 2 then 'b' end as col41";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql412), "NULL"));

        // 4.2.1 test null in when expr
        String sql421 = "select case 'a' when null then 'a' else 'other' end as col421";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql421), "'other'"));

        // 4.2.2 test null/false in when expr
        String sql422 = "select case 'a' when null then 'a' when false then 'b' else 'other' end as col421";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql422), "'other'"));

        // 4.2.3 test null false in when expr return null
        String sql423 = "select case 'a' when null then 'a' when false then 'b' end as col421";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql423), "NULL"));
    }

    @Test
    public void testLargeIntLiteralCompare() throws Exception {
        String sql = "select k2 from baseall group by ((10800861)/(((NULL)%(((-1114980787)+(-1182952114)))))), " +
                "((10800861)*(-9223372036854775808)), k2";
        starRocksAssert.query(sql).explainContains("group by: 2: k2");
    }

    @Test
    public void testDateTypeCastSyntax() throws Exception {
        String castSql = "select * from test.baseall where k11 < cast('2020-03-26' as date)";
        starRocksAssert.query(castSql).explainContains("8: k11 < '2020-03-26 00:00:00'");

        String castSql2 = "select str_to_date('11/09/2011', '%m/%d/%Y');";
        starRocksAssert.query(castSql2).explainContains("constant exprs:", "'2011-11-09'");

        String castSql3 = "select str_to_date('11/09/2011', k6) from test.baseall";
        starRocksAssert.query(castSql3).explainContains("  1:Project\n" +
                "  |  <slot 12> : str_to_date('11/09/2011', 6: k6)");
    }

    @Test
    public void testCastExprAnalyze() throws Exception {
        String sql = "select AVG(DATEDIFF(curdate(),DATE_ADD(curdate(),interval -day(curdate())+1 day))) as a FROM t0";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("cast(curdate() as datetime)"));
    }

    @Test
    public void testCastFloat() throws Exception {
        String sql = "SELECT SUM(count) FROM (" +
                "SELECT CAST((CAST( ( CAST(CAST(t1.v4 AS BOOLEAN )  AS FLOAT )  ) >= ( t1.v5 )  AS BOOLEAN) = true)\n" +
                "AND (CAST( ( CAST(CAST(t1.v4 AS BOOLEAN )  AS FLOAT )  ) >= ( t1.v5 )  AS BOOLEAN) IS NOT NULL) AS "
                + "INT) "
                +
                "as count FROM t1) t;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(1: v4 AS BOOLEAN)"));
    }

    @Test
    public void testProjectUsingConstantArgs() throws Exception {
        String sql = "select months_diff(\"2074-03-04T17:43:24\", \"2074-03-04T17:43:24\") from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("1:Project\n"
                + "  |  <slot 11> : months_diff('2074-03-04 17:43:24', '2074-03-04 17:43:24')\n"));
    }

    @Test
    public void testReduceNonNumberCast() throws Exception {
        String sql;
        String plan;
        sql = "select cast(cast(id_date as string) as boolean) from test_all_type;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(CAST(9: id_date AS VARCHAR(65533)) AS BOOLEAN)");

        sql = "select cast(cast(id_date as datetime) as string) from test_all_type;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(CAST(9: id_date AS DATETIME) AS VARCHAR(65533))");

        sql = "select cast(cast(id_date as boolean) as string) from test_all_type;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(CAST(9: id_date AS BOOLEAN) AS VARCHAR(65533))");

        sql = "select cast(cast(id_datetime as string) as date) from test_all_type;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(CAST(8: id_datetime AS VARCHAR(65533)) AS DATE)");

        sql = "select cast(cast(t1d as int) as boolean) from test_all_type;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(CAST(4: t1d AS INT) AS BOOLEAN)");

        sql = "select cast(cast(t1a as int) as bigint) from test_all_type;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(CAST(1: t1a AS INT) AS BIGINT)");
    }

    @Test
    public void testVarcharAsBitmapCast() throws Exception {
        String sql = "select cast(t1a as BITMAP) from test_all_type;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(1: t1a AS BITMAP)");
    }

    @Test
    public void testTimeSlicePlan() throws Exception {
        String sql = "select time_slice(th, interval 1 year) from tall;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "time_slice(8: th, 1, 'year', 'floor')");

        sql = "select time_slice(th, interval 1 year, CEIL) from tall;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "time_slice(8: th, 1, 'year', 'ceil')");
    }

    @Test
    public void testQualifyForWindowFunction() throws Exception {
        // for '='
        String sql = "select tc from tall qualify row_number() OVER(PARTITION by ta order by tg) = 1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: row_number() = 1");

        sql = "select tc from tall qualify rank() OVER(PARTITION by ta order by tg) = 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: rank() = 1");

        sql = "select tc from tall qualify dense_rank() OVER(PARTITION by ta order by tg) = 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: dense_rank() = 1");

        sql = "select tc from tall qualify cume_dist() OVER(PARTITION by ta order by tg) = 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: cume_dist() = 1");

        sql = "select tc from tall qualify percent_rank() OVER(PARTITION by ta order by tg) = 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: percent_rank() = 1");

        // for '<'
        sql = "select tc from tall qualify row_number() OVER(PARTITION by ta order by tg) < 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: row_number() < 1");

        sql = "select tc from tall qualify rank() OVER(PARTITION by ta order by tg) < 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: rank() < 1");

        sql = "select tc from tall qualify dense_rank() OVER(PARTITION by ta order by tg) < 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: dense_rank() < 1");

        sql = "select tc from tall qualify cume_dist() OVER(PARTITION by ta order by tg) < 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: cume_dist() < 1");

        sql = "select tc from tall qualify percent_rank() OVER(PARTITION by ta order by tg) < 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: percent_rank() < 1");

        // for '>'
        sql = "select tc from tall qualify row_number() OVER(PARTITION by ta order by tg) > 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: row_number() > 1");

        sql = "select tc from tall qualify rank() OVER(PARTITION by ta order by tg) > 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: rank() > 1");

        sql = "select tc from tall qualify dense_rank() OVER(PARTITION by ta order by tg) > 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: dense_rank() > 1");

        sql = "select tc from tall qualify cume_dist() OVER(PARTITION by ta order by tg) > 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: cume_dist() > 1");

        sql = "select tc from tall qualify percent_rank() OVER(PARTITION by ta order by tg) > 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: percent_rank() > 1");

        // for alias
        sql = "select ta as col1 from tall qualify dense_rank() OVER(PARTITION by ta order by tg) > 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: dense_rank() > 1");

        // for where and having
        sql = "select tc from tall WHERE tb <> '' HAVING tc > 3\n" +
                "qualify dense_rank() OVER(PARTITION by ta order by tg) > 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: dense_rank() > 1");
        assertContains(plan, "PREDICATES: 3: tc > 3, CAST(2: tb AS VARCHAR(1048576)) != ''");

        sql = "select tc from tall qualify qualify row_number() OVER(PARTITION by ta order by tg) = 1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: 11: row_number() = 1");

    }

    @Test
    public void testReduceNumberCast() throws Exception {
        String sql;
        String plan;
        sql = "select cast(cast(t1c as bigint) as string) from test_all_type;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(3: t1c AS VARCHAR(65533))");

        sql = "select cast(cast(t1c as bigint) as int) from test_all_type;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:3: t1c");

        sql = "select cast(cast(id_bool as bigint) as int) from test_bool;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(11: id_bool AS INT)");

        sql = "select cast(cast(id_bool as bigint) as string) from test_bool;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(11: id_bool AS VARCHAR(65533))");

        sql = "select cast(cast(id_bool as boolean) as string) from test_bool;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(11: id_bool AS VARCHAR(65533))");
    }

    @Test
    public void testDecimalReuse() throws Exception {
        String sql = "select id_decimal + 1, id_decimal + 2 from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "common expressions:\n" +
                "  |  <slot 13> : CAST(10: id_decimal AS DECIMAL64(12,2))");

        sql = "select concat(cast(t1c as varchar(10)), 'a'), concat(cast(t1c as varchar(10)), 'b') from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "common expressions:\n" +
                "  |  <slot 13> : CAST(3: t1c AS VARCHAR(10))");
    }

    @Test
    public void testTableFunctionNull() throws Exception {
        String sql = "select * from test_all_type, json_each(null)";
        try {
            getFragmentPlan(sql);
            Assert.fail();
        } catch (StarRocksPlannerException e) {
            Assert.assertEquals("table function not support null parameter", e.getMessage());
        }

        String sql2 = "select * from test_all_type, json_each(parse_json(null))";
        try {
            getFragmentPlan(sql2);
            Assert.fail();
        } catch (StarRocksPlannerException e) {
            Assert.assertEquals("table function not support null parameter", e.getMessage());
        }

        // normal case
        String sql3 = "select * from test_all_type, json_each(parse_json('{}'))";
        getFragmentPlan(sql3);
    }

    @Test
    public void testTimestampadd() throws Exception {
        String sql = "select timestampadd(YEAR,1,'2022-04-02 13:21:03')";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<slot 2> : '2023-04-02 13:21:03'"));
    }

    @Test
    public void testDaysSub() throws Exception {
        String sql = "select days_sub('2010-11-30 23:59:59', 0)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<slot 2> : '2010-11-30 23:59:59'"));
    }

    @Test
    public void testMultiStarItem() throws Exception {
        String sql = "select *,v1,* from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3 | 1: v1 | 1: v1 | 2: v2 | 3: v3"));

        sql = "select *,* from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3 | 1: v1 | 2: v2 | 3: v3"));
    }

    @Test
    public void testAssertTrue() throws Exception {
        {
            String sql = "select assert_true(null, 'a')";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : assert_true(NULL, 'a')");
        }
        {
            String sql = "select assert_true(true, 'a')";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : assert_true(TRUE, 'a')");
        }
        {
            String sql = "select assert_true(false, 'a')";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : assert_true(FALSE, 'a')");
        }
    }

    @Test
    public void testBitShift() throws Exception {
        {
            String sql = "select BIT_SHIFT_LEFT(cast(-100 as largeint), 3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : -100 BITSHIFTLEFT 3");
        }
        {
            String sql = "select BIT_SHIFT_RIGHT(cast(-100 as largeint), 3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : -100 BITSHIFTRIGHT 3");
        }
        {
            String sql = "select BIT_SHIFT_RIGHT_LOGICAL(cast(-100 as largeint), 3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : -100 BITSHIFTRIGHTLOGICAL 3");
        }
        {
            String sql = "select cast(-11 as smallint) BITSHIFTLEFT 3";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : -88");
        }
        {
            String sql = "select cast(-11 as smallint) BITSHIFTRIGHT 3";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : -2");
        }
        {
            String sql = "select cast(-11 as smallint) BITSHIFTRIGHTLOGICAL 3";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : 8190");
        }
    }

    @Test
    public void testArithmeticExpressions() throws Exception {
        String sql = "select multiply(400, 500);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 2> : 200000");

        sql = "select subtract(-30000, 40000);";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 2> : -70000");

        sql = "select int_divide(128, 100);";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 2> : 1");

        sql = "select multiply(200, 50);";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 2> : 10000");

        sql = "select multiply(429496, 429496);";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 2> : 184466814016");
    }

    @Test
    public void testInPredicate() throws Exception {
        String sql = "select * from t0 where v1 in (v2, v3, 3, 4, 5) ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: ((1: v1 IN (3, 4, 5)) OR (1: v1 = 2: v2)) OR (1: v1 = 3: v3)");
    }

    @Test
    public void testBitNotLargeInt() throws Exception {
        String sql = "select bitnot(cast(power(-2,127) as largeint)+1);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "~ CAST(power(-2.0, 127.0) AS LARGEINT) + 1");
    }

    @Test
    public void testConstantFoldInLikeFunction() throws Exception {
        String sql1 = "select like('AA', concat('a', 'A'))";
        String plan1 = getFragmentPlan(sql1);
        assertContains(plan1, "<slot 2> : like('AA', 'aA')");

        String sql2 = "select ilike('AA', concat('a', 'A'))";
        String plan2 = getFragmentPlan(sql2);
        assertContains(plan2, "<slot 2> : like(lower('AA'), lower('aA'))");
    }

    @Test
    public void testStructExpression() throws Exception {
        String sql = "select struct('a', 1, 2, 10000)";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "struct<col1 varchar, col2 tinyint(4), col3 tinyint(4), col4 smallint(6)>");

        sql = "select row('a', 1, 2, 10000, [1, 2, 3], NULL, map{'a': 1, 'b': 2})";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "struct<col1 varchar, col2 tinyint(4), col3 tinyint(4), col4 smallint(6), " +
                "col5 array<tinyint(4)>, col6 boolean, col7 map<varchar,tinyint(4)>>");

        sql = "select named_struct('a', 1, 'b', 2)";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "struct<a tinyint(4), b tinyint(4)>");

        try {
            sql = "select named_struct('a', 1, 'b', 2, 3, 6)";
            plan = getVerboseExplain(sql);
        } catch (Exception e) {
            assertCContains(e.getMessage(), "The 5-th input of named_struct must be string literal");
        }

        try {
            sql = "select named_struct('a', 1, 'b', 2, 3, 'x', 6)";
            plan = getVerboseExplain(sql);
        } catch (Exception e) {
            assertCContains(e.getMessage(), "named_struct arguments must be in name/value pairs");
        }

        try {
            sql = "select named_struct('a', 1, 'a', 2)";
            plan = getVerboseExplain(sql);
        } catch (Exception e) {
            assertCContains(e.getMessage(), "named_struct contains duplicate subfield name: a at 3-th input");
        }
    }

    @Test
    public void testStructRow() throws Exception {
        String sql = "select row('a', 1, 'a', 2).col1";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "row('a', 1, 'a', 2).col1");
    }

    @Test
    public void testStructCollection() throws Exception {
        String sql = "select row('a', 1, 'a', 2)[1]";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "row('a', 1, 'a', 2).col1");

        sql = "select row('a', 1, 'a', 2)[4]";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "row('a', 1, 'a', 2).col4");

        sql = "select row('a', 1, 'a', 2)[4]";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "row('a', 1, 'a', 2).col4");

        sql = "select row('a', 1, 'a', 2)[-1]";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "row('a', 1, 'a', 2).col4");

        sql = "select row('a', 1, 'a', 2)[-4]";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "row('a', 1, 'a', 2).col1");
    }

    @Test
    public void testJsonArray() throws Exception {
        String sql = "select array_distinct([PARSE_JSON('{1: 2}')])";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "array_distinct[([parse_json('{1: 2}')]); args: INVALID_TYPE; result: ARRAY<JSON>;");

        sql = "select array_distinct([NULL])";
        plan = getVerboseExplain(sql);
        assertCContains(plan, "array_distinct[([NULL]); args: INVALID_TYPE; result: ARRAY<BOOLEAN>;");
    }

    @Test
    public void testDoubleCastToString() throws Exception {
        String sql = "select concat(substr(DATE_SUB(CURDATE(), INTERVAL 1 DAY), 1, 4) -1, '-');";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "2022-");

        sql = "select cast(cast(20.00 as double) as string);";
        plan = getVerboseExplain(sql);
        assertContains(plan, "'20'");
    }

    @Test
    public void testCastTDatetime() throws Exception {
        String sql = "select cast('2020-05-01T13:45:57' as date);";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "'2020-05-01'");

        sql = "select cast('2020-05-01T13:45:57' as datetime);";
        plan = getVerboseExplain(sql);
        assertContains(plan, "2020-05-01 13:45:57");
    }

    @Test
    public void testDecimalCastString() throws Exception {
        String sql = "select cast(cast('1.10000' as decimal(27,9)) as string)";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "1.100000000");
    }

    @Test
    public void testDecimalV2Cast1() throws Exception {
        String sql = "select length(cast('12.3567' as decimalV2(9,1)) * 200)";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "2 <-> length[('2471.34'); args: VARCHAR; result: INT;");
        sql = "select length(col) from (select cast('12.3567' as decimalV2(9,1)) * 200 as col) t";
        plan = getVerboseExplain(sql);
        assertContains(plan, "3 <-> length[('2471.34'); args: VARCHAR; result: INT;");

        sql = "select length(col) from (select cast('12.3567' as decimal(9,1)) * 200 as col) t";
        plan = getVerboseExplain(sql);
        assertContains(plan, "3 <-> length[('2480.0'); args: VARCHAR; result: INT;");
    }

    @Test
    public void testCoalesce() throws Exception {
        String sql = "select Coalesce(null, 1, 2, null)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 2> : 1");

        sql = "select Coalesce(1, 2, null)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 2> : 1");
    }

    @Test
    public void testCoalesceJoin() throws Exception {
        String sql = "select * from t0 left outer join t1 on t0.v2 = t1.v5 where coalesce(t1.v6, 2) = 3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 5: v5");
    }

    @Test
    public void testDateAddReduce() throws Exception {
        String sql = "select date_add(date_add(date_add(v2, 1), 2), 3) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "days_add(CAST(2: v2 AS DATETIME), 6)");

        sql = "select date_add(date_add(date_add(v2, -1), -2), -3) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "days_sub(CAST(2: v2 AS DATETIME), 6)");

        sql = "select years_add(years_sub(years_sub(v2, -1), -2), -3) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 4> : CAST(2: v2 AS DATETIME)");

        sql = "select date_add(date_add(date_sub(v2, -1), -2), -3) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "days_sub(CAST(2: v2 AS DATETIME), 4)");

        sql = "select date_add(weeks_add(date_sub(v2, -1), -2), 3) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "days_add(weeks_add(days_sub(CAST(2: v2 AS DATETIME), -1), -2), 3)");

        sql = "select adddate(subdate(adddate(v2, -1), 2), -3) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "days_sub(CAST(2: v2 AS DATETIME), 6)");

        sql = "select months_add(months_add(months_sub(v2, -1), 2), NULL) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 4> : NULL");
    }

    @Test
    public void testDateTrunc() throws Exception {
        String sql = "select date_trunc('day', cast(v2 as date)) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 4> : CAST(2: v2 AS DATE)");

        sql = "select date_trunc('day', cast(v2 as datetime)) from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 4> : date_trunc('day', CAST(2: v2 AS DATETIME))");
    }
}
