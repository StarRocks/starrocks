// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.Test;

public class WindowTest extends PlanTestBase {
    @Test
    public void testLagWindowFunction() throws Exception {
        String sql = "select lag(id_datetime, 1, '2020-01-01') over(partition by t1c) from test_all_type;";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("signature:lag(DATETIME, BIGINT, DATETIME)"));

        sql = "select lag(id_decimal, 1, 10000) over(partition by t1c) from test_all_type;";
        plan = getThriftPlan(sql);
        String expectSlice = "fn:TFunction(name:TFunctionName(function_name:lag), binary_type:BUILTIN," +
                " arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL64," +
                " precision:10, scale:2))])], ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, " +
                "scalar_type:TScalarType(type:DECIMAL64, precision:10, scale:2))]), has_var_args:false, " +
                "signature:lag(DECIMAL64(10,2))";
        Assert.assertTrue(plan.contains(expectSlice));

        sql = "select lag(null, 1,1) OVER () from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("functions: [, lag(NULL, 1, 1), ]"));

        sql = "select lag(id_datetime, 1, '2020-01-01xxx') over(partition by t1c) from test_all_type;";
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("The third parameter of `lag` can't not convert to DATETIME");
        getThriftPlan(sql);
    }

    @Test
    public void testPruneWindowColumn() throws Exception {
        String sql = "select sum(t1c) from (select t1c, lag(id_datetime, 1, '2020-01-01') over( partition by t1c)" +
                "from test_all_type) a ;";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("ANALYTIC"));
    }

    @Test
    public void testWindowFunctionTest() throws Exception {
        String sql = "select sum(id_decimal - ifnull(id_decimal, 0)) over (partition by t1c) from test_all_type";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(
                plan.contains("decimal_literal:TDecimalLiteral(value:0, integer_value:00 00 00 00 00 00 00 00)"));
    }

    @Test
    public void testSameWindowFunctionReuse() throws Exception {
        String sql = "select sum(v1) over() as c1, sum(v1) over() as c2 from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Project\n" +
                "  |  <slot 4> : 4: sum(1: v1)\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]"));

        sql = "select c1+1, c2+2 from (select sum(v1) over() as c1, sum(v1) over() as c2 from t0) t";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Project\n" +
                "  |  <slot 5> : 4: sum(1: v1) + 1\n" +
                "  |  <slot 6> : 4: sum(1: v1) + 2\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]"));

        sql = "select c1+1, c2+2 from (select sum(v1) over() as c1, sum(v3) over() as c2 from t0) t";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Project\n" +
                "  |  <slot 6> : 4: sum(1: v1) + 1\n" +
                "  |  <slot 7> : 5: sum(3: v3) + 2\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ], [, sum(3: v3), ]"));
    }

    @Test
    public void testLeadAndLagFunction() {
        String sql = "select LAG(k7, 3, 3) OVER () from baseall";
        starRocksAssert.query(sql).analysisError("The third parameter of `lag` can't not convert");

        sql = "select lead(k7, 3, 3) OVER () from baseall";
        starRocksAssert.query(sql).analysisError("The third parameter of `lead` can't not convert");

        sql = "select lead(k3, 3, 'kks') OVER () from baseall";
        starRocksAssert.query(sql)
                .analysisError("Convert type error in offset fn(default value); old_type=VARCHAR new_type=INT");

        sql = "select lead(id2, 1, 1) OVER () from bitmap_table";
        starRocksAssert.query(sql).analysisError("No matching function with signature: lead(bitmap,");

        sql = "select lag(id2, 1, 1) OVER () from hll_table";
        starRocksAssert.query(sql).analysisError("No matching function with signature: lag(hll,");
    }

    @Test
    public void testWindowWithAgg() throws Exception {
        String sql = "SELECT v1, sum(v2),  sum(v2) over (ORDER BY v1) AS `rank` FROM t0 group BY v1, v2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"));

        sql =
                "SELECT v1, sum(v2),  sum(v2) over (ORDER BY CASE WHEN v1 THEN 1 END DESC) AS `rank`  FROM t0 group BY v1, v2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"));
    }

    @Test
    public void testWindowWithChildProjectAgg() throws Exception {
        String sql = "SELECT v1, sum(v2) as x1, row_number() over (ORDER BY CASE WHEN v1 THEN 1 END DESC) AS `rank` " +
                "FROM t0 group BY v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  <slot 8> : if(CAST(1: v1 AS BOOLEAN), 1, NULL)"));
    }

    @Test
    public void testWindowPartitionAndSortSameColumn() throws Exception {
        String sql = "SELECT k3, avg(k3) OVER (partition by k3 order by k3) AS sum FROM baseall;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:ANALYTIC\n" +
                "  |  functions: [, avg(3: k3), ]\n" +
                "  |  partition by: 3: k3\n" +
                "  |  order by: 3: k3 ASC"));
        Assert.assertTrue(plan.contains("  2:SORT\n" +
                "  |  order by: <slot 3> 3: k3 ASC"));
    }

    @Test
    public void testWindowDuplicatePartition() throws Exception {
        String sql = "select max(v3) over (partition by v2,v2,v2 order by v2,v2) from t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:SORT\n"
                + "  |  order by: <slot 2> 2: v2 ASC\n"
                + "  |  offset: 0"));

    }

    @Test
    public void testWindowDuplicatedColumnInPartitionExprAndOrderByExpr() throws Exception {
        String sql = "select v1, sum(v2) over (partition by v1, v2 order by v2 desc) as sum1 from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertNotNull(plan);
    }

    @Test
    public void testSupersetEnforce() throws Exception {
        String sql = "select * from (select v3, rank() over (partition by v1 order by v2) as j1 from t0) as x0 "
                + "join t1 on x0.v3 = t1.v4 order by x0.v3, t1.v4 limit 100;";
        getFragmentPlan(sql);
    }
}
