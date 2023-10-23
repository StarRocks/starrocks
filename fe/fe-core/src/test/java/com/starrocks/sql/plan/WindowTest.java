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

import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class WindowTest extends PlanTestBase {

    @Test
    public void testLagWindowFunction() throws Exception {
        String sql = "select lag(id_datetime, 1, '2020-01-01') over(partition by t1c) from test_all_type;";
        String plan = getThriftPlan(sql);
        assertContains(plan, "lag");

        sql = "select lag(id_decimal, 1, 10000) over(partition by t1c) from test_all_type;";
        plan = getThriftPlan(sql);
        String expectSlice = "fn:TFunction(name:TFunctionName(function_name:lag), binary_type:BUILTIN, " +
                "arg_types:[" +
                "TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL64, precision:10, scale:2))]), " +
                "TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:BIGINT))]), " +
                "TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL64, precision:10, scale:2))])], " +
                "ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, " +
                "scalar_type:TScalarType(type:DECIMAL64, precision:10, scale:2))]), " +
                "has_var_args:false";
        System.out.println(expectSlice);
        Assert.assertTrue(plan, plan.contains(expectSlice));

        sql = "select lag(null, 1,1) OVER () from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, lag(NULL, 1, 1), ]");

        sql = "select lag(id_datetime, 1, '2020-01-01xxx') over(partition by t1c) from test_all_type;";
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("The type of the third parameter of LEAD/LAG not match the type DATETIME");
        getThriftPlan(sql);
    }

    @Test
    public void testPruneWindowColumn() throws Exception {
        String sql = "select sum(t1c) from (select t1c, lag(id_datetime, 1, '2020-01-01') over( partition by t1c)" +
                "from test_all_type) a ;";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "ANALYTIC");
    }

    @Test
    public void testPruneEmptyWindow() throws Exception {
        String sql = "select count(*) from( select avg(t1g) over(partition by t1a) from test_all_type ) r";
        assertLogicalPlanContains(sql,
                "AGGREGATE ([GLOBAL] aggregate [{12: count=count()}] group by [[]] having [null]\n" +
                        "    SCAN (columns[2: t1b] predicate[null])");
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
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 4> : 4: sum(1: v1)\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]");

        sql = "select sum(v1) over(order by v2 rows between 1 preceding and 1 following) as sum_v1_1," +
                " sum(v1) over(order by v2 rows between 1 preceding and 1 following) as sum_v1_2 from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:Project\n" +
                "  |  <slot 4> : 4: sum(1: v1)\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]");

        sql = "select c1+1, c2+2 from (select sum(v1) over() as c1, sum(v1) over() as c2 from t0) t";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 5> : 4: sum(1: v1) + 1\n" +
                "  |  <slot 6> : 4: sum(1: v1) + 2\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]");

        sql = "select c1+1, c2+2 from (select sum(v1) over() as c1, sum(v3) over() as c2 from t0) t";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 6> : 4: sum(1: v1) + 1\n" +
                "  |  <slot 7> : 5: sum(3: v3) + 2\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ], [, sum(3: v3), ]");
    }

    @Test
    public void testLeadAndLagFunction() throws Exception {
        String sql = "select LAG(k7, 3, 3) OVER () from baseall";
        starRocksAssert.query(sql).explainContains("functions: [, lag(9: k7, 3, '3'), ]");

        sql = "select lead(k7, 3, 3) OVER () from baseall";
        starRocksAssert.query(sql).explainContains("functions: [, lead(9: k7, 3, '3'), ]");

        sql = "select lead(k3, 3, 'kks') OVER () from baseall";
        starRocksAssert.query(sql).analysisError("The third parameter of LEAD/LAG can't convert to INT");

        sql = "select lead(k3, 3, abs(k3)) over () from baseall";
        starRocksAssert.query(sql).analysisError("The default parameter (parameter 3) of LAG must be a constant");

        sql = "select lead(id2, 1, 1) OVER () from bitmap_table";
        starRocksAssert.query(sql).analysisError("No matching function with signature: lead(bitmap,");

        sql = "select lag(id2, 1, 1) OVER () from hll_table";
        starRocksAssert.query(sql).analysisError("No matching function with signature: lag(hll,");
    }

    @Test
    public void testLeadAndLagWithBitmapAndHll() throws Exception {
        String sql = "select lead(id2, 1, bitmap_empty()) OVER () from bitmap_table";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "lead(2: id2, 1, bitmap_empty())");

        sql = "select lead(id2, 1, null) OVER () from bitmap_table";
        plan = getFragmentPlan(sql);
        assertContains(plan, "lead(2: id2, 1, null)");

        sql = "select lag(id2, 1, bitmap_empty()) OVER () from bitmap_table";
        plan = getFragmentPlan(sql);
        assertContains(plan, "lag(2: id2, 1, bitmap_empty())");

        sql = "select lag(id2, 1, null) OVER () from bitmap_table";
        plan = getFragmentPlan(sql);
        assertContains(plan, "lag(2: id2, 1, null)");

        sql = "select lead(id2, 1, hll_empty()) OVER () from hll_table";
        plan = getFragmentPlan(sql);
        assertContains(plan, "lead(2: id2, 1, hll_empty())");

        sql = "select lead(id2, 1, null) OVER () from hll_table";
        plan = getFragmentPlan(sql);
        assertContains(plan, "lead(2: id2, 1, null)");

        sql = "select lag(id2, 1, hll_empty()) OVER () from hll_table";
        plan = getFragmentPlan(sql);
        assertContains(plan, "lag(2: id2, 1, hll_empty())");

        sql = "select lag(id2, 1, null) OVER () from hll_table";
        plan = getFragmentPlan(sql);
        assertContains(plan, "lag(2: id2, 1, null)");
    }

    @Test
    public void testWindowWithAgg() throws Exception {
        String sql = "SELECT v1, sum(v2),  sum(v2) over (ORDER BY v1) AS `rank` FROM t0 group BY v1, v2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql =
                "SELECT v1, sum(v2),  sum(v2) over (ORDER BY CASE WHEN v1 THEN 1 END DESC) AS `rank`  FROM t0 group BY v1, v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    public void testWindowWithChildProjectAgg() throws Exception {
        String sql = "SELECT v1, sum(v2) as x1, row_number() over (ORDER BY CASE WHEN v1 THEN 1 END DESC) AS `rank` " +
                "FROM t0 group BY v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  <slot 8> : if(CAST(1: v1 AS BOOLEAN), 1, NULL)");
    }

    @Test
    public void testWindowPartitionAndSortSameColumn() throws Exception {
        String sql = "SELECT k3, avg(k3) OVER (partition by k3 order by k3) AS sum FROM baseall;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:ANALYTIC\n" +
                "  |  functions: [, avg(3: k3), ]\n" +
                "  |  partition by: 3: k3\n" +
                "  |  order by: 3: k3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: <slot 3> 3: k3 ASC\n" +
                "  |  offset: 0");
    }

    @Test
    public void testWindowDuplicatePartition() throws Exception {
        String sql = "select max(v3) over (partition by v2,v2,v2 order by v2,v2) from t0;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: <slot 2> 2: v2 ASC\n" +
                "  |  offset: 0");

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

    @Test
    public void testNtileWindowFunction() throws Exception {
        // Must have exactly one positive bigint integer parameter.
        String sql = "select v1, v2, NTILE(v2) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(1.1) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(0) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(-1) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE('abc') over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(null) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(1 + 2) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(9223372036854775808) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql).analysisError("Number out of range");

        sql = "select v1, v2, NTILE((select v1 from t0)) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE() " +
                "over (partition by v1 order by v2 rows between 1 preceding and 1 following) as j1 from t0";
        starRocksAssert.query(sql).analysisError("No matching function with signature: ntile()");

        // Windowing clause not allowed with NTILE.
        sql = "select v1, v2, NTILE(2) " +
                "over (partition by v1 order by v2 rows between 1 preceding and 1 following) as j1 from t0";
        starRocksAssert.query(sql).analysisError("Windowing clause not allowed");

        // Normal case.
        sql = "select v1, v2, NTILE(2) over (partition by v1 order by v2) as j1 from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:ANALYTIC\n" +
                "  |  functions: [, ntile(2), ]\n" +
                "  |  partition by: 1: v1\n" +
                "  |  order by: 2: v2 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT RO");
    }

    @Test
    public void testStatisticWindowFunction() throws Exception {
        String sql = "select CORR(t1e,t1f) over (partition by t1a order by t1b) from test_all_type";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql = "select CORR(t1e,t1f) over (partition by t1a order by t1b " +
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    public void testRankingWindowWithoutPartitionPredicatePushDown() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:TOP-N\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 4");
            sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk < 4;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:TOP-N\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 4");
            sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk = 4;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:TOP-N\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 4");
        }
        {
            // Two window function share the same sort group cannot be optimized
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (order by v2) as rk, " +
                    "        sum(v1) over (order by v2) as sm " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        sum(v1) over (order by v2) as sm, " +
                    "        row_number() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Two window function do not share the same sort group
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        sum(v1) over (order by v3) as sm," +
                    "        row_number() over (order by v2) as rk" +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  4:TOP-N\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 4");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testRankingWindowWithoutPartitionLimitPushDown() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk limit 5;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:TOP-N\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 5");
        }
        {
            // Two window function share the same sort group cannot be optimized
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (order by v2) as rk, " +
                    "        sum(v1) over (order by v2) as sm " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk limit 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        sum(v1) over (order by v2) as sm, " +
                    "        row_number() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk limit 5";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Two window function do not share the same sort group
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        sum(v1) over (order by v3) as sm, " +
                    "        row_number() over (order by v2) as rk" +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk,sm limit 100,1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:TOP-N\n" +
                    "  |  order by: <slot 5> 5: row_number() ASC, <slot 4> 4: sum(1: v1) ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 101");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testRankingWindowWithPartitionPredicatePushDown() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 3: v3 \n" +
                    "  |  partition limit: 4\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
            sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk < 4;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 3: v3 \n" +
                    "  |  partition limit: 4\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        rank() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  type: RANK\n" +
                    "  |  partition by: 3: v3 \n" +
                    "  |  partition limit: 4\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
            sql = "select * from (\n" +
                    "    select *, " +
                    "        rank() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk = 4;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  type: RANK\n" +
                    "  |  partition by: 3: v3 \n" +
                    "  |  partition limit: 4\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Do not support dense_rank by now
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        dense_rank() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:SORT\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  1:EXCHANGE");
        }
        {
            // Support multi partition by.
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (partition by v2, v3 order by v1) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 2: v2 , 3: v3 \n" +
                    "  |  partition limit: 4\n" +
                    "  |  order by: <slot 2> 2: v2 ASC, <slot 3> 3: v3 ASC, <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (partition by v2, v3 order by v1) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk limit 4;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 2: v2 , 3: v3 \n" +
                    "  |  partition limit: 4\n" +
                    "  |  order by: <slot 2> 2: v2 ASC, <slot 3> 3: v3 ASC, <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testRankingWindowWithPartitionLimitPushDown() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk limit 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 3: v3 \n" +
                    "  |  partition limit: 5\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        rank() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk limit 10, 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  type: RANK\n" +
                    "  |  partition by: 3: v3 \n" +
                    "  |  partition limit: 15\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        rank() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk,v2 limit 10, 5";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  type: RANK\n" +
                    "  |  partition by: 3: v3 \n" +
                    "  |  partition limit: 15\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // order by direction mismatch
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        rank() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk desc limit 10, 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:SORT\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        rank() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk desc,v2 limit 10, 5";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  2:SORT\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Order by column mismatch
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by v2 limit 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:SORT\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by v2,rk limit 5";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  2:SORT\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Do not support dense_rank by now
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        dense_rank() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by rk limit 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:SORT\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  1:EXCHANGE");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testCumeWindowFunctionCommon() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            // Windowing clause not allowed with CUME_DIST and PERCENT_RANK.
            String sql = "select v1, v2, cume_dist() " +
                    "over (partition by v1 order by v2 rows between 1 preceding and 1 following) as cd from t0";
            starRocksAssert.query(sql).analysisError("Windowing clause not allowed");

            sql = "select v1, v2, percent_rank() " +
                    "over (partition by v1 order by v2 rows between 1 preceding and 1 following) as pr from t0";
            starRocksAssert.query(sql).analysisError("Windowing clause not allowed");

            // Normal case.
            sql = "select v1, v2, cume_dist() over (partition by v1 order by v2) as cd from t0";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:ANALYTIC\n" +
                    "  |  functions: [, cume_dist(), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  order by: 2: v2 ASC\n" +
                    "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

            sql = "select v1, v2, percent_rank() over (partition by v1 order by v2) as pr from t0";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  2:ANALYTIC\n" +
                    "  |  functions: [, percent_rank(), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  order by: 2: v2 ASC\n" +
                    "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testCumeWindowWithoutPartitionPredicate() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            // Cannot be optimized, the result of cume_dist() and percent_rank() is double.
            // They need partition_count and peer_group_count to calculate.
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        cume_dist() over (order by v2) as cd " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where cd <= 4;";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "TOP-N");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        percent_rank() over (order by v2) as pr " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where pr <= 4;";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "TOP-N");
        }
        {
            // Cannot be optimized, Share the same sort group with rank().
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        cume_dist() over (order by v2) as cd, " +
                    "        rank() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where cd <= 4;";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "TOP-N");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        rank() over (order by v2) as rk, " +
                    "        cume_dist() over (order by v2) as cd " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where cd <= 4;";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "TOP-N");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        percent_rank() over (order by v2) as pr, " +
                    "        rank() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where pr <= 4;";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "TOP-N");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        rank() over (order by v2) as rk, " +
                    "        percent_rank() over (order by v2) as pr " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where pr <= 4;";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "TOP-N");
        }
        {
            // Cannot be optimized and do not share the same sort group with row_number().
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        cume_dist() over (order by v3) as cd," +
                    "        row_number() over (order by v2) as rk" +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 3> 3: v3 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        percent_rank() over (order by v3) as pr," +
                    "        row_number() over (order by v2) as rk" +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 3> 3: v3 ASC\n" +
                    "  |  offset: 0");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testCumeWindowWithoutPartitionLimit() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            // Cannot be optimized.
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        cume_dist() over (order by v2) as cd " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by cd limit 5;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        percent_rank() over (order by v2) as pr " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by pr limit 5;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Cannot be optimized and share the same sort group with rank().
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        cume_dist() over (order by v2) as cd, " +
                    "        rank() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by cd limit 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        rank() over (order by v2) as rk, " +
                    "        cume_dist() over (order by v2) as cd " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by cd limit 5";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        percent_rank() over (order by v2) as pr, " +
                    "        sum(v1) over (order by v2) as sm " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by pr limit 5";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        sum(v1) over (order by v2) as sm, " +
                    "        percent_rank() over (order by v2) as pr " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by pr limit 5";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Cannot be optimized and two window function do not share the same sort group.
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        sum(v1) over (order by v3) as sm, " +
                    "        cume_dist() over (order by v2) as cd" +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by cd,sm limit 100,1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  4:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        sum(v1) over (order by v3) as sm, " +
                    "        percent_rank() over (order by v2) as pr" +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by pr,sm limit 100,1";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  4:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testCumeWindowWithPartition() throws Exception {
        FeConstants.runningUnitTest = true;
        // Predicate.
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        cume_dist() over (partition by v3 order by v2) as cd " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where cd <= 4;";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "PARTITION-TOP-N");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        cume_dist() over (partition by v3 order by v2) as cd " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where cd < 4;";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "PARTITION-TOP-N");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        percent_rank() over (partition by v3 order by v2) as pr " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where pr <= 4;";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "PARTITION-TOP-N");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        percent_rank() over (partition by v3 order by v2) as pr " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where pr < 4;";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "PARTITION-TOP-N");
        }
        // Limit.
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        cume_dist() over (partition by v3 order by v2) as cd " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by cd limit 5";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "PARTITION-TOP-N");

            sql = "select * from (\n" +
                    "    select *, " +
                    "        percent_rank() over (partition by v3 order by v2) as pr " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "order by pr limit 5";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "PARTITION-TOP-N");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testRuntimeFilterPushWithoutPartition() throws Exception {
        String sql = "select * from " +
                "(select v1, sum(v2) over (order by v2 desc) as sum1 from t0) a," +
                "(select v1 from t0 where v1 = 1) b " +
                "where a.v1 = b.v1";

        String plan = getVerboseExplain(sql);
        assertContains(plan, "3:ANALYTIC\n" +
                "  |  functions: [, sum[([2: v2, BIGINT, true]); args: BIGINT; result: BIGINT; " +
                "args nullable: true; result nullable: true], ]\n" +
                "  |  order by: [2: v2, BIGINT, true] DESC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                "  |  cardinality: 1\n" +
                "  |  probe runtime filters:\n" +
                "  |  - filter_id = 0, probe_expr = (1: v1)");
    }

    @Test
    public void testRuntimeFilterPushWithRightPartition() throws Exception {
        String sql = "select * from " +
                "(select v1, sum(v2) over (partition by v1 order by v2 desc) as sum1 from t0) a," +
                "(select v1 from t0 where v1 = 1) b " +
                "where a.v1 = b.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: [1, BIGINT, true] ASC, [2, BIGINT, true] DESC\n" +
                "  |  offset: 0\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: t0, rollup: t0\n" +
                "     preAggregation: on\n" +
                "     Predicates: 1: v1 IS NOT NULL\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=2.0\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: v1)");
    }

    @Test
    public void testRuntimeFilterPushWithOtherPartition() throws Exception {
        String sql = "select * from " +
                "(select v1, v3, sum(v2) over (partition by v3 order by v2 desc) as sum1 from t0) a," +
                "(select v1 from t0 where v1 = 1) b " +
                "where a.v1 = b.v1";

        String plan = getVerboseExplain(sql);
        assertContains(plan, "  2:ANALYTIC\n" +
                "  |  functions: [, sum[([2: v2, BIGINT, true]); args: BIGINT; " +
                "result: BIGINT; args nullable: true; result nullable: true], ]\n" +
                "  |  partition by: [3: v3, BIGINT, true]\n" +
                "  |  order by: [2: v2, BIGINT, true] DESC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                "  |  cardinality: 1\n" +
                "  |  probe runtime filters:\n" +
                "  |  - filter_id = 0, probe_expr = (1: v1)");
    }

    @Test
    public void testHashBasedWindowTest() throws Exception {
        {
            String sql = "select sum(v1) over ([hash] partition by v1,v2 )," +
                    "sum(v1/v3) over ([hash] partition by v1,v2 ) " +
                    "from t0";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:ANALYTIC\n" +
                    "  |  functions: [, sum(1: v1), ], [, sum(CAST(1: v1 AS DOUBLE) / CAST(3: v3 AS DOUBLE)), ]\n" +
                    "  |  partition by: 1: v1, 2: v2\n" +
                    "  |  useHashBasedPartition");
        }
        {
            String sql = "select sum(v1) over ( partition by v1,v2 )," +
                    "sum(v1/v3) over ([hash] partition by v1,v2 ) " +
                    "from t0";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  4:ANALYTIC\n" +
                    "  |  functions: [, sum(1: v1), ]\n" +
                    "  |  partition by: 1: v1, 2: v2");
            assertContains(plan, "  1:ANALYTIC\n" +
                    "  |  functions: [, sum(CAST(1: v1 AS DOUBLE) / CAST(3: v3 AS DOUBLE)), ]\n" +
                    "  |  partition by: 1: v1, 2: v2\n" +
                    "  |  useHashBasedPartition");
        }
        {
            String sql = "select sum(v1) over ([hash] partition by v1 )," +
                    "sum(v1/v3) over ([hash] partition by v1,v2 ) " +
                    "from t0";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  3:ANALYTIC\n" +
                    "  |  functions: [, sum(1: v1), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  useHashBasedPartition");
            assertContains(plan, "  1:ANALYTIC\n" +
                    "  |  functions: [, sum(CAST(1: v1 AS DOUBLE) / CAST(3: v3 AS DOUBLE)), ]\n" +
                    "  |  partition by: 1: v1, 2: v2\n" +
                    "  |  useHashBasedPartition");
        }
    }

    @Test
    public void testOneTablet() throws Exception {
        {
            String sql = "select v1 from (" +
                    "select v1, dense_rank() over (partition by v1 order by v3) rk from t0" +
                    ") temp " +
                    "where rk = 1 " +
                    "group by v1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:ANALYTIC\n" +
                    "  |  functions: [, dense_rank(), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  order by: 3: v3 ASC\n" +
                    "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  1:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC, <slot 3> 3: v3 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: t0");
            assertContains(plan, "  5:AGGREGATE (update finalize)\n" +
                    "  |  group by: 1: v1\n" +
                    "  |  \n" +
                    "  4:Project\n" +
                    "  |  <slot 1> : 1: v1");
        }
        {
            String sql = "select v1 from (" +
                    "select v1, dense_rank() over (partition by v1, v2 order by v3) rk from t0" +
                    ") temp " +
                    "where rk = 1 " +
                    "group by v1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:ANALYTIC\n" +
                    "  |  functions: [, dense_rank(), ]\n" +
                    "  |  partition by: 1: v1, 2: v2\n" +
                    "  |  order by: 3: v3 ASC\n" +
                    "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  1:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC, <slot 2> 2: v2 ASC, <slot 3> 3: v3 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: t0");
            assertContains(plan, "  7:AGGREGATE (merge finalize)\n" +
                    "  |  group by: 1: v1\n" +
                    "  |  \n" +
                    "  6:EXCHANGE");
        }
    }

    @Test
    public void testWindowWithHaving() throws Exception {
        String sql =
                "    select *, " +
                        "        row_number() over (PARTITION BY v1 order by v2) as rk " +
                        "    from t0 where v1 > 1 and v2 > 1 having rk = 1;\n";

        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("HAVING clause cannot contain window function");
        getFragmentPlan(sql);
    }

    @Test
    public void testApproxTopK() throws Exception {
        {
            String sql = "select approx_top_k(L_LINENUMBER) over() from lineitem";
            getFragmentPlan(sql);
            sql = "select approx_top_k(L_LINENUMBER, 10000) over() from lineitem";
            getFragmentPlan(sql);
            sql = "select approx_top_k(L_LINENUMBER, 100, 10000) over() from lineitem";
            getFragmentPlan(sql);
            sql = "select approx_top_k(L_LINENUMBER, 10000, 10000) over() from lineitem";
            getFragmentPlan(sql);
            sql = "select approx_top_k(L_LINENUMBER, 1, 1) over() from lineitem";
            getFragmentPlan(sql);
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, '10001') over() from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The second parameter of APPROX_TOP_K must be a constant positive integer";
            String actualMessage = exception.getMessage();
            Assert.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 1, '11111') over() from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The third parameter of APPROX_TOP_K must be a constant positive integer";
            String actualMessage = exception.getMessage();
            Assert.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 10001) over() from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The maximum number of the second parameter is 10000";
            String actualMessage = exception.getMessage();
            Assert.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 0) over() from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The second parameter of APPROX_TOP_K must be a constant positive integer";
            String actualMessage = exception.getMessage();
            Assert.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 1, 10001) over() from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The maximum number of the third parameter is 10000";
            String actualMessage = exception.getMessage();
            Assert.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 1, -1) over() from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The third parameter of APPROX_TOP_K must be a constant positive integer";
            String actualMessage = exception.getMessage();
            Assert.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(SemanticException.class, () -> {
                String sql = "select approx_top_k(L_LINENUMBER, 100, 99) over() from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "The second parameter must be smaller than or equal to the third parameter";
            String actualMessage = exception.getMessage();
            Assert.assertTrue(actualMessage.contains(expectedMessage));
        }
        {
            Exception exception = Assertions.assertThrows(IllegalStateException.class, () -> {
                String sql =
                        "select approx_top_k(L_LINENUMBER, 10000, 10000) over(order by L_LINESTATUS) from lineitem";
                getFragmentPlan(sql);
            });
            String expectedMessage = "Unexpected order by clause for approx_top_k()";
            String actualMessage = exception.getMessage();
            Assert.assertEquals(expectedMessage, actualMessage);
        }
    }

    @Test
    public void testWindowColumnReuse() throws Exception {
        String sql = "select *, row_number() over(partition by concat(v1, '_', v2) " +
                "order by cast(v3 as bigint)) from t0";
        String plan = getCostExplain(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  3 <-> [3: v3, BIGINT, true]\n" +
                "  |  4 <-> [1: v1, BIGINT, true]\n" +
                "  |  5 <-> [2: v2, BIGINT, true]\n" +
                "  |  6 <-> clone([3: v3, BIGINT, true])\n" +
                "  |  7 <-> concat[(cast([1: v1, BIGINT, true] as VARCHAR), '_', " +
                "cast([2: v2, BIGINT, true] as VARCHAR)); args: VARCHAR; result: VARCHAR; " +
                "args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1");
    }
}
