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

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

class OrderByTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    void testExistOrderBy() throws Exception {
        String sql = "SELECT * \n" +
                "FROM   emp \n" +
                "WHERE  EXISTS (SELECT dept.dept_id \n" +
                "               FROM   dept \n" +
                "               WHERE  emp.dept_id = dept.dept_id \n" +
                "               ORDER  BY state) \n" +
                "ORDER  BY hiredate";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("LEFT SEMI JOIN"));
    }

    @Test
    void testSort() throws Exception {
        String sql = "select count(*) from (select L_QUANTITY, L_PARTKEY, L_ORDERKEY from lineitem " +
                "order by L_QUANTITY, L_PARTKEY, L_ORDERKEY limit 5000, 10000) as a;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:MERGING-EXCHANGE"));
    }

    @Test
    void testPruneSortColumns() throws Exception {
        String sql = "select count(v1) from (select v1 from t0 order by v2 limit 10) t";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Project\n" +
                "  |  <slot 1> : 1: v1"));
    }

    @Test
    void testSortProject() throws Exception {
        String sql = "select avg(null) over (order by ref_0.v1) as c2 "
                + "from t0 as ref_0 left join t1 as ref_1 on (ref_0.v1 = ref_1.v4 );";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains(
                "sort_tuple_slot_exprs:[TExpr(nodes:[TExprNode(node_type:SLOT_REF, type:TTypeDesc(types:[TTypeNode"
                        + "(type:SCALAR, scalar_type:TScalarType(type:BIGINT))]), num_children:0, slot_ref:TSlotRef"
                        + "(slot_id:1, tuple_id:2), output_scale:-1, output_column:-1, "
                        + "has_nullable_child:false, is_nullable:true, is_monotonic:true)])]"));
    }

    @Test
    void testTopNOffsetError() throws Exception {
        long limit = connectContext.getSessionVariable().getSqlSelectLimit();
        connectContext.getSessionVariable().setSqlSelectLimit(200);
        String sql = "select * from (select * from t0 order by v1 limit 5) as a left join t1 on a.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:TOP-N\n" +
                "  |  order by: <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 5"));
        connectContext.getSessionVariable().setSqlSelectLimit(limit);
    }

    @Test
    void testOrderBySameColumnDiffOrder() throws Exception {
        String sql = "select v1 from t0 order by v1 desc, v1 asc";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:SORT\n" +
                "  |  order by: <slot 1> 1: v1 DESC"));
    }

    @Test
    void testUnionOrderByDuplicateColumn() throws Exception {
        String sql = "select * from t0 union all select * from t1 order by v1, v2, v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 7> 7: v1 ASC, <slot 8> 8: v2 ASC");
    }

    @Test
    void testSqlSelectLimit() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(200);
        // test order by with project
        String sql;
        String plan;

        sql = "select L_QUANTITY from lineitem order by L_QUANTITY, L_PARTKEY";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:TOP-N\n" +
                "  |  order by: <slot 5> 5: L_QUANTITY ASC, <slot 2> 2: L_PARTKEY ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 200");

        sql = sql + " limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:TOP-N\n" +
                "  |  order by: <slot 5> 5: L_QUANTITY ASC, <slot 2> 2: L_PARTKEY ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10");
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
    }

    @Test
    void testOrderByWithSubquery() throws Exception {
        String sql = "select t0.*, " +
                "(select sum(v5) from t1) as x1, " +
                "(select sum(v7) from t2) as x2 from t0 order by t0.v3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  15:SORT\n" +
                "  |  order by: <slot 3> 3: v3 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  14:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 8> : 8: sum\n" +
                "  |  <slot 13> : 12: sum\n" +
                "  |  \n" +
                "  13:NESTLOOP JOIN");
    }

    @Test
    void tstOrderByNullLiteral() throws Exception {
        String sql;
        String plan;

        sql = "select * from t0 order by null limit 10;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     limit: 10");

        sql = "select * from (select max(v5) from t1) tmp order by null limit 10;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: max(2: v5)\n" +
                "  |  group by: \n" +
                "  |  limit: 10");

        // TODO opt this case
        sql = "select * from (select max(v5) from t1) tmp order by \"\" > null limit 10;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:TOP-N\n" +
                "  |  order by: <slot 5> 5: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 4> : 4: max\n" +
                "  |  <slot 5> : NULL");
    }

    @Test
    void testOrderByGroupByWithSubquery() throws Exception {
        String sql = "select t0.v2, sum(v3) as x3, " +
                "(select sum(v5) from t1) as x1, " +
                "(select sum(v7) from t2) as x2 from t0 group by v2 order by x3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  16:SORT\n" +
                "  |  order by: <slot 4> 4: sum ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  15:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  <slot 9> : 9: sum\n" +
                "  |  <slot 14> : 13: sum\n" +
                "  |  \n" +
                "  14:NESTLOOP JOIN\n");
    }

    @Test
    void testOrderByTransform() throws Exception {
        String sql = "select v1, * from test.t0 order by v2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 2> 2: v2 ASC");

        sql = "select v1+1 as v from t0 group by v1+1 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: expr ASC");
        assertContains(plan, "<slot 4> : 1: v1 + 1");

        sql = "select distinct v1, v2 from t0 order by v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: <slot 2> 2: v2 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: v1, 2: v2");

        sql = "select distinct v1, v2 from t0 order by v1 + 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "3:SORT\n" +
                "  |  order by: <slot 4> 4: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 1: v1 + 1");

        sql = "select * from t0 a join t0 b where a.v1 = b.v1 order by a.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 2> 2: v2 ASC");

        sql = "select v1 v from t0 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1 v from t0 order by v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select sum(v1) as v, v2, v3 from t0 group by v2, v3 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:SORT\n" +
                "  |  order by: <slot 4> 4: sum ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: 2: v2, 3: v3");

        sql = "select sum(v1) as v, v2 from t0 group by v2, v3 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:SORT\n" +
                "  |  order by: <slot 4> 4: sum ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: 2: v2, 3: v3");


        sql = "select abs(t0.v1), abs(t1.v1) from t0, t0_not_null t1 order by t1.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:SORT\n" +
                "  |  order by: <slot 5> 5: v2 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 5> : 5: v2\n" +
                "  |  <slot 7> : abs(1: v1)\n" +
                "  |  <slot 8> : abs(4: v1)");

        sql = "select abs(t0.v1), abs(t1.v1) from t0, t0_not_null t1 order by t1.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 5:SORT\n" +
                "  |  order by: <slot 4> 4: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 4> : 4: v1\n" +
                "  |  <slot 7> : abs(1: v1)\n" +
                "  |  <slot 8> : abs(4: v1)");
    }

    @Test
    void testUDTFWithOrderBy() throws Exception {
        String sql = "select t.* from t0, unnest([1,2,3]) as t order by `unnest`";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: unnest ASC");
    }

    @Test
    void testOrderByWithSameColumnName() throws Exception {
        String sql = "select t0_not_null.*, t0.* from t0, t0_not_null order by t0_not_null.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: v1 ASC");

        sql = "select t0_not_null.*, t0.* from t0, t0_not_null order by t0.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select t0.*, t0_not_null.* from t0, t0_not_null order by t0.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select t0.*, t0_not_null.* from t0, t0_not_null order by t0_not_null.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: v1 ASC");

        sql = "select t0.v1, t0_not_null.v1 from t0, t0_not_null order by t0_not_null.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: v1 ASC");
    }

    @Test
    void testOrderByWithFieldReference() throws Exception {
        String sql = "select * from t0 order by 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select * from t0 order by 3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 3> 3: v3 ASC");

        sql = "select *, v1 from t0 order by 3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 3> 3: v3 ASC");

        sql = "select *, v1 from t0 order by 4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1, v1 from t0 order by 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1, v1 from t0 order by 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1 as v, v1 as v from t0 order by 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1 as v, v1 as v from t0 order by 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1 as v, *, v2 + 1 as vv from t0 order by 5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:SORT\n" +
                "  |  order by: <slot 4> 4: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 2: v2 + 1");
    }

    @Test
    void testOrderByWithWindow() throws Exception {
        String sql = "select sum(v1) over(partition by v1 + 1) from t0 order by v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: v1 ASC");

        sql = "select sum(v1) over(partition by v1 + 1) from t0 order by v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 5> 5: v2 ASC");

        sql = "select v1, v2, sum(v1) over(partition by v1 + 1) as v from t0 order by v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 5> 5: v2 ASC");

        sql = "select v1, v2, sum(v1) over(partition by v1 + 1) as v from t0 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:SORT\n" +
                "  |  order by: <slot 8> 8: sum(4: v1) ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 4> : 4: v1\n" +
                "  |  <slot 5> : 5: v2\n" +
                "  |  <slot 8> : 8: sum(4: v1)\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, sum(4: v1), ]\n" +
                "  |  partition by: 7: expr");

        sql = "select v1, v2, sum(v1) over(partition by v1 + 1) as v from t0 order by avg(v1) over(partition by v1 + 1)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:SORT\n" +
                "  |  order by: <slot 9> 9: avg(4: v1) ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 4> : 4: v1\n" +
                "  |  <slot 5> : 5: v2\n" +
                "  |  <slot 8> : 8: sum(4: v1)\n" +
                "  |  <slot 9> : 9: avg(4: v1)\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, sum(4: v1), ], [, avg(4: v1), ]");

        sql = "select v1, v2, sum(v1) over(partition by v1 + 1) as v from t0 order by 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 5> 5: v2 ASC");

        sql = "select v1, v2, sum(v1) over(partition by v1 + 1) as v from t0 order by 3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:SORT\n" +
                "  |  order by: <slot 8> 8: sum(4: v1) ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 4> : 4: v1\n" +
                "  |  <slot 5> : 5: v2\n" +
                "  |  <slot 8> : 8: sum(4: v1)\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, sum(4: v1), ]\n" +
                "  |  partition by: 7: expr");

        sql = "select avg(v1) over(partition by sum(v2) + 1) as v from t0 group by v1 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 6:SORT\n" +
                "  |  order by: <slot 9> 9: avg(1: v1) ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 9> : 9: avg(1: v1)\n" +
                "  |  \n" +
                "  4:ANALYTIC\n" +
                "  |  functions: [, avg(1: v1), ]\n" +
                "  |  partition by: 8: expr\n" +
                "  |  \n" +
                "  3:SORT\n" +
                "  |  order by: <slot 8> 8: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 8> : 4: sum + 1\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 1: v1");

        sql = "select v1, avg(v1) over(partition by sum(v2) + 1) as v from t0 group by v1 order by 2,1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "6:SORT\n" +
                "  |  order by: <slot 9> 9: avg(1: v1) ASC, <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 9> : 9: avg(1: v1)\n" +
                "  |  \n" +
                "  4:ANALYTIC\n" +
                "  |  functions: [, avg(1: v1), ]\n" +
                "  |  partition by: 8: expr\n" +
                "  |  \n" +
                "  3:SORT\n" +
                "  |  order by: <slot 8> 8: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 8> : 4: sum + 1");

        sql = "select avg(v1) over(partition by sum(v2) + 1) as v from t0 group by v1 order by v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 6:SORT\n" +
                "  |  order by: <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 9> : 9: avg(1: v1)\n" +
                "  |  \n" +
                "  4:ANALYTIC\n" +
                "  |  functions: [, avg(1: v1), ]\n" +
                "  |  partition by: 8: expr\n" +
                "  |  \n" +
                "  3:SORT\n" +
                "  |  order by: <slot 8> 8: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 8> : 4: sum + 1\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 1: v1");
    }

    @Test
    void testTopNFilter() throws Exception {
        String sql = "select * from test_all_type_not_null order by t1a limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:TOP-N\n" +
                "  |  order by: [1, VARCHAR, false] ASC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 1> 1: t1a), remote = false");

        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (<slot 1> 1: t1a)");

        // TopN filter only works in no-nullable column
        sql = "select * from test_all_type order by t1a limit 10";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "runtime filters");

        // only first order by column can use top n filter
        sql = "select * from test_all_type_not_null order by t1a, t1b limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  1:TOP-N\n" +
                "  |  order by: [1, VARCHAR, false] ASC, [2, SMALLINT, false] ASC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 1> 1: t1a), remote = false");

        // no order by column case
        sql = "SELECT a, b FROM ( SELECT t1a AS a, t1b AS b, row_number() OVER() AS rn FROM" +
                " test_all_type_not_null ) tb_rn WHERE rn>=10 and rn<19;";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "runtime filters");

        sql = "SELECT a, b FROM ( SELECT t1a AS a, t1b AS b, row_number() OVER( partition by t1a) AS rn FROM" +
                " test_all_type_not_null ) tb_rn WHERE rn<19;";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "runtime filters");

        // no order by column pattern 2
        sql = "select * from test_all_type_not_null limit 1000,200";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "runtime filters");

        // order by null case
        sql = "select * from test_all_type_not_null order by null + 1 limit 1";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "runtime filters");
    }

    @Test
    void testGroupByOrderBy() throws Exception {
        String sql = "select v2,v3,v2 from t0 group by 1,2,3 order by 1,2,3";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "2:SORT\n" +
                "  |  order by: <slot 2> 2: v2 ASC, <slot 3> 3: v3 ASC");
    }

    @Test
    void testTopNFilterWithProject() throws Exception {
        String sql;
        String plan;

        // project passthrough good case
        sql = "select t1a, t1b from test_all_type_not_null where t1f < 10 order by t1a limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:TOP-N\n" +
                "  |  order by: [1, VARCHAR, false] ASC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 1> 1: t1a), remote = false");

        // Hash join probe good case
        sql = "select l.t1a, l.t1b from test_all_type_not_null l join test_all_type_not_null r " +
                "on l.t1a=r.t1a order by l.t1a limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  5:TOP-N\n" +
                "  |  order by: [1, VARCHAR, false] ASC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 1, build_expr = (<slot 1> 1: t1a), remote = false");
        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: t1a)\n" +
                "     - filter_id = 1, probe_expr = (1: t1a)");

        // sort/Bucket AGG order by good case
        sql = "select count(*) from test_all_type_not_null group by t1a order by t1a limit 10;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: t1a)");

        // shouldn't generate filter for agg column
        sql = "select count(*) cnt from test_all_type_not_null group by t1a order by cnt limit 10;";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "runtime filters");

        // shouldn't generate filter for window function
        sql = "select row_number() over (partition by t1b) from test_all_type_not_null order by t1a limit 1;";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "runtime filters");

        // partition by column case
        // but we have not implements streaming sort in BE
        sql = "select row_number() over (partition by t1a) from test_all_type_not_null order by t1a limit 1;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  3:TOP-N\n" +
                "  |  order by: [1, VARCHAR, false] ASC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 1> 1: t1a), remote = false");
    }

    @Test
    public void testTopNRuntimeFilterWithFilter() throws Exception {
        String sql = "select * from t0 where v1 > 1 order by v1 limit 10";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (<slot 1> 1: v1)");

        String sql1 = "select * from t0 where v1 is not null order by v1 limit 10";
        String plan1 = getVerboseExplain(sql1);

        assertContains(plan1, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (<slot 1> 1: v1)");

        String sql2 = "select * from t0 where v1 is null order by v1 limit 10";
        String plan2 = getVerboseExplain(sql2);
        assertNotContains(plan2, " runtime filters");
    }

    @Test
    public void testTopNRuntimeFilterWithNotPrunedPartitionFilter() throws Exception {
        String sql = "select * from lineitem_partition where L_SHIPDATE >= '1990-01-01' order by L_SHIPDATE limit 100;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (<slot 11> 11: L_SHIPDATE)");
    }

    @Test
    public void testTopNRuntimeFilterWithPrunedPartitionFilter() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from lineitem_partition where L_SHIPDATE >= '1993-01-01' order by L_SHIPDATE limit 100;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "     probe runtime filters:\n");
        FeConstants.runningUnitTest = false;
    }

    @ParameterizedTest
    @MethodSource("failToStrictSql")
    void testFailToStrictOrderByExpression(String sql) {
        Assert.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
    }

    @ParameterizedTest
    @MethodSource("successToStrictSql")
    void testSuccessToStrictOrderByExpression(String sql, String expectedPlan) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedPlan);
    }

    @ParameterizedTest
    @MethodSource("allOrderBySql")
    void testNotStrictOrderByExpression(String sql, String expectedPlan) throws Exception {
        String hint = "select /*+ set_var(enable_strict_order_by = false) */ ";
        sql = hint + sql.substring(7);
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedPlan);
    }

    private static Stream<Arguments> allOrderBySql() {
        return Stream.concat(successToStrictSql(), failToStrictSql());
    }

    private static Stream<Arguments> successToStrictSql() {
        List<Arguments> list = Lists.newArrayList();
        list.add(Arguments.of("select * from t0 order by 1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select abs(v1) v1, * from t0  order by 1", "order by: <slot 4> 4: abs ASC"));
        list.add(Arguments.of("select distinct * from t0  order by 1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select * from t0 order by v1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select t0.* from t0 order by t0.v1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select distinct t0.* from t0 order by t0.v1", "order by: <slot 1> 1: v1 ASC"));


        list.add(Arguments.of("select *, v1 from t0  order by v1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select *, v1 from t0  order by abs(v1)", "order by: <slot 4> 4: abs ASC"));
        list.add(Arguments.of("select v1, * from t0  order by abs(v1)", "order by: <slot 4> 4: abs ASC"));
        list.add(Arguments.of("select distinct * from t0 order by v1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select distinct *, v1 from t0  order by abs(v1)", "order by: <slot 4> 4: abs ASC"));
        list.add(Arguments.of("select distinct abs(v1) v1 from t0 order by v1", "order by: <slot 4> 4: abs ASC"));
        list.add(Arguments.of("select distinct abs(v1) from t0 order by abs(v1)", "order by: <slot 4> 4: abs ASC"));
        list.add(Arguments.of("select distinct abs(v1) v1 from t0 order by abs(v1)", "order by: <slot 5> 5: abs ASC"));
        return list.stream();
    }

    private static Stream<Arguments> failToStrictSql() {
        List<Arguments> list = Lists.newArrayList();
        list.add(Arguments.of("select *, v1, abs(v1) v1 from t0  order by 1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select distinct *, v1, abs(v1) v1 from t0  order by 1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select distinct abs(v1) v1, * from t0  order by 1", "order by: <slot 4> 4: abs ASC"));
        list.add(Arguments.of("select distinct *, v1, abs(v1) v1 from t0  order by v1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select v1, max(v2) v1 from t0 group by v1  order by abs(v1)", "order by: <slot 5> 5: abs ASC"));
        list.add(Arguments.of("select max(v2) v1, v1 from t0 group by v1  order by abs(v1)", "order by: <slot 5> 5: abs ASC"));
        list.add(Arguments.of("select v2, max(v2) v2 from t0 group by v2  order by max(v2)", "order by: <slot 4> 4: max ASC"));
        list.add(Arguments.of("select max(v2) v2, v2 from t0 group by v2  order by max(v2)", "order by: <slot 4> 4: max ASC"));
        list.add(Arguments.of("select upper(v1) v1, *, v1 from t0 order by v1", "order by: <slot 4> 4: upper ASC"));
        list.add(Arguments.of("select *, v1, upper(v1) v1 from t0 order by v1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select distinct upper(v1) v1, *, v1 from t0 order by v1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select distinct *, v1, upper(v1) v1 from t0 order by v1", "order by: <slot 1> 1: v1 ASC"));
        list.add(Arguments.of("select distinct abs(v1) v1, v1 from t0 order by v1", "order by: <slot 4> 4: abs ASC"));

        return list.stream();
    }
}