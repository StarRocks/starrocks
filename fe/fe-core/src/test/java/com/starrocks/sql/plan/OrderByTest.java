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

import com.starrocks.qe.SessionVariable;
import org.junit.Assert;
import org.junit.Test;

public class OrderByTest extends PlanTestBase {

    @Test
    public void testExistOrderBy() throws Exception {
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
    public void testSort() throws Exception {
        String sql = "select count(*) from (select L_QUANTITY, L_PARTKEY, L_ORDERKEY from lineitem " +
                "order by L_QUANTITY, L_PARTKEY, L_ORDERKEY limit 5000, 10000) as a;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:MERGING-EXCHANGE"));
    }

    @Test
    public void testPruneSortColumns() throws Exception {
        String sql = "select count(v1) from (select v1 from t0 order by v2 limit 10) t";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Project\n" +
                "  |  <slot 1> : 1: v1"));
    }

    @Test
    public void testSortProject() throws Exception {
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
    public void testTopNOffsetError() throws Exception {
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
    public void testOrderBySameColumnDiffOrder() throws Exception {
        String sql = "select v1 from t0 order by v1 desc, v1 asc";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:SORT\n" +
                "  |  order by: <slot 1> 1: v1 DESC"));
    }

    @Test
    public void testUnionOrderByDuplicateColumn() throws Exception {
        String sql = "select * from t0 union all select * from t1 order by v1, v2, v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 7> 7: v1 ASC, <slot 8> 8: v2 ASC");
    }

    @Test
    public void testSqlSelectLimit() throws Exception {
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
    public void testOrderByWithSubquery() throws Exception {
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
    public void tstOrderByNullLiteral() throws Exception {
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
                "     numNodes=0\n" +
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
    public void testOrderByGroupByWithSubquery() throws Exception {
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
    public void testOrderByTransform() throws Exception {
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
    public void testUDTFWithOrderBy() throws Exception {
        String sql = "select t.* from t0, unnest([1,2,3]) as t order by `unnest`";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: unnest ASC");
    }

    @Test
    public void testOrderByWithSameColumnName() throws Exception {
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
    public void testOrderByWithFieldReference() throws Exception {
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
    public void testOrderByWithWindow() throws Exception {
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
        assertContains(plan, "6:SORT\n" +
                "  |  order by: <slot 8> 8: sum(4: v1) ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 4> : 4: v1\n" +
                "  |  <slot 5> : 5: v2\n" +
                "  |  <slot 8> : 8: sum(4: v1)\n" +
                "  |  \n" +
                "  4:ANALYTIC\n" +
                "  |  functions: [, sum(4: v1), ]\n" +
                "  |  partition by: 7: expr");

        sql = "select v1, v2, sum(v1) over(partition by v1 + 1) as v from t0 order by avg(v1) over(partition by v1 + 1)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "6:SORT\n" +
                "  |  order by: <slot 9> 9: avg(4: v1) ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 4> : 4: v1\n" +
                "  |  <slot 5> : 5: v2\n" +
                "  |  <slot 8> : 8: sum(4: v1)\n" +
                "  |  <slot 9> : 9: avg(4: v1)\n" +
                "  |  \n" +
                "  4:ANALYTIC\n" +
                "  |  functions: [, sum(4: v1), ], [, avg(4: v1), ]");

        sql = "select v1, v2, sum(v1) over(partition by v1 + 1) as v from t0 order by 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 5> 5: v2 ASC");

        sql = "select v1, v2, sum(v1) over(partition by v1 + 1) as v from t0 order by 3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "6:SORT\n" +
                "  |  order by: <slot 8> 8: sum(4: v1) ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 4> : 4: v1\n" +
                "  |  <slot 5> : 5: v2\n" +
                "  |  <slot 8> : 8: sum(4: v1)\n" +
                "  |  \n" +
                "  4:ANALYTIC\n" +
                "  |  functions: [, sum(4: v1), ]\n" +
                "  |  partition by: 7: expr");

        sql = "select avg(v1) over(partition by sum(v2) + 1) as v from t0 group by v1 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 7:SORT\n" +
                "  |  order by: <slot 9> 9: avg(1: v1) ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  6:Project\n" +
                "  |  <slot 9> : 9: avg(1: v1)\n" +
                "  |  \n" +
                "  5:ANALYTIC\n" +
                "  |  functions: [, avg(1: v1), ]\n" +
                "  |  partition by: 8: expr\n" +
                "  |  \n" +
                "  4:SORT\n" +
                "  |  order by: <slot 8> 8: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 8: expr\n" +
                "\n" +
                "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 8> : 4: sum + 1\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 1: v1");

        sql = "select v1, avg(v1) over(partition by sum(v2) + 1) as v from t0 group by v1 order by 2,1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "7:SORT\n" +
                "  |  order by: <slot 9> 9: avg(1: v1) ASC, <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  6:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 9> : 9: avg(1: v1)\n" +
                "  |  \n" +
                "  5:ANALYTIC\n" +
                "  |  functions: [, avg(1: v1), ]\n" +
                "  |  partition by: 8: expr\n" +
                "  |  \n" +
                "  4:SORT\n" +
                "  |  order by: <slot 8> 8: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 8: expr\n" +
                "\n" +
                "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 8> : 4: sum + 1");

        sql = "select avg(v1) over(partition by sum(v2) + 1) as v from t0 group by v1 order by v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 7:SORT\n" +
                "  |  order by: <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  6:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 9> : 9: avg(1: v1)\n" +
                "  |  \n" +
                "  5:ANALYTIC\n" +
                "  |  functions: [, avg(1: v1), ]\n" +
                "  |  partition by: 8: expr\n" +
                "  |  \n" +
                "  4:SORT\n" +
                "  |  order by: <slot 8> 8: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 8: expr\n" +
                "\n" +
                "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 8> : 4: sum + 1\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 1: v1");
    }

    @Test
    public void testTopNFilter() throws Exception {
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
    }
}