// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.Test;

public class SetTest extends PlanTestBase {
    @Test
    public void testValuesNodePredicate() throws Exception {
        String queryStr = "SELECT 1 AS z, MIN(a.x) FROM (select 1 as x) a WHERE abs(1) = 2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  2:AGGREGATE (update finalize)\n"
                + "  |  output: min(1: expr)\n"
                + "  |  group by: \n"
                + "  |  \n"
                + "  1:SELECT\n"
                + "  |  predicates: abs(1) = 2\n"));
    }

    @Test
    public void testUnionSameValues() throws Exception {
        String query = "SELECT 76072, COUNT(DISTINCT b3) * 10, '', '', now() FROM test_object" +
                " UNION ALL" +
                " SELECT 76072, COUNT(DISTINCT b4) *10, '', '', now() FROM test.test_object";
        getFragmentPlan(query);
    }

    @Test
    public void testUnionAllConst() throws Exception {
        String sql = "select b from (select t1a as a, t1b as b, t1c as c, t1d as d from test_all_type " +
                "union all select 1 as a, 2 as b, 3 as c, 4 as d) t1;";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains(
                "const_expr_lists:[[TExpr(nodes:[TExprNode(node_type:INT_LITERAL, type:TTypeDesc(types:[TTypeNode"
                        + "(type:SCALAR, scalar_type:TScalarType(type:SMALLINT))]), num_children:0, "
                        + "int_literal:TIntLiteral"
                        + "(value:2), "
                        + "output_scale:-1, has_nullable_child:false, is_nullable:false, "
                        + "is_monotonic:true)])]]"));
    }

    @Test
    public void testUnionEmpty() throws Exception {
        String sql =
                "SELECT DISTINCT RPAD('kZcD', 1300605171, '') FROM t0 WHERE false UNION ALL SELECT DISTINCT RPAD"
                        + "('kZcD', 1300605171, '') FROM t0 WHERE false IS NULL;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:UNION"));
    }

    @Test
    public void testSetOperation() throws Exception {
        // union
        String sql1 = "select * from\n"
                + "  (select k1, k2 from db1.tbl6\n"
                + "   union all\n"
                + "   select k1, k2 from db1.tbl6) a\n"
                + "  inner join\n"
                + "  db1.tbl6 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        starRocksAssert.query(sql1).explainContains("UNION", 1);

        String sql2 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "union distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   select * from db1.tbl6 where k1='b' and k4=2)\n"
                + "union distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "union all\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   union all\n"
                + "   select * from db1.tbl6 where k1='b' and k4=4)\n"
                + "union all\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   union all\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";
        starRocksAssert.query(sql2).explainContains("UNION", 6);

        // intersect
        String sql3 = "select * from\n"
                + "  (select k1, k2 from db1.tbl6\n"
                + "   intersect\n"
                + "   select k1, k2 from db1.tbl6) a\n"
                + "  inner join\n"
                + "  db1.tbl6 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        starRocksAssert.query(sql3).explainContains("INTERSECT", 1);

        String sql4 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   intersect\n"
                + "   select * from db1.tbl6 where k1='b' and k4=2)\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   intersect\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "intersect\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   select * from db1.tbl6 where k1='b' and k4=4)\n"
                + "intersect\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";
        starRocksAssert.query(sql4).explainContains("INTERSECT", 5);

        String sql5 = "select * from\n"
                + "  (select k1, k2 from db1.tbl6\n"
                + "   except\n"
                + "   select k1, k2 from db1.tbl6) a\n"
                + "  inner join\n"
                + "  db1.tbl6 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        starRocksAssert.query(sql5).explainContains("EXCEPT", 1);

        String sql6 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl6 where k1='a' and k4=2\n"
                + "except distinct\n"
                + "(select * from db1.tbl6 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        starRocksAssert.query(sql6).explainContains("EXCEPT", 1);

        String sql7 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except distinct\n"
                + "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl6 where k1='a' and k4=2\n"
                + "except\n"
                + "(select * from db1.tbl6 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        starRocksAssert.query(sql7).explainContains("EXCEPT", 1);

        // mixed
        String sql8 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "union\n"
                + "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl6 where k1='a' and k4=2\n"
                + "intersect\n"
                + "(select * from db1.tbl6 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        starRocksAssert.query(sql8).explainContains("UNION", "INTERSECT", "EXCEPT");

        String sql9 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   select * from db1.tbl6 where k1='b' and k4=2)\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   except\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "union all\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   select * from db1.tbl6 where k1='b' and k4=4)\n"
                + "except\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";
        starRocksAssert.query(sql9).explainContains("UNION", 2);
        starRocksAssert.query(sql9).explainContains("INTERSECT", 3);
        starRocksAssert.query(sql9).explainContains("EXCEPT", 2);

        String sql10 = "select 499 union select 670 except select 499";
        String plan = getFragmentPlan(sql10);
        Assert.assertTrue(plan.contains("  10:UNION\n" +
                "     constant exprs: \n" +
                "         499"));
        Assert.assertTrue(plan.contains("1:UNION"));
        Assert.assertTrue(plan.contains("  4:UNION\n" +
                "     constant exprs: \n" +
                "         670"));
        Assert.assertTrue(plan.contains("  2:UNION\n" +
                "     constant exprs: \n" +
                "         499"));
        Assert.assertTrue(plan.contains("0:EXCEPT"));
    }

}
