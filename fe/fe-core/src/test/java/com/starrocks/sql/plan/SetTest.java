// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SetExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

public class SetTest extends PlanTestBase {
    @Test
    public void testValuesNodePredicate() throws Exception {
        String queryStr = "SELECT 1 AS z, MIN(a.x) FROM (select 1 as x) a WHERE abs(1) = 2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  3:AGGREGATE (update finalize)\n" +
                "  |  output: min(2: expr)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: abs(1) = 2"));
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
                "TExprNode(node_type:INT_LITERAL, type:TTypeDesc(types:[TTypeNode(type:SCALAR, " +
                        "scalar_type:TScalarType(type:TINYINT))]), num_children:0, int_literal:TIntLiteral(value:2)," +
                        " output_scale:-1, has_nullable_child:false, is_nullable:false, is_monotonic:true"));
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
        Assert.assertTrue(plan.contains("  3:Project\n" +
                "  |  <slot 2> : 499\n" +
                "  |  \n" +
                "  2:UNION\n" +
                "     constant exprs: \n" +
                "         NULL"));
        Assert.assertTrue(plan.contains("  6:Project\n" +
                "  |  <slot 4> : 670\n" +
                "  |  \n" +
                "  5:UNION\n" +
                "     constant exprs: \n" +
                "         NULL"));
        Assert.assertTrue(plan.contains("  12:Project\n" +
                "  |  <slot 7> : 499\n" +
                "  |  \n" +
                "  11:UNION\n" +
                "     constant exprs: \n" +
                "         NULL"));
        Assert.assertTrue(plan.contains("0:EXCEPT"));
    }

    @Test
    public void testSetOpCast() throws Exception {
        String sql = "select * from t0 union all (select * from t1 union all select k1,k7,k8 from  baseall)";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [1: v1, BIGINT, true] | [4: cast, VARCHAR(20), true] | [5: cast, DOUBLE, true]\n" +
                "  |      [23: v4, BIGINT, true] | [24: cast, VARCHAR(20), true] | [25: cast, DOUBLE, true]");
        Assert.assertTrue(plan.contains(
                "  |  19 <-> [19: k7, VARCHAR, true]\n" +
                        "  |  20 <-> [20: k8, DOUBLE, true]\n" +
                        "  |  22 <-> cast([11: k1, TINYINT, true] as BIGINT)"));

        sql = "select * from t0 union all (select cast(v4 as int), v5,v6 " +
                "from t1 except select cast(v7 as int), v8, v9 from t2)";
        plan = getVerboseExplain(sql);
        assertContains(plan, "0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [1: v1, BIGINT, true] | [2: v2, BIGINT, true] | [3: v3, BIGINT, true]\n" +
                "  |      [15: cast, BIGINT, true] | [13: v5, BIGINT, true] | [14: v6, BIGINT, true]\n" +
                "  |  pass-through-operands: all\n" +
                "  |  cardinality: 2\n" +
                "  |  \n" +
                "  |----11:EXCHANGE\n" +
                "  |       cardinality: 1\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     cardinality: 1\n" +
                "\n" +
                "PLAN FRAGMENT 1(F02)\n" +
                "\n" +
                "  Input Partition: RANDOM\n" +
                "  OutPut Partition: RANDOM\n" +
                "  OutPut Exchange Id: 11\n" +
                "\n" +
                "  10:Project\n" +
                "  |  output columns:\n" +
                "  |  13 <-> [13: v5, BIGINT, true]\n" +
                "  |  14 <-> [14: v6, BIGINT, true]\n" +
                "  |  15 <-> cast([12: cast, INT, true] as BIGINT)\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  3:EXCEPT\n" +
                "  |  child exprs:\n" +
                "  |      [7: cast, INT, true] | [5: v5, BIGINT, true] | [6: v6, BIGINT, true]\n" +
                "  |      [11: cast, INT, true] | [9: v8, BIGINT, true] | [10: v9, BIGINT, true]");
    }

    @Test
    public void testUnionNullConstant() throws Exception {
        String sql = "select count(*) from (select null as c1 union all select null as c1) t group by t.c1";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  child exprs:\n" +
<<<<<<< HEAD
                "  |      [2, BOOLEAN, true]\n" +
                "  |      [4, BOOLEAN, true]"));
=======
                "  |      [2: expr, BOOLEAN, true]\n" +
                "  |      [4: expr, BOOLEAN, true]"));
>>>>>>> 2.5.18

        sql = "select count(*) from (select 1 as c1 union all select null as c1) t group by t.c1";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [2: expr, TINYINT, false]\n" +
                "  |      [5: cast, TINYINT, true]"));

        sql = "select count(*) from (select cast('1.2' as decimal(10,2)) as c1 union all " +
                "select cast('1.2' as decimal(10,0)) as c1) t group by t.c1";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [3: cast, DECIMAL64(12,2), true]\n" +
                "  |      [6: cast, DECIMAL64(12,2), true]\n"));

        sql = "select count(*) from (select cast('1.2' as decimal(5,2)) as c1 union all " +
                "select cast('1.2' as decimal(10,0)) as c1) t group by t.c1";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [3: cast, DECIMAL64(12,2), true]\n" +
                "  |      [6: cast, DECIMAL64(12,2), true]"));
    }

    @Test
    public void testUnionDefaultLimit() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(2);
        String sql = "select * from t0 union all select * from t0;";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        assertContains(plan, "RESULT SINK\n" +
                "\n" +
                "  5:EXCHANGE\n" +
                "     limit: 2");
        assertContains(plan, "  0:UNION\n" +
                "  |  limit: 2\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       limit: 2\n" +
                "  |    \n" +
                "  2:EXCHANGE");
    }

    @Test
    public void testValuesDefaultLimit() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(1);
        String sql = "select * from (values (1,2,3), (4,5,6)) x";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         1 | 2 | 3\n" +
                "         4 | 5 | 6\n" +
                "     limit: 1"));
    }

    @Test
    public void testExceptEmptyNode() throws Exception {
        String sql;
        String plan;
        sql = "select * from (select * from t0 except select * from t1 except select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:EXCEPT\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));

        sql = "select * from (select * from (select * from t0 limit 0) t except " +
                "select * from t1 except select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: v1 | 11: v2 | 12: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET\n"));

        sql = "select * from ( select * from t2 except (select * from t0 limit 0) except " +
                "select * from t1) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:EXCEPT\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));
    }

    @Test
    public void testUnionEmptyNode() throws Exception {
        String sql;
        String plan;
        sql = "select * from (select * from t0 union all select * from t1 union all select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));

        sql = "select * from (select * from (select * from t0 limit 0) t union all " +
                "select * from t1 union all select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));

        sql = "select * from (select * from (select * from t0 limit 0) t union all select * from t1 where false" +
                " union all select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: v1 | 11: v2 | 12: v3\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 10> : 7: v7\n" +
                "  |  <slot 11> : 8: v8\n" +
                "  |  <slot 12> : 9: v9\n" +
                "  |  \n" +
                "  0:OlapScanNode"));
    }

    @Test
    public void testIntersectEmptyNode() throws Exception {
        String sql;
        String plan;
        sql = "select * from (select * from t0 intersect select * from t1 intersect select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:INTERSECT\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE"));

        sql = "select * from (select * from (select * from t0 limit 0) t intersect " +
                "select * from t1 intersect select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: v1 | 11: v2 | 12: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET\n"));

        sql = "select * from (select * from (select * from t0 limit 0) t intersect select * from t1 where false " +
                "intersect select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: v1 | 11: v2 | 12: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET\n"));
    }

    @Test
    public void testUnionAll() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t1 union all select * from t2;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 04\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t1"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testUnionChildProjectHasNullable() throws Exception {
        String sql = "SELECT \n" +
                "  DISTINCT * \n" +
                "FROM \n" +
                "  (\n" +
                "    SELECT \n" +
                "      DISTINCT DAY(\"292269055-12-03 00:47:04\") \n" +
                "    FROM \n" +
                "      t1\n" +
                "    WHERE \n" +
                "      true \n" +
                "    UNION ALL \n" +
                "    SELECT \n" +
                "      DISTINCT DAY(\"292269055-12-03 00:47:04\") \n" +
                "    FROM \n" +
                "      t1\n" +
                "    WHERE \n" +
                "      (\n" +
                "        (true) IS NULL\n" +
                "      )\n" +
                "  ) t;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "8:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: [9: day, TINYINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [4: day, TINYINT, true]\n" +
                "  |      [8: day, TINYINT, true]\n" +
                "  |  pass-through-operands: all");
    }

    @Test
    public void testUnionWithOrderBy() throws Exception {
        String sql =
                "select * from t0 union all select * from t0 union all select * from t0 where v1 > 1 order by v3 limit 2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  7:TOP-N\n" +
                "  |  order by: <slot 12> 12: v3 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 2\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE"));

        sql = "select * from (select * from t0 order by v1 limit 1) t union select * from t1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:TOP-N\n" +
                "  |  order by: <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0"));

        sql = "select v1+v2 from t0 union all select v4 from t1 order by 1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  6:SORT\n" +
                "  |  order by: <slot 8> 8: expr ASC"));
        Assert.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 4> : 1: v1 + 2: v2"));
    }

    @Test
    public void testUserVariable() throws Exception {
        String sql = "set @var = (select v1,v2 from test.t0)";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        SetExecutor setExecutor = new SetExecutor(connectContext, (SetStmt) statementBase);
        Assert.assertThrows("Scalar subquery should output one column", SemanticException.class,
                () -> setExecutor.execute());
        try {
            setExecutor.execute();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Scalar subquery should output one column", e.getMessage());
        }
    }

    @Test
    public void testMinus() throws Exception {
        String sql = "select * from t0 minus select * from t1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:EXCEPT\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE");
    }
<<<<<<< HEAD
=======

    @Test
    public void testUnionNull() throws Exception {
        String sql = "SELECT DISTINCT NULL\n" +
                "WHERE NULL\n" +
                "UNION ALL\n" +
                "SELECT DISTINCT NULL\n" +
                "WHERE NULL";
        getThriftPlan(sql);
    }
>>>>>>> 2.5.18
}
