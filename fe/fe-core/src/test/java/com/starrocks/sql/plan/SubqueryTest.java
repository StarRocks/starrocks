// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SubqueryTest extends PlanTestBase {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testCorrelatedSubqueryWithEqualsExpressions() throws Exception {
        String sql = "select t0.v1 from t0 where (t0.v2 in (select t1.v4 from t1 where t0.v3 + t1.v5 = 1)) is NULL";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("17:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 3: v3 + 14: v5 = 13: expr, CASE WHEN (15: countRows IS NULL) OR (15: countRows = 0) THEN FALSE WHEN 2: v2 IS NULL THEN NULL WHEN 9: v4 IS NOT NULL THEN TRUE WHEN 16: countNotNulls < 15: countRows THEN NULL ELSE FALSE END IS NULL"));
        Assert.assertTrue(plan.contains("10:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 9: v4 = 2: v2\n" +
                "  |  other join predicates: 3: v3 + 11: v5 = 10: expr"));
    }

    @Test
    public void testCountConstantWithSubquery() throws Exception {
        String sql = "SELECT 1 FROM (SELECT COUNT(1) FROM t0 WHERE false) t;";
        String thriftPlan = getThriftPlan(sql);
        Assert.assertTrue(thriftPlan.contains("function_name:count"));
    }

    @Test
    public void testSubqueryGatherJoin() throws Exception {
        String sql = "select t1.v5 from (select * from t0 limit 1) as x inner join t1 on x.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:\n"
                + "  PARTITION: RANDOM\n"
                + "\n"
                + "  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 02\n"
                + "    UNPARTITIONED\n"
                + "\n"
                + "  1:OlapScanNode\n"
                + "     TABLE: t0"));
    }

    @Test
    public void testSubqueryBroadJoin() throws Exception {
        String sql = "select t1.v5 from t0 inner join[broadcast] t1 on cast(t0.v1 as int) = cast(t1.v4 as int)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  equal join conjunct: 7: cast = 8: cast\n"));
        Assert.assertTrue(plan.contains("<slot 7> : CAST(1: v1 AS INT)"));
        Assert.assertTrue(plan.contains("<slot 8> : CAST(4: v4 AS INT)"));
    }

    @Test
    public void testMultiScalarSubquery() throws Exception {
        String sql = "SELECT CASE \n"
                + "    WHEN (SELECT count(*) FROM t1 WHERE v4 BETWEEN 1 AND 20) > 74219\n"
                + "    THEN ( \n"
                + "        SELECT avg(v7) FROM t2 WHERE v7 BETWEEN 1 AND 20\n"
                + "        )\n"
                + "    ELSE (\n"
                + "        SELECT avg(v8) FROM t2 WHERE v8 BETWEEN 1 AND 20\n"
                + "        ) END AS bucket1\n"
                + "FROM t0\n"
                + "WHERE v1 = 1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  15:Project\n" +
                "  |  <slot 19> : if(8: expr > 74219, 13: expr, 17: avg)\n" +
                "  |  \n" +
                "  14:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----13:EXCHANGE\n" +
                "  |    \n" +
                "  10:Project");
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 13\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  12:AGGREGATE (update finalize)\n" +
                "  |  output: avg(15: v8)");
    }

    @Test
    public void testSubqueryLimit() throws Exception {
        String sql = "select * from t0 where 2 = (select v4 from t1 limit 1);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:SELECT\n" +
                "  |  predicates: 4: v4 = 2\n" +
                "  |  \n" +
                "  3:ASSERT NUMBER OF ROWS\n" +
                "  |  assert number of rows: LE 1");
    }

    @Test
    public void testUnionSubqueryDefaultLimit() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(2);
        String sql = "select * from (select * from t0 union all select * from t0) xx limit 10;";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        assertContains(plan, "RESULT SINK\n" +
                "\n" +
                "  5:EXCHANGE\n" +
                "     limit: 10");
        assertContains(plan, "  0:UNION\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       limit: 10\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     limit: 10\n");
    }

    @Test
    public void testExistsRewrite() throws Exception {
        String sql =
                "select count(*) FROM  test.join1 WHERE  EXISTS (select max(id) from test.join2 where join2.id = join1.id)";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("LEFT SEMI JOIN"));
    }

    @Test
    public void testMultiNotExistPredicatePushDown() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.setDatabase("test");

        String sql =
                "select * from join1 where join1.dt > 1 and NOT EXISTS (select * from join1 as a where join1.dt = 1 and a.id = join1.id)" +
                        "and NOT EXISTS (select * from join1 as a where join1.dt = 2 and a.id = join1.id);";
        String explainString = getFragmentPlan(sql);

        Assert.assertTrue(explainString.contains("  5:HASH JOIN\n" +
                "  |  join op: RIGHT ANTI JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 9: id = 2: id\n" +
                "  |  other join predicates: 1: dt = 2"));
        Assert.assertTrue(explainString.contains("  |    3:HASH JOIN\n" +
                "  |    |  join op: LEFT ANTI JOIN (COLOCATE)\n" +
                "  |    |  colocate: true\n" +
                "  |    |  equal join conjunct: 2: id = 5: id\n" +
                "  |    |  other join predicates: 1: dt = 1"));
        Assert.assertTrue(explainString.contains("  |    1:OlapScanNode\n" +
                "  |       TABLE: join1\n" +
                "  |       PREAGGREGATION: ON\n" +
                "  |       PREDICATES: 1: dt > 1"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testAssertWithJoin() throws Exception {
        String sql =
                "SELECT max(1) FROM t0 WHERE 1 = (SELECT t1.v4 FROM t0, t1 WHERE t1.v4 IN (SELECT t1.v4 FROM  t1))";
        String explainString = getFragmentPlan(sql);
        assertContains(explainString, ("9:Project\n" +
                "  |  <slot 7> : 7: v4\n" +
                "  |  \n" +
                "  8:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)"));
    }

    @Test
    public void testCorrelatedSubQuery() throws Exception {
        String sql =
                "select count(*) from t2 where (select v4 from t1 where (select v1 from t0 where t2.v7 = 1) = 1)  = 1";
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("Column '`test`.`t2`.`v7`' cannot be resolved");
        getFragmentPlan(sql);
    }

    @Test
    public void testConstScalarSubQuery() throws Exception {
        String sql = "select * from t0 where 2 = (select v4 from t1)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:SELECT\n" +
                "  |  predicates: 4: v4 = 2\n" +
                "  |  \n" +
                "  3:ASSERT NUMBER OF ROWS");
        assertContains(plan, "1:OlapScanNode\n" +
                "     TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1");
    }

    @Test
    public void testCorrelatedComplexInSubQuery() throws Exception {
        String sql = "SELECT v4  FROM t1\n" +
                "WHERE ( (\"1969-12-09 14:18:03\") IN (\n" +
                "          SELECT t2.v8 FROM t2 WHERE (t1.v5) = (t2.v9))\n" +
                "    ) IS NULL\n";
        Assert.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
    }

    @Test
    public void testInSubQueryWithAggAndPredicate() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "SELECT DISTINCT 1\n" +
                    "FROM test_all_type\n" +
                    "WHERE (t1a IN \n" +
                    "   (\n" +
                    "      SELECT v1\n" +
                    "      FROM t0\n" +
                    "   )\n" +
                    ")IS NULL";

            String plan = getFragmentPlan(sql);
            assertContains(plan, "  18:Project\n" +
                    "  |  <slot 15> : 1\n" +
                    "  |  \n" +
                    "  17:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN");
        }
        {
            String sql = "SELECT DISTINCT 1\n" +
                    "FROM test_all_type\n" +
                    "WHERE t1a IN \n" +
                    "   (\n" +
                    "      SELECT v1\n" +
                    "      FROM t0\n" +
                    "   )\n" +
                    "IS NULL";

            String plan = getFragmentPlan(sql);
            assertContains(plan, "  18:Project\n" +
                    "  |  <slot 15> : 1\n" +
                    "  |  \n" +
                    "  17:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN");
        }
        {
            String sql = "SELECT DISTINCT(t1d)\n" +
                    "FROM test_all_type\n" +
                    "WHERE (t1a IN \n" +
                    "   (\n" +
                    "      SELECT v1\n" +
                    "      FROM t0\n" +
                    "   )\n" +
                    ")IS NULL";

            String plan = getFragmentPlan(sql);
            assertContains(plan, "  18:Project\n" +
                    "  |  <slot 4> : 4: t1d\n" +
                    "  |  \n" +
                    "  17:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testCTEAnchorProperty() throws Exception {
        String sql = "explain SELECT\n" +
                "max (t0_2.v1 IN (SELECT t0_2.v1 FROM  t0 AS t0_2 where abs(2) < 1) )\n" +
                "FROM\n" +
                "  t0 AS t0_2\n" +
                "GROUP BY\n" +
                "  ( CAST(t0_2.v1 AS INT) - NULL ) IN (SELECT subt0.v1  FROM  t1 AS t1_3 RIGHT ANTI JOIN t0 subt0 ON t1_3.v5 = subt0.v1 ),\n" +
                "  t0_2.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "30:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 16: v1\n" +
                "  |  \n" +
                "  |----29:EXCHANGE");
    }

    @Test
    public void testHavingSubquery() throws Exception {
        String sql = "SELECT \n" +
                "  LPAD('x', 1878790738, '') \n" +
                "FROM \n" +
                "  t1 AS t1_104, \n" +
                "  t2 AS t2_105\n" +
                "GROUP BY \n" +
                "  t2_105.v7, \n" +
                "  t2_105.v8 \n" +
                "HAVING \n" +
                "  (\n" +
                "    (\n" +
                "      MIN(\n" +
                "        (t2_105.v9) IN (\n" +
                "          (\n" +
                "            SELECT \n" +
                "              t1_104.v4 \n" +
                "            FROM \n" +
                "              t1 AS t1_104 \n" +
                "            WHERE \n" +
                "              (t2_105.v8) IN ('')\n" +
                "          )\n" +
                "        )\n" +
                "      )\n" +
                "    ) IS NULL\n" +
                "  );";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "13:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 6: v9 = 14: v4\n" +
                "  |  equal join conjunct: 21: cast = 15: cast");
    }

    @Test
    public void testComplexInAndExistsPredicate() throws Exception {
        String sql = "select * from t0 where t0.v1 in (select v4 from t1) or (1=0 and t0.v1 in (select v7 from t2));";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  16:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: CASE WHEN (18: countRows IS NULL) OR (18: countRows = 0) THEN FALSE WHEN 1: v1 IS NULL THEN NULL WHEN 16: v4 IS NOT NULL THEN TRUE WHEN 19: countNotNulls < 18: countRows THEN NULL ELSE FALSE END");

        sql = "select * from t0 where exists (select v4 from t1) or (1=0 and exists (select v7 from t2));";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:AGGREGATE (update finalize)\n" +
                "  |  output: count(1)\n" +
                "  |  group by: \n" +
                "  |  having: 13: COUNT(1) > 0");
    }

    @Test
    public void testSubqueryReorder() throws Exception {
        String sql = "select * from t0 join t1 on t0.v3 = t1.v6 where t1.v5 > (select t2.v7 from t2);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  6:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 5: v5 > 7: v7\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t1");
    }

    @Test
    public void testMultiSubqueryReorder() throws Exception {
        String sql = "select * from t0 join t1 on t0.v3 = t1.v6 " +
                "where t1.v5 > (select t2.v7 from t2) and t1.v4 < (select t3.v10 from t3);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  15:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: v3 = 6: v6\n" +
                "  |  \n" +
                "  |----14:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "  12:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 4: v4 < 11: v10\n" +
                "  |  \n" +
                "  |----11:EXCHANGE\n" +
                "  |    \n" +
                "  7:Project\n" +
                "  |  <slot 4> : 4: v4\n" +
                "  |  <slot 5> : 5: v5\n" +
                "  |  <slot 6> : 6: v6\n" +
                "  |  \n" +
                "  6:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 5: v5 > 7: v7\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t1");
    }

    @Test
    public void testCorrelatedScalarNonAggSubqueryByWhereClause() throws Exception {
        {
            String sql = "SELECT * FROM t0\n" +
                    "WHERE t0.v2 > (\n" +
                    "      SELECT t1.v5 FROM t1\n" +
                    "      WHERE t0.v1 = t1.v4\n" +
                    ");";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 3> : 3: v3\n" +
                    "  |  \n" +
                    "  6:SELECT\n" +
                    "  |  predicates: 2: v2 > 7: expr\n" +
                    "  |  \n" +
                    "  5:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 3> : 3: v3\n" +
                    "  |  <slot 7> : 9: anyValue\n" +
                    "  |  <slot 10> : assert_true((8: countRows IS NULL) OR (8: countRows <= 1))\n" +
                    "  |  \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                    "  |  output: count(1), any_value(5: v5)\n" +
                    "  |  group by: 4: v4");
        }
    }

    @Test
    public void testCorrelatedScalarNonAggSubqueryBySelectClause() throws Exception {
        {
            String sql = "SELECT t0.*, (\n" +
                    "      SELECT t1.v5 FROM t1\n" +
                    "      WHERE t0.v1 = t1.v4\n" +
                    ") as t1_v5 FROM t0;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  5:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 3> : 3: v3\n" +
                    "  |  <slot 7> : 9: anyValue\n" +
                    "  |  <slot 10> : assert_true((8: countRows IS NULL) OR (8: countRows <= 1))\n" +
                    "  |  \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                    "  |  output: count(1), any_value(5: v5)\n" +
                    "  |  group by: 4: v4");
        }
    }

    @Test
    public void testCorrelatedScalarNonAggSubqueryByHavingClause() throws Exception {
        {
            String sql = "SELECT v1, SUM(v2) FROM t0\n" +
                    "GROUP BY v1\n" +
                    "HAVING SUM(v2) > (\n" +
                    "      SELECT t1.v5 FROM t1\n" +
                    "      WHERE t0.v1 = t1.v4\n" +
                    ");";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  9:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : 4: sum\n" +
                    "  |  \n" +
                    "  8:SELECT\n" +
                    "  |  predicates: 4: sum > 8: expr\n" +
                    "  |  \n" +
                    "  7:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : 4: sum\n" +
                    "  |  <slot 8> : 10: anyValue\n" +
                    "  |  <slot 11> : assert_true((9: countRows IS NULL) OR (9: countRows <= 1))\n" +
                    "  |  \n" +
                    "  6:HASH JOIN\n" +
                    "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v4 = 1: v1");
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: count(1), any_value(6: v5)\n" +
                    "  |  group by: 5: v4");
        }
    }

    @Test
    public void testCorrelatedScalarNonAggSubqueryWithExpression() throws Exception {
        String sql = "SELECT \n" +
                "  subt0.v1 \n" +
                "FROM \n" +
                "  (\n" +
                "    SELECT \n" +
                "      t0.v1\n" +
                "    FROM \n" +
                "      t0 \n" +
                "    WHERE \n" +
                "      (\n" +
                "          SELECT \n" +
                "            t2.v7 \n" +
                "          FROM \n" +
                "            t2 \n" +
                "          WHERE \n" +
                "            t0.v2 = 284082749\n" +
                "      ) >= 1\n" +
                "  ) subt0;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 2:AGGREGATE (update finalize)\n" +
                "  |  output: count(1), any_value(4: v7)\n" +
                "  |  group by: 8: expr\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 4> : 4: v7\n" +
                "  |  <slot 8> : 284082749");
        sql = "SELECT \n" +
                "  subt0.v1 \n" +
                "FROM \n" +
                "  (\n" +
                "    SELECT \n" +
                "      t0.v1\n" +
                "    FROM \n" +
                "      t0 \n" +
                "    WHERE \n" +
                "      (\n" +
                "          SELECT \n" +
                "            t2.v7 \n" +
                "          FROM \n" +
                "            t2 \n" +
                "          WHERE \n" +
                "            t0.v2 = t2.v8 + 1\n" +
                "      ) >= 1\n" +
                "  ) subt0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 3:AGGREGATE (update finalize)\n" +
                "  |  output: count(1), any_value(4: v7)\n" +
                "  |  group by: 8: add\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 4> : 4: v7\n" +
                "  |  <slot 8> : 5: v8 + 1");
    }

    @Test
    public void testTransformNAAJ() throws Exception {
        String sql = "select * from t0_not_null where v1 not in (select v2 from t0_not_null)";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "NULL AWARE LEFT ANTI JOIN");
        assertContains(plan, "LEFT ANTI JOIN");

    }
}
