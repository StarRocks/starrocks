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
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.fail;

public class SubqueryTest extends PlanTestBase {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testCorrelatedSubqueryWithEqualsExpressions() throws Exception {
        String sql = "select t0.v1 from t0 where (t0.v2 in (select t1.v4 from t1 where t0.v3 + t1.v5 = 1)) is NULL";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("15:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 3: v3 + 11: v5 = 1, CASE WHEN (12: countRows IS NULL) " +
                "OR (12: countRows = 0) THEN FALSE WHEN 2: v2 IS NULL THEN NULL WHEN 8: v4 IS NOT NULL " +
                "THEN TRUE WHEN 13: countNotNulls < 12: countRows THEN NULL ELSE FALSE END IS NULL"));
        assertContains(plan, "8:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v4\n" +
                "  |  other join predicates: 3: v3 + 9: v5 = 1");
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
        assertContains(plan, " OUTPUT EXPRS:\n"
                + "  PARTITION: RANDOM\n"
                + "\n"
                + "  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 02\n"
                + "    UNPARTITIONED\n"
                + "\n"
                + "  1:OlapScanNode\n"
                + "     TABLE: t0");
    }

    @Test
    public void testSubqueryBroadJoin() throws Exception {
        String sql = "select t1.v5 from t0 inner join[broadcast] t1 on cast(t0.v1 as int) = cast(t1.v4 as int)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  equal join conjunct: 7: cast = 8: cast\n");
        assertContains(plan, "<slot 7> : CAST(1: v1 AS INT)");
        assertContains(plan, "<slot 8> : CAST(4: v4 AS INT)");
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
        assertContains(plan, "  16:Project\n" +
                "  |  <slot 13> : 13: count\n" +
                "  |  <slot 18> : 17: avg\n" +
                "  |  \n" +
                "  15:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----14:EXCHANGE\n" +
                "  |    \n" +
                "  9:Project");
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 19\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  18:AGGREGATE (update finalize)\n" +
                "  |  output: avg(5: v8)");
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
        String plan = getFragmentPlan(sql);
        assertContains(plan, "LEFT SEMI JOIN");
    }

    @Test
    public void testMultiNotExistPredicatePushDown() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.setDatabase("test");

        String sql =
                "select * from join1 where join1.dt > 1 and NOT EXISTS " +
                        "(select * from join1 as a where join1.dt = 1 and a.id = join1.id)" +
                        "and NOT EXISTS (select * from join1 as a where join1.dt = 2 and a.id = join1.id);";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "5:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 2: id = 9: id\n" +
                "  |  other join predicates: 1: dt = 2");
        assertContains(plan, "2:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 2: id = 5: id\n" +
                "  |  other join predicates: 1: dt = 1");
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: dt > 1");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testAssertWithJoin() throws Exception {
        String sql =
                "SELECT max(1) FROM t0 WHERE 1 = (SELECT t1.v4 FROM t0, t1 WHERE t1.v4 IN (SELECT t1.v4 FROM  t1))";
        String plan = getFragmentPlan(sql);
        assertContains(plan, ("11:Project\n" +
                "  |  <slot 7> : 7: v4\n" +
                "  |  \n" +
                "  10:HASH JOIN\n" +
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
            assertContains(plan, "18:Project\n" +
                    "  |  <slot 15> : 1\n" +
                    "  |  limit: 1\n" +
                    "  |  \n" +
                    "  17:NESTLOOP JOIN\n" +
                    "  |  join op: INNER JOIN\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  other join predicates: CASE WHEN");
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
            assertContains(plan, "18:Project\n" +
                    "  |  <slot 15> : 1\n" +
                    "  |  limit: 1\n" +
                    "  |  \n" +
                    "  17:NESTLOOP JOIN\n" +
                    "  |  join op: INNER JOIN\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  other join predicates: CASE WHEN ");
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
                    "  |  join op: INNER JOIN\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  other join predicates: CASE WHEN ");
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
                "  ( CAST(t0_2.v1 AS INT) - NULL ) IN (SELECT subt0.v1  FROM  t1 " +
                "AS t1_3 RIGHT ANTI JOIN t0 subt0 ON t1_3.v5 = subt0.v1 ),\n" +
                "  t0_2.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  30:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 16: v1\n" +
                "  |  \n" +
                "  |----29:AGGREGATE (merge finalize)");
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
        assertContains(plan, "20:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 6: v9 = 13: v4\n" +
                "  |  other join predicates: CAST(5: v8 AS DOUBLE) = CAST('' AS DOUBLE)\n" +
                "  |  \n" +
                "  |----19:EXCHANGE");
        assertContains(plan, "13:NESTLOOP JOIN\n" +
                "  |  join op: LEFT OUTER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: CAST(5: v8 AS DOUBLE) = CAST('' AS DOUBLE)");
    }

    @Test
    public void testComplexInAndExistsPredicate() throws Exception {
        String sql = "select * from t0 where t0.v1 in (select v4 from t1) " +
                "or (t0.v2=0 and t0.v1 in (select v7 from t2));";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  24:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 12: v7");
        assertContains(plan, "  9:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 16: v4");

        sql = "select * from t0 where exists (select v4 from t1) " +
                "or (t0.v2=0 and exists (select v7 from t2));";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  13:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: (7: expr) OR ((2: v2 = 0) AND (12: COUNT(1) > 0))");
    }

    @Test
    public void testSubqueryReorder() throws Exception {
        String sql = "select * from t0 join t1 on t0.v3 = t1.v6 where t1.v5 > (select t2.v7 from t2);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  6:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
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
                "  |  join op: INNER JOIN\n" +
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
                "  |  join op: INNER JOIN\n" +
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
                    "  |  predicates: 2: v2 > 7: v5\n" +
                    "  |  \n" +
                    "  5:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 3> : 3: v3\n" +
                    "  |  <slot 7> : 9: anyValue\n" +
                    "  |  <slot 10> : assert_true((8: countRows IS NULL) OR (8: countRows <= 1), " +
                    "'correlate scalar subquery result must 1 row')\n" +
                    "  |  \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                    "  |  output: count(1), any_value(5: v5)\n" +
                    "  |  group by: 4: v4");
        }
        {
            String sql = "SELECT * FROM t0\n" +
                    "WHERE t0.v2 > (\n" +
                    "      SELECT t1.v5 FROM t1\n" +
                    "      WHERE t0.v1 = t1.v4 and t1.v4 = 10 and t1.v5 < 2\n" +
                    ");";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 3> : 3: v3\n" +
                    "  |  \n" +
                    "  6:SELECT\n" +
                    "  |  predicates: 2: v2 > 7: v5\n" +
                    "  |  \n" +
                    "  5:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 3> : 3: v3\n" +
                    "  |  <slot 7> : 9: anyValue\n" +
                    "  |  <slot 10> : assert_true((8: countRows IS NULL) OR (8: countRows <= 1), " +
                    "'correlate scalar subquery result must 1 row')\n" +
                    "  |  \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                    "  |  output: count(1), any_value(5: v5)\n" +
                    "  |  group by: 4: v4\n" +
                    "  |  \n" +
                    "  1:OlapScanNode\n" +
                    "     TABLE: t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 4: v4 = 10, 5: v5 < 2\n" +
                    "     partitions=0/1\n" +
                    "     rollup: t1\n" +
                    "     tabletRatio=0/0\n" +
                    "     tabletList=\n" +
                    "     cardinality=1\n" +
                    "     avgRowSize=2.0\n" +
                    "     numNodes=0");
        }
        {
            connectContext.getSessionVariable().setNewPlanerAggStage(2);
            String sql = "select l.id_decimal from test_all_type l \n" +
                    "where l.id_decimal > (\n" +
                    "    select r.id_decimal from test_all_type_not_null r\n" +
                    "    where l.t1a = r.t1a\n" +
                    ");";
            String plan = getVerboseExplain(sql);
            assertContains(plan,
                    "args: DECIMAL64; result: DECIMAL64(10,2); args nullable: false; result nullable: true");
            assertContains(plan, "  7:Project\n" +
                    "  |  output columns:\n" +
                    "  |  10 <-> [10: id_decimal, DECIMAL64(10,2), true]\n" +
                    "  |  21 <-> [23: anyValue, DECIMAL64(10,2), true]");
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
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
            assertContains(plan, "5:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 3> : 3: v3\n" +
                    "  |  <slot 7> : 9: anyValue\n" +
                    "  |  <slot 10> : assert_true((8: countRows IS NULL) OR (8: countRows <= 1), " +
                    "'correlate scalar subquery result must 1 row')\n" +
                    "  |  \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "2:AGGREGATE (update finalize)\n" +
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
            assertContains(plan, "9:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : 4: sum\n" +
                    "  |  \n" +
                    "  8:SELECT\n" +
                    "  |  predicates: 4: sum > 8: v5\n" +
                    "  |  \n" +
                    "  7:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : 4: sum\n" +
                    "  |  <slot 8> : 10: anyValue\n" +
                    "  |  <slot 11> : assert_true((9: countRows IS NULL) OR (9: countRows <= 1), " +
                    "'correlate scalar subquery result must 1 row')\n" +
                    "  |  \n" +
                    "  6:HASH JOIN\n" +
                    "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v4 = 1: v1");
            assertContains(plan, "1:AGGREGATE (update finalize)\n" +
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
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  output: count(1), any_value(4: v7)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2");
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
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  output: count(1), any_value(4: v7)\n" +
                "  |  group by: 8: add\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 4> : 4: v7\n" +
                "  |  <slot 8> : 5: v8 + 1");
    }

    @Test
    public void testCorrelationScalarSubqueryWithNonEQPredicate() throws Exception {
        String sql = "SELECT v1, SUM(v2) FROM t0\n" +
                "GROUP BY v1\n" +
                "HAVING SUM(v2) > (\n" +
                "      SELECT t1.v5 FROM t1\n" +
                "      WHERE nullif(false, t0.v1 < 0)\n" +
                ");";
        Assert.assertThrows("Not support Non-EQ correlation predicate correlation scalar-subquery",
                SemanticException.class, () -> getFragmentPlan(sql));
    }

    @Test
    public void testOnClauseCorrelatedScalarAggSubquery() throws Exception {
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select count(*) from t1 where t0.v2 = t1.v5)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  8:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason:");
            assertContains(plan, "  5:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v5\n" +
                    "  |  other predicates: 1: v1 = ifnull(10: count, 0)");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on (select count(*) from t0 where t0.v2 = t1.v5) = t1.v5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  8:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "  5:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v2\n" +
                    "  |  other predicates: ifnull(10: count, 0) = 5: v5");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select count(*) from t1 where t0.v2 = t1.v5) + t1.v6";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  8:NESTLOOP JOIN\n" +
                    "  |  join op: INNER JOIN\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  other join predicates: 1: v1 = 11: ifnull + 6: v6");
            assertContains(plan, "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v5");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v3 = (select count(*) from t1 where t0.v2 = t1.v5)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 1: v1");
            assertContains(plan, "  5:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v5\n" +
                    "  |  other predicates: 3: v3 = ifnull(10: count, 0)");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on (select count(*) from t0 where t0.v2 = t1.v5) + t0.v3 = t1.v5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  8:NESTLOOP JOIN\n" +
                    "  |  join op: INNER JOIN\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  other join predicates: 11: ifnull + 3: v3 = 5: v5");
            assertContains(plan, "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v2");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select count(*) from t1 where t0.v2 = t1.v5 and t0.v3 = t1.v6)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  8:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v5\n" +
                    "  |  equal join conjunct: 3: v3 = 9: v6\n" +
                    "  |  other predicates: 1: v1 = ifnull(10: count, 0)");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select count(*) from t1 where t0.v2 = t1.v5) " +
                    "and t1.v6 = (select max(v7) from t2 where t2.v8 = t0.v1)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  13:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 16: max = 6: v6");
            assertContains(plan, "  9:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 13: v8");
            assertContains(plan, "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v5\n" +
                    "  |  other predicates: 1: v1 = ifnull(10: count, 0)");
        }
    }

    @Test
    public void testOnClauseCorrelatedScalarNonAggSubquery() throws Exception {
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select v4 from t1 where t0.v2 = t1.v5)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  10:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "  5:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v5");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v3 = (select v4 from t1 where t0.v2 = t1.v5)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  10:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 1: v1");
            assertContains(plan, "  5:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v5");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select v4 from t1 where t0.v2 = t1.v5 and t1.v5 = 10 and t1.v6 != 3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  11:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v5");
            assertContains(plan, "OlapScanNode\n" +
                    "     TABLE: t1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 8: v5 = 10, 9: v6 != 3\n");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on (select v1 from t0,t2 where t0.v2 = t1.v5 and t0.v2 = t2.v7) * 2 = t1.v4";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  16:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "11:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v2");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v3 = (select v4 from t1 where t0.v2 = t1.v5) " +
                    "and t1.v6 = (select v8 from t2 where t2.v9 = t0.v3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  15:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                    "  |  equal join conjunct: 14: v8 = 6: v6");
            assertContains(plan, "  9:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 3: v3 = 13: v9");
            assertContains(plan, "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v5");
        }
    }

    @Test
    public void testOnClauseNonCorrelatedScalarAggSubquery() throws Exception {
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select count(*) from t3 join t4)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  18:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason:");
            assertContains(plan, "  14:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 13: count");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on (select count(*) from t3) = t1.v5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "12:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason:");
            assertContains(plan, "9:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 10: count");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select count(*) from t1)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  12:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 10: count");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select count(*) from t0)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  12:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 10: count");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v3 = (select count(*) from t0)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  12:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 3: v3 = 10: count");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v3 = (select count(*) from t0) " +
                    "and t1.v5 != (select count(*) from t2)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  20:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 3: v3 = 10: count");
            assertContains(plan, "  17:NESTLOOP JOIN\n" +
                    "  |  join op: INNER JOIN\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  other join predicates: 5: v5 != 15: count");
        }
    }

    @Test
    public void testOnClauseNonCorrelatedScalarNonAggSubquery() throws Exception {
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select t2.v8 from t2 where t2.v7 = 10)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  11:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "  7:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 8: v8");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = (select t2.v7 + t3.v10 from t2 join t3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  14:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "  10:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 13: expr");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v3 = (select t2.v8 from t2 where t2.v7 = 10)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  11:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  7:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 3: v3 = 8: v8");
        }
        {
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v3 = (select t2.v8 from t2 where t2.v7 = 10) " +
                    "and t0.v2 = (select t2.v9 from t2 where t2.v8 < 100)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  19:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  15:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 13: v9");
            assertContains(plan, "  7:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 3: v3 = 8: v8");
        }
    }

    @Test
    public void testOnClauseExistentialSubquery() throws Exception {
        {
            // Uncorrelated 1
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v1 = (exists (select v7 from t2 where t2.v8 = 1))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  12:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 12: cast");
        }
        {
            // Uncorrelated 2
            String sql = "select * from t0 " +
                    "join t1 on " +
                    "t0.v1 = (exists (select v7 from t2 where t2.v8 = 1))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  11:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 12: cast");
        }
        {
            // Uncorrelated 3, multi subqueries
            String sql = "select * from t0 " +
                    "join t1 on " +
                    "t0.v1 = (not exists (select v7 from t2 where t2.v8 = 1)) " +
                    "and t1.v4 = (exists(select v10 from t3 where t3.v11 < 5))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  19:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 18: cast");
            assertContains(plan, "HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 17: cast");
        }
        {
            // correlated 1
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v1 = (exists (select v7 from t2 where t2.v8 = t1.v5))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 1: v1\n" +
                    "  |  equal join conjunct: 12: cast = 1: v1");
            assertContains(plan, "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  other predicates: CAST(11: countRows IS NOT NULL AS BIGINT) IS NOT NULL");
        }
        {
            // correlated 2
            String sql = "select * from t0 " +
                    "join t1 on " +
                    "t0.v1 = (exists (select v7 from t2 where t2.v8 = t1.v5))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  8:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 12: cast = 1: v1");
            assertContains(plan, "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  other predicates: CAST(11: countRows IS NOT NULL AS BIGINT) IS NOT NULL");
        }
        {
            // correlated 3, multi subqueries
            String sql = "select * from t0 " +
                    "join t1 on " +
                    "t0.v1 = (exists (select v7 from t2 where t2.v8 = t1.v5)) " +
                    "and t0.v2 = (not exists(select v11 from t3 where t3.v10 = t0.v1))";
            String plan = getFragmentPlan(sql);
            System.out.println(plan);
            assertContains(plan, "HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 17: cast = 1: v1");
            assertContains(plan, "  4:HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  other predicates: CAST(15: countRows IS NOT NULL AS BIGINT) IS NOT NULL");
            assertContains(plan, "HASH JOIN\n" +
                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 11: v10\n" +
                    "  |  other predicates: 2: v2 = CAST(16: countRows IS NULL AS BIGINT)");
        }
    }

    @Test
    public void testOnClauseQuantifiedSubquery() throws Exception {
        {
            // Uncorrelated 1
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v1 in (select v7 from t2 where t2.v8 = 1)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  9:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  5:HASH JOIN\n" +
                    "  |  join op: LEFT SEMI JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 7: v7");
        }
        {
            // Uncorrelated 2
            String sql = "select * from t0 " +
                    "join t1 on " +
                    "t0.v1 in (select v7 from t2)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "  3:HASH JOIN\n" +
                    "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 7: v7");
        }
        {
            // Uncorrelated 3, multi subqueries
            String sql = "select * from t0 " +
                    "join t1 on " +
                    "t0.v1 not in (select v7 from t2) " +
                    "and t0.v2 in (select v8 from t2)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  11:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason:");
            assertContains(plan, "7:HASH JOIN\n" +
                    "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 12: v8");
            assertContains(plan, "3:HASH JOIN\n" +
                    "  |  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 7: v7");
        }
        {
            // correlated 1
            String sql = "select * from t0 " +
                    "join t1 on t0.v1 = t1.v4 " +
                    "and t0.v1 in (select v7 from t2 where t2.v9 = t0.v3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v4");
            assertContains(plan, "  3:HASH JOIN\n" +
                    "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 7: v7\n" +
                    "  |  equal join conjunct: 3: v3 = 9: v9");
        }
        {
            // correlated 2
            String sql = "select * from t0 " +
                    "join t1 on " +
                    "t0.v1 in (select v7 from t2 where t2.v9 = t0.v3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason:");
            assertContains(plan, "  3:HASH JOIN\n" +
                    "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 7: v7\n" +
                    "  |  equal join conjunct: 3: v3 = 9: v9");
        }
        {
            // correlated 3, multi subqueries
            String sql = "select * from t0 " +
                    "join t1 on " +
                    "t0.v1 in (select v7 from t2 where t2.v9 = t0.v3) " +
                    "and t0.v2 not in (select v8 from t2 where t2.v9 = t0.v2)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  11:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: ");
            assertContains(plan, "7:HASH JOIN\n" +
                    "  |  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 12: v8\n" +
                    "  |  other join predicates: 2: v2 = 13: v9");
            assertContains(plan, "3:HASH JOIN\n" +
                    "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 7: v7\n" +
                    "  |  equal join conjunct: 3: v3 = 9: v9");
        }
    }

    @Test
    public void testOnClauseNotSupportedCases() {
        assertExceptionMsgContains("select * from t0 " +
                        "join t1 on (select v1 from t0 where t0.v2 = t1.v5) = (select v4 from t1 where t0.v2 = t1.v5)",
                "contains more than one subquery");

        assertExceptionMsgContains("select * from t0 " +
                        "join t1 on 1 = (select count(*) from t2 where t0.v1 = t2.v7 and t1.v4 = t2.v7)",
                "referencing columns from more than one table");

        assertExceptionMsgContains("select * from t0 " +
                        "join t1 on 1 = (select count(*) from t2 where t0.v1 = t2.v7 and t1.v4 = t2.v7)",
                "referencing columns from more than one table");

        assertExceptionMsgContains("select * from t0 " +
                        "join t1 on t0.v1 in (select t2.v7 from t2 where t1.v5 = t2.v8)",
                "referencing columns from more than one table");
    }

    @Test
    public void testWherePredicateSubqueryElimination() throws Exception {
        {
            String sql = "select * from t0 where 1 < 1 and v2 = (select v4 from t1);";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select * from t0 where 1 = 1 or v2 = (select v4 from t1);";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select * from t0 where v1 = 'a' or (1 = 2 and v2 = (select v4 from t1));";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select * from t0 where v1 = 'a' and (1 < 2 or v2 = (select v4 from t1));";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql =
                    "select * from t0 where v1 = 'a' and (1 < 2 or v2 = (select v4 from t1)) " +
                            "and exists(select v5 from t1 where t0.v3 = t1.v6);";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "v3");
            assertContains(plan, "v6");
            assertNotContains(plan, "v4");
        }
        {
            String sql =
                    "select * from t0 where v1 = 'a' and (1 < 2 and v2 = (select v4 from t1)) " +
                            "and ( 2 != 3  or exists(select v5 from t1 where t0.v3 = t1.v6));";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "v2");
            assertContains(plan, "v4");
            assertNotContains(plan, "v5");
            assertNotContains(plan, "v6");
        }
    }

    @Test
    public void testOnPredicateSubqueryElimination() throws Exception {
        {
            String sql = "select * from t0 join t3 " +
                    "on 1 < 1 and v2 = (select v4 from t1);";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select * from t0 join t3 " +
                    "on 1 = 1 or v2 = (select v4 from t1);";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select * from t0 join t3 " +
                    "on v1 = 'a' or (1 = 2 and v2 = (select v4 from t1));";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select * from t0 join t3 " +
                    "on v1 = 'a' and (1 < 2 or v2 = (select v4 from t1));";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql =
                    "select * from t0 where v1 = 'a' and (1 < 2 or v2 = (select v4 from t1)) " +
                            "and exists(select v5 from t1 where t0.v3 = t1.v6);";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "v3");
            assertContains(plan, "v6");
            assertNotContains(plan, "v4");
        }
        {
            String sql =
                    "select * from t0 where v1 = 'a' and (1 < 2 and v2 = (select v4 from t1)) " +
                            "and ( 2 != 3  or exists(select v5 from t1 where t0.v3 = t1.v6));";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "v2");
            assertContains(plan, "v4");
            assertNotContains(plan, "v5");
            assertNotContains(plan, "v6");
        }
    }

    @Test
    public void testHavingPredicateSubqueryElimination() throws Exception {
        {
            String sql = "select v1 from t0 group by v1 " +
                    "having 1 < 1 and sum(v2) = (select v4 from t1);";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select v1 from t0 group by v1 " +
                    "having 1 = 1 or sum(v2) = (select v4 from t1);";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select v1 from t0 group by v1 " +
                    "having v1 = 'a' or (1 = 2 and max(v2) = (select v4 from t1));";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select v1 from t0 group by v1 " +
                    "having v1 = 'a' and (1 < 2 or avg(v2) = (select v4 from t1));";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select v1 from t0 group by v1 " +
                    "having v1 = 'a' and (1 < 2 or max(v2) = (select v4 from t1)) " +
                    "and max(v3) = (select max(v5) from t1 where t0.v1 = t1.v6);";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "v3");
            assertContains(plan, "v6");
            assertNotContains(plan, "v4");
        }
        {
            String sql = "select v1 from t0 group by v1 " +
                    "having v1 = 'a' and (1 < 2 and max(v2) = (select v4 from t1)) " +
                    "and ( 2 != 3  or exists(select v5 from t1 where t0.v1 = t1.v6));";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "v2");
            assertContains(plan, "v4");
            assertNotContains(plan, "v5");
            assertNotContains(plan, "v6");
        }
    }

    @Test
    public void testPushDownAssertProject() throws Exception {
        String sql = "select (select 1 from t2) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  3:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t2");
    }

    @Test
    public void testSubqueryTypeRewrite() throws Exception {
        {
            String sql =
                    "select nullif((select max(v4) from t1), (select min(v6) from t1))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  14:Project\n" +
                    "  |  <slot 12> : nullif(6: max, 10: min)");
        }
        {
            String sql =
                    "select nullif((select max(v4) from t1), (select avg(v6) from t1))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  14:Project\n" +
                    "  |  <slot 12> : nullif(CAST(6: max AS DOUBLE), 10: avg)");
        }
        {
            String sql =
                    "select ifnull((select max(v4) from t1), (select min(v6) from t1))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  14:Project\n" +
                    "  |  <slot 12> : ifnull(6: max, 10: min)");
        }
        {
            String sql =
                    "select ifnull((select max(v4) from t1), (select avg(v6) from t1))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  14:Project\n" +
                    "  |  <slot 12> : ifnull(CAST(6: max AS DOUBLE), 10: avg)");
        }
        {
            String sql =
                    "select ifnull((select max(v4) from t1), (select min(v6) from t1))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  14:Project\n" +
                    "  |  <slot 12> : ifnull(6: max, 10: min)");
        }
        {
            String sql =
                    "select coalesce((select max(v4) from t1), (select any_value(v5) from t1), (select min(v6) from t1))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  21:Project\n" +
                    "  |  <slot 17> : coalesce(6: max, 11: any_value, 15: min)");
        }
        {
            String sql =
                    "select coalesce((select max(v4) from t1), (select avg(v5) from t1), (select min(v6) from t1))";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  21:Project\n" +
                    "  |  <slot 17> : coalesce(CAST(6: max AS DOUBLE), 11: avg, CAST(15: min AS DOUBLE))");
        }
        {
            String sql =
                    "select case " +
                            "when(select count(*) from t2) > 10 then (select max(v4) from t1) " +
                            "else (select min(v6) from t1) " +
                            "end c";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  22:Project\n" +
                    "  |  <slot 17> : if(11: count > 10, 16: max, 5: min)");
        }
        {
            String sql =
                    "select case " +
                            "when(select count(*) from t2) > 10 then (select max(v4) from t1) " +
                            "else (select avg(v6) from t1) " +
                            "end c";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  22:Project\n" +
                    "  |  <slot 17> : if(11: count > 10, CAST(16: max AS DOUBLE), 5: avg)");
        }
        {
            String sql =
                    "select case " +
                            "when(select count(*) from t2) > 10 then (select max(v4) from t1) " +
                            "when(select count(*) from t3) > 20 then (select any_value(v5) from t1) " +
                            "else (select min(v6) from t1) " +
                            "end c";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  37:Project\n" +
                    "  |  <slot 27> : CASE WHEN 11: count > 10 THEN 16: max WHEN 21: count > 20 " +
                    "THEN 26: any_value ELSE 5: min END");
        }
        {
            String sql =
                    "select case " +
                            "when(select count(*) from t2) > 10 then (select max(v4) from t1) " +
                            "when(select count(*) from t3) > 20 then (select avg(v5) from t1) " +
                            "else (select min(v6) from t1) " +
                            "end c";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  37:Project\n" +
                    "  |  <slot 27> : CASE WHEN 11: count > 10 THEN CAST(16: max AS DOUBLE) " +
                    "WHEN 21: count > 20 THEN 26: avg ELSE CAST(5: min AS DOUBLE) END");
        }
    }

    @Test
    public void testSubqueryTypeCast() throws Exception {
        String sql = "select * from test_all_type where t1a like (select t1a from test_all_type_not_null);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "5:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: t1a LIKE 11: t1a");
    }

    @Test
    public void testSubqueryMissLimit() throws Exception {
        String sql = "select * from t0 limit 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "limit: 1");

        sql = "select * from (select * from t0 limit 1) t";
        plan = getFragmentPlan(sql);
        assertContains(plan, "limit: 1");

        sql = "(select * from t0 limit 1)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "limit: 1");

        sql = "(select * from t0) limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "limit: 1");

        sql = "((select * from t0) limit 2) limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "limit: 1");

        sql = "((select * from t0) limit 1) limit 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "limit: 1");

        sql = "((select * from t0) limit 1) order by v1 limit 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  2:TOP-N\n" +
                "  |  order by: <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 2\n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 1\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     numNodes=0\n" +
                "     limit: 1");

        sql = "((select * from t0) limit 2) order by v1 limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  2:TOP-N\n" +
                "  |  order by: <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 2\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     numNodes=0\n" +
                "     limit: 2");
    }

    @Test
    public void testSubqueryElimination() throws Exception {
        {
            // Two tables
            String sql = "select * from t0, t1 " +
                    "where t0.v1 = t1.v4 " +
                    "and t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select max(v6) from t1 where t0.v1 = t1.v4 and t1.v5 > 10 " +
                    ")";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:SELECT\n" +
                    "  |  predicates: 3: v3 < 12: max\n" +
                    "  |  \n" +
                    "  6:ANALYTIC\n" +
                    "  |  functions: [, max(6: v6), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  \n" +
                    "  5:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Three tables
            String sql = "select * from t0, t1, t2 " +
                    "where t0.v1 = t1.v4 and t1.v4 = t2.v7 " +
                    "and t0.v2 < 5 and t1.v5 > 10 and t2.v8 < 15 " +
                    "and t0.v3 < (" +
                    "    select max(v6) from t1, t2 where t0.v1 = t1.v4 and t1.v4 = t2.v7 and t1.v5 > 10 and t2.v8 < 15" +
                    ")";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  10:SELECT\n" +
                    "  |  predicates: 3: v3 < 18: max\n" +
                    "  |  \n" +
                    "  9:ANALYTIC\n" +
                    "  |  functions: [, max(6: v6), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  \n" +
                    "  8:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Multi correlated conjuncts
            String sql = "select * from t0, t1 " +
                    "where t0.v1 = t1.v4 and t0.v3 = t1.v6 " +
                    "and t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select max(v6) from t1 " +
                    "    where t0.v1 = t1.v4 and t0.v3 = t1.v6 " +
                    "    and t1.v5 > 10 " +
                    ")";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:SELECT\n" +
                    "  |  predicates: 3: v3 < 12: max\n" +
                    "  |  \n" +
                    "  6:ANALYTIC\n" +
                    "  |  functions: [, max(6: v6), ]\n" +
                    "  |  partition by: 1: v1, 3: v3\n" +
                    "  |  \n" +
                    "  5:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC, <slot 3> 3: v3 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Argument of aggregate function is complex expr
            String sql = "select * from t0, t1 " +
                    "where t0.v1 = t1.v4 " +
                    "and t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select max(v5 / v6 % v4) from t1 where t0.v1 = t1.v4 and t1.v5 > 10 " +
                    ")";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:SELECT\n" +
                    "  |  predicates: CAST(3: v3 AS DOUBLE) < 13: max\n" +
                    "  |  \n" +
                    "  6:ANALYTIC\n" +
                    "  |  functions: [, max(CAST(5: v5 AS DOUBLE) / CAST(6: v6 AS DOUBLE) % CAST(4: v4 AS DOUBLE)), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  \n" +
                    "  5:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Aggregate itself is in another expr
            String sql = "select *, t0.v1 * t0.v2 " +
                    "from t0, t1, t2 " +
                    "where t0.v1 = t1.v4 and t1.v4 = t2.v7 " +
                    "and t0.v2 < 5 and t1.v5 > 10 and t2.v8 > 20 " +
                    "and t0.v3 < (" +
                    "    select max(v9 * 3) /19 from t1, t2 " +
                    "    where t0.v1 = t1.v4 and t1.v4 = t2.v7 and t1.v5 > 10 and t2.v8 > 20" +
                    ")";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  10:SELECT\n" +
                    "  |  predicates: CAST(3: v3 AS DOUBLE) < CAST(21: max AS DOUBLE) / 19.0\n" +
                    "  |  \n" +
                    "  9:ANALYTIC\n" +
                    "  |  functions: [, max(9: v9 * 3), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  \n" +
                    "  8:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // Multi-aggregate functions
            String sql = "select * from t0, t1, t2 " +
                    "where t0.v1 = t1.v4 and t1.v4 = t2.v7 " +
                    "and t0.v2 < 5 and t1.v5 > 10 and t2.v8 > 20 " +
                    "and t0.v3 < (" +
                    "    select max(v9)/min(v9%v8) from t1, t2 where t0.v1 = t1.v4 and t1.v4 = t2.v7" +
                    "    and t1.v5 > 10 and t2.v8 > 20" +
                    ")";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  10:SELECT\n" +
                    "  |  predicates: CAST(3: v3 AS DOUBLE) < CAST(21: max AS DOUBLE) / CAST(22: min AS DOUBLE)\n" +
                    "  |  \n" +
                    "  9:ANALYTIC\n" +
                    "  |  functions: [, max(9: v9), ], [, min(9: v9 % 8: v8), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  \n" +
                    "  8:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0");
        }
    }

    @Test
    public void testSubqueryCannotElimination() throws Exception {
        {
            // Non-correlated
            String sql = "select * from t0, t1 " +
                    "where t0.v1 = t1.v4 " +
                    "and t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select max(v6) from t0, t1 where t0.v1 = t1.v4 " +
                    "    and t0.v2 < 5 and t1.v5 > 10 " +
                    ")";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "ANALYTIC");
        }
        {
            // Subquery miss predicates
            String sql = "select * from t0, t1 " +
                    "where t0.v1 = t1.v4 " +
                    "and t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select max(v6) from t1 where t0.v1 = t1.v4" +
                    ")";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "ANALYTIC");
        }
        {
            // Subquery has more predicates
            String sql = "select * from t0, t1 " +
                    "where t0.v1 = t1.v4 " +
                    "and t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select max(v6) from t1 where t0.v1 = t1.v4 " +
                    "    and t1.v5 > 10 and t1.v5 < 15" +
                    ")";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "ANALYTIC");
        }
        {
            // Outer block missing correlated conjunct
            String sql = "select * from t0, t1 " +
                    "where t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select max(v6) from t1 where t0.v1 = t1.v4 and t1.v5 > 10 " +
                    ")";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "ANALYTIC");
        }
        {
            // Join types other than cross join
            String sql = "select * from t0 left outer join t1 on t0.v1 = t1.v4 " +
                    "where t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select max(v6) from t1 where t0.v1 = t1.v4 and t1.v5 > 10 " +
                    ")";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "ANALYTIC");
        }
        {
            // Aggregate with distinct
            String sql = "select * from t0, t1 " +
                    "where t0.v1 = t1.v4 " +
                    "and t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select sum(distinct(v6)) from t1 where t0.v1 = t1.v4 and t1.v5 > 10 " +
                    ")";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "ANALYTIC");
        }
        {
            // Aggregate function without window version
            String sql = "select * from t0, t1 " +
                    "where t0.v1 = t1.v4 " +
                    "and t0.v2 < 5 and t1.v5 > 10 " +
                    "and t0.v3 < (" +
                    "    select any_value(v6) from t1 where t0.v1 = t1.v4 and t1.v5 > 10 " +
                    ")";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "ANALYTIC");
        }
        {
            // Same tables
            String sql = "select * from t0, t0 as t1 " +
                    "where t0.v1 = t1.v1 " +
                    "and t0.v2 < 5 and t1.v2 > 10 " +
                    "and t0.v3 < (" +
                    "    select max(t1.v3) from t0 as t1 " +
                    "    where t0.v1 = t1.v1 and t1.v2 > 10" +
                    ")";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "ANALYTIC");
        }
    }

    @Test
    public void testNestSubquery() throws Exception {
        String sql = "select * from t0 where exists (select * from t1 where t0.v1 in (select v7 from t2));";
        assertExceptionMsgContains(sql, "Getting analyzing error. Detail message: Unsupported complex nested in-subquery.");

        sql = "select * from t0 where exists (select * from t1 where t0.v1 = (select v7 from t2));";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "11:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 10: v7");
    }

    @Test
    public void testCorrelatedPredicateRewrite_1() throws Exception {

        String sql = "select v1 from t0 where v1 = 1 or v2 in (select v4 from t1 where v2 = v4 and v5 = 1)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "15:AGGREGATE (merge finalize)\n" +
                "  |  group by: 8: v4\n" +
                "  |  \n" +
                "  14:EXCHANGE");
        assertContains(plan, "9:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: v4 = 2: v2\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  6:AGGREGATE (merge finalize)\n" +
                "  |  output: count(11: countRows), count(12: countNotNulls)\n" +
                "  |  group by: 10: v4\n" +
                "  |  \n" +
                "  5:EXCHANGE");
    }

    @Test
    public void testCorrelatedPredicateRewrite_2() throws Exception {
        String sql = "select v1 from t0 where v1 in (select v5 from t1 where v1 = v5 and v4 = 3) " +
                "or v2 in (select v4 from t1 where v2 = v4 and v5 = 1)";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "33:AGGREGATE (merge finalize)\n" +
                "  |  group by: 12: v4\n" +
                "  |  \n" +
                "  32:EXCHANGE");
        assertContains(plan, "27:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: v4 = 2: v2\n" +
                "  |  \n" +
                "  |----26:EXCHANGE\n" +
                "  |    \n" +
                "  6:AGGREGATE (merge finalize)\n" +
                "  |  output: count(15: countRows), count(16: countNotNulls)\n" +
                "  |  group by: 14: v4\n" +
                "  |  \n" +
                "  5:EXCHANGE");
    }

    @Test
    public void testCaseWhenSubquery1() throws Exception {
        String sql = "with tmp as (select 8 id, 'season' type1, 'a.season' pretype, 'season' ranktype from dual ) " +
                "select case when id = abs(0) then 'a' " +
                "else concat( case when a.pretype is not null then CONCAT(',', a.ranktype) " +
                "else null end, " +
                "case when exists (select 1 from tmp) then (select type1 from tmp) " +
                "else null end) end from tmp a;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "14:Project\n" +
                "  |  <slot 24> : if(CAST(7: expr AS SMALLINT) = abs(0), " +
                "'a', concat(if(9: expr IS NOT NULL, concat(',', 10: expr), NULL), if(17: expr, 22: expr, NULL)))");
    }

    @Test
    public void testCaseWhenSubquery2() throws Exception {
        String sql = "with tmp as (select 8 id, 'season' type1, 'a.season' pretype, 'season' ranktype from dual ) " +
                "select case when id = abs(0) then 'a' else case when exists (select 1 from tmp) " +
                "then (select type1 from tmp) > (select pretype from tmp) else null end end from tmp a;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "20:Project\n" +
                "  |  <slot 30> : if(CAST(7: expr AS SMALLINT) = abs(0), 'a', " +
                "CAST(if(17: expr, 23: expr > 27: expr, NULL) AS VARCHAR))");
    }

    @Test
    public void testCaseWhenSubquery3() throws Exception {
        String sql = "with tmp as (select 8 id, 'season' type1, 'a.season' pretype, 'season' ranktype from dual ) " +
                "select case when id = abs(0) then 'a' else " +
                "case when exists (select 1 from tmp) then id in (select id > (select type1 from tmp) from tmp )" +
                " else null end end from tmp a;";
        Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        assertContains(pair.first, "CTEAnchor(cteid=2)");
    }

    @Test
    public void testHavingSubqueryNoGroupMode() {
        String sql = "select v3 from t0 group by v2 having 1 > (select v4 from t1 where t0.v1 = t1.v5)";
        long sqlMode = connectContext.getSessionVariable().getSqlMode();
        try {
            connectContext.getSessionVariable().setSqlMode(0);
            Assert.assertThrows("must be an aggregate expression or appear in GROUP BY clause", SemanticException.class,
                    () -> getFragmentPlan(sql));
        } finally {
            connectContext.getSessionVariable().setSqlMode(sqlMode);
        }
    }

    @Test
    public void testComplexCorrelationPredicateInSubquery1() throws Exception {
        String sql = "select v2 in (select v5 from t1 where (t0.v1 IS NULL) = (t1.v4 IS NULL)) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 5> : 5: v5\n" +
                "  |  <slot 8> : 4: v4 IS NULL\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t1");
    }

    @Test
    public void testComplexCorrelationPredicateNotInSubquery2() throws Exception {
        String sql = "select v2 not in (select v5 from t1 where t0.v1 = t1.v4 + t0.v1) from t0";
        Assert.assertThrows("IN subquery not supported the correlation predicate of the" +
                        " WHERE clause that used multiple outer-table columns at the same time.\n", SemanticException.class,
                () -> getFragmentPlan(sql));
    }

    @Test
    public void testUnsupportedInSubquery() {
        String sql = "select * from t0 left join t1 on v1 in (select v7 from t2 where v4 = v8) or v1 < v5;";
        try {
            getFragmentPlan(sql);
            fail("sql should fail");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("referencing columns from more than one table"));
        }

        sql = "select * from t0 left join t1 on v1 + v4 in (select v7 from t2) or v1 < v5;";
        try {
            getFragmentPlan(sql);
            fail("sql should fail");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("referencing columns from more than one table"));
        }
    }

    @Test
    public void testUnsupportedExistSubquery() {
        String sql = "select * from t0 left join t1 on exists (select v7 from t2 where v1 = v4) or v1 < v5;";
        try {
            getFragmentPlan(sql);
            fail("sql should fail");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("referencing columns from more than one table"));
        }
    }

}
