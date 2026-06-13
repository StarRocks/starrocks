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

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AggregatePushDownTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withDatabase("test_window_db");
        starRocksAssert.withTable("CREATE TABLE if not exists trans\n" +
                "(\n" +
                "region VARCHAR(128)  NULL,\n" +
                "order_date DATE NOT NULL,\n" +
                "income DECIMAL128(10, 2) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`region`, `order_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`region`, `order_date`) BUCKETS 128\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ")");
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS t_json_a (\n" +
                "  c0 INT NULL,\n" +
                "  c1 BIGINT NULL,\n" +
                "  c2 DATE NULL,\n" +
                "  c3 JSON NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(c0, c1)\n" +
                "DISTRIBUTED BY HASH(c0) BUCKETS 3\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setEnableRewriteSumByAssociativeRule(false);
        connectContext.getSessionVariable().setEnableEliminateAgg(false);
    }

    private static OlapTable olapTable(String tableName) {
        return (OlapTable) starRocksAssert.getTable("test", tableName);
    }

    private static int countRegexOccurrences(String text, String regex) {
        return text.split(regex, -1).length - 1;
    }

    private static void assertNonGroupByPushDownUsesPartialState(String plan) {
        Assertions.assertTrue(plan.contains("update serialize"),
                "expected branch-level AGGREGATE (update serialize) for serialized partial state:\n" + plan);
        Assertions.assertTrue(plan.contains("merge finalize"),
                "expected top-level AGGREGATE (merge finalize) over serialized partial state:\n" + plan);
    }

    @Test
    public void testPushDownDisableOnBroadcastJoin() {
        connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(false);
        try {
            runFileUnitTest("optimized-plan/agg-pushdown-disable_on_broadcast_join");
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(true);
        }
    }

    @Test
    public void testPushDownEnableOnBroadcastJoin() {
        runFileUnitTest("optimized-plan/agg-pushdown-enable_on_broadcast_join");
    }

    @Test
    public void testPushDownPreAggDisableOnBroadcastJoin() {
        connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(false);
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            runFileUnitTest("optimized-plan/preagg-pushdown-disable_on_broadcast_join");
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
            connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(true);
        }
    }

    @Test
    public void testPushDownPreAggEnableOnBroadcastJoin() {
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            runFileUnitTest("optimized-plan/preagg-pushdown-enable_on_broadcast_join");
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testPushDownDistinctAggBelowWindow()
            throws Exception {
        String q1 = "SELECT DISTINCT \n" +
                "  COALESCE(region, 'Other') AS region, \n" +
                "  order_date, \n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   order_date) AS gp_income,  \n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   MONTH(order_date) ORDER BY order_date) AS gp_income_MTD,\n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   YEAR (order_date), QUARTER(order_date) ORDER BY order_date) AS gp_income_QTD,\n" +
                "  SUM(income) OVER ( PARTITION BY  COALESCE(region, 'Other'), " +
                "   YEAR (order_date) ORDER BY order_date) AS gp_income_YTD  \n" +
                "FROM  trans\n" +
                "where month(order_date)=1\n" +
                "order by region, order_date";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, q1);
        Assertions.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: sum[([3: income, DECIMAL128(10,2), false]); args: DECIMAL128; " +
                "result: DECIMAL128(38,2); args nullable: false; result nullable: true]\n" +
                "  |  group by: [1: region, VARCHAR, true], [2: order_date, DATE, false]\n"), plan);

        Assertions.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     table: trans, rollup: trans\n" +
                "     preAggregation: on\n" +
                "     Predicates: month[([2: order_date, DATE, false]); args: DATE; result: TINYINT; " +
                "args nullable: false; result nullable: false] = 1\n" +
                ""));
    }

    @Test
    public void testNotPushdownWithJsonType() throws Exception {
        String sql = "select /*+ SET_VAR(cbo_push_down_aggregate_mode=1) */ distinct " +
                "cast(json_query(a.c3, '$.\"14\"') as varchar) as v0 " +
                "from t_json_a a " +
                "where a.c0 = 1 and a.c1 in (" +
                "  select distinct v1 from t0 where v1 in (4)" +
                ")";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        assertContains(plan, "|----4:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1\n" +
                "  |    \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  2 <-> [2: c1, BIGINT, true]\n" +
                "  |  10 <-> json_query[([4: c3, JSON, true], '$.\"14\"'); args: JSON,VARCHAR; result: JSON; args nullable: " +
                "true; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: t_json_a, rollup: t_json_a");
    }

    @Test
    public void testPushDownDistinctAggBelowWindow_1() throws Exception {
        // unsupported window func ref cols from partition by cols
        String sql = "select distinct t1d from (select *, sum(t1e) over (partition by t1d) as cnt from test_all_type ) " +
                "t where cnt > 1 limit 10;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "5:SELECT\n" +
                "  |  predicates: 11: sum(5: t1e) > 1.0\n" +
                "  |  \n" +
                "  4:ANALYTIC\n" +
                "  |  functions: [, sum(12: sum), ]\n" +
                "  |  partition by: 4: t1d\n" +
                "  |  \n" +
                "  3:SORT\n" +
                "  |  order by: <slot 4> 4: t1d ASC\n" +
                "  |  analytic partition by: 4: t1d\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(5: t1e)\n" +
                "  |  group by: 4: t1d\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testPushDownDistinctAggBelowWindow_2() throws Exception {
        // unsupported window func ref cols from partition by cols
        String sql = "select distinct t1d from (select *, sum(t1d) over (partition by t1d, t1e) as cnt from " +
                "test_all_type ) t where cnt > 1 limit 10;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:ANALYTIC\n" +
                "  |  functions: [, sum(12: sum), ]\n" +
                "  |  partition by: 4: t1d, 5: t1e\n" +
                "  |  \n" +
                "  3:SORT\n" +
                "  |  order by: <slot 4> 4: t1d ASC, <slot 5> 5: t1e ASC\n" +
                "  |  analytic partition by: 4: t1d, 5: t1e\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: t1d)\n" +
                "  |  group by: 4: t1d, 5: t1e\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testPushDownDistinctAggBelowWindow_3() throws Exception {
        // unsupported window func ref cols from partition by cols
        String sql = "select distinct t1c from (select *, sum(t1d) over (partition by t1e order by t1d) as cnt from " +
                "test_all_type ) t where cnt > 1 limit 10;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: t1d)\n" +
                "  |  group by: 3: t1c, 4: t1d, 5: t1e\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testNotPushDownDistinctAggBelowWindow_1() throws Exception {
        // unsupported count function
        String sql = "select distinct t1d from (select *, count(1) over (partition by t1d) as cnt from test_all_type ) " +
                "t where cnt > 1 limit 10;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:SELECT\n" +
                "  |  predicates: 11: count(1) > 1\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, count(1), ]\n" +
                "  |  partition by: 4: t1d\n" +
                "  |  \n" +
                "  2:SORT\n" +
                "  |  order by: <slot 4> 4: t1d ASC\n" +
                "  |  analytic partition by: 4: t1d\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:EXCHANGE");
    }

    @Test
    public void testPruneColsAfterPushdownAgg_1() throws Exception {
        String sql = "select L_PARTKEY from lineitem_partition where L_SHIPDATE >= '1992-01-01' and L_SHIPDATE < '1993-01-01'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 2> : 2: L_PARTKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lineitem_partition\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/7");
    }

    @Test
    public void testPruneColsAfterPushdownAgg_2() throws Exception {
        String sql = "select max(L_ORDERKEY), sum(2), L_PARTKEY from lineitem_partition " +
                "join t0 on L_PARTKEY = v1 " +
                "where L_SHIPDATE >= '1992-01-01' and L_SHIPDATE < '1993-01-01' group by L_PARTKEY";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "1:Project\n" +
                "  |  <slot 1> : 1: L_ORDERKEY\n" +
                "  |  <slot 2> : 2: L_PARTKEY\n" +
                "  |  <slot 26> : 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lineitem_partition\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/7",
                "7:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 23: cast = 18: v1\n" +
                        "  |  \n" +
                        "  |----6:EXCHANGE\n" +
                        "  |    \n" +
                        "  4:Project\n" +
                        "  |  <slot 2> : 2: L_PARTKEY\n" +
                        "  |  <slot 23> : CAST(2: L_PARTKEY AS BIGINT)\n" +
                        "  |  <slot 24> : 24: max\n" +
                        "  |  <slot 25> : 25: sum\n" +
                        "  |  \n" +
                        "  3:AGGREGATE (update finalize)\n" +
                        "  |  output: sum(26: expr), max(1: L_ORDERKEY)\n" +
                        "  |  group by: 2: L_PARTKEY\n" +
                        "  |  \n" +
                        "  2:EXCHANGE");
    }

    @Test
    public void testPruneDistinctWindow() throws Exception {
        String sql = "select distinct t1c, t1d, t1g, amount " +
                " from (" +
                " select  t1b, t1c, t1d, t1g, id_date, \n" +
                "     sum(id_decimal)over(partition by t1c) as amount\n" +
                "from test_all_type_not_null) tt";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  5:ANALYTIC\n" +
                "  |  functions: [, sum[([12: sum, DECIMAL128(38,2), true]);" +
                " args: DECIMAL128; result: DECIMAL128(38,2); args nullable: true; result nullable: true], ]");
        assertContains(plan, "2:AGGREGATE (update finalize)");
    }

    @Test
    public void testPushDownWithNestedCaseWhenIfs() throws Exception {
        String sql = """
                WITH cte1 AS (
                  SELECT
                    t.t1d AS fk,
                    t.t1a AS cat,
                    CASE WHEN t.t1b = 1 THEN t.t1e ELSE t.t1f END AS cval
                  FROM test_all_type t
                ),
                cte2 AS (
                  SELECT a.cval, a.fk, a.cat
                  FROM cte1 a
                  LEFT JOIN t1 ON a.fk = t1.v4
                ),
                cte3 AS (
                  SELECT CASE WHEN c.cat THEN c.cval ELSE NULL END gval, c.fk
                  FROM cte2 c
                )
                SELECT SUM(gval)
                FROM cte3
                GROUP BY fk;
                """;
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  aggregate: sum[([21: cast, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result" +
                " nullable: true], sum[([6: t1f, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result" +
                " nullable: true]\n" +
                "  |  group by: [1: t1a, VARCHAR, true], [2: t1b, SMALLINT, true], [4: t1d, BIGINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: t1a, VARCHAR, true]\n" +
                "  |  2 <-> [2: t1b, SMALLINT, true]\n" +
                "  |  4 <-> [4: t1d, BIGINT, true]\n" +
                "  |  6 <-> [6: t1f, DOUBLE, true]\n" +
                "  |  21 <-> cast([5: t1e, FLOAT, true] as DOUBLE)\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: test_all_type, rollup: test_all_type\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                "     tabletList=10140,10142,10144\n" +
                "     actualRows=0, avgRowSize=6.0\n" +
                "     cardinality: 1");

    }

    @Test
    public void testRewriterSharedMutationWithCaseWhen() throws Exception {
        // Bug: PushDownAggregateRewriter.rewriteProject() mutates shared CaseWhenOperator
        // in-place via setThenClause(). When two aggregations (SUM + MIN) reference the same
        // CASE WHEN column, the first aggregation's processing corrupts the CaseWhenOperator,
        // causing the second aggregation to see pushed-down column refs instead of original columns.
        String sql = "SELECT SUM(sub.cval), MIN(sub.cval), sub.fk " +
                "FROM ( " +
                "    SELECT t1d AS fk, " +
                "           CASE WHEN t1b = 1 THEN t1e ELSE NULL END AS cval " +
                "    FROM test_all_type " +
                ") sub " +
                "JOIN t0 ON sub.fk = t0.v1 " +
                "GROUP BY sub.fk";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "sum");
        assertContains(plan, "min");
    }

    @Test
    public void testRewriterSharedMutationWithIf() throws Exception {
        // Bug: PushDownAggregateRewriter.rewriteProject() mutates shared CallOperator (IF)
        // in-place via setChild(). Same root cause as the CaseWhen bug but on the IF path.
        String sql = "SELECT SUM(sub.cval), MIN(sub.cval), sub.fk " +
                "FROM ( " +
                "    SELECT t1d AS fk, " +
                "           IF(t1b = 1, t1e, NULL) AS cval " +
                "    FROM test_all_type " +
                ") sub " +
                "JOIN t0 ON sub.fk = t0.v1 " +
                "GROUP BY sub.fk";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "sum");
        assertContains(plan, "min");
    }

    @Test
    public void testNonGroupByAggPushDownThroughUnionAll() throws Exception {
        // A non-group-by aggregation over UNION ALL must produce a multi-stage plan:
        //   1) A partial AGGREGATE node sits in each UNION branch (below the UNION operator).
        //   2) Branch partials serialize aggregate intermediate state.
        //   3) The final-stage aggregate merges that intermediate state and finalizes the result.
        // Without the optimization, the plan would have only a single top-level agg directly
        // over UNION->scans, dragging raw rows through the Exchange.
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            String sql = "SELECT COUNT(*), SUM(v1), hex(hll_serialize(hll_raw(v2))) " +
                    "FROM (SELECT v1, v2 FROM t0 " +
                    "      UNION ALL " +
                    "      SELECT v4, v5 FROM t1) u";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

            // Branch-level partial AGG must include the original functions verbatim.
            // "count[(*);" proves COUNT(*) is being computed at each branch (the verbose
            // explain format is `count[(*); args: ; result: ...]`, not `count[(*)]`).
            // "hll_raw[(" proves HLL_RAW is being computed at each branch.
            Assertions.assertTrue(plan.contains("count[(*);"),
                    "expected partial count[(*) in branches:\n" + plan);
            Assertions.assertTrue(plan.contains("hll_raw[("),
                    "expected partial hll_raw[(...)] in branches:\n" + plan);

            assertNonGroupByPushDownUsesPartialState(plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testNonGroupByAggPushDownCountStarMixedWithComplexExpr() throws Exception {
        // Regression for a P1 in the Project-rewriting loop of PushDownAggregateRewriter.rewrite():
        // when push-down inserts a Project to materialize complex aggregation arguments
        // (e.g. SUM(ABS(v1))), the loop iterates ALL aggregations to set up the project ref.
        // COUNT(*) has no children — getChild(0) on it threw IndexOutOfBoundsException.
        // This is exactly the production statistics SQL shape:
        //     SELECT COUNT(*), SUM(CHAR_LENGTH(col)), MAX(LEFT(col, 200)) FROM (... UNION ALL ...)
        // Here we use ABS(v1) over our bigint tables to trigger the same complex-arg path.
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            String sql = "SELECT COUNT(*), SUM(ABS(v1)), MAX(v1 + 1) " +
                    "FROM (SELECT v1 FROM t0 " +
                    "      UNION ALL " +
                    "      SELECT v4 FROM t1) u";
            // The mere fact this getVerboseFragmentPlan completes (no exception) is the main
            // assertion — before the fix it threw IndexOutOfBoundsException inside the Project
            // creation loop. We additionally verify push-down still triggers.
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
            // Confirm partial agg in branches: COUNT(*) is present below the UNION.
            Assertions.assertTrue(plan.contains("count[(*);"),
                    "branches must compute partial count[(*):\n" + plan);
            assertNonGroupByPushDownUsesPartialState(plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testNonGroupByAggPushDownStatisticsSqlShape() throws Exception {
        // Closer to the real statistics-collection SQL: COUNT(*) + SUM(CHAR_LENGTH(varchar))
        // + HLL_RAW(varchar) + MAX/MIN(LEFT(varchar, 200)) over a UNION-of-sampled-tablets,
        // exercising partial-state merge plus the Project-creation path
        // for complex aggregation arguments (CHAR_LENGTH, LEFT, ABS).
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            String sql = "SELECT COUNT(*), " +
                    "       SUM(CHAR_LENGTH(region)), " +
                    "       hex(hll_serialize(hll_raw(region))), " +
                    "       MAX(LEFT(region, 200)), " +
                    "       MIN(LEFT(region, 200)) " +
                    "FROM (SELECT region FROM trans " +
                    "      UNION ALL " +
                    "      SELECT region FROM trans) u";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
            // Partial count[(*) and hll_raw[ live in branches.
            Assertions.assertTrue(plan.contains("count[(*);"),
                    "expected partial count[(*) in branches:\n" + plan);
            Assertions.assertTrue(plan.contains("hll_raw[("),
                    "expected partial hll_raw[ in branches:\n" + plan);
            assertNonGroupByPushDownUsesPartialState(plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testNonGroupByAggOverUnionDistinctNotPushedDown() throws Exception {
        // UNION DISTINCT requires global dedup. A non-group-by SUM/COUNT cannot be
        // decomposed as "partial per branch + combine on top" because duplicates in different
        // branches need a global view to be deduped. The isUnionAll() guard in
        // visitLogicalUnion must short-circuit this case.
        String sql = "SELECT SUM(v1), COUNT(*) " +
                "FROM (SELECT v1 FROM t0 " +
                "      UNION " +
                "      SELECT v4 FROM t1) u";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

        // count[(*) at the FINAL stage remains the original and is not duplicated per branch.
        Assertions.assertTrue(plan.contains("count[(*);"),
                "expected original count[(*) preserved at top:\n" + plan);
        Assertions.assertEquals(1, countRegexOccurrences(plan, "count\\[\\(\\*\\);"),
                "push-down must not happen for UNION DISTINCT:\n" + plan);
    }

    @Test
    public void testNonGroupByAggBlockedAtJoin() throws Exception {
        // Non-group-by agg over a JOIN must NOT be pushed below the JOIN: a 1-N join would
        // multiply rows and break SUM/COUNT correctness without group-by keys to anchor results.
        //
        // We assert plan shape rather than just "JOIN and AGGREGATE appear somewhere": the
        // original `count[(*)` should appear only once at the top-level final agg, not once per
        // pushed branch.
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            String sql = "SELECT SUM(t0.v1), COUNT(*) FROM t0 JOIN t1 ON t0.v1 = t1.v4";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

            // COUNT(*) appears only once — at the final agg above the join, not duplicated
            // as a partial aggregate inside the join branches.
            int countStarOccurrences = countRegexOccurrences(plan, "count\\[\\(\\*\\);");
            Assertions.assertEquals(1, countStarOccurrences,
                    "expected exactly one count[(*) (at the top-level agg only):\n" + plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testNonGroupByAggOverFilterThenUnionDistinctNotPushedDown() throws Exception {
        // Regression for review feedback: visitLogicalFilter and Project CASE/IF handling add
        // artificial entries to context.groupBys as the traversal descends. The earlier guard
        // `context.groupBys.isEmpty() && !union.isUnionAll()` could be silently bypassed when
        // an intervening Filter injected its column into context.groupBys before reaching the
        // UNION DISTINCT, decomposing a non-group-by aggregate per branch and double-counting
        // duplicates that should have been globally deduped.
        //
        // The fix detects "originally non-group-by" via context.origAggregator.getGroupingKeys()
        // (immutable, set when the agg is first encountered) rather than context.groupBys.
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            String sql = "SELECT SUM(v1), COUNT(*) FROM (" +
                    "  SELECT v1 FROM t0 " +
                    "  UNION " +
                    "  SELECT v4 FROM t1" +
                    ") u WHERE v1 > 0";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

            int countStarOccurrences = countRegexOccurrences(plan, "count\\[\\(\\*\\);");
            Assertions.assertEquals(1, countStarOccurrences,
                    "non-group-by agg over UNION DISTINCT must NOT be pushed even when a Filter " +
                            "adds keys to context.groupBys:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testNonGroupByAggOverFilterThenJoinNotPushedDown() throws Exception {
        // Regression for review P1: visitLogicalJoin previously used context.groupBys.isEmpty()
        // to detect "non-group-by", which is the same mutable-state pitfall as the UNION DISTINCT
        // guard. visitLogicalFilter injects the WHERE column into context.groupBys before the
        // traversal reaches the JOIN, so context.groupBys.isEmpty() is false there and the guard
        // would silently let an originally non-group-by aggregate slip past, pushing partial agg
        // below a 1-N JOIN and producing incorrect SUM/COUNT under multi-match rows.
        //
        // The fix mirrors the UNION DISTINCT guard: detect "originally non-group-by" via
        // context.origAggregator.getGroupingKeys() (immutable, set once when the aggregate is
        // first encountered).
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            String sql = "SELECT SUM(t0.v1), COUNT(*) " +
                    "FROM t0 JOIN t1 ON t0.v1 = t1.v4 " +
                    "WHERE t0.v1 > 0";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

            // COUNT(*) appears only once -- at the final agg above the join, not duplicated
            // as a partial aggregate inside the join branches.
            int countStarOccurrences = countRegexOccurrences(plan, "count\\[\\(\\*\\);");
            Assertions.assertEquals(1, countStarOccurrences,
                    "expected exactly one count[(*) (at the top-level agg only):\n" + plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testNonGroupByAggOverFilterThenUnionAllStillPushedDown() throws Exception {
        // A Filter may inject predicate columns into context.groupBys while the original
        // aggregate is still non-group-by. COUNT/HLL_RAW must still use the non-group-by path.
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            String sql = "SELECT COUNT(*), hex(hll_serialize(hll_raw(v2))) " +
                    "FROM (SELECT v1, v2 FROM t0 " +
                    "      UNION ALL " +
                    "      SELECT v4, v5 FROM t1) u " +
                    "WHERE v1 > 0";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

            // Partial agg in branches must still appear despite the Filter polluting groupBys.
            Assertions.assertTrue(plan.contains("count[(*);"),
                    "expected partial count[(*) in branches even with Filter above UNION ALL:\n" + plan);
            Assertions.assertTrue(plan.contains("hll_raw[("),
                    "expected partial hll_raw[(...)] in branches even with Filter above UNION ALL:\n" + plan);

            assertNonGroupByPushDownUsesPartialState(plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }

    @Test
    public void testNonGroupByAggPushDownAutoModeIgnoresFilterGroupBys() throws Exception {
        // In auto mode, filter-added groupBys are semantic guards for push-down placement, not
        // original grouping keys. Non-group-by aggregation should only use the row-count floor.
        setTableStatistics(olapTable("t0"), 10_000_000);
        setTableStatistics(olapTable("t1"), 10_000_000);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(0);
        try {
            String sql = "SELECT COUNT(*), hex(hll_serialize(hll_raw(v2))) " +
                    "FROM (SELECT v1, v2 FROM t0 " +
                    "      UNION ALL " +
                    "      SELECT v4, v5 FROM t1) u " +
                    "WHERE v1 > 0";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

            assertNonGroupByPushDownUsesPartialState(plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
            setTableStatistics(olapTable("t0"), 0);
            setTableStatistics(olapTable("t1"), 0);
        }
    }

    @Test
    public void testNonGroupByAggPushDownAutoModeRespectsRowCountFloor() throws Exception {
        // The @BeforeAll sets cboPushDownAggregateMode=1 (force). Production default is 0 (auto),
        // where checkStatistics applies a row-count floor (SMALL_SCALE_ROWS_LIMIT) for the
        // non-group-by path: without enough input rows, adding a partial-agg node to each branch
        // is pure overhead and the rule should skip push-down. t0/t1 are empty mock tables here,
        // well below the floor, so push-down must NOT happen in auto mode.
        connectContext.getSessionVariable().setCboPushDownAggregateMode(0);
        try {
            String sql = "SELECT COUNT(*), SUM(v1), hex(hll_serialize(hll_raw(v2))) " +
                    "FROM (SELECT v1, v2 FROM t0 " +
                    "      UNION ALL " +
                    "      SELECT v4, v5 FROM t1) u";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

            int countStarOccurrences = countRegexOccurrences(plan, "count\\[\\(\\*\\);");
            Assertions.assertEquals(1, countStarOccurrences,
                    "small input under auto mode must NOT trigger per-branch partial aggregation:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        }
    }

    @Test
    public void testNonGroupByAggPushDownGlobalMode() throws Exception {
        // The other testNonGroupBy* cases set cbo_push_down_aggregate=local. Production default is
        // global, which generates a different AggType for the pushed partial agg. Verify the merge
        // finalize rewrite path holds under the production default.
        String sql = "SELECT COUNT(*), SUM(v1), hex(hll_serialize(hll_raw(v2))) " +
                "FROM (SELECT v1, v2 FROM t0 " +
                "      UNION ALL " +
                "      SELECT v4, v5 FROM t1) u";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

        Assertions.assertTrue(plan.contains("count[(*);"),
                "expected partial count[(*) in branches under global mode:\n" + plan);
        Assertions.assertTrue(plan.contains("hll_raw[("),
                "expected partial hll_raw[(...)] in branches under global mode:\n" + plan);
        assertNonGroupByPushDownUsesPartialState(plan);
    }

    @Test
    public void testNonGroupByAggOverUnionAllWithJoinBranchNotPushedDown() throws Exception {
        // Regression for review feedback: a UNION ALL whose one branch contains a JOIN must
        // abort push-down. The all-or-nothing decomposition in visitLogicalUnion requires every
        // branch to accept the partial aggregate; the JOIN branch contributes none (a non-group-by
        // aggregate cannot push below a 1-N join), so the whole UNION push-down must be skipped.
        // Without this guard, the aggregate would land in only some branches and silently mis-count.
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        try {
            String sql = "SELECT COUNT(*), SUM(v1) FROM ( " +
                    "  SELECT v1 FROM t0 " +
                    "  UNION ALL " +
                    "  SELECT t1.v4 AS v1 FROM t1 JOIN t2 ON t1.v5 = t2.v8 " +
                    ") u";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);

            // COUNT(*) appears exactly once — at the final agg above the UNION, not per-branch.
            int countStarOccurrences = countRegexOccurrences(plan, "count\\[\\(\\*\\);");
            Assertions.assertEquals(1, countStarOccurrences,
                    "expected exactly one count[(*) at the top-level agg (no per-branch partials):\n" + plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("global");
        }
    }
}
