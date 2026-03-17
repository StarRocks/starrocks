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

import com.starrocks.common.profile.Tracers;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for the commonSubOperatorMap field in LogicalProjectOperator.
 * <p>
 * A non-empty commonSubOperatorMap arises when MergeTwoProjectRule merges two stacked
 * LogicalProjectOperators and the inner project defines a non-trivial expression that is
 * referenced 2+ times in the outer project.  compactSubColumnRefMap() keeps such entries
 * (inlining constants and single-reference entries).
 * <p>
 * All tests below use subqueries or CTEs to create stacked projects so the inner-project
 * derived columns end up in commonSubOperatorMap rather than being handled by
 * ScalarOperatorsReuse (which operates on a single projection level).
 */
public class ProjectCommonSubExprPlanTest extends PlanTestBase {

    /**
     * Disable optimizer timeout to avoid StarRocksPlannerException when complex SQL
     * takes long in the memo phase (e.g. new_planner_optimize_timeout).
     */
    @BeforeAll
    public static void disableOptimizerTimeout() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        // Table for HoistHeavyCostExprsUponTopnRule: DECIMAL128 division triggers the rule.
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `t_hoist_heavy` (\n"
                + "  `k` bigint NULL COMMENT \"\",\n"
                + "  `d1` decimal(20,2) NULL COMMENT \"\",\n"
                + "  `d2` decimal(20,2) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k`)\n"
                + "DISTRIBUTED BY HASH(`k`) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
    }

    private String currentTestName;
    private String lastSql;
    private String lastPlan;

    @BeforeEach
    public void initTracer(TestInfo testInfo) {
        currentTestName = testInfo.getDisplayName();
        Tracers.register(connectContext);
        Tracers.init(connectContext, "TIMING", "Optimizer");
    }

    @AfterEach
    public void closeTracer() {
        String traceAll = Tracers.printLogs();
        System.out.println("====== TRACE for " + currentTestName + " ======");
        System.out.println(traceAll);
        System.out.println("====== END TRACE ======\n");
        Tracers.close();
        if (lastSql != null) {
            System.out.println("====== SQL ======");
            System.out.println(lastSql);
            System.out.println("====== PLAN ======");
            System.out.println(lastPlan);
            lastSql = null;
            lastPlan = null;
        }
    }

    private void printSqlAndPlan(String sql, String plan) {
        lastSql = sql;
        lastPlan = plan;
    }

    // -----------------------------------------------------------------------
    // CTE-based stacked projects: inner CASE bucket referenced 2+ times
    // in outer CASE branches → kept in commonSubOperatorMap
    // -----------------------------------------------------------------------

    @Test
    public void testCTECaseWhenBucketPassedThrough() throws Exception {
        String sql = """
                WITH cte1 AS (
                  SELECT v1, v2,
                    CASE
                      WHEN v1 = 10 AND v3 < 10 THEN 1
                      WHEN v1 = 10 AND v3 >= 10 THEN 2
                      ELSE 3
                    END AS bucket
                  FROM t0
                ),
                cte2 AS (
                  SELECT v1, v2,
                    CASE
                      WHEN bucket * 10 = 10 AND v2 > 0 THEN 'a'
                      WHEN bucket * 10 = 20 AND v2 > 0 THEN 'b'
                      ELSE NULL
                    END AS label
                  FROM cte1
                )
                SELECT * FROM cte2
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "TABLE: t0");
        assertContains(plan, "Project");
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testCTECaseWhenBucketUsedOnlyInOuterCase() throws Exception {
        String sql = """
                WITH cte1 AS (
                  SELECT v1, v2,
                    CASE
                      WHEN v1 < 10 THEN 1
                      WHEN v1 < 20 THEN 2
                      ELSE 3
                    END AS bucket
                  FROM t0
                ),
                cte2 AS (
                  SELECT v1, v2,
                    CASE
                      WHEN bucket = 1 AND v2 > 0 THEN 'a'
                      WHEN bucket = 2 AND v2 > 0 THEN 'b'
                      ELSE NULL
                    END AS label
                  FROM cte1
                )
                SELECT * FROM cte2
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "TABLE: t0");
        assertContains(plan, "Project");
    }

    @Test
    public void testDuplicateCaseWhenInSubqueryProducesCommonExpr() throws Exception {
        String sql = """
                SELECT id, region, priority FROM (
                  SELECT v1 AS id, v2 AS region,
                    CASE
                      WHEN (CASE WHEN v1 < 5 THEN 1 WHEN v1 < 10 THEN 2 ELSE 3 END) = 1 AND v2 > 0 THEN 'p1'
                      WHEN (CASE WHEN v1 < 5 THEN 1 WHEN v1 < 10 THEN 2 ELSE 3 END) = 2 AND v2 > 0 THEN 'p2'
                      ELSE NULL
                    END AS priority
                  FROM t0
                ) t WHERE priority IS NOT NULL
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "TABLE: t0");
        assertContains(plan, "Project");
        assertContains(plan, "if(");
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Subquery-based: inner derived expression used 2+ times in outer SELECT
    // (MergeTwoProjectRule merges stacked projects; compactSubColumnRefMap
    // keeps the inner expression because refcount >= 2)
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryDerivedColumnUsedTwice() throws Exception {
        String sql = """
                SELECT a + 1, a * 2 FROM (
                  SELECT v1 * v2 AS a FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testSubqueryDerivedColumnUsedThreeTimes() throws Exception {
        String sql = """
                SELECT a + 1, a * 2, a - 3 FROM (
                  SELECT v1 + v2 AS a FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testSubqueryCaseWhenUsedTwice() throws Exception {
        String sql = """
                SELECT bucket + 1, bucket * 2 FROM (
                  SELECT CASE WHEN v1 < 10 THEN 1
                              WHEN v1 < 20 THEN 2
                              ELSE 3 END AS bucket
                  FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testSubqueryMultipleDerivedColumnsEachUsedTwice() throws Exception {
        String sql = """
                SELECT a + b, a - b, a * b FROM (
                  SELECT v1 + v2 AS a, v2 * v3 AS b FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testSubqueryDerivedColumnInSelectAndWhere() throws Exception {
        String sql = """
                SELECT product, product + 1 FROM (
                  SELECT v1 * v2 AS product FROM t0
                ) sub
                WHERE product > 100
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "Project");
        assertContains(plan, "TABLE: t0");
    }

    // -----------------------------------------------------------------------
    // Negative case: inner expression used only once → inlined by compaction,
    // commonSubOperatorMap becomes empty
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryDerivedColumnUsedOnce_NoCommonExpr() throws Exception {
        String sql = """
                SELECT a + 1 FROM (
                  SELECT v1 * v2 AS a FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertNotContains(plan, "common expressions:");
    }

    @Test
    public void testIdentitySubquery_NoCommonExpr() throws Exception {
        String sql = """
                SELECT v1, v2 FROM (
                  SELECT v1, v2 FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertNotContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // CTE-based: CTE defines derived expression referenced 2+ times
    // -----------------------------------------------------------------------

    @Test
    public void testCTEDerivedColumnUsedTwice() throws Exception {
        String sql = """
                WITH cte AS (
                  SELECT v1, v2, v1 + v2 AS sum12 FROM t0
                )
                SELECT sum12 + 1, sum12 * 2 FROM cte
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testCTEDerivedColumnUsedInMultipleExprs() throws Exception {
        String sql = """
                WITH cte AS (
                  SELECT v1, v2, v1 * v2 AS product FROM t0
                )
                SELECT product, product + v1, product - v2 FROM cte
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testCTECaseWhenUsedMultipleTimes() throws Exception {
        String sql = """
                WITH cte AS (
                  SELECT v1, v2,
                    CASE WHEN v1 < 10 THEN 1
                         WHEN v1 < 20 THEN 2
                         ELSE 3 END AS bucket
                  FROM t0
                )
                SELECT
                  bucket,
                  bucket + 1 AS next_bucket,
                  CASE bucket WHEN 1 THEN 'low'
                              WHEN 2 THEN 'mid'
                              ELSE 'high' END AS label
                FROM cte
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "Project");
        assertContains(plan, "TABLE: t0");
    }

    @Test
    public void testCTECoalesceUsedMultipleTimes() throws Exception {
        String sql = """
                WITH cte AS (
                  SELECT v1, coalesce(v2, 0) + coalesce(v3, 0) AS total FROM t0
                )
                SELECT total, total * 2, total > 100 FROM cte
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Multi-level nested subqueries: MergeTwoProjectRule fires multiple times
    // producing a chain of entries in commonSubOperatorMap
    // -----------------------------------------------------------------------

    @Test
    public void testTwoLevelNestedSubquery() throws Exception {
        String sql = """
                SELECT x + 1, x * 2 FROM (
                  SELECT y + 10 AS x FROM (
                    SELECT v1 * v2 AS y FROM t0
                  ) q1
                ) q2
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testThreeLevelNestedSubquery() throws Exception {
        String sql = """
                SELECT z + 1, z * 2 FROM (
                  SELECT y + 100 AS z FROM (
                    SELECT x + 10 AS y FROM (
                      SELECT v1 * v2 AS x FROM t0
                    ) q1
                  ) q2
                ) q3
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Multi-level CTE chains: each level adds a derived column
    // -----------------------------------------------------------------------

    @Test
    public void testThreeLevelCTEChain() throws Exception {
        String sql = """
                WITH
                  level1 AS (
                    SELECT v1, v2, v1 + v2 AS sum12 FROM t0
                  ),
                  level2 AS (
                    SELECT v1, sum12,
                      CASE WHEN sum12 > 100 THEN 'high' ELSE 'low' END AS tier
                    FROM level1
                  ),
                  level3 AS (
                    SELECT sum12, tier,
                      CASE WHEN tier = 'high' THEN sum12 * 2 ELSE sum12 END AS adjusted
                    FROM level2
                  )
                SELECT sum12, tier, adjusted FROM level3
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "TABLE: t0");
        assertContains(plan, "Project");
    }

    @Test
    public void testCTEWithMultipleDerivedColumnsPerLevel() throws Exception {
        String sql = """
                WITH
                  base AS (
                    SELECT v1, v2, v3,
                      v1 + v2 AS s, v1 * v2 AS p
                    FROM t0
                  ),
                  derived AS (
                    SELECT s, p,
                      s + p AS sp_sum,
                      s * p AS sp_prod
                    FROM base
                  )
                SELECT sp_sum + 1, sp_sum * 2, sp_prod + 1, sp_prod * 2 FROM derived
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Subquery CASE WHEN where condition expression is used multiple times
    // in the outer query (e.g., referenced in multiple outer CASE branches)
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryCaseWhenConditionReferencedInOuterCaseBranches() throws Exception {
        String sql = """
                SELECT
                  CASE WHEN bucket = 1 THEN v2 * 10
                       WHEN bucket = 2 THEN v2 * 20
                       ELSE v2 END AS weighted
                FROM (
                  SELECT v1, v2,
                    CASE WHEN v1 < 5 THEN 1
                         WHEN v1 < 15 THEN 2
                         ELSE 3 END AS bucket
                  FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "Project");
        assertContains(plan, "TABLE: t0");
    }

    @Test
    public void testSubqueryDerivedExprInMultipleOuterCaseColumns() throws Exception {
        String sql = """
                SELECT
                  CASE WHEN bucket = 1 AND v2 > 0 THEN 'a'
                       WHEN bucket = 2 AND v2 > 0 THEN 'b'
                       ELSE 'c' END AS label,
                  CASE WHEN bucket = 1 THEN v2 * 10
                       WHEN bucket = 2 THEN v2 * 20
                       ELSE v2 * 30 END AS weight
                FROM (
                  SELECT v1, v2,
                    CASE WHEN v1 < 10 THEN 1
                         WHEN v1 < 20 THEN 2
                         ELSE 3 END AS bucket
                  FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "Project");
        assertContains(plan, "TABLE: t0");
    }

    // -----------------------------------------------------------------------
    // Derived column from subquery used in GROUP BY context
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryDerivedColumnInGroupBy() throws Exception {
        String sql = """
                SELECT bucket, count(*), sum(v2) FROM (
                  SELECT v1, v2,
                    CASE WHEN v1 < 10 THEN 1
                         WHEN v1 < 20 THEN 2
                         ELSE 3 END AS bucket
                  FROM t0
                ) sub
                GROUP BY bucket
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "AGGREGATE");
        assertContains(plan, "TABLE: t0");
    }

    @Test
    public void testSubqueryDerivedColumnInGroupByAndHaving() throws Exception {
        String sql = """
                SELECT bucket, count(*) AS cnt FROM (
                  SELECT v1 + v2 AS bucket FROM t0
                ) sub
                GROUP BY bucket
                HAVING count(*) > 1
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "AGGREGATE");
    }

    // -----------------------------------------------------------------------
    // Derived column used in ORDER BY context
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryDerivedColumnInOrderBy() throws Exception {
        String sql = """
                SELECT a, a + 1 FROM (
                  SELECT v1 * v2 AS a FROM t0
                ) sub
                ORDER BY a
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "SORT");
    }

    // -----------------------------------------------------------------------
    // Subquery with function-call-based derived column
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryAbsFunctionUsedTwice() throws Exception {
        String sql = """
                SELECT abs_v1 + 1, abs_v1 * 2 FROM (
                  SELECT abs(v1) AS abs_v1 FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
        assertContains(plan, "abs");
    }

    @Test
    public void testSubqueryComplexFunctionUsedTwice() throws Exception {
        String sql = """
                SELECT computed + 1, computed - 1 FROM (
                  SELECT abs(v1 - v2) + abs(v2 - v3) AS computed FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Subquery with CAST-based derived column
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryCastUsedTwice() throws Exception {
        String sql = """
                SELECT d + 1.0, d * 2.0 FROM (
                  SELECT CAST(v1 AS DOUBLE) AS d FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Subquery with IF/COALESCE derived column
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryIfUsedTwice() throws Exception {
        String sql = """
                SELECT val + 1, val * 10 FROM (
                  SELECT IF(v1 > 0, v2, v3) AS val FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
        assertContains(plan, "if(");
    }

    @Test
    public void testSubqueryCoalesceUsedTwice() throws Exception {
        String sql = """
                SELECT c + 1, c * 2 FROM (
                  SELECT COALESCE(v1, v2, v3) AS c FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
        assertContains(plan, "coalesce");
    }

    // -----------------------------------------------------------------------
    // Subquery with string function derived column
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryStringConcatUsedTwice() throws Exception {
        String sql = """
                SELECT full_name, length(full_name) FROM (
                  SELECT concat(k1, '-', k2) AS full_name FROM t7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
        assertContains(plan, "concat");
    }

    // -----------------------------------------------------------------------
    // Subquery with arithmetic chain producing deeper common sub expr
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryDeepArithmeticChain() throws Exception {
        String sql = """
                SELECT s * s, s + 100, s - 50 FROM (
                  SELECT (v1 + v2) * (v1 - v2) AS s FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Subquery with boolean derived column used multiple times
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryBooleanExprUsedTwice() throws Exception {
        String sql = """
                SELECT
                  IF(is_positive, v2 * 2, v2) AS adjusted,
                  IF(is_positive, 'yes', 'no') AS label
                FROM (
                  SELECT v2, v1 > 0 AS is_positive FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "if(");
    }

    // -----------------------------------------------------------------------
    // Cross-table subquery: derived expr from join result used 2+ times
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryFromJoinUsedTwice() throws Exception {
        String sql = """
                SELECT total + 1, total * 2 FROM (
                  SELECT t0.v1 + t1.v4 AS total
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // Left outer join: derived expr from nullable side used 2+ times
    // -----------------------------------------------------------------------

    @Test
    public void testLeftJoinDerivedExprUsedTwice() throws Exception {
        String sql = """
                SELECT total + 1, total * 2 FROM (
                  SELECT IFNULL(t0.v1 + t1.v4, 0) AS total
                  FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "LEFT OUTER JOIN");
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Self-join with a derived expression referenced multiple times
    // -----------------------------------------------------------------------

    @Test
    public void testSelfJoinDerivedExprUsedTwice() throws Exception {
        String sql = """
                SELECT diff + 1, diff * 3 FROM (
                  SELECT a.v1 - b.v1 AS diff
                  FROM t0 a JOIN t0 b ON a.v2 = b.v2
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Three-table join with derived expression used 2+ times
    // -----------------------------------------------------------------------

    @Test
    public void testThreeTableJoinDerivedExprUsedTwice() throws Exception {
        String sql = """
                SELECT s + 1, s * 2 FROM (
                  SELECT t0.v1 + t1.v4 + t0.v3 AS s
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t0 t2 ON t1.v4 = t2.v1
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // Three-way join across distinct tables with derived expressions
    // -----------------------------------------------------------------------

    @Test
    public void testThreeWayJoinDerivedExprFromAllTables() throws Exception {
        String sql = """
                SELECT score + 1, score * 2, score - 10 FROM (
                  SELECT t0.v1 + t1.v5 + t2.v9 AS score
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testThreeWayJoinCTEWithCaseWhen() throws Exception {
        String sql = """
                WITH joined AS (
                  SELECT t0.v1, t1.v5, t2.v9,
                    CASE
                      WHEN t0.v1 > t1.v5 AND t1.v5 > t2.v9 THEN 1
                      WHEN t0.v1 > t2.v9 THEN 2
                      ELSE 3
                    END AS rank_bucket
                  FROM t0
                  JOIN t1 ON t0.v2 = t1.v5
                  JOIN t2 ON t1.v6 = t2.v8
                )
                SELECT
                  rank_bucket,
                  rank_bucket + 10 AS shifted,
                  CASE rank_bucket WHEN 1 THEN 'top'
                                   WHEN 2 THEN 'mid'
                                   ELSE 'low' END AS tier
                FROM joined
                """;

        //        String plan = getFragmentPlan(sql);
        String plan = getExecPlan(sql).getExplainString(StatementBase.ExplainLevel.LOGICAL);
        printSqlAndPlan(sql, plan);
        //        assertContains(plan, "HASH JOIN");
        //        assertContains(plan, "Project");
    }

    @Test
    public void testThreeWayJoinCTEWithCaseWhen1() throws Exception {
        String sql = """
                WITH t0_t1 AS (
                  SELECT t1.v6,
                    CASE
                      WHEN t0.v1 + t1.v4 > 10 THEN 1
                      WHEN (t0.v1 + t1.v4) * 2 > 100 THEN 2
                      ELSE 3
                    END AS rank_bucket
                  FROM t0
                  JOIN t1 ON t0.v2 = t1.v5
                )
                SELECT
                  rank_bucket,
                  t2.v7
                FROM t0_t1
                JOIN t2 ON t0_t1.v6 = t2.v8
                """;

        //        String plan = getFragmentPlan(sql);
        String plan = getExecPlan(sql).getExplainString(StatementBase.ExplainLevel.LOGICAL);
        printSqlAndPlan(sql, plan);
        //        assertContains(plan, "HASH JOIN");
        //        assertContains(plan, "Project");
    }

    @Test
    public void testThreeWayJoinCTEWithCaseWhen2() throws Exception {
        String sql = """
                WITH t0_0 AS (
                  SELECT v1 + 10 AS v1_0, v2 FROM t0
                ),
                t0_1 AS (
                  SELECT v1_0 * 2 AS v1_1, v1_0 / 2 AS v1_2, v2 FROM t0_0
                ),
                t0_t1 AS (
                  SELECT t0_1.v1_1 AS f1, t0_1.v1_2 AS f2, t1.v6
                  FROM t0_1
                  JOIN t1 ON t0_1.v2 = t1.v5
                )
                SELECT
                  f1,
                  f2,
                  t2.v7
                FROM t0_t1
                JOIN t2 ON t0_t1.v6 = t2.v8
                """;

        //        String plan = getFragmentPlan(sql);
        String plan = getExecPlan(sql).getExplainString(StatementBase.ExplainLevel.LOGICAL);
        printSqlAndPlan(sql, plan);
        //        assertContains(plan, "HASH JOIN");
        //        assertContains(plan, "Project");
    }

    @Test
    public void testThreeWayJoinCTEWithCaseWhen3() throws Exception {
        String sql = """
                WITH t0_t1 AS (
                  SELECT t1.v6,
                    CASE
                      WHEN (t0.v1 + 10) > 10 THEN 1
                      WHEN (t0.v1 + 10) * 2 > 100 THEN 2
                      ELSE 3
                    END AS rank_bucket
                  FROM t0
                  JOIN t1 ON t0.v2 = t1.v5
                )
                SELECT
                  rank_bucket,
                  t2.v7
                FROM t0_t1
                JOIN t2 ON t0_t1.v6 = t2.v8
                """;

        //        String plan = getFragmentPlan(sql);
        String plan = getExecPlan(sql).getExplainString(StatementBase.ExplainLevel.LOGICAL);
        printSqlAndPlan(sql, plan);
        //        assertContains(plan, "HASH JOIN");
        //        assertContains(plan, "Project");
    }

    @Test
    public void testTwoWay() throws Exception {
        String sql = """
                WITH t0_t1 AS (
                  SELECT t0.v1 AS v1, t0.v2 * 10 AS v2, t1.v5 AS v5
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                )
                SELECT
                  t0_t1.v1,
                  t0_t1.v2 + 1 AS v2_plus,
                  t0_t1.v2 * 2 AS v2_double,
                  t0_t1.v5
                FROM t0_t1
                """;
        String plan = getExecPlan(sql).getExplainString(StatementBase.ExplainLevel.LOGICAL);
        printSqlAndPlan(sql, plan);
    }

    @Test
    public void testJoinAssociativityRuleBottomJoinWithCommSub() throws Exception {
        String sql = """
                WITH t0_t1 AS (
                  SELECT t0.v1 AS v1, t0.v2 * 10 AS v2, t1.v5 AS v5
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                ),
                t0_t1_project AS (
                  SELECT
                    t0_t1.v1,
                    t0_t1.v2 + 1 AS v2_plus,
                    t0_t1.v2 * 2 AS v2_double,
                    t0_t1.v5
                  FROM t0_t1
                )
                SELECT
                  t0_t1_project.v1,
                  t0_t1_project.v2_plus,
                  t0_t1_project.v2_double,
                  t0_t1_project.v5,
                  t2.v9
                FROM t0_t1_project
                JOIN t2 ON t0_t1_project.v1 = t2.v8
                """;

        //        String plan = getFragmentPlan(sql);
        String plan = getExecPlan(sql).getExplainString(StatementBase.ExplainLevel.LOGICAL);
        printSqlAndPlan(sql, plan);
        //        assertContains(plan, "HASH JOIN");
        //        assertContains(plan, "Project");
    }

    @Test
    public void testThreeWayJoinMixedJoinTypes() throws Exception {
        String sql = """
                SELECT combo + 1, combo * 3 FROM (
                  SELECT COALESCE(t0.v2, 0) + COALESCE(t2.v8, 0) AS combo
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  LEFT JOIN t2 ON t1.v6 = t2.v7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testThreeWayJoinWithAggAndDerivedExpr() throws Exception {
        String sql = """
                SELECT total + 1, total * 2 FROM (
                  SELECT SUM(t0.v2 + t1.v5 + t2.v9) AS total
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                  GROUP BY t0.v1
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
        assertContains(plan, "AGGREGATE");
    }

    // -----------------------------------------------------------------------
    // CTE over a join: derived expression used multiple times in outer query
    // -----------------------------------------------------------------------

    @Test
    public void testCTEOverJoinDerivedExprUsedTwice() throws Exception {
        String sql = """
                WITH joined AS (
                  SELECT t0.v1 * t1.v5 AS product
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                )
                SELECT product + 10, product * 5, product - 1
                FROM joined
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Join with aggregation: derived expr from grouped join used 2+ times
    // -----------------------------------------------------------------------

    @Test
    public void testJoinAggDerivedExprUsedTwice() throws Exception {
        String sql = """
                SELECT total_sum + 1, total_sum * 2 FROM (
                  SELECT SUM(t0.v2 + t1.v5) AS total_sum
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                  GROUP BY t0.v1
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
        assertContains(plan, "AGGREGATE");
        //        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Right join: derived expr on the nullable side used 2+ times
    // -----------------------------------------------------------------------

    @Test
    public void testRightJoinDerivedExprUsedTwice() throws Exception {
        String sql = """
                SELECT val + 1, val * 2 FROM (
                  SELECT COALESCE(t0.v2, 0) + t1.v5 AS val
                  FROM t0 RIGHT JOIN t1 ON t0.v1 = t1.v4
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "RIGHT OUTER JOIN");
    }

    // -----------------------------------------------------------------------
    // Join with CASE WHEN on join columns used multiple times
    // -----------------------------------------------------------------------

    @Test
    public void testJoinCaseWhenDerivedUsedTwice() throws Exception {
        String sql = """
                SELECT
                  IF(flag = 1, v2, v5) AS picked,
                  flag + 10 AS flag_plus
                FROM (
                  SELECT t0.v2, t1.v5,
                    CASE WHEN t0.v1 > t1.v4 THEN 1 ELSE 0 END AS flag
                  FROM t0 JOIN t1 ON t0.v3 = t1.v6
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // Subquery with NULLIF/IFNULL used multiple times
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryNullIfUsedTwice() throws Exception {
        String sql = """
                SELECT safe_val + 1, safe_val * 2 FROM (
                  SELECT nullif(v1, 0) AS safe_val FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testSubqueryIfNullUsedTwice() throws Exception {
        String sql = """
                SELECT filled + 1, filled * 2 FROM (
                  SELECT ifnull(v1, -1) AS filled FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Verbose explain output verification
    // -----------------------------------------------------------------------

    @Test
    public void testVerboseExplainShowsCommonExprFromSubquery() throws Exception {
        String sql = """
                SELECT a + 1, a * 2 FROM (
                  SELECT v1 * v2 AS a FROM t0
                ) sub
                """;

        String plan = getVerboseExplain(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Subquery with multiple stacked derived expressions, outer uses both
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryTwoDerivedBothUsedTwice() throws Exception {
        String sql = """
                SELECT x + y, x - y, x * y, x / y FROM (
                  SELECT v1 + v2 AS x, v2 + v3 AS y FROM t0
                ) sub
                WHERE y != 0
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // CTE with complex CASE returning string, used in multiple string ops
    // -----------------------------------------------------------------------

    @Test
    public void testCTEStringCaseUsedInMultipleStringOps() throws Exception {
        String sql = """
                WITH cte AS (
                  SELECT v1,
                    CASE WHEN v1 < 10 THEN 'alpha'
                         WHEN v1 < 20 THEN 'beta'
                         ELSE 'gamma' END AS category
                  FROM t0
                )
                SELECT
                  upper(category),
                  length(category),
                  concat(category, '-suffix')
                FROM cte
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // LIMIT combined with stacked project common sub expr
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryWithLimitPreservesCommonExpr() throws Exception {
        String sql = """
                SELECT a + 1, a * 2 FROM (
                  SELECT v1 * v2 AS a FROM t0
                ) sub
                LIMIT 10
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
        assertContains(plan, "limit: 10");
    }

    @Test
    public void testInnerLimitAndOuterCommonExpr() throws Exception {
        String sql = """
                SELECT a + 1, a * 2 FROM (
                  SELECT v1 * v2 AS a FROM t0 LIMIT 100
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Subquery derived column used in window function partition/order
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryDerivedColumnInWindowPartition() throws Exception {
        String sql = """
                SELECT bucket, v2,
                  sum(v2) OVER (PARTITION BY bucket ORDER BY v2) AS window_sum
                FROM (
                  SELECT v1, v2,
                    CASE WHEN v1 < 10 THEN 1 ELSE 2 END AS bucket
                  FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "ANALYTIC");
    }

    // -----------------------------------------------------------------------
    // Subquery with modulo-based bucketing used multiple times
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryModuloBucketUsedMultipleTimes() throws Exception {
        String sql = """
                SELECT
                  bucket,
                  bucket + 1,
                  CASE bucket
                    WHEN 0 THEN 'zero'
                    WHEN 1 THEN 'one'
                    WHEN 2 THEN 'two'
                    ELSE 'other'
                  END AS bucket_name
                FROM (
                  SELECT v1, CAST(v1 % 3 AS INT) AS bucket FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "Project");
        assertContains(plan, "TABLE: t0");
    }

    // -----------------------------------------------------------------------
    // UNION ALL where each branch has its own stacked project
    // -----------------------------------------------------------------------

    @Test
    public void testUnionAllWithSubqueryDerivedColumns() throws Exception {
        String sql = """
                SELECT a + 1, a * 2 FROM (
                  SELECT v1 + v2 AS a FROM t0
                ) s1
                UNION ALL
                SELECT b + 1, b * 2 FROM (
                  SELECT v4 + v5 AS b FROM t1
                ) s2
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "UNION");
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Subquery with power/exponent derived column
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryPowerExprUsedTwice() throws Exception {
        String sql = """
                SELECT sq + 1, sq * 3 FROM (
                  SELECT v1 * v1 AS sq FROM t0
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // Subquery with mixed types derived column (implicit cast chains)
    // -----------------------------------------------------------------------

    @Test
    public void testSubqueryMixedTypeDerivedColumn() throws Exception {
        String sql = """
                SELECT d + 0.5, d * 3 FROM (
                  SELECT CAST(t1b AS DOUBLE) + CAST(t1c AS DOUBLE) AS d
                  FROM test_all_type
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
    }

    // -----------------------------------------------------------------------
    // MergeProjectWithChildRule no-op: when the child of a Project is a
    // LOGICAL_META_SCAN, the rule's check() returns false so the Project is
    // preserved for PushDownAggToMetaScanRule to consume.
    // -----------------------------------------------------------------------

    @Test
    public void testMergeProjectWithChildRuleNoOpForMetaScan() throws Exception {
        String sql = "SELECT max(v1), min(v2) FROM t0 [_META_]";
        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "MetaScan");
    }

    // -----------------------------------------------------------------------
    // Traditional (single-level) common subexpression reuse via
    // ScalarOperatorsReuse — these must keep working with the new
    // commonSubOperatorMap plumbing.
    // -----------------------------------------------------------------------

    @Test
    public void testTraditionalSameLevelExpressionReuse() throws Exception {
        String sql = "select v1 + v2, v1 + v2 + v3, v1 + v2 + v3 + 1 from t0";
        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
        assertContains(plan, "1: v1 + 2: v2");
    }

    @Test
    public void testTraditionalCastReuse() throws Exception {
        String sql = """
                SELECT
                  CAST(v1 * v1 AS DOUBLE) / CAST(v1 AS DOUBLE) % CAST(v1 AS DOUBLE)
                    + CAST(v1 AS DOUBLE) - CAST(v1 DIV v1 AS DOUBLE),
                  v2 & ~ v1 | v3 ^ 1
                FROM t0
                """;
        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "common expressions:");
        assertContains(plan, "CAST(1: v1 AS DOUBLE)");
    }

    // =======================================================================
    // Join reorder + common sub-expression interaction tests.
    //
    // These exercise the code path where:
    //   1. MergeTwoProjectRule creates a LogicalProjectOperator whose
    //      commonSubOperatorMap is non-empty.
    //   2. MergeProjectWithChildRule pushes that Projection onto a
    //      LogicalJoinOperator.
    //   3. JoinAssociativityRule or JoinLeftAsscomRule rearranges the joins.
    //
    // If JoinAssociateBaseRule silently drops commonSubOperatorMap entries,
    // planning will either fail or produce wrong results.
    // =======================================================================

    // -----------------------------------------------------------------------
    // Associativity: (A ⋈ B) ⋈ C  →  A ⋈ (B ⋈ C)
    // CSE defined on the inner join projection, used 2+ times in outer SELECT
    // -----------------------------------------------------------------------

    @Test
    public void testAssociativityTwoTableCSEUsedTwice() throws Exception {
        String sql = """
                SELECT total + 1, total * 2 FROM (
                  SELECT t0.v1 + t1.v4 AS total
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                ) sub
                JOIN t2 ON sub.total = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
        assertContains(plan, "TABLE: t0");
        assertContains(plan, "TABLE: t1");
        assertContains(plan, "TABLE: t2");
    }

    @Test
    public void testAssociativityCSEFromBothSidesOfInnerJoin() throws Exception {
        String sql = """
                SELECT combo + 10, combo * 5, combo - 1 FROM (
                  SELECT t0.v2 + t1.v5 AS combo
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                ) sub
                JOIN t2 ON sub.combo = t2.v8
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testAssociativityCSECaseWhenOnJoin() throws Exception {
        String sql = """
                SELECT bucket + 1, bucket * 3,
                       CASE bucket WHEN 1 THEN 'low' WHEN 2 THEN 'mid' ELSE 'high' END
                FROM (
                  SELECT CASE WHEN t0.v1 > t1.v4 THEN 1
                              WHEN t0.v1 = t1.v4 THEN 2
                              ELSE 3 END AS bucket
                  FROM t0 JOIN t1 ON t0.v2 = t1.v5
                ) sub
                JOIN t2 ON sub.bucket = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // Three-table inner join with CSE across all three tables.
    // The optimizer may explore associativity: (t0 ⋈ t1) ⋈ t2
    // -----------------------------------------------------------------------

    @Test
    public void testThreeTableInnerJoinCSEAcrossAllTables() throws Exception {
        String sql = """
                SELECT score + 1, score * 2 FROM (
                  SELECT t0.v1 + t1.v5 + t2.v9 AS score
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testThreeTableInnerJoinCSEFromTwoTables() throws Exception {
        String sql = """
                SELECT product + 1, product * 2, product - 3 FROM (
                  SELECT t0.v2 * t1.v5 AS product
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t0.v1 = t2.v7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // Four-table joins: more exploration surface for associativity/left-asscom
    // -----------------------------------------------------------------------

    @Test
    public void testFourTableJoinCSEUsedTwice() throws Exception {
        String sql = """
                SELECT x + 1, x * 2 FROM (
                  SELECT t0.v1 + t1.v5 AS x
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                  JOIN t3 ON t2.v7 = t3.v10
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
        assertContains(plan, "TABLE: t0");
        assertContains(plan, "TABLE: t3");
    }

    @Test
    public void testFourTableJoinCSECaseWhen() throws Exception {
        String sql = """
                SELECT bucket, bucket + 100,
                       CASE bucket WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END
                FROM (
                  SELECT CASE WHEN t0.v1 + t1.v4 > 100 THEN 1
                              WHEN t2.v7 + t3.v10 > 100 THEN 2
                              ELSE 3 END AS bucket
                  FROM t0
                  JOIN t1 ON t0.v2 = t1.v5
                  JOIN t2 ON t1.v4 = t2.v7
                  JOIN t3 ON t2.v8 = t3.v11
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // Left-asscom: (t0 ⋈ t1) ⋈ t2  →  (t0 ⋈ t2) ⋈ t1
    // Using LEFT/SEMI joins that trigger left-asscom rather than associativity
    // -----------------------------------------------------------------------

    @Test
    public void testLeftJoinAssociativityCSEUsedTwice() throws Exception {
        String sql = """
                SELECT val + 1, val * 2 FROM (
                  SELECT COALESCE(t0.v2 + t1.v5, 0) AS val
                  FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4
                ) sub
                JOIN t2 ON sub.val = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
    }

    @Test
    public void testLeftJoinThreeTableCSE() throws Exception {
        String sql = """
                SELECT combo + 1, combo * 3 FROM (
                  SELECT COALESCE(t0.v2, 0) + COALESCE(t1.v5, 0) AS combo
                  FROM t0
                  LEFT JOIN t1 ON t0.v1 = t1.v4
                  LEFT JOIN t2 ON t0.v1 = t2.v7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
    }

    @Test
    public void testSemiJoinWithCSEOnOuterJoin() throws Exception {
        String sql = """
                SELECT total + 1, total * 2 FROM (
                  SELECT t0.v1 + t0.v2 AS total
                  FROM t0
                  WHERE t0.v1 IN (SELECT t1.v4 FROM t1)
                ) sub
                JOIN t2 ON sub.total = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
    }

    // -----------------------------------------------------------------------
    // Mixed join types: inner + left/right
    // -----------------------------------------------------------------------

    @Test
    public void testMixedInnerLeftJoinCSE() throws Exception {
        String sql = """
                SELECT d + 1, d * 2, d - 5 FROM (
                  SELECT COALESCE(t0.v2, 0) + COALESCE(t2.v8, 0) AS d
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  LEFT JOIN t2 ON t1.v6 = t2.v7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
    }

    @Test
    public void testMixedInnerRightJoinCSE() throws Exception {
        String sql = """
                SELECT r + 10, r * 3 FROM (
                  SELECT COALESCE(t0.v2, 0) + t1.v5 AS r
                  FROM t0
                  RIGHT JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
    }

    // -----------------------------------------------------------------------
    // CTE over multi-table join: CSE gets merged into the join's projection,
    // then join reordering fires during exploration
    // -----------------------------------------------------------------------

    @Test
    public void testCTEOverThreeTableJoinCSEUsedTwice() throws Exception {
        String sql = """
                WITH base AS (
                  SELECT t0.v1 * t1.v5 AS product, t2.v9
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                )
                SELECT product + 10, product * 5, product - v9
                FROM base
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testCTEOverFourTableJoinCSECaseWhen() throws Exception {
        String sql = """
                WITH ranked AS (
                  SELECT t0.v1, t1.v5, t2.v9,
                    CASE
                      WHEN t0.v1 > t1.v5 THEN 1
                      WHEN t0.v1 > t2.v9 THEN 2
                      ELSE 3
                    END AS rank_bucket
                  FROM t0
                  JOIN t1 ON t0.v2 = t1.v5
                  JOIN t2 ON t1.v6 = t2.v8
                  JOIN t3 ON t2.v7 = t3.v10
                )
                SELECT
                  rank_bucket,
                  rank_bucket + 10 AS shifted,
                  CASE rank_bucket WHEN 1 THEN 'top'
                                   WHEN 2 THEN 'mid'
                                   ELSE 'low' END AS tier
                FROM ranked
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // Nested subquery over join: two project merges creating a deeper
    // commonSubOperatorMap chain, followed by join reorder
    // -----------------------------------------------------------------------

    @Test
    public void testNestedSubqueryOverJoinCSE() throws Exception {
        String sql = """
                SELECT z + 1, z * 2 FROM (
                  SELECT y + 100 AS z FROM (
                    SELECT t0.v1 * t1.v5 AS y
                    FROM t0 JOIN t1 ON t0.v1 = t1.v4
                  ) inner_sub
                ) outer_sub
                JOIN t2 ON outer_sub.z = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testTwoLevelNestedSubqueryOverThreeTableJoin() throws Exception {
        String sql = """
                SELECT w + 1, w * 2, w - 10 FROM (
                  SELECT x + y AS w FROM (
                    SELECT t0.v1 + t1.v5 AS x, t2.v9 AS y
                    FROM t0
                    JOIN t1 ON t0.v1 = t1.v4
                    JOIN t2 ON t1.v4 = t2.v7
                  ) q1
                ) q2
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // CSE in join ON condition context: derived column from inner join used
    // in the ON condition of a subsequent join
    // -----------------------------------------------------------------------

    @Test
    public void testCSEInJoinOnCondition() throws Exception {
        String sql = """
                SELECT sub.s, t2.v8 FROM (
                  SELECT t0.v1, t0.v2 + t1.v5 AS s
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                ) sub
                JOIN t2 ON sub.s = t2.v7 AND sub.s > t2.v8
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // CSE in WHERE predicate combined with join reorder
    // -----------------------------------------------------------------------

    @Test
    public void testCSEInWherePredicateWithJoinReorder() throws Exception {
        String sql = """
                SELECT total FROM (
                  SELECT t0.v1 + t1.v5 AS total, t2.v9
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                ) sub
                WHERE total > 100 AND total < 1000
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // CSE with aggregate on top of a join that may be reordered
    // -----------------------------------------------------------------------

    @Test
    public void testCSEWithAggOverReorderableJoin() throws Exception {
        String sql = """
                SELECT bucket, count(*), sum(v9) FROM (
                  SELECT CASE WHEN t0.v1 + t1.v5 > 100 THEN 1 ELSE 2 END AS bucket, t2.v9
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                ) sub
                GROUP BY bucket
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
        assertContains(plan, "AGGREGATE");
    }

    // -----------------------------------------------------------------------
    // CSE with function calls (abs, coalesce, etc.) on join result
    // -----------------------------------------------------------------------

    @Test
    public void testCSEAbsFunctionOnJoinResultWithReorder() throws Exception {
        String sql = """
                SELECT abs_diff + 1, abs_diff * 2 FROM (
                  SELECT abs(t0.v2 - t1.v5) AS abs_diff
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                ) sub
                JOIN t2 ON sub.abs_diff = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
    }

    @Test
    public void testCSECoalesceOnJoinResultWithReorder() throws Exception {
        String sql = """
                SELECT c + 1, c * 5 FROM (
                  SELECT COALESCE(t0.v2, t1.v5, 0) AS c
                  FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4
                ) sub
                JOIN t2 ON sub.c = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
    }

    // -----------------------------------------------------------------------
    // Multiple CSEs from the same join, each used 2+ times
    // -----------------------------------------------------------------------

    @Test
    public void testMultipleCSEsFromSameJoinReorder() throws Exception {
        String sql = """
                SELECT a + b, a - b, a * b FROM (
                  SELECT t0.v1 + t1.v4 AS a, t0.v2 * t1.v5 AS b
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                ) sub
                JOIN t2 ON sub.a = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testMultipleCSEsInCTEOverJoinReorder() throws Exception {
        String sql = """
                WITH base AS (
                  SELECT t0.v1 + t1.v4 AS sum_val,
                         t0.v2 * t1.v5 AS prod_val,
                         t2.v9
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                )
                SELECT sum_val + prod_val, sum_val - prod_val,
                       sum_val * 2, prod_val * 3
                FROM base
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // CSE where the derived column is used in both SELECT and the ON
    // condition of a subsequent join (split across top/bot join)
    // -----------------------------------------------------------------------

    @Test
    public void testCSEUsedInSelectAndSubsequentJoinOn() throws Exception {
        String sql = """
                SELECT sub.derived_col + 1, sub.derived_col * 2, t2.v8
                FROM (
                  SELECT t0.v1, t0.v2 * t1.v5 AS derived_col
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                ) sub
                JOIN t2 ON sub.derived_col = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // CSE with CAST on join columns — exercises the JoinOnConditionShuttle
    // -----------------------------------------------------------------------

    @Test
    public void testCSEWithCastOnJoinColumnsReorder() throws Exception {
        String sql = """
                SELECT d + 1.0, d * 2.0 FROM (
                  SELECT CAST(t0.v1 AS DOUBLE) + CAST(t1.v5 AS DOUBLE) AS d
                  FROM t0 JOIN t1 ON t0.v1 = t1.v4
                ) sub
                JOIN t2 ON CAST(sub.d AS BIGINT) = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
    }

    // -----------------------------------------------------------------------
    // CSE with join reorder disabled: baseline to compare against
    // -----------------------------------------------------------------------

    @Test
    public void testCSEOverJoinWithReorderDisabled() throws Exception {
        connectContext.getSessionVariable().disableJoinReorder();
        try {
            String sql = """
                    SELECT product + 1, product * 2 FROM (
                      SELECT t0.v1 * t1.v5 AS product
                      FROM t0
                      JOIN t1 ON t0.v1 = t1.v4
                      JOIN t2 ON t1.v4 = t2.v7
                    ) sub
                    """;

            String plan = getFragmentPlan(sql);
            printSqlAndPlan(sql, plan);
            assertContains(plan, "HASH JOIN");
        } finally {
            connectContext.getSessionVariable().enableJoinReorder();
        }
    }

    // -----------------------------------------------------------------------
    // Cross join promoted to inner join when CSE becomes ON condition
    // -----------------------------------------------------------------------

    @Test
    public void testCSEOnCrossJoinPromotedToInner() throws Exception {
        String sql = """
                SELECT x + 1, x * 2 FROM (
                  SELECT t0.v1 + t0.v2 AS x, t1.v4
                  FROM t0, t1
                  WHERE t0.v1 = t1.v4
                ) sub
                JOIN t2 ON sub.x = t2.v7
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
    }

    // -----------------------------------------------------------------------
    // CSE constant expression on a join projection (constCols bucket in
    // ProjectionSplitter)
    // -----------------------------------------------------------------------

    @Test
    public void testCSEConstantExprOnJoinProjection() throws Exception {
        String sql = """
                SELECT c + v9, c * 2, c - v9 FROM (
                  SELECT 42 AS c, t2.v9
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // CSE on join with ORDER BY + LIMIT: verifies plan survives the
    // full optimization pipeline
    // -----------------------------------------------------------------------

    @Test
    public void testCSEOnJoinWithOrderByLimit() throws Exception {
        String sql = """
                SELECT s + 1, s * 2 FROM (
                  SELECT t0.v1 + t1.v5 AS s
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                ) sub
                ORDER BY s
                LIMIT 10
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testCSEOnJoinWithOrderByLimit1() throws Exception {
        String sql = """
                SELECT a + 1, a * 2 FROM (
                  SELECT t0.v1 + t1.v5 AS a, t0.v2 AS b
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                ) sub
                ORDER BY b
                LIMIT 10
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "HASH JOIN");
    }

    // -----------------------------------------------------------------------
    // CSE involving window function partitioned by a derived column
    // from a multi-table join
    // -----------------------------------------------------------------------

    @Test
    public void testCSEWindowFunctionOverJoinReorder() throws Exception {
        String sql = """
                SELECT bucket, v9,
                       sum(v9) OVER (PARTITION BY bucket ORDER BY v9) AS wsum
                FROM (
                  SELECT CASE WHEN t0.v1 + t1.v5 > 50 THEN 1 ELSE 2 END AS bucket,
                         t2.v9
                  FROM t0
                  JOIN t1 ON t0.v1 = t1.v4
                  JOIN t2 ON t1.v4 = t2.v7
                ) sub
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "JOIN");
        assertContains(plan, "ANALYTIC");
    }

    @Test
    public void testEliminateAggFunctionRule() throws Exception {
        String sql = """
                SELECT max(v1) FROM t0 GROUP BY v1
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "TABLE: t0");
    }

    // -----------------------------------------------------------------------
    // HoistHeavyCostExprsUponTopnRule: hoist DECIMAL128/LARGEINT division
    // above TopN so the heavy expr is computed only on the limited result.
    // -----------------------------------------------------------------------

    @Test
    public void testHoistHeavyCostExprsUponTopnRule() throws Exception {
        // TopN (ORDER BY k LIMIT 10) over Project(k, d1, d2, d1/d2). The division
        // d1/d2 is DECIMAL128 and not used in ORDER BY, so the rule hoists it
        // above TopN (computed after limit).
        String sql = """
                SELECT k, d1, d2, d1 / d2
                FROM t_hoist_heavy
                ORDER BY k
                LIMIT 10
                """;

        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "table: t_hoist_heavy");
        assertContains(plan, "limit: 10");
        // When the rule fires, the Project that outputs the division sits above
        // the TopN and has "limit: 10" in its fragment (division computed after limit).
        assertContains(plan, "/");
        assertContains(plan, "DECIMAL128");
    }

    // -----------------------------------------------------------------------
    // Join with scalar subquery in WHERE correlating with both t0 and t1
    // -----------------------------------------------------------------------

    @Test
    public void testScalarSubquery() throws Exception {
        String sql = """
                SELECT * FROM t0 JOIN t1
                WHERE v1 + v4 = (
                  SELECT max(t1d) FROM test_all_type
                  WHERE t1c = 1
                    AND t1c + t0.v2 = t0.v1 + t1c
                    AND t1d + t0.v3 = t1.v5 + t1d
                )
                """;

        String plan = getFragmentPlan(sql);
        printSqlAndPlan(sql, plan);
        assertContains(plan, "TABLE: t0");
        assertContains(plan, "TABLE: t1");
        assertContains(plan, "test_all_type");
    }
}
