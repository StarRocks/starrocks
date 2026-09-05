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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests MV rewrite when the MV's GROUP BY key is a CASE WHEN expression and
 * the query filters on that CASE WHEN expression. The original query predicate is
 * retained for MV rewrite while the execution predicate remains independently optimized.
 */
public class MvRewriteCaseWhenGroupByTest extends MVTestBase {

    private static final String CREATE_TABLE =
            "CREATE TABLE `test_pt3` (\n" +
            "  `id` int(11) NULL COMMENT \"id\",\n" +
            "  `id2` int(11) NULL COMMENT \"id2\",\n" +
            "  `id3` int(11) NULL COMMENT \"id3\",\n" +
            "  `pt` date NOT NULL COMMENT \"\",\n" +
            "  `gmv` double NULL COMMENT \"gmv\"\n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY(`id`)\n" +
            "PARTITION BY date_trunc('day', pt)\n" +
            "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\"\n" +
            ");";

    // MV with CASE WHEN as GROUP BY key — no partition (simpler, avoids partition refresh issues in UT)
    private static final String CREATE_MV_CASE_WHEN =
            "CREATE MATERIALIZED VIEW `test_pt3_mv`\n" +
            "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
            "REFRESH ASYNC\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\"\n" +
            ")\n" +
            "AS SELECT `pt`, `id2`,\n" +
            "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END AS `is_flag`,\n" +
            "  CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END AS `is_flag2`,\n" +
            "  CASE WHEN (abs(`id3`) = 1) THEN 'p' WHEN (abs(`id3`) = 2) THEN 'q' ELSE 'r' END AS `is_flag3`,\n" +
            "  SUM(`gmv`) AS `sum_gmv`\n" +
            "FROM `test_pt3`\n" +
            "GROUP BY `pt`, `id2`,\n" +
            "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END,\n" +
            "  CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END,\n" +
            "  CASE WHEN (abs(`id3`) = 1) THEN 'p' WHEN (abs(`id3`) = 2) THEN 'q' ELSE 'r' END;";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable(CREATE_TABLE);
        executeInsertSql(connectContext, "insert into test_pt3 values(1,10,1,'2025-12-12',1)");
        executeInsertSql(connectContext, "insert into test_pt3 values(2,20,2,'2025-12-12',1)");
        createAndRefreshMv(CREATE_MV_CASE_WHEN);
    }

    @AfterAll
    public static void afterClass() throws Exception {
        try {
            dropMv(DB_NAME, "test_pt3_mv");
        } catch (Exception ignored) {
        }
        try {
            starRocksAssert.dropTable("test_pt3");
        } catch (Exception ignored) {
        }
    }

    /**
     * Query:  CASE WHEN (id=1) THEN 'a' ... END IN ('a')
     * The preserved CASE expression maps to the MV group-by column is_flag.
     */
    @Test
    public void testCaseWhenEqHitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END IN ('a')\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query, "MV");
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag = 'a'");
    }

    /**
     * Query:  CASE WHEN (id=1) THEN 'a' WHEN (id=2) THEN 'b' ... END IN ('b')
     * The preserved CASE expression maps to the MV group-by column is_flag.
     */
    @Test
    public void testCaseWhenInSingleValueHitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END IN ('b')\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag = 'b'");
    }

    /**
     * Query:  CASE WHEN (id=1) THEN 'a' WHEN (id=2) THEN 'b' ... END IN ('a', 'b')
     * The preserved CASE expression maps to the MV group-by column is_flag.
     */
    @Test
    public void testCaseWhenInTwoValuesHitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END IN ('a', 'b')\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag IN ('a','b'");
    }

    /**
     * Query:  CASE WHEN (id=1) THEN 'a' WHEN (id=2) THEN 'b' ELSE 'c' END != 'a'
     * Regression guard for != and NOT IN: unlike = and IN, simplifyCaseWhen does NOT
     * expand these forms (the InvertedCaseWhen.simplify branch hits a guard around
     * line 404 that returns Optional.empty when branchToNull is present), so the
     * residualPredicate keeps the full CASE WHEN...END != 'a' expression. The MV
     * rewrite then goes through EquationRewriter's generic CASE WHEN → MV scan
     * column mapping (mv.is_flag), producing the compensation is_flag != 'a'.
     *
     * <p>This verifies the generic EquationRewriter path for != and NOT IN.
     */
    @Test
    public void testCaseWhenNotEqHitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END != 'a'\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query, "MV");
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag != 'a'");
    }

    /**
     * Query:  CASE WHEN (id=1) THEN 'a' WHEN (id=2) THEN 'b' ELSE 'c' END NOT IN ('a', 'b')
     * Same code path as {@link #testCaseWhenNotEqHitsMV}: simplifyCaseWhen does not
     * expand NOT IN (same branchToNull guard), so the residualPredicate keeps both
     * full CASE WHEN...END != 'a' and CASE WHEN...END != 'b'. EquationRewriter
     * produces is_flag != 'a' AND is_flag != 'b' as the compensation.
     */
    @Test
    public void testCaseWhenNotInHitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END NOT IN ('a', 'b')\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query, "MV");
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag != 'a'");
        PlanTestBase.assertContains(plan, "is_flag != 'b'");
    }

    /**
     * Query:  CASE WHEN (id=1) THEN 'a' WHEN (id=2) THEN 'b' ELSE 'c' END IN ('c')
     * EquationRewriter maps the preserved CASE WHEN expression to is_flag = 'c'.
     */
    @Test
    public void testCaseWhenInSingleValueHitsMV2() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END IN ('c')\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag = 'c'");
    }

    /**
     * Query:  CASE WHEN (id=1) THEN 'a' WHEN (id=2) THEN 'b' ELSE 'c' END IN ('invalid')
     * The execution predicate simplifies to FALSE, producing an EMPTYSET plan.
     */
    @Test
    public void testCaseWhenInInvalidValueNoRewrite() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END IN ('invalid')\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "EMPTYSET");
    }

    /**
     * Query: id = 99 (no CASE WHEN predicate).
     * Raw id cannot map to an MV output column, so rewrite falls back to the base table.
     */
    @Test
    public void testNonCaseWhenFilterNoRewrite() throws Exception {
        String query = "SELECT sum(gmv) FROM test_pt3 WHERE id = 99 GROUP BY pt;";
        String plan = getFragmentPlan(query);
        Assertions.assertNotNull(plan);
        PlanTestBase.assertNotContains(plan, "test_pt3_mv");
    }

    /**
     * Query: CASE WHEN ... END = 'b' AND id2 = 20.
     * EquationRewriter maps both predicates to MV output columns.
     */
    @Test
    public void testCaseWhenAndId2HitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'b'\n" +
                "  AND id2 = 20\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag = 'b'");
    }

    /**
     * Query: CASE WHEN ... END = 'b' OR id2 = 20.
     * EquationRewriter maps the preserved OR expression to the MV-side form
     * (is_flag = 'b' OR id2 = 20), which is a valid
     * compensation predicate since both is_flag and id2 are MV GROUP BY keys.
     */
    @Test
    public void testCaseWhenOrId2HitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'b'\n" +
                "  OR id2 = 20\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query, "MV");
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "id2 = 20");
        PlanTestBase.assertContains(plan, "is_flag = 'b'");
        PlanTestBase.assertContains(plan, " OR ");
    }

    /**
     * Query:  CASE WHEN (id=1) THEN 'a' WHEN (id=2) THEN 'b' ELSE 'c' END = 'c'
     *     AND CASE WHEN (id=3) THEN 'x' ELSE 'y' END = 'x'
     *
     * Both preserved CASE WHEN expressions map independently to their MV output columns.
     */
    @Test
    public void testTwoCaseWhenBothHitMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'c'\n" +
                "  AND CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END = 'x'\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag = 'c'");
        PlanTestBase.assertContains(plan, "is_flag2 = 'x'");
    }

    /**
     * Query:  CASE WHEN (id=3) THEN 'x' ELSE 'y' END = 'x'
     * The normalized single-WHEN expression maps directly to is_flag2 = 'x'.
     */
    @Test
    public void testSingleWhenSimplifiedToIfHitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END = 'x'\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag2 = 'x'");
    }

    /**
     * Query: CASE WHEN (abs(id3)=1) THEN 'p' WHEN (abs(id3)=2) THEN 'q' ELSE 'r' END = 'p'.
     * The preserved CASE expression maps to is_flag3 = 'p'.
     */
    @Test
    public void testCaseWhenWithFunctionInConditionHitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (abs(`id3`) = 1) THEN 'p' WHEN (abs(`id3`) = 2) THEN 'q' ELSE 'r' END = 'p'\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        PlanTestBase.assertContains(plan, "is_flag3 = 'p'");
    }

    /**
     * Query: id2 = 20 (no CASE WHEN predicate).
     * EquationRewriter maps id2 directly to the MV output column.
     */
    @Test
    public void testOnlyId2FilterHitsMV() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE id2 = 20\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "test_pt3_mv");
    }

    /**
     * Regression test for Set-based conjunct consumption correctness.
     *
     * When MV has multiple CASE WHEN keys whose when-conditions share the same
     * predicates on both CASE WHEN expressions must map independently even when their
     * branch conditions overlap.
     */
    @Test
    public void testSharedConjunctMultipleCaseWhenKeys() throws Exception {
        // Create a separate MV where two CASE WHEN keys share the "id = 1" condition.
        String createMvShared =
                "CREATE MATERIALIZED VIEW `test_pt3_mv_shared`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END AS `is_flag`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'p' WHEN (`id3` = 2) THEN 'q' ELSE 'r' END AS `is_flag3_shared`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_pt3`\n" +
                "GROUP BY `pt`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END,\n" +
                "  CASE WHEN (`id` = 1) THEN 'p' WHEN (`id3` = 2) THEN 'q' ELSE 'r' END;";
        createAndRefreshMv(createMvShared);

        try {
            // Query filters on both CASE WHEN columns with '='.
            // Both expand to "id = 1" (shared conjunct).
            String query =
                    "SELECT sum(gmv) FROM test_pt3\n" +
                    "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'a'\n" +
                    "  AND CASE WHEN (`id` = 1) THEN 'p' WHEN (`id3` = 2) THEN 'q' ELSE 'r' END = 'p'\n" +
                    "GROUP BY pt;";
            String plan = getFragmentPlan(query, "MV");

            // The MV must be hit.
            PlanTestBase.assertContains(plan, "test_pt3_mv_shared");

            // BOTH compensation predicates must be present in the plan.
            // If the shared conjunct bug exists, only one would be present.
            PlanTestBase.assertContains(plan, "is_flag = 'a'");
            PlanTestBase.assertContains(plan, "is_flag3_shared = 'p'");
        } finally {
            dropMv(DB_NAME, "test_pt3_mv_shared");
        }
    }

    /**
     * Regression for consuming a shared OR conjunct exactly once (uses a dedicated table
     * so the two-key MV is the only candidate, free of main-fixture-MV interference).
     *
     * <p>Both CASE WHEN keys share the id=1 branch: {@code cw_is_flag} (id=1->'a') and
     * {@code cw_is_flag2} (id=1->'x'). The preserved OR predicate must map once to
     * MV output columns without duplicating its {@code aux=20} disjunct.
     */
    @Test
    public void testSharedOrConjunctConsumedOnce() throws Exception {
        String createTable =
                "CREATE TABLE `test_cw_or_tbl` (\n" +
                "  `id` int(11) NULL COMMENT \"id\",\n" +
                "  `aux` int(11) NULL COMMENT \"aux\",\n" +
                "  `pt` date NOT NULL COMMENT \"\",\n" +
                "  `gmv` double NULL COMMENT \"gmv\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        // aux is a plain group-by key, so every OR disjunct can map to an MV output column.
        String createMv =
                "CREATE MATERIALIZED VIEW `test_cw_or_mv`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  `aux`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END AS `cw_is_flag`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'x' ELSE 'y' END AS `cw_is_flag2`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_cw_or_tbl`\n" +
                "GROUP BY `pt`,\n" +
                "  `aux`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END,\n" +
                "  CASE WHEN (`id` = 1) THEN 'x' ELSE 'y' END;";

        starRocksAssert.withTable(createTable);
        // Enough rows that the aggregated MV scan beats the base scan in cost.
        for (int id = 1; id <= 8; id++) {
            executeInsertSql(connectContext,
                    "insert into test_cw_or_tbl values(" + id + "," + (id * 10) + ",'2025-12-12'," + id + ")");
        }
        createAndRefreshMv(createMv);

        try {
            String query =
                    "SELECT sum(gmv) FROM test_cw_or_tbl\n" +
                    "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'a'\n" +
                    "  OR aux = 20\n" +
                    "GROUP BY pt;";
            String plan = planForceMv(query);

            // MV must be hit.
            PlanTestBase.assertContains(plan, "test_cw_or_mv");
            // The preserved OR must map once, so aux = 20 appears once.
            Assertions.assertEquals(1,
                    countOccurrences(plan, "aux = 20"),
                    "aux = 20 should appear exactly once (source OR consumed once)");
        } finally {
            try {
                dropMv(DB_NAME, "test_cw_or_mv");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("test_cw_or_tbl");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Cross-CASE-key OR: each OR disjunct maps to a distinct CASE WHEN group-by key.
     * The preserved expression maps to {@code is_flag='a' OR is_flag2='x'} on the MV scan.
     */
    @Test
    public void testCrossKeyOrHitsMv() throws Exception {
        String query =
                "SELECT sum(gmv) FROM test_pt3\n" +
                "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'a'\n" +
                "  OR CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END = 'x'\n" +
                "GROUP BY pt;";
        String plan = getFragmentPlan(query, "MV");
        // The preserved cross-key OR predicate must be mapped to the MV scan.
        PlanTestBase.assertContains(plan, "test_pt3_mv");
        // Both CASE keys' compensation predicates appear, joined by OR.
        PlanTestBase.assertContains(plan, "is_flag = 'a'");
        PlanTestBase.assertContains(plan, "is_flag2 = 'x'");
        PlanTestBase.assertContains(plan, " OR ");
    }

    /**
     * Cross-key OR with a residual conjunct inside one disjunct. The preserved predicate must keep
     * the residual ({@code id2 = 20}) inside its original disjunct and
     *
     * <p>Query: {@code (CASE1='a' AND id2=20) OR CASE2='x'} must yield
     * {@code (mv.flag1='a' AND mv.id2=20) OR mv.flag2='x'}, not the relaxed
     * {@code mv.flag1='a' OR mv.flag2='x'} (which would drop the id2 filter and over-count).
     */
    @Test
    public void testCrossKeyOrWithResidualConjunctHitsMv() throws Exception {
        String createTable =
                "CREATE TABLE `test_cw_cross_tbl` (\n" +
                "  `id` int(11) NULL,\n" +
                "  `id2` int(11) NULL,\n" +
                "  `pt` date NOT NULL,\n" +
                "  `gmv` double NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");";
        String createMv =
                "CREATE MATERIALIZED VIEW `test_cw_cross_mv`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  `id2`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END AS `cw_flag1`,\n" +
                "  CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END AS `cw_flag2`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_cw_cross_tbl`\n" +
                "GROUP BY `pt`,\n" +
                "  `id2`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END,\n" +
                "  CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END;";
        starRocksAssert.withTable(createTable);
        for (int id = 1; id <= 8; id++) {
            executeInsertSql(connectContext,
                    "insert into test_cw_cross_tbl values(" + id + "," + (id * 10) + ",'2025-12-12'," + id + ")");
        }
        createAndRefreshMv(createMv);
        try {
            String query =
                    "SELECT sum(gmv) FROM test_cw_cross_tbl\n" +
                    "WHERE (\n" +
                    "    CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'a'\n" +
                    "    AND id2 = 20\n" +
                    "  )\n" +
                    "  OR CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END = 'x'\n" +
                    "GROUP BY pt;";
            String plan = planForceMv(query);
            PlanTestBase.assertContains(plan, "test_cw_cross_mv");
            // id2 = 20 must stay inside the first disjunct (AND with cw_flag1='a'), not pulled
            // out to the top level — otherwise the OR filter is relaxed.
            PlanTestBase.assertContains(plan, "id2 = 20");
            PlanTestBase.assertContains(plan, "cw_flag1 = 'a'");
            PlanTestBase.assertContains(plan, "cw_flag2 = 'x'");
        } finally {
            try {
                dropMv(DB_NAME, "test_cw_cross_mv");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("test_cw_cross_tbl");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Single-CASE-key OR with a residual conjunct inside one disjunct. Expression mapping must not
     * drop the residual {@code id2 = 20}.
     *
     * <p>Query: {@code (CASE1='a' AND id2=20) OR id2=30} must yield
     * {@code (mv.flag1='a' AND mv.id2=20) OR mv.id2=30}, not the relaxed
     * {@code mv.flag1='a' OR mv.id2=30}.
     */
    @Test
    public void testSingleKeyOrWithResidualConjunctHitsMv() throws Exception {
        String createTable =
                "CREATE TABLE `test_cw_single_or_tbl` (\n" +
                "  `id` int(11) NULL,\n" +
                "  `id2` int(11) NULL,\n" +
                "  `pt` date NOT NULL,\n" +
                "  `gmv` double NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");";
        String createMv =
                "CREATE MATERIALIZED VIEW `test_cw_single_or_mv`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  `id2`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END AS `cw_flag1`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_cw_single_or_tbl`\n" +
                "GROUP BY `pt`,\n" +
                "  `id2`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END;";
        starRocksAssert.withTable(createTable);
        for (int id = 1; id <= 8; id++) {
            executeInsertSql(connectContext,
                    "insert into test_cw_single_or_tbl values(" + id + "," + (id * 10) + ",'2025-12-12'," + id + ")");
        }
        createAndRefreshMv(createMv);
        try {
            String query =
                    "SELECT sum(gmv) FROM test_cw_single_or_tbl\n" +
                    "WHERE (\n" +
                    "    CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'a'\n" +
                    "    AND id2 = 20\n" +
                    "  )\n" +
                    "  OR id2 = 30\n" +
                    "GROUP BY pt;";
            String plan = planForceMv(query);
            PlanTestBase.assertContains(plan, "test_cw_single_or_mv");
            PlanTestBase.assertContains(plan, "cw_flag1 = 'a'");
            PlanTestBase.assertContains(plan, "id2 = 20");
            PlanTestBase.assertContains(plan, "id2 = 30");
        } finally {
            try {
                dropMv(DB_NAME, "test_cw_single_or_mv");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("test_cw_single_or_tbl");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Cross-key OR with an unmappable residual column (not in the MV) must not be partially
     * rewritten. EquationRewriter rejects the expression, so the query falls back to the base table.
     *
     * <p>This guards against a relaxed compensation predicate that mixes MV-scan colrefs
     * with query/base colrefs.
     */
    @Test
    public void testCrossKeyOrUnmappableResidualFallsBackToBase() throws Exception {
        String createTable =
                "CREATE TABLE `test_cw_cross_tbl` (\n" +
                "  `id` int(11) NULL,\n" +
                "  `extra` int(11) NULL,\n" +
                "  `pt` date NOT NULL,\n" +
                "  `gmv` double NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");";
        // extra is NOT in the MV.
        String createMv =
                "CREATE MATERIALIZED VIEW `test_cw_cross_mv`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END AS `cw_flag1`,\n" +
                "  CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END AS `cw_flag2`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_cw_cross_tbl`\n" +
                "GROUP BY `pt`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END,\n" +
                "  CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END;";
        starRocksAssert.withTable(createTable);
        for (int id = 1; id <= 8; id++) {
            executeInsertSql(connectContext,
                    "insert into test_cw_cross_tbl values(" + id + "," + (id * 10) + ",'2025-12-12'," + id + ")");
        }
        createAndRefreshMv(createMv);
        try {
            // extra is not in the MV; the residual (CASE1='a' AND extra=20) cannot be mapped,
            // so the OR must fall back rather than emit a mixed compensation predicate.
            String query =
                    "SELECT sum(gmv) FROM test_cw_cross_tbl\n" +
                    "WHERE (\n" +
                    "    CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'a'\n" +
                    "    AND extra = 20\n" +
                    "  )\n" +
                    "  OR CASE WHEN (`id` = 3) THEN 'x' ELSE 'y' END = 'x'\n" +
                    "GROUP BY pt;";
            String plan = planForceMv(query);
            // MV rewrite must miss (no partial rewrite with the unmappable extra column).
            PlanTestBase.assertNotContains(plan, "test_cw_cross_mv");
        } finally {
            try {
                dropMv(DB_NAME, "test_cw_cross_mv");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("test_cw_cross_tbl");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * OR disjunct mixing a mappable column and a non-mappable one must NOT be partially
     * rewritten. Here {@code aux} is an MV group-by key but {@code id} is not in the MV;
     * {@code aux = id} cannot be expressed purely on MV-scan columns.
     *
     * <p>Before the fix: replaceColumnRefsForMV mapped {@code aux} but left {@code id}
     * as a query colref, producing a mixed compensation predicate referencing a column the
     * MV scan does not produce. The generic all-expressions-replaced check must reject it.
     */
    @Test
    public void testMixedColumnOrDisjunctFallsBackToBase() throws Exception {
        String createTable =
                "CREATE TABLE `test_cw_or_tbl` (\n" +
                "  `id` int(11) NULL COMMENT \"id\",\n" +
                "  `aux` int(11) NULL COMMENT \"aux\",\n" +
                "  `pt` date NOT NULL COMMENT \"\",\n" +
                "  `gmv` double NULL COMMENT \"gmv\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        // aux is an MV group-by key; id is NOT in the MV.
        String createMv =
                "CREATE MATERIALIZED VIEW `test_cw_or_mv`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  `aux`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END AS `cw_is_flag`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_cw_or_tbl`\n" +
                "GROUP BY `pt`,\n" +
                "  `aux`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END;";
        starRocksAssert.withTable(createTable);
        for (int id = 1; id <= 8; id++) {
            executeInsertSql(connectContext,
                    "insert into test_cw_or_tbl values(" + id + "," + (id * 10) + ",'2025-12-12'," + id + ")");
        }
        createAndRefreshMv(createMv);

        try {
            // aux is in the MV, id is not. Plan generation must not NPE; the MV rewrite
            // must fall back to the base scan rather than emit a mixed compensation predicate.
            String query =
                    "SELECT sum(gmv) FROM test_cw_or_tbl\n" +
                    "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'a'\n" +
                    "  OR aux = id\n" +
                    "GROUP BY pt;";
            String plan = planForceMv(query);
            // Must fall back to the base table scan (MV rewrite fails because aux=id
            // cannot be expressed on MV-scan columns).
            PlanTestBase.assertNotContains(plan, "test_cw_or_mv");
        } finally {
            try {
                dropMv(DB_NAME, "test_cw_or_mv");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("test_cw_or_tbl");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * CASE WHEN without ELSE has an implicit NULL branch. Preserving the original expression
     * allows its selected non-NULL branch to map to the MV CASE key without changing NULL semantics.
     */
    @Test
    public void testCaseWhenWithoutElseNullBranchHitsMv() throws Exception {
        String createTable =
                "CREATE TABLE `test_cw_null_tbl` (\n" +
                "  `id` int(11) NULL COMMENT \"id\",\n" +
                "  `aux` int(11) NULL COMMENT \"aux\",\n" +
                "  `pt` date NOT NULL COMMENT \"\",\n" +
                "  `gmv` double NULL COMMENT \"gmv\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        String createMv =
                "CREATE MATERIALIZED VIEW `test_cw_null_mv`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  `aux`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' END AS `cw_flag`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_cw_null_tbl`\n" +
                "GROUP BY `pt`,\n" +
                "  `aux`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' END;";
        starRocksAssert.withTable(createTable);
        for (int id = 1; id <= 8; id++) {
            executeInsertSql(connectContext,
                    "insert into test_cw_null_tbl values(" + id + "," + (id * 10) + ",'2025-12-12'," + id + ")");
        }
        createAndRefreshMv(createMv);

        try {
            String query =
                    "SELECT sum(gmv) FROM test_cw_null_tbl\n" +
                    "WHERE CASE WHEN (`id` = 1) THEN 'a' END = 'a'\n" +
                    "GROUP BY pt;";
            String plan = planForceMv(query);
            PlanTestBase.assertContains(plan, "test_cw_null_mv");
            PlanTestBase.assertContains(plan, "cw_flag = 'a'");

            String orQuery =
                    "SELECT sum(gmv) FROM test_cw_null_tbl\n" +
                    "WHERE CASE WHEN (`id` = 1) THEN 'a' END = 'a'\n" +
                    "  OR aux = 20\n" +
                    "GROUP BY pt;";
            String orPlan = planForceMv(orQuery);
            PlanTestBase.assertContains(orPlan, "test_cw_null_mv");
            PlanTestBase.assertContains(orPlan, "cw_flag = 'a'");
            PlanTestBase.assertContains(orPlan, "aux = 20");

            String missOrQuery =
                    "SELECT sum(gmv) FROM test_cw_null_tbl\n" +
                    "WHERE CASE WHEN (`id` = 1) THEN 'a' END IN ('x')\n" +
                    "  OR aux = 20\n" +
                    "GROUP BY pt;";
            String missOrPlan = planForceMv(missOrQuery);
            PlanTestBase.assertContains(missOrPlan, "test_cw_null_mv");
            PlanTestBase.assertContains(missOrPlan, "aux = 20");
        } finally {
            try {
                dropMv(DB_NAME, "test_cw_null_mv");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("test_cw_null_tbl");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Regression guard: the base column used by the CASE WHEN branch condition can also
     * be an exposed MV group-by key. The query still writes the original CASE WHEN
     * predicate, and rewrite should match the MV CASE key instead of missing because
     * {@code id} is present both as a raw group-by column and inside the CASE expression.
     */
    @Test
    public void testCaseWhenPredicateHitsMvWhenConditionColumnAlsoExposed() throws Exception {
        String createTable =
                "CREATE TABLE `test_cw_exposed_id_tbl` (\n" +
                "  `id` int(11) NULL COMMENT \"id\",\n" +
                "  `pt` date NOT NULL COMMENT \"\",\n" +
                "  `gmv` double NULL COMMENT \"gmv\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");";
        String createMv =
                "CREATE MATERIALIZED VIEW `test_cw_exposed_id_mv`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  `id`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END AS `is_flag`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_cw_exposed_id_tbl`\n" +
                "GROUP BY `pt`,\n" +
                "  `id`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END;";

        starRocksAssert.withTable(createTable);
        for (int id = 1; id <= 8; id++) {
            executeInsertSql(connectContext,
                    "insert into test_cw_exposed_id_tbl values(" + id + ",'2025-12-12'," + id + ")");
        }
        createAndRefreshMv(createMv);

        try {
            String query =
                    "SELECT sum(gmv) FROM test_cw_exposed_id_tbl\n" +
                    "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'a'\n" +
                    "GROUP BY pt;";
            String plan = planForceMv(query);
            PlanTestBase.assertContains(plan, "test_cw_exposed_id_mv");
            PlanTestBase.assertContains(plan, "is_flag = 'a'");
        } finally {
            try {
                dropMv(DB_NAME, "test_cw_exposed_id_mv");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("test_cw_exposed_id_tbl");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * A raw predicate must not interfere with mapping a later CASE WHEN disjunct.
     * The query {@code tag='a' OR CASE WHEN...='a'} should map both disjuncts to MV outputs.
     * equals a CASE THEN value.
     */
    @Test
    public void testOrRawPredicateDoesNotBlockCaseBranchMatch() throws Exception {
        String createTable =
                "CREATE TABLE `test_cw_or_tbl` (\n" +
                "  `id` int(11) NULL COMMENT \"id\",\n" +
                "  `aux` int(11) NULL COMMENT \"aux\",\n" +
                "  `tag` varchar(8) NULL COMMENT \"tag\",\n" +
                "  `pt` date NOT NULL COMMENT \"\",\n" +
                "  `gmv` double NULL COMMENT \"gmv\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        // CASE WHEN THEN values include 'a'; tag is a plain MV group-by key whose value
        // can also be 'a'. Both disjuncts of the OR should map onto MV-scan columns.
        String createMv =
                "CREATE MATERIALIZED VIEW `test_cw_or_mv`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  `aux`,\n" +
                "  `tag`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END AS `cw_is_flag`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_cw_or_tbl`\n" +
                "GROUP BY `pt`,\n" +
                "  `aux`,\n" +
                "  `tag`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END;";
        starRocksAssert.withTable(createTable);
        for (int id = 1; id <= 8; id++) {
            executeInsertSql(connectContext,
                    "insert into test_cw_or_tbl values(" + id + "," + (id * 10) + ",'a','2025-12-12'," + id + ")");
        }
        createAndRefreshMv(createMv);

        try {
            // The raw predicate precedes the CASE WHEN disjunct; both must map to MV outputs.
            String query =
                    "SELECT sum(gmv) FROM test_cw_or_tbl\n" +
                    "WHERE tag = 'a'\n" +
                    "  OR CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b' ELSE 'c' END = 'a'\n" +
                    "GROUP BY pt;";
            String plan = planForceMv(query);
            // MV must be hit; the raw predicate must not block the CASE branch conversion.
            PlanTestBase.assertContains(plan, "test_cw_or_mv");
            // Both MV-side predicates must be present in the OR compensation.
            PlanTestBase.assertContains(plan, "cw_is_flag = 'a'");
        } finally {
            try {
                dropMv(DB_NAME, "test_cw_or_mv");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("test_cw_or_tbl");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * View-Delta / FK-PK: the MV has an extra dim table that the query does not touch.
     *
     * <p>The MV groups by two CASE WHEN keys: {@code dim_flag} (when references dim_case.flag)
     * and {@code fact_flag} (when references fact_case.id). The query filters only on
     * fact_flag. FK/PK + dim_id NOT NULL lets the MV prune dim_case via view-delta.
     *
     * <p>When rewriteAll reaches dim_flag, rewriteViewToQuery(d.flag=1) finds no view→query
     * relation mapping for d.flag's relation (dim_case was pruned) and returns null by its
     * contract. Before the fix buildIfThen(null) NPEs at construction, the MV rewrite fails
     * with "mv rewrite exception" and the query falls back to the base table. After the fix
     * the dim_flag branch is skipped and fact_flag matches normally, so the MV is hit.
     */
    @Test
    public void testViewDeltaCaseWhenKeyNotInQuery() throws Exception {
        try {
            starRocksAssert.dropTable("mv_case_extra_dim");
        } catch (Exception ignored) {
        }
        try {
            starRocksAssert.dropTable("fact_case");
        } catch (Exception ignored) {
        }
        try {
            starRocksAssert.dropTable("dim_case");
        } catch (Exception ignored) {
        }
        starRocksAssert.withTable(
                "CREATE TABLE `dim_case` (\n" +
                "  `id` INT,\n" +
                "  `flag` INT\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"unique_constraints\" = \"id\"\n" +
                ");");
        // dim_id NOT NULL so the FK/PK view-delta rewrite passes its not-null check.
        starRocksAssert.withTable(
                "CREATE TABLE `fact_case` (\n" +
                "  `id` INT,\n" +
                "  `dim_id` INT NOT NULL,\n" +
                "  `dt` DATE,\n" +
                "  `v` BIGINT\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"foreign_key_constraints\" = \"(dim_id) REFERENCES dim_case(id)\"\n" +
                ");");
        for (int i = 1; i <= 8; i++) {
            executeInsertSql(connectContext, "insert into dim_case values(" + i + "," + (i % 2) + ")");
            executeInsertSql(connectContext, "insert into fact_case values(" + i + "," + i + ",'2025-12-12'," + i + ")");
        }
        createAndRefreshMv(
                "CREATE MATERIALIZED VIEW `mv_case_extra_dim`\n" +
                "DISTRIBUTED BY HASH(`dt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT\n" +
                "  f.dt,\n" +
                "  CASE WHEN d.flag = 1 THEN 'hot' ELSE 'cold' END AS `dim_flag`,\n" +
                "  CASE WHEN f.id = 1 THEN 'a' ELSE 'b' END AS `fact_flag`,\n" +
                "  SUM(f.v) AS `sum_v`\n" +
                "FROM fact_case f\n" +
                "JOIN dim_case d ON f.dim_id = d.id\n" +
                "GROUP BY\n" +
                "  f.dt,\n" +
                "  CASE WHEN d.flag = 1 THEN 'hot' ELSE 'cold' END,\n" +
                "  CASE WHEN f.id = 1 THEN 'a' ELSE 'b' END;");
        try {
            String q =
                    "SELECT dt, SUM(v) FROM fact_case\n" +
                    "WHERE CASE WHEN id = 1 THEN 'a' ELSE 'b' END = 'a'\n" +
                    "GROUP BY dt;";
            // The preserved CASE predicate must also map through view-delta rewrite.
            String plan = planForceMv(q);
            PlanTestBase.assertContains(plan, "mv_case_extra_dim");
            // fact_flag = 'a' confirms the query CASE expression mapped to the MV output column.
            PlanTestBase.assertContains(plan, "fact_flag = 'a'");
        } finally {
            try {
                dropMv(DB_NAME, "mv_case_extra_dim");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("fact_case");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("dim_case");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * An IN predicate selecting both a regular branch and the ELSE branch must map to
     * the corresponding MV output values without losing either branch.
     */
    @Test
    public void testMixedElseAndNonElseInHitsMv() throws Exception {
        String createTable =
                "CREATE TABLE `test_cw_mixed_in_tbl` (\n" +
                "  `id` int(11) NULL COMMENT \"id\",\n" +
                "  `pt` date NOT NULL COMMENT \"\",\n" +
                "  `gmv` double NULL COMMENT \"gmv\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        // 4-branch CASE so that IN('a','c') has selected.size(2) <= unSelected.size(2),
        // which is the only simplify branch that emits an OR (rather than a NOT) when
        // the ELSE value is among the selected values.
        String createMv =
                "CREATE MATERIALIZED VIEW `test_cw_mixed_in_mv`\n" +
                "DISTRIBUTED BY HASH(`pt`) BUCKETS 1\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT `pt`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b'" +
                " WHEN (`id` = 3) THEN 'd' ELSE 'c' END AS `is_flag`,\n" +
                "  SUM(`gmv`) AS `sum_gmv`\n" +
                "FROM `test_cw_mixed_in_tbl`\n" +
                "GROUP BY `pt`,\n" +
                "  CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b'" +
                " WHEN (`id` = 3) THEN 'd' ELSE 'c' END;";
        starRocksAssert.withTable(createTable);
        for (int id = 1; id <= 8; id++) {
            executeInsertSql(connectContext,
                    "insert into test_cw_mixed_in_tbl values(" + id + ",'2025-12-12'," + id + ")");
        }
        createAndRefreshMv(createMv);

        try {
            String query =
                    "SELECT sum(gmv) FROM test_cw_mixed_in_tbl\n" +
                    "WHERE CASE WHEN (`id` = 1) THEN 'a' WHEN (`id` = 2) THEN 'b'" +
                    " WHEN (`id` = 3) THEN 'd' ELSE 'c' END IN ('a', 'c')\n" +
                    "GROUP BY pt;";
            String plan = planForceMv(query);

            // MV must be hit.
            PlanTestBase.assertContains(plan, "test_cw_mixed_in_mv");
            // The final plan carries both selected MV output values as an IN predicate.
            PlanTestBase.assertContains(plan, "is_flag IN ('a','c')");
        } finally {
            try {
                dropMv(DB_NAME, "test_cw_mixed_in_mv");
            } catch (Exception ignored) {
            }
            try {
                starRocksAssert.dropTable("test_cw_mixed_in_tbl");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Runs a query with {@code materialized_view_rewrite_mode = force} so the rewritten
     * MV plan wins the cost comparison regardless of row count, then restores the mode.
     */
    private String planForceMv(String query) throws Exception {
        String prev = connectContext.getSessionVariable().getMaterializedViewRewriteMode();
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        try {
            return getFragmentPlan(query, "MV");
        } finally {
            connectContext.getSessionVariable().setMaterializedViewRewriteMode(prev);
        }
    }

    /** Counts non-overlapping occurrences of {@code sub} in {@code s}. */
    private static int countOccurrences(String s, String sub) {
        if (s == null || sub == null || sub.isEmpty()) {
            return 0;
        }
        int count = 0;
        int idx = 0;
        while ((idx = s.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }
}
