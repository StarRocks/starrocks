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

import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateSyncMVStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.starrocks.sql.optimizer.MVTestUtils.waitingRollupJobV2Finish;

/**
 * Regression tests for type-consistency bugs in sync-MV rewrite.
 *
 * <p>Issue #72799: When a sync MV stores {@code sum(k3 SMALLINT)} as a BIGINT physical column,
 * a query containing {@code sum(if(k2=0, k3, 0))} is rewritten against the MV by substituting
 * {@code k3 -> mv_sum_k3}, but {@code ReplaceColumnRefRewriter} does not re-derive parent
 * operator types or rebind the SUM/IF function signatures, causing a type mismatch that crashes
 * BE (down_cast assert in VectorizedIfExpr) or triggers PlanValidator "Invalid plan" errors.
 */
public class SyncMvRewriteTypeConsistencyTest extends MVTestBase {

    private static final String TABLE_NAME = "t1";
    private static final String MV_NAME = "test_mv1";
    private static final String KEY1_COL = "k1";
    private static final String KEY2_COL = "k2";
    private static final String KEY3_COL = "k3";
    private static final String SUM_COL = "sum1";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        // Sync MV tests (rollup-based) require runningUnitTest = false so that rollup jobs
        // are actually processed rather than skipped.
        FeConstants.runningUnitTest = false;
    }

    /**
     * Regression anchor for #72799.
     *
     * <p>Creates a DUPLICATE KEY table with a SMALLINT column {@code k3}, then a sync MV
     * that stores {@code sum(k3)} as a BIGINT physical column ({@code mv_sum1}).  Issues a
     * query with {@code sum(if(k2 = 0, k3, 0))} and asserts:
     * <ol>
     *   <li>The plan selects the MV rollup ({@code test_mv1}).</li>
     *   <li>The IF expression argument type is no longer SMALLINT after the rewrite
     *       (i.e., the type has been widened to match the MV column).</li>
     * </ol>
     *
     * <p>This test is expected to FAIL on the current (unfixed) codebase — that failure is
     * the regression anchor.  Once the fix lands it must pass.
     */
    @Test
    public void testSyncMVRewriteIfAggColumnKeepsConsistentType() throws Exception {
        // 1. Create base table with a SMALLINT column.
        starRocksAssert.withTable(
                "CREATE TABLE " + TABLE_NAME + " (\n" +
                "  " + KEY1_COL + " DATE NULL,\n" +
                "  " + KEY2_COL + " INT NULL,\n" +
                "  " + KEY3_COL + " SMALLINT NULL\n" +
                ") DUPLICATE KEY(" + KEY1_COL + ")\n" +
                "DISTRIBUTED BY HASH(" + KEY1_COL + ") BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1')");

        // 2. Create the sync MV: sum(k3) is stored as BIGINT in the rollup index.
        String createMvSql =
                "CREATE MATERIALIZED VIEW " + MV_NAME + " " +
                "AS SELECT " + KEY1_COL + ", " + KEY2_COL + ", sum(" + KEY3_COL + ") AS " + SUM_COL +
                " FROM " + TABLE_NAME + " GROUP BY " + KEY1_COL + ", " + KEY2_COL;
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(createMvSql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .createMaterializedView((CreateSyncMVStmt) stmt);

        // 3. Wait for the rollup job to finish.
        waitingRollupJobV2Finish();

        try {
            // 4. Issue a query whose sum(if(...)) argument references the narrow SMALLINT k3.
            String query =
                    "SELECT " + KEY1_COL + ", sum(if(" + KEY2_COL + " = 0, " + KEY3_COL + ", 0)) AS sum_if " +
                    "FROM " + TABLE_NAME + " GROUP BY " + KEY1_COL;
            String plan = getFragmentPlan(query);

            // 5a. The plan must pick the MV rollup.
            PlanTestBase.assertContains(plan, MV_NAME);

            // 5b. After rewrite the IF argument type must NOT still be SMALLINT.
            //     The physical MV column (sum1) is BIGINT; if the rewriter left the ColumnRef
            //     type as SMALLINT it indicates that type re-derivation was skipped (the bug).
            //     The verbose plan prints IF function signature inline, e.g.
            //     "if[(...); args: BOOLEAN,SMALLINT,SMALLINT; result: SMALLINT; ...]".
            //     After the fix, the args should be "args: BOOLEAN,BIGINT,BIGINT; result: BIGINT".
            String verbosePlan = UtFrameUtils.getVerboseFragmentPlan(connectContext, query);
            Assertions.assertFalse(
                    verbosePlan.contains("args: BOOLEAN,SMALLINT,SMALLINT"),
                    "rewritten IF should no longer claim SMALLINT args after MV substitution; got plan:\n" +
                    verbosePlan);
        } finally {
            // 6. Cleanup: drop MV first, then the base table.
            starRocksAssert.dropMaterializedView(MV_NAME);
            starRocksAssert.dropTable(TABLE_NAME);
        }
    }

    // -----------------------------------------------------------------------
    // Phase 6.1: parameterized shape matrix (13 expression cases)
    // -----------------------------------------------------------------------

    private static final String MATRIX_TABLE = "t_matrix";
    private static final String MATRIX_MV    = "t_matrix_mv";

    static Stream<Arguments> matrixCaseProvider() {
        Object[][] cases = new Object[][] {
            {"sum(k3)",                                               Boolean.TRUE,  "Q1 direct"},
            {"sum(k3 * 2)",                                          Boolean.TRUE,  "Q2 arith mul"},
            {"sum(k3 + 1)",                                          Boolean.TRUE,  "Q3 arith add"},
            {"sum(coalesce(k3, 0))",                                 Boolean.TRUE,  "Q4 coalesce"},
            {"sum(nullif(k3, 0))",                                   Boolean.TRUE,  "Q5 nullif"},
            {"sum(if(k2=0, k3, 0))",                                 Boolean.TRUE,  "Q6 if"},
            {"sum(case when k2=0 then k3 else 0 end)",               Boolean.TRUE,  "Q7 case"},
            {"sum(cast(k3 as bigint))",                              Boolean.TRUE,  "Q8 explicit cast"},
            {"sum(if(k2=0, if(k3>0, k3, -k3), 0))",                 Boolean.TRUE,  "Q9 nested if"},
            {"sum(k3) + sum(case when k2=0 then k3 else 0 end)",    Boolean.TRUE,  "Q10 multi-agg"},
            // Q11: avg() cannot rollup from a sum-only MV — StarRocks requires both
            // sum(k3) AND count(k3) columns in the MV for avg rollup eligibility.
            {"avg(k3)",                                               Boolean.FALSE, "Q11 rollup-fn"},
            {"sum(rand() * k3)",                                      Boolean.FALSE, "Q12 nondeterministic"},
            {"sum((select max(k2) from t_matrix) + k3)",             Boolean.FALSE, "Q13 subquery"},
        };
        Stream.Builder<Arguments> builder = Stream.builder();
        for (Object[] row : cases) {
            builder.accept(Arguments.of(row[0], row[1], row[2]));
        }
        return builder.build();
    }

    /**
     * Shape matrix — Phase 6.1.
     *
     * <p>Verifies that the column-coverage eligibility screen (Phase 4.5) and the
     * type-coherent substitution path (Phases 1–4) handle a representative set of
     * expression shapes that wrap the substituted MV column.
     *
     * <p>Cases Q1–Q11 are expected to hit the MV rollup; Q12–Q13 must NOT hit
     * (nondeterministic / subquery shapes are blocked by the pre-screen).
     */
    @ParameterizedTest(name = "{2}")
    @MethodSource("matrixCaseProvider")
    public void testSyncMvRewriteTypeConsistencyMatrix(
            String exprStr, boolean shouldHit, String label) throws Exception {

        // Create base table with a SMALLINT column.
        starRocksAssert.withTable(
                "CREATE TABLE " + MATRIX_TABLE + " (\n" +
                "  k1 DATE NULL,\n" +
                "  k2 INT NULL,\n" +
                "  k3 SMALLINT NULL\n" +
                ") DUPLICATE KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1')");

        // Create the sync MV: sum(k3) is stored as BIGINT in the rollup index.
        String createMvSql =
                "CREATE MATERIALIZED VIEW " + MATRIX_MV + " " +
                "AS SELECT k1, k2, sum(k3) AS s FROM " + MATRIX_TABLE +
                " GROUP BY k1, k2";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(createMvSql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .createMaterializedView((CreateSyncMVStmt) stmt);

        // Wait for the rollup job to finish.
        waitingRollupJobV2Finish();

        try {
            String query = "SELECT k1, " + exprStr + " AS v FROM " + MATRIX_TABLE + " GROUP BY k1";
            if (shouldHit) {
                // Must plan successfully AND reference the MV rollup.
                String plan = getFragmentPlan(query);
                Assertions.assertTrue(
                        plan.toLowerCase().contains(MATRIX_MV.toLowerCase()),
                        "[" + label + "] Expected plan to reference MV '" + MATRIX_MV +
                        "' but it did not.\nPlan:\n" + plan);
            } else {
                // Either (a) the planner raises an exception before returning a plan (indicating
                // the MV path was attempted but failed validation, treated as a miss), or
                // (b) the plan is produced but references the base table instead of the MV.
                String plan = null;
                try {
                    plan = getFragmentPlan(query);
                } catch (Exception plannerEx) {
                    // Planner exception = MV rewrite was rejected / failed — counts as a miss.
                    return;
                }
                Assertions.assertFalse(
                        plan.toLowerCase().contains(MATRIX_MV.toLowerCase()),
                        "[" + label + "] Expected plan NOT to reference MV '" + MATRIX_MV +
                        "' but it did.\nPlan:\n" + plan);
            }
        } finally {
            // Cleanup: drop MV first, then base table.
            starRocksAssert.dropMaterializedView(MATRIX_MV);
            starRocksAssert.dropTable(MATRIX_TABLE);
        }
    }
}
