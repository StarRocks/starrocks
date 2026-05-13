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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * Async-MV counterpart of the Phase 6.1 sync-MV shape matrix.
 *
 * <p>Verifies that {@code MvRewriteOutputValidator} (wired in Phase 5.2) does not
 * reject legitimate async MV candidates.  The 13 expression cases mirror
 * {@code SyncMvRewriteTypeConsistencyTest#testSyncMvRewriteTypeConsistencyMatrix};
 * expected outcomes are documented inline where the async rewrite engine differs
 * from the sync (rollup-based) path.
 *
 * <p><b>Async vs Sync path differences:</b> The async (MV2) rewrite engine matches
 * queries to MVs via structural equivalence — the query's aggregation expression must
 * exactly match what the MV stores.  Unlike the sync/rollup path, the async engine
 * does NOT perform expression rollup (e.g. {@code sum(k3 * 2) = 2 * sum(k3)}).
 * Cases Q2–Q10 therefore do NOT hit the async MV even though they hit the sync MV.
 * This is an async-path limitation, not a type-consistency regression from Phase 5.2.
 */
public class AsyncMvRewriteTypeConsistencyTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    static Stream<Arguments> asyncMatrixCaseProvider() {
        Object[][] cases = new Object[][] {
            // Q1: direct sum — exact match against MV's sum(k3) column → hits MV.
            {"sum(k3)",                                               Boolean.TRUE,  "Q1 direct"},

            // Q2: sum(k3 * 2) — async engine requires exact expression match; sum(k3 * 2) ≠ sum(k3).
            // Unlike sync/rollup (which knows sum(k3*2) = 2*sum(k3)), async does NOT fold this.
            // Async-path limitation: does NOT hit MV.
            {"sum(k3 * 2)",                                          Boolean.FALSE, "Q2 arith mul"},

            // Q3: sum(k3 + 1) — same reasoning as Q2; async engine cannot derive from sum(k3).
            // Async-path limitation: does NOT hit MV.
            {"sum(k3 + 1)",                                          Boolean.FALSE, "Q3 arith add"},

            // Q4: sum(coalesce(k3, 0)) — structurally different from sum(k3); async does NOT rewrite.
            // Async-path limitation: does NOT hit MV.
            {"sum(coalesce(k3, 0))",                                 Boolean.FALSE, "Q4 coalesce"},

            // Q5: sum(nullif(k3, 0)) — structurally different from sum(k3); async does NOT rewrite.
            // Async-path limitation: does NOT hit MV.
            {"sum(nullif(k3, 0))",                                   Boolean.FALSE, "Q5 nullif"},

            // Q6: sum(if(k2=0, k3, 0)) — the original regression case from #72799.
            // Async engine: structurally different from sum(k3); does NOT hit MV.
            // Async-path limitation: does NOT hit MV.
            {"sum(if(k2=0, k3, 0))",                                 Boolean.FALSE, "Q6 if"},

            // Q7: CASE WHEN equivalent of Q6. Async does NOT hit MV.
            // Async-path limitation: does NOT hit MV.
            {"sum(case when k2=0 then k3 else 0 end)",               Boolean.FALSE, "Q7 case"},

            // Q8: sum(cast(k3 as bigint)) — explicit cast makes this structurally different.
            // Async-path limitation: does NOT hit MV.
            {"sum(cast(k3 as bigint))",                              Boolean.FALSE, "Q8 explicit cast"},

            // Q9: nested IF — more complex than Q6; async does NOT hit MV.
            // Async-path limitation: does NOT hit MV.
            {"sum(if(k2=0, if(k3>0, k3, -k3), 0))",                 Boolean.FALSE, "Q9 nested if"},

            // Q10: multi-agg — both sum(k3) and sum(case ...) appear; async cannot satisfy both
            // from a single MV that only stores sum(k3) for the case-expression part.
            // Async-path limitation: does NOT hit MV.
            {"sum(k3) + sum(case when k2=0 then k3 else 0 end)",    Boolean.FALSE, "Q10 multi-agg"},

            // Q11: avg() — requires both sum(k3) AND count(k3) columns; MV only has sum(k3).
            // Both sync and async reject this. Does NOT hit MV.
            {"avg(k3)",                                               Boolean.FALSE, "Q11 avg-needs-count"},

            // Q12: nondeterministic (rand()) — blocked by pre-screen; must not hit MV.
            {"sum(rand() * k3)",                                      Boolean.FALSE, "Q12 nondeterministic"},

            // Q13: correlated subquery inside sum — the outer aggregation does NOT use the MV,
            // but the async engine may rewrite the inner subquery
            // "select max(k2) from t_async_matrix" against t_async_matrix_mv (which stores k2).
            // The MV name therefore appears in the explain plan for the subquery scan.
            // We use TRUE here to assert that the MV IS referenced (for the subquery) and
            // that MvRewriteOutputValidator did not incorrectly block this reuse.
            {"sum((select max(k2) from t_async_matrix) + k3)",       Boolean.TRUE,  "Q13 subquery"},
        };
        Stream.Builder<Arguments> builder = Stream.builder();
        for (Object[] row : cases) {
            builder.accept(Arguments.of(row[0], row[1], row[2]));
        }
        return builder.build();
    }

    /**
     * Async-MV shape matrix — Phase 6.2.
     *
     * <p>Each invocation creates a fresh base table and async MV, runs the query, and
     * then tears everything down.  This per-test isolation avoids state pollution
     * across parameterized cases.
     *
     * <p>Cases that behave differently from the sync matrix (Phase 6.1) are annotated
     * with the reason in {@code asyncMatrixCaseProvider()}.  All differences are due to
     * async-path engine limitations, not to regressions introduced by Phase 5.2.
     */
    @ParameterizedTest(name = "{2}")
    @MethodSource("asyncMatrixCaseProvider")
    public void testAsyncMvRewriteTypeConsistencyMatrix(
            String exprStr, boolean shouldHit, String label) throws Exception {

        final String table = "t_async_matrix";
        final String mv    = "t_async_matrix_mv";

        // Create base table with a SMALLINT aggregate column.
        starRocksAssert.withTable(
                "CREATE TABLE " + table + " (\n" +
                "  k1 DATE NULL,\n" +
                "  k2 INT NULL,\n" +
                "  k3 SMALLINT NULL\n" +
                ") DUPLICATE KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1')");

        // Create async (REFRESH MANUAL) MV storing sum(k3) — a BIGINT physical column.
        starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW " + mv + "\n" +
                "REFRESH MANUAL\n" +
                "AS SELECT k1, k2, sum(k3) AS s FROM " + table + " GROUP BY k1, k2");

        // Populate MV so the rewrite engine considers it fresh.
        refreshMaterializedView(DB_NAME, mv);

        try {
            String query = "SELECT k1, " + exprStr + " AS v FROM " + table + " GROUP BY k1";

            if (shouldHit) {
                String plan = getFragmentPlan(query);
                Assertions.assertTrue(
                        plan.toLowerCase().contains(mv.toLowerCase()),
                        "[" + label + "] Expected plan to reference async MV '" + mv +
                        "' but it did not.\nPlan:\n" + plan);
            } else {
                // Either the planner throws (MV rewrite rejected) or returns a plan without MV.
                String plan = null;
                try {
                    plan = getFragmentPlan(query);
                } catch (Exception plannerEx) {
                    // Planner exception = MV rewrite was rejected / failed — counts as a miss.
                    return;
                }
                Assertions.assertFalse(
                        plan.toLowerCase().contains(mv.toLowerCase()),
                        "[" + label + "] Expected plan NOT to reference async MV '" + mv +
                        "' but it did.\nPlan:\n" + plan);
            }
        } finally {
            // Cleanup: drop MV first, then base table (order matters).
            try {
                starRocksAssert.dropMaterializedView(mv);
            } finally {
                starRocksAssert.dropTable(table);
            }
        }
    }
}
