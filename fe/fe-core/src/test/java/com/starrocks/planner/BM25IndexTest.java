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

package com.starrocks.planner;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Plan tests for the builtin GIN BM25 {@code score()} full-text top-N ranking.
 *
 * <p>Positive cases assert the rewritten plan shape ({@code TopN -> Project(__bm25_score) -> OlapScan}
 * with the {@code BM25 SCORE:} scan attribute and the serialized options) produced by RewriteToBM25PlanRule. Negative
 * cases assert that every unsupported use of {@code score()} is rejected fail-fast with a
 * {@link SemanticException} by Bm25ScoreValidator (never silently). A regression case guards against
 * the synthetic score column leaking into the shared catalog schema.
 */
public class BM25IndexTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        Config.enable_experimental_gin = true;
        FeConstants.enablePruneEmptyOutputScan = false;

        // Full-text base table: GIN index built with index_options='DOCS_AND_FREQS' (BM25-capable).
        starRocksAssert.withTable("CREATE TABLE test.test_bm25 ("
                + " id INT,"
                + " content STRING,"
                + " category STRING,"
                + " INDEX idx_content (content) USING GIN("
                + "  'imp_lib' = 'builtin', 'parser' = 'english', 'index_options' = 'DOCS_AND_FREQS') "
                + ") "
                + "DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        // Two DOCS_AND_FREQS full-text columns: used to reject multi-column MATCH.
        starRocksAssert.withTable("CREATE TABLE test.test_bm25_two ("
                + " id INT,"
                + " c1 STRING,"
                + " c2 STRING,"
                + " INDEX idx_c1 (c1) USING GIN("
                + "  'imp_lib' = 'builtin', 'parser' = 'english', 'index_options' = 'DOCS_AND_FREQS'), "
                + " INDEX idx_c2 (c2) USING GIN("
                + "  'imp_lib' = 'builtin', 'parser' = 'english', 'index_options' = 'DOCS_AND_FREQS') "
                + ") "
                + "DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        // Plain GIN (DOCS only): full-text filter works but BM25 scoring is not possible.
        starRocksAssert.withTable("CREATE TABLE test.test_bm25_docs ("
                + " id INT,"
                + " content STRING,"
                + " INDEX idx_content (content) USING GIN("
                + "  'imp_lib' = 'builtin', 'parser' = 'english') "
                + ") "
                + "DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        // No index at all.
        starRocksAssert.withTable("CREATE TABLE test.test_bm25_noindex ("
                + " id INT,"
                + " content STRING"
                + ") "
                + "DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        // Clucene GIN with index_options='DOCS_AND_FREQS': the property is accepted, but clucene does
        // not produce the builtin block-posting data BM25 scoring needs, so score() must reject it.
        starRocksAssert.withTable("CREATE TABLE test.test_bm25_clucene ("
                + " id INT,"
                + " content STRING,"
                + " INDEX idx_content (content) USING GIN("
                + "  'imp_lib' = 'clucene', 'parser' = 'english', 'index_options' = 'DOCS_AND_FREQS') "
                + ") "
                + "DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");
    }

    // ---------------------------------------------------------------------------------------------
    // Positive cases.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCanonicalMatchAny() throws Exception {
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'apache starrocks' order by score() desc limit 10";
        String plan = getVerboseExplain(sql);
        // Rewritten shape: TopN over a Project that outputs the synthetic score column, over the scan.
        assertContains(plan, "TOP-N");
        assertContains(plan, "OlapScanNode");
        assertContains(plan, "__bm25_score");
        assertContains(plan, "BM25 SCORE:");
        // Serialized options mirror the thrift TBM25SearchOptions content.
        assertContains(plan, "Score Column: <");
        assertContains(plan, "apache starrocks");
        assertContains(plan, "K1: 1.2");
        assertContains(plan, "B: 0.75");
    }

    @Test
    public void testMatchAll() throws Exception {
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ALL 'apache starrocks' order by score() desc limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "BM25 SCORE:");
        assertContains(plan, "apache starrocks");
    }

    @Test
    public void testPlainMatch() throws Exception {
        // The bare MATCH operator (not just MATCH_ANY/MATCH_ALL) drives BM25 top-N ranking: the validator
        // and rewrite match any MatchExpr, independent of its operator.
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH 'apache starrocks' order by score() desc limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "BM25 SCORE:");
        assertContains(plan, "apache starrocks");
    }

    @Test
    public void testScoreOnlyInOrderBy() throws Exception {
        // score() is used only for ranking (not projected to the user).
        String sql = "select id from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' order by score() desc limit 5";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "BM25 SCORE:");
        assertContains(plan, "__bm25_score");
    }

    @Test
    public void testResidualScalarPredicate() throws Exception {
        // A scalar predicate AND-ed with MATCH stays as a residual on the scan; BM25 still fires.
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' and category = 'news' order by score() desc limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "BM25 SCORE:");
        // The residual scalar predicate must be retained on the scan.
        assertContains(plan, "category");
    }

    @Test
    public void testExplainOptionsContent() throws Exception {
        // The explain block reflects the exact TBM25SearchOptions the rule set.
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'relevance ranking' order by score() desc limit 7";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "BM25 SCORE:");
        assertContains(plan, "Query: [");
        assertContains(plan, "relevance ranking");
        assertContains(plan, "Score Column: <");
    }

    @Test
    public void testTopkFoldsOffset() throws Exception {
        // Top-k pushdown folds OFFSET into the pushed limit: LIMIT 10 OFFSET 5 -> TopK: 15.
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'apache starrocks' order by score() desc limit 10 offset 5";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "BM25 SCORE:");
        assertContains(plan, "TopK: 15");
    }

    @Test
    public void testSessionK1BPropagate() throws Exception {
        // Non-default SET bm25_k1 / bm25_b must propagate into the scored scan's options.
        double originalK1 = connectContext.getSessionVariable().getBm25K1();
        double originalB = connectContext.getSessionVariable().getBm25B();
        try {
            connectContext.getSessionVariable().setBm25K1(2.0);
            connectContext.getSessionVariable().setBm25B(0.5);
            String sql = "select id, score() from test.test_bm25 "
                    + "where content MATCH_ANY 'apache starrocks' order by score() desc limit 10";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "K1: 2.0");
            assertContains(plan, "B: 0.5");
        } finally {
            connectContext.getSessionVariable().setBm25K1(originalK1);
            connectContext.getSessionVariable().setBm25B(originalB);
        }
    }

    @Test
    public void testCompositionInnerTopNJoinedInOuterBlock() throws Exception {
        // score() lives in an inner single-table top-N subquery; the outer block joins its result and
        // references only the score column (not score()). This composition must be allowed (per-block).
        String sql = "select t.id, t.s from "
                + "(select id, score() as s from test.test_bm25 where content MATCH_ANY 'starrocks' "
                + " order by score() desc limit 10) t "
                + "join test.test_bm25_noindex n on t.id = n.id "
                + "order by t.s desc limit 5";
        String plan = getVerboseExplain(sql);
        // The inner block still produces a BM25 scan.
        assertContains(plan, "BM25 SCORE:");
        assertContains(plan, "__bm25_score");
    }

    // ---------------------------------------------------------------------------------------------
    // Negative cases -- every unsupported use of score() must be a hard SemanticException.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testMissingLimitRejected() {
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' order by score() desc";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("positive LIMIT");
    }

    @Test
    public void testMissingOrderByScoreRejected() {
        // score() in the SELECT list but no ORDER BY score().
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("ORDER BY score()");
    }

    @Test
    public void testScoreInWhereRejected() {
        String sql = "select id from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' and score() > 0.5 order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("score() is only supported for full-text top-N ranking");
    }

    @Test
    public void testScoreInAggregateRejected() {
        String sql = "select sum(score()) from test.test_bm25 where content MATCH_ANY 'starrocks'";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("score() is only supported for full-text top-N ranking");
    }

    @Test
    public void testScoreInGroupByRejected() {
        String sql = "select score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' group by score() order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("score() is only supported for full-text top-N ranking");
    }

    @Test
    public void testScoreAcrossJoinRejected() {
        // score() in a block whose FROM is a join is not bound to a single base full-text table.
        String sql = "select a.id, score() from test.test_bm25 a "
                + "join test.test_bm25_noindex b on a.id = b.id "
                + "where a.content MATCH_ANY 'starrocks' order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("single full-text base table");
    }

    @Test
    public void testScoreWithSubqueryRejected() {
        // A subquery in the score() block decorrelates into a join, so the TopN -> OlapScan rewrite cannot
        // fire; score() would otherwise survive unrewritten and reach the BE (which has no implementation).
        // The plan-build guard must reject it.
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'apache starrocks' and id in (select id from test.test_bm25_docs) "
                + "order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("score() is only supported for full-text top-N ranking");
    }

    @Test
    public void testTwoMatchColumnsRejected() {
        String sql = "select id, score() from test.test_bm25_two "
                + "where c1 MATCH_ANY 'apache' and c2 MATCH_ANY 'starrocks' order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("exactly one MATCH column");
    }

    @Test
    public void testMatchInsideOrRejected() {
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' or category = 'news' order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("score()");
    }

    @Test
    public void testDocsOnlyIndexRejected() {
        String sql = "select id, score() from test.test_bm25_docs "
                + "where content MATCH_ANY 'starrocks' order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("DOCS_AND_FREQS");
    }

    @Test
    public void testCluceneIndexRejected() {
        // A clucene GIN index with index_options='DOCS_AND_FREQS' passes property validation but cannot
        // serve BM25 scoring (no builtin block-posting), so score() must reject it as a hard error.
        String sql = "select id, score() from test.test_bm25_clucene "
                + "where content MATCH_ANY 'starrocks' order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("builtin");
    }

    @Test
    public void testScoreWithoutMatchRejected() {
        String sql = "select id, score() from test.test_bm25 order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("MATCH predicate");
    }

    @Test
    public void testScoreOutsideSelectBlockRejected() {
        // Bm25ScoreValidator only runs on SelectRelation. score() reaching a context it does not see --
        // here an INSERT ... VALUES row (a ValuesRelation) -- is rejected during analysis, rather than
        // surviving to fail later against its nullptr BE implementation.
        String sql = "insert into test.test_bm25 (id) values (score())";
        assertThatThrownBy(() -> getFragmentPlan(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("score()");
    }

    @Test
    public void testScoreWithArgumentsIsNotBm25Builtin() {
        // score(col) has arguments, so it is a user function, not the zero-arg BM25 builtin
        // (isBM25ScoreCall requires zero args): it must NOT be captured by the BM25 shape validator, and
        // just fails to resolve as an unknown function signature instead.
        assertThatThrownBy(() -> getFragmentPlan("select score(id) from test.test_bm25"))
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("MATCH"));
    }

    @Test
    public void testWindowFunctionRejected() {
        // A window function makes the plan TopN -> Window -> Scan, which the rewrite cannot match, so
        // score() must be rejected at analysis rather than surviving to the BE.
        String sql = "select id, score(), row_number() over (order by id) from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' order by score() desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("window");
    }

    @Test
    public void testSetOperationOrderByScoreRejected() {
        // score() in a set-operation ORDER BY is never seen by the per-SelectRelation validator and can
        // never be rewritten; it must still be rejected fail-fast.
        String sql = "select id from test.test_bm25 union all select id from test.test_bm25 "
                + "order by score() desc limit 5";
        assertThatThrownBy(() -> getFragmentPlan(sql))
                .isInstanceOf(SemanticException.class);
    }

    @Test
    public void testValuesScoreRejected() {
        // Feature is on (set in @BeforeAll); score() in a VALUES row can never be rewritten.
        assertThatThrownBy(() -> getFragmentPlan("insert into test.test_bm25 (id) values (score())"))
                .isInstanceOf(SemanticException.class);
    }

    @Test
    public void testJoinOnScoreRejected() {
        // score() in a JOIN ON condition is never seen by the per-SelectRelation validator and can never
        // be rewritten; AnalyzerUtils.verifyNoScoreFunction (mirroring the aggregate/window guards) rejects it.
        String sql = "select t1.id from test.test_bm25 t1 join test.test_bm25 t2 on score() > 0";
        assertThatThrownBy(() -> getFragmentPlan(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("JOIN clause cannot contain score()");
    }

    @Test
    public void testBm25SessionVarRangeValidation() {
        // The analyzer wraps the SemanticException into AnalysisException, so assert on the message.
        assertThatThrownBy(() -> analyzeSet("set bm25_b = -1")).hasMessageContaining("bm25_b");
        assertThatThrownBy(() -> analyzeSet("set bm25_b = 2")).hasMessageContaining("bm25_b");
        assertThatThrownBy(() -> analyzeSet("set bm25_k1 = -0.5")).hasMessageContaining("bm25_k1");
        Assertions.assertDoesNotThrow(() -> analyzeSet("set bm25_b = 0.5"));
        Assertions.assertDoesNotThrow(() -> analyzeSet("set bm25_k1 = 1.5"));
    }

    private void analyzeSet(String sql) throws Exception {
        // parseStmtWithNewParser analyzes the SET statement (running SetStmtAnalyzer) as part of parsing.
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    // ---------------------------------------------------------------------------------------------
    // Top-k pushdown: the LIMIT reaches the scored scan only when MATCH is the whole filter and the
    // order is DESC (mirrors OSS StarRocks #75952's under-return gating).
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testTopKPushedWhenMatchIsSoleFilter() throws Exception {
        String plan = getVerboseExplain("select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' order by score() desc limit 10");
        assertContains(plan, "TopK: 10");
    }

    @Test
    public void testTopKNotPushedWithResidualPredicate() throws Exception {
        // A post-scan scalar filter could drop rows after the top-k, so the limit must NOT be pushed;
        // the BE scores every matched row and the TopN above applies the limit.
        String plan = getVerboseExplain("select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' and category = 'news' order by score() desc limit 10");
        assertContains(plan, "TopK: 0");
    }

    @Test
    public void testTopKNotPushedForAscendingOrder() throws Exception {
        String plan = getVerboseExplain("select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' order by score() asc limit 10");
        assertContains(plan, "TopK: 0");
    }

    @Test
    public void testOrderByScoreAliasAccepted() throws Exception {
        // The natural form aliases score() and orders by the alias (ORDER BY s); the alias must resolve
        // so the rewrite and top-k pushdown still fire.
        String plan = getVerboseExplain("select id, score() s from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' order by s desc limit 10");
        assertContains(plan, "BM25 SCORE:");
        assertContains(plan, "TopK: 10");
    }

    @Test
    public void testOrderByNonScoreColumnRejected() {
        // score() in the SELECT list but ordered by a non-score column: the rewrite cannot fire, so it
        // must be rejected fail-fast (guards that alias resolution did not over-relax the ORDER BY score() rule).
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' order by id limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("ORDER BY score()");
    }

    // ---------------------------------------------------------------------------------------------
    // Regression: the synthetic score column must never leak into the shared catalog table schema.
    // Mirrors VectorIndexTest.testRewriteDoesNotPolluteSharedCatalogSchema (the Table.addColumn bug).
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testRewriteDoesNotPolluteSharedCatalogSchema() throws Exception {
        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "test_bm25");
        String scoreColumn = "__bm25_score";
        String sql = "select id, score() from test.test_bm25 "
                + "where content MATCH_ANY 'starrocks' order by score() desc limit 10";

        boolean originalLock = connectContext.getSessionVariable().isCboUseDBLock();
        // Force the whole-phase-lock path so the rewrite plans on the live shared table, not a copy.
        connectContext.getSessionVariable().setCboUseDBLock(true);
        try {
            assertEquals(0, countColumns(table, scoreColumn));
            for (int i = 0; i < 5; i++) {
                String plan = getVerboseExplain(sql);
                assertContains(plan, "BM25 SCORE:");
                assertEquals(0, countColumns(table, scoreColumn),
                        "the rewrite must not add the score column to the shared catalog schema");
            }
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(originalLock);
        }
    }

    private static long countColumns(OlapTable table, String columnName) {
        return table.getFullSchema().stream()
                .filter(c -> c.getName().equalsIgnoreCase(columnName))
                .count();
    }
}
