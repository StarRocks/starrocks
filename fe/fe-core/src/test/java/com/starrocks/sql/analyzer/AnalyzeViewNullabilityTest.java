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

package com.starrocks.sql.analyzer;

import com.starrocks.common.Pair;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getConnectContext;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

/**
 * Regression tests for view column nullability during semantic analysis. QueryAnalyzer.visitView
 * used to mark every view column nullable (the 4-arg Field constructor defaults isNullable to true),
 * which leaks through analysis-only schema consumers such as the Arrow Flight prepared-statement
 * schema.
 */
public class AnalyzeViewNullabilityTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();

        getStarRocksAssert()
                .withTable("CREATE TABLE `nn_base` (\n" +
                        "  `id` bigint NOT NULL,\n" +
                        "  `tenant_id` bigint NOT NULL,\n" +
                        "  `name` varchar(128) NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");")
                .withTable("CREATE TABLE `nn_base2` (\n" +
                        "  `id` bigint NOT NULL,\n" +
                        "  `label` varchar(128) NOT NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");")
                .withView("CREATE VIEW `nn_view` AS SELECT `id`, `tenant_id`, `name` FROM `nn_base`")
                .withView("CREATE VIEW `nn_join_view` AS " +
                        "SELECT a.id AS a_id, b.label AS b_label " +
                        "FROM nn_base a LEFT JOIN nn_base2 b ON a.id = b.id")
                .withView("CREATE VIEW `nn_expr_view` AS SELECT id + 1 AS id_plus, name FROM nn_base");
    }

    /** Analysis-time nullability (what buildSchemaFromQuery reads) must match the expected pattern. */
    private void assertAnalysisNullability(String sql, boolean[] expectedNullable) {
        List<Expr> outputExprs = ((QueryStatement) analyzeSuccess(sql)).getQueryRelation().getOutputExpression();
        Assertions.assertEquals(expectedNullable.length, outputExprs.size());
        for (int i = 0; i < expectedNullable.length; i++) {
            Assertions.assertEquals(expectedNullable[i], outputExprs.get(i).isNullable(),
                    "analysis-time nullability mismatch at column " + i + " for: " + sql);
        }
    }

    /**
     * Analysis-time nullability must equal the ExecPlan output nullability (what buildSchema reads
     * on the executed result) -- the contract the bug violated. Only valid for non-join queries;
     * across outer joins the optimizer conservatively widens preserved-side columns to nullable.
     */
    private void assertAnalysisMatchesExecPlan(String sql) throws Exception {
        List<Expr> analysisExprs =
                ((QueryStatement) analyzeSuccess(sql)).getQueryRelation().getOutputExpression();
        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(getConnectContext(), sql);
        List<Expr> planExprs = planPair.second.getOutputExprs();
        Assertions.assertEquals(analysisExprs.size(), planExprs.size());
        for (int i = 0; i < analysisExprs.size(); i++) {
            Assertions.assertEquals(planExprs.get(i).isNullable(), analysisExprs.get(i).isNullable(),
                    "analysis vs executed-plan nullability diverged at column " + i + " for: " + sql);
        }
    }

    @Test
    public void testBaseTableNullabilityUnchanged() throws Exception {
        // Base-table scan path -- already correct before the fix.
        assertAnalysisNullability("SELECT id, tenant_id, name FROM nn_base",
                new boolean[] {false, false, true});
        assertAnalysisMatchesExecPlan("SELECT id, tenant_id, name FROM nn_base");
    }

    @Test
    public void testExplicitColumnViewNullability() throws Exception {
        // The fix: NOT NULL columns through a view must not be reported nullable.
        assertAnalysisNullability("SELECT id, tenant_id, name FROM nn_view",
                new boolean[] {false, false, true});
        assertAnalysisMatchesExecPlan("SELECT id, tenant_id, name FROM nn_view");
    }

    @Test
    public void testViewColumnSubsetAndReorder() throws Exception {
        assertAnalysisNullability("SELECT name, id FROM nn_view",
                new boolean[] {true, false});
        assertAnalysisMatchesExecPlan("SELECT name, id FROM nn_view");
    }

    @Test
    public void testViewWithFilterPredicate() throws Exception {
        // Customer's query shape: explicit columns + equality predicate + limit.
        String sql = "SELECT id, tenant_id, name FROM nn_view WHERE tenant_id = CAST(1681 AS BIGINT) LIMIT 5000";
        assertAnalysisNullability(sql, new boolean[] {false, false, true});
        assertAnalysisMatchesExecPlan(sql);
    }

    @Test
    public void testViewOverOuterJoinKeepsNullable() throws Exception {
        // b_label sits on the null-supplying side of a LEFT JOIN in the view body, so it must stay
        // nullable even though its base column is NOT NULL; the preserved side a_id stays NOT NULL.
        // (No exec-plan check: the optimizer widens the preserved side to nullable across joins.)
        assertAnalysisNullability("SELECT a_id, b_label FROM nn_join_view",
                new boolean[] {false, true});
    }

    @Test
    public void testViewWithExpressionColumn() throws Exception {
        assertAnalysisNullability("SELECT id_plus, name FROM nn_expr_view",
                new boolean[] {false, true});
    }
}
