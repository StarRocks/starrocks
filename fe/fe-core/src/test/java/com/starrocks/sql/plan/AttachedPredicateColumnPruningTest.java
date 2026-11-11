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
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * SQL regressions for view predicates across UNION, WINDOW, and VALUES roots.
 * Planning must succeed and the source filter must remain in the executable plan.
 */
public class AttachedPredicateColumnPruningTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        PlanTestNoneDBBase.beforeClass();
    }

    @Test
    public void testUnionPredicateColumnPruning() throws Exception {
        assertPredicateSurvivesPruning("union_predicate_repro",
                "SELECT id, source FROM t UNION ALL SELECT id, source FROM t",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b");
    }

    @Test
    public void testWindowPredicateColumnPruning() throws Exception {
        assertPredicateSurvivesPruning("window_predicate_repro",
                "SELECT id, source, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id, rn FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.rn FROM c a CROSS JOIN c b");
    }

    @Test
    public void testWindowPredicateWhenWindowResultIsUnused() throws Exception {
        assertPredicateSurvivesPruning("unused_window_predicate_repro",
                "SELECT id, source, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b");
    }

    @Test
    public void testWindowResultUsedOnlyByPredicate() throws Exception {
        assertPredicateSurvivesPruning("window_result_predicate_repro",
                "SELECT id, source, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id FROM v WHERE rn = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b",
                "row_number");
    }

    @Test
    public void testViewPredicateOnProjectedExpression() throws Exception {
        assertPredicateSurvivesPruning("projected_predicate_repro",
                "SELECT id, CAST(source AS BIGINT) AS source_alias, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
                "WITH c AS (SELECT id, rn FROM v WHERE source_alias = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.rn FROM c a CROSS JOIN c b");
    }

    @Test
    public void testValuesPredicateColumnPruning() throws Exception {
        assertPredicateSurvivesPruning("values_predicate_repro",
                "SELECT id, source FROM t WHERE FALSE UNION ALL SELECT 1, 2 UNION ALL SELECT 2, 1",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b");
    }

    @Test
    public void testViewPredicatePreservesOriginalViewFilter() throws Exception {
        ExecPlan plan = assertPredicateSurvivesPruning("original_filter_predicate_repro",
                "SELECT id, source FROM t WHERE id > 0",
                "WITH c AS (SELECT id FROM v WHERE source = 1) "
                        + "SELECT id FROM v UNION ALL SELECT a.id FROM c a CROSS JOIN c b");
        List<String> predicates = plan.getFragments().stream()
                .flatMap(fragment -> fragment.collectNodes().stream())
                .flatMap(node -> node.getConjuncts().stream())
                .map(ExprToSql::toSql)
                .toList();
        Assertions.assertTrue(predicates.stream().anyMatch(predicate -> predicate.contains("id")
                        && (predicate.contains("> 0") || predicate.contains(">= 1"))),
                () -> "The view's original filter was lost: " + predicates);
    }

    private ExecPlan assertPredicateSurvivesPruning(String database, String viewSql, String query) throws Exception {
        return assertPredicateSurvivesPruning(database, viewSql, query, "source");
    }

    private ExecPlan assertPredicateSurvivesPruning(String database, String viewSql, String query,
                                                    String predicateColumn) throws Exception {
        String previousDatabase = connectContext.getDatabase();
        SessionVariable previousSession = connectContext.getSessionVariable();
        SessionVariable session = (SessionVariable) previousSession.clone();
        session.setEnableMaterializedViewRewrite(true);
        session.setEnableViewBasedMvRewrite(true);
        session.setOptimizerExecuteTimeout(120000);
        session.setOptimizerMaterializedViewTimeLimitMillis(120000);
        connectContext.setSessionVariable(session);
        try {
            starRocksAssert.withDatabase(database).useDatabase(database);
            starRocksAssert.withTable("CREATE TABLE t (id INT, source INT) DUPLICATE KEY(id) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
            starRocksAssert.withView("CREATE VIEW v AS " + viewSql);
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH DEFERRED MANUAL "
                    + "PROPERTIES ('replication_num'='1','query_rewrite_consistency'='force_mv') "
                    + "AS SELECT id FROM v");

            ExecPlan plan = Assertions.assertDoesNotThrow(
                    () -> UtFrameUtils.getPlanAndFragment(connectContext, query).second,
                    "A predicate-only column must survive until its filter is evaluated: " + query);
            Assertions.assertEquals(1, plan.getOutputExprs().size(), "The query selects only one output column");
            List<String> predicates = plan.getFragments().stream()
                    .flatMap(fragment -> fragment.collectNodes().stream())
                    .flatMap(node -> node.getConjuncts().stream())
                    .map(ExprToSql::toSql)
                    .toList();
            Assertions.assertTrue(predicates.stream()
                            .anyMatch(predicate -> predicate.contains(predicateColumn) && predicate.contains("= 1")),
                    () -> "The " + predicateColumn + " = 1 filter was lost: " + predicates + "\n"
                            + plan.getExplainString(TExplainLevel.NORMAL));
            return plan;
        } finally {
            try {
                starRocksAssert.dropDatabase(database);
            } finally {
                connectContext.setDatabase(previousDatabase);
                connectContext.setSessionVariable(previousSession);
            }
        }
    }
}
