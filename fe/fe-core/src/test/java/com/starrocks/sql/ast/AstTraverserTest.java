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

package com.starrocks.sql.ast;

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Regression tests for the pre-analysis SELECT projection traversal gap: {@link AstTraverser#visitSelect}
 * visits {@code getOutputExpression()}, which is null before analysis, but must still reach tables that are
 * only referenced inside the parser-populated {@code selectList} (e.g. a scalar subquery in the SELECT list).
 */
public class AstTraverserTest {

    /**
     * Counts how many times each table name is visited by {@link AstTraverser#visitTable}.
     */
    private static final class TableVisitCounter extends AstTraverser<Void, Void> {
        private final Map<String, Integer> visitCounts = new HashMap<>();

        @Override
        public Void visitTable(TableRelation node, Void context) {
            visitCounts.merge(node.getName().getTbl(), 1, Integer::sum);
            return null;
        }

        private int countOf(String table) {
            return visitCounts.getOrDefault(table, 0);
        }
    }

    private static QueryStatement parseWithoutAnalysis(String sql) {
        return (QueryStatement) SqlParser.parse(sql, SqlModeHelper.MODE_DEFAULT).get(0);
    }

    @Test
    public void testTableInsideProjectionSubqueryVisitedBeforeAnalysis() {
        QueryStatement stmt = parseWithoutAnalysis("SELECT (SELECT COUNT(*) FROM protected_table)");

        TableVisitCounter counter = new TableVisitCounter();
        counter.visit(stmt);

        Assertions.assertEquals(1, counter.countOf("protected_table"),
                "protected_table is only reachable through the parser-populated selectList before analysis");
    }

    @Test
    public void testWildcardAndProjectionSubqueryBothVisitedBeforeAnalysis() {
        QueryStatement stmt = parseWithoutAnalysis(
                "SELECT t.*, (SELECT COUNT(*) FROM protected_table) FROM outer_table t");

        TableVisitCounter counter = new TableVisitCounter();
        counter.visit(stmt);

        Assertions.assertEquals(1, counter.countOf("outer_table"),
                "outer_table is referenced by the FROM clause");
        Assertions.assertEquals(1, counter.countOf("protected_table"),
                "protected_table is only reachable through the parser-populated selectList before analysis");
    }

    @Test
    public void testResolvedOutputExpressionPreventsDuplicateSelectListTraversal() {
        QueryStatement stmt = parseWithoutAnalysis(
                "SELECT (SELECT COUNT(*) FROM protected_table) FROM outer_table t");
        SelectRelation selectRelation = (SelectRelation) stmt.getQueryRelation();
        Assertions.assertNotNull(selectRelation.getSelectList(),
                "the parser-populated selectList must still be retained");
        Assertions.assertNull(selectRelation.getOutputExpression(),
                "outputExpr is not populated until analysis");

        // Simulate a post-analysis state: outputExpr is resolved to a distinct subquery expression
        // referencing the same table, while the original parsed selectList is left untouched.
        Expr resolvedOutputExpr = SqlParser.parseSqlToExpr(
                "(SELECT COUNT(*) FROM protected_table)", SqlModeHelper.MODE_DEFAULT);
        selectRelation.setOutputExpr(Collections.singletonList(resolvedOutputExpr));

        TableVisitCounter counter = new TableVisitCounter();
        counter.visit(stmt);

        Assertions.assertEquals(1, counter.countOf("outer_table"));
        Assertions.assertEquals(1, counter.countOf("protected_table"),
                "once outputExpr is resolved, only it (not the stale parsed selectList) must be traversed");
    }
}
