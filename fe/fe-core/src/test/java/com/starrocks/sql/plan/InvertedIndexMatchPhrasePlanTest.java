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

import com.starrocks.analysis.MatchExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan-level coverage for {@code MATCH_PHRASE}: round-trip via toSql, slop
 * preservation across clone, and parser-driven slop integration with
 * {@link MatchExpr#parsePhrasePattern(String)}.
 *
 * <p>Full SQL planning tests (verifying {@code OlapScanNode.conjuncts}
 * contain the new operator) require setting up a tablet with a GIN index;
 * those run via {@code GINIndexTest}-style fixtures in dedicated suites
 * once the BE plugin lands. This test focuses on FE-only invariants.
 */
public class InvertedIndexMatchPhrasePlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.enable_experimental_gin = true;
        PlanTestBase.beforeClass();
    }

    @Test
    public void matchPhraseExpr_carriesSlop() {
        StringLiteral lhs = new StringLiteral("col");
        StringLiteral pattern = new StringLiteral("hello world");
        MatchExpr expr = new MatchExpr(MatchExpr.MatchOperator.MATCH_PHRASE, lhs, pattern, 3, NodePosition.ZERO);
        Assertions.assertEquals(MatchExpr.MatchOperator.MATCH_PHRASE, expr.getMatchOperator());
        Assertions.assertEquals(3, expr.getSlop());
    }

    @Test
    public void matchPhraseExpr_clonePreservesSlop() {
        StringLiteral lhs = new StringLiteral("col");
        StringLiteral pattern = new StringLiteral("hello world");
        MatchExpr expr = new MatchExpr(MatchExpr.MatchOperator.MATCH_PHRASE, lhs, pattern, 5, NodePosition.ZERO);
        MatchExpr cloned = (MatchExpr) expr.clone();
        Assertions.assertEquals(MatchExpr.MatchOperator.MATCH_PHRASE, cloned.getMatchOperator());
        Assertions.assertEquals(5, cloned.getSlop());
    }

    @Test
    public void matchPhraseExpr_toSqlRoundTripsSlop() {
        // Use StringLiteral on both sides to avoid SlotRef.toSql requiring a real
        // SlotDescriptor — the test only cares that MATCH_PHRASE keyword + ~N
        // suffix surface in the rendered SQL.
        StringLiteral lhs = new StringLiteral("col");
        StringLiteral pattern = new StringLiteral("hello world");
        MatchExpr expr = new MatchExpr(MatchExpr.MatchOperator.MATCH_PHRASE, lhs, pattern, 3, NodePosition.ZERO);
        String sql = expr.toSqlImpl();
        Assertions.assertTrue(sql.contains("MATCH_PHRASE"),
                "expected MATCH_PHRASE keyword in toSql: " + sql);
        Assertions.assertTrue(sql.contains("~3"),
                "expected slop ~3 in toSql: " + sql);
    }

    @Test
    public void matchPhraseExpr_zeroSlopHasNoSuffix() {
        StringLiteral lhs = new StringLiteral("col");
        StringLiteral pattern = new StringLiteral("hello world");
        MatchExpr expr = new MatchExpr(MatchExpr.MatchOperator.MATCH_PHRASE, lhs, pattern, 0, NodePosition.ZERO);
        String sql = expr.toSqlImpl();
        Assertions.assertFalse(sql.contains("~"), "no slop suffix expected: " + sql);
    }

    @Test
    public void matchExprEquals_distinguishesSlop() {
        StringLiteral lhs = new StringLiteral("col");
        StringLiteral pattern = new StringLiteral("hello world");
        MatchExpr a = new MatchExpr(MatchExpr.MatchOperator.MATCH_PHRASE, lhs.clone(), pattern.clone(), 0, NodePosition.ZERO);
        MatchExpr b = new MatchExpr(MatchExpr.MatchOperator.MATCH_PHRASE, lhs.clone(), pattern.clone(), 3, NodePosition.ZERO);
        Assertions.assertNotEquals(a, b, "MatchExprs with different slop must not be equal");
    }

    @Test
    public void matchExprEquals_distinguishesOperator() {
        StringLiteral lhs = new StringLiteral("col");
        StringLiteral pattern = new StringLiteral("hello world");
        MatchExpr a = new MatchExpr(MatchExpr.MatchOperator.MATCH, lhs.clone(), pattern.clone(), 0, NodePosition.ZERO);
        MatchExpr b = new MatchExpr(MatchExpr.MatchOperator.MATCH_ALL, lhs.clone(), pattern.clone(), 0, NodePosition.ZERO);
        Assertions.assertNotEquals(a, b, "MatchExprs with different operators must not be equal");
    }

    @Test
    public void parsePhrasePattern_integration() {
        MatchExpr.PhrasePattern parsed = MatchExpr.parsePhrasePattern("foo bar ~7");
        Assertions.assertEquals("foo bar", parsed.getText());
        Assertions.assertEquals(7, parsed.getSlop());
    }

    @Test
    public void parsePhrasePattern_emptyTextDoesNotCrash() {
        // Caller (ExpressionAnalyzer) is responsible for rejecting empty text.
        // Here we only verify that parsing returns the expected (text="", slop=N).
        MatchExpr.PhrasePattern parsed = MatchExpr.parsePhrasePattern("~3");
        Assertions.assertEquals("", parsed.getText());
        Assertions.assertEquals(3, parsed.getSlop());
    }

    @Test
    public void parsePhrasePattern_slopOutOfRange() {
        Assertions.assertThrows(SemanticException.class,
                () -> MatchExpr.parsePhrasePattern("foo bar ~99999999999999"));
    }
}
