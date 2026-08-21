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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MatchExprOperator}. This operator is the bridge between the AST-level
 * {@link MatchExpr} and the planner/executor; it is therefore essential that its
 * {@code matchOperator} field (in particular {@code MATCH_PHRASE}) round-trips correctly through
 * cloning, equality, and visitor dispatch. The pre-existing test suite for this class was empty.
 */
public class MatchExprOperatorTest {

    private static ColumnRefOperator newCol(int id, String name) {
        return new ColumnRefOperator(id, VarcharType.VARCHAR, name, /*nullable=*/true);
    }

    // The constructor must record the MatchOperator verbatim so that downstream visitors
    // can dispatch on it. Without this, all four MatchExpr variants would collapse into the
    // default MATCH and MATCH_PHRASE would silently lose its phrase semantics.
    @Test
    public void testConstructorPreservesMatchOperator() {
        ColumnRefOperator col = newCol(1, "content");
        ConstantOperator literal = ConstantOperator.createVarchar("hello world");

        for (MatchExpr.MatchOperator op : MatchExpr.MatchOperator.values()) {
            MatchExprOperator expr = new MatchExprOperator(op, col, literal);
            Assertions.assertEquals(op, expr.getMatchOperator(), "matchOperator must round-trip");
            Assertions.assertEquals(OperatorType.MATCH_EXPR, expr.getOpType());
            Assertions.assertEquals(2, expr.getChildren().size());
            Assertions.assertSame(col, expr.getChild(0));
            Assertions.assertSame(literal, expr.getChild(1));
        }
    }

    // clone() must deep-copy the children AND preserve matchOperator. A shallow clone that
    // drops matchOperator would silently demote MATCH_PHRASE to the default MATCH.
    @Test
    public void testCloneRoundTripsMatchPhrase() {
        ColumnRefOperator col = newCol(1, "content");
        ConstantOperator literal = ConstantOperator.createVarchar("inverted index");
        MatchExprOperator original = new MatchExprOperator(
                MatchExpr.MatchOperator.MATCH_PHRASE, col, literal);

        MatchExprOperator cloned = (MatchExprOperator) original.clone();

        Assertions.assertNotSame(original, cloned);
        Assertions.assertEquals(MatchExpr.MatchOperator.MATCH_PHRASE, cloned.getMatchOperator());
        Assertions.assertEquals(original, cloned);
        // Deep copy: children are independent instances, but equal.
        Assertions.assertEquals(original.getChild(0), cloned.getChild(0));
        Assertions.assertEquals(original.getChild(1), cloned.getChild(1));
    }

    // equals() must distinguish two MatchExprOperators that have identical children but
    // different match operators (MATCH vs MATCH_PHRASE). This protects rewrite rules that
    // dedupe predicates by equals().
    @Test
    public void testEqualsDistinguishesMatchOperators() {
        ColumnRefOperator col = newCol(1, "content");
        ConstantOperator literal = ConstantOperator.createVarchar("hello world");

        MatchExprOperator asMatch = new MatchExprOperator(MatchExpr.MatchOperator.MATCH, col, literal);
        MatchExprOperator asPhrase = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_PHRASE, col, literal);
        MatchExprOperator asAny = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_ANY, col, literal);
        MatchExprOperator asAll = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_ALL, col, literal);

        Assertions.assertNotEquals(asMatch, asPhrase);
        Assertions.assertNotEquals(asPhrase, asAny);
        Assertions.assertNotEquals(asPhrase, asAll);
        // Reflexivity sanity check.
        Assertions.assertEquals(asPhrase, asPhrase);
        Assertions.assertEquals(asPhrase, new MatchExprOperator(MatchExpr.MatchOperator.MATCH_PHRASE, col, literal));
    }

    // The base ScalarOperator contract requires that equal objects have equal hashCodes.
    // Note: MatchExprOperator's hashCode is intentionally derived from arguments only (not
    // matchOperator), so two operators with the same children but different operators
    // share a hashCode -- equals() is the discriminator. We test the contract direction
    // that the spec actually mandates: equal objects produce equal hashCodes.
    @Test
    public void testEqualObjectsHaveEqualHashCodes() {
        ColumnRefOperator col = newCol(1, "content");
        ConstantOperator literal = ConstantOperator.createVarchar("hello world");

        MatchExprOperator a = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_PHRASE, col, literal);
        MatchExprOperator b = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_PHRASE, col, literal);

        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }

    // Visitor dispatch: accept() must route into visitMatchExprOperator. This was
    // implicitly tested by the planner end-to-end but is now pinned down here so that a
    // future refactor of ScalarOperatorVisitor won't silently break MATCH_PHRASE.
    @Test
    public void testVisitorDispatchRoutesToMatchExprOperator() {
        ColumnRefOperator col = newCol(1, "content");
        ConstantOperator literal = ConstantOperator.createVarchar("inverted index");
        MatchExprOperator expr = new MatchExprOperator(
                MatchExpr.MatchOperator.MATCH_PHRASE, col, literal);

        ScalarOperatorVisitor<MatchExpr.MatchOperator, Void> visitor =
                new ScalarOperatorVisitor<MatchExpr.MatchOperator, Void>() {
                    @Override
                    public MatchExpr.MatchOperator visit(ScalarOperator scalarOperator, Void context) {
                        Assertions.fail("default visit should not be called for MatchExprOperator");
                        return null;
                    }

                    @Override
                    public MatchExpr.MatchOperator visitMatchExprOperator(MatchExprOperator predicate, Void context) {
                        return predicate.getMatchOperator();
                    }
                };

        Assertions.assertEquals(MatchExpr.MatchOperator.MATCH_PHRASE, expr.accept(visitor, null));
    }

    // toString() and debugString() must include the operator name (MATCH_PHRASE etc.) so
    // EXPLAIN output and planner logs surface the right intent.
    @Test
    public void testToStringContainsOperatorName() {
        ColumnRefOperator col = newCol(7, "content");
        ConstantOperator literal = ConstantOperator.createVarchar("inverted index");
        MatchExprOperator expr = new MatchExprOperator(
                MatchExpr.MatchOperator.MATCH_PHRASE, col, literal);

        String s = expr.toString();
        Assertions.assertTrue(s.contains("MATCH_PHRASE"),
                "toString must contain operator name, got: " + s);
        Assertions.assertTrue(s.contains("inverted index"),
                "toString must contain literal, got: " + s);

        String ds = expr.debugString();
        Assertions.assertTrue(ds.contains("MATCH_PHRASE"),
                "debugString must contain operator name, got: " + ds);
    }

    // The match column must surface in getUsedColumns(); without this the optimizer's
    // pushdown / column-prune analysis would fail to recognize MATCH_PHRASE as referencing
    // the column.
    @Test
    public void testGetUsedColumnsIncludesColumnRef() {
        ColumnRefOperator col = newCol(42, "content");
        ConstantOperator literal = ConstantOperator.createVarchar("hello");
        MatchExprOperator expr = new MatchExprOperator(
                MatchExpr.MatchOperator.MATCH_PHRASE, col, literal);

        ColumnRefSet used = expr.getUsedColumns();
        Assertions.assertTrue(used.contains(42), "MATCH_PHRASE's column must appear in used columns");
        Assertions.assertEquals(1, used.size());
    }

    // Nullability propagation: a MATCH_PHRASE on a nullable column produces a potentially
    // null result and must reflect that in isNullable(). This matters for filter/projection
    // type inference downstream.
    @Test
    public void testIsNullablePropagatesFromChildren() {
        ColumnRefOperator nullableCol = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", /*nullable=*/true);
        ColumnRefOperator notNullCol = new ColumnRefOperator(2, VarcharType.VARCHAR, "c", /*nullable=*/false);
        ConstantOperator literal = ConstantOperator.createVarchar("x");

        Assertions.assertTrue(new MatchExprOperator(
                MatchExpr.MatchOperator.MATCH_PHRASE, nullableCol, literal).isNullable());
        Assertions.assertFalse(new MatchExprOperator(
                MatchExpr.MatchOperator.MATCH_PHRASE, notNullCol, literal).isNullable());
    }

    // Type column type does not affect MATCH_PHRASE semantics (still BOOLEAN), but exercising
    // a non-string left operand is useful to confirm the operator itself does not assert on
    // child types: those checks live in ExpressionAnalyzer.visitMatchExpr, not here.
    @Test
    public void testConstructorAcceptsNonStringChildrenWithoutAssertion() {
        ColumnRefOperator intCol = new ColumnRefOperator(1, IntegerType.INT, "n", true);
        ConstantOperator literal = ConstantOperator.createVarchar("x");
        // The operator itself only enforces children.size() == 2; semantic type checks live
        // in the analyzer. Constructing here must not throw.
        Assertions.assertDoesNotThrow(() -> new MatchExprOperator(
                MatchExpr.MatchOperator.MATCH_PHRASE, intCol, literal));
    }
}
