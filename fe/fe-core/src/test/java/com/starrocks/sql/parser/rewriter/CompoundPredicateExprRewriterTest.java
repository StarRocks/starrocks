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

package com.starrocks.sql.parser.rewriter;

import com.starrocks.common.Config;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompoundPredicateExprRewriterTest {

    private CompoundPredicateExprRewriter rewriter;
    private int originalThreshold;

    @BeforeEach
    public void setUp() {
        rewriter = new CompoundPredicateExprRewriter();
        // Save original threshold and set a lower value for testing
        originalThreshold = Config.compound_predicate_flatten_threshold;
        Config.compound_predicate_flatten_threshold = 3;
    }

    @AfterEach
    public void tearDown() {
        // Restore original threshold
        Config.compound_predicate_flatten_threshold = originalThreshold;
    }

    // Helper methods to create test expressions
    private SlotRef createSlotRef(String columnName) {
        return new SlotRef(new TableName("test_db", "test_table"), columnName);
    }

    private IntLiteral createIntLiteral(long value) {
        return new IntLiteral(value);
    }

    private StringLiteral createStringLiteral(String value) {
        return new StringLiteral(value);
    }

    private BoolLiteral createBoolLiteral(boolean value) {
        return new BoolLiteral(value);
    }

    private BinaryPredicate createBinaryPredicate(Expr left, BinaryType op, Expr right) {
        return new BinaryPredicate(op, left, right);
    }

    private CompoundPredicate createCompoundPredicate(CompoundPredicate.Operator op, Expr left, Expr right) {
        return new CompoundPredicate(op, left, right);
    }

    /**
     * Test that null input returns null
     */
    @Test
    public void testRewriteWithNullInput() {
        Expr result = rewriter.rewrite(null);
        assertNull(result);
    }

    /**
     * Test that non-CompoundPredicate input returns unchanged
     */
    @Test
    public void testRewriteWithNonCompoundPredicate() {
        SlotRef slotRef = createSlotRef("col1");
        Expr result = rewriter.rewrite(slotRef);
        assertSame(slotRef, result);
    }

    /**
     * Test that compound predicate below threshold returns unchanged
     */
    @Test
    public void testRewriteWithShallowPredicate() {
        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);
        IntLiteral val2 = createIntLiteral(2);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        BinaryPredicate pred2 = createBinaryPredicate(col1, BinaryType.EQ, val2);
        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.AND, pred1, pred2);

        Expr result = rewriter.rewrite(compound);
        assertSame(compound, result);
    }

    /**
     * Test that compound predicate with threshold disabled (0 or negative) returns unchanged
     */
    @Test
    public void testRewriteWithDisabledThreshold() {
        Config.compound_predicate_flatten_threshold = 0;

        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);
        IntLiteral val2 = createIntLiteral(2);
        IntLiteral val3 = createIntLiteral(3);
        IntLiteral val4 = createIntLiteral(4);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        BinaryPredicate pred2 = createBinaryPredicate(col1, BinaryType.EQ, val2);
        BinaryPredicate pred3 = createBinaryPredicate(col1, BinaryType.EQ, val3);
        BinaryPredicate pred4 = createBinaryPredicate(col1, BinaryType.EQ, val4);

        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.AND,
                createCompoundPredicate(CompoundPredicate.Operator.AND, pred1, pred2),
                createCompoundPredicate(CompoundPredicate.Operator.AND, pred3, pred4));

        Expr result = rewriter.rewrite(compound);
        assertSame(compound, result);
    }

    /**
     * Test OR to IN predicate conversion with SlotRef = LiteralExpr pattern
     */
    @Test
    public void testOrToInPredicateConversion_SlotRefEqualsLiteral() {
        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);
        IntLiteral val2 = createIntLiteral(2);
        IntLiteral val3 = createIntLiteral(3);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        BinaryPredicate pred2 = createBinaryPredicate(col1, BinaryType.EQ, val2);
        BinaryPredicate pred3 = createBinaryPredicate(col1, BinaryType.EQ, val3);
        BinaryPredicate pred4 = createBinaryPredicate(col1, BinaryType.EQ, val3);

        // Create a deep OR chain that exceeds threshold
        CompoundPredicate compound1 = createCompoundPredicate(CompoundPredicate.Operator.OR,
                createCompoundPredicate(CompoundPredicate.Operator.OR, pred1, pred2), pred3);
        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.OR,
                compound1, pred4);

        Expr result = rewriter.rewrite(compound);

        assertTrue(result instanceof InPredicate);
        InPredicate inPredicate = (InPredicate) result;
        assertEquals(col1, inPredicate.getChild(0));
        assertEquals(4, inPredicate.getInElementNum());
        assertTrue(inPredicate.getChildren().contains(val1));
        assertTrue(inPredicate.getChildren().contains(val2));
        assertTrue(inPredicate.getChildren().contains(val3));
        assertFalse(inPredicate.isNotIn());
    }

    /**
     * Test OR to IN predicate conversion with LiteralExpr = SlotRef pattern
     */
    @Test
    public void testOrToInPredicateConversion_LiteralEqualsSlotRef() {
        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);
        IntLiteral val2 = createIntLiteral(2);
        IntLiteral val3 = createIntLiteral(3);

        BinaryPredicate pred1 = createBinaryPredicate(val1, BinaryType.EQ, col1);
        BinaryPredicate pred2 = createBinaryPredicate(val2, BinaryType.EQ, col1);
        BinaryPredicate pred3 = createBinaryPredicate(val3, BinaryType.EQ, col1);
        BinaryPredicate pred4 = createBinaryPredicate(val3, BinaryType.EQ, col1);

        // Create a deep OR chain that exceeds threshold
        CompoundPredicate compound1 = createCompoundPredicate(CompoundPredicate.Operator.OR,
                createCompoundPredicate(CompoundPredicate.Operator.OR, pred1, pred2), pred3);
        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.OR,
                compound1, pred4);

        Expr result = rewriter.rewrite(compound);

        assertTrue(result instanceof InPredicate);
        InPredicate inPredicate = (InPredicate) result;
        assertEquals(col1, inPredicate.getChild(0));
        assertEquals(4, inPredicate.getInElementNum());
        assertTrue(inPredicate.getChildren().contains(val1));
        assertTrue(inPredicate.getChildren().contains(val2));
        assertTrue(inPredicate.getChildren().contains(val3));
        assertFalse(inPredicate.isNotIn());
    }

    /**
     * Test that OR conversion fails when columns are different
     */
    @Test
    public void testOrToInPredicateConversion_DifferentColumns() {
        SlotRef col1 = createSlotRef("col1");
        SlotRef col2 = createSlotRef("col2");
        IntLiteral val1 = createIntLiteral(1);
        IntLiteral val2 = createIntLiteral(2);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        BinaryPredicate pred2 = createBinaryPredicate(col2, BinaryType.EQ, val2);

        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.OR, pred1, pred2);

        Expr result = rewriter.rewrite(compound);

        // Should not convert to IN predicate, should return balanced tree
        assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate resultCompound = (CompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.OR, resultCompound.getOp());
    }

    /**
     * Test that OR conversion fails when operators are not equality
     */
    @Test
    public void testOrToInPredicateConversion_NonEqualityOperators() {
        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);
        IntLiteral val2 = createIntLiteral(2);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.LT, val1);
        BinaryPredicate pred2 = createBinaryPredicate(col1, BinaryType.GT, val2);

        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.OR, pred1, pred2);

        Expr result = rewriter.rewrite(compound);

        // Should not convert to IN predicate, should return balanced tree
        assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate resultCompound = (CompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.OR, resultCompound.getOp());
    }

    /**
     * Test that OR conversion fails when operands are not BinaryPredicates
     */
    @Test
    public void testOrToInPredicateConversion_NonBinaryPredicates() {
        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        CompoundPredicate nestedOr = createCompoundPredicate(CompoundPredicate.Operator.OR,
                createBinaryPredicate(col1, BinaryType.EQ, val1),
                createBinaryPredicate(col1, BinaryType.EQ, val1));

        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.OR, pred1, nestedOr);

        Expr result = rewriter.rewrite(compound);

        // Should not convert to IN predicate, should return balanced tree
        assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate resultCompound = (CompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.OR, resultCompound.getOp());
    }

    /**
     * Test balanced tree building for AND predicates
     */
    @Test
    public void testBalancedTreeBuilding_AndPredicates() {
        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);
        IntLiteral val2 = createIntLiteral(2);
        IntLiteral val3 = createIntLiteral(3);
        IntLiteral val4 = createIntLiteral(4);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        BinaryPredicate pred2 = createBinaryPredicate(col1, BinaryType.EQ, val2);
        BinaryPredicate pred3 = createBinaryPredicate(col1, BinaryType.EQ, val3);
        BinaryPredicate pred4 = createBinaryPredicate(col1, BinaryType.EQ, val4);

        // Create a deep AND chain that exceeds threshold
        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.AND,
                createCompoundPredicate(CompoundPredicate.Operator.AND, pred1, pred2),
                createCompoundPredicate(CompoundPredicate.Operator.AND, pred3, pred4));

        Expr result = rewriter.rewrite(compound);

        assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate resultCompound = (CompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.AND, resultCompound.getOp());

        // Verify the tree is balanced by checking depth
        assertTrue(resultCompound.getDepth() <= 3); // Should be balanced
    }

    /**
     * Test balanced tree building for OR predicates (when IN conversion fails)
     */
    @Test
    public void testBalancedTreeBuilding_OrPredicates() {
        SlotRef col1 = createSlotRef("col1");
        SlotRef col2 = createSlotRef("col2");
        IntLiteral val1 = createIntLiteral(1);
        IntLiteral val2 = createIntLiteral(2);
        IntLiteral val3 = createIntLiteral(3);
        IntLiteral val4 = createIntLiteral(4);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        BinaryPredicate pred2 = createBinaryPredicate(col2, BinaryType.EQ, val2);
        BinaryPredicate pred3 = createBinaryPredicate(col1, BinaryType.EQ, val3);
        BinaryPredicate pred4 = createBinaryPredicate(col2, BinaryType.EQ, val4);

        // Create a deep OR chain that exceeds threshold but can't be converted to IN
        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.OR,
                createCompoundPredicate(CompoundPredicate.Operator.OR, pred1, pred2),
                createCompoundPredicate(CompoundPredicate.Operator.OR, pred3, pred4));

        Expr result = rewriter.rewrite(compound);

        assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate resultCompound = (CompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.OR, resultCompound.getOp());

        // Verify the tree is balanced by checking depth
        assertTrue(resultCompound.getDepth() <= 3); // Should be balanced
    }

    /**
     * Test mixed literal types in OR to IN conversion
     */
    @Test
    public void testOrToInPredicateConversion_MixedLiteralTypes() {
        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);
        StringLiteral val2 = createStringLiteral("test");
        BoolLiteral val3 = createBoolLiteral(true);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        BinaryPredicate pred2 = createBinaryPredicate(col1, BinaryType.EQ, val2);
        BinaryPredicate pred3 = createBinaryPredicate(col1, BinaryType.EQ, val3);
        BinaryPredicate pred4 = createBinaryPredicate(col1, BinaryType.EQ, val3);

        // Create a deep OR chain that exceeds threshold
        CompoundPredicate compound1 = createCompoundPredicate(CompoundPredicate.Operator.OR,
                createCompoundPredicate(CompoundPredicate.Operator.OR, pred1, pred2), pred3);
        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.OR, compound1, pred4);
        Expr result = rewriter.rewrite(compound);

        assertTrue(result instanceof InPredicate);
        InPredicate inPredicate = (InPredicate) result;
        assertEquals(col1, inPredicate.getChild(0));
        assertEquals(4, inPredicate.getInElementNum());
        assertTrue(inPredicate.getChildren().contains(val1));
        assertTrue(inPredicate.getChildren().contains(val2));
        assertTrue(inPredicate.getChildren().contains(val3));
    }

    /**
     * Test empty operands list in OR to IN conversion
     */
    @Test
    public void testOrToInPredicateConversion_EmptyOperands() {
        // This test is more of a defensive programming test
        // The method should handle empty operands gracefully
        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.OR, pred1, pred1);

        Expr result = rewriter.rewrite(compound);

        // Should return the original compound predicate since it doesn't exceed threshold
        assertSame(compound, result);
    }

    /**
     * Test that NOT operator is not processed for balancing
     */
    @Test
    public void testNotOperatorNotProcessed() {
        SlotRef col1 = createSlotRef("col1");
        IntLiteral val1 = createIntLiteral(1);
        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);

        CompoundPredicate notPredicate = createCompoundPredicate(CompoundPredicate.Operator.NOT, pred1, null);

        Expr result = rewriter.rewrite(notPredicate);

        // NOT predicates should not be processed for balancing
        assertSame(notPredicate, result);
    }

    /**
     * Test complex nested structure with mixed operators
     */
    @Test
    public void testComplexNestedStructure() {
        SlotRef col1 = createSlotRef("col1");
        SlotRef col2 = createSlotRef("col2");
        IntLiteral val1 = createIntLiteral(1);
        IntLiteral val2 = createIntLiteral(2);
        IntLiteral val3 = createIntLiteral(3);
        IntLiteral val4 = createIntLiteral(4);
        IntLiteral val5 = createIntLiteral(5);

        BinaryPredicate pred1 = createBinaryPredicate(col1, BinaryType.EQ, val1);
        BinaryPredicate pred2 = createBinaryPredicate(col1, BinaryType.EQ, val2);
        BinaryPredicate pred3 = createBinaryPredicate(col1, BinaryType.EQ, val3);
        BinaryPredicate pred4 = createBinaryPredicate(col1, BinaryType.EQ, val3);

        BinaryPredicate pred5 = createBinaryPredicate(col2, BinaryType.EQ, val4);

        // Create a complex nested structure
        CompoundPredicate leftOr1 = createCompoundPredicate(CompoundPredicate.Operator.OR,
                createCompoundPredicate(CompoundPredicate.Operator.OR, pred1, pred2), pred3);
        CompoundPredicate leftOr = createCompoundPredicate(CompoundPredicate.Operator.OR, leftOr1, pred4);

        CompoundPredicate rightOr = createCompoundPredicate(CompoundPredicate.Operator.OR, pred4, pred5);
        CompoundPredicate compound = createCompoundPredicate(CompoundPredicate.Operator.AND, leftOr, rightOr);

        Expr result = rewriter.rewrite(compound);

        // The left OR should be converted to IN predicate, right OR should be balanced
        assertTrue(result instanceof CompoundPredicate);
        CompoundPredicate resultCompound = (CompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.AND, resultCompound.getOp());

        // Left child should be IN predicate
        assertTrue(resultCompound.getChild(0) instanceof InPredicate);
        InPredicate leftIn = (InPredicate) resultCompound.getChild(0);
        assertEquals(col1, leftIn.getChild(0));
        assertEquals(4, leftIn.getInElementNum());

        // Right child should be balanced OR
        assertTrue(resultCompound.getChild(1) instanceof CompoundPredicate);
        CompoundPredicate rightResult = (CompoundPredicate) resultCompound.getChild(1);
        assertEquals(CompoundPredicate.Operator.OR, rightResult.getOp());
    }
}
