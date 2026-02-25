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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.starrocks.type.UnknownType.UNKNOWN_TYPE;

public class PredicateOperatorTest {

    // Concrete implementation of ExistsPredicateOperator for testing
    private static class TestExistsPredicateOperator extends ExistsPredicateOperator {
        public TestExistsPredicateOperator(boolean notExists, ScalarOperator... arguments) {
            super(notExists, arguments);
        }

        @Override
        public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
            return null;
        }
    }

    // ==================== PredicateOperator Base Class Tests ====================

    // Concrete implementation of PredicateOperator for testing
    private static class TestPredicateOperator extends PredicateOperator {
        public TestPredicateOperator(OperatorType operatorType, ScalarOperator... arguments) {
            super(operatorType, arguments);
        }

        @Override
        public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
            return null;
        }
    }

    @Test
    public void testEquivalentWithDifferentArgumentSizes() {
        // Create two predicate operators with different numbers of arguments
        ScalarOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "col1", true);
        ScalarOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "col2", true);
        ScalarOperator col3 = new ColumnRefOperator(3, IntegerType.INT, "col3", true);

        // One argument
        TestPredicateOperator op1 = new TestPredicateOperator(OperatorType.BINARY, col1);

        // Three arguments
        TestPredicateOperator op2 = new TestPredicateOperator(OperatorType.BINARY, col1, col2, col3);

        Assertions.assertFalse(op1.equivalent(op2),
                "PredicateOperators with different argument sizes should not be equivalent");

        // Test the reverse direction as well
        Assertions.assertFalse(op2.equivalent(op1),
                "PredicateOperators with different argument sizes should not be equivalent");
    }

    @Test
    public void testEquivalentWithSameArgumentSizes() {
        // Create two predicate operators with the same number of arguments
        ScalarOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "col1", true);
        ScalarOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "col2", true);
        ScalarOperator col3 = new ColumnRefOperator(3, IntegerType.INT, "col3", true);
        ScalarOperator col4 = new ColumnRefOperator(4, IntegerType.INT, "col4", true);

        // Both have two arguments
        TestPredicateOperator op1 = new TestPredicateOperator(OperatorType.BINARY, col1, col2);
        TestPredicateOperator op2 = new TestPredicateOperator(OperatorType.BINARY, col3, col4);

        Assertions.assertFalse(op1.equivalent(op2),
                "PredicateOperators with same argument sizes but different arguments should not be equivalent");

        // Now test with same arguments
        TestPredicateOperator op3 = new TestPredicateOperator(OperatorType.BINARY, col1, col2);
        Assertions.assertTrue(op1.equivalent(op3),
                "PredicateOperators with same argument sizes and arguments should be equivalent");
    }

    // ==================== BetweenPredicateOperator Tests ====================

    @Test
    public void testBetweenEquivalentWithDifferentNotBetweenFlag() {
        // Create two BetweenPredicateOperator instances with same arguments but different notBetween flag
        ScalarOperator value = new ColumnRefOperator(1, IntegerType.INT, "value", true);
        ScalarOperator lower = new ColumnRefOperator(2, IntegerType.INT, "lower", true);
        ScalarOperator upper = new ColumnRefOperator(3, IntegerType.INT, "upper", true);

        // One with notBetween = false (BETWEEN)
        BetweenPredicateOperator betweenOp = new BetweenPredicateOperator(false, value, lower, upper);

        // One with notBetween = true (NOT BETWEEN)
        BetweenPredicateOperator notBetweenOp = new BetweenPredicateOperator(true, value, lower, upper);

        // They should not be equivalent because notBetween flags are different
        Assertions.assertFalse(betweenOp.equivalent(notBetweenOp),
                "BetweenPredicateOperators with different notBetween flags should not be equivalent");

        // Test the reverse direction as well
        Assertions.assertFalse(notBetweenOp.equivalent(betweenOp),
                "BetweenPredicateOperators with different notBetween flags should not be equivalent");
    }

    @Test
    public void testBetweenEquivalentWithSameNotBetweenFlag() {
        // Create two BetweenPredicateOperator instances with same arguments and same notBetween flag
        ScalarOperator value1 = new ColumnRefOperator(1, IntegerType.INT, "value", true);
        ScalarOperator lower1 = new ColumnRefOperator(2, IntegerType.INT, "lower", true);
        ScalarOperator upper1 = new ColumnRefOperator(3, IntegerType.INT, "upper", true);

        ScalarOperator value2 = new ColumnRefOperator(4, IntegerType.INT, "value2", true);
        ScalarOperator lower2 = new ColumnRefOperator(5, IntegerType.INT, "lower2", true);
        ScalarOperator upper2 = new ColumnRefOperator(6, IntegerType.INT, "upper2", true);

        // Both with notBetween = false
        BetweenPredicateOperator op1 = new BetweenPredicateOperator(false, value1, lower1, upper1);
        BetweenPredicateOperator op2 = new BetweenPredicateOperator(false, value2, lower2, upper2);

        // They have the same notBetween flag but different arguments, so they should not be equivalent
        Assertions.assertFalse(op1.equivalent(op2),
                "BetweenPredicateOperators with same notBetween flags but different arguments should not be equivalent");

        // Now test with same arguments
        BetweenPredicateOperator op3 = new BetweenPredicateOperator(false, value1, lower1, upper1);
        Assertions.assertTrue(op1.equivalent(op3),
                "BetweenPredicateOperators with same notBetween flags and arguments should be equivalent");
    }

    // ==================== BinaryPredicateOperator Tests ====================

    @Test
    public void testBinaryEquivalentWithDifferentBinaryTypes() {
        // Create two BinaryPredicateOperator instances with same arguments but different binary types
        ScalarOperator left = new ColumnRefOperator(1, IntegerType.INT, "left", true);
        ScalarOperator right = new ColumnRefOperator(2, IntegerType.INT, "right", true);

        // One with EQ type
        BinaryPredicateOperator eqOp = new BinaryPredicateOperator(BinaryType.EQ, left, right);

        // One with NE type
        BinaryPredicateOperator neOp = new BinaryPredicateOperator(BinaryType.NE, left, right);

        // They should not be equivalent because binary types are different
        Assertions.assertFalse(eqOp.equivalent(neOp),
                "BinaryPredicateOperators with different binary types should not be equivalent");

        // Test the reverse direction as well
        Assertions.assertFalse(neOp.equivalent(eqOp),
                "BinaryPredicateOperators with different binary types should not be equivalent");
    }

    @Test
    public void testBinaryEquivalentWithSameBinaryType() {
        // Create two BinaryPredicateOperator instances with same binary type but different arguments
        ScalarOperator left1 = new ColumnRefOperator(1, IntegerType.INT, "left1", true);
        ScalarOperator right1 = new ColumnRefOperator(2, IntegerType.INT, "right1", true);

        ScalarOperator left2 = new ColumnRefOperator(3, IntegerType.INT, "left2", true);
        ScalarOperator right2 = new ColumnRefOperator(4, IntegerType.INT, "right2", true);

        // Both with EQ type
        BinaryPredicateOperator op1 = new BinaryPredicateOperator(BinaryType.EQ, left1, right1);
        BinaryPredicateOperator op2 = new BinaryPredicateOperator(BinaryType.EQ, left2, right2);

        // They have the same binary type but different arguments, so they should not be equivalent
        Assertions.assertFalse(op1.equivalent(op2),
                "BinaryPredicateOperators with same binary type but different arguments should not be equivalent");

        // Now test with same arguments
        BinaryPredicateOperator op3 = new BinaryPredicateOperator(BinaryType.EQ, left1, right1);
        Assertions.assertTrue(op1.equivalent(op3),
                "BinaryPredicateOperators with same binary type and arguments should be equivalent");
    }

    // ==================== CompoundPredicateOperator Tests ====================

    @Test
    public void testCompoundEquivalentWithDifferentCompoundTypes() {
        // Create two CompoundPredicateOperator instances with same arguments but different compound types
        ScalarOperator left = new ColumnRefOperator(1, BooleanType.BOOLEAN, "left", true);
        ScalarOperator right = new ColumnRefOperator(2, BooleanType.BOOLEAN, "right", true);

        // One with AND type
        CompoundPredicateOperator andOp = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, left, right);

        // One with OR type
        CompoundPredicateOperator orOp = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, left, right);

        // They should not be equivalent because compound types are different
        Assertions.assertFalse(andOp.equivalent(orOp),
                "CompoundPredicateOperators with different compound types should not be equivalent");

        // Test the reverse direction as well
        Assertions.assertFalse(orOp.equivalent(andOp),
                "CompoundPredicateOperators with different compound types should not be equivalent");
    }

    @Test
    public void testCompoundEquivalentWithSameCompoundType() {
        // Create two CompoundPredicateOperator instances with same compound type but different arguments
        ScalarOperator left1 = new ColumnRefOperator(1, BooleanType.BOOLEAN, "left1", true);
        ScalarOperator right1 = new ColumnRefOperator(2, BooleanType.BOOLEAN, "right1", true);

        ScalarOperator left2 = new ColumnRefOperator(3, BooleanType.BOOLEAN, "left2", true);
        ScalarOperator right2 = new ColumnRefOperator(4, BooleanType.BOOLEAN, "right2", true);

        // Both with AND type
        CompoundPredicateOperator op1 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, left1, right1);
        CompoundPredicateOperator op2 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, left2, right2);

        // They have the same compound type but different arguments, so they should not be equivalent
        Assertions.assertFalse(op1.equivalent(op2),
                "CompoundPredicateOperators with same compound type but different arguments should not be equivalent");

        // Now test with same arguments
        CompoundPredicateOperator op3 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, left1, right1);
        Assertions.assertTrue(op1.equivalent(op3),
                "CompoundPredicateOperators with same compound type and arguments should be equivalent");
    }

    // ==================== ExistsPredicateOperator Tests ====================

    @Test
    public void testExistsEquivalentWithDifferentNotExistsFlag() {
        // Create two ExistsPredicateOperator instances with same arguments but different notExists flag
        ScalarOperator subquery = new ColumnRefOperator(1, UNKNOWN_TYPE, "subquery", true);

        // One with notExists = false (EXISTS)
        ExistsPredicateOperator existsOp = new TestExistsPredicateOperator(false, subquery);

        // One with notExists = true (NOT EXISTS)
        ExistsPredicateOperator notExistsOp = new TestExistsPredicateOperator(true, subquery);

        // They should not be equivalent because notExists flags are different
        Assertions.assertFalse(existsOp.equivalent(notExistsOp),
                "ExistsPredicateOperators with different notExists flags should not be equivalent");

        // Test the reverse direction as well
        Assertions.assertFalse(notExistsOp.equivalent(existsOp),
                "ExistsPredicateOperators with different notExists flags should not be equivalent");
    }

    @Test
    public void testExistsEquivalentWithSameNotExistsFlag() {
        // Create two ExistsPredicateOperator instances with same notExists flag but different arguments
        ScalarOperator subquery1 = new ColumnRefOperator(1, UNKNOWN_TYPE, "subquery1", true);
        ScalarOperator subquery2 = new ColumnRefOperator(2, UNKNOWN_TYPE, "subquery2", true);

        // Both with notExists = false
        ExistsPredicateOperator op1 = new TestExistsPredicateOperator(false, subquery1);
        ExistsPredicateOperator op2 = new TestExistsPredicateOperator(false, subquery2);

        // They have the same notExists flag but different arguments, so they should not be equivalent
        Assertions.assertFalse(op1.equivalent(op2),
                "ExistsPredicateOperators with same notExists flags but different arguments should not be equivalent");

        // Now test with same arguments
        ExistsPredicateOperator op3 = new TestExistsPredicateOperator(false, subquery1);
        Assertions.assertTrue(op1.equivalent(op3),
                "ExistsPredicateOperators with same notExists flags and arguments should be equivalent");
    }

    // ==================== InPredicateOperator Tests ====================

    @Test
    public void testInEquivalentWithDifferentIsNotInFlags() {
        // Create two InPredicateOperator instances with same arguments but different isNotIn flags
        ScalarOperator column = new ColumnRefOperator(1, IntegerType.INT, "column", true);
        ScalarOperator value1 = new ColumnRefOperator(2, IntegerType.INT, "value1", true);
        ScalarOperator value2 = new ColumnRefOperator(3, IntegerType.INT, "value2", true);

        // One with isNotIn = false (IN)
        InPredicateOperator inOp = new InPredicateOperator(false, column, value1, value2);

        // One with isNotIn = true (NOT IN)
        InPredicateOperator notInOp = new InPredicateOperator(true, column, value1, value2);

        // They should not be equivalent because isNotIn flags are different
        Assertions.assertFalse(inOp.equivalent(notInOp),
                "InPredicateOperators with different isNotIn flags should not be equivalent");

        // Test the reverse direction as well
        Assertions.assertFalse(notInOp.equivalent(inOp),
                "InPredicateOperators with different isNotIn flags should not be equivalent");
    }

    @Test
    public void testInEquivalentWithSameIsNotInFlag() {
        // Create two InPredicateOperator instances with same isNotIn flag but different arguments
        ScalarOperator column1 = new ColumnRefOperator(1, IntegerType.INT, "column1", true);
        ScalarOperator value1 = new ColumnRefOperator(2, IntegerType.INT, "value1", true);
        ScalarOperator value2 = new ColumnRefOperator(3, IntegerType.INT, "value2", true);

        ScalarOperator column2 = new ColumnRefOperator(4, IntegerType.INT, "column2", true);
        ScalarOperator value3 = new ColumnRefOperator(5, IntegerType.INT, "value3", true);
        ScalarOperator value4 = new ColumnRefOperator(6, IntegerType.INT, "value4", true);

        // Both with isNotIn = false
        InPredicateOperator op1 = new InPredicateOperator(false, column1, value1, value2);
        InPredicateOperator op2 = new InPredicateOperator(false, column2, value3, value4);

        // They have the same isNotIn flag but different arguments, so they should not be equivalent
        Assertions.assertFalse(op1.equivalent(op2),
                "InPredicateOperators with same isNotIn flags but different arguments should not be equivalent");

        // Now test with same arguments
        InPredicateOperator op3 = new InPredicateOperator(false, column1, value1, value2);
        Assertions.assertTrue(op1.equivalent(op3),
                "InPredicateOperators with same isNotIn flags and arguments should be equivalent");
    }

    // ==================== IsNullPredicateOperator Tests ====================

    @Test
    public void testIsNullEquivalentWithDifferentIsNotNullFlags() {
        // Create two IsNullPredicateOperator instances with same arguments but different isNotNull flags
        ScalarOperator column = new ColumnRefOperator(1, IntegerType.INT, "column", true);

        // One with isNotNull = false (IS NULL)
        IsNullPredicateOperator nullOp = new IsNullPredicateOperator(false, column);

        // One with isNotNull = true (IS NOT NULL)
        IsNullPredicateOperator notNullOp = new IsNullPredicateOperator(true, column);

        // They should not be equivalent because isNotNull flags are different
        Assertions.assertFalse(nullOp.equivalent(notNullOp),
                "IsNullPredicateOperators with different isNotNull flags should not be equivalent");

        // Test the reverse direction as well
        Assertions.assertFalse(notNullOp.equivalent(nullOp),
                "IsNullPredicateOperators with different isNotNull flags should not be equivalent");
    }

    @Test
    public void testIsNullEquivalentWithSameIsNotNullFlag() {
        // Create two IsNullPredicateOperator instances with same isNotNull flag but different arguments
        ScalarOperator column1 = new ColumnRefOperator(1, IntegerType.INT, "column1", true);
        ScalarOperator column2 = new ColumnRefOperator(2, IntegerType.INT, "column2", true);

        // Both with isNotNull = false
        IsNullPredicateOperator op1 = new IsNullPredicateOperator(false, column1);
        IsNullPredicateOperator op2 = new IsNullPredicateOperator(false, column2);

        // They have the same isNotNull flag but different arguments, so they should not be equivalent
        Assertions.assertFalse(op1.equivalent(op2),
                "IsNullPredicateOperators with same isNotNull flags but different arguments should not be equivalent");

        // Now test with same arguments
        IsNullPredicateOperator op3 = new IsNullPredicateOperator(false, column1);
        Assertions.assertTrue(op1.equivalent(op3),
                "IsNullPredicateOperators with same isNotNull flags and arguments should be equivalent");
    }

    // ==================== LikePredicateOperator Tests ====================

    @Test
    public void testLikeEquivalentWithDifferentLikeTypes() {
        // Create two LikePredicateOperator instances with same arguments but different like types
        ScalarOperator column = new ColumnRefOperator(1, VarcharType.VARCHAR, "column", true);
        ScalarOperator pattern = new ColumnRefOperator(2, VarcharType.VARCHAR, "pattern", true);

        // One with LIKE type
        LikePredicateOperator likeOp = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, column, pattern);

        // One with REGEXP type
        LikePredicateOperator regexpOp = new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, column, pattern);

        // They should not be equivalent because like types are different
        Assertions.assertFalse(likeOp.equivalent(regexpOp),
                "LikePredicateOperators with different like types should not be equivalent");

        // Test the reverse direction as well
        Assertions.assertFalse(regexpOp.equivalent(likeOp),
                "LikePredicateOperators with different like types should not be equivalent");
    }

    @Test
    public void testLikeEquivalentWithSameLikeType() {
        // Create two LikePredicateOperator instances with same like type but different arguments
        ScalarOperator column1 = new ColumnRefOperator(1, VarcharType.VARCHAR, "column1", true);
        ScalarOperator pattern1 = new ColumnRefOperator(2, VarcharType.VARCHAR, "pattern1", true);

        ScalarOperator column2 = new ColumnRefOperator(3, VarcharType.VARCHAR, "column2", true);
        ScalarOperator pattern2 = new ColumnRefOperator(4, VarcharType.VARCHAR, "pattern2", true);

        // Both with LIKE type
        LikePredicateOperator op1 = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, column1, pattern1);
        LikePredicateOperator op2 = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, column2, pattern2);

        // They have the same like type but different arguments, so they should not be equivalent
        Assertions.assertFalse(op1.equivalent(op2),
                "LikePredicateOperators with same like type but different arguments should not be equivalent");

        // Now test with same arguments
        LikePredicateOperator op3 = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, column1, pattern1);
        Assertions.assertTrue(op1.equivalent(op3),
                "LikePredicateOperators with same like type and arguments should be equivalent");
    }
}