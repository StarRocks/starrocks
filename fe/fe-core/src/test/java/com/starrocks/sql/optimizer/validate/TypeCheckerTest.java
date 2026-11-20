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

package com.starrocks.sql.optimizer.validate;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypeCheckerTest {

    @Test
    void testHashJoinEqualPredicateTypeMismatch() {
        {
            OptExpression root = generateHashJoinExpr(IntegerType.BIGINT, VarcharType.VARCHAR);
            StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                    () -> TypeChecker.getInstance().validate(root, null));
            assertTrue(ex.getMessage().contains("hash join equal predicate type mismatch"),
                    "Unexpected error: " + ex.getMessage());
        }

        {
            OptExpression root = generateHashJoinExpr(IntegerType.BIGINT, IntegerType.INT);
            StarRocksPlannerException ex = assertThrows(StarRocksPlannerException.class,
                    () -> TypeChecker.getInstance().validate(root, null));
            assertTrue(ex.getMessage().contains("hash join equal predicate type mismatch"),
                    "Unexpected error: " + ex.getMessage());
        }

        {
            OptExpression root = generateHashJoinExpr(IntegerType.BIGINT, IntegerType.BIGINT);
            assertDoesNotThrow(() -> TypeChecker.getInstance().validate(root, null));
        }
    }

    @Test
    void testAggregateSignatureWithWildcardDecimal() {
        Type concrete = DecimalType.DEFAULT_DECIMAL128;
        Type wildcard = DecimalType.DECIMAL128;
        assertDoesNotThrow(() -> TypeChecker.getInstance().validate(generateAggExpr(concrete, concrete), null));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> TypeChecker.getInstance().validate(generateAggExpr(wildcard, concrete), null));
        assertTrue(ex.getMessage().contains("wildcard decimal return type"), "Unexpected error: " + ex.getMessage());

        ex = assertThrows(IllegalArgumentException.class,
                () -> TypeChecker.getInstance().validate(generateAggExpr(concrete, wildcard), null));
        assertTrue(ex.getMessage().contains("wildcard decimal intermediate type"), "Unexpected error: " + ex.getMessage());
    }

    @Test
    void testAnalyticSignatureWithWildcardDecimal() {
        Type concrete = new ArrayType(DecimalType.DEFAULT_DECIMAL128);
        Type wildcard = new ArrayType(DecimalType.DECIMAL128);
        assertDoesNotThrow(() -> TypeChecker.getInstance().validate(generateWindowExpr(concrete), null));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> TypeChecker.getInstance().validate(generateWindowExpr(wildcard), null));
        assertTrue(ex.getMessage().contains("wildcard decimal return type"), "Unexpected error: " + ex.getMessage());
    }

    private static OptExpression generateAggExpr(Type retType, Type intermediateType) {
        Type argType = DecimalType.DEFAULT_DECIMAL128;
        ColumnRefOperator arg = new ColumnRefOperator(1, argType, "d", true);
        ColumnRefOperator output = new ColumnRefOperator(2, argType, "sum", true);
        AggregateFunction fn = AggregateFunction.createBuiltin(FunctionSet.SUM, List.of(argType), retType,
                intermediateType, false, true, false);
        CallOperator call = new CallOperator(FunctionSet.SUM, argType, List.of(arg), fn);
        LogicalAggregationOperator agg = new LogicalAggregationOperator(AggType.GLOBAL, List.of(),
                ImmutableMap.of(output, call));
        return OptExpression.create(agg);
    }

    private static OptExpression generateWindowExpr(Type retType) {
        Type argType = new ArrayType(DecimalType.DEFAULT_DECIMAL128);
        ColumnRefOperator arg = new ColumnRefOperator(1, argType, "a", true);
        ColumnRefOperator output = new ColumnRefOperator(2, argType, "last_value", true);
        AggregateFunction fn = AggregateFunction.createAnalyticBuiltin(FunctionSet.LAST_VALUE, List.of(argType),
                retType, retType);
        CallOperator call = new CallOperator(FunctionSet.LAST_VALUE, argType, List.of(arg), fn);
        LogicalWindowOperator window = new LogicalWindowOperator.Builder()
                .setWindowCall(ImmutableMap.of(output, call))
                .build();
        return OptExpression.create(window);
    }

    private static OptExpression generateHashJoinExpr(Type leftType, Type rightType) {
        ColumnRefOperator leftCol = new ColumnRefOperator(1, leftType, "l", true);
        ColumnRefOperator rightCol = new ColumnRefOperator(2, rightType, "r", true);
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, rightCol, leftCol);

        PhysicalHashJoinOperator join = new PhysicalHashJoinOperator(
                JoinOperator.INNER_JOIN,
                predicate,
                "",
                Operator.DEFAULT_LIMIT,
                null,
                null,
                null,
                null,
                null);

        OptExpression left = new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES));
        left.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(leftCol)));

        OptExpression right = new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES));
        right.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(rightCol)));

        return OptExpression.create(join, left, right);
    }
}
