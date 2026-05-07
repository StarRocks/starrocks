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

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

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
                null);

        OptExpression left = new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES));
        left.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(leftCol)));

        OptExpression right = new OptExpression(new MockOperator(OperatorType.LOGICAL_VALUES));
        right.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(rightCol)));

        return OptExpression.create(join, left, right);
    }
}
