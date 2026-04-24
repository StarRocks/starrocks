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


package com.starrocks.sql.optimizer.operator.operator;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LambdaFunctionOperatorTest {
    @Test
    public void lambdaFunction() {
        ScalarOperator lambdaExpr = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, IntegerType.INT, "x", true),
                ConstantOperator.createInt(1));
        ColumnRefOperator colRef = new ColumnRefOperator(1, IntegerType.INT, "x", true, true);
        LambdaFunctionOperator lambda = new LambdaFunctionOperator(Lists.newArrayList(colRef), lambdaExpr, BooleanType.BOOLEAN);
        Assertions.assertTrue(lambda.getChild(0).equals(lambdaExpr));
        Assertions.assertTrue(lambda.getLambdaExpr().equals(lambdaExpr));
        Assertions.assertTrue(lambda.getRefColumns().get(0).getName() == "x");
        Assertions.assertTrue(lambda.getChildren().size() == 1);
        Assertions.assertTrue(lambda.getUsedColumns().getFirstId() == 1);
        Assertions.assertTrue(lambda.isNullable());
        Assertions.assertEquals("([1: x]->1: x = 1)", lambda.toString());
        Assertions.assertTrue(lambda.equals(lambda.clone()));
    }

    @Test
    void testArgumentDependency() {
        // GIVEN: lambda (x) -> x = 1, where x has the same id in refColumns and in the expression
        var lambdaColRef = new ColumnRefOperator(1, IntegerType.INT, "x", true, true);
        PredicateOperator lambdaExpr = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, IntegerType.INT, "x", true),
                ConstantOperator.createInt(1));
        var lambda = new LambdaFunctionOperator(Lists.newArrayList(lambdaColRef), lambdaExpr, BooleanType.BOOLEAN);

        // WHEN / THEN
        assertEquals(1, lambda.getNumberOfDependentArguments());
        assertFalse(lambda.isIndependentOfArguments());

        // GIVEN: lambda (x, y) -> x = y, both arguments used
        var lambdaColRef2 = new ColumnRefOperator(2, IntegerType.INT, "y", true, true);
        lambdaExpr = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, IntegerType.INT, "x", true),
                new ColumnRefOperator(2, IntegerType.INT, "y", true));
        lambda = new LambdaFunctionOperator(List.of(lambdaColRef, lambdaColRef2), lambdaExpr, BooleanType.BOOLEAN);

        // WHEN / THEN
        assertEquals(2, lambda.getNumberOfDependentArguments());
        assertFalse(lambda.isIndependentOfArguments());

        // GIVEN: lambda (x) -> IS NULL(1), argument not used
        lambdaExpr = new IsNullPredicateOperator(ConstantOperator.createInt(1));
        lambda = new LambdaFunctionOperator(List.of(lambdaColRef), lambdaExpr, BooleanType.BOOLEAN);
        // WHEN / THEN
        assertEquals(0, lambda.getNumberOfDependentArguments());
        assertTrue(lambda.isIndependentOfArguments());
    }
}
