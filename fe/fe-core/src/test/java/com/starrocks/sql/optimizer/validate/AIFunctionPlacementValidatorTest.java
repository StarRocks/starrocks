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

import com.starrocks.catalog.ScalarFunction;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AIFunctionPlacementValidatorTest {
    @Test
    public void testOnlyDirectAIProjectOutputsMayContainAIFunctions() {
        ColumnRefOperator input = new ColumnRefOperator(1, VarcharType.VARCHAR, "input", true);
        ColumnRefOperator output = new ColumnRefOperator(2, VarcharType.VARCHAR, "output", true);
        ColumnRefOperator common = new ColumnRefOperator(3, VarcharType.VARCHAR, "common", true);
        CallOperator aiCall = aiCall(input);
        OptExpression leaf = values(input);

        Assertions.assertDoesNotThrow(() -> AIFunctionPlacementValidator.validate(
                OptExpression.create(new LogicalAIProjectOperator(Map.of(output, aiCall)), leaf)));
        Assertions.assertDoesNotThrow(() -> AIFunctionPlacementValidator.validate(
                OptExpression.create(new PhysicalAIProjectOperator(Map.of(output, aiCall), Map.of()),
                        physicalValues(input))));

        Assertions.assertAll(
                () -> assertInvalid(OptExpression.create(
                        new LogicalProjectOperator(Map.of(output, aiCall)), leaf)),
                () -> assertInvalid(OptExpression.create(
                        new LogicalAIProjectOperator(
                                Map.of(output, new CallOperator("concat", VarcharType.VARCHAR,
                                        List.of(aiCall, ConstantOperator.createVarchar("!")))),
                                Map.of()),
                        leaf)),
                () -> assertInvalid(OptExpression.create(
                        new LogicalAIProjectOperator(Map.of(output, aiCall(aiCall))), leaf)),
                () -> assertInvalid(OptExpression.create(
                        new LogicalAIProjectOperator(Map.of(output, input), Map.of(common, aiCall)),
                        leaf)),
                () -> assertInvalid(OptExpression.create(
                        new PhysicalProjectOperator(Map.of(output, aiCall), Map.of()),
                        physicalValues(input))),
                () -> assertInvalid(OptExpression.create(
                        new PhysicalProjectOperator(Map.of(output, input), Map.of(common, aiCall)),
                        physicalValues(input))),
                () -> assertInvalid(OptExpression.create(
                        new PhysicalAIProjectOperator(
                                Map.of(output, new CallOperator("concat", VarcharType.VARCHAR,
                                        List.of(aiCall, ConstantOperator.createVarchar("!")))),
                                Map.of()),
                        physicalValues(input))),
                () -> assertInvalid(OptExpression.create(
                        new PhysicalAIProjectOperator(Map.of(output, input), Map.of(common, aiCall)),
                        physicalValues(input))));
    }

    private static void assertInvalid(OptExpression expression) {
        StarRocksPlannerException exception = Assertions.assertThrows(
                StarRocksPlannerException.class, () -> AIFunctionPlacementValidator.validate(expression));
        Assertions.assertTrue(exception.getMessage().contains(
                "AI calls must be direct outputs of an AI project operator"));
    }

    private static CallOperator aiCall(ScalarOperator argument) {
        ScalarFunction function = ScalarFunction.createBuiltin(
                "ai_complete", "", new ArrayList<>(List.of(VarcharType.VARCHAR)),
                false, VarcharType.VARCHAR, true);
        function.setBinaryType(TFunctionBinaryType.AI);
        return new CallOperator("ai_complete", VarcharType.VARCHAR, List.of(argument), function);
    }

    private static OptExpression values(ColumnRefOperator input) {
        return OptExpression.create(new LogicalValuesOperator(
                List.of(input), List.of(List.of(ConstantOperator.createVarchar("prompt")))));
    }

    private static OptExpression physicalValues(ColumnRefOperator input) {
        return OptExpression.create(new PhysicalValuesOperator(
                List.of(input), List.of(List.of(ConstantOperator.createVarchar("prompt"))),
                Operator.DEFAULT_LIMIT, null, null));
    }
}
