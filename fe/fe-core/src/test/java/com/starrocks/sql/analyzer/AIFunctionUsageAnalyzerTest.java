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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class AIFunctionUsageAnalyzerTest {
    @Test
    public void testPlacementUsesExpressionPositionAndFirstFunction() {
        NodePosition expressionPosition = new NodePosition(1, 1, 1, 30);
        FunctionCallExpr firstAI = function("first_ai", true, new NodePosition(1, 10, 1, 16));
        FunctionCallExpr secondAI = function("second_ai", true, new NodePosition(1, 20, 1, 27));
        FunctionCallExpr expression = function("wrapper", false, expressionPosition, firstAI, secondAI);

        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> AIFunctionUsageAnalyzer.verifyNoAIFunctions(
                        expression, AIFunctionUsageAnalyzer.PlacementContext.GROUP_BY));

        Assertions.assertEquals("GROUP BY clause cannot contain AI function first_ai", exception.getDetailMsg());
        Assertions.assertSame(expressionPosition, exception.pos);
    }

    @Test
    public void testCorrelationUsesClauseOrderAndFunctionPosition() {
        NodePosition outputFunctionPosition = new NodePosition(1, 8, 1, 16);
        FunctionCallExpr outputAI = function("output_ai", true, outputFunctionPosition);
        FunctionCallExpr predicateAI = function("predicate_ai", true, new NodePosition(1, 30, 1, 41));
        AnalyzeState analyzeState = new AnalyzeState();
        analyzeState.mergeOuterColumnReference(true);
        analyzeState.setOutputExpression(List.of(outputAI));
        analyzeState.setPredicate(predicateAI);
        analyzeState.setOrderBy(List.of());

        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> AIFunctionUsageAnalyzer.verifyNoCorrelatedAIFunctionsInQueryBlock(analyzeState));

        Assertions.assertEquals(
                "SELECT list cannot contain correlated AI function output_ai", exception.getDetailMsg());
        Assertions.assertSame(outputFunctionPosition, exception.pos);
    }

    private static FunctionCallExpr function(String name, boolean isAI, NodePosition position,
                                             FunctionCallExpr... children) {
        FunctionCallExpr call = new FunctionCallExpr(name, List.of(children), position);
        Type[] argumentTypes = new Type[children.length];
        Arrays.fill(argumentTypes, InvalidType.INVALID);
        Function function = new Function(new FunctionName(name), argumentTypes, InvalidType.INVALID, false);
        function.setBinaryType(isAI ? TFunctionBinaryType.AI : TFunctionBinaryType.BUILTIN);
        call.setFn(function);
        return call;
    }
}
