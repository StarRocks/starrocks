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
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.PivotAggregation;
import com.starrocks.sql.ast.PivotRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ResolvedAIFunctionDetectorTest {
    @Test
    public void testResolvedMetadataControlsDetection() {
        FunctionCallExpr unresolvedAIName = new FunctionCallExpr("ai_complete", List.of());
        FunctionCallExpr resolvedRegularFunction = resolvedCall("regular", TFunctionBinaryType.BUILTIN);
        FunctionCallExpr resolvedAI = resolvedAI("resolved");
        FunctionCallExpr nested = new FunctionCallExpr(
                "wrapper", List.of(unresolvedAIName, resolvedRegularFunction, resolvedAI));

        Assertions.assertSame(resolvedAI, ResolvedAIFunctionDetector.findFirst(nested).orElseThrow());
        Assertions.assertFalse(ResolvedAIFunctionDetector.contains(unresolvedAIName));
        Assertions.assertFalse(ResolvedAIFunctionDetector.contains(resolvedRegularFunction));
    }

    @Test
    public void testCurrentQueryBlockStopsAtSubqueryExpression() {
        FunctionCallExpr nestedAI = resolvedAI("nested");
        ValuesRelation nestedValues = values(nestedAI);
        Subquery subquery = new Subquery(new QueryStatement(nestedValues));

        Assertions.assertSame(nestedAI, ResolvedAIFunctionDetector.findFirst(subquery).orElseThrow());
        Assertions.assertTrue(ResolvedAIFunctionDetector.findFirstInCurrentQueryBlock(subquery).isEmpty());

        FunctionCallExpr localAI = resolvedAI("local");
        FunctionCallExpr expression = new FunctionCallExpr("wrapper", List.of(subquery, localAI));
        Assertions.assertSame(localAI,
                ResolvedAIFunctionDetector.findFirstInCurrentQueryBlock(expression).orElseThrow());
    }

    @Test
    public void testSetOperationOrderByIsTraversed() {
        UnionRelation union = new UnionRelation(List.of(values(new StringLiteral("left")),
                values(new StringLiteral("right"))), SetQualifier.ALL);
        FunctionCallExpr orderByAI = resolvedAI("set-order");
        union.setOrderBy(List.of(new OrderByElement(orderByAI, true, null)));

        Assertions.assertSame(orderByAI, ResolvedAIFunctionDetector.findFirst(union).orElseThrow());
    }

    @Test
    public void testValuesRowsAndOrderByAreTraversedInAstOrder() {
        FunctionCallExpr rowAI = resolvedAI("values-row");
        ValuesRelation values = values(rowAI);
        FunctionCallExpr orderByAI = resolvedAI("values-order");
        values.setOrderBy(List.of(new OrderByElement(orderByAI, true, null)));

        Assertions.assertSame(orderByAI, ResolvedAIFunctionDetector.findFirst(values).orElseThrow());
    }

    @Test
    public void testTableFunctionFormsAreTraversed() {
        FunctionCallExpr directAI = resolvedAI("direct-table-function");
        TableFunctionRelation direct = tableFunction(directAI);
        Assertions.assertSame(directAI, ResolvedAIFunctionDetector.findFirst(direct).orElseThrow());

        FunctionCallExpr normalizedAI = resolvedAI("normalized-table-function");
        NormalizedTableFunctionRelation normalized = new NormalizedTableFunctionRelation(tableFunction(normalizedAI));
        Assertions.assertSame(normalizedAI, ResolvedAIFunctionDetector.findFirst(normalized).orElseThrow());
    }

    @Test
    public void testPivotQueryAndFunctionsAreTraversedInAstOrder() {
        FunctionCallExpr queryAI = resolvedAI("pivot-query");
        FunctionCallExpr aggregateAI = resolvedAI("pivot-aggregate");
        FunctionCallExpr rewrittenAI = resolvedAI("pivot-rewritten");
        PivotRelation pivot = new PivotRelation(values(queryAI),
                List.of(new PivotAggregation(aggregateAI, "answer")), List.of(), List.of());
        pivot.addRewrittenAggFunction(rewrittenAI);

        Assertions.assertSame(queryAI, ResolvedAIFunctionDetector.findFirst(pivot).orElseThrow());

        PivotRelation functionsOnly = new PivotRelation(values(new StringLiteral("value")),
                List.of(new PivotAggregation(aggregateAI, "answer")), List.of(), List.of());
        functionsOnly.addRewrittenAggFunction(rewrittenAI);
        Assertions.assertSame(aggregateAI, ResolvedAIFunctionDetector.findFirst(functionsOnly).orElseThrow());

        PivotRelation rewrittenOnly = new PivotRelation(values(new StringLiteral("value")),
                List.of(), List.of(), List.of());
        rewrittenOnly.addRewrittenAggFunction(rewrittenAI);
        Assertions.assertSame(rewrittenAI, ResolvedAIFunctionDetector.findFirst(rewrittenOnly).orElseThrow());
    }

    private static FunctionCallExpr resolvedAI(String value) {
        return resolvedCall(value, TFunctionBinaryType.AI);
    }

    private static FunctionCallExpr resolvedCall(String value, TFunctionBinaryType binaryType) {
        FunctionCallExpr call = new FunctionCallExpr("test_function", List.of(new StringLiteral(value)));
        Function function = new Function(new FunctionName("test_function"),
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR, false);
        function.setBinaryType(binaryType);
        call.setFn(function);
        return call;
    }

    private static ValuesRelation values(Expr expression) {
        return new ValuesRelation(List.of(List.of(expression)), List.of("value"));
    }

    private static TableFunctionRelation tableFunction(Expr expression) {
        TableFunctionRelation relation = new TableFunctionRelation(
                "test_table_function", new FunctionParams(false, List.of(expression)), expression.getPos());
        relation.setChildExpressions(List.of(expression));
        return relation;
    }
}
