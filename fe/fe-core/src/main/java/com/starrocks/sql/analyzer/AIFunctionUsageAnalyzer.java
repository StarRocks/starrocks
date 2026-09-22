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

import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;

/** Enforces semantic restrictions on where resolved AI functions may be used. */
final class AIFunctionUsageAnalyzer {
    enum PlacementContext {
        GROUP_BY("GROUP BY clause"),
        SELECT_DISTINCT("SELECT DISTINCT"),
        AGGREGATE_ARGUMENTS("aggregate function arguments"),
        WINDOW_FUNCTION("window function"),
        LAMBDA_FUNCTION_BODY("Lambda function body"),
        CONDITIONAL_EXPRESSION("conditional expression"),
        JOIN_ON_CLAUSE("JOIN ON clause"),
        TABLE_FUNCTION_ARGUMENT("Table Function argument"),
        GENERATED_COLUMN_EXPRESSION("Generated Column expression"),
        SQL_UDF_BODY("SQL UDF body");

        private final String description;

        PlacementContext(String description) {
            this.description = description;
        }
    }

    private enum CorrelationContext {
        SELECT_LIST("SELECT list"),
        WHERE_CLAUSE("WHERE clause"),
        HAVING_CLAUSE("HAVING clause"),
        ORDER_BY_CLAUSE("ORDER BY clause"),
        JOIN_ON_CLAUSE("JOIN ON clause");

        private final String description;

        CorrelationContext(String description) {
            this.description = description;
        }
    }

    private AIFunctionUsageAnalyzer() {
    }

    static void verifyNoAIFunctions(Expr expression, PlacementContext context) {
        FunctionCallExpr function = ResolvedAIFunctionDetector.findFirst(expression).orElse(null);
        if (function != null) {
            throw new SemanticException(context.description + " cannot contain AI function "
                    + function.getFunctionName(), expression.getPos());
        }
    }

    static void verifyNoCorrelatedAIFunctionsInQueryBlock(AnalyzeState analyzeState) {
        if (!analyzeState.hasOuterColumnReference()) {
            return;
        }

        for (Expr expression : analyzeState.getOutputExpressions()) {
            verifyNoCorrelatedAIFunctionsInQueryBlock(expression, CorrelationContext.SELECT_LIST);
        }
        if (analyzeState.getPredicate() != null) {
            verifyNoCorrelatedAIFunctionsInQueryBlock(analyzeState.getPredicate(), CorrelationContext.WHERE_CLAUSE);
        }
        if (analyzeState.getHaving() != null) {
            verifyNoCorrelatedAIFunctionsInQueryBlock(analyzeState.getHaving(), CorrelationContext.HAVING_CLAUSE);
        }
        for (OrderByElement orderByElement : analyzeState.getOrderBy()) {
            verifyNoCorrelatedAIFunctionsInQueryBlock(orderByElement.getExpr(), CorrelationContext.ORDER_BY_CLAUSE);
        }
        for (Expr joinOnPredicate : analyzeState.getJoinOnPredicates()) {
            verifyNoCorrelatedAIFunctionsInQueryBlock(joinOnPredicate, CorrelationContext.JOIN_ON_CLAUSE);
        }
    }

    private static void verifyNoCorrelatedAIFunctionsInQueryBlock(
            Expr expression, CorrelationContext context) {
        FunctionCallExpr function = ResolvedAIFunctionDetector.findFirstInCurrentQueryBlock(expression).orElse(null);
        if (function != null) {
            throw new SemanticException(context.description + " cannot contain correlated AI function "
                    + function.getFunctionName(), function.getPos());
        }
    }
}
