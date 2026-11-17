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

import com.google.common.collect.Maps;
import com.starrocks.builtins.VectorizedBuiltinFunctions;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import com.starrocks.type.Type;

final class AIFunctionAnalyzer {
    private AIFunctionAnalyzer() {
    }

    static void analyze(FunctionCallExpr node) {
        if (!VectorizedBuiltinFunctions.AI_FUNCTION_NAMES.contains(node.getFunctionName())) {
            throw new SemanticException("Unsupported AI function '" + node.getFunctionName() + "'", node.getPos());
        }

        int argumentCount = node.getChildren().size();
        validateNamedArguments(node);
        // Function analysis precedes argument coercion, including VARCHAR/JSON to ARRAY casts.
        for (int i = 0; i < argumentCount; i++) {
            if (node.getFn().getArgs()[i].isArrayType()) {
                validateHelperArray(node.getChild(i), node.getFn().getArgs()[i], node);
            }
        }
    }

    // Arguments have been reordered and defaults appended by the named argument framework.
    private static void validateNamedArguments(FunctionCallExpr node) {
        for (int i = 0; i < node.getChildren().size(); i++) {
            String name = node.getFn().getArgNames()[i];
            Expr expression = node.getChild(i);
            if (!name.equals("model") && !name.equals("provider") && !name.equals("options")) {
                continue;
            }
            if (!expression.isConstant()) {
                throw new SemanticException(node.getFunctionName() + " requires a constant " + name,
                        expression.getPos());
            }
            if (name.equals("options")) {
                Expr original = unwrapImplicitCasts(expression);
                if (!original.getType().isJsonType() && !original.getType().isNull()) {
                    throw new SemanticException(node.getFunctionName() + " options must be a constant JSON",
                            expression.getPos());
                }
                // BE checks the JSON object and reserved keys before sending a request.
                // Keep SQL NULL as-is; replacing it here would introduce an unanalyzed default expression.
                continue;
            }
            Expr folded = unwrapCasts(foldAIConstant(expression));
            if (folded instanceof NullLiteral) {
                // Canonicalize optional NULLs after overload resolution so stored SQL also uses the defaults.
                Expr defaultExpr = node.getFn().getDefaultNamedExpr(name).clone();
                node.setChild(i, defaultExpr);
                node.getParams().exprs().set(i, defaultExpr);
                continue;
            }
            if (!(folded instanceof StringLiteral literal)) {
                throw new SemanticException(node.getFunctionName() + " " + name
                        + " must be a constant string", expression.getPos());
            }
            if (!literal.getValue().isEmpty() && literal.getValue().isBlank()) {
                throw new SemanticException(node.getFunctionName() + " " + name
                        + " must not contain only whitespace", expression.getPos());
            }
        }
    }

    private static void validateHelperArray(Expr expression, Type formalType, FunctionCallExpr node) {
        if (!expression.isConstant()) {
            throw new SemanticException(node.getFunctionName() + " requires a constant ARRAY", node.getPos());
        }
        Expr folded = foldAnalyzedAIConstant(foldAIConstantAsType(expression, formalType));
        if (folded instanceof NullLiteral) {
            throw invalidHelperArray(node);
        }
        // The constant folder can retain ARRAY casts. They preserve the cardinality of an empty literal.
        Expr arrayLiteral = folded;
        while (arrayLiteral instanceof CastExpr && arrayLiteral.getType().isArrayType()
                && arrayLiteral.getChild(0).getType().isArrayType()) {
            arrayLiteral = arrayLiteral.getChild(0);
        }
        if (arrayLiteral instanceof ArrayExpr array && array.getChildren().isEmpty()) {
            throw invalidHelperArray(node);
        }
        // Use the same constant folder as other option validation. Unfoldable constants are checked by BE.
        if (!(folded instanceof ArrayExpr array)) {
            return;
        }
        for (Expr child : array.getChildren()) {
            Expr item = foldAIConstant(child);
            if (item instanceof NullLiteral || item instanceof StringLiteral literal && literal.getValue().isBlank()) {
                throw invalidHelperArray(node);
            }
        }
    }

    private static SemanticException invalidHelperArray(FunctionCallExpr node) {
        return new SemanticException(node.getFunctionName()
                + " requires a nonempty ARRAY with non-null, nonblank elements", node.getPos());
    }

    private static Expr unwrapImplicitCasts(Expr expression) {
        Expr current = expression;
        while (current instanceof CastExpr && ((CastExpr) current).isImplicit()) {
            current = current.getChild(0);
        }
        return current;
    }

    private static Expr foldAIConstant(Expr expression) {
        return ExprUtils.analyzeAndCastFold(expression.clone());
    }

    private static Expr foldAIConstantAsType(Expr expression, Type targetType) {
        if (expression.getType().equals(targetType)) {
            if (targetType.isComplexType() || expression instanceof LiteralExpr) {
                return expression;
            }
        }
        Expr typedExpression = expression.getType().equals(targetType)
                ? expression : new CastExpr(targetType, expression);
        return foldAnalyzedAIConstant(typedExpression);
    }

    private static Expr foldAnalyzedAIConstant(Expr expression) {
        try {
            ScalarOperator scalarOperator = rewriteAnalyzedAIConstant(expression);
            return ScalarOperatorToExpr.buildExprIgnoreSlot(scalarOperator,
                    new ScalarOperatorToExpr.FormatterContext(Maps.newHashMap()));
        } catch (UnsupportedException e) {
            return expression;
        }
    }

    private static ScalarOperator rewriteAnalyzedAIConstant(Expr expression) throws UnsupportedException {
        ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(expression.clone());
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        return rewriter.rewrite(scalarOperator, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
    }

    private static Expr unwrapCasts(Expr expression) {
        Expr current = expression;
        while (current instanceof CastExpr) {
            current = current.getChild(0);
        }
        return current;
    }

}
