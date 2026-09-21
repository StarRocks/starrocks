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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.tree.exprreuse.ScalarOperatorsReuse;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** Shared, occurrence-aware extraction support for AI scalar calls. */
public final class AiFunctionExtractor {
    private AiFunctionExtractor() {
    }

    public static boolean isAICall(ScalarOperator expression) {
        if (!(expression instanceof CallOperator call)) {
            return false;
        }
        return call.getFunction() != null && call.getFunction().isAi();
    }

    public static boolean containsAI(ScalarOperator expression) {
        if (expression == null) {
            return false;
        }
        if (isAICall(expression)) {
            return true;
        }
        return expression.getChildren().stream().anyMatch(AiFunctionExtractor::containsAI);
    }

    public static Result extract(OptExpression child, List<ScalarOperator> expressions, OptimizerContext context) {
        OptExpression currentChild = child;
        List<ScalarOperator> rewritten = new ArrayList<>(expressions);
        List<ColumnRefOperator> availableColumns = child.getOutputColumns().getStream()
                .map(context.getColumnRefFactory()::getColumnRef).toList();

        while (rewritten.stream().anyMatch(AiFunctionExtractor::containsAI)) {
            LinkedHashMap<CallOperator, ColumnRefOperator> calls = new LinkedHashMap<>();
            rewritten.forEach(expression -> collectInnermostCalls(expression, calls, context));
            if (calls.isEmpty()) {
                throw new IllegalStateException("Unable to extract nested AI function");
            }

            Map<ScalarOperator, ColumnRefOperator> replacements = new LinkedHashMap<>(calls);
            rewritten = rewritten.stream().map(expression -> rewrite(expression, replacements)).toList();

            Map<ColumnRefOperator, ScalarOperator> slotMap = new LinkedHashMap<>();
            availableColumns.forEach(column -> slotMap.put(column, column));
            calls.forEach((call, output) -> slotMap.put(output, call));

            Projection projection = ScalarOperatorsReuse.rewriteProjectionOrLambdaExpr(
                    new Projection(slotMap), context.getColumnRefFactory());
            if (!isSafeCommonMap(projection.getCommonSubOperatorMap())) {
                projection = new Projection(slotMap);
            }
            currentChild = OptExpression.create(new LogicalAIProjectOperator(
                    projection.getColumnRefMap(), projection.getCommonSubOperatorMap()), currentChild);
            currentChild.deriveLogicalPropertyItself();
            availableColumns = new ArrayList<>(projection.getColumnRefMap().keySet());
        }
        return new Result(currentChild, rewritten);
    }

    public static OptExpression applyProjection(OptExpression child, Projection projection) {
        if (projection == null) {
            return child;
        }
        Map<ColumnRefOperator, ScalarOperator> map = new LinkedHashMap<>();
        ReplaceColumnRefRewriter inliner = new ReplaceColumnRefRewriter(
                projection.getCommonSubOperatorMap(), true);
        projection.getColumnRefMap().forEach((column, expression) -> map.put(column, inliner.rewrite(expression)));
        OptExpression result = OptExpression.create(new LogicalProjectOperator(map), child);
        result.deriveLogicalPropertyItself();
        return result;
    }

    public static OptExpression retainOutputs(OptExpression child, ColumnRefSet outputs, OptimizerContext context) {
        Map<ColumnRefOperator, ScalarOperator> map = new LinkedHashMap<>();
        outputs.getStream().map(context.getColumnRefFactory()::getColumnRef).forEach(column -> map.put(column, column));
        OptExpression result = OptExpression.create(new LogicalProjectOperator(map), child);
        result.deriveLogicalPropertyItself();
        return result;
    }

    private static boolean isSafeCommonMap(Map<ColumnRefOperator, ScalarOperator> commonMap) {
        return commonMap.values().stream().noneMatch(AiFunctionExtractor::containsNonReusableExpression);
    }

    static boolean containsNonReusableExpression(ScalarOperator expression) {
        if (expression == null) {
            return false;
        }
        if (isAICall(expression)) {
            return true;
        }
        if (expression instanceof CallOperator call
                && FunctionSet.allNonDeterministicFunctions.contains(
                        call.getFnName().toLowerCase(Locale.ROOT))) {
            return true;
        }
        return expression.getChildren().stream()
                .anyMatch(AiFunctionExtractor::containsNonReusableExpression);
    }

    private static void collectInnermostCalls(ScalarOperator expression,
                                              Map<CallOperator, ColumnRefOperator> calls,
                                              OptimizerContext context) {
        if (expression == null) {
            return;
        }
        if (isAICall(expression)) {
            CallOperator call = expression.cast();
            int semanticArity = Math.min(call.getFunction().getNumArgs(), call.getChildren().size());
            boolean hasNested = false;
            for (int index = 0; index < semanticArity; index++) {
                ScalarOperator argument = call.getChild(index);
                if (containsAI(argument)) {
                    hasNested = true;
                    collectInnermostCalls(argument, calls, context);
                }
            }
            if (!hasNested) {
                calls.computeIfAbsent(call, ignored -> context.getColumnRefFactory()
                        .create("ai_result", call.getType(), call.isNullable()));
            }
            return;
        }
        expression.getChildren().forEach(child -> collectInnermostCalls(child, calls, context));
    }

    private static ScalarOperator rewrite(ScalarOperator expression,
                                          Map<ScalarOperator, ColumnRefOperator> replacements) {
        return expression.accept(new BaseScalarOperatorShuttle() {
            @Override
            public Optional<ScalarOperator> preprocess(ScalarOperator operator) {
                return Optional.ofNullable(replacements.get(operator));
            }
        }, null);
    }

    public record Result(OptExpression root, List<ScalarOperator> expressions) {
    }
}
