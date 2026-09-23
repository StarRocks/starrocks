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

package com.starrocks.sql.optimizer.rule.tree.exprreuse;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Combine paths over the same input before common-expression extraction assigns shared slots. */
final class JsonExtractFusionReuse {
    private JsonExtractFusionReuse() {
    }

    static Projection rewrite(Projection projection) {
        ConnectContext context = ConnectContext.get();
        if (context == null || !context.getSessionVariable().isEnableJsonExtractFusion()
                || SqlModeHelper.check(context.getSessionVariable().getSqlMode(), SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION)) {
            return projection;
        }
        Map<ScalarOperator, Map<ScalarOperator, Integer>> groups = new HashMap<>();
        for (ScalarOperator expression : projection.getColumnRefMap().values()) {
            if (expression.getDepth() <= Config.max_scalar_operator_optimize_depth) {
                collect(expression, groups);
            }
        }
        groups.values().removeIf(paths -> paths.size() < 2);
        if (groups.isEmpty()) {
            return projection;
        }
        BaseScalarOperatorShuttle rewriter = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitLambdaFunctionOperator(LambdaFunctionOperator operator, Void unused) {
                // Lambda-dependent expressions are processed in their own reuse scope.
                return operator;
            }

            @Override
            public ScalarOperator visitDictMappingOperator(DictMappingOperator operator, Void unused) {
                return operator;
            }

            @Override
            public ScalarOperator visitCall(CallOperator call, Void unused) {
                Map<ScalarOperator, Integer> paths = candidate(call) ? groups.get(call.getChild(0)) : null;
                if (paths == null) {
                    return super.visitCall(call, unused);
                }
                List<ScalarOperator> args = new ArrayList<>();
                args.add(call.getChild(0).accept(this, unused));
                args.addAll(paths.keySet());
                Function many = ExprUtils.getBuiltinFunction(FunctionSet.JSON_QUERY_MANY_FROM_STRING,
                        args.stream().map(ScalarOperator::getType).toArray(Type[]::new),
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                ScalarOperator outputPath = ConstantOperator.createVarchar("$." + paths.get(call.getChild(1)));
                Function query = ExprUtils.getBuiltinFunction(FunctionSet.JSON_QUERY,
                        new Type[] {JsonType.JSON, outputPath.getType()}, Function.CompareMode.IS_IDENTICAL);
                if (many == null || query == null) {
                    return super.visitCall(call, unused);
                }
                CallOperator shared = new CallOperator(many.functionName(), JsonType.JSON, args, many);
                return new CallOperator(query.functionName(), JsonType.JSON, List.of(shared, outputPath), query);
            }
        };
        Projection result = projection.deepClone();
        result.getColumnRefMap().replaceAll((column, expression) -> expression.accept(rewriter, null));
        return result;
    }

    private static boolean candidate(ScalarOperator expression) {
        return expression instanceof CallOperator
                && FunctionSet.JSON_QUERY_FROM_STRING.equalsIgnoreCase(((CallOperator) expression).getFnName())
                && expression.getChildren().size() == 2
                && expression.getChild(1) instanceof ConstantOperator
                && !((ConstantOperator) expression.getChild(1)).isNull()
                && !Utils.hasNonDeterministicFunc(expression.getChild(0));
    }

    private static void collect(ScalarOperator expression, Map<ScalarOperator, Map<ScalarOperator, Integer>> groups) {
        if (expression instanceof LambdaFunctionOperator || expression instanceof DictMappingOperator) {
            return;
        }
        if (candidate(expression)) {
            Map<ScalarOperator, Integer> paths = groups.computeIfAbsent(expression.getChild(0), k -> new LinkedHashMap<>());
            paths.computeIfAbsent(expression.getChild(1), k -> paths.size());
        }
        for (ScalarOperator child : expression.getChildren()) {
            collect(child, groups);
        }
    }
}
