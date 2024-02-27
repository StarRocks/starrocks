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

package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

import static com.starrocks.catalog.FunctionSet.MULTI_DISTINCT_COUNT;

/**
 * Rewrite array_agg function
 * Examples:
 * 1. count(distinct x) => array_length(array_agg_distinct(x))
 * 2. sum(distinct x) => array_sum(array_agg_distinct(x))
 */
public class ArrayRewriteEquivalent extends IAggregateRewriteEquivalent {

    public static IRewriteEquivalent INSTANCE = new ArrayRewriteEquivalent();

    private static final Map<String, String> MAPPING = ImmutableMap.of(
                    FunctionSet.COUNT, FunctionSet.ARRAY_LENGTH,
                    FunctionSet.SUM, FunctionSet.ARRAY_SUM);

    @Override
    public RewriteEquivalentContext prepare(ScalarOperator input) {
        // TODO: use pattern-match instead of manual check
        if (input.getOpType() != OperatorType.CALL) {
            return null;
        }
        CallOperator call = (CallOperator) input;
        if (!isArrayAggDistinct(call)) {
            return null;
        }

        return new RewriteEquivalentContext(call, input);
    }

    private static boolean isArrayAggDistinct(CallOperator call) {
        return call.getFnName().equalsIgnoreCase(FunctionSet.ARRAY_AGG_DISTINCT) ||
                (call.getFnName().equalsIgnoreCase(FunctionSet.ARRAY_AGG) && call.isDistinct());
    }

    @Override
    public ScalarOperator rewrite(RewriteEquivalentContext eqContext, EquivalentShuttleContext shuttleContext,
                                  ColumnRefOperator replace, ScalarOperator newInput) {
        ScalarOperator eq = eqContext.getEquivalent();
        CallOperator call = (CallOperator) newInput;
        String fn = call.getFnName();

        String mapped = MAPPING.get(fn);
        if (mapped != null && (call.isDistinct() || fn.equalsIgnoreCase(MULTI_DISTINCT_COUNT))) {
            Type newInputArgType = call.getChild(0).getType();
            Type[] argTypes = new Type[] {new ArrayType(newInputArgType)};
            Function.CompareMode compareMode = Function.CompareMode.IS_IDENTICAL;
            Function replaced = Expr.getBuiltinFunction(mapped, argTypes, compareMode);
            if (replaced == null) {
                return null;
            }
            CallOperator res = null;
            if (shuttleContext.isRollup()) {
                // rollup into fn(array_unique_agg(col))
                Function rollup = Expr.getBuiltinFunction(FunctionSet.ARRAY_UNIQUE_AGG, argTypes, compareMode);
                res = new CallOperator(
                        FunctionSet.ARRAY_UNIQUE_AGG,
                        new ArrayType(newInputArgType),
                        Lists.newArrayList(replace),
                        rollup);
                res = new CallOperator(mapped, call.getType(), Lists.newArrayList(res), replaced);
            } else {
                // rewrite into fn(col)
                res = new CallOperator(mapped, call.getType(), Lists.newArrayList(replace), replaced);
            }
            return res;
        }
        return null;
    }
}
