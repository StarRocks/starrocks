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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

import static com.starrocks.catalog.FunctionSet.ARRAY_AGG;
import static com.starrocks.catalog.FunctionSet.COUNT;
import static com.starrocks.catalog.FunctionSet.MULTI_DISTINCT_COUNT;
import static com.starrocks.catalog.FunctionSet.MULTI_DISTINCT_SUM;
import static com.starrocks.catalog.FunctionSet.SUM;

/**
 * Rewrite array_agg function
 * Examples:
 * 1. count(distinct x) => array_length(array_agg_distinct(x))
 * 2. sum(distinct x) => array_sum(array_agg_distinct(x))
 */
public class ArrayRewriteEquivalent extends IAggregateRewriteEquivalent {

    public static IAggregateRewriteEquivalent INSTANCE = new ArrayRewriteEquivalent();

    private static final Map<String, String> MAPPING = ImmutableMap.of(
            COUNT, FunctionSet.ARRAY_LENGTH,
            FunctionSet.MULTI_DISTINCT_COUNT, FunctionSet.ARRAY_LENGTH,
            FunctionSet.MULTI_DISTINCT_SUM, FunctionSet.ARRAY_SUM,
            SUM, FunctionSet.ARRAY_SUM);

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

        return new RewriteEquivalentContext(call.getChild(0), input);
    }

    private static boolean isArrayAggDistinct(CallOperator call) {
        String fn = call.getFnName();
        return fn.equalsIgnoreCase(FunctionSet.ARRAY_AGG_DISTINCT) ||
                (call.isDistinct() && fn.equalsIgnoreCase(ARRAY_AGG));
    }

    private static final ImmutableSet<String> SUPPORTED_PUSHDOWN_AGG_FUNCTIONS = ImmutableSet.of(
            MULTI_DISTINCT_COUNT, MULTI_DISTINCT_SUM
    );
    private static final ImmutableSet<String> SUPPORTED_PUSHDOWN_AGG_DISTINCT_FUNCTIONS = ImmutableSet.of(
            COUNT, SUM, ARRAY_AGG
    );

    @Override
    public boolean isSupportPushDownRewrite(CallOperator call) {
        String fn = call.getFnName();
        if (!call.isDistinct() && SUPPORTED_PUSHDOWN_AGG_FUNCTIONS.contains(fn)) {
            return true;
        }
        if (call.isDistinct() && SUPPORTED_PUSHDOWN_AGG_DISTINCT_FUNCTIONS.contains(fn)) {
            return true;
        }
        return false;
    }

    @Override
    public ScalarOperator rewrite(RewriteEquivalentContext eqContext,
                                  EquivalentShuttleContext shuttleContext,
                                  ColumnRefOperator replace,
                                  ScalarOperator newInput) {
        ScalarOperator eq = eqContext.getEquivalent();
        CallOperator call = (CallOperator) newInput;
        String fn = call.getFnName();
        if ((call.isDistinct() && (fn.equalsIgnoreCase(COUNT) || fn.equalsIgnoreCase(SUM)))
                || fn.equalsIgnoreCase(MULTI_DISTINCT_COUNT) || fn.equalsIgnoreCase(MULTI_DISTINCT_SUM)) {
            String mapped = MAPPING.get(fn);
            Preconditions.checkState(mapped != null);
            ScalarOperator arg0 = call.getChild(0);
            if (!arg0.equals(eq)) {
                return null;
            }
            return rewriteImpl(shuttleContext, call, replace);
        } else if (fn.equalsIgnoreCase(ARRAY_AGG)) {
            if (!call.isDistinct()) {
                return null;
            }
            ScalarOperator arg0 = call.getChild(0);
            if (!arg0.equals(eq)) {
                return null;
            }
            return rewriteImpl(shuttleContext, call, replace);
        }
        return null;
    }

    CallOperator makeArrayUniqAggFunc(ScalarOperator arg0, CallOperator aggFunc, Function.CompareMode compareMode) {
        Type newInputArgType = aggFunc.getChild(0).getType();
        Type[] argTypes = new Type[] {new ArrayType(newInputArgType)};
        Function rollup = Expr.getBuiltinFunction(FunctionSet.ARRAY_UNIQUE_AGG, argTypes, compareMode);
        if (rollup == null) {
            return null;
        }
        return new CallOperator(FunctionSet.ARRAY_UNIQUE_AGG, new ArrayType(newInputArgType), Lists.newArrayList(arg0), rollup);
    }

    CallOperator makeRollupAggFunc(ScalarOperator replace, CallOperator call,
                                   Function.CompareMode compareMode, String mapped) {
        Type newInputArgType = call.getChild(0).getType();
        Type[] argTypes = new Type[] {new ArrayType(newInputArgType)};
        Function replaced = Expr.getBuiltinFunction(mapped, argTypes, compareMode);
        if (replaced == null) {
            return null;
        }
        Function rollup = Expr.getBuiltinFunction(FunctionSet.ARRAY_UNIQUE_AGG, argTypes, compareMode);
        CallOperator res = new CallOperator(
                FunctionSet.ARRAY_UNIQUE_AGG,
                new ArrayType(newInputArgType),
                Lists.newArrayList(replace),
                rollup);
        return new CallOperator(mapped, call.getType(), Lists.newArrayList(res), replaced);
    }

    CallOperator makeNoRollupAggFunc(ScalarOperator replace, CallOperator call,
                                   Function.CompareMode compareMode, String mapped) {
        Type newInputArgType = call.getChild(0).getType();
        Type[] argTypes = new Type[] {new ArrayType(newInputArgType)};
        Function replaced = Expr.getBuiltinFunction(mapped, argTypes, compareMode);
        if (replaced == null) {
            return null;
        }
        return new CallOperator(mapped, call.getType(), Lists.newArrayList(replace), replaced);
    }

    @Override
    public ScalarOperator rewriteRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                     CallOperator aggFunc,
                                                     ColumnRefOperator replace) {
        String fn = aggFunc.getFnName();
        if (fn.equals(ARRAY_AGG)) {
            return makeArrayUniqAggFunc(replace, aggFunc, Function.CompareMode.IS_IDENTICAL);
        } else {
            String mapped = MAPPING.get(fn);
            Preconditions.checkState(mapped != null);
            return makeRollupAggFunc(replace, aggFunc, Function.CompareMode.IS_IDENTICAL, mapped);
        }
    }

    @Override
    public ScalarOperator rewriteAggregateFuncWithoutRollup(EquivalentShuttleContext shuttleContext,
                                                            CallOperator aggFunc,
                                                            ColumnRefOperator replace) {
        String fn = aggFunc.getFnName();
        if (fn.equals(ARRAY_AGG)) {
            return makeArrayUniqAggFunc(replace, aggFunc, Function.CompareMode.IS_IDENTICAL);
        } else {
            String mapped = MAPPING.get(fn);
            Preconditions.checkState(mapped != null);
            return makeNoRollupAggFunc(replace, aggFunc, Function.CompareMode.IS_IDENTICAL, mapped);
        }
    }

    @Override
    public Pair<CallOperator, CallOperator> rewritePushDownRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                                               CallOperator aggFunc,
                                                                               ColumnRefOperator replace) {
        String fn = aggFunc.getFnName();
        if (fn.equals(ARRAY_AGG)) {
            CallOperator partialFn = makeArrayUniqAggFunc(replace, aggFunc, Function.CompareMode.IS_IDENTICAL);
            CallOperator finalFn = makeArrayUniqAggFunc(replace, aggFunc, Function.CompareMode.IS_IDENTICAL);
            return Pair.create(partialFn, finalFn);
        } else {
            String mapped = MAPPING.get(fn);
            Preconditions.checkState(mapped != null);
            CallOperator partialFn = makeArrayUniqAggFunc(replace, aggFunc, Function.CompareMode.IS_IDENTICAL);
            CallOperator finalFn = makeRollupAggFunc(replace, aggFunc, Function.CompareMode.IS_IDENTICAL, mapped);
            return Pair.create(partialFn, finalFn);
        }
    }
}
