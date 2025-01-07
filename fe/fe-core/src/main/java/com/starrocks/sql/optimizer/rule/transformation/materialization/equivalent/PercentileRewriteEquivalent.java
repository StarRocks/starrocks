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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Arrays;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;
import static com.starrocks.catalog.FunctionSet.PERCENTILE_APPROX;
import static com.starrocks.catalog.FunctionSet.PERCENTILE_APPROX_RAW;
import static com.starrocks.catalog.FunctionSet.PERCENTILE_UNION;

public class PercentileRewriteEquivalent extends IAggregateRewriteEquivalent {
    public static IAggregateRewriteEquivalent INSTANCE = new PercentileRewriteEquivalent();

    public PercentileRewriteEquivalent() {
    }

    @Override
    public RewriteEquivalentContext prepare(ScalarOperator op) {
        if (op == null || !(op instanceof CallOperator)) {
            return null;
        }
        CallOperator aggFunc = (CallOperator) op;
        String aggFuncName = aggFunc.getFnName();

        if (aggFuncName.equals(PERCENTILE_UNION)) {
            ScalarOperator arg0 = aggFunc.getChild(0);
            if (arg0 == null) {
                return null;
            }
            if (!arg0.getType().isPercentile()) {
                return null;
            }
            if (arg0 instanceof CallOperator) {
                CallOperator call0 = (CallOperator) arg0;
                if (call0.getFnName().equals(FunctionSet.PERCENTILE_HASH)) {
                    // percentile_union(percentile_hash()) can be used for rewrite
                    if (call0 instanceof CastOperator) {
                        CastOperator castOperator = (CastOperator) call0;
                        if (castOperator.getType().isDouble()) {
                            return new RewriteEquivalentContext(castOperator.getChild(0), op);
                        }
                    } else {
                        return new RewriteEquivalentContext(call0.getChild(0), op);
                    }
                }
            } else {
                return new RewriteEquivalentContext(arg0, op);
            }
        }
        return null;
    }

    @Override
    public boolean isSupportPushDownRewrite(CallOperator aggFunc) {
        if (aggFunc == null) {
            return false;
        }

        String aggFuncName = aggFunc.getFnName();
        if (aggFuncName.equalsIgnoreCase(PERCENTILE_APPROX)) {
            return true;
        }
        return false;
    }

    @Override
    public ScalarOperator rewrite(RewriteEquivalentContext eqContext,
                                  EquivalentShuttleContext shuttleContext,
                                  ColumnRefOperator replace,
                                  ScalarOperator newInput) {
        if (newInput == null || !(newInput instanceof CallOperator)) {
            return null;
        }
        ScalarOperator eqChild = eqContext.getEquivalent();
        CallOperator aggFunc = (CallOperator) newInput;
        String aggFuncName = aggFunc.getFnName();

        if (aggFuncName.equalsIgnoreCase(PERCENTILE_APPROX)) {
            ScalarOperator eqArg = aggFunc.getChild(0);
            if (!eqArg.equals(eqChild)) {
                return null;
            }
            return rewriteImpl(shuttleContext, aggFunc, replace);
        }
        return null;
    }

    private CallOperator makePercentileUnion(ScalarOperator replace) {
        Function unionFn = Expr.getBuiltinFunction(FunctionSet.PERCENTILE_UNION, new Type[] { Type.PERCENTILE },
                IS_IDENTICAL);
        Preconditions.checkState(unionFn != null);
        return new CallOperator(PERCENTILE_UNION, Type.PERCENTILE, Arrays.asList(replace), unionFn);
    }

    private CallOperator makePercentileApproxRaw(ScalarOperator replace, ScalarOperator arg1) {
        Function approxRawFn = Expr.getBuiltinFunction(FunctionSet.PERCENTILE_APPROX_RAW,
                new Type[] { Type.PERCENTILE, Type.DOUBLE }, Function.CompareMode.IS_IDENTICAL);
        Preconditions.checkState(approxRawFn != null);
        // percentile_approx_raw(percentile_union(input), arg1)
        return new CallOperator(PERCENTILE_APPROX_RAW, Type.DOUBLE, Arrays.asList(replace, arg1), approxRawFn);
    }

    private CallOperator makeRollupFunc(ScalarOperator replace, ScalarOperator arg1) {
        Function unionFn = Expr.getBuiltinFunction(FunctionSet.PERCENTILE_UNION, new Type[] { Type.PERCENTILE },
                IS_IDENTICAL);
        Preconditions.checkState(unionFn != null);
        CallOperator rollup = new CallOperator(PERCENTILE_UNION, Type.PERCENTILE, Arrays.asList(replace), unionFn);

        Function approxRawFn = Expr.getBuiltinFunction(FunctionSet.PERCENTILE_APPROX_RAW,
                new Type[] { Type.PERCENTILE, Type.DOUBLE }, Function.CompareMode.IS_IDENTICAL);
        Preconditions.checkState(approxRawFn != null);
        // percentile_approx_raw(percentile_union(input), arg1)
        return new CallOperator(PERCENTILE_APPROX_RAW, Type.DOUBLE, Arrays.asList(rollup, arg1), approxRawFn);
    }

    @Override
    public ScalarOperator rewriteRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                     CallOperator aggFunc,
                                                     ColumnRefOperator replace) {
        ScalarOperator arg1 = aggFunc.getChild(1);
        return makeRollupFunc(replace, arg1);
    }

    @Override
    public ScalarOperator rewriteAggregateFuncWithoutRollup(EquivalentShuttleContext shuttleContext,
                                                            CallOperator aggFunc,
                                                            ColumnRefOperator replace) {
        ScalarOperator arg1 = aggFunc.getChild(1);
        return makePercentileApproxRaw(replace, arg1);
    }

    @Override
    public Pair<CallOperator, CallOperator> rewritePushDownRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                                               CallOperator aggFunc,
                                                                               ColumnRefOperator replace) {
        ScalarOperator arg1 = aggFunc.getChild(1);
        CallOperator finalFn = makeRollupFunc(replace, arg1);
        CallOperator partialFn = makePercentileUnion(replace);
        return Pair.create(partialFn, finalFn);
    }
}