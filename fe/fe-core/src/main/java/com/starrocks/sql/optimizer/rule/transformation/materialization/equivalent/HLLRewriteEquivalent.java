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
import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Arrays;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;
import static com.starrocks.catalog.FunctionSet.APPROX_COUNT_DISTINCT;
import static com.starrocks.catalog.FunctionSet.HLL_CARDINALITY;
import static com.starrocks.catalog.FunctionSet.HLL_HASH;
import static com.starrocks.catalog.FunctionSet.HLL_UNION;
import static com.starrocks.catalog.FunctionSet.HLL_UNION_AGG;
import static com.starrocks.catalog.FunctionSet.MULTI_DISTINCT_COUNT;
import static com.starrocks.catalog.FunctionSet.NDV;

public class HLLRewriteEquivalent extends IAggregateRewriteEquivalent {
    public static IAggregateRewriteEquivalent INSTANCE = new HLLRewriteEquivalent();

    public HLLRewriteEquivalent() {}

    @Override
    public RewriteEquivalentContext prepare(ScalarOperator op) {
        if (op == null || !(op instanceof CallOperator)) {
            return null;
        }
        CallOperator aggFunc = (CallOperator) op;
        String aggFuncName = aggFunc.getFnName();

        if (aggFuncName.equals(HLL_UNION)) {
            ScalarOperator arg0 = aggFunc.getChild(0);
            if (arg0 == null) {
                return null;
            }
            if (!arg0.getType().isHllType()) {
                return null;
            }
            if (arg0 instanceof CallOperator) {
                CallOperator call0 = (CallOperator) arg0;
                if (call0.getFnName().equals(FunctionSet.HLL_HASH)) {
                    // hll_union(hll_hash(x)) can be used for rewrite
                    ScalarOperator child0 = call0.getChild(0);
                    if (child0 instanceof CastOperator) {
                        CastOperator castOperator = (CastOperator) child0;
                        // hll_union(hll_hash(cast(x as char))) can be used for rewrite
                        if (castOperator.getType().isStringType()) {
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

    public static ImmutableSet<String> SUPPORT_AGG_FUNC = ImmutableSet.of(
            APPROX_COUNT_DISTINCT,
            NDV,
            HLL_UNION_AGG,
            MULTI_DISTINCT_COUNT
    );

    @Override
    public boolean isSupportPushDownRewrite(CallOperator aggFunc) {
        if (aggFunc == null) {
            return false;
        }

        String aggFuncName = aggFunc.getFnName();
        if (SUPPORT_AGG_FUNC.contains(aggFuncName)) {
            return true;
        }
        // count distinct
        if (aggFuncName.equals(FunctionSet.COUNT) && aggFunc.isDistinct()) {
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
        boolean isRollup = shuttleContext.isRollup();

        if (aggFuncName.equals(APPROX_COUNT_DISTINCT) || aggFuncName.equals(NDV)) {
            ScalarOperator arg0 = aggFunc.getChild(0);
            if (!arg0.equals(eqChild)) {
                return null;
            }
            return rewriteImpl(shuttleContext, aggFunc, replace);
        } else if (aggFuncName.equals(HLL_UNION_AGG)) {
            ScalarOperator eqArg = aggFunc.getChild(0);
            if (eqArg instanceof CallOperator) {
                CallOperator arg = (CallOperator) eqArg;
                if (!arg.getFnName().equals(HLL_HASH)) {
                    return null;
                }
                ScalarOperator arg0 = arg.getChild(0);
                if (arg0 instanceof CastOperator) {
                    CastOperator castOperator = (CastOperator) arg0;
                    // hll_union(hll_hash(cast(x as char))) can be used for rewrite
                    if (!castOperator.getType().isStringType()) {
                        return null;
                    }
                    eqArg = arg0.getChild(0);
                } else {
                    eqArg = arg0;
                }
            }
            if (!eqArg.equals(eqChild)) {
                return null;
            }
            return rewriteImpl(shuttleContext, aggFunc, replace);
        } else if ((aggFuncName.equalsIgnoreCase(FunctionSet.COUNT) && aggFunc.isDistinct()
                && aggFunc.getChildren().size() == 1) || aggFuncName.equalsIgnoreCase(MULTI_DISTINCT_COUNT)) {
            SessionVariable sessionVariable = shuttleContext.getRewriteContext().getOptimizerContext().getSessionVariable();
            if (!sessionVariable.isEnableCountDistinctRewriteByHllBitmap()) {
                return null;
            }
            ScalarOperator eqArg = aggFunc.getChild(0);
            if (!eqArg.equals(eqChild)) {
                return null;
            }
            return rewriteImpl(shuttleContext, aggFunc, replace);
        }
        return null;
    }

    private CallOperator makeHllUnionAggFunc(ScalarOperator arg0) {
        Function fn = Expr.getBuiltinFunction(FunctionSet.HLL_UNION_AGG, new Type[] {Type.HLL}, IS_IDENTICAL);
        Preconditions.checkState(fn != null);
        return new CallOperator(HLL_UNION_AGG, Type.BIGINT, Arrays.asList(arg0), fn);
    }

    private CallOperator makeHllCardinalityFunc(ScalarOperator arg0) {
        Function fn = Expr.getBuiltinFunction(FunctionSet.HLL_CARDINALITY, new Type[] {Type.HLL}, IS_IDENTICAL);
        Preconditions.checkState(fn != null);
        return new CallOperator(HLL_CARDINALITY, Type.BIGINT, Arrays.asList(arg0), fn);
    }

    private CallOperator makeHllUnion(ScalarOperator arg0) {
        Function fn = Expr.getBuiltinFunction(HLL_UNION, new Type[] {arg0.getType()}, IS_IDENTICAL);
        Preconditions.checkState(fn != null);
        return new CallOperator(HLL_UNION, Type.HLL, Arrays.asList(arg0), fn);
    }

    @Override
    public ScalarOperator rewriteRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                     CallOperator aggFunc,
                                                     ColumnRefOperator replace) {
        return makeHllUnionAggFunc(replace);
    }

    @Override
    public ScalarOperator rewriteAggregateFuncWithoutRollup(EquivalentShuttleContext shuttleContext,
                                                            CallOperator aggFunc,
                                                            ColumnRefOperator replace) {
        return makeHllCardinalityFunc(replace);
    }

    @Override
    public Pair<CallOperator, CallOperator> rewritePushDownRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                                               CallOperator aggFunc,
                                                                               ColumnRefOperator replace) {
        CallOperator partialFn = makeHllUnion(replace);
        CallOperator finalFn = makeHllUnionAggFunc(replace);
        return Pair.create(partialFn, finalFn);
    }
}