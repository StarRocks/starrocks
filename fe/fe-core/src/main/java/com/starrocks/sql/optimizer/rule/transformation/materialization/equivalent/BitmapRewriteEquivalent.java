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

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Arrays;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;
import static com.starrocks.catalog.FunctionSet.BITMAP_AGG;
import static com.starrocks.catalog.FunctionSet.BITMAP_HASH;
import static com.starrocks.catalog.FunctionSet.BITMAP_UNION;
import static com.starrocks.catalog.FunctionSet.BITMAP_UNION_COUNT;
import static com.starrocks.catalog.FunctionSet.MULTI_DISTINCT_COUNT;
import static com.starrocks.catalog.FunctionSet.TO_BITMAP;

public class BitmapRewriteEquivalent extends IAggregateRewriteEquivalent {
    public static IAggregateRewriteEquivalent INSTANCE = new BitmapRewriteEquivalent();

    public BitmapRewriteEquivalent() {}

    @Override
    public RewriteEquivalentContext prepare(ScalarOperator op) {
        if (op == null || !(op instanceof CallOperator)) {
            return null;
        }
        CallOperator aggFunc = (CallOperator) op;
        String aggFuncName = aggFunc.getFnName();

        if (aggFuncName.equals(BITMAP_UNION)) {
            ScalarOperator arg0 = aggFunc.getChild(0);
            if (arg0 == null) {
                return null;
            }
            if (!arg0.getType().isBitmapType()) {
                return null;
            }
            if (arg0 instanceof CallOperator) {
                CallOperator call0 = (CallOperator) arg0;
                if (call0.getFnName().equals(FunctionSet.TO_BITMAP)) {
                    // bitmap_union(to_bitmap()) can be used for rewrite
                    return new RewriteEquivalentContext(call0.getChild(0), op);
                } else if (call0.getFnName().equals(FunctionSet.BITMAP_HASH)) {
                    // bitmap_union(bitmap_hash()) can be used for rewrite
                    return new RewriteEquivalentContext(call0.getChild(0), op);
                }
            } else {
                return new RewriteEquivalentContext(arg0, op);
            }
        } else if (aggFuncName.equals(BITMAP_AGG)) {
            ScalarOperator arg0 = aggFunc.getChild(0);
            if (arg0 == null) {
                return null;
            }
            return new RewriteEquivalentContext(arg0, op);
        }
        return null;
    }

    private CallOperator makeBitmapUnionCountFunc(ScalarOperator arg0) {
        return new CallOperator(BITMAP_UNION_COUNT, Type.BIGINT,
                Arrays.asList(arg0), Expr.getBuiltinFunction(BITMAP_UNION_COUNT, new Type[] {Type.BITMAP},
                        IS_IDENTICAL));
    }

    private CallOperator makeBitmapUnionFunc(ScalarOperator arg0) {
        return new CallOperator(BITMAP_UNION, Type.BITMAP,
                Arrays.asList(arg0), Expr.getBuiltinFunction(BITMAP_UNION, new Type[] {Type.BITMAP},
                        IS_IDENTICAL));
    }

    private CallOperator makeBitmapCountFunc(ScalarOperator arg0) {
        return new CallOperator(FunctionSet.BITMAP_COUNT, Type.BIGINT,
                Arrays.asList(arg0), Expr.getBuiltinFunction(FunctionSet.BITMAP_COUNT, new Type[] {Type.BITMAP},
                        IS_IDENTICAL));
    }

    public static final ImmutableSet<String> SUPPORT_AGG_FUNC = ImmutableSet.of(
            MULTI_DISTINCT_COUNT,
            BITMAP_UNION_COUNT,
            BITMAP_AGG
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
        if ((aggFuncName.equals(FunctionSet.COUNT) && aggFunc.isDistinct() && aggFunc.getChildren().size() == 1) ||
                aggFuncName.equals(MULTI_DISTINCT_COUNT)) {
            SessionVariable sessionVariable = shuttleContext.getRewriteContext().getOptimizerContext().getSessionVariable();
            if (!sessionVariable.isEnableCountDistinctRewriteByHllBitmap()) {
                return null;
            }
            ScalarOperator arg0 = aggFunc.getChild(0);
            if (!arg0.equals(eqChild)) {
                return null;
            }
            return rewriteImpl(shuttleContext, aggFunc, replace);
        } else if (aggFuncName.equals(BITMAP_UNION_COUNT)) {
            ScalarOperator eqArg = aggFunc.getChild(0);
            if (eqArg == null) {
                return null;
            }
            if (eqArg instanceof CallOperator) {
                CallOperator arg00 = (CallOperator) eqArg;
                if (!arg00.getFnName().equals(TO_BITMAP) && !arg00.getFnName().equals(BITMAP_HASH)) {
                    return null;
                }
                eqArg = arg00.getChild(0);
            }
            if (!eqArg.equals(eqChild)) {
                return null;
            }
            return rewriteImpl(shuttleContext, aggFunc, replace);
        } else if (aggFuncName.equals(BITMAP_AGG)) {
            ScalarOperator arg0 = aggFunc.getChild(0);
            if (!arg0.equals(eqChild)) {
                return null;
            }
            return rewriteImpl(shuttleContext, aggFunc, replace);
        }
        return null;
    }

    @Override
    public ScalarOperator rewriteRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                     CallOperator aggFunc,
                                                     ColumnRefOperator replace) {
        String aggFuncName = aggFunc.getFnName();
        if (aggFuncName.equals(BITMAP_AGG)) {
            return makeBitmapUnionFunc(replace);
        } else {
            return makeBitmapUnionCountFunc(replace);
        }
    }

    @Override
    public ScalarOperator rewriteAggregateFuncWithoutRollup(EquivalentShuttleContext shuttleContext,
                                                            CallOperator aggFunc,
                                                            ColumnRefOperator replace) {
        String aggFuncName = aggFunc.getFnName();
        if (aggFuncName.equals(BITMAP_AGG)) {
            return makeBitmapUnionFunc(replace);
        } else {
            return makeBitmapCountFunc(replace);
        }
    }

    @Override
    public Pair<CallOperator, CallOperator> rewritePushDownRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                                               CallOperator aggFunc,
                                                                               ColumnRefOperator replace) {
        String aggFuncName = aggFunc.getFnName();
        if (aggFuncName.equals(BITMAP_AGG)) {
            CallOperator partialFn = makeBitmapUnionFunc(replace);
            CallOperator finalFn = makeBitmapUnionFunc(replace);
            return Pair.create(partialFn, finalFn);
        } else {
            CallOperator partialFn = makeBitmapUnionFunc(replace);
            CallOperator finalFn = makeBitmapUnionCountFunc(replace);
            return Pair.create(partialFn, finalFn);
        }
    }
}