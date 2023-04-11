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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;

import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;

/**
 * `AggregateFunctionRewriter` will try to rewrite some agg functions to some transformations so can be
 * better to be rewritten.
 * eg: AVG -> SUM / COUNT
 * eg: COUNT(DISTINCT) -> BITMAP_COUNT(BITMAP_UNION(TO_BITMAP)))
 */
public class AggregateFunctionRewriter {
    final ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
    final ColumnRefFactory queryColumnRefFactory;

    // new rewrite agg can reuse old agg functions if it has existed.
    final Map<ColumnRefOperator, CallOperator> oldAggregations;

    Map<ColumnRefOperator, CallOperator> newColumnRefToAggFuncMap;

    public AggregateFunctionRewriter(ColumnRefFactory queryColumnRefFactory,
                                     Map<ColumnRefOperator, CallOperator> oldAggregations) {
        this.queryColumnRefFactory = queryColumnRefFactory;
        this.oldAggregations = oldAggregations;
    }

    public AggregateFunctionRewriter(ColumnRefFactory queryColumnRefFactory,
                                     Map<ColumnRefOperator, CallOperator> oldAggregations,
                                     Map<ColumnRefOperator, CallOperator> newColumnRefToAggFuncMap) {
        this.queryColumnRefFactory = queryColumnRefFactory;
        this.oldAggregations = oldAggregations;
        this.newColumnRefToAggFuncMap = newColumnRefToAggFuncMap;
    }

    public boolean canRewriteAggFunction(ScalarOperator op) {
        if (!(op instanceof CallOperator)) {
            return false;
        }

        CallOperator aggFunc = (CallOperator) op;
        if (aggFunc.getFnName().equals(FunctionSet.AVG)) {
            return true;
        }
        if (aggFunc.getFnName().equals(FunctionSet.COUNT) && aggFunc.isDistinct()) {
            return true;
        }
        return false;
    }

    public CallOperator rewriteAggFunction(CallOperator aggFunc) {
        if (aggFunc.getFnName().equals(FunctionSet.COUNT) && aggFunc.isDistinct()) {
            return rewriteCountDistinct(aggFunc);
        } else if (aggFunc.getFnName().equals(FunctionSet.AVG)) {
            return rewriteAvg(aggFunc);
        }

        return null;
    }

    private Function findArithmeticFunction(Type[] argsType, String fnName) {
        return Expr.getBuiltinFunction(fnName, argsType, IS_IDENTICAL);
    }

    private Pair<ColumnRefOperator, CallOperator> createNewCallOperator(Function newFn,
                                                                        List<ScalarOperator> args) {
        Preconditions.checkState(newFn != null);
        CallOperator newCallOp = new CallOperator(newFn.functionName(), newFn.getReturnType(), args, newFn);
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : oldAggregations.entrySet()) {
            if (entry.getValue().equals(newCallOp)) {
                return Pair.create(entry.getKey(), newCallOp);
            }
        }
        ColumnRefOperator newColRef =
                queryColumnRefFactory.create(newCallOp, newCallOp.getType(), newCallOp.isNullable());
        return Pair.create(newColRef, newCallOp);
    }

    private CallOperator rewriteAvg(CallOperator aggFunc) {
        Type argType = aggFunc.getChild(0).getType();

        // construct `sum` agg
        Function sumFn = findArithmeticFunction(aggFunc.getFunction().getArgs(), FunctionSet.SUM);
        Preconditions.checkState(sumFn != null);
        Type sumReturnType;
        if (argType.isDecimalV3()) {
            sumReturnType =
                    ScalarType.createDecimalV3NarrowestType(38, ((ScalarType) argType).getScalarScale());
        } else {
            sumReturnType = sumFn.getReturnType();
        }
        sumFn = sumFn.copy();
        sumFn.setArgsType(aggFunc.getFunction().getArgs());
        sumFn.setRetType(sumReturnType);
        Pair<ColumnRefOperator, CallOperator> sumCallOp =
                createNewCallOperator(sumFn, aggFunc.getChildren());

        Function countFn = findArithmeticFunction(aggFunc.getFunction().getArgs(), FunctionSet.COUNT);
        Pair<ColumnRefOperator, CallOperator> countCallOp = createNewCallOperator(countFn, aggFunc.getChildren());

        // add sum/count into projection
        CallOperator newAvg;
        if (newColumnRefToAggFuncMap != null) {
            newAvg = new CallOperator(FunctionSet.DIVIDE, aggFunc.getType(),
                    Lists.newArrayList(sumCallOp.first, countCallOp.first));
        } else {
            newAvg = new CallOperator(FunctionSet.DIVIDE, aggFunc.getType(),
                    Lists.newArrayList(sumCallOp.second, countCallOp.second));
        }
        if (argType.isDecimalV3()) {
            // There is not need to apply ImplicitCastRule to divide operator of decimal types.
            // but we should cast BIGINT-typed countColRef into DECIMAL(38,0).
            ScalarType decimal128p38s0 = ScalarType.createDecimalV3NarrowestType(38, 0);
            newAvg.getChildren().set(1, new CastOperator(decimal128p38s0, newAvg.getChild(1), true));
        } else {
            newAvg = (CallOperator) scalarRewriter.rewrite(newAvg, Lists.newArrayList(new ImplicitCastRule()));
        }

        // add sum/count agg into aggregations map
        if (newColumnRefToAggFuncMap != null) {
            newColumnRefToAggFuncMap.put(sumCallOp.first, sumCallOp.second);
            newColumnRefToAggFuncMap.put(countCallOp.first, countCallOp.second);
        }
        return newAvg;
    }

    private CallOperator rewriteCountDistinct(CallOperator aggFunc) {
        // What if bitmap_union aggregate table?
        Type childType = aggFunc.getChild(0).getType();
        List<ScalarOperator> aggChild = aggFunc.getChildren();
        if (!childType.isBitmapType()) {
            // add `to_bitmap` function for input
            CallOperator toBitmapOp = new CallOperator(FunctionSet.TO_BITMAP,
                    Type.BITMAP,
                    aggFunc.getChildren(),
                    Expr.getBuiltinFunction(FunctionSet.TO_BITMAP, new Type[] { Type.VARCHAR },
                            IS_IDENTICAL));
            toBitmapOp = (CallOperator) scalarRewriter.rewrite(toBitmapOp,
                    Lists.newArrayList(new ImplicitCastRule()));
            aggChild = Lists.newArrayList(toBitmapOp);
        }

        // rewrite count distinct to bitmap_count(bitmap_union(to_bitmap(x)));
        CallOperator bitmapUnionOp = new CallOperator(FunctionSet.BITMAP_UNION,
                Type.BITMAP,
                aggChild,
                Expr.getBuiltinFunction(FunctionSet.BITMAP_UNION, new Type[] {Type.BITMAP},
                        IS_IDENTICAL));
        List<ScalarOperator> newAggFunc = Lists.newArrayList(bitmapUnionOp);
        if (newColumnRefToAggFuncMap != null) {
            ColumnRefOperator newColRef =
                    queryColumnRefFactory.create(bitmapUnionOp, bitmapUnionOp.getType(), bitmapUnionOp.isNullable());
            newColumnRefToAggFuncMap.put(newColRef, bitmapUnionOp);
            newAggFunc = Lists.newArrayList(newColRef);
        }

        CallOperator bitmapCountOp = new CallOperator(FunctionSet.BITMAP_COUNT,
                aggFunc.getType(),
                newAggFunc,
                Expr.getBuiltinFunction(FunctionSet.BITMAP_COUNT, new Type[] {Type.BITMAP},
                        IS_IDENTICAL));
        return bitmapCountOp;
    }
}
