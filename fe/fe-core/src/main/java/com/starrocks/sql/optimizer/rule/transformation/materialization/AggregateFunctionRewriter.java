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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;

import java.util.Map;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.getRollupAggregateFunc;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.createAvgBySumCount;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.createNewCallOperator;

/**
 * `AggregateFunctionRewriter` will try to rewrite some agg functions to some transformations so can be
 * better to be rewritten.
 * eg: AVG -> SUM / COUNT
 */
public class AggregateFunctionRewriter {
    private final ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
    private final Map<ColumnRefOperator, CallOperator> newColumnRefToAggFuncMap = Maps.newHashMap();

    private final EquationRewriter equationRewriter;
    private final ColumnRefFactory queryColumnRefFactory;
    // new rewrite agg can reuse old agg functions if it has existed.
    private final Map<ColumnRefOperator, CallOperator> oldAggregations;

    public AggregateFunctionRewriter(EquationRewriter equationRewriter,
                                     ColumnRefFactory queryColumnRefFactory,
                                     Map<ColumnRefOperator, CallOperator> oldAggregations) {
        this.equationRewriter = equationRewriter;
        this.queryColumnRefFactory = queryColumnRefFactory;
        this.oldAggregations = oldAggregations;
    }

    public boolean canRewriteAggFunction(ScalarOperator op) {
        if (!(op instanceof CallOperator)) {
            return false;
        }

        CallOperator aggFunc = (CallOperator) op;
        String aggFuncName = aggFunc.getFnName();
        if (aggFuncName.equals(FunctionSet.AVG)) {
            return true;
        }
        return false;
    }

    public ScalarOperator rewriteAggFunction(CallOperator aggFunc, boolean isRollup) {
        String aggFuncName = aggFunc.getFnName();
        if (aggFuncName.equals(FunctionSet.AVG)) {
            return rewriteAvg(aggFunc, isRollup);
        } else {
            return null;
        }
    }

    /**
     * For avg with rollup, return div(sum_col_ref, count_col_ref) and new sum/count call operator with newColumnRefToAggFuncMap.
     * For avg without rollup, return rewritten div(sum_call_op, count_call_op).
     * @param aggFunc  input avg function
     * @param isRollup whether the avg function is with rollup
     */
    private ScalarOperator rewriteAvg(CallOperator aggFunc, boolean isRollup) {
        // construct `sum` agg
        Function sumFn = ScalarOperatorUtil.findSumFn(aggFunc.getFunction().getArgs());
        Pair<ColumnRefOperator, CallOperator> sumCallOp =
                createNewCallOperator(queryColumnRefFactory, oldAggregations, sumFn, aggFunc.getChildren());

        Function countFn = ScalarOperatorUtil.findArithmeticFunction(aggFunc.getFunction().getArgs(), FunctionSet.COUNT);
        Pair<ColumnRefOperator, CallOperator> countCallOp = createNewCallOperator(queryColumnRefFactory, oldAggregations,
                countFn, aggFunc.getChildren());

        CallOperator newAvg = getNewAVGBySumCount(aggFunc, sumCallOp, countCallOp, isRollup);
        if (isRollup) {
            // add sum/count agg into aggregations map
            CallOperator sumRollupCall = getRollupFunction(sumCallOp.second);
            if (sumRollupCall == null) {
                return null;
            }
            CallOperator cntRollupCall = getRollupFunction(countCallOp.second);
            if (cntRollupCall == null) {
                return null;
            }
            newColumnRefToAggFuncMap.put(sumCallOp.first, sumRollupCall);
            newColumnRefToAggFuncMap.put(countCallOp.first, cntRollupCall);
            return newAvg;
        } else {
            return rewriteAggFunction(newAvg);
        }
    }

    private CallOperator getNewAVGBySumCount(CallOperator aggFunc,
                                             Pair<ColumnRefOperator, CallOperator> sumCallOp,
                                             Pair<ColumnRefOperator, CallOperator> countCallOp,
                                             boolean isRollup) {
        if (isRollup) {
            return createAvgBySumCount(aggFunc, sumCallOp.first, countCallOp.first);
        } else {
            return createAvgBySumCount(aggFunc, sumCallOp.second, countCallOp.second);
        }
    }

    private ScalarOperator rewriteAggFunction(CallOperator aggFunc) {
        ScalarOperator rewritten = equationRewriter.replaceExprWithTarget(aggFunc);
        if (rewritten == null || aggFunc.equals(rewritten)) {
            return null;
        }
        return rewritten;
    }

    private CallOperator getRollupFunction(CallOperator aggFunc) {
        ScalarOperator rewritten = rewriteAggFunction(aggFunc);
        if (rewritten == null || !(rewritten instanceof ColumnRefOperator)) {
            return null;
        }
        return getRollupAggregateFunc(aggFunc, (ColumnRefOperator) rewritten, false);
    }

    public Map<ColumnRefOperator, CallOperator> getNewColumnRefToAggFuncMap() {
        return newColumnRefToAggFuncMap;
    }
}
