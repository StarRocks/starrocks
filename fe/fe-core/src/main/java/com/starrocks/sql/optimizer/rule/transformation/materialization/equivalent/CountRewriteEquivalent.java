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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.getRollupAggregateFunc;

public class CountRewriteEquivalent extends IAggregateRewriteEquivalent {
    public static IAggregateRewriteEquivalent INSTANCE = new CountRewriteEquivalent();

    public CountRewriteEquivalent() {}

    public static boolean check(ScalarOperator op) {
        if (op == null || !(op instanceof CallOperator)) {
            return false;
        }
        CallOperator agg = (CallOperator) op;
        String aggFuncName = agg.getFnName();
        if (!aggFuncName.equals(FunctionSet.COUNT) || agg.isDistinct()) {
            return false;
        }
        return isNonNullableCount(agg);
    }

    @Override
    public RewriteEquivalentContext prepare(ScalarOperator input) {
        if (!check(input)) {
            return null;
        }
        return new RewriteEquivalentContext(null, input);
    }

    private static boolean isNonNullableCount(CallOperator aggFunc) {
        return aggFunc.getChildren().size() == 0 || !aggFunc.getChild(0).isNullable();
    }

    @Override
    public boolean isSupportPushDownRewrite(CallOperator aggFunc) {
        if (aggFunc == null) {
            return false;
        }

        String aggFuncName = aggFunc.getFnName();
        if (!aggFuncName.equals(FunctionSet.COUNT) || aggFunc.isDistinct()) {
            return false;
        }
        return true;
    }

    @Override
    public ScalarOperator rewrite(RewriteEquivalentContext eqContext,
                                  EquivalentShuttleContext shuttleContext,
                                  ColumnRefOperator replace,
                                  ScalarOperator newInput) {
        if (!check(newInput)) {
            return null;
        }
        if (shuttleContext.isRollup()) {
            return getRollupAggregateFunc((CallOperator) eqContext.getInput(), replace, false);
        } else {
            return replace;
        }
    }

    @Override
    public ScalarOperator rewriteRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                     CallOperator aggFunc,
                                                     ColumnRefOperator replace) {
        return getRollupAggregateFunc(aggFunc, replace, false);
    }

    @Override
    public ScalarOperator rewriteAggregateFuncWithoutRollup(EquivalentShuttleContext shuttleContext,
                                                            CallOperator aggFunc,
                                                            ColumnRefOperator replace) {
        return replace;
    }

    @Override
    public Pair<CallOperator, CallOperator> rewritePushDownRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                                               CallOperator aggFunc,
                                                                               ColumnRefOperator replace) {
        return null;
    }
}