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
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AggregatedMaterializedViewRewriter;

public class CountRewriteEquivalent extends IAggregateRewriteEquivalent {
    public static IRewriteEquivalent INSTANCE = new CountRewriteEquivalent();

    public CountRewriteEquivalent() {}

    private boolean check(ScalarOperator op) {
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
    public ScalarOperator rewrite(RewriteEquivalentContext eqContext,
                                  EquivalentShuttleContext shuttleContext,
                                  ColumnRefOperator replace,
                                  ScalarOperator newInput) {
        if (!check(newInput)) {
            return null;
        }
        if (shuttleContext.isRollup()) {
            return AggregatedMaterializedViewRewriter.getRollupAggregate((CallOperator) eqContext.getInput(), replace);
        } else {
            return replace;
        }
    }
}