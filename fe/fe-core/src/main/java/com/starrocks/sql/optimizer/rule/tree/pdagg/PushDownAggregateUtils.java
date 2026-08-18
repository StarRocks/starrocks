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

package com.starrocks.sql.optimizer.rule.tree.pdagg;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;

/**
 * Shared helpers between {@link PushDownAggregateCollector} and {@link PushDownAggregateRewriter} so the
 * two passes can't drift apart on what counts as a pushable count aggregate.
 */
class PushDownAggregateUtils {
    private PushDownAggregateUtils() {
    }

    static boolean isCountAgg(CallOperator call) {
        return !call.isDistinct() && FunctionSet.COUNT.equalsIgnoreCase(call.getFnName());
    }

    /**
     * COUNT's rollup is sum(partial). Unlike the rest of the whitelisted functions: sum-of-nothing is NULL
     * while count-of-nothing is 0, so the partial column must never be NULL-padded by a join; and because
     * count over a join is N0*N1 (not recoverable from sum(cnt_left) and sum(cnt_right)), it must be pushed
     * to exactly one side. v1 only covers INNER/CROSS pushed to child 0.
     */
    static boolean canPushCountToJoinChild(JoinOperator type, int child) {
        if (type.isInnerJoin() || type.isCrossJoin()) {
            return child == 0;
        }
        return false;
    }
}
