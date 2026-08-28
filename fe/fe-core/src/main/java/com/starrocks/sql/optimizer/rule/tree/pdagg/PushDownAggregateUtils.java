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
     * count over an inner/cross join is N0*N1 (not recoverable from sum(cnt_left) and sum(cnt_right)), it
     * must be pushed to exactly one side.
     * <p>
     * INNER/CROSS: `child == 0` is an arbitrary tie-break, not a semantic requirement. count(*) uses no
     * columns, so the "aggregation columns must come from this child" check accepts both children and
     * something has to pick one; child 1 would be equally correct. Note child 0 is the logical left input at
     * this point in the plan, which is not necessarily the left table as written in the SQL, so which shape
     * of query benefits from this push down depends on the join order chosen upstream.
     * <p>
     * SEMI/ANTI: the join only filters its preserved side -- it neither duplicates those rows nor pads them
     * with NULLs -- and it decides row by row on the on-predicate columns, which splitJoinAggregate always
     * puts into the pushed group-by set. Every row of a pushed group therefore shares one verdict, so
     * sum(partial_count) over the surviving groups is exactly the count of the surviving rows. Rejecting
     * these would not merely forgo a count push down: since a stripped count forces the whole push down to
     * be abandoned, it would also take away the sum/min/max push downs that already worked below semi joins
     * (this is what regressed TPC-DS Q14, whose `ss_item_sk IN (...)` becomes a LEFT SEMI JOIN).
     */
    static boolean canPushCountToJoinChild(JoinOperator type, int child) {
        if (type.isInnerJoin() || type.isCrossJoin() || type.isLeftSemiAntiJoin()) {
            return child == 0;
        }
        if (type.isRightSemiAntiJoin()) {
            return child == 1;
        }
        return false;
    }

    /**
     * A count pushed without any real grouping key degenerates into a scalar (ungrouped) aggregate, which
     * always emits exactly one row even when its child has zero input rows. Re-joining that phantom row
     * through a keyless join (CROSS JOIN, or INNER JOIN with a non-column condition, where neither the
     * on-predicate nor the post-join-predicate contributed any column) would corrupt the join's
     * cardinality instead of correctly producing no rows/groups.
     * <p>
     * "No real grouping key" is not the same as an empty group-by map: a group-by on a constant
     * (`group by 1 + 1`) keeps an entry whose expression uses no column at all, and pushing a count under
     * it is just as ungrouped. Both passes must also check this on the group-by set they actually build,
     * because they build different ones -- the collector keeps composite expressions spanning both join
     * sides (`a + b` -> `a + NULL`) while the rewriter flattens group-bys into their component columns.
     */
    static boolean isUngroupedCountPush(AggregatePushDownContext context) {
        if (context.aggregations.values().stream().noneMatch(PushDownAggregateUtils::isCountAgg)) {
            return false;
        }
        return context.groupBys.values().stream().allMatch(g -> g.getUsedColumns().isEmpty());
    }
}
