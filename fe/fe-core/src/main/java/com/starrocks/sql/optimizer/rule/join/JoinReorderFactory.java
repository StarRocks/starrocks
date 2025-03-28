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
package com.starrocks.sql.optimizer.rule.join;

import com.google.api.client.util.Lists;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptimizerContext;

import java.util.List;

// JoinReorderFactory is used to choose join reorder algorithm in RBO phase,
// it is used by ReorderJoinRule.rewrite method, at present JoinReorderFactory
// provides two factory implementation:
// 1. factory for creating JoinReorderDummyStatistics, which used by AutoMV to eliminate
//    cross join;
// 2. factory for creating JoinReorderCardinalityPreserving, which used by table pruning
//    feature.
public interface JoinReorderFactory {
    List<JoinOrder> create(OptimizerContext context, MultiJoinNode multiJoinNode);

    // used by AutoMV to eliminate cross join.
    static JoinReorderFactory createJoinReorderDummyStatisticsFactory() {
        return (context, multiJoinNode) -> List.of(new JoinReorderDummyStatistics(context));
    }

    // used by table pruning feature.
    static JoinReorderFactory createJoinReorderCardinalityPreserving() {
        return (context, multiJoinNode) -> List.of(new JoinReorderCardinalityPreserving(context));
    }

    static JoinReorderFactory createJoinReorderAdaptive() {
        return (context, multiJoinNode) -> {
            List<JoinOrder> algorithms = Lists.newArrayList();
            algorithms.add(new JoinReorderLeftDeep(context));

            SessionVariable sv = context.getSessionVariable();
            if (sv.isCboEnableDPJoinReorder() && multiJoinNode.getAtoms().size() <= sv.getCboMaxReorderNodeUseDP()) {
                algorithms.add(new JoinReorderDP(context));
            }

            if (sv.isCboEnableGreedyJoinReorder() && multiJoinNode.getAtoms().size() <= sv.getCboMaxReorderNodeUseGreedy()) {
                algorithms.add(new JoinReorderGreedy(context));
            }

            return algorithms;
        };
    }
}
