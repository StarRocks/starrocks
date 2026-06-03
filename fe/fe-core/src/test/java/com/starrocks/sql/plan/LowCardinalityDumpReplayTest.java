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

package com.starrocks.sql.plan;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.compress.utils.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

/**
 * Replay-from-dump tests that verify low-cardinality dictionary optimization
 * produces correct query plans.
 */
public class LowCardinalityDumpReplayTest extends ReplayFromDumpTestBase {

    /**
     * Regression test for the bug where low-cardinality dict optimization (V2)
     * incorrectly rewrites a max(VARCHAR) aggregate function to max(INT) signature
     * when the VARCHAR column is decoded back from an INT dict column by a project
     * node that also uses find_in_set on the same column.
     * <p>
     * The wrong plan had: max[([label, VARCHAR]); args: INT; result: INT]
     * which caused a BE SIGSEGV crash because MaxMinAggregateFunction<INT>
     * processed a VARCHAR input column.
     * <p>
     * Fixed in AggregateRewriter.visitCall(): guard with !hasChange[0] before
     * creating an INT-typed aggregate function signature.
     * Fixed in DecodeRewriter.visitPhysicalHashAggregate(): skip rewriting an
     * aggregate function when its input columns are not all dict-encoded.
     */
    @Test
    public void testLowCardMaxTypeMismatchBugFix() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        try {
            String dumpJson = getDumpInfoFromFile("query_dump/mock-files/low_cardinality_max_type_mismatch");
            Pair<String, ExecPlan> result =
                    UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, getDumpInfoFromJson(dumpJson));
            List<PlanNode> planNodes = Lists.newArrayList();
            result.second.getTopFragment().getPlanRoot()
                    .collectAll(node -> AggregationNode.class.equals(node.getClass()), planNodes);
            List<AggregationNode> aggNodes = planNodes.stream().map(node -> (AggregationNode) node).toList();
            Assertions.assertFalse(aggNodes.isEmpty());
            Optional<Pair<AggregationNode, FunctionCallExpr>> mismatchAggExprs = aggNodes.stream().flatMap(
                            agg -> agg.getAggInfo().getAggregateExprs().stream().map(aggExpr -> Pair.create(agg, aggExpr)))
                    .filter(p -> p.second.getFn().getFunctionName().getFunction().equals(FunctionSet.MAX) &&
                            !p.second.getChild(0).getType().equals(p.second.getFn().getArgs()[0])).findFirst();
            Assertions.assertTrue(mismatchAggExprs.isEmpty());
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = false;
        }
    }
}
