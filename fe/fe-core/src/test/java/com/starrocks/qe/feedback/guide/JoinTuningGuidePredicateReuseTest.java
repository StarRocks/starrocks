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

package com.starrocks.qe.feedback.guide;

import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.qe.feedback.skeleton.JoinNode;
import com.starrocks.qe.feedback.skeleton.SkeletonBuilder;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.validate.InputDependenciesChecker;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

class JoinTuningGuidePredicateReuseTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    // A join whose (post-join) predicate reuses a common sub expression carries predicateCommonOperators.
    // When a JoinTuningGuide rebuilds the join, it must preserve predicateCommonOperators; otherwise the
    // rebuilt join keeps a predicate referencing columns that are only defined in predicateCommonOperators,
    // and the plan fails InputDependenciesChecker with
    // "The required cols {..} cannot obtain from input cols {..}".
    @Test
    void testPredicateCommonOperatorsPreservedAfterTuning() throws Exception {
        // where repeats abs(c_custkey + s_suppkey), reused as a common sub expression on the join predicate.
        // The inner sub-queries only project the columns they need: customer and supplier both carry a PAD
        // column, so `select *` over the join would otherwise produce a duplicate output column named PAD.
        String sql = "select * from (select c_custkey from customer where c_acctbal > 5000 ) c " +
                "left join (select s_suppkey from supplier where s_acctbal = 1) s on abs(c_custkey) = abs(s_suppkey) " +
                "where abs(c_custkey + s_suppkey) = 10 or abs(c_custkey + s_suppkey) = 20";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();

        Assertions.assertInstanceOf(PhysicalHashJoinOperator.class, root.getOp());
        PhysicalHashJoinOperator origJoin = (PhysicalHashJoinOperator) root.getOp();
        Assertions.assertNotNull(origJoin.getPredicateCommonOperators());
        Assertions.assertFalse(origJoin.getPredicateCommonOperators().isEmpty());

        int leftPlanNodeId = root.inputAt(0).getOp().getPlanNodeId();
        int rightScanPlanNodeId = root.inputAt(1).inputAt(0).getOp().getPlanNodeId();
        NodeExecStats left = new NodeExecStats(leftPlanNodeId, 5000000, 5000000, 0, 0, 0);
        NodeExecStats right = new NodeExecStats(rightScanPlanNodeId, 500000, 500000, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(leftPlanNodeId, left);
        map.put(rightScanPlanNodeId, right);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        RightChildEstimationErrorTuningGuide guide = new RightChildEstimationErrorTuningGuide((JoinNode) pair.first,
                JoinTuningGuide.EstimationErrorType.RIGHT_INPUT_UNDERESTIMATED);
        Optional<OptExpression> res = guide.applyImpl(root);
        Assertions.assertTrue(res.isPresent());

        OptExpression newPlan = res.get();
        PhysicalHashJoinOperator newJoin = (PhysicalHashJoinOperator) newPlan.getOp();
        // The rebuilt join keeps the reused predicate, so it must keep the common operators that define it.
        Assertions.assertNotNull(newJoin.getPredicateCommonOperators());
        Assertions.assertEquals(origJoin.getPredicateCommonOperators(), newJoin.getPredicateCommonOperators());

        // Should no longer throw StarRocksPlannerException.
        InputDependenciesChecker.getInstance().validate(newPlan, null);
    }
}