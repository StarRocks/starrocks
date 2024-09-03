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
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

class LeftChildEstimationErrorTuningGuideTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @Test
    void testApplyImpl() throws Exception {
        String sql = "select * from lineitem l join supplier s on abs(l.l_orderkey) = abs(s.s_suppkey)";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();
        Assert.assertTrue(root.inputAt(0).getOp() instanceof PhysicalOlapScanOperator);
        Assert.assertTrue(root.inputAt(1).getOp() instanceof PhysicalDistributionOperator);
        NodeExecStats left = new NodeExecStats(0, 0, 50000, 0, 0, 0);
        NodeExecStats right = new NodeExecStats(4, 10000000, 10000000, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(0, left);
        map.put(4, right);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        LeftChildEstimationErrorTuningGuide guide = new LeftChildEstimationErrorTuningGuide((JoinNode) pair.first,
                JoinTuningGuide.EstimationErrorType.LEFT_INPUT_OVERESTIMATED);
        Optional<OptExpression> res = guide.applyImpl(root);
        Assert.assertTrue(res.isPresent());

        OptExpression newPlan = res.get();
        OptExpression newLeftChild = newPlan.inputAt(0);
        OptExpression newRightChild = newPlan.inputAt(1);
        Assert.assertTrue(newLeftChild.getOp() instanceof PhysicalDistributionOperator);
        Assert.assertTrue(newRightChild.getOp() instanceof PhysicalDistributionOperator);

        Assert.assertTrue("lineitem".equals(
                ((PhysicalOlapScanOperator) newLeftChild.inputAt(0).getOp()).getTable().getName()));
        Assert.assertTrue("supplier".equals(
                ((PhysicalOlapScanOperator) newRightChild.inputAt(0).getOp()).getTable().getName()));
    }
}