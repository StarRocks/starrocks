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

class RightChildEstimationErrorTuningGuideTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    // original plan: small table inner join large table(broadcast)
    // rewrite to: large table inner join small table(broadcast)
    @Test
    void testInputUnderestimatedCaseOne() throws Exception {
        String sql = "select * from (select * from customer where c_acctbal > 5000 ) c " +
                "join (select * from supplier where s_acctbal = 1) s on abs(c_custkey) = abs(s_suppkey)";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();
        Assert.assertTrue(root.inputAt(0).getOp() instanceof PhysicalOlapScanOperator);
        Assert.assertTrue(root.inputAt(1).getOp() instanceof PhysicalDistributionOperator);
        NodeExecStats left = new NodeExecStats(0, 500000, 500000, 0, 0, 0);
        NodeExecStats right = new NodeExecStats(4, 5000000, 5000000, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(0, left);
        map.put(4, right);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        RightChildEstimationErrorTuningGuide guide = new RightChildEstimationErrorTuningGuide((JoinNode) pair.first,
                JoinTuningGuide.EstimationErrorType.RIGHT_INPUT_UNDERESTIMATED);
        Optional<OptExpression> res = guide.applyImpl(root);
        Assert.assertTrue(res.isPresent());

        OptExpression newPlan = res.get();
        OptExpression newLeftChild = newPlan.inputAt(0);
        OptExpression newRightChild = newPlan.inputAt(1);
        Assert.assertTrue(newLeftChild.getOp() instanceof PhysicalOlapScanOperator);
        Assert.assertTrue(newRightChild.getOp() instanceof PhysicalDistributionOperator);

        Assert.assertTrue("supplier".equals(
                ((PhysicalOlapScanOperator) newLeftChild.getOp()).getTable().getName()));
        Assert.assertTrue("customer".equals(
                ((PhysicalOlapScanOperator) newRightChild.inputAt(0).getOp()).getTable().getName()));
    }

    // original plan: medium table inner join large table(broadcast)
    // rewrite to: large table(shuffle) inner join medium table(shuffle)
    @Test
    void testInputUnderestimatedCaseTwo() throws Exception {
        String sql = "select * from (select * from customer where c_acctbal > 5000 ) c " +
                "join (select * from supplier where s_acctbal = 1) s on abs(c_custkey) = abs(s_suppkey)";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();
        Assert.assertTrue(root.inputAt(0).getOp() instanceof PhysicalOlapScanOperator);
        Assert.assertTrue(root.inputAt(1).getOp() instanceof PhysicalDistributionOperator);
        NodeExecStats left = new NodeExecStats(0, 2000000, 2000000, 0, 0, 0);
        NodeExecStats right = new NodeExecStats(4, 5000000, 5000000, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(0, left);
        map.put(4, right);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        RightChildEstimationErrorTuningGuide guide = new RightChildEstimationErrorTuningGuide((JoinNode) pair.first,
                JoinTuningGuide.EstimationErrorType.RIGHT_INPUT_UNDERESTIMATED);
        Optional<OptExpression> res = guide.applyImpl(root);
        Assert.assertTrue(res.isPresent());

        OptExpression newPlan = res.get();
        OptExpression newLeftChild = newPlan.inputAt(0);
        OptExpression newRightChild = newPlan.inputAt(1);
        Assert.assertTrue(newLeftChild.getOp() instanceof PhysicalDistributionOperator);
        Assert.assertTrue(newRightChild.getOp() instanceof PhysicalDistributionOperator);

        Assert.assertTrue("supplier".equals(
                ((PhysicalOlapScanOperator) newLeftChild.inputAt(0).getOp()).getTable().getName()));
        Assert.assertTrue("customer".equals(
                ((PhysicalOlapScanOperator) newRightChild.inputAt(0).getOp()).getTable().getName()));
    }

    // original plan: large table1 inner join large table2(broadcast)
    // rewrite to: large table1(shuffle) inner join large table2(shuffle)
    @Test
    void testInputUnderestimatedCaseThree() throws Exception {
        String sql = "select * from (select * from customer where c_acctbal > 5000 ) c " +
                "join (select * from supplier where s_acctbal = 1) s on abs(c_custkey) = abs(s_suppkey)";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();
        Assert.assertTrue(root.inputAt(0).getOp() instanceof PhysicalOlapScanOperator);
        Assert.assertTrue(root.inputAt(1).getOp() instanceof PhysicalDistributionOperator);
        NodeExecStats left = new NodeExecStats(0, 5000000, 5000000, 0, 0, 0);
        NodeExecStats right = new NodeExecStats(4, 5000000, 5000000, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(0, left);
        map.put(4, right);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        RightChildEstimationErrorTuningGuide guide = new RightChildEstimationErrorTuningGuide((JoinNode) pair.first,
                JoinTuningGuide.EstimationErrorType.RIGHT_INPUT_UNDERESTIMATED);
        Optional<OptExpression> res = guide.applyImpl(root);
        Assert.assertTrue(res.isPresent());

        OptExpression newPlan = res.get();
        OptExpression newLeftChild = newPlan.inputAt(0);
        OptExpression newRightChild = newPlan.inputAt(1);
        Assert.assertTrue(newLeftChild.getOp() instanceof PhysicalDistributionOperator);
        Assert.assertTrue(newRightChild.getOp() instanceof PhysicalDistributionOperator);

        Assert.assertTrue("customer".equals(
                ((PhysicalOlapScanOperator) newLeftChild.inputAt(0).getOp()).getTable().getName()));
        Assert.assertTrue("supplier".equals(
                ((PhysicalOlapScanOperator) newRightChild.inputAt(0).getOp()).getTable().getName()));
    }

    // original plan: small table(shuffle) inner join large table(shuffle)
    // rewrite to: large table inner join small table(broadcast)
    @Test
    void testInputUnderestimatedCaseFour() throws Exception {
        String sql = "select * from (select * from customer where c_acctbal > 3000) c " +
                "join (select * from supplier) s on abs(c_custkey) = abs(s_suppkey)";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();
        Assert.assertTrue(root.inputAt(0).getOp() instanceof PhysicalDistributionOperator);
        Assert.assertTrue(root.inputAt(1).getOp() instanceof PhysicalDistributionOperator);
        NodeExecStats left = new NodeExecStats(2, 10, 10, 0, 0, 0);
        NodeExecStats right = new NodeExecStats(5, 5000000, 5000000, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(2, left);
        map.put(5, right);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        RightChildEstimationErrorTuningGuide guide = new RightChildEstimationErrorTuningGuide((JoinNode) pair.first,
                JoinTuningGuide.EstimationErrorType.RIGHT_INPUT_UNDERESTIMATED);
        Optional<OptExpression> res = guide.applyImpl(root);
        Assert.assertTrue(res.isPresent());

        OptExpression newPlan = res.get();
        OptExpression newLeftChild = newPlan.inputAt(0);
        OptExpression newRightChild = newPlan.inputAt(1);
        Assert.assertTrue(newLeftChild.getOp() instanceof PhysicalOlapScanOperator);
        Assert.assertTrue(newRightChild.getOp() instanceof PhysicalDistributionOperator);

        Assert.assertTrue("supplier".equals(
                ((PhysicalOlapScanOperator) newLeftChild.getOp()).getTable().getName()));
        Assert.assertTrue("customer".equals(
                ((PhysicalOlapScanOperator) newRightChild.inputAt(0).getOp()).getTable().getName()));
    }

    // original plan: medium table(shuffle) inner join large table(shuffle)
    // rewrite to: large table(shuffle) inner join medium table(shuffle)
    @Test
    void testInputUnderestimatedCaseFive() throws Exception {
        String sql = "select * from (select * from customer where c_acctbal > 3000) c " +
                "join (select * from supplier) s on abs(c_custkey) = abs(s_suppkey)";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();
        Assert.assertTrue(root.inputAt(0).getOp() instanceof PhysicalDistributionOperator);
        Assert.assertTrue(root.inputAt(1).getOp() instanceof PhysicalDistributionOperator);
        NodeExecStats left = new NodeExecStats(2, 2000000, 2000000, 0, 0, 0);
        NodeExecStats right = new NodeExecStats(5, 5000000, 5000000, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(2, left);
        map.put(5, right);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        RightChildEstimationErrorTuningGuide guide = new RightChildEstimationErrorTuningGuide((JoinNode) pair.first,
                JoinTuningGuide.EstimationErrorType.RIGHT_INPUT_UNDERESTIMATED);
        Optional<OptExpression> res = guide.applyImpl(root);
        Assert.assertTrue(res.isPresent());

        OptExpression newPlan = res.get();
        OptExpression newLeftChild = newPlan.inputAt(0);
        OptExpression newRightChild = newPlan.inputAt(1);
        Assert.assertTrue(newLeftChild.getOp() instanceof PhysicalDistributionOperator);
        Assert.assertTrue(newRightChild.getOp() instanceof PhysicalDistributionOperator);

        Assert.assertTrue("supplier".equals(
                ((PhysicalOlapScanOperator) newLeftChild.inputAt(0).getOp()).getTable().getName()));
        Assert.assertTrue("customer".equals(
                ((PhysicalOlapScanOperator) newRightChild.inputAt(0).getOp()).getTable().getName()));
    }

    // original plan: large table(shuffle) inner join small table(shuffle)
    // rewrite to: large table inner join small table(broadcast)
    @Test
    void testInputOverestimated() throws Exception {
        String sql = "select * from (select * from customer where c_acctbal > 3000) c " +
                "join (select * from supplier) s on abs(c_custkey) = abs(s_suppkey)";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();
        Assert.assertTrue(root.inputAt(0).getOp() instanceof PhysicalDistributionOperator);
        Assert.assertTrue(root.inputAt(1).getOp() instanceof PhysicalDistributionOperator);
        NodeExecStats left = new NodeExecStats(2, 1000000, 1000000, 0, 0, 0);
        NodeExecStats right = new NodeExecStats(5, 5000, 5000, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(2, left);
        map.put(5, right);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        RightChildEstimationErrorTuningGuide guide = new RightChildEstimationErrorTuningGuide((JoinNode) pair.first,
                JoinTuningGuide.EstimationErrorType.RIGHT_INPUT_OVERESTIMATED);
        Optional<OptExpression> res = guide.applyImpl(root);
        Assert.assertTrue(res.isPresent());

        OptExpression newPlan = res.get();
        OptExpression newLeftChild = newPlan.inputAt(0);
        OptExpression newRightChild = newPlan.inputAt(1);
        Assert.assertTrue(newLeftChild.getOp() instanceof PhysicalOlapScanOperator);
        Assert.assertTrue(newRightChild.getOp() instanceof PhysicalDistributionOperator);

        Assert.assertTrue("customer".equals(
                ((PhysicalOlapScanOperator) newLeftChild.getOp()).getTable().getName()));
        Assert.assertTrue("supplier".equals(
                ((PhysicalOlapScanOperator) newRightChild.inputAt(0).getOp()).getTable().getName()));
    }
}