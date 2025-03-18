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

package com.starrocks.qe.feedback.analyzer;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.qe.feedback.OperatorTuningGuides;
import com.starrocks.qe.feedback.PlanTuningAdvisor;
import com.starrocks.qe.feedback.guide.LeftChildEstimationErrorTuningGuide;
import com.starrocks.qe.feedback.guide.RightChildEstimationErrorTuningGuide;
import com.starrocks.qe.feedback.guide.StreamingAggTuningGuide;
import com.starrocks.qe.feedback.skeleton.ScanNode;
import com.starrocks.qe.feedback.skeleton.SkeletonBuilder;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;


class PlanTuningAnalyzerTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @Test
    void testStreamingAnalyzer() throws Exception {
        String sql = "select count(*) from lineitem group by l_shipmode";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();

        NodeExecStats localAgg = new NodeExecStats(1, 3000000000L, 2000000L, 0, 0, 0);
        NodeExecStats globalAgg = new NodeExecStats(3, 500000, 7, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(1, localAgg);
        map.put(3, globalAgg);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        OperatorTuningGuides tuningGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
        PlanTuningAnalyzer.getInstance().analyzePlan(execPlan.getPhysicalPlan(), pair.second, tuningGuides);
        Assert.assertTrue(tuningGuides.getTuningGuides(1).get(0) instanceof StreamingAggTuningGuide);
    }

    @Test
    void testJoinAnalyzer() throws Exception {
        {
            String sql = "select * from (select * from customer where c_acctbal > 5000 ) c " +
                    "join (select * from supplier where s_acctbal = 1) s on abs(c_custkey) = abs(s_suppkey)";
            ExecPlan execPlan = getExecPlan(sql);
            OptExpression root = execPlan.getPhysicalPlan();
            NodeExecStats left = new NodeExecStats(0, 500000, 500000, 0, 0, 0);
            NodeExecStats right = new NodeExecStats(4, 20000000, 20000000, 0, 0, 0);
            Map<Integer, NodeExecStats> map = Maps.newHashMap();
            map.put(0, left);
            map.put(4, right);
            SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
            Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
            OperatorTuningGuides tuningGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
            PlanTuningAnalyzer.getInstance().analyzePlan(execPlan.getPhysicalPlan(), pair.second, tuningGuides);
            Assert.assertTrue(tuningGuides.getTuningGuides(5).get(0) instanceof RightChildEstimationErrorTuningGuide);
        }

        {
            String sql = "select * from (select * from customer) c " +
                    "join (select * from supplier) s on abs(c_custkey) = abs(s_suppkey)";
            ExecPlan execPlan = getExecPlan(sql);
            OptExpression root = execPlan.getPhysicalPlan();
            NodeExecStats left = new NodeExecStats(0, 500000, 500000, 0, 0, 0);
            NodeExecStats right = new NodeExecStats(4, 20, 20, 0, 0, 0);
            Map<Integer, NodeExecStats> map = Maps.newHashMap();
            map.put(0, left);
            map.put(4, right);
            SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
            Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
            OperatorTuningGuides tuningGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
            PlanTuningAnalyzer.getInstance().analyzePlan(execPlan.getPhysicalPlan(), pair.second, tuningGuides);
            Assert.assertTrue(tuningGuides.getTuningGuides(5).get(0) instanceof RightChildEstimationErrorTuningGuide);
        }

        {
            String sql = "select * from (select * from customer) c " +
                    "join (select * from supplier) s on abs(c_custkey) = abs(s_suppkey)";
            ExecPlan execPlan = getExecPlan(sql);
            OptExpression root = execPlan.getPhysicalPlan();
            NodeExecStats left = new NodeExecStats(0, 500, 500, 0, 0, 0);
            NodeExecStats right = new NodeExecStats(4, 20000000, 20000000, 0, 0, 0);
            Map<Integer, NodeExecStats> map = Maps.newHashMap();
            map.put(0, left);
            map.put(4, right);
            SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
            Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
            OperatorTuningGuides tuningGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
            PlanTuningAnalyzer.getInstance().analyzePlan(execPlan.getPhysicalPlan(), pair.second, tuningGuides);
            Assert.assertTrue(tuningGuides.getTuningGuides(5).get(0) instanceof LeftChildEstimationErrorTuningGuide);
            PlanTuningAdvisor.getInstance().putTuningGuides(sql, pair.first, tuningGuides);
            List<List<String>> showResult = PlanTuningAdvisor.getInstance().getShowResult();
            Assert.assertEquals(8, showResult.get(0).size());
        }
    }

    @Test
    public void testSameScanNode() throws Exception {
        String sql = "select * from customer l join customer r on l.c_custkey = r.c_custkey";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();
        NodeExecStats left = new NodeExecStats(0, 500, 500, 0, 0, 0);
        NodeExecStats right = new NodeExecStats(1, 20000000, 20000000, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(0, left);
        map.put(1, right);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        SkeletonNode rootSkeletonNode = pair.first;
        List<ScanNode> scanNodeList = new ArrayList<>();
        rootSkeletonNode.collectAll(Predicates.instanceOf(ScanNode.class), scanNodeList);
        Assertions.assertNotEquals(scanNodeList.get(0), scanNodeList.get(1));
        Assertions.assertEquals(scanNodeList.get(0).getTableIdentifier(), scanNodeList.get(1).getTableIdentifier());
        Assertions.assertEquals(scanNodeList.get(0).getTableId(), scanNodeList.get(1).getTableId());
    }
}