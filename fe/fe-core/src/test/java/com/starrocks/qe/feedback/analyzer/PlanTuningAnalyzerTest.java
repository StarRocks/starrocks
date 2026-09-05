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
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.qe.feedback.OperatorTuningGuides;
import com.starrocks.qe.feedback.PlanAdvisorMetrics;
import com.starrocks.qe.feedback.PlanTuningAdvisor;
import com.starrocks.qe.feedback.guide.LeftChildEstimationErrorTuningGuide;
import com.starrocks.qe.feedback.guide.RightChildEstimationErrorTuningGuide;
import com.starrocks.qe.feedback.guide.StreamingAggTuningGuide;
import com.starrocks.qe.feedback.skeleton.ScanNode;
import com.starrocks.qe.feedback.skeleton.SkeletonBuilder;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
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

    private long getMetricValue(String name, String operatorType) {
        for (Metric<?> metric : MetricRepo.getMetricsByName(name)) {
            for (MetricLabel label : metric.getLabels()) {
                if ("operator_type".equals(label.getKey()) && operatorType.equals(label.getValue())) {
                    return (Long) metric.getValue();
                }
            }
        }
        return 0L;
    }

    @Test
    void testStreamingAnalyzer() throws Exception {
        String sql = "select count(*) from lineitem group by l_shipmode";
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();

        NodeExecStats localAgg = new NodeExecStats(1, 3000000000L, 2000000000L, 0, 0, 0);
        NodeExecStats globalAgg = new NodeExecStats(3, 500000, 7, 0, 0, 0);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(1, localAgg);
        map.put(3, globalAgg);
        SkeletonBuilder skeletonBuilder = new SkeletonBuilder(map);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = skeletonBuilder.buildSkeleton(root);
        OperatorTuningGuides tuningGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
        PlanTuningAnalyzer.getInstance().analyzePlan(execPlan.getPhysicalPlan(), pair.second, tuningGuides);
        Assertions.assertTrue(tuningGuides.getTuningGuides(2).get(0) instanceof StreamingAggTuningGuide);
    }

    @Test
    void testPlanAdvisorGeneratedMetric() throws Exception {
        PlanTuningAdvisor.getInstance().clearAllAdvisor();

        String aggSql = "select count(*) from lineitem group by l_shipmode";
        ExecPlan aggExecPlan = getExecPlan(aggSql);
        Map<Integer, NodeExecStats> aggExecStats = Maps.newHashMap();
        aggExecStats.put(1, new NodeExecStats(1, 3000000000L, 2000000000L, 0, 0, 0));
        aggExecStats.put(3, new NodeExecStats(3, 500000, 7, 0, 0, 0));
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> aggPair =
                new SkeletonBuilder(aggExecStats).buildSkeleton(aggExecPlan.getPhysicalPlan());
        OperatorTuningGuides aggGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
        PlanTuningAnalyzer.getInstance().analyzePlan(aggExecPlan.getPhysicalPlan(), aggPair.second, aggGuides);

        long aggBefore = getMetricValue("plan_advisor_guide_generated_total", PlanAdvisorMetrics.AGG_OPERATOR_TYPE);
        PlanTuningAdvisor.getInstance().putTuningGuides(aggSql, aggPair.first, aggGuides);
        long aggAfter = getMetricValue("plan_advisor_guide_generated_total", PlanAdvisorMetrics.AGG_OPERATOR_TYPE);
        Assertions.assertEquals(aggBefore + 1, aggAfter);

        String joinSql = "select * from (select * from customer) c " +
                "join (select * from supplier) s on abs(c_custkey) = abs(s_suppkey)";
        ExecPlan joinExecPlan = getExecPlan(joinSql);
        Map<Integer, NodeExecStats> joinExecStats = Maps.newHashMap();
        joinExecStats.put(0, new NodeExecStats(0, 500, 500, 0, 0, 0));
        joinExecStats.put(4, new NodeExecStats(4, 20000000, 20000000, 0, 0, 0));
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> joinPair =
                new SkeletonBuilder(joinExecStats).buildSkeleton(joinExecPlan.getPhysicalPlan());
        OperatorTuningGuides joinGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
        PlanTuningAnalyzer.getInstance().analyzePlan(joinExecPlan.getPhysicalPlan(), joinPair.second, joinGuides);

        long joinBefore = getMetricValue("plan_advisor_guide_generated_total", PlanAdvisorMetrics.JOIN_OPERATOR_TYPE);
        PlanTuningAdvisor.getInstance().putTuningGuides(joinSql, joinPair.first, joinGuides);
        long joinAfter = getMetricValue("plan_advisor_guide_generated_total", PlanAdvisorMetrics.JOIN_OPERATOR_TYPE);
        Assertions.assertEquals(joinBefore + 1, joinAfter);
    }

    @Test
    void testPlanAdvisorAppliedAndOptimizationMetrics() throws Exception {
        PlanTuningAdvisor.getInstance().clearAllAdvisor();

        String sql = "select count(*) from lineitem group by l_shipmode";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        connectContext.setExecutor(new StmtExecutor(connectContext, stmt));

        ExecPlan execPlan = getExecPlan(sql);
        Map<Integer, NodeExecStats> execStats = Maps.newHashMap();
        execStats.put(1, new NodeExecStats(1, 3000000000L, 2000000000L, 0, 0, 0));
        execStats.put(3, new NodeExecStats(3, 500000, 7, 0, 0, 0));
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair =
                new SkeletonBuilder(execStats).buildSkeleton(execPlan.getPhysicalPlan());
        OperatorTuningGuides tuningGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
        PlanTuningAnalyzer.getInstance().analyzePlan(execPlan.getPhysicalPlan(), pair.second, tuningGuides);
        PlanTuningAdvisor.getInstance().putTuningGuides(sql, pair.first, tuningGuides);

        long appliedBefore = getMetricValue("plan_advisor_guide_applied_total", PlanAdvisorMetrics.AGG_OPERATOR_TYPE);
        getExecPlan(sql);
        long appliedAfter = getMetricValue("plan_advisor_guide_applied_total", PlanAdvisorMetrics.AGG_OPERATOR_TYPE);
        Assertions.assertEquals(appliedBefore + 1, appliedAfter);

        OperatorTuningGuides usedTuningGuides =
                PlanTuningAdvisor.getInstance().getOperatorTuningGuides(connectContext.getQueryId());
        Assertions.assertNotNull(usedTuningGuides);

        long durationBefore = MetricRepo.COUNTER_PLAN_ADVISOR_OPTIMIZATION_DURATION_MS_TOTAL.getValue();
        usedTuningGuides.addOptimizedRecord(connectContext.getQueryId(), usedTuningGuides.getOriginalTimeCost() - 1);
        long durationAfter = MetricRepo.COUNTER_PLAN_ADVISOR_OPTIMIZATION_DURATION_MS_TOTAL.getValue();
        Assertions.assertEquals(durationBefore + 1, durationAfter);
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
            Assertions.assertTrue(tuningGuides.getTuningGuides(0).get(0) instanceof RightChildEstimationErrorTuningGuide);
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
            Assertions.assertTrue(tuningGuides.getTuningGuides(0).get(0) instanceof RightChildEstimationErrorTuningGuide);
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
            Assertions.assertTrue(tuningGuides.getTuningGuides(0).get(0) instanceof LeftChildEstimationErrorTuningGuide);
            PlanTuningAdvisor.getInstance().putTuningGuides(sql, pair.first, tuningGuides);
            List<List<String>> showResult = PlanTuningAdvisor.getInstance().getShowResult();
            Assertions.assertEquals(8, showResult.get(0).size());
        }
    }

    @Test
    public void testTuneGuidesInfoFormat() throws Exception {
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

        String tuneGuidesInfo = tuningGuides.getTuneGuidesInfo(false);
        String fullTuneGuidesInfo = tuningGuides.getFullTuneGuidesInfo();

        Assertions.assertEquals("[PlanNode 5] (RightChildEstimationErrorTuningGuide)", tuneGuidesInfo);
        Assertions.assertEquals("[PlanNode 5] (RightChildEstimationErrorTuningGuide) " +
                                "(Reason: Right child statistics of JoinNode 5 had been underestimated.) " +
                                "(Advice: Adjust the distribution join execution type " +
                                "and join plan to improve the performance.)", fullTuneGuidesInfo);
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