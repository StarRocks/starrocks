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
import com.starrocks.sql.parser.SqlParser;
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

    private OperatorTuningGuides analyzeAgg(String sql, NodeExecStats localAgg, NodeExecStats globalAgg)
            throws Exception {
        ExecPlan execPlan = getExecPlan(sql);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(1, localAgg);
        map.put(3, globalAgg);
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair =
                new SkeletonBuilder(map).buildSkeleton(execPlan.getPhysicalPlan());
        OperatorTuningGuides tuningGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
        PlanTuningAnalyzer.getInstance().analyzePlan(execPlan.getPhysicalPlan(), pair.second, tuningGuides);
        return tuningGuides;
    }

    // Post-filter count reads 200M/7 and would fire; the real 10M groups read 200M/10M = 20 and must not.
    @Test
    void testStreamingAnalyzerUsesGroupCountNotPostHavingRows() throws Exception {
        String sql = "select count(*) as c from lineitem group by l_shipmode having count(*) > 315";
        NodeExecStats localAgg = new NodeExecStats(1, 200000000L, 150000000L, 0, 0, 0);
        NodeExecStats globalAgg = new NodeExecStats(3, 200000000L, 7, 9999993L, 0, 0);
        Assertions.assertNull(analyzeAgg(sql, localAgg, globalAgg).getTuningGuides(2));
    }

    // The filter drops 5000000 of the 10000000 groups, HAVING then drops 4999993 of the 5000000 left
    // and 7 survive, so the three counters telescope back to the cardinality. On pull rows alone
    // 200M/7 would fire; on the real 10M groups it reads 20 and must not.
    @Test
    void testStreamingAnalyzerCountsRuntimeFilteredGroups() throws Exception {
        String sql = "select count(*) as c from lineitem group by l_shipmode having count(*) > 315";
        NodeExecStats localAgg = new NodeExecStats(1, 200000000L, 150000000L, 0, 0, 0);
        NodeExecStats globalAgg = new NodeExecStats(3, 200000000L, 7, 4999993L, 0, 5000000L);
        Assertions.assertNull(analyzeAgg(sql, localAgg, globalAgg).getTuningGuides(2));
    }

    // The predicate drops almost nothing, so the guide must still be produced.
    @Test
    void testStreamingAnalyzerStillFiresWhenHavingKeepsMostGroups() throws Exception {
        String sql = "select count(*) as c from lineitem group by l_shipmode having count(*) > 315";
        NodeExecStats localAgg = new NodeExecStats(1, 3000000000L, 2000000000L, 0, 0, 0);
        NodeExecStats globalAgg = new NodeExecStats(3, 500000, 7, 3, 0, 0);
        Assertions.assertTrue(analyzeAgg(sql, localAgg, globalAgg)
                .getTuningGuides(2).get(0) instanceof StreamingAggTuningGuide);
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

    private static final String JOIN_SQL_TEMPLATE = "select * from (select * from customer where c_acctbal > %d) c " +
            "join (select * from supplier where s_acctbal = 1) s on abs(c_custkey) = abs(s_suppkey)";

    // ApplyTuningGuideRule looks the guides up with the plan before it applies them, and the guides are analyzed from
    // a plan that was not tuned. Plan with the advisor off, so that a cached guide does not change the plan first.
    private ExecPlan getUntunedExecPlan(String sql) throws Exception {
        boolean enablePlanAdvisor = connectContext.getSessionVariable().isEnablePlanAdvisor();
        connectContext.getSessionVariable().setEnablePlanAdvisor(false);
        try {
            return getExecPlan(sql);
        } finally {
            connectContext.getSessionVariable().setEnablePlanAdvisor(enablePlanAdvisor);
        }
    }

    private OperatorTuningGuides putJoinTuningGuides(int acctbal) throws Exception {
        String sql = String.format(JOIN_SQL_TEMPLATE, acctbal);
        ExecPlan execPlan = getUntunedExecPlan(sql);
        Map<Integer, NodeExecStats> map = Maps.newHashMap();
        map.put(0, new NodeExecStats(0, 500000, 500000, 0, 0, 0));
        map.put(4, new NodeExecStats(4, 20000000, 20000000, 0, 0, 0));
        Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair =
                new SkeletonBuilder(map).buildSkeleton(execPlan.getPhysicalPlan());
        OperatorTuningGuides tuningGuides = new OperatorTuningGuides(UUID.randomUUID(), 50);
        PlanTuningAnalyzer.getInstance().analyzePlan(execPlan.getPhysicalPlan(), pair.second, tuningGuides);
        Assertions.assertFalse(tuningGuides.isEmpty());
        PlanTuningAdvisor.getInstance().putTuningGuides(sql, pair.first, tuningGuides);
        return tuningGuides;
    }

    private OperatorTuningGuides getJoinTuningGuides(int acctbal) throws Exception {
        String sql = String.format(JOIN_SQL_TEMPLATE, acctbal);
        SkeletonNode root = new SkeletonBuilder().buildSkeleton(getUntunedExecPlan(sql).getPhysicalPlan()).first;
        return PlanTuningAdvisor.getInstance().getTuningGuides(sql, root);
    }

    @Test
    void testTuningGuidesReusedForEnclosedRanges() throws Exception {
        PlanTuningAdvisor advisor = PlanTuningAdvisor.getInstance();
        advisor.clearAllAdvisor();
        try {
            OperatorTuningGuides tuningGuides = putJoinTuningGuides(5000);

            // c_acctbal > 6000 is enclosed by c_acctbal > 5000, c_acctbal > 4000 is not
            Assertions.assertSame(tuningGuides, getJoinTuningGuides(6000));
            Assertions.assertNull(getJoinTuningGuides(4000));

            // ApplyTuningGuideRule applies them to the enclosed query as well
            getExecPlan(String.format(JOIN_SQL_TEMPLATE, 6000));
            Assertions.assertSame(tuningGuides, advisor.getOperatorTuningGuides(connectContext.getQueryId()));

            // the same guides found for c_acctbal > 4000 widen the cached range instead of adding an entry
            putJoinTuningGuides(4000);
            Assertions.assertEquals(1, advisor.getShowResult().size());
            Assertions.assertSame(tuningGuides, getJoinTuningGuides(4500));

            // the blacklisted entry only blocks the ranges it covered
            advisor.deleteTuningGuides(tuningGuides.getOriginalQueryId(), true);
            Assertions.assertNull(getJoinTuningGuides(4500));
            putJoinTuningGuides(4500);
            Assertions.assertNull(getJoinTuningGuides(4500));
            OperatorTuningGuides outsideBlacklist = putJoinTuningGuides(3000);
            Assertions.assertSame(outsideBlacklist, getJoinTuningGuides(3000));
        } finally {
            advisor.clearAllAdvisor();
        }
    }

    @Test
    void testOptimizedQueryRecordRemovedWhenStatementEnds() throws Exception {
        PlanTuningAdvisor advisor = PlanTuningAdvisor.getInstance();
        advisor.clearAllAdvisor();
        try {
            putJoinTuningGuides(5000);
            String sql = String.format(JOIN_SQL_TEMPLATE, 5000);

            // planning with the cached guides adds the record of the query; every test plan gets a new query id
            getExecPlan(sql);
            UUID queryId = connectContext.getQueryId();
            Assertions.assertNotNull(advisor.getOptimizedRecord(queryId));

            // EXPLAIN plans with the guides again but never reports exec stats, so only the end of the statement
            // can remove the record
            long appliedBefore = getMetricValue("plan_advisor_guide_applied_total", PlanAdvisorMetrics.JOIN_OPERATOR_TYPE);
            StatementBase stmt = SqlParser.parse("explain " + sql, connectContext.getSessionVariable()).get(0);
            connectContext.getState().reset();
            new StmtExecutor(connectContext, stmt).execute();
            Assertions.assertFalse(connectContext.getState().isError(), connectContext.getState().getErrorMessage());
            Assertions.assertEquals(appliedBefore + 1,
                    getMetricValue("plan_advisor_guide_applied_total", PlanAdvisorMetrics.JOIN_OPERATOR_TYPE));
            Assertions.assertEquals(queryId, connectContext.getQueryId());
            Assertions.assertNull(advisor.getOptimizedRecord(queryId));
        } finally {
            advisor.clearAllAdvisor();
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