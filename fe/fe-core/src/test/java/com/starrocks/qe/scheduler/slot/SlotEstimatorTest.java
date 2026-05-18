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

package com.starrocks.qe.scheduler.slot;

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.planner.PlanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SlotEstimatorTest extends SchedulerTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        SchedulerTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME);
    }

    @Test
    public void testDefaultSlotEstimator() {
        SlotEstimatorFactory.DefaultSlotEstimator estimator = new SlotEstimatorFactory.DefaultSlotEstimator();
        assertThat(estimator.estimateSlots(null, null, null)).isOne();
    }

    @Test
    public void testMemoryBasedSlotsEstimator() throws Exception {
        final int numWorkers = 3;
        final long memLimitBytesPerWorker = 64L * 1024 * 1024 * 1024;
        QueryQueueOptions opts =
                new QueryQueueOptions(true, new QueryQueueOptions.V2(4, numWorkers, 16, memLimitBytesPerWorker, 4096, 1));
        SlotEstimatorFactory.MemoryBasedSlotsEstimator estimator = new SlotEstimatorFactory.MemoryBasedSlotsEstimator();

        DefaultCoordinator coordinator = getScheduler("SELECT * FROM lineitem");

        connectContext.getAuditEventBuilder().setPlanMemCosts(-1L);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(3);

        connectContext.getAuditEventBuilder().setPlanMemCosts(memLimitBytesPerWorker * numWorkers);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(opts.v2().getTotalSlots());

        connectContext.getAuditEventBuilder().setPlanMemCosts(memLimitBytesPerWorker * numWorkers - 10);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(opts.v2().getTotalSlots());

        connectContext.getAuditEventBuilder().setPlanMemCosts(memLimitBytesPerWorker * numWorkers + 10);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(opts.v2().getTotalSlots());

        connectContext.getAuditEventBuilder().setPlanMemCosts(memLimitBytesPerWorker * numWorkers * 100);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(opts.v2().getTotalSlots());
    }

    @Test
    public void testParallelismBasedSlotsEstimator() throws Exception {
        SlotEstimatorFactory.ParallelismBasedSlotsEstimator estimator = new SlotEstimatorFactory.ParallelismBasedSlotsEstimator();
        final int numWorkers = 3;
        final int numCoresPerWorker = 16;
        final int numRowsPerWorker = 4096;
        final int dop = numCoresPerWorker / 2;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, numCoresPerWorker, 64L * 1024 * 1024 * 1024, numRowsPerWorker, 100));

        {
            DefaultCoordinator coordinator = getScheduler("SELECT * FROM lineitem");
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(20 / 3 * 3);
        }

        String sql = "SELECT /*+SET_VAR(pipeline_dop=8)*/ " +
                "count(1) FROM lineitem t1 join [shuffle] lineitem t2 on t1.l_orderkey = t2.l_orderkey";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: L_ORDERKEY = 18: L_ORDERKEY\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:EXCHANGE");
        assertContains(plan, "  RESULT SINK\n" +
                "\n" +
                "  8:AGGREGATE (merge finalize)\n" +
                "  |  output: count(35: count)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  7:EXCHANGE");
        {
            DefaultCoordinator coordinator = getScheduler(sql);
            setNodeCardinality(coordinator, 1, 10);
            setNodeCardinality(coordinator, 3, 10);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(20 / 3 * 3);
        }
        {
            DefaultCoordinator coordinator = getScheduler(sql);
            setNodeCardinality(coordinator, 1, numWorkers * numRowsPerWorker * dop);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(dop * numWorkers);
        }
        {
            DefaultCoordinator coordinator = getScheduler(sql);
            setNodeCardinality(coordinator, 1, numWorkers * numRowsPerWorker * dop * 10);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(dop * numWorkers);
        }
        {
            DefaultCoordinator coordinator = getScheduler(sql);
            setNodeCardinality(coordinator, 1, numWorkers * numRowsPerWorker * 7);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(7 * numWorkers);
        }
        {
            DefaultCoordinator coordinator = getScheduler("SELECT /*+SET_VAR(pipeline_dop=100)*/ " +
                    "count(1) FROM lineitem t1 join [shuffle] lineitem t2 on t1.l_orderkey = t2.l_orderkey");
            setNodeCardinality(coordinator, 1, numWorkers * numRowsPerWorker * 100);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(opts.v2().getTotalSlots());
        }

        {
            DefaultCoordinator coordinator = getScheduler("SELECT /*+SET_VAR(pipeline_dop=19)*/ " +
                    "count(1) FROM lineitem t1 join [shuffle] lineitem t2 on t1.l_orderkey = t2.l_orderkey");
            // The exchange source node of the sink node only on one BE.
            setNodeCardinality(coordinator, 7, numRowsPerWorker * 19);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(19);
        }

        {
            DefaultCoordinator coordinator = getScheduler(sql);
            setNodeCardinality(coordinator, 1, numWorkers * numRowsPerWorker * dop * 10);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100. * numWorkers * dop * 1 / 4);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(dop / 2 * numWorkers);
        }
        {
            DefaultCoordinator coordinator = getScheduler(sql);
            setNodeCardinality(coordinator, 1, numWorkers * numRowsPerWorker * dop * 10);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100. * numWorkers * dop * 3 / 4);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(dop * 3 / 4 * numWorkers);
        }
        {
            DefaultCoordinator coordinator = getScheduler(sql);
            setNodeCardinality(coordinator, 1, numWorkers * numRowsPerWorker * dop * 10);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100. * numWorkers * dop * 5 / 4);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(dop * numWorkers);
        }
    }

    @Test
    public void testParallelismBasedSlotsEstimatorForConnectorScan() throws Exception {
        SlotEstimatorFactory.ParallelismBasedSlotsEstimator estimator = new SlotEstimatorFactory.ParallelismBasedSlotsEstimator();
        final int numWorkers = 3;
        final int numCoresPerWorker = 16;
        final int numRowsPerWorker = 4096;
        final int dop = numCoresPerWorker / 2;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, numCoresPerWorker, 64L * 1024 * 1024 * 1024, numRowsPerWorker, 100));

        String sql = "SELECT /*+SET_VAR(pipeline_dop=8)*/ " +
                "count(1) FROM hive0.tpch.lineitem t1 join [shuffle] hive0.tpch.lineitem t2 on t1.l_orderkey = t2.l_orderkey";

        {
            DefaultCoordinator coordinator = getScheduler(sql);
            setNodeCardinality(coordinator, 1, numWorkers * numRowsPerWorker * dop);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(dop * numWorkers);
        }
        {
            DefaultCoordinator coordinator = getScheduler(sql);
            setNodeCardinality(coordinator, 1, numWorkers * numRowsPerWorker * dop * 10);
            connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
            assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(dop * numWorkers);
        }
    }

    @Test
    public void testMaxSlotsEstimator() {
        SlotEstimator estimator1 = (opts, context, coord) -> 1;
        SlotEstimator estimator2 = (opts, context, coord) -> 2;
        SlotEstimator estimator3 = (opts, context, coord) -> 3;

        {
            SlotEstimatorFactory.MaxSlotsEstimator estimator = new SlotEstimatorFactory.MaxSlotsEstimator(estimator1, estimator2);
            assertThat(estimator.estimateSlots(null, null, null)).isEqualTo(2);
        }

        {
            SlotEstimatorFactory.MaxSlotsEstimator estimator = new SlotEstimatorFactory.MaxSlotsEstimator(estimator1, estimator3);
            assertThat(estimator.estimateSlots(null, null, null)).isEqualTo(3);
        }

        {
            SlotEstimatorFactory.MaxSlotsEstimator estimator = new SlotEstimatorFactory.MaxSlotsEstimator(estimator2, estimator3);
            assertThat(estimator.estimateSlots(null, null, null)).isEqualTo(3);
        }
    }

    @Test
    public void testCreate() {
        // Reset to default policy to ensure test isolation
        Config.query_queue_slots_estimator_strategy = SlotEstimatorFactory.EstimatorPolicy.createDefault().name();

        QueryQueueOptions opts = new QueryQueueOptions(false, new QueryQueueOptions.V2());
        assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.DefaultSlotEstimator.class);

        opts = new QueryQueueOptions(true, new QueryQueueOptions.V2(1, 1, 1, 1, 1, 1));
        assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.MaxSlotsEstimator.class);
    }

    private static void setNodeCardinality(DefaultCoordinator coordinator, int nodeId, long cardinality) {
        PlanNode node = findPlanNodeById(coordinator.getExecutionDAG().getRootFragment().getPlanFragment().getPlanRoot(), nodeId);
        assertThat(node).isNotNull();

        Statistics statistics = Statistics.builder().setOutputRowCount(cardinality).build();
        node.computeStatistics(statistics);
    }

    private static PlanNode findPlanNodeById(PlanNode root, int nodeId) {
        if (root.getId().asInt() == nodeId) {
            return root;
        }

        for (PlanNode child : root.getChildren()) {
            PlanNode res = findPlanNodeById(child, nodeId);
            if (res != null) {
                return res;
            }
        }

        return null;
    }

    @Test
    public void testCreateWithPolicy() {
        {
            Config.query_queue_slots_estimator_strategy = "MBE";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.MemoryBasedSlotsEstimator.class);
        }
        {
            Config.query_queue_slots_estimator_strategy = "PBE";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.ParallelismBasedSlotsEstimator.class);
        }
        {
            Config.query_queue_slots_estimator_strategy = "MAX";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.MaxSlotsEstimator.class);
        }
        {
            Config.query_queue_slots_estimator_strategy = "MIN";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.MinSlotsEstimator.class);
        }
        {
            Config.query_queue_slots_estimator_strategy = "BAD_SET";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.MaxSlotsEstimator.class);
        }
    }

    @Test
    public void testEstimateSlotsForExplainWithV1() {
        // V1 should always return 1
        QueryQueueOptions opts = new QueryQueueOptions(false, new QueryQueueOptions.V2());
        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, null);
        assertThat(slots).isEqualTo(1);
    }

    @Test
    public void testEstimateSlotsForExplainWithMemoryBasedPolicy() throws Exception {
        Config.query_queue_slots_estimator_strategy = "MBE";
        final int numWorkers = 3;
        final long memLimitBytesPerWorker = 64L * 1024 * 1024 * 1024;
        QueryQueueOptions opts =
                new QueryQueueOptions(true, new QueryQueueOptions.V2(4, numWorkers, 16, memLimitBytesPerWorker, 4096, 1));

        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, "SELECT * FROM lineitem");
        ExecPlan execPlan = planPair.second;

        connectContext.getAuditEventBuilder().setPlanMemCosts(-1L);
        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(3);

        connectContext.getAuditEventBuilder().setPlanMemCosts(memLimitBytesPerWorker * numWorkers);
        slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(opts.v2().getTotalSlots());
    }

    @Test
    public void testEstimateSlotsForExplainWithParallelBasedPolicy() throws Exception {
        Config.query_queue_slots_estimator_strategy = "PBE";
        final int numWorkers = 3;
        final int numCoresPerWorker = 16;
        final int numRowsPerWorker = 4096;
        final int dop = numCoresPerWorker / 2;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, numCoresPerWorker, 64L * 1024 * 1024 * 1024, numRowsPerWorker, 100));

        String sql = "SELECT /*+SET_VAR(pipeline_dop=8)*/ " +
                "count(1) FROM lineitem t1 join [shuffle] lineitem t2 on t1.l_orderkey = t2.l_orderkey";
        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        ExecPlan execPlan = planPair.second;

        // Set cardinality on scan nodes in the exec plan
        setNodeCardinalityInExecPlan(execPlan, 1, numWorkers * numRowsPerWorker * dop);

        // Set planCpuCosts high enough to not be clamped by the CPU-cost clamp
        connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);

        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(dop * numWorkers);
    }

    @Test
    public void testEstimateSlotsForExplainWithMaxPolicy() throws Exception {
        Config.query_queue_slots_estimator_strategy = "MAX";
        final int numWorkers = 3;
        final long memLimitBytesPerWorker = 64L * 1024 * 1024 * 1024;
        QueryQueueOptions opts =
                new QueryQueueOptions(true, new QueryQueueOptions.V2(4, numWorkers, 16, memLimitBytesPerWorker, 4096, 1));

        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, "SELECT * FROM lineitem");
        ExecPlan execPlan = planPair.second;

        // Memory-based will return 3, parallelism-based will return different value
        // MAX should return the larger of the two
        connectContext.getAuditEventBuilder().setPlanMemCosts(-1L);
        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testEstimateSlotsForExplainWithMinPolicy() throws Exception {
        Config.query_queue_slots_estimator_strategy = "MIN";
        final int numWorkers = 3;
        final long memLimitBytesPerWorker = 64L * 1024 * 1024 * 1024;
        QueryQueueOptions opts =
                new QueryQueueOptions(true, new QueryQueueOptions.V2(4, numWorkers, 16, memLimitBytesPerWorker, 4096, 1));

        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, "SELECT * FROM lineitem");
        ExecPlan execPlan = planPair.second;

        // MIN should return the smaller of memory-based and parallelism-based
        connectContext.getAuditEventBuilder().setPlanMemCosts(-1L);
        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testEstimateSlotsForExplainWithEmptyFragments() {
        // Test with empty fragments
        Config.query_queue_slots_estimator_strategy = "PBE";
        final int numWorkers = 3;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, 16, 64L * 1024 * 1024 * 1024, 4096, 100));

        // Create an exec plan with no fragments
        ExecPlan emptyExecPlan = new ExecPlan();
        
        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, emptyExecPlan);
        assertThat(slots).isEqualTo(1);
    }

    private static void setNodeCardinalityInExecPlan(ExecPlan execPlan, int nodeId, long cardinality) {
        if (execPlan.getFragments() == null || execPlan.getFragments().isEmpty()) {
            return;
        }
        PlanNode rootNode = execPlan.getFragments().get(0).getPlanRoot();
        PlanNode node = findPlanNodeById(rootNode, nodeId);
        if (node != null) {
            Statistics statistics = Statistics.builder().setOutputRowCount(cardinality).build();
            node.computeStatistics(statistics);
        }
    }

    @Test
    public void testParallelismBasedReturnsRawAboveTotalSlots() throws Exception {
        // totalSlots = numCoresPerWorker * concurrencyLevel * numWorkers = 8 * 1 * 1 = 8.
        // numRowsPerSlot = 1 so per-source-node raw count is dominated by cardinality.
        // pipeline_dop=100 lets per-fragment raw (before clamp) reach 100 > totalSlots.
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(1, 1, 8, 1L << 30, 1, 1));
        assertThat(opts.v2().getTotalSlots()).isEqualTo(8);

        DefaultCoordinator coord = getScheduler("SELECT /*+SET_VAR(pipeline_dop=100)*/ " +
                "count(1) FROM lineitem t1 join [shuffle] lineitem t2 on t1.l_orderkey = t2.l_orderkey");
        // Push cardinality way above totalSlots so raw >> 8.
        setNodeCardinality(coord, 1, 1_000_000);
        connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);

        SlotEstimator.SlotEstimate est = new SlotEstimatorFactory.ParallelismBasedSlotsEstimator()
                .estimateBoth(opts, connectContext, coord);
        assertThat(est.rawSlots())
                .as("raw should exceed totalSlots for big query")
                .isGreaterThan(8);
        assertThat(est.clampedSlots())
                .as("clamped should equal totalSlots")
                .isEqualTo(8);
    }

    @Test
    public void testMemoryBasedReturnsRawAboveTotalSlots() throws Exception {
        final int numWorkers = 3;
        final long memLimitBytesPerWorker = 64L * 1024 * 1024 * 1024;
        QueryQueueOptions opts =
                new QueryQueueOptions(true, new QueryQueueOptions.V2(4, numWorkers, 16, memLimitBytesPerWorker, 4096, 1));

        DefaultCoordinator coord = getScheduler("SELECT * FROM lineitem");
        // Mem cost far exceeds capacity: raw should exceed totalSlots, clamped == totalSlots.
        connectContext.getAuditEventBuilder().setPlanMemCosts(memLimitBytesPerWorker * numWorkers * 100);

        SlotEstimator.SlotEstimate est = new SlotEstimatorFactory.MemoryBasedSlotsEstimator()
                .estimateBoth(opts, connectContext, coord);
        assertThat(est.rawSlots()).isGreaterThan(opts.v2().getTotalSlots());
        assertThat(est.clampedSlots()).isEqualTo(opts.v2().getTotalSlots());
    }

    @Test
    public void testDefaultEstimateBothReturnsEqualValues() {
        SlotEstimator estimator = (opts, context, coord) -> 5;
        SlotEstimator.SlotEstimate est = estimator.estimateBoth(null, null, null);
        assertThat(est.rawSlots()).isEqualTo(5);
        assertThat(est.clampedSlots()).isEqualTo(5);
    }

    @Test
    public void testSlotEstimateRejectsRawLessThanClamped() {
        try {
            new SlotEstimator.SlotEstimate(1, 5);
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testMaxEstimatorEstimateBothAggregatesComponentwise() {
        SlotEstimator a = new SlotEstimator() {
            @Override
            public int estimateSlots(QueryQueueOptions opts, com.starrocks.qe.ConnectContext ctx, DefaultCoordinator coord) {
                return 3;
            }

            @Override
            public SlotEstimate estimateBoth(QueryQueueOptions opts, com.starrocks.qe.ConnectContext ctx,
                                             DefaultCoordinator coord) {
                return new SlotEstimate(100, 3);
            }
        };
        SlotEstimator b = new SlotEstimator() {
            @Override
            public int estimateSlots(QueryQueueOptions opts, com.starrocks.qe.ConnectContext ctx, DefaultCoordinator coord) {
                return 7;
            }

            @Override
            public SlotEstimate estimateBoth(QueryQueueOptions opts, com.starrocks.qe.ConnectContext ctx,
                                             DefaultCoordinator coord) {
                return new SlotEstimate(10, 7);
            }
        };

        SlotEstimatorFactory.MaxSlotsEstimator estimator = new SlotEstimatorFactory.MaxSlotsEstimator(a, b);
        SlotEstimator.SlotEstimate est = estimator.estimateBoth(null, null, null);
        // max(100, 10) = 100 for raw; max(3, 7) = 7 for clamped.
        assertThat(est.rawSlots()).isEqualTo(100);
        assertThat(est.clampedSlots()).isEqualTo(7);
        // estimateSlots must agree with clampedSlots for backwards compat.
        assertThat(estimator.estimateSlots(null, null, null)).isEqualTo(7);
    }

    @Test
    public void testMinEstimatorEstimateBothAggregatesComponentwise() {
        SlotEstimator a = new SlotEstimator() {
            @Override
            public int estimateSlots(QueryQueueOptions opts, com.starrocks.qe.ConnectContext ctx, DefaultCoordinator coord) {
                return 3;
            }

            @Override
            public SlotEstimate estimateBoth(QueryQueueOptions opts, com.starrocks.qe.ConnectContext ctx,
                                             DefaultCoordinator coord) {
                return new SlotEstimate(100, 3);
            }
        };
        SlotEstimator b = new SlotEstimator() {
            @Override
            public int estimateSlots(QueryQueueOptions opts, com.starrocks.qe.ConnectContext ctx, DefaultCoordinator coord) {
                return 7;
            }

            @Override
            public SlotEstimate estimateBoth(QueryQueueOptions opts, com.starrocks.qe.ConnectContext ctx,
                                             DefaultCoordinator coord) {
                return new SlotEstimate(10, 7);
            }
        };

        SlotEstimatorFactory.MinSlotsEstimator estimator = new SlotEstimatorFactory.MinSlotsEstimator(a, b);
        SlotEstimator.SlotEstimate est = estimator.estimateBoth(null, null, null);
        // min(100, 10) = 10 for raw; min(3, 7) = 3 for clamped. Invariant 10 >= 3 holds.
        assertThat(est.rawSlots()).isEqualTo(10);
        assertThat(est.clampedSlots()).isEqualTo(3);
        assertThat(estimator.estimateSlots(null, null, null)).isEqualTo(3);
    }

    @Test
    public void testMinEstimatorEstimateBothEmptyDefaultsToOne() {
        SlotEstimatorFactory.MinSlotsEstimator estimator = new SlotEstimatorFactory.MinSlotsEstimator();
        SlotEstimator.SlotEstimate est = estimator.estimateBoth(null, null, null);
        assertThat(est.rawSlots()).isOne();
        assertThat(est.clampedSlots()).isOne();
    }

    @Test
    public void testMaxEstimatorReturnsRawAboveTotalSlots() throws Exception {
        // Default policy is MAX, wrapping MemoryBasedSlotsEstimator + ParallelismBasedSlotsEstimator.
        // The parallelism-based child should produce raw > totalSlots; the wrapper must surface that
        // raw value (rather than the trivially-equal default fallback).
        Config.query_queue_slots_estimator_strategy = SlotEstimatorFactory.EstimatorPolicy.MAX.name();
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(1, 1, 8, 1L << 30, 1, 1));
        assertThat(opts.v2().getTotalSlots()).isEqualTo(8);

        SlotEstimator estimator = SlotEstimatorFactory.create(opts);
        assertThat(estimator).isInstanceOf(SlotEstimatorFactory.MaxSlotsEstimator.class);

        DefaultCoordinator coord = getScheduler("SELECT /*+SET_VAR(pipeline_dop=100)*/ " +
                "count(1) FROM lineitem t1 join [shuffle] lineitem t2 on t1.l_orderkey = t2.l_orderkey");
        setNodeCardinality(coord, 1, 1_000_000);
        connectContext.getAuditEventBuilder().setPlanCpuCosts(100 * 10000);
        connectContext.getAuditEventBuilder().setPlanMemCosts(-1L);

        SlotEstimator.SlotEstimate est = estimator.estimateBoth(opts, connectContext, coord);
        assertThat(est.rawSlots())
                .as("wrapper must propagate child raw beyond totalSlots")
                .isGreaterThan(est.clampedSlots());
        assertThat(est.clampedSlots())
                .as("clamped is bounded by totalSlots")
                .isLessThanOrEqualTo(opts.v2().getTotalSlots());
    }
}
