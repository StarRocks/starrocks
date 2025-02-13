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

import com.starrocks.planner.PlanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SlotEstimatorTest extends SchedulerTestBase {
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
}
