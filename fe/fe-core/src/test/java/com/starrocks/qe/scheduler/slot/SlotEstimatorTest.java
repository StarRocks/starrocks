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
import com.starrocks.common.ConfigBase;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.Pair;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.planner.PlanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SlotEstimatorTest extends SchedulerTestBase {
    private String oldQueryQueueSlotsEstimatorStrategy;
    private int oldQueryQueueV2ConcurrencyLevel;
    private long oldQueryQueueV2MemBytesPerSlot;

    @BeforeAll
    public static void beforeClass() throws Exception {
        SchedulerTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME);
    }

    @BeforeEach
    public void setUp() {
        oldQueryQueueSlotsEstimatorStrategy = Config.query_queue_slots_estimator_strategy;
        oldQueryQueueV2ConcurrencyLevel = Config.query_queue_v2_concurrency_level;
        oldQueryQueueV2MemBytesPerSlot = Config.query_queue_v2_mem_bytes_per_slot;
        Config.query_queue_slots_estimator_strategy = SlotEstimatorFactory.EstimatorPolicy.createDefault().name();
        Config.query_queue_v2_concurrency_level = 4;
        Config.query_queue_v2_mem_bytes_per_slot = 0;
    }

    @AfterEach
    public void tearDown() {
        Config.query_queue_slots_estimator_strategy = oldQueryQueueSlotsEstimatorStrategy;
        Config.query_queue_v2_concurrency_level = oldQueryQueueV2ConcurrencyLevel;
        Config.query_queue_v2_mem_bytes_per_slot = oldQueryQueueV2MemBytesPerSlot;
        BackendResourceStat.getInstance().reset();
    }

    @Test
    public void testDefaultSlotEstimator() {
        SlotEstimatorFactory.DefaultSlotEstimator estimator = new SlotEstimatorFactory.DefaultSlotEstimator();
        assertThat(estimator.estimateSlots(null, null, null)).isOne();
    }

    @Test
    public void testMemoryBasedSlotsEstimator() throws Exception {
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(16, 3, 16, 64L * 1024 * 1024 * 1024,
                        1024, 4096, 100));
        SlotEstimatorFactory.MemoryBasedSlotsEstimator estimator = new SlotEstimatorFactory.MemoryBasedSlotsEstimator();

        DefaultCoordinator coordinator = getScheduler("SELECT /*+SET_VAR(pipeline_dop=8)*/ * FROM lineitem");

        connectContext.getAuditEventBuilder().setPlanMemCosts(-1L);
        connectContext.getAuditEventBuilder().setPlanCpuCosts(1000000L);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(1);

        connectContext.getAuditEventBuilder().setPlanMemCosts(1024L);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(1);

        connectContext.getAuditEventBuilder().setPlanMemCosts(1024.5);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(2);

        connectContext.getAuditEventBuilder().setPlanMemCosts(1025L);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(2);

        connectContext.getAuditEventBuilder().setPlanMemCosts(1024L * 1000);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(12);
    }

    @Test
    public void testMemoryBasedSlotsEstimatorClampsTotalPlanMemCostsByMemoryBudget() throws Exception {
        final int numCores = 16;
        final long memLimitBytes = 64L * 1024 * 1024 * 1024;
        final long memBytesPerSlot = 32L * 1024 * 1024 * 1024;

        BackendResourceStat.getInstance().setNumCoresOfBe(WarehouseManager.DEFAULT_WAREHOUSE_ID, 1, numCores);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WarehouseManager.DEFAULT_WAREHOUSE_ID, 1, memLimitBytes);
        Config.query_queue_slots_estimator_strategy = "MBE";
        Config.query_queue_v2_concurrency_level = numCores;
        Config.query_queue_v2_mem_bytes_per_slot = memBytesPerSlot;

        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotEstimatorFactory.MemoryBasedSlotsEstimator estimator = new SlotEstimatorFactory.MemoryBasedSlotsEstimator();
        DefaultCoordinator coordinator = getScheduler("SELECT /*+SET_VAR(pipeline_dop=16)*/ * FROM lineitem");

        connectContext.getAuditEventBuilder().setPlanMemCosts(100 * (double) memBytesPerSlot);

        assertThat(opts.v2().getTotalSlots()).isEqualTo(8);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(8);
    }

    @Test
    public void testCpuCostBasedSlotsEstimator() throws Exception {
        SlotEstimatorFactory.CpuCostBasedSlotsEstimator estimator =
                new SlotEstimatorFactory.CpuCostBasedSlotsEstimator();
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(16, 3, 16, 64L * 1024 * 1024 * 1024,
                        0, 4096, 100));

        DefaultCoordinator coordinator = getScheduler("SELECT /*+SET_VAR(pipeline_dop=8)*/ * FROM lineitem");

        connectContext.getAuditEventBuilder().setPlanCpuCosts(-1L);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(1);

        connectContext.getAuditEventBuilder().setPlanCpuCosts(100L);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(1);

        connectContext.getAuditEventBuilder().setPlanCpuCosts(100.5);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(2);

        connectContext.getAuditEventBuilder().setPlanCpuCosts(101L);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(2);

        connectContext.getAuditEventBuilder().setPlanCpuCosts(100L * 1000);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(12);
    }

    @Test
    public void testCpuCostBasedSlotsEstimatorClampsByPipelineDopPerWorker() throws Exception {
        SlotEstimatorFactory.CpuCostBasedSlotsEstimator estimator =
                new SlotEstimatorFactory.CpuCostBasedSlotsEstimator();
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(16, 3, 16, 64L * 1024 * 1024 * 1024,
                        0, 4096, 100));

        DefaultCoordinator coordinator = getScheduler("SELECT /*+SET_VAR(pipeline_dop=8)*/ " +
                "count(1) FROM lineitem t1 join [shuffle] lineitem t2 on t1.l_orderkey = t2.l_orderkey");
        setNodeCardinality(coordinator, 1, 3 * 4096 * 100);
        connectContext.getAuditEventBuilder().setPlanCpuCosts(250L);

        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(3);

        connectContext.getAuditEventBuilder().setPlanCpuCosts(100L * 1000);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(12);
    }

    @Test
    public void testParallelismBasedSlotsEstimatorForSingleOlapScanRange() throws Exception {
        SlotEstimatorFactory.ParallelismBasedSlotsEstimator estimator =
                new SlotEstimatorFactory.ParallelismBasedSlotsEstimator();
        final int numWorkers = 3;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, 16, 64L * 1024 * 1024 * 1024, 4096, 100));

        DefaultCoordinator coordinator = getScheduler("SELECT * FROM nation");

        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(1);
    }

    @Test
    public void testParallelismBasedSlotsEstimatorForOlapScanCappedByWorkers() throws Exception {
        SlotEstimatorFactory.ParallelismBasedSlotsEstimator estimator =
                new SlotEstimatorFactory.ParallelismBasedSlotsEstimator();
        final int numWorkers = 3;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, 16, 64L * 1024 * 1024 * 1024, 4096, 100));

        DefaultCoordinator coordinator = getScheduler("SELECT * FROM lineitem");

        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(numWorkers);
    }

    @Test
    public void testParallelismBasedSlotsEstimatorForConnectorScan() throws Exception {
        SlotEstimatorFactory.ParallelismBasedSlotsEstimator estimator =
                new SlotEstimatorFactory.ParallelismBasedSlotsEstimator();
        final int numWorkers = 3;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, 16, 64L * 1024 * 1024 * 1024, 4096, 100));

        DefaultCoordinator coordinator = getScheduler("SELECT * FROM hive0.tpch.lineitem");

        // PBE aims for parallelism == number_of_workers; a connector/external scan's split count is not
        // available at estimation time, so it is treated as a full-parallelism scan (number_of_workers work
        // units) instead of collapsing to a single slot. The estimate does not depend on cardinality or on
        // the deprecated query_queue_v2_num_rows_per_slot.
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(numWorkers);

        // Cardinality changes must not change the connector estimate.
        setNodeCardinality(coordinator, 0, 1L);
        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(numWorkers);
    }

    @Test
    public void testParallelismBasedSlotsEstimatorForNoScanQuery() throws Exception {
        SlotEstimatorFactory.ParallelismBasedSlotsEstimator estimator =
                new SlotEstimatorFactory.ParallelismBasedSlotsEstimator();
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, 3, 16, 64L * 1024 * 1024 * 1024, 4096, 100));

        DefaultCoordinator coordinator = getScheduler("SELECT 1");

        assertThat(estimator.estimateSlots(opts, connectContext, coordinator)).isEqualTo(1);
    }

    @Test
    public void testEstimatorPolicyConfigValidationAcceptsLegacyMaxMin() throws Exception {
        Field field = Config.class.getField("query_queue_slots_estimator_strategy");

        ConfigBase.setConfigField(field, "PBE");
        assertThat(Config.query_queue_slots_estimator_strategy).isEqualTo("PBE");
        assertThat(SlotEstimatorFactory.getEstimatorPolicy()).isEqualTo(SlotEstimatorFactory.EstimatorPolicy.PBE);

        ConfigBase.setConfigField(field, "MBE");
        assertThat(Config.query_queue_slots_estimator_strategy).isEqualTo("MBE");
        assertThat(SlotEstimatorFactory.getEstimatorPolicy()).isEqualTo(SlotEstimatorFactory.EstimatorPolicy.MBE);

        ConfigBase.setConfigField(field, "CBE");
        assertThat(Config.query_queue_slots_estimator_strategy).isEqualTo("CBE");
        assertThat(SlotEstimatorFactory.getEstimatorPolicy()).isEqualTo(SlotEstimatorFactory.EstimatorPolicy.CBE);

        ConfigBase.setConfigField(field, "MAX");
        assertThat(Config.query_queue_slots_estimator_strategy).isEqualTo("MAX");
        assertThat(SlotEstimatorFactory.getEstimatorPolicy())
                .isEqualTo(SlotEstimatorFactory.EstimatorPolicy.createDefault());

        ConfigBase.setConfigField(field, "MIN");
        assertThat(Config.query_queue_slots_estimator_strategy).isEqualTo("MIN");
        assertThat(SlotEstimatorFactory.getEstimatorPolicy())
                .isEqualTo(SlotEstimatorFactory.EstimatorPolicy.createDefault());

        assertThatThrownBy(() -> ConfigBase.setConfigField(field, "BAD_SET"))
                .isInstanceOf(InvalidConfException.class)
                .hasMessageContaining("query_queue_slots_estimator_strategy");
        assertThatThrownBy(() -> ConfigBase.setConfigField(field, "FBE"))
                .isInstanceOf(InvalidConfException.class)
                .hasMessageContaining("query_queue_slots_estimator_strategy");
    }

    @Test
    public void testPersistedEstimatorPolicyValidationAcceptsLegacyMaxMin(@TempDir Path tempDir)
            throws Exception {
        Field configPathField = ConfigBase.class.getDeclaredField("configPath");
        Field isPersistedField = ConfigBase.class.getDeclaredField("isPersisted");
        configPathField.setAccessible(true);
        isPersistedField.setAccessible(true);
        String oldConfigPath = (String) configPathField.get(null);
        boolean oldIsPersisted = isPersistedField.getBoolean(null);

        String initialConfig = "query_queue_slots_estimator_strategy = PBE\n";
        Path configPath = tempDir.resolve("fe.conf");
        Files.writeString(configPath, initialConfig);

        try {
            configPathField.set(null, configPath.toString());
            isPersistedField.setBoolean(null, true);

            ConfigBase.setMutableConfig("query_queue_slots_estimator_strategy", "MAX", true, "root");
            assertThat(Config.query_queue_slots_estimator_strategy).isEqualTo("MAX");
            assertThat(SlotEstimatorFactory.getEstimatorPolicy())
                    .isEqualTo(SlotEstimatorFactory.EstimatorPolicy.createDefault());

            assertThat(Files.readString(configPath)).contains("query_queue_slots_estimator_strategy = MAX");
        } finally {
            configPathField.set(null, oldConfigPath);
            isPersistedField.setBoolean(null, oldIsPersisted);
        }
    }

    @Test
    public void testCreate() {
        // Reset to default policy to ensure test isolation
        Config.query_queue_slots_estimator_strategy = SlotEstimatorFactory.EstimatorPolicy.createDefault().name();

        QueryQueueOptions opts = new QueryQueueOptions(false, new QueryQueueOptions.V2());
        assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.DefaultSlotEstimator.class);

        opts = new QueryQueueOptions(true, new QueryQueueOptions.V2(1, 1, 1, 1, 1, 1));
        assertThat(SlotEstimatorFactory.create(opts))
                .isInstanceOf(SlotEstimatorFactory.ParallelismBasedSlotsEstimator.class);
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
            Config.query_queue_slots_estimator_strategy = "PBE";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts))
                    .isInstanceOf(SlotEstimatorFactory.ParallelismBasedSlotsEstimator.class);
        }
        {
            Config.query_queue_slots_estimator_strategy = "MBE";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.MemoryBasedSlotsEstimator.class);
        }
        {
            Config.query_queue_slots_estimator_strategy = "CBE";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts))
                    .isInstanceOf(SlotEstimatorFactory.CpuCostBasedSlotsEstimator.class);
        }
        {
            Config.query_queue_slots_estimator_strategy = "MAX";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts))
                    .isInstanceOf(SlotEstimatorFactory.ParallelismBasedSlotsEstimator.class);
        }
        {
            Config.query_queue_slots_estimator_strategy = "MIN";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts))
                    .isInstanceOf(SlotEstimatorFactory.ParallelismBasedSlotsEstimator.class);
        }
        {
            Config.query_queue_slots_estimator_strategy = "BAD_SET";
            QueryQueueOptions opts = new QueryQueueOptions(true, new QueryQueueOptions.V2());
            assertThat(SlotEstimatorFactory.create(opts))
                    .isInstanceOf(SlotEstimatorFactory.ParallelismBasedSlotsEstimator.class);
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
    public void testEstimateSlotsForExplainWithParallelismBasedPolicy() throws Exception {
        Config.query_queue_slots_estimator_strategy = "PBE";
        final int numWorkers = 3;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, 16, 64L * 1024 * 1024 * 1024, 4096, 100));

        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, "SELECT * FROM lineitem");
        ExecPlan execPlan = planPair.second;

        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(numWorkers);
    }

    @Test
    public void testEstimateSlotsForExplainUsesParallelismBasedPolicyByDefault() throws Exception {
        final int numWorkers = 3;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, 16, 64L * 1024 * 1024 * 1024, 4096, 100));

        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, "SELECT * FROM lineitem");
        ExecPlan execPlan = planPair.second;

        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(numWorkers);
    }

    @Test
    public void testEstimateSlotsForExplainWithInvalidPolicyFallsBackToParallelismBasedPolicy() throws Exception {
        Config.query_queue_slots_estimator_strategy = "BAD_SET";
        final int numWorkers = 3;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, 16, 64L * 1024 * 1024 * 1024, 4096, 100));

        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, "SELECT * FROM lineitem");
        ExecPlan execPlan = planPair.second;

        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(numWorkers);
    }

    @Test
    public void testEstimateSlotsForExplainWithMemoryBasedPolicy() throws Exception {
        Config.query_queue_slots_estimator_strategy = "MBE";
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(16, 3, 16, 64L * 1024 * 1024 * 1024, 1024, 4096, 100));

        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, "SELECT * FROM lineitem");
        ExecPlan execPlan = planPair.second;

        connectContext.getAuditEventBuilder().setPlanMemCosts(-1L);
        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(1);

        connectContext.getAuditEventBuilder().setPlanMemCosts(1025L);
        slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(2);
    }

    @Test
    public void testEstimateSlotsForExplainWithCpuCostBasedPolicy() throws Exception {
        Config.query_queue_slots_estimator_strategy = "CBE";
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(16, 3, 16, 64L * 1024 * 1024 * 1024, 0, 4096, 100));

        Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, "SELECT * FROM lineitem");
        ExecPlan execPlan = planPair.second;

        connectContext.getAuditEventBuilder().setPlanCpuCosts(-1L);
        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(1);

        connectContext.getAuditEventBuilder().setPlanCpuCosts(250L);
        slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, execPlan);
        assertThat(slots).isEqualTo(3);
    }

    @Test
    public void testEstimateSlotsForExplainWithParallelismBasedPolicyAndEmptyFragments() {
        Config.query_queue_slots_estimator_strategy = "PBE";
        final int numWorkers = 3;
        QueryQueueOptions opts = new QueryQueueOptions(true,
                new QueryQueueOptions.V2(4, numWorkers, 16, 64L * 1024 * 1024 * 1024, 4096, 100));

        ExecPlan emptyExecPlan = new ExecPlan();

        int slots = SlotEstimatorFactory.estimateSlotsForExplain(opts, connectContext, emptyExecPlan);
        assertThat(slots).isEqualTo(1);
    }
}
