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
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.epack.warehouse.WarehouseProperty;
import com.starrocks.epack.warehouse.WarehouseSlotManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class WarehouseQueryQueueOptionsTest {
    private boolean prevEnableQueryQueueV2 = false;
    private boolean prevEnableQueryQueueSelect = false;

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private SystemInfoService infoService;
    @Mocked
    private ConnectContext connectContext;

    @BeforeEach
    public void before() {
        prevEnableQueryQueueV2 = Config.enable_query_queue_v2;
        prevEnableQueryQueueSelect = GlobalVariable.isEnableQueryQueueSelect();
    }

    @AfterEach
    public void after() {
        Config.enable_query_queue_v2 = prevEnableQueryQueueV2;
        GlobalVariable.setEnableQueryQueueSelect(prevEnableQueryQueueSelect);
        BackendResourceStat.getInstance().reset();
    }

    @Test
    public void testCreateV2WithMetricsInSharedData() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        long warehouseId = 10000L;
        connectContext.setThreadLocalInfo();
        GlobalStateMgr.getCurrentState().getWarehouseMgr().addWarehouse(new DefaultWarehouse(warehouseId, "wh10000"));
        connectContext.setCurrentWarehouseId(warehouseId);

        final int numWorkers = 3;
        final int numCores = 16;
        BackendResourceStat.getInstance().setNumCoresOfBe(warehouseId, 10003L, numCores);
        BackendResourceStat.getInstance().setNumCoresOfBe(warehouseId, 10004L, numCores);
        BackendResourceStat.getInstance().setNumCoresOfBe(warehouseId, 10005L, numCores);

        {
            Config.enable_query_queue_v2 = false;
            QueryQueueOptions opts = QueryQueueOptions.createFromEnv(warehouseId);
            assertThat(opts.isEnableQueryQueueV2()).isFalse();
            assertThat(opts.v2()).isEqualTo(new QueryQueueOptions.V2());
        }

        // Note: GlobalStateMgr is @Mocked class-wide in this test, so GlobalStateMgr.getCurrentState().getSlotManager()
        // cascades to an auto-generated BaseSlotManager fake whose (otherwise unmocked) concrete methods return nice
        // defaults (0/null), not the real Config-reading BaseSlotManager bodies. Explicitly mock the v2 param getters
        // consulted by createFromEnv to delegate to Config, matching this test's Config-driven (no per-warehouse
        // override) intent -- the per-warehouse fallback-to-Config behavior itself is covered by
        // testWarehouseV2ParamGettersWithFallback using a real WarehouseSlotManager.
        new MockUp<BaseSlotManager>() {
            @Mock
            public boolean isEnableQueryQueueV2(long warehouseId) {
                return true;
            }

            @Mock
            public int getQueryQueueV2ConcurrencyLevel(long warehouseId) {
                return Config.query_queue_v2_concurrency_level;
            }

            @Mock
            public long getQueryQueueV2MemBytesPerSlot(long warehouseId) {
                return Config.query_queue_v2_mem_bytes_per_slot;
            }

            @Mock
            public long getQueryQueueV2CpuCostsPerSlot(long warehouseId) {
                return Config.query_queue_v2_cpu_costs_per_slot;
            }

            @Mock
            public String getQueryQueueSlotsEstimatorStrategy(long warehouseId) {
                return Config.query_queue_slots_estimator_strategy;
            }

            @Mock
            public String getQueryQueueV2ScheduleStrategy(long warehouseId) {
                return Config.query_queue_v2_schedule_strategy;
            }
        };

        {
            Config.enable_query_queue_v2 = true;
            QueryQueueOptions opts = QueryQueueOptions.createFromEnv(warehouseId);
            assertThat(opts.isEnableQueryQueueV2()).isTrue();
            assertThat(opts.v2()).isNotEqualTo(new QueryQueueOptions.V2());
            QueryQueueOptions.V2 v2 = opts.v2();
            int effectiveConcurrencyLevel = Config.query_queue_v2_concurrency_level <= 0 ? 4 :
                    Config.query_queue_v2_concurrency_level;
            double capacityRatio = (double) effectiveConcurrencyLevel / 4;

            assertThat(v2.getNumWorkers()).isEqualTo(numWorkers);
            assertThat(v2.getNumRowsPerSlot()).isEqualTo(Config.query_queue_v2_num_rows_per_slot);
            assertThat(QueryQueueOptions.correctSlotNum(v2.getTotalSlots()))
                    .isEqualTo((int) Math.round(numWorkers * numCores * capacityRatio));
            assertThat(v2.getTotalSmallSlots()).isZero();
            assertThat(v2.getCpuCostsPerSlot()).isEqualTo(Config.query_queue_v2_cpu_costs_per_slot);
        }
    }

    /**
     * NOTE on test setup: unlike {@link #testCreateV2WithMetricsInSharedData()}, this test cannot reuse
     * {@code GlobalStateMgr.getCurrentState().getWarehouseMgr().addWarehouse(...)} to install the warehouse,
     * because in this test class {@link GlobalStateMgr} is a class-level {@code @Mocked} type: with no
     * recorded {@code Expectations}, JMockit's cascading returns an auto-generated fake
     * ({@code $Subclass_Warehouse_getWarehouse}) from {@code getWarehouse(id)}, not the real object passed to
     * {@code addWarehouse(...)}; similarly {@code GlobalStateMgr.getCurrentState().getSlotManager()} would
     * cascade to a fake {@code BaseSlotManager} subclass rather than a real {@link WarehouseSlotManager}
     * (verified empirically while writing this test). {@code testCreateV2WithMetricsInSharedData} never
     * notices this because it never dereferences the returned {@code Warehouse} object and never casts the
     * slot manager to {@code WarehouseSlotManager}.
     * <p>
     * Instead, this test constructs a real {@link WarehouseSlotManager} directly (same pattern as
     * {@code new SlotManager(new ResourceUsageMonitor())} in {@link SlotTrackerTest}), builds a real
     * {@link LocalWarehouse} backed by a real, mutable {@link WarehouseProperty}, and mocks the static
     * {@link BaseSlotManager#getWarehouse(long)} lookup (the same method the existing
     * {@code getQueryQueueConcurrencyLimit}/{@code isEnableQueryQueueV2} overrides call) to resolve to that
     * warehouse -- mirroring the file's existing {@code MockUp<BaseSlotManager>} idiom used a few lines above
     * for {@code isEnableQueryQueueV2}. This harness was sanity-checked against the already-shipped
     * {@code getQueryQueueConcurrencyLimit} override before being used here.
     */
    @Test
    public void testWarehouseV2ParamGettersWithFallback() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        long warehouseId = 30000L;
        WarehouseProperty property = new WarehouseProperty();
        LocalWarehouse wh = new LocalWarehouse(warehouseId, "wh_v2_params", property, "");

        new MockUp<BaseSlotManager>() {
            @Mock
            public Warehouse getWarehouse(long id) {
                return id == warehouseId ? wh : null;
            }
        };

        ResourceUsageMonitor resourceUsageMonitor = new ResourceUsageMonitor();
        WarehouseSlotManager warehouseSlotManager = new WarehouseSlotManager(resourceUsageMonitor);

        // unset => fallback to Config global
        assertThat(warehouseSlotManager.getQueryQueueV2ConcurrencyLevel(warehouseId))
                .isEqualTo(Config.query_queue_v2_concurrency_level);
        assertThat(warehouseSlotManager.getQueryQueueSlotsEstimatorStrategy(warehouseId))
                .isEqualTo(Config.query_queue_slots_estimator_strategy);
        assertThat(warehouseSlotManager.getQueryQueueV2MemBytesPerSlot(warehouseId))
                .isEqualTo(Config.query_queue_v2_mem_bytes_per_slot);
        assertThat(warehouseSlotManager.getQueryQueueV2CpuCostsPerSlot(warehouseId))
                .isEqualTo(Config.query_queue_v2_cpu_costs_per_slot);
        assertThat(warehouseSlotManager.getQueryQueueV2ScheduleStrategy(warehouseId))
                .isEqualTo(Config.query_queue_v2_schedule_strategy);

        // set on warehouse => override wins
        wh.getProperty().setQueryQueueV2ConcurrencyLevel(8);
        wh.getProperty().setQueryQueueSlotsEstimatorStrategy("MBE");
        wh.getProperty().setQueryQueueV2MemBytesPerSlot(2048L);
        wh.getProperty().setQueryQueueV2CpuCostsPerSlot(500L);
        wh.getProperty().setQueryQueueV2ScheduleStrategy("SJF");

        assertThat(warehouseSlotManager.getQueryQueueV2ConcurrencyLevel(warehouseId)).isEqualTo(8);
        assertThat(warehouseSlotManager.getQueryQueueSlotsEstimatorStrategy(warehouseId)).isEqualTo("MBE");
        assertThat(warehouseSlotManager.getQueryQueueV2MemBytesPerSlot(warehouseId)).isEqualTo(2048L);
        assertThat(warehouseSlotManager.getQueryQueueV2CpuCostsPerSlot(warehouseId)).isEqualTo(500L);
        assertThat(warehouseSlotManager.getQueryQueueV2ScheduleStrategy(warehouseId)).isEqualTo("SJF");
    }

    /**
     * {@link QueryQueueOptions#createFromEnv(long)} must resolve the estimator policy through the (possibly
     * per-warehouse-overridden) {@code BaseSlotManager} getters, and store the resolved policy on the returned
     * {@link QueryQueueOptions} so that {@link SlotEstimatorFactory#create(QueryQueueOptions)} agrees with it --
     * instead of {@code SlotEstimatorFactory} independently re-reading the FE-global {@code Config} at query time,
     * which would desync from the per-warehouse strategy used to size {@code V2#getTotalSlots()}.
     * <p>
     * Same real-object + MockUp harness as {@link #testWarehouseV2ParamGettersWithFallback()}, plus wiring
     * {@code GlobalStateMgr.getCurrentState().getSlotManager()} (which {@code createFromEnv} calls) to the real
     * {@link WarehouseSlotManager} via a {@code MockUp<GlobalStateMgr>} override of {@code getSlotManager()} --
     * the same idiom used elsewhere in the codebase (e.g. {@code WarehouseManagerEPackTest},
     * {@code QueryQueueManagerTest}) to make a specific real object come back from a getter on a class that is
     * otherwise {@code @Mocked} (an {@code Expectations} block recorded on the class-level {@code @Mocked}
     * {@link #globalStateMgr} field was tried first, but empirically does NOT intercept the call as seen from
     * {@code QueryQueueOptions.createFromEnv} in a different class -- {@code MockUp} does, since it redefines the
     * method's bytecode directly rather than relying on cascading-instance identity).
     */
    @Test
    public void testCreateFromEnvReflectsWarehouseEstimatorStrategy() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        long warehouseId = 30001L;
        WarehouseProperty property = new WarehouseProperty();
        LocalWarehouse wh = new LocalWarehouse(warehouseId, "wh_estimator_policy", property, "");

        new MockUp<BaseSlotManager>() {
            @Mock
            public Warehouse getWarehouse(long id) {
                return id == warehouseId ? wh : null;
            }
        };

        ResourceUsageMonitor resourceUsageMonitor = new ResourceUsageMonitor();
        WarehouseSlotManager warehouseSlotManager = new WarehouseSlotManager(resourceUsageMonitor);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public BaseSlotManager getSlotManager() {
                return warehouseSlotManager;
            }
        };

        // warehouse property sets the estimator strategy to MBE and a concurrency level, overriding Config
        wh.getProperty().setEnableQueryQueue(true);
        wh.getProperty().setQueryQueueSlotsEstimatorStrategy("MBE");
        wh.getProperty().setQueryQueueV2ConcurrencyLevel(8);

        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(warehouseId);

        assertThat(opts.getEstimatorPolicy()).isEqualTo(SlotEstimatorFactory.EstimatorPolicy.MBE);
        // The per-query estimator must agree with the stored (per-warehouse) policy, not re-read from global Config.
        assertThat(SlotEstimatorFactory.create(opts)).isInstanceOf(SlotEstimatorFactory.MemoryBasedSlotsEstimator.class);
    }
}
