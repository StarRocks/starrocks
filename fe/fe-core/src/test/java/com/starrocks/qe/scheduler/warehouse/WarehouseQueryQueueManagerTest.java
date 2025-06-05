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

package com.starrocks.qe.scheduler.warehouse;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.epack.warehouse.WarehouseProperty;
import com.starrocks.epack.warehouse.WarehouseSlotManager;
import com.starrocks.epack.warehouse.WarehouseSlotTracker;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.MetricVisitor;
import com.starrocks.metric.PrometheusMetricVisitor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.qe.scheduler.SchedulerTestNoneDBBase;
import com.starrocks.qe.scheduler.slot.BaseSlotTracker;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.QueryQueueOptions;
import com.starrocks.qe.scheduler.slot.SlotSelectionStrategyV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;

public class WarehouseQueryQueueManagerTest extends SchedulerTestBase {

    private static int MAX_QUEUE_PENDING_LENGTH = 4;
    private static int DEFAULT_QUEUE_WAITING_TIMEOUT_SECOND = 30;
    private static int CPU_CORE_PER_BACKEND = 32;

    @BeforeClass
    public static void beforeClass() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();

        FeConstants.runningUnitTest = true;
        Config.enable_statistic_collect = false;

        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        backend2 = UtFrameUtils.addMockBackend(10002, "127.0.0.2", 9060);
        backend2.setAlive(true);
        backend2.setCpuCores(16);
        backend2.setMemLimitBytes(16 * 1024 * 1024 * 1024L);
        backend2.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);

        backend3 = UtFrameUtils.addMockBackend(10003, "127.0.0.3", 9060);
        backend3.setAlive(true);
        backend3.setCpuCores(16);
        backend3.setMemLimitBytes(16 * 1024 * 1024 * 1024L);

        backend2.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend2);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend3);

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> getWorkersByWorkerGroup(long workerGroupId) throws StarRocksException {
                return Lists.newArrayList(backend2.getId(), backend3.getId());
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public List<ComputeNode> getAliveComputeNodes(long warehouseId) {
                return new ArrayList<>(Arrays.asList(backend2, backend3));
            }
        };
        new MockUp<BackendResourceStat>() {
            @Mock
            public static int getAvgNumHardwareCoresOfBe(Map<Long, Integer> numHardwareCoresPerBe) {
                return CPU_CORE_PER_BACKEND;
            }
        };
        SchedulerTestBase.prepareTables(connectContext);
        LocalWarehouse warehouse = (LocalWarehouse) GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
        WarehouseProperty property = warehouse.getProperty();
        property.setEnableQueryQueue(true);
        property.setQueryQueueMaxQueuedQueries(MAX_QUEUE_PENDING_LENGTH);
        property.setQueryQueuePendingTimeoutSecond(DEFAULT_QUEUE_WAITING_TIMEOUT_SECOND);
    }

    @AfterClass
    public static void afterClass() {
        SchedulerTestNoneDBBase.afterClass();
    }

    @Before
    public void before() throws Exception {
        mockFrontends(FRONTENDS);
        mockFrontendService(new MockFrontendServiceClient());
        MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(-MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        connectContext.setStartTime();
    }

    @After
    public void after() {
        Awaitility.await().atMost(DEFAULT_QUEUE_WAITING_TIMEOUT_SECOND, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());
    }

    @Test
    public void testWarehouseSlotManager1() throws Exception {
        WarehouseSlotManager slotManager = (WarehouseSlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = slotManager.getWarehouseIdToSlotTracker();
        Assert.assertTrue(warehouseIdToSlotTracker.size() == 1);
        Assert.assertTrue(slotManager.getSlots().isEmpty());
        // run without query queue
        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        manager.maybeWait(connectContext, coord);
        assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
        Assert.assertTrue(warehouseIdToSlotTracker.size() == 1);
        BaseSlotTracker baseSlotTracker = warehouseIdToSlotTracker.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertTrue(baseSlotTracker != null);
        Assert.assertTrue(baseSlotTracker instanceof WarehouseSlotTracker);
        WarehouseSlotTracker slotTracker = (WarehouseSlotTracker) baseSlotTracker;
        Assert.assertTrue(slotTracker.getSlots().size() == 1);
        LogicalSlot logicalSlot = slotTracker.getSlots().iterator().next();
        assertEquals(LogicalSlot.State.ALLOCATED, logicalSlot.getState());
        coord.onFinished();
    }

    @Test
    public void testWarehouseSlotManager2() throws Exception {
        WarehouseSlotManager slotManager = (WarehouseSlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = slotManager.getWarehouseIdToSlotTracker();
        BaseSlotTracker baseSlotTracker = warehouseIdToSlotTracker.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        WarehouseSlotTracker slotTracker = (WarehouseSlotTracker) baseSlotTracker;
        assertEquals(slotTracker.getMaxSlots(), Optional.of(2 * CPU_CORE_PER_BACKEND));

        Assert.assertTrue(warehouseIdToSlotTracker.size() == 1);
        Assert.assertTrue(slotManager.getSlots().isEmpty());
        // run without query queue
        List<DefaultCoordinator> coordinators = Lists.newArrayList();
        for (int i = 0; i < MAX_QUEUE_PENDING_LENGTH; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
            System.out.println(coord.getSlot());
            coordinators.add(coord);
        }
        coordinators.stream().forEach(Coordinator::onFinished);
    }

    @Test
    public void testWarehouseSlotManager3() throws Exception {
        WarehouseSlotManager slotManager = (WarehouseSlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = slotManager.getWarehouseIdToSlotTracker();
        BaseSlotTracker baseSlotTracker = warehouseIdToSlotTracker.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        WarehouseSlotTracker slotTracker = (WarehouseSlotTracker) baseSlotTracker;
        assertEquals(slotTracker.getMaxSlots(), Optional.of(2 * CPU_CORE_PER_BACKEND));

        Assert.assertTrue(warehouseIdToSlotTracker.size() == 1);
        Assert.assertTrue(slotManager.getSlots().isEmpty());
        // run without query queue
        List<Thread> threads = Lists.newArrayList();
        List<DefaultCoordinator> runningCoords = Lists.newArrayList();
        // running
        for (int i = 0; i < MAX_QUEUE_PENDING_LENGTH; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
            System.out.println(coord.getSlot());
            runningCoords.add(coord);
        }

        new MockUp<BackendResourceStat>() {
            @Mock
            public static int getAvgNumHardwareCoresOfBe(Map<Long, Integer> numHardwareCoresPerBe) {
                return 1;
            }
        };
        Thread.sleep(1000);
        slotTracker.getSlotSelectionStrategy().updateOptionsPeriodically();

        // pending
        List<DefaultCoordinator> pendingCoords = Lists.newArrayList();
        for (int i = 0; i < MAX_QUEUE_PENDING_LENGTH; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            pendingCoords.add(coord);
            threads.add(new Thread(() -> {
                try {
                    manager.maybeWait(connectContext, coord);
                    System.out.println(coord.getSlot());
                } catch (Exception e) {
                    Assert.assertTrue(e instanceof StarRocksException);
                }
            }));
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() ->
                        pendingCoords.stream()
                                .filter(coord -> coord.getSlot().getState().equals(LogicalSlot.State.REQUIRING))
                                .count() == MAX_QUEUE_PENDING_LENGTH);
        pendingCoords.forEach(coord -> assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState()));
        try {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            pendingCoords.add(coord);
            manager.maybeWait(connectContext, coord);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksException);
            Assert.assertTrue(e.getMessage().contains("Resource is not enough and the number of " +
                    "pending queries exceeds capacity"));
        }
        runningCoords.forEach(Coordinator::onFinished);
        pendingCoords.forEach(Coordinator::onFinished);
        runningCoords.forEach(
                coord -> assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState()));
    }

    @Test
    public void testWarehouseSlotManagerFilterValidWarehouses() {
        WarehouseSlotManager slotManager = (WarehouseSlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = slotManager.getWarehouseIdToSlotTracker();
        assertThat(warehouseIdToSlotTracker.size()).isEqualTo(1);

        Map<Long, WarehouseMetricEntity> warehouseMetricEntityMap = slotManager.getWarehouseMetrics();
        assertThat(warehouseMetricEntityMap.size()).isEqualTo(1);
        WarehouseMetricEntity warehouseMetricEntity = warehouseMetricEntityMap.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(warehouseMetricEntity != null).isTrue();
        assertThat(warehouseMetricEntity.getWarehouseId() == WarehouseManager.DEFAULT_WAREHOUSE_ID).isTrue();

        BaseSlotTracker baseSlotTracker = warehouseIdToSlotTracker.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(baseSlotTracker != null).isTrue();
        WarehouseSlotTracker slotTracker = (WarehouseSlotTracker) baseSlotTracker;
        assertThat(slotTracker.getWarehouseId() == WarehouseManager.DEFAULT_WAREHOUSE_ID).isTrue();

        LocalWarehouse warehouse = (LocalWarehouse) GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
        WarehouseProperty property = warehouse.getProperty();
        property.setEnableQueryQueue(false);

        warehouseIdToSlotTracker = slotManager.getWarehouseIdToSlotTracker();
        assertThat(warehouseIdToSlotTracker.size()).isEqualTo(0);
        warehouseMetricEntityMap = slotManager.getWarehouseMetrics();
        assertThat(warehouseMetricEntityMap.size()).isEqualTo(0);
        property.setEnableQueryQueue(true);
    }

    private static Set<String> WAREHOUSE_METRICS_KEYS = ImmutableSet.of(
            "query_pending_length",
            "query_running_length",
            "max_query_queue_length",
            "earliest_query_wait_time",
            "max_query_pending_time_second",
            "max_required_slots",
            "sum_required_slots",
            "remain_slots",
            "max_slots");

    @Test
    public void testCollectWarehouseMetricsNormal() {
        WarehouseSlotManager slotManager = (WarehouseSlotManager) GlobalStateMgr.getCurrentState().getSlotManager();

        MetricVisitor visitor = new PrometheusMetricVisitor("fe_ut");
        slotManager.collectWarehouseMetrics(visitor);

        String result = visitor.build();
        System.out.println("MetricVisitor produces: " + result);
        assertThat(WAREHOUSE_METRICS_KEYS.stream().allMatch(x -> result.contains(x))).isTrue();
    }

    @Test
    public void testCollectWarehouseMetricsBad1() {
        new MockUp<WarehouseSlotManager>() {
            @Mock
            public Map<Long, WarehouseMetricEntity> getWarehouseMetrics() {
                throw new RuntimeException("Mocked exception for testing");
            }
        };
        WarehouseSlotManager slotManager = (WarehouseSlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        MetricVisitor visitor = new PrometheusMetricVisitor("fe_ut");
        slotManager.collectWarehouseMetrics(visitor);
        String result = visitor.build();
        System.out.println("MetricVisitor produces: " + result);
        assertThat(result.equals(""));
    }

    @Test
    public void testCollectWarehouseMetricsBad2() {
        new MockUp<WarehouseMetricEntity>() {
            public List<Metric> getMetrics() {
                throw new RuntimeException("Mocked exception for testing");
            }
        };
        WarehouseSlotManager slotManager = (WarehouseSlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        MetricVisitor visitor = new PrometheusMetricVisitor("fe_ut");
        slotManager.collectWarehouseMetrics(visitor);
        String result = visitor.build();
        System.out.println("MetricVisitor produces: " + result);
        assertThat(result.equals(""));
    }

    @Test
    public void testWarehouseSlotTrackerGetOptsV2Normal() {
        WarehouseSlotManager slotManager = (WarehouseSlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = slotManager.getWarehouseIdToSlotTracker();
        WarehouseSlotTracker warehouseSlotTracker =
                (WarehouseSlotTracker) warehouseIdToSlotTracker.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(warehouseSlotTracker != null).isTrue();
        assertThat(warehouseSlotTracker.getOptsV2()).isPresent();
    }

    @Test
    public void testWarehouseSlotTrackerGetOptsV2Bad() {
        new MockUp<SlotSelectionStrategyV2>() {
            @Mock
            public QueryQueueOptions getOpts() {
                throw new RuntimeException("Mocked exception for testing");
            }
        };
        WarehouseSlotManager slotManager = (WarehouseSlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        Map<Long, BaseSlotTracker> warehouseIdToSlotTracker = slotManager.getWarehouseIdToSlotTracker();
        WarehouseSlotTracker warehouseSlotTracker =
                (WarehouseSlotTracker) warehouseIdToSlotTracker.get(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(warehouseSlotTracker != null).isTrue();
        assertThat(warehouseSlotTracker.getOptsV2()).isEmpty();
    }
}
