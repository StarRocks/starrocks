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

package com.starrocks.qe.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.ast.ShowRunningQueriesStmt;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.FrontendHbResponse;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TFinishSlotRequirementRequest;
import com.starrocks.thrift.TFinishSlotRequirementResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReleaseSlotRequest;
import com.starrocks.thrift.TReleaseSlotResponse;
import com.starrocks.thrift.TRequireSlotRequest;
import com.starrocks.thrift.TRequireSlotResponse;
import com.starrocks.thrift.TResourceGroupUsage;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.utframe.MockGenericPool;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryQueueManagerTest extends SchedulerTestBase {
    private static final List<Frontend> FRONTENDS = ImmutableList.of(
            new Frontend(FrontendNodeType.FOLLOWER, "fe-1", "127.0.0.1", 8030),
            new Frontend(FrontendNodeType.FOLLOWER, "fe-2", "127.0.0.2", 8030),
            new Frontend(FrontendNodeType.FOLLOWER, "fe-3", "127.0.0.3", 8030)
    );
    private static final Frontend LOCAL_FRONTEND = FRONTENDS.get(1);

    private static final int ABSENT_CONCURRENCY_LIMIT = -1;
    private static final int ABSENT_MAX_CPU_CORES = -1;

    private final QueryQueueManager manager = QueryQueueManager.getInstance();

    private final Map<Long, ResourceGroup> mockedGroups = new ConcurrentHashMap<>();

    private boolean prevQueueEnableSelect;
    private boolean prevQueueEnableStatistic;
    private boolean prevQueueEnableLoad;
    private boolean prevEnableGroupLevelQueue;
    private int prevQueueConcurrencyHardLimit;
    private double prevQueueMemUsedPctHardLimit;
    private int prevQueuePendingTimeoutSecond;
    private int prevQueueTimeoutSecond;
    private int prevQueueMaxQueuedQueries;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SchedulerTestBase.beforeClass();

        MetricRepo.init();
    }

    @Before
    public void before() {
        prevQueueEnableSelect = GlobalVariable.isEnableQueryQueueSelect();
        prevQueueEnableStatistic = GlobalVariable.isEnableQueryQueueStatistic();
        prevQueueEnableLoad = GlobalVariable.isEnableQueryQueueLoad();
        prevEnableGroupLevelQueue = GlobalVariable.isEnableGroupLevelQueryQueue();
        prevQueueConcurrencyHardLimit = GlobalVariable.getQueryQueueConcurrencyLimit();
        prevQueueMemUsedPctHardLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        prevQueuePendingTimeoutSecond = GlobalVariable.getQueryQueuePendingTimeoutSecond();
        prevQueueTimeoutSecond = connectContext.getSessionVariable().getQueryTimeoutS();
        prevQueueMaxQueuedQueries = GlobalVariable.getQueryQueueMaxQueuedQueries();

        GlobalVariable.setEnableGroupLevelQueryQueue(true);

        GlobalVariable.setQueryQueuePendingTimeoutSecond(Config.max_load_timeout_second);
        connectContext.getSessionVariable().setQueryTimeoutS(Config.max_load_timeout_second);

        mockFrontends(FRONTENDS);

        mockFrontendService(new MockFrontendServiceClient());

        SlotManager slotManager = new SlotManager(GlobalStateMgr.getCurrentState().getResourceUsageMonitor());
        slotManager.start();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SlotManager getSlotManager() {
                return slotManager;
            }
        };

        MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(-MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
    }

    @After
    public void after() {
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());

        // Reset query queue configs.
        GlobalVariable.setEnableQueryQueueSelect(prevQueueEnableSelect);
        GlobalVariable.setEnableQueryQueueStatistic(prevQueueEnableStatistic);
        GlobalVariable.setEnableQueryQueueLoad(prevQueueEnableLoad);
        GlobalVariable.setEnableGroupLevelQueryQueue(prevEnableGroupLevelQueue);
        GlobalVariable.setQueryQueueConcurrencyLimit(prevQueueConcurrencyHardLimit);
        GlobalVariable.setQueryQueueMemUsedPctLimit(prevQueueMemUsedPctHardLimit);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(prevQueuePendingTimeoutSecond);
        connectContext.getSessionVariable().setQueryTimeoutS(prevQueueTimeoutSecond);
        GlobalVariable.setQueryQueueMaxQueuedQueries(prevQueueMaxQueuedQueries);
    }

    @Test
    public void testNotWait() throws Exception {
        {
            //  Case 1: Coordinator needn't check queue.
            GlobalVariable.setEnableQueryQueueSelect(true);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select TABLE_CATALOG from information_schema.tables");
            manager.maybeWait(connectContext, coordinator);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
        }

        {
            //  Case 1: Coordinator needn't check queue.
            GlobalVariable.setEnableQueryQueueSelect(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coordinator);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
        }
    }

    @Test
    public void testNeedCheckQueue() throws Exception {
        {
            // 1. ScanNodes is empty.
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select 1");
            Assert.assertFalse(coordinator.getJobSpec().isNeedQueued());
        }

        {
            // 2. ScanNodes only contain SchemaNode.
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select TABLE_CATALOG from information_schema.tables");
            Assert.assertFalse(coordinator.getJobSpec().isNeedQueued());
        }

        {
            // 3. ScanNodes include non-SchemaNode.
            DefaultCoordinator coordinator = getSchedulerWithQueryId(
                    "select TABLE_CATALOG from information_schema.tables UNION ALL select count(1) from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isNeedQueued());
        }

        {
            // 4. set connectContext.needQueued to false.
            connectContext.setNeedQueued(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId(
                    "select TABLE_CATALOG from information_schema.tables UNION ALL select count(1) from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isNeedQueued());
            connectContext.setNeedQueued(true);
        }
    }

    @Test
    public void testEnableQueue() throws Exception {
        {
            // 1. Load type.
            GlobalVariable.setEnableQueryQueueLoad(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("insert into lineitem select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueLoad(true);
            coordinator = getSchedulerWithQueryId("insert into lineitem select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());
            GlobalVariable.setEnableQueryQueueLoad(false);
        }

        {
            // 2. Query for select.
            GlobalVariable.setEnableQueryQueueSelect(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueSelect(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());
            GlobalVariable.setEnableQueryQueueSelect(false);
        }

        {
            // 3. Query for statistic.
            connectContext.setStatisticsContext(false);
            connectContext.setStatisticsJob(true); // Mock statistics job.
            GlobalVariable.setEnableQueryQueueStatistic(false);
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueStatistic(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());

            connectContext.setStatisticsJob(false);
            GlobalVariable.setEnableQueryQueueStatistic(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            connectContext.setStatisticsContext(true);
            GlobalVariable.setEnableQueryQueueStatistic(false);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertFalse(coordinator.getJobSpec().isEnableQueue());

            GlobalVariable.setEnableQueryQueueStatistic(true);
            coordinator = getSchedulerWithQueryId("select * from lineitem");
            Assert.assertTrue(coordinator.getJobSpec().isEnableQueue());
            connectContext.setStatisticsContext(false);
            GlobalVariable.setEnableQueryQueueStatistic(false);
        }
    }

    /**
     * FIXME(liuzihe): This case is unstable, should fix it and enable it in the future.
     */
    @Ignore
    @Test
    public void testGlobalQueueNormal() throws Exception {
        final int concurrencyLimit = 3;
        final int numPendingCoordinators = concurrencyLimit * 5 + 1;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueMaxQueuedQueries(numPendingCoordinators);

        // 1. Run `concurrencyLimit` queries first, and they shouldn't be queued.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        // 2. Then run `numPendingCoordinators` queries, and they should be queued.
        List<DefaultCoordinator> coords = new ArrayList<>(numPendingCoordinators);
        List<Thread> threads = new ArrayList<>();
        Map<TUniqueId, Throwable> queryIdToShouldThrow = new HashMap<>();
        for (int i = 0; i < numPendingCoordinators; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            coords.add(coord);
            threads.add(new Thread(() -> {
                try {
                    manager.maybeWait(connectContext, coord);
                } catch (UserException | InterruptedException e) {
                    Throwable expected = queryIdToShouldThrow.get(coord.getQueryId());
                    if (expected == null) {
                        throw new RuntimeException(e);
                    } else {
                        Assert.assertEquals(expected.getClass(), e.getClass());
                        assertThat(e.getMessage()).contains(expected.getMessage());
                    }
                }
            }));
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> numPendingCoordinators == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        coords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState()));

        // 3. The coming queries exceed the query queue capacity.
        for (int i = 0; i < 10; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Assert.assertThrows("the number of pending queries exceeds capacity", UserException.class,
                    () -> manager.maybeWait(connectContext, coord));
        }

        // 4. Finish the first `concurrencyLimit` queries.
        runningCoords.forEach(DefaultCoordinator::onFinished);
        runningCoords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState()));

        // 5. Each loop dequeues `concurrencyLimit` queries, and cancel one pending queries.
        List<DefaultCoordinator> resetCoords = coords;
        while (!resetCoords.isEmpty()) {
            final int numResetCoords = resetCoords.size();
            int expectedAllocatedCoords = Math.min(numResetCoords, concurrencyLimit);
            // 5.1 `concurrencyLimit` queries become allocated.
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> numResetCoords - expectedAllocatedCoords == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            List<DefaultCoordinator> allocatedCoords =
                    coords.stream().filter(coord -> coord.getSlot().getState() == LogicalSlot.State.ALLOCATED)
                            .collect(Collectors.toList());
            Assert.assertEquals(expectedAllocatedCoords, allocatedCoords.size());

            // 5.2 Cancel one pending query.
            resetCoords = resetCoords.stream().filter(coord -> coord.getSlot().getState() != LogicalSlot.State.ALLOCATED)
                    .collect(Collectors.toList());
            resetCoords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState()));

            if (!resetCoords.isEmpty()) {
                queryIdToShouldThrow.put(resetCoords.get(0).getQueryId(), new UserException("Cancelled"));
                resetCoords.get(0).cancel("Cancel by test");
                Assert.assertEquals(LogicalSlot.State.CANCELLED, resetCoords.get(0).getSlot().getState());
                resetCoords.remove(0);
            }

            // 4.3 Finish these new allocated queries.
            allocatedCoords.forEach(DefaultCoordinator::onFinished);
            allocatedCoords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState()));
        }
    }

    @Test
    public void testDisableGroupLevelQueue() throws Exception {
        final int concurrencyLimit = 100;
        final int groupConcurrencyLimit = 1;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setEnableGroupLevelQueryQueue(false);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        TWorkGroup group10 = new TWorkGroup().setId(10L).setConcurrency_limit(groupConcurrencyLimit);

        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            mockResourceGroup(group10);
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        runningCoords.forEach(DefaultCoordinator::onFinished);
        runningCoords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState()));
    }

    /**
     * FIXME(liuzihe): This case is unstable, should fix it and enable it in the future.
     */
    @Ignore
    @Test
    public void testGroupQueueNormal() throws Exception {
        final int concurrencyLimit = 2;
        final int numGroups = 5;
        final int numGroupPendingCoords = concurrencyLimit * 2 + 1;
        // Each group and non-group has `numGroupPendingCoords` coordinators.
        final int numPendingCoords = numGroupPendingCoords * (numGroups + 1);

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueMaxQueuedQueries(numPendingCoords);

        TWorkGroup group0 = new TWorkGroup().setId(0L).setConcurrency_limit(concurrencyLimit - 1);
        TWorkGroup group1 = new TWorkGroup().setId(1L).setConcurrency_limit(0);
        TWorkGroup group2 = new TWorkGroup().setId(2L).setConcurrency_limit(ABSENT_CONCURRENCY_LIMIT);
        TWorkGroup group3 = new TWorkGroup().setId(3L).setConcurrency_limit(concurrencyLimit);
        TWorkGroup group4 = new TWorkGroup().setId(4L).setConcurrency_limit(concurrencyLimit + 1);
        TWorkGroup nonGroup = new TWorkGroup().setId(LogicalSlot.ABSENT_GROUP_ID);
        List<TWorkGroup> groups = ImmutableList.of(group0, group1, group2, group3, group4, nonGroup);

        // 1. Run `concurrencyLimit` non-group queries first, and they shouldn't be queued.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            mockResourceGroup(null);
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        // 2. Then run `numPendingCoordinators` queries, and they should be queued.
        List<DefaultCoordinator> coords = new ArrayList<>(numPendingCoords);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numGroupPendingCoords; i++) {
            for (TWorkGroup group : groups) {
                if (group.getId() == LogicalSlot.ABSENT_GROUP_ID) {
                    mockResourceGroup(null);
                } else {
                    mockResourceGroup(group);
                }
                DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
                coords.add(coord);

                threads.add(new Thread(() -> {
                    try {
                        manager.maybeWait(connectContext, coord);
                    } catch (UserException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> numPendingCoords == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        coords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState()));
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().size() ==
                        numPendingCoords + concurrencyLimit);

        // 3. The coming queries exceed the query queue capacity.
        for (int i = 0; i < 10; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Assert.assertThrows("the number of pending queries exceeds capacity", UserException.class,
                    () -> manager.maybeWait(connectContext, coord));
        }

        // 4. Finish the first `concurrencyLimit` non-group queries.
        runningCoords.forEach(DefaultCoordinator::onFinished);
        runningCoords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState()));

        // 5. Each loop dequeues `concurrencyLimit` queries, and cancel one pending queries.
        List<DefaultCoordinator> resetCoords = coords;
        while (!resetCoords.isEmpty()) {
            final int numResetCoords = resetCoords.size();
            int expectedAllocatedCoords = Math.min(numResetCoords, concurrencyLimit);
            // 5.1 `concurrencyLimit` queries become allocated.
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> numResetCoords - expectedAllocatedCoords == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            List<DefaultCoordinator> allocatedCoords =
                    coords.stream().filter(coord -> coord.getSlot().getState() == LogicalSlot.State.ALLOCATED)
                            .collect(Collectors.toList());
            Assert.assertEquals(expectedAllocatedCoords, allocatedCoords.size());

            resetCoords = resetCoords.stream().filter(coord -> coord.getSlot().getState() != LogicalSlot.State.ALLOCATED)
                    .collect(Collectors.toList());

            // 5.2 Finish these new allocated queries.
            allocatedCoords.forEach(DefaultCoordinator::onFinished);
            allocatedCoords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState()));
        }
    }

    public void testGroupQueueNormalWithoutGlobalLimit() throws Exception {
        final int concurrencyLimit = 4;
        final int numGroups = 4;
        final int numGroupPendingCoords = concurrencyLimit * 2 + 1;
        // Each group and non-group has `numGroupPendingCoords` coordinators.
        final int numPendingCoords = numGroupPendingCoords * (numGroups + 1);

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(0);
        GlobalVariable.setQueryQueueMaxQueuedQueries(numPendingCoords);

        TWorkGroup group0 = new TWorkGroup().setId(0L).setConcurrency_limit(concurrencyLimit - 1);
        TWorkGroup group1 = new TWorkGroup().setId(1L).setConcurrency_limit(0);
        TWorkGroup group2 = new TWorkGroup().setId(2L).setConcurrency_limit(concurrencyLimit);
        TWorkGroup group3 = new TWorkGroup().setId(3L).setConcurrency_limit(concurrencyLimit + 1);
        TWorkGroup nonGroup = new TWorkGroup().setId(LogicalSlot.ABSENT_GROUP_ID);
        List<TWorkGroup> groups = ImmutableList.of(group0, group1, group2, group3, nonGroup);

        Map<TWorkGroup, List<DefaultCoordinator>> groupToCoords =
                groups.stream().collect(Collectors.toMap(Function.identity(), (group) -> new ArrayList<>()));
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numGroupPendingCoords; i++) {
            for (TWorkGroup group : groups) {
                if (group.getId() == LogicalSlot.ABSENT_GROUP_ID) {
                    mockResourceGroup(null);
                } else {
                    mockResourceGroup(group);
                }
                DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
                groupToCoords.get(group).add(coord);

                threads.add(new Thread(() -> {
                    try {
                        manager.maybeWait(connectContext, coord);
                    } catch (UserException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
        }
        threads.forEach(Thread::start);

        boolean allFinished = false;
        while (!allFinished) {
            groupToCoords.forEach((group, coords) -> {
                int limit = group.getConcurrency_limit();
                if (limit <= 0) {
                    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> coords.stream()
                            .allMatch(coord -> coord.getSlot().getState() == LogicalSlot.State.ALLOCATED));
                    coords.forEach(DefaultCoordinator::onFinished);
                    groupToCoords.put(group, Collections.emptyList());
                    return;
                }

                // In each loop, each group allocates `limit` slots.
                int numAllocatedCoords = Math.min(limit, coords.size());
                Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                        coords.stream().filter(coord -> coord.getSlot().getState() == LogicalSlot.State.ALLOCATED).count() ==
                                numAllocatedCoords);

                List<DefaultCoordinator> allocatedCoords =
                        coords.stream().filter(coord -> coord.getSlot().getState() == LogicalSlot.State.ALLOCATED)
                                .collect(Collectors.toList());

                List<DefaultCoordinator> pendingCoords =
                        coords.stream().filter(coord -> coord.getSlot().getState() != LogicalSlot.State.ALLOCATED)
                                .collect(Collectors.toList());
                groupToCoords.put(group, pendingCoords);

                allocatedCoords.forEach(DefaultCoordinator::onFinished);
            });

            allFinished = groupToCoords.values().stream().allMatch(List::isEmpty);
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());
    }

    @Test
    public void testPendingTimeout() throws Exception {
        final int concurrencyLimit = 3;
        final int pendingTimeoutSecond = 2;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(pendingTimeoutSecond);

        // 1. Run `concurrencyLimit` queries first, and they shouldn't be queued.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        {
            // 2.1 The coming query pending timeout, query_timeout (300) > pending_timeout (2).
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Assert.assertThrows("pending timeout", UserException.class, () -> manager.maybeWait(connectContext, coord));
        }

        {
            // 2.2 The coming query pending timeout, query_timeout (2) < pending_timeout (300).
            GlobalVariable.setQueryQueuePendingTimeoutSecond(300);
            DefaultCoordinator coord = getSchedulerWithQueryId("select /*+SET_VAR(query_timeout=2)*/ count(1) from lineitem");
            Assert.assertThrows("pending timeout", UserException.class, () -> manager.maybeWait(connectContext, coord));
        }

        {
            // 2.3 The coming query pending timeout but failed to releaseSlot,
            // and then SlotManager should clear this expired pending query.
            mockFrontendService(new MockFrontendServiceClient() {
                @Override
                public TReleaseSlotResponse releaseSlot(TReleaseSlotRequest request) throws TException {
                    throw new TException("mocked-release-slot-exception");
                }
            });
            GlobalVariable.setQueryQueuePendingTimeoutSecond(300);
            DefaultCoordinator coord = getSchedulerWithQueryId("select /*+SET_VAR(query_timeout=2)*/ count(1) from lineitem");
            Assert.assertThrows("pending timeout", UserException.class, () -> manager.maybeWait(connectContext, coord));
            mockFrontendService(new MockFrontendServiceClient());
        }

        // 3. Finish the first `concurrencyLimit` non-group queries.
        runningCoords.forEach(DefaultCoordinator::onFinished);
        runningCoords.forEach(coordinator -> Assert.assertEquals(LogicalSlot.State.RELEASED, coordinator.getSlot().getState()));

        // SlotManager should clear this expired pending query.
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());
    }

    @Test
    public void testAllocatedSlotTimeout() throws Exception {
        final int concurrencyLimit = 3;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        // 1. Run `concurrencyLimit` queries first, and they shouldn't be queued.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select /*+SET_VAR(query_timeout=2)*/ count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        // 2. The coming query is allocated slots, after the previous queries with allocated slot is expired.
        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        manager.maybeWait(connectContext, coord);

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

        // 3. Finish this query.
        coord.onFinished();
        Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState());
    }

    @Test
    public void testRequireSlotFromManagerFailed() throws Exception {
        mockFrontendService(new MockFrontendServiceClient() {
            @Override
            public TRequireSlotResponse requireSlotAsync(TRequireSlotRequest request) throws TException {
                throw new TException("mock-require-slot-async-exception");
            }
        });

        GlobalVariable.setEnableQueryQueueSelect(true);

        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        Assert.assertThrows("mock-require-slot-async-exception", UserException.class,
                () -> manager.maybeWait(connectContext, coord));
    }

    @Test
    public void testLowerVersionLeaderAtUpgrading() throws Exception {
        {
            mockFrontendService(new MockFrontendServiceClient() {
                @Override
                public TRequireSlotResponse requireSlotAsync(TRequireSlotRequest request) throws TException {
                    throw new TApplicationException(TApplicationException.UNKNOWN_METHOD, "mock-invalid-method");
                }
            });

            GlobalVariable.setEnableQueryQueueSelect(true);

            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState());
        }

        {
            mockFrontendService(new MockFrontendServiceClient() {
                @Override
                public TRequireSlotResponse requireSlotAsync(TRequireSlotRequest request) throws TException {
                    throw new TApplicationException(TApplicationException.UNKNOWN, "mock-not-invalid-method");
                }
            });

            GlobalVariable.setEnableQueryQueueSelect(true);

            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Assert.assertThrows("mock-not-invalid-method", UserException.class, () -> manager.maybeWait(connectContext, coord));
        }
    }

    @Test
    public void testLeaderChangeWhenPending() throws Exception {
        final int concurrencyLimit = 3;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        // 1. Run `concurrencyLimit` queries first, and they shouldn't be queued.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        // 2. The coming query is pending.
        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        Thread thread = new Thread(() -> {
            try {
                manager.maybeWait(connectContext, coord);
            } catch (UserException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue() > 0);
        Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState());

        // 2. The leader is changed, so the query can get slot from the new leader.
        SlotManager oldSlotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        SlotManager slotManager = new SlotManager(GlobalStateMgr.getCurrentState().getResourceUsageMonitor());
        slotManager.start();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SlotManager getSlotManager() {
                return slotManager;
            }
        };
        changeLeader(FRONTENDS.get(1));
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

        // 3. Finish this query.
        coord.onFinished();
        Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState());

        new MockUp<GlobalStateMgr>() {
            @Mock
            public SlotManager getSlotManager() {
                return oldSlotManager;
            }
        };
        runningCoords.forEach(DefaultCoordinator::onFinished);
    }

    @Test
    public void testFinishSlotRequirementForCancelledQuery() throws Exception {
        final int concurrencyLimit = 3;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        // 1. Run `concurrencyLimit` queries first, and they shouldn't be queued.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        {
            // 2. The coming query is pending.
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Thread thread = new Thread(
                    () -> Assert.assertThrows("Cancelled", UserException.class, () -> manager.maybeWait(connectContext, coord)));
            thread.start();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> 1 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState());

            // 3. Cancel this query, and failed to releaseSlot due to exception.
            mockFrontendService(new MockFrontendServiceClient() {
                @Override
                public TReleaseSlotResponse releaseSlot(TReleaseSlotRequest request) throws TException {
                    throw new TException("mocked-release-slot-exception");
                }
            });
            coord.cancel("Cancel by test");
            mockFrontendService(new MockFrontendServiceClient());
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(LogicalSlot.State.CANCELLED, coord.getSlot().getState());
        }

        {
            // 2. The coming query is pending.
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Thread thread = new Thread(
                    () -> Assert.assertThrows("Cancelled", UserException.class, () -> manager.maybeWait(connectContext, coord)));
            thread.start();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> 1 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState());

            // 3. Cancel this query, and failed to releaseSlot due to error status without msg.
            mockFrontendService(new MockFrontendServiceClient() {
                @Override
                public TReleaseSlotResponse releaseSlot(TReleaseSlotRequest request) throws TException {
                    TReleaseSlotResponse res = new TReleaseSlotResponse();
                    TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
                    res.setStatus(status);
                    return res;
                }
            });
            coord.cancel("Cancel by test");
            mockFrontendService(new MockFrontendServiceClient());
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(LogicalSlot.State.CANCELLED, coord.getSlot().getState());
        }

        {
            // 2. The coming query is pending.
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Thread thread = new Thread(
                    () -> Assert.assertThrows("Cancelled", UserException.class, () -> manager.maybeWait(connectContext, coord)));
            thread.start();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> 1 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState());

            // 3. Cancel this query, and failed to releaseSlot due to error status with msg.
            mockFrontendService(new MockFrontendServiceClient() {
                @Override
                public TReleaseSlotResponse releaseSlot(TReleaseSlotRequest request) throws TException {
                    TReleaseSlotResponse res = new TReleaseSlotResponse();
                    TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
                    status.setError_msgs(ImmutableList.of("msg1", "msg2"));
                    res.setStatus(status);
                    return res;
                }
            });
            coord.cancel("Cancel by test");
            mockFrontendService(new MockFrontendServiceClient());
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(LogicalSlot.State.CANCELLED, coord.getSlot().getState());
        }

        // 4. Finish the first `concurrencyLimit` queries.
        runningCoords.forEach(DefaultCoordinator::onFinished);
        runningCoords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState()));

        // 5. SlotManager sends the RPC `finishSlotRequirement` and failed due to the cancelled query,
        // so there shouldn't be any slot in SlotManager anymore.
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());
    }

    @Test
    public void testFinishSlotRequirementFailed() throws Exception {
        final int concurrencyLimit = 3;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        // 1. Run `concurrencyLimit-1` queries first, and they shouldn't be queued.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit - 1; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        {
            // 2. `finishSlotRequirement` for the coming query failed.
            mockFrontendService(new MockFrontendServiceClient() {
                @Override
                public TFinishSlotRequirementResponse finishSlotRequirement(TFinishSlotRequirementRequest request)
                        throws TException {
                    throw new TException("mocked-finish-slot-requirement-exception");
                }
            });
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Thread thread = new Thread(
                    () -> Assert.assertThrows("Cancelled", UserException.class, () -> manager.maybeWait(connectContext, coord)));
            thread.start();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> 1 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState());

            // The slot should be removed after failing to `finishSlotRequirement`.
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().size() == concurrencyLimit - 1);

            coord.cancel("Cancel by test");
        }

        {
            // 3. `finishSlotRequirement` for the coming query success.
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            mockFrontendService(new MockFrontendServiceClient());
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
            runningCoords.add(coord);
        }

        // 4. Finish the first `concurrencyLimit` queries.
        runningCoords.forEach(DefaultCoordinator::onFinished);
        runningCoords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.RELEASED, coord.getSlot().getState()));
    }

    @Test
    public void testFrontendDead() throws Exception {
        final int concurrencyLimit = 3;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        // 1. Run `concurrencyLimit` queries first, and they shouldn't be queued.
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
        }

        // 2. Then run `numPendingCoordinators` queries, and they should be queued.
        List<DefaultCoordinator> coords = new ArrayList<>(concurrencyLimit);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            coords.add(coord);
            threads.add(new Thread(
                    () -> Assert.assertThrows("Cancelled", UserException.class, () -> manager.maybeWait(connectContext, coord))));
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> concurrencyLimit == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        coords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState()));

        // 3. The frontend of the allocated and pending slots becomes dead, the slots should be released.
        for (int i = 0; i <= Config.heartbeat_retry_times; i++) {
            handleHbResponse(LOCAL_FRONTEND, System.currentTimeMillis(), false);
        }
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());

        // 4. Then the next `concurrencyLimit` queries shouldn't be queued.
        List<DefaultCoordinator> coords2 = new ArrayList<>(concurrencyLimit);
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
            coords2.add(coord);
        }
        coords2.forEach(DefaultCoordinator::onFinished);

        coords.forEach((coor -> coor.cancel("Cancel by test")));
    }

    @Test
    public void testFrontendRestart() throws Exception {
        final int concurrencyLimit = 3;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        // 1. Run `concurrencyLimit` queries first, and they shouldn't be queued.
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
        }

        // 2. Restart LOCAL_FRONTEND by updating FE.startTime.
        handleHbResponse(LOCAL_FRONTEND, System.currentTimeMillis(), true);

        // 3. Then run `numPendingCoordinators` queries, and they shouldn't be queued.
        List<DefaultCoordinator> coords = new ArrayList<>(concurrencyLimit);
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
            coords.add(coord);
        }

        coords.forEach(DefaultCoordinator::onFinished);
    }

    @Test
    public void testSlotRequirementFromOldFeStartTime() throws Exception {
        final int concurrencyLimit = 3;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        mockFrontendService(new MockFrontendServiceClient() {
            @Override
            public TRequireSlotResponse requireSlotAsync(TRequireSlotRequest request) throws TException {
                // Mock FE restarting between creating slot requirement and sending RPC.
                handleHbResponse(LOCAL_FRONTEND, System.currentTimeMillis(), true);
                return super.requireSlotAsync(request);
            }
        });

        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        Assert.assertThrows("FeStartTime is not the latest", UserException.class, () -> manager.maybeWait(connectContext, coord));
        Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
        Assert.assertEquals(LogicalSlot.State.CANCELLED, coord.getSlot().getState());
    }

    @Test
    public void testSlotRequirementFromUnknownFe() throws Exception {
        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(1);

        // Make LOCAL_FRONTEND is unknown.
        List<Frontend> frontendsByName = ImmutableList.of(FRONTENDS.get(0), FRONTENDS.get(2));
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getFeByName(String name) {
                return frontendsByName.stream().filter(fe -> name.equals(fe.getNodeName())).findAny().orElse(null);
            }

            @Mock
            public Frontend getFeByHost(String host) {
                return FRONTENDS.stream().filter(fe -> host.equals(fe.getHost())).findAny().orElse(null);
            }

            @Mock
            public Pair<String, Integer> getSelfIpAndRpcPort() {
                return Pair.create(LOCAL_FRONTEND.getHost(), LOCAL_FRONTEND.getRpcPort());
            }
        };

        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        Assert.assertThrows("pending timeout", UserException.class, () -> manager.maybeWait(connectContext, coord));
    }

    @Test
    public void testResourceUsageEmptyWorker() throws Exception {
        final int concurrencyLimit = 3;
        final int cpuUsagePermilleLimit = 10;
        final double memUsagePctLimit = 0.1;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueCpuUsedPermilleLimit(cpuUsagePermilleLimit);
        GlobalVariable.setQueryQueueMemUsedPctLimit(memUsagePctLimit);

        // Empty backend needn't check resource usage.
        {
            List<DefaultCoordinator> coords = new ArrayList<>(concurrencyLimit);
            for (int i = 0; i < concurrencyLimit; i++) {
                DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
                coords.add(coord);
                manager.maybeWait(connectContext, coord);
                Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
                Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
            }
            coords.forEach(DefaultCoordinator::onFinished);
        }
    }

    @Test
    public void testResourceUsageCpuPermilleLimit() throws Exception {
        final int concurrencyLimit = 3;
        final int cpuUsagePermilleLimit = 10;
        final double memUsagePctLimit = 0.1;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueCpuUsedPermilleLimit(cpuUsagePermilleLimit);
        GlobalVariable.setQueryQueueMemUsedPctLimit(memUsagePctLimit);

        List<Backend> backends = ImmutableList.of(new Backend(0L, "be0-host", 8030), new Backend(1L, "be1-host", 8030),
                new Backend(2L, "be2-host", 8030));
        List<ComputeNode> computeNodes =
                ImmutableList.of(new ComputeNode(3L, "cn3-host", 8030), new ComputeNode(4L, "cn4-host", 8030),
                        new ComputeNode(5L, "cn5-host", 8030));
        Stream.concat(backends.stream(), computeNodes.stream()).forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);
        computeNodes.forEach(GlobalStateMgr.getCurrentSystemInfo()::addComputeNode);

        // 1. Queries are queued, due to CPU usage exceeds cpuUsagePermilleLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 0, 10, null);

        List<DefaultCoordinator> coords = new ArrayList<>(concurrencyLimit);
        List<Thread> threads = new ArrayList<>(concurrencyLimit);
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            coords.add(coord);
            threads.add(new Thread(() -> {
                try {
                    manager.maybeWait(connectContext, coord);
                } catch (UserException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> LogicalSlot.State.REQUIRING == coord.getSlot().getState()));

        // 2. Queries are not queued anymore, after CPU usage doesn't exceed cpuUsagePermilleLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 0, 1, null);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> LogicalSlot.State.ALLOCATED == coord.getSlot().getState()));
        coords.forEach(DefaultCoordinator::onFinished);
    }

    @Test
    public void testResourceUsageMemUsagePctLimit() throws Exception {
        final int concurrencyLimit = 3;
        final int cpuUsagePermilleLimit = 10;
        final double memUsagePctLimit = 0.1;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueCpuUsedPermilleLimit(cpuUsagePermilleLimit);
        GlobalVariable.setQueryQueueMemUsedPctLimit(memUsagePctLimit);

        List<Backend> backends = ImmutableList.of(new Backend(0L, "be0-host", 8030), new Backend(1L, "be1-host", 8030),
                new Backend(2L, "be2-host", 8030));
        List<ComputeNode> computeNodes =
                ImmutableList.of(new ComputeNode(3L, "cn3-host", 8030), new ComputeNode(4L, "cn4-host", 8030),
                        new ComputeNode(5L, "cn5-host", 8030));
        Stream.concat(backends.stream(), computeNodes.stream()).forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);
        computeNodes.forEach(GlobalStateMgr.getCurrentSystemInfo()::addComputeNode);

        // 1. Queries are queued, due to mem usage exceeds memUsagePctLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0, null);

        List<DefaultCoordinator> coords = new ArrayList<>(concurrencyLimit);
        List<Thread> threads = new ArrayList<>(concurrencyLimit);
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            coords.add(coord);
            threads.add(new Thread(() -> {
                try {
                    manager.maybeWait(connectContext, coord);
                } catch (UserException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> LogicalSlot.State.REQUIRING == coord.getSlot().getState()));

        // 2. Queries are not queued anymore, after mem usage doesn't exceed cpuUsagePermilleLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 0, 0, null);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> LogicalSlot.State.ALLOCATED == coord.getSlot().getState()));
        coords.forEach(DefaultCoordinator::onFinished);
    }

    @Test
    public void testResourceGroupMaxCpuCores() throws Exception {
        final int numGroupsWithEffectiveMaxCores = 2;
        final int numQueriesPerGroup = 3;

        GlobalVariable.setEnableQueryQueueSelect(true);

        TWorkGroup group0 = new TWorkGroup().setId(0L).setMax_cpu_cores(1);
        TWorkGroup group1 = new TWorkGroup().setId(1L).setMax_cpu_cores(0);
        TWorkGroup group2 = new TWorkGroup().setId(2L).setMax_cpu_cores(ABSENT_MAX_CPU_CORES);
        TWorkGroup group3 = new TWorkGroup().setId(3L).setMax_cpu_cores(2);
        TWorkGroup nonGroup = new TWorkGroup().setId(LogicalSlot.ABSENT_GROUP_ID);
        List<TWorkGroup> groups = ImmutableList.of(group0, group1, group2, group3, nonGroup);
        groups.forEach(this::mockResourceGroup);

        List<Backend> backends = ImmutableList.of(
                new Backend(0L, "be0-host", 8030),
                new Backend(1L, "be1-host", 8030),
                new Backend(2L, "be2-host", 8030));
        List<ComputeNode> computeNodes = ImmutableList.of(
                new ComputeNode(3L, "cn3-host", 8030),
                new ComputeNode(4L, "cn4-host", 8030),
                new ComputeNode(5L, "cn5-host", 8030));
        Stream.concat(backends.stream(), computeNodes.stream()).forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);
        computeNodes.forEach(GlobalStateMgr.getCurrentSystemInfo()::addComputeNode);

        // 1. Queries of group #0 and #3 will be pending.
        List<TResourceGroupUsage> groupUsages = ImmutableList.of(
                new TResourceGroupUsage().setGroup_id(0L).setCpu_core_used_permille(1000).setMem_used_bytes(10),
                new TResourceGroupUsage().setGroup_id(1L).setCpu_core_used_permille(10000),
                new TResourceGroupUsage().setGroup_id(2L).setCpu_core_used_permille(10000),
                new TResourceGroupUsage().setGroup_id(3L).setCpu_core_used_permille(3000)
        );
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0, groupUsages);

        List<Thread> threads = new ArrayList<>();
        List<DefaultCoordinator> coords = new ArrayList<>();
        for (int i = 0; i < numQueriesPerGroup; i++) {
            for (TWorkGroup group : groups) {
                mockResourceGroup(group);

                DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
                coords.add(coord);

                threads.add(new Thread(() -> {
                    try {
                        manager.maybeWait(connectContext, coord);
                    } catch (UserException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
        }

        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                () -> numGroupsWithEffectiveMaxCores * numQueriesPerGroup == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());

        // 2. Group #0 is not overloaded anymore.
        groupUsages = ImmutableList.of(
                new TResourceGroupUsage().setGroup_id(0L).setCpu_core_used_permille(900),
                new TResourceGroupUsage().setGroup_id(3L).setCpu_core_used_permille(3500)
        );
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0, groupUsages);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                () -> numQueriesPerGroup == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());

        // 3. Group #3 is not overloaded anymore.
        groupUsages = ImmutableList.of(
                new TResourceGroupUsage().setGroup_id(0L).setCpu_core_used_permille(900)
        );
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0, groupUsages);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                () -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());

        coords.forEach(DefaultCoordinator::onFinished);
    }

    @Test
    public void testResourceUsageFreshInterval() throws Exception {
        final int concurrencyLimit = 3;
        final int cpuUsagePermilleLimit = 10;
        final double memUsagePctLimit = 0.1;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueCpuUsedPermilleLimit(cpuUsagePermilleLimit);
        GlobalVariable.setQueryQueueMemUsedPctLimit(memUsagePctLimit);

        List<Backend> backends = ImmutableList.of(new Backend(0L, "be0-host", 8030), new Backend(1L, "be1-host", 8030),
                new Backend(2L, "be2-host", 8030));
        List<ComputeNode> computeNodes =
                ImmutableList.of(new ComputeNode(3L, "cn3-host", 8030), new ComputeNode(4L, "cn4-host", 8030),
                        new ComputeNode(5L, "cn5-host", 8030));
        Stream.concat(backends.stream(), computeNodes.stream()).forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);
        computeNodes.forEach(GlobalStateMgr.getCurrentSystemInfo()::addComputeNode);

        // 1. Queries are queued, due to mem usage exceeds memUsagePctLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0, null);

        List<DefaultCoordinator> coords = new ArrayList<>(concurrencyLimit);
        List<Thread> threads = new ArrayList<>(concurrencyLimit);
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            coords.add(coord);
            threads.add(new Thread(() -> {
                try {
                    manager.maybeWait(connectContext, coord);
                } catch (UserException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> LogicalSlot.State.REQUIRING == coord.getSlot().getState()));

        // 2. Queries are not queued anymore, because the overloaded BE doesn't report in resourceUsageIntervalMs.
        GlobalVariable.setQueryQueueResourceUsageIntervalMs(1000);
        Thread.sleep(2000);
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(1L, 0, 100, 0, 0, null);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> LogicalSlot.State.ALLOCATED == coord.getSlot().getState()));
        coords.forEach(DefaultCoordinator::onFinished);
    }

    @Test
    public void testResourceUsageBackendDead() throws Exception {
        final int concurrencyLimit = 3;
        final int cpuUsagePermilleLimit = 10;
        final double memUsagePctLimit = 0.1;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueCpuUsedPermilleLimit(cpuUsagePermilleLimit);
        GlobalVariable.setQueryQueueMemUsedPctLimit(memUsagePctLimit);

        List<Backend> backends = ImmutableList.of(new Backend(0L, "be0-host", 8030), new Backend(1L, "be1-host", 8030),
                new Backend(2L, "be2-host", 8030));
        List<ComputeNode> computeNodes =
                ImmutableList.of(new ComputeNode(3L, "cn3-host", 8030), new ComputeNode(4L, "cn4-host", 8030),
                        new ComputeNode(5L, "cn5-host", 8030));
        Stream.concat(backends.stream(), computeNodes.stream()).forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);
        computeNodes.forEach(GlobalStateMgr.getCurrentSystemInfo()::addComputeNode);

        // 1. Queries are queued, due to mem usage exceeds memUsagePctLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0, null);

        List<DefaultCoordinator> coords = new ArrayList<>(concurrencyLimit);
        List<Thread> threads = new ArrayList<>(concurrencyLimit);
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            coords.add(coord);
            threads.add(new Thread(() -> {
                try {
                    manager.maybeWait(connectContext, coord);
                } catch (UserException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> LogicalSlot.State.REQUIRING == coord.getSlot().getState()));

        // 2. Queries are not queued anymore, because the overloaded BE becomes dead.
        backends.get(0).setAlive(false);
        GlobalStateMgr.getCurrentState().getResourceUsageMonitor().notifyBackendDead();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> LogicalSlot.State.ALLOCATED == coord.getSlot().getState()));
        coords.forEach(DefaultCoordinator::onFinished);
    }

    @Test
    public void testShowRunningQueriesEmpty() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "show running queries;";
        ShowRunningQueriesStmt showStmt = (ShowRunningQueriesStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, showStmt);
        ShowResultSet res = executor.execute();
        Assert.assertEquals(showStmt.getMetaData().getColumns(), res.getMetaData().getColumns());
        Assert.assertTrue(res.getResultRows().isEmpty());
    }

    private DefaultCoordinator runNoPendingQuery() throws Exception {
        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        manager.maybeWait(connectContext, coord);
        Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
        Assert.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
        return coord;
    }

    @Test
    public void testShowRunningQueries() throws Exception {
        final int concurrencyLimit = 2;

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        TWorkGroup group0 = new TWorkGroup().setId(0L).setConcurrency_limit(concurrencyLimit - 1);
        TWorkGroup group1 = new TWorkGroup().setId(1L).setConcurrency_limit(concurrencyLimit);
        TWorkGroup nonGroup = new TWorkGroup().setId(LogicalSlot.ABSENT_GROUP_ID);
        List<TWorkGroup> groups = ImmutableList.of(nonGroup, group0, group1);

        final int numPendingCoords = groups.size() * concurrencyLimit;

        // 1. Run `concurrencyLimit` queries.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        mockResourceGroup(null);
        runningCoords.add(runNoPendingQuery());
        mockResourceGroup(group0);
        runningCoords.add(runNoPendingQuery());

        // 2. Each group has `concurrencyLimit` pending queries.
        List<DefaultCoordinator> coords = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            for (TWorkGroup group : groups) {
                if (group.getId() == LogicalSlot.ABSENT_GROUP_ID) {
                    mockResourceGroup(null);
                } else {
                    mockResourceGroup(group);
                }
                DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
                coords.add(coord);

                threads.add(new Thread(() -> Assert.assertThrows("Cancelled", UserException.class,
                        () -> manager.maybeWait(connectContext, coord))));
            }
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> numPendingCoords == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        coords.forEach(coord -> Assert.assertEquals(LogicalSlot.State.REQUIRING, coord.getSlot().getState()));
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().size() ==
                        numPendingCoords + concurrencyLimit);

        {
            // 3. show running queries.
            ConnectContext ctx = starRocksAssert.getCtx();
            String sql = "show running queries;";
            ShowRunningQueriesStmt showStmt = (ShowRunningQueriesStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            ShowExecutor executor = new ShowExecutor(ctx, showStmt);
            ShowResultSet res = executor.execute();
            Assert.assertEquals(showStmt.getMetaData().getColumns(), res.getMetaData().getColumns());

            final int groupIndex = 1;
            final int stateIndex = 5;
            Map<String, Map<String, Integer>> groupToStateToCount = res.getResultRows().stream().collect(Collectors.groupingBy(
                    row -> row.get(groupIndex),
                    Collectors.groupingBy(
                            row -> row.get(stateIndex),
                            Collectors.summingInt(row -> 1)
                    )
            ));
            Map<String, Map<String, Integer>> expectedGroupToStateToCount = ImmutableMap.of(
                    "0", ImmutableMap.of("RUNNING", 1, "PENDING", 2),
                    "1", ImmutableMap.of("PENDING", 2),
                    "-", ImmutableMap.of("RUNNING", 1, "PENDING", 2)
            );
            assertThat(groupToStateToCount).containsExactlyInAnyOrderEntriesOf(expectedGroupToStateToCount);
        }

        {
            // 4. show running queries with limit.
            ConnectContext ctx = starRocksAssert.getCtx();
            String sql = "show running queries limit 4;";
            ShowRunningQueriesStmt showStmt = (ShowRunningQueriesStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            ShowExecutor executor = new ShowExecutor(ctx, showStmt);
            ShowResultSet res = executor.execute();
            Assert.assertEquals(showStmt.getMetaData().getColumns(), res.getMetaData().getColumns());
            Assert.assertEquals(4, res.getResultRows().size());
        }

        coords.forEach(coor -> coor.cancel("Cancel by test"));
        runningCoords.forEach(DefaultCoordinator::onFinished);
    }

    @Test
    public void testShowResourceGroupUsage() throws Exception {
        TWorkGroup group0 = new TWorkGroup().setId(10L).setName("wg0");
        TWorkGroup group1 = new TWorkGroup().setId(11L).setName("wg1");
        TWorkGroup group2 = new TWorkGroup().setId(12L).setName("wg2");
        TWorkGroup group3 = new TWorkGroup().setId(13L).setName("wg3");
        TWorkGroup nonGroup = new TWorkGroup().setId(LogicalSlot.ABSENT_GROUP_ID);
        List<TWorkGroup> groups = ImmutableList.of(group0, group1, group2, group3, nonGroup);
        groups.forEach(this::mockResourceGroup);

        List<Backend> backends = ImmutableList.of(
                new Backend(0L, "be0-host", 8030),
                new Backend(1L, "be1-host", 8030));
        backends.forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);

        {
            String res = starRocksAssert.executeShowResourceUsageSql("SHOW USAGE RESOURCE GROUPS;");
            assertThat(res).isEqualTo("Name|Id|Backend|BEInUseCpuCores|BEInUseMemBytes|BERunningQueries\n");
        }

        List<TResourceGroupUsage> groupUsages = ImmutableList.of(
                new TResourceGroupUsage().setGroup_id(ResourceGroup.DEFAULT_WG_ID).setCpu_core_used_permille(3112)
                        .setMem_used_bytes(39).setNum_running_queries(38),
                new TResourceGroupUsage().setGroup_id(10L).setCpu_core_used_permille(112).setMem_used_bytes(9)
                        .setNum_running_queries(8),
                new TResourceGroupUsage().setGroup_id(11L).setCpu_core_used_permille(100),
                new TResourceGroupUsage().setGroup_id(12L).setCpu_core_used_permille(120).setMem_used_bytes(7)
                        .setNum_running_queries(6),
                new TResourceGroupUsage().setGroup_id(13L).setCpu_core_used_permille(30)
        );
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0, groupUsages);
        groupUsages = ImmutableList.of(
                new TResourceGroupUsage().setGroup_id(ResourceGroup.DEFAULT_MV_WG_ID).setCpu_core_used_permille(4110)
                        .setMem_used_bytes(49).setNum_running_queries(48),
                new TResourceGroupUsage().setGroup_id(10L).setCpu_core_used_permille(1110).setMem_used_bytes(19)
                        .setNum_running_queries(18),
                new TResourceGroupUsage().setGroup_id(11L).setCpu_core_used_permille(1100),
                new TResourceGroupUsage().setGroup_id(12L).setCpu_core_used_permille(1120).setMem_used_bytes(17)
                        .setNum_running_queries(16),
                new TResourceGroupUsage().setGroup_id(13L).setCpu_core_used_permille(130)
        );
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(1L, 0, 100, 30, 0, groupUsages);

        {
            String res = starRocksAssert.executeShowResourceUsageSql("SHOW USAGE RESOURCE GROUPS;");
            assertThat(res).isEqualTo("Name|Id|Backend|BEInUseCpuCores|BEInUseMemBytes|BERunningQueries\n" +
                    "default_wg|0|be0-host|3.112|39|38\n" +
                    "default_mv_wg|1|be1-host|4.11|49|48\n" +
                    "wg0|10|be0-host|0.112|9|8\n" +
                    "wg0|10|be1-host|1.11|19|18\n" +
                    "wg1|11|be0-host|0.1|0|0\n" +
                    "wg1|11|be1-host|1.1|0|0\n" +
                    "wg2|12|be0-host|0.12|7|6\n" +
                    "wg2|12|be1-host|1.12|17|16\n" +
                    "wg3|13|be0-host|0.03|0|0\n" +
                    "wg3|13|be1-host|0.13|0|0");
        }

        groupUsages = ImmutableList.of(
                new TResourceGroupUsage().setGroup_id(10L).setCpu_core_used_permille(210).setMem_used_bytes(29)
                        .setNum_running_queries(28),
                new TResourceGroupUsage().setGroup_id(11L).setCpu_core_used_permille(200)
        );
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0, groupUsages);
        groupUsages = ImmutableList.of(
                new TResourceGroupUsage().setGroup_id(12L).setCpu_core_used_permille(1220).setMem_used_bytes(27)
                        .setNum_running_queries(26),
                new TResourceGroupUsage().setGroup_id(13L).setCpu_core_used_permille(230)
        );
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(1L, 0, 100, 30, 0, groupUsages);

        {
            String res = starRocksAssert.executeShowResourceUsageSql("SHOW USAGE RESOURCE GROUPS;");
            assertThat(res).isEqualTo("Name|Id|Backend|BEInUseCpuCores|BEInUseMemBytes|BERunningQueries\n" +
                    "wg0|10|be0-host|0.21|29|28\n" +
                    "wg1|11|be0-host|0.2|0|0\n" +
                    "wg2|12|be1-host|1.22|27|26\n" +
                    "wg3|13|be1-host|0.23|0|0");
        }
    }

    private static class MockFrontendServiceClient extends FrontendService.Client {
        private final FrontendService.Iface frontendService = new FrontendServiceImpl(null);

        public MockFrontendServiceClient() {
            super(null);
        }

        @Override
        public TRequireSlotResponse requireSlotAsync(TRequireSlotRequest request) throws org.apache.thrift.TException {
            return frontendService.requireSlotAsync(request);
        }

        @Override
        public TReleaseSlotResponse releaseSlot(TReleaseSlotRequest request) throws org.apache.thrift.TException {
            return frontendService.releaseSlot(request);
        }

        @Override
        public TFinishSlotRequirementResponse finishSlotRequirement(TFinishSlotRequirementRequest request)
                throws org.apache.thrift.TException {
            return frontendService.finishSlotRequirement(request);
        }
    }

    private static void mockFrontendService(MockFrontendServiceClient client) {
        ClientPool.frontendPool = new MockGenericPool<FrontendService.Client>("query-queue-mocked-pool") {
            @Override
            public FrontendService.Client borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
                return client;
            }
        };
    }

    /**
     * Make the coordinator of every query uses the mocked resource group.
     *
     * <p> Mock methods:
     * <ul>
     *  <li> {@link CoordinatorPreprocessor#prepareResourceGroup(ConnectContext, ResourceGroupClassifier.QueryType)}
     *  <li> {@link ResourceGroupMgr#getResourceGroup(long)}
     *  <li> {@link ResourceGroupMgr#getResourceGroupIds()}
     * </ul>
     *
     * @param group The returned group of the mocked method.
     */
    private void mockResourceGroup(TWorkGroup group) {
        new MockUp<CoordinatorPreprocessor>() {
            @Mock
            public TWorkGroup prepareResourceGroup(ConnectContext connect, ResourceGroupClassifier.QueryType queryType) {
                return group;
            }
        };

        if (group != null && group.getId() != LogicalSlot.ABSENT_GROUP_ID) {
            ResourceGroup resourceGroup = new ResourceGroup();
            if (group.getConcurrency_limit() != ABSENT_CONCURRENCY_LIMIT) {
                resourceGroup.setConcurrencyLimit(group.getConcurrency_limit());
            }
            if (group.getMax_cpu_cores() != ABSENT_MAX_CPU_CORES) {
                resourceGroup.setMaxCpuCores(group.getMax_cpu_cores());
            }
            resourceGroup.setId(group.getId());
            resourceGroup.setName(group.getName());
            mockedGroups.put(group.getId(), resourceGroup);
            new MockUp<ResourceGroupMgr>() {
                @Mock
                public ResourceGroup getResourceGroup(long id) {
                    return mockedGroups.get(id);
                }

                @Mock
                public List<Long> getResourceGroupIds() {
                    return new ArrayList<>(mockedGroups.keySet());
                }
            };
        }
    }

    /**
     * Mock {@link NodeMgr} to make it return the specific RPC endpoint of self and leader.
     * The mocked methods including {@link NodeMgr#getFeByName(String)}, {@link NodeMgr#getFeByName(String)}
     * and {@link NodeMgr#getSelfIpAndRpcPort()}.
     */
    private static void mockFrontends(List<Frontend> frontends) {
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getFeByName(String name) {
                return frontends.stream().filter(fe -> name.equals(fe.getNodeName())).findAny().orElse(null);
            }

            @Mock
            public Frontend getFeByHost(String host) {
                return frontends.stream().filter(fe -> host.equals(fe.getHost())).findAny().orElse(null);
            }

            @Mock
            public Pair<String, Integer> getSelfIpAndRpcPort() {
                return Pair.create(LOCAL_FRONTEND.getHost(), LOCAL_FRONTEND.getRpcPort());
            }
        };

        long startTimeMs = System.currentTimeMillis() - 1000L;
        frontends.forEach(fe -> handleHbResponse(fe, startTimeMs, true));

        changeLeader(frontends.get(0));
    }

    private static void changeLeader(Frontend fe) {
        LeaderInfo leaderInfo = new LeaderInfo();
        leaderInfo.setIp(fe.getHost());
        leaderInfo.setHttpPort(80);
        leaderInfo.setRpcPort(fe.getRpcPort());
        GlobalStateMgr.getCurrentState().getNodeMgr().setLeader(leaderInfo);
    }

    private static void handleHbResponse(Frontend fe, long startTimeMs, boolean isAlive) {
        FrontendHbResponse hbResponse;
        if (isAlive) {
            hbResponse = new FrontendHbResponse(fe.getNodeName(), fe.getQueryPort(), fe.getRpcPort(),
                    fe.getReplayedJournalId(), fe.getLastUpdateTime(), startTimeMs, fe.getFeVersion());
        } else {
            hbResponse = new FrontendHbResponse(fe.getNodeName(), "mock-dead-frontend");
        }
        fe.handleHbResponse(hbResponse, false);
    }

}
