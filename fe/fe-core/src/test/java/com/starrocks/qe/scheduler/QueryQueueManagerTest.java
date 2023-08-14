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
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.qe.scheduler.slot.Slot;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TFinishSlotRequirementRequest;
import com.starrocks.thrift.TFinishSlotRequirementResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReleaseSlotRequest;
import com.starrocks.thrift.TReleaseSlotResponse;
import com.starrocks.thrift.TRequireSlotRequest;
import com.starrocks.thrift.TRequireSlotResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.utframe.MockGenericPool;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TException;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
    private static final String FE1 = "fe1";
    private static final String FE2 = "fe2";
    private static final String FE3 = "fe3";

    private final QueryQueueManager manager = QueryQueueManager.getInstance();

    private final Map<Long, ResourceGroup> mockedGroups = new ConcurrentHashMap<>();

    private boolean prevQueueEnable;
    private int prevQueueConcurrencyHardLimit;
    private double prevQueueMemUsedPctHardLimit;
    private int prevQueuePendingTimeoutSecond;
    private int prevQueueMaxQueuedQueries;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SchedulerTestBase.beforeClass();

        MetricRepo.init();
    }

    @Before
    public void before() {
        prevQueueEnable = GlobalVariable.isEnableQueryQueueSelect();
        prevQueueConcurrencyHardLimit = GlobalVariable.getQueryQueueConcurrencyLimit();
        prevQueueMemUsedPctHardLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        prevQueuePendingTimeoutSecond = GlobalVariable.getQueryQueuePendingTimeoutSecond();
        prevQueueMaxQueuedQueries = GlobalVariable.getQueryQueueMaxQueuedQueries();

        mockNodeMgr(FE1, 8888, FE2, 8888);

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
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue() == 0L);

        // Reset query queue configs.
        GlobalVariable.setEnableQueryQueueSelect(prevQueueEnable);
        GlobalVariable.setQueryQueueConcurrencyLimit(prevQueueConcurrencyHardLimit);
        GlobalVariable.setQueryQueueMemUsedPctLimit(prevQueueMemUsedPctHardLimit);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(prevQueuePendingTimeoutSecond);
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
            Assert.assertFalse(manager.needCheckQueue(coordinator));
        }

        {
            // 2. ScanNodes only contain SchemaNode.
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select TABLE_CATALOG from information_schema.tables");
            Assert.assertFalse(manager.needCheckQueue(coordinator));
        }

        {
            // 3. ScanNodes include non-SchemaNode.
            DefaultCoordinator coordinator = getSchedulerWithQueryId(
                    "select TABLE_CATALOG from information_schema.tables UNION ALL select count(1) from lineitem");
            Assert.assertTrue(manager.needCheckQueue(coordinator));
        }
    }

    @Test
    public void testEnableQueue() throws Exception {
        {
            // 1. Load type.
            DefaultCoordinator coordinator = getSchedulerWithQueryId("insert into lineitem select * from lineitem");
            GlobalVariable.setEnableQueryQueueLoad(false);
            Assert.assertFalse(manager.isEnableQueue(coordinator));
            GlobalVariable.setEnableQueryQueueLoad(true);
            Assert.assertTrue(manager.isEnableQueue(coordinator));
        }

        {
            // 2. Query for select.
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select * from lineitem");
            GlobalVariable.setEnableQueryQueueSelect(false);
            Assert.assertFalse(manager.isEnableQueue(coordinator));
            GlobalVariable.setEnableQueryQueueSelect(true);
            Assert.assertTrue(manager.isEnableQueue(coordinator));
        }

        {
            // 3. Query for statistic.
            connectContext.setStatisticsJob(true); // Mock statistics job.
            DefaultCoordinator coordinator = getSchedulerWithQueryId("select * from lineitem");
            GlobalVariable.setEnableQueryQueueStatistic(false);
            Assert.assertFalse(manager.isEnableQueue(coordinator));
            GlobalVariable.setEnableQueryQueueStatistic(true);
            Assert.assertTrue(manager.isEnableQueue(coordinator));
            connectContext.setStatisticsJob(false);
        }
    }

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
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());

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
        coords.forEach(coord -> Assert.assertEquals(Slot.State.REQUIRING, coord.getSlot().getState()));

        // 3. The coming queries exceed the query queue capacity.
        for (int i = 0; i < 10; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Assert.assertThrows("the number of pending queries exceeds capacity", UserException.class,
                    () -> manager.maybeWait(connectContext, coord));
        }

        // 4. Finish the first `concurrencyLimit` queries.
        runningCoords.forEach(DefaultCoordinator::onFinished);
        runningCoords.forEach(coord -> Assert.assertEquals(Slot.State.RELEASED, coord.getSlot().getState()));

        // 5. Each loop dequeues `concurrencyLimit` queries, and cancel one pending queries.
        List<DefaultCoordinator> resetCoords = coords;
        while (!resetCoords.isEmpty()) {
            final int numResetCoords = resetCoords.size();
            int expectedAllocatedCoords = Math.min(numResetCoords, concurrencyLimit);
            // 5.1 `concurrencyLimit` queries become allocated.
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> numResetCoords - expectedAllocatedCoords == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            List<DefaultCoordinator> allocatedCoords = coords.stream()
                    .filter(coord -> coord.getSlot().getState() == Slot.State.ALLOCATED)
                    .collect(Collectors.toList());
            Assert.assertEquals(expectedAllocatedCoords, allocatedCoords.size());

            // 5.2 Cancel one pending query.
            resetCoords = resetCoords.stream()
                    .filter(coord -> coord.getSlot().getState() != Slot.State.ALLOCATED)
                    .collect(Collectors.toList());
            resetCoords.forEach(coord -> Assert.assertEquals(Slot.State.REQUIRING, coord.getSlot().getState()));

            if (!resetCoords.isEmpty()) {
                queryIdToShouldThrow.put(resetCoords.get(0).getQueryId(), new UserException("Cancelled"));
                resetCoords.get(0).cancel();
                Assert.assertEquals(Slot.State.CANCELLED, resetCoords.get(0).getSlot().getState());
                resetCoords.remove(0);
            }

            // 4.3 Finish these new allocated queries.
            allocatedCoords.forEach(DefaultCoordinator::onFinished);
            allocatedCoords.forEach(coord -> Assert.assertEquals(Slot.State.RELEASED, coord.getSlot().getState()));
        }
    }

    @Test
    public void testGroupQueueNormal() throws Exception {
        final int concurrencyLimit = 3;
        final int numGroups = 4;
        final int numGroupPendingCoords = concurrencyLimit * 5 + 1;
        // Each group and non-group has `numGroupPendingCoords` coordinators.
        final int numPendingCoords = numGroupPendingCoords * (numGroups + 1);

        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueMaxQueuedQueries(numPendingCoords);

        TWorkGroup group0 = new TWorkGroup().setId(0L).setConcurrency_limit(concurrencyLimit - 1);
        TWorkGroup group1 = new TWorkGroup().setId(1L).setConcurrency_limit(0);
        TWorkGroup group2 = new TWorkGroup().setId(2L).setConcurrency_limit(concurrencyLimit);
        TWorkGroup group3 = new TWorkGroup().setId(3L).setConcurrency_limit(concurrencyLimit + 1);
        TWorkGroup nonGroup = new TWorkGroup().setId(Slot.ABSENT_GROUP_ID);
        List<TWorkGroup> groups = ImmutableList.of(group0, group1, group2, group3, nonGroup);

        // 1. Run `concurrencyLimit` non-group queries first, and they shouldn't be queued.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            mockResourceGroup(null);
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        // 2. Then run `numPendingCoordinators` queries, and they should be queued.
        List<DefaultCoordinator> coords = new ArrayList<>(numPendingCoords);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numGroupPendingCoords; i++) {
            for (TWorkGroup group : groups) {
                if (group.getId() == Slot.ABSENT_GROUP_ID) {
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
        coords.forEach(coord -> Assert.assertEquals(Slot.State.REQUIRING, coord.getSlot().getState()));
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
        runningCoords.forEach(coord -> Assert.assertEquals(Slot.State.RELEASED, coord.getSlot().getState()));

        // 5. Each loop dequeues `concurrencyLimit` queries, and cancel one pending queries.
        List<DefaultCoordinator> resetCoords = coords;
        while (!resetCoords.isEmpty()) {
            final int numResetCoords = resetCoords.size();
            int expectedAllocatedCoords = Math.min(numResetCoords, concurrencyLimit);
            // 5.1 `concurrencyLimit` queries become allocated.
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> numResetCoords - expectedAllocatedCoords == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            List<DefaultCoordinator> allocatedCoords = coords.stream()
                    .filter(coord -> coord.getSlot().getState() == Slot.State.ALLOCATED)
                    .collect(Collectors.toList());
            Assert.assertEquals(expectedAllocatedCoords, allocatedCoords.size());

            resetCoords = resetCoords.stream()
                    .filter(coord -> coord.getSlot().getState() != Slot.State.ALLOCATED)
                    .collect(Collectors.toList());

            // 5.2 Finish these new allocated queries.
            allocatedCoords.forEach(DefaultCoordinator::onFinished);
            allocatedCoords.forEach(coord -> Assert.assertEquals(Slot.State.RELEASED, coord.getSlot().getState()));
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
        TWorkGroup nonGroup = new TWorkGroup().setId(Slot.ABSENT_GROUP_ID);
        List<TWorkGroup> groups = ImmutableList.of(group0, group1, group2, group3, nonGroup);

        Map<TWorkGroup, List<DefaultCoordinator>> groupToCoords = groups.stream().collect(Collectors.toMap(
                Function.identity(),
                (group) -> new ArrayList<>()
        ));
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numGroupPendingCoords; i++) {
            for (TWorkGroup group : groups) {
                if (group.getId() == Slot.ABSENT_GROUP_ID) {
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
                    Awaitility.await().atMost(5, TimeUnit.SECONDS)
                            .until(() -> coords.stream().allMatch(coord -> coord.getSlot().getState() == Slot.State.ALLOCATED));
                    coords.forEach(DefaultCoordinator::onFinished);
                    groupToCoords.put(group, Collections.emptyList());
                    return;
                }

                // In each loop, each group allocates `limit` slots.
                int numAllocatedCoords = Math.min(limit, coords.size());
                Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                        coords.stream().filter(coord -> coord.getSlot().getState() == Slot.State.ALLOCATED).count() ==
                                numAllocatedCoords);

                List<DefaultCoordinator> allocatedCoords =
                        coords.stream().filter(coord -> coord.getSlot().getState() == Slot.State.ALLOCATED)
                                .collect(Collectors.toList());

                List<DefaultCoordinator> pendingCoords =
                        coords.stream().filter(coord -> coord.getSlot().getState() != Slot.State.ALLOCATED)
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
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());

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
        runningCoords.forEach(coordinator -> Assert.assertEquals(Slot.State.RELEASED, coordinator.getSlot().getState()));

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
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        // 2. The coming query is allocated slots, after the previous queries with allocated slot is expired.
        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        manager.maybeWait(connectContext, coord);

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());

        // 3. Finish this query.
        coord.onFinished();
        Assert.assertEquals(Slot.State.RELEASED, coord.getSlot().getState());
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
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());

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
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> 1 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        Assert.assertEquals(Slot.State.REQUIRING, coord.getSlot().getState());

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
        mockNodeMgr(FE3, 8888, FE2, 8888);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());

        // 3. Finish this query.
        coord.onFinished();
        Assert.assertEquals(Slot.State.RELEASED, coord.getSlot().getState());

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
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());

            runningCoords.add(coord);
        }

        {
            // 2. The coming query is pending.
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Thread thread = new Thread(() -> Assert.assertThrows("Cancelled", UserException.class,
                    () -> manager.maybeWait(connectContext, coord)));
            thread.start();
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> 1 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(Slot.State.REQUIRING, coord.getSlot().getState());

            // 3. Cancel this query, and failed to releaseSlot due to exception.
            mockFrontendService(new MockFrontendServiceClient() {
                @Override
                public TReleaseSlotResponse releaseSlot(TReleaseSlotRequest request) throws TException {
                    throw new TException("mocked-release-slot-exception");
                }
            });
            coord.cancel();
            mockFrontendService(new MockFrontendServiceClient());
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(Slot.State.CANCELLED, coord.getSlot().getState());
        }

        {
            // 2. The coming query is pending.
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Thread thread = new Thread(() -> Assert.assertThrows("Cancelled", UserException.class,
                    () -> manager.maybeWait(connectContext, coord)));
            thread.start();
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> 1 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(Slot.State.REQUIRING, coord.getSlot().getState());

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
            coord.cancel();
            mockFrontendService(new MockFrontendServiceClient());
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(Slot.State.CANCELLED, coord.getSlot().getState());
        }

        {
            // 2. The coming query is pending.
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            Thread thread = new Thread(() -> Assert.assertThrows("Cancelled", UserException.class,
                    () -> manager.maybeWait(connectContext, coord)));
            thread.start();
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> 1 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(Slot.State.REQUIRING, coord.getSlot().getState());

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
            coord.cancel();
            mockFrontendService(new MockFrontendServiceClient());
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(Slot.State.CANCELLED, coord.getSlot().getState());
        }

        // 4. Finish the first `concurrencyLimit` queries.
        runningCoords.forEach(DefaultCoordinator::onFinished);
        runningCoords.forEach(coord -> Assert.assertEquals(Slot.State.RELEASED, coord.getSlot().getState()));

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
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());

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
            Thread thread = new Thread(() -> Assert.assertThrows("Cancelled", UserException.class,
                    () -> manager.maybeWait(connectContext, coord)));
            thread.start();
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> 1 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
            Assert.assertEquals(Slot.State.REQUIRING, coord.getSlot().getState());

            // The slot should be removed after failing to `finishSlotRequirement`.
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().size() == concurrencyLimit - 1);

            coord.cancel();
        }

        {
            // 3. `finishSlotRequirement` for the coming query success.
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            mockFrontendService(new MockFrontendServiceClient());
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());
            runningCoords.add(coord);
        }

        // 4. Finish the first `concurrencyLimit` queries.
        runningCoords.forEach(DefaultCoordinator::onFinished);
        runningCoords.forEach(coord -> Assert.assertEquals(Slot.State.RELEASED, coord.getSlot().getState()));
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
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());
        }

        // 2. Then run `numPendingCoordinators` queries, and they should be queued.
        List<DefaultCoordinator> coords = new ArrayList<>(concurrencyLimit);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            coords.add(coord);
            threads.add(new Thread(() -> Assert.assertThrows("Cancelled", UserException.class,
                    () -> manager.maybeWait(connectContext, coord))));
        }
        threads.forEach(Thread::start);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> concurrencyLimit == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        coords.forEach(coord -> Assert.assertEquals(Slot.State.REQUIRING, coord.getSlot().getState()));

        // 3. The frontend of the allocated and pending slots becomes dead, the slots should be released.
        GlobalStateMgr.getCurrentState().getSlotManager().notifyFrontendDeadAsync(FE2);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());

        // 4. Then the next `concurrencyLimit` queries shouldn't be queued.
        List<DefaultCoordinator> coords2 = new ArrayList<>(concurrencyLimit);
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());
            coords2.add(coord);
        }
        coords2.forEach(DefaultCoordinator::onFinished);

        coords.forEach(DefaultCoordinator::cancel);
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
                Assert.assertEquals(Slot.State.ALLOCATED, coord.getSlot().getState());
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

        List<Backend> backends = ImmutableList.of(
                new Backend(0L, "be0-host", 8030),
                new Backend(1L, "be1-host", 8030),
                new Backend(2L, "be2-host", 8030)
        );
        List<ComputeNode> computeNodes = ImmutableList.of(
                new ComputeNode(3L, "cn3-host", 8030),
                new ComputeNode(4L, "cn4-host", 8030),
                new ComputeNode(5L, "cn5-host", 8030)
        );
        Stream.concat(backends.stream(), computeNodes.stream()).forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);
        computeNodes.forEach(GlobalStateMgr.getCurrentSystemInfo()::addComputeNode);

        // 1. Queries are queued, due to CPU usage exceeds cpuUsagePermilleLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 0, 10);

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
                .until(() -> coords.stream().allMatch(coord -> Slot.State.REQUIRING == coord.getSlot().getState()));

        // 2. Queries are not queued anymore, after CPU usage doesn't exceed cpuUsagePermilleLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 0, 1);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> Slot.State.ALLOCATED == coord.getSlot().getState()));
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

        List<Backend> backends = ImmutableList.of(
                new Backend(0L, "be0-host", 8030),
                new Backend(1L, "be1-host", 8030),
                new Backend(2L, "be2-host", 8030)
        );
        List<ComputeNode> computeNodes = ImmutableList.of(
                new ComputeNode(3L, "cn3-host", 8030),
                new ComputeNode(4L, "cn4-host", 8030),
                new ComputeNode(5L, "cn5-host", 8030)
        );
        Stream.concat(backends.stream(), computeNodes.stream()).forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);
        computeNodes.forEach(GlobalStateMgr.getCurrentSystemInfo()::addComputeNode);

        // 1. Queries are queued, due to mem usage exceeds memUsagePctLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0);

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
                .until(() -> coords.stream().allMatch(coord -> Slot.State.REQUIRING == coord.getSlot().getState()));

        // 2. Queries are not queued anymore, after mem usage doesn't exceed cpuUsagePermilleLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 0, 0);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> Slot.State.ALLOCATED == coord.getSlot().getState()));
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

        List<Backend> backends = ImmutableList.of(
                new Backend(0L, "be0-host", 8030),
                new Backend(1L, "be1-host", 8030),
                new Backend(2L, "be2-host", 8030)
        );
        List<ComputeNode> computeNodes = ImmutableList.of(
                new ComputeNode(3L, "cn3-host", 8030),
                new ComputeNode(4L, "cn4-host", 8030),
                new ComputeNode(5L, "cn5-host", 8030)
        );
        Stream.concat(backends.stream(), computeNodes.stream()).forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);
        computeNodes.forEach(GlobalStateMgr.getCurrentSystemInfo()::addComputeNode);

        // 1. Queries are queued, due to mem usage exceeds memUsagePctLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0);

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
                .until(() -> coords.stream().allMatch(coord -> Slot.State.REQUIRING == coord.getSlot().getState()));

        // 2. Queries are not queued anymore, because the overloaded BE doesn't report in resourceUsageIntervalMs.
        GlobalVariable.setQueryQueueResourceUsageIntervalMs(1000);
        Thread.sleep(2000);
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(1L, 0, 100, 0, 0);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> Slot.State.ALLOCATED == coord.getSlot().getState()));
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

        List<Backend> backends = ImmutableList.of(
                new Backend(0L, "be0-host", 8030),
                new Backend(1L, "be1-host", 8030),
                new Backend(2L, "be2-host", 8030)
        );
        List<ComputeNode> computeNodes = ImmutableList.of(
                new ComputeNode(3L, "cn3-host", 8030),
                new ComputeNode(4L, "cn4-host", 8030),
                new ComputeNode(5L, "cn5-host", 8030)
        );
        Stream.concat(backends.stream(), computeNodes.stream()).forEach(cn -> cn.setAlive(true));
        backends.forEach(GlobalStateMgr.getCurrentSystemInfo()::addBackend);
        computeNodes.forEach(GlobalStateMgr.getCurrentSystemInfo()::addComputeNode);

        // 1. Queries are queued, due to mem usage exceeds memUsagePctLimit.
        GlobalStateMgr.getCurrentSystemInfo().updateResourceUsage(0L, 0, 100, 30, 0);

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
                .until(() -> coords.stream().allMatch(coord -> Slot.State.REQUIRING == coord.getSlot().getState()));

        // 2. Queries are not queued anymore, because the overloaded BE becomes dead.
        backends.get(0).setAlive(false);
        GlobalStateMgr.getCurrentState().getResourceUsageMonitor().notifyBackendDead();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> coords.stream().allMatch(coord -> Slot.State.ALLOCATED == coord.getSlot().getState()));
        coords.forEach(DefaultCoordinator::onFinished);
    }

    private void mockNeedCheckQueue() {
        new Expectations(manager) {
            {
                manager.needCheckQueue((DefaultCoordinator) any);
                result = true;
            }
        };
    }

    private void mockNotNeedCheckQueue() {
        new Expectations(manager) {
            {
                manager.needCheckQueue((DefaultCoordinator) any);
                result = false;
            }
        };
    }

    private void mockEnableQueue() {
        new Expectations(manager) {
            {
                manager.isEnableQueue((DefaultCoordinator) any);
                result = true;
            }
        };
    }

    private void mockNotEnableCheckQueue() {
        new Expectations(manager) {
            {
                manager.isEnableQueue((DefaultCoordinator) any);
                result = false;
            }
        };
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
     * <p> Mock {@link CoordinatorPreprocessor#prepareResourceGroup(ConnectContext, ResourceGroupClassifier.QueryType)} and
     * {@link ResourceGroupMgr#getResourceGroup(long)}.
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

        if (group != null && group.getId() != Slot.ABSENT_GROUP_ID) {
            ResourceGroup resourceGroup = new ResourceGroup();
            resourceGroup.setId(group.getId());
            resourceGroup.setConcurrencyLimit(group.getConcurrency_limit());
            mockedGroups.put(group.getId(), resourceGroup);
            new MockUp<ResourceGroupMgr>() {
                @Mock
                public ResourceGroup getResourceGroup(long id) {
                    return mockedGroups.get(id);
                }
            };
        }
    }

    /**
     * Mock {@link NodeMgr} to make it return the specific RPC endpoint of self and leader.
     * The mocked methods including {@link NodeMgr#getLeaderIpAndRpcPort()} and {@link NodeMgr#getSelfIpAndRpcPort()}.
     */
    private static void mockNodeMgr(String leaderHost, Integer leaderPort, String selfHost, Integer selfPort) {
        new MockUp<NodeMgr>() {
            @Mock
            public Pair<String, Integer> getLeaderIpAndRpcPort() {
                return Pair.create(leaderHost, leaderPort);
            }

            @Mock
            public Pair<String, Integer> getSelfIpAndRpcPort() {
                return Pair.create(selfHost, selfPort);
            }
        };
    }

}
