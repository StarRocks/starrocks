// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License..

package com.starrocks.qe.scheduler.warehouse;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.scheduler.QueryQueueManagerTest;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetWarehouseQueriesRequest;
import com.starrocks.thrift.TGetWarehouseQueriesResponse;
import com.starrocks.thrift.TWorkGroup;
import mockit.Mock;
import mockit.MockUp;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class WarehouseQueryQueueMetricsTest extends SchedulerTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        SchedulerTestBase.beforeClass();
        MetricRepo.init();
    }

    @Before
    public void before() {
        GlobalVariable.setEnableGroupLevelQueryQueue(true);

        GlobalVariable.setQueryQueuePendingTimeoutSecond(Config.max_load_timeout_second);
        connectContext.getSessionVariable().setQueryTimeoutS(Config.max_load_timeout_second);

        mockFrontends(FRONTENDS);

        mockFrontendService(new QueryQueueManagerTest.MockFrontendServiceClient());

        SlotManager slotManager = new SlotManager(GlobalStateMgr.getCurrentState().getResourceUsageMonitor());
        slotManager.start();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public BaseSlotManager getSlotManager() {
                return slotManager;
            }
        };

        MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(-MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        connectContext.setStartTime();
    }

    @After
    public void after() {
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());
    }

    @Test
    public void testBuildTGetWarehouseQueriesResponse() throws Exception {
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

                threads.add(new Thread(() -> Assert.assertThrows("Cancelled", StarRocksException.class,
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
            TGetWarehouseQueriesRequest req = new TGetWarehouseQueriesRequest();
            TGetWarehouseQueriesResponse res = WarehouseQueryQueueMetrics.build(req);
            assertThat(res.queries.size()).isEqualTo(numPendingCoords + concurrencyLimit);
        }

        coords.forEach(coor -> coor.cancel("Cancel by test"));
        runningCoords.forEach(DefaultCoordinator::onFinished);
    }
}
