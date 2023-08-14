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


package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.UserException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.planner.ExportSink;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResultSinkType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class QueryQueueManagerTest {
    @Mocked
    private DefaultCoordinator coordinator;

    private boolean taskQueueEnable;
    private int taskQueueConcurrencyHardLimit;
    private double taskQueueMemUsedPctHardLimit;
    private int taskQueuePendingTimeoutSecond;
    private int taskQueueMaxQueuedQueries;

    @BeforeClass
    public static void beforeClass() {
        MetricRepo.init();
    }

    @Before
    public void before() {
        taskQueueEnable = GlobalVariable.isEnableQueryQueueSelect();
        taskQueueConcurrencyHardLimit = GlobalVariable.getQueryQueueConcurrencyLimit();
        taskQueueMemUsedPctHardLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        taskQueuePendingTimeoutSecond = GlobalVariable.getQueryQueuePendingTimeoutSecond();
        taskQueueMaxQueuedQueries = GlobalVariable.getQueryQueueMaxQueuedQueries();
    }

    @After
    public void after() {
        // Reset query queue configs.
        GlobalVariable.setEnableQueryQueueSelect(taskQueueEnable);
        GlobalVariable.setQueryQueueConcurrencyLimit(taskQueueConcurrencyHardLimit);
        GlobalVariable.setQueryQueueMemUsedPctLimit(taskQueueMemUsedPctHardLimit);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(taskQueuePendingTimeoutSecond);
        GlobalVariable.setQueryQueueMaxQueuedQueries(taskQueueMaxQueuedQueries);
    }

    private void mockNotCanRunMore() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        new Expectations(manager) {
            {
                manager.canRunMore();
                result = false;
            }
        };
    }

    private void mockCanRunMore() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        new Expectations(manager) {
            {
                manager.canRunMore();
                result = true;
            }
        };
    }

    private void mockCoordinatorNeedCheckQueue() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        new Expectations(manager) {
            {
                manager.needCheckQueue((DefaultCoordinator) any);
                result = true;
            }
        };
    }

    private void mockCoordinatorNotNeedCheckQueue() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        new Expectations(manager) {
            {
                manager.needCheckQueue((DefaultCoordinator) any);
                result = false;
            }
        };
    }

    private void mockCoordinatorEnableCheckQueue() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        new Expectations(manager) {
            {
                manager.enableCheckQueue((DefaultCoordinator) any);
                result = true;
            }
        };
    }

    private void mockCoordinatorNotEnableCheckQueue() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        new Expectations(manager) {
            {
                manager.enableCheckQueue((DefaultCoordinator) any);
                result = false;
            }
        };
    }

    @Test
    public void testNotWait() throws UserException, InterruptedException {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        ConnectContext connectCtx = new ConnectContext();

        // Case 1: Coordinator needn't check queue.
        mockCoordinatorNotNeedCheckQueue();
        mockCoordinatorEnableCheckQueue();
        GlobalVariable.setEnableQueryQueueSelect(true);
        manager.maybeWait(connectCtx, coordinator);
        Assert.assertEquals(-1, connectCtx.getAuditEventBuilder().build().pendingTimeMs);
        Assert.assertEquals(0, manager.numPendingQueries());

        // Case 2: Coordinator doesn't enable to check queue.
        mockCoordinatorNeedCheckQueue();
        mockCoordinatorNotEnableCheckQueue();
        GlobalVariable.setEnableQueryQueueSelect(true);
        manager.maybeWait(connectCtx, coordinator);
        Assert.assertEquals(-1, connectCtx.getAuditEventBuilder().build().pendingTimeMs);
        Assert.assertEquals(0, manager.numPendingQueries());

        // Case 3: Coordinator need check queue but resource isn't overloaded.
        mockCoordinatorNeedCheckQueue();
        mockCoordinatorEnableCheckQueue();
        mockCanRunMore();
        manager.maybeWait(connectCtx, coordinator);
        Assert.assertEquals(-1, connectCtx.getAuditEventBuilder().build().pendingTimeMs);
        Assert.assertEquals(0, manager.numPendingQueries());

    }

    @Test
    public void testWait() throws InterruptedException {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        ConnectContext connectCtx1 = new ConnectContext();
        ConnectContext connectCtx2 = new ConnectContext();

        mockCoordinatorNeedCheckQueue();
        mockCoordinatorEnableCheckQueue();
        mockNotCanRunMore();

        // Case 1: Pending timeout.
        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(1);
        Thread thread = new Thread(() -> {
            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> {
                Assert.assertThrows("Pending timeout", UserException.class,
                        () -> manager.maybeWait(connectCtx1, coordinator));
                return true;
            });
            Assert.assertEquals(0, manager.numPendingQueries());
        });
        thread.start();
        thread.join();

        GlobalVariable.setQueryQueuePendingTimeoutSecond(300);
        GlobalVariable.setQueryQueueMaxQueuedQueries(1);
        thread = new Thread(() -> {
            // Case 3: Cancel the query, when it is pending.
            Assert.assertThrows("Cancelled", UserException.class, () -> manager.maybeWait(connectCtx1, coordinator));
            Assert.assertEquals(0, manager.numPendingQueries());
        });
        thread.start();

        // Case 2: exceed query queue capacity.
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(connectCtx1::isPending);
        Assert.assertThrows("Need pend but exceed query queue capacity", UserException.class,
                () -> manager.maybeWait(connectCtx2, coordinator));

        // Case 3: Cancel the query, when it is pending.
        manager.cancelQuery(connectCtx1);
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> !connectCtx1.isPending());

        // Case 4: wait first, and then wakeup and don't wait anymore after resource isn't overloaded.
        thread = new Thread(() -> {
            try {
                manager.maybeWait(connectCtx1, coordinator);
                Assert.assertEquals(0, manager.numPendingQueries());
            } catch (UserException | InterruptedException e) {
                Assert.fail("Unexpected exception");
            }
        });
        thread.start();
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(connectCtx1::isPending);
        mockCanRunMore();
        manager.maybeNotify();
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> !connectCtx1.isPending());
        // noneffective.
        manager.maybeNotify();
    }

    @Test
    public void testCanRunMore() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        ComputeNode be1 = new Backend();
        be1.setAlive(true);
        be1.setId(1);
        ComputeNode be2 = new ComputeNode();
        be2.setAlive(true);
        be2.setId(2);

        new MockUp<SystemInfoService>() {
            @Mock
            public Stream<ComputeNode> backendAndComputeNodeStream() {
                return ImmutableList.of(be1, be2).stream();
            }

            @Mock
            public ComputeNode getBackendOrComputeNode(long idx) {
                if (idx == be2.getId()) {
                    return be2;
                }
                if (idx == be1.getId()) {
                    return be1;
                }
                return null;
            }
        };

        // Case 1: enable query_queue, but min_concurrency and min_mem_used_pct not effective.
        GlobalVariable.setQueryQueueConcurrencyLimit(0);
        GlobalVariable.setQueryQueueMemUsedPctLimit(0);
        GlobalVariable.setQueryQueueCpuUsedPermilleLimit(0);
        Assert.assertTrue(manager.canRunMore());

        GlobalVariable.setQueryQueueConcurrencyLimit(3);
        GlobalVariable.setQueryQueueMemUsedPctLimit(0.3);
        GlobalVariable.setQueryQueueCpuUsedPermilleLimit(400);
        // Case 2: exceed concurrency threshold.
        manager.updateResourceUsage(be2.getId(), 3, 10, 0, 200);
        Assert.assertFalse(manager.canRunMore());

        // Case 3: exceed memory threshold.
        manager.updateResourceUsage(be2.getId(), 0, 10, 4, 200);
        Assert.assertFalse(manager.canRunMore());

        // Case 4: exceed CPU threshold.
        manager.updateResourceUsage(be2.getId(), 0, 10, 2, 500);
        Assert.assertFalse(manager.canRunMore());

        // Case 5: BE isn't alive after resource overload.
        be2.setAlive(false);
        Assert.assertTrue(manager.canRunMore());

        // Case 6: don't exceed concurrency and memory threshold.
        be2.setAlive(true);
        manager.updateResourceUsage(be2.getId(), 2, 10, 2, 200);
        Assert.assertTrue(manager.canRunMore());
    }

    @Test
    public void testNeedCheckQueue(@Mocked SchemaScanNode schemaScanNode,
                                   @Mocked OlapScanNode olapScanNode) {
        QueryQueueManager manager = QueryQueueManager.getInstance();

        // 1. ScanNodes is empty.
        List<ScanNode> scanNodes = new ArrayList<>();
        new Expectations(coordinator) {
            {
                coordinator.getScanNodes();
                result = scanNodes;
            }
        };
        Assert.assertFalse(manager.needCheckQueue(coordinator));

        // 2. ScanNodes only contain SchemaNode.
        scanNodes.add(schemaScanNode);
        Assert.assertFalse(manager.needCheckQueue(coordinator));

        // 3. ScanNodes contain non-SchemaNode.
        scanNodes.add(olapScanNode);
        Assert.assertTrue(manager.needCheckQueue(coordinator));
    }

    @Test
    public void testEnableCheckQueue(@Mocked PlanFragment fragment,
                                     @Mocked ExportSink exportSink) {
        QueryQueueManager manager = QueryQueueManager.getInstance();

        // 1. Load type.
        new Expectations(coordinator) {
            {
                coordinator.isLoadType();
                result = true;
            }
        };
        GlobalVariable.setEnableQueryQueueLoad(false);
        Assert.assertFalse(manager.enableCheckQueue(coordinator));
        GlobalVariable.setEnableQueryQueueLoad(true);
        Assert.assertTrue(manager.enableCheckQueue(coordinator));
        new Expectations(coordinator) {
            {
                coordinator.isLoadType();
                result = false;
            }
        };

        // 2. Query for select.
        new Expectations(coordinator, fragment) {
            {
                coordinator.getFragments();
                result = ImmutableList.of(fragment);
            }
        };
        PlanNodeId nodeId = new PlanNodeId(0);
        ResultSink queryResultSink = new ResultSink(nodeId, TResultSinkType.MYSQL_PROTOCAL);
        new Expectations(coordinator, fragment) {
            {
                fragment.getSink();
                result = queryResultSink;
            }
        };
        GlobalVariable.setEnableQueryQueueSelect(false);
        Assert.assertFalse(manager.enableCheckQueue(coordinator));
        GlobalVariable.setEnableQueryQueueSelect(true);
        Assert.assertTrue(manager.enableCheckQueue(coordinator));

        // 3. Query for statistic.
        ResultSink statisticResultSink = new ResultSink(nodeId, TResultSinkType.STATISTIC);
        new Expectations(coordinator, fragment) {
            {
                fragment.getSink();
                result = statisticResultSink;
            }
        };
        GlobalVariable.setEnableQueryQueueStatistic(false);
        Assert.assertFalse(manager.enableCheckQueue(coordinator));
        GlobalVariable.setEnableQueryQueueStatistic(true);
        Assert.assertTrue(manager.enableCheckQueue(coordinator));

        // 4. Query for outfile.
        ResultSink fileResultSink = new ResultSink(nodeId, TResultSinkType.FILE);
        new Expectations(coordinator, fragment) {
            {
                fragment.getSink();
                result = fileResultSink;
            }
        };
        Assert.assertFalse(manager.enableCheckQueue(coordinator));

        // 5. Export.
        new Expectations(coordinator, fragment) {
            {
                fragment.getSink();
                result = exportSink;
            }
        };
        Assert.assertFalse(manager.enableCheckQueue(coordinator));
    }
}
