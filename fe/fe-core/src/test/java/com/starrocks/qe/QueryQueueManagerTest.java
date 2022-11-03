// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.UserException;
import com.starrocks.planner.ExportSink;
import com.starrocks.planner.MysqlTableSink;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResultSinkType;
import mockit.Expectations;
import mockit.Mocked;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class QueryQueueManagerTest {
    @Mocked
    private Coordinator coordinator;

    private boolean taskQueueEnable;
    private int taskQueueConcurrencyHardLimit;
    private double taskQueueMemUsedPctHardLimit;
    private int taskQueuePendingTimeoutSecond;
    private int taskQueueMaxQueuedQueries;

    @Before
    public void before() {
        taskQueueEnable = GlobalVariable.isQueryQueueSelectEnable();
        taskQueueConcurrencyHardLimit = GlobalVariable.getQueryQueueConcurrencyHardLimit();
        taskQueueMemUsedPctHardLimit = GlobalVariable.getQueryQueueMemUsedPctHardLimit();
        taskQueuePendingTimeoutSecond = GlobalVariable.getQueryQueuePendingTimeoutSecond();
        taskQueueMaxQueuedQueries = GlobalVariable.getQueryQueueMaxQueuedQueries();
    }

    @After
    public void after() {
        // Reset query queue configs.
        GlobalVariable.setQueryQueueSelectEnable(taskQueueEnable);
        GlobalVariable.setQueryQueueConcurrencyHardLimit(taskQueueConcurrencyHardLimit);
        GlobalVariable.setQueryQueueMemUsedPctHardLimit(taskQueueMemUsedPctHardLimit);
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
                manager.needCheckQueue((Coordinator) any);
                result = true;
            }
        };
    }

    private void mockCoordinatorNotNeedCheckQueue() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        new Expectations(manager) {
            {
                manager.needCheckQueue((Coordinator) any);
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
        GlobalVariable.setQueryQueueSelectEnable(true);
        manager.maybeWait(connectCtx, coordinator);
        Assert.assertEquals(0, connectCtx.getAuditEventBuilder().build().pendingTimeMs);
        Assert.assertEquals(0, manager.numPendingQueries());

        // Case 2: Coordinator need check queue but resource isn't overloaded.
        mockCoordinatorNeedCheckQueue();
        GlobalVariable.setQueryQueueSelectEnable(true);
        mockCanRunMore();
        manager.maybeWait(connectCtx, coordinator);
        Assert.assertEquals(0, connectCtx.getAuditEventBuilder().build().pendingTimeMs);
        Assert.assertEquals(0, manager.numPendingQueries());

    }

    @Test
    public void testWait() throws InterruptedException {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        ConnectContext connectCtx1 = new ConnectContext();
        ConnectContext connectCtx2 = new ConnectContext();

        mockCoordinatorNeedCheckQueue();
        mockNotCanRunMore();

        // Case 1: Pending timeout.
        GlobalVariable.setQueryQueueSelectEnable(true);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(1);
        Thread thread = new Thread(() -> {
            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> {
                Assert.assertThrows("Pending timeout", UserException.class, () -> manager.maybeWait(connectCtx1, coordinator));
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
        SystemInfoService service = GlobalStateMgr.getCurrentSystemInfo();
        Backend be1 = new Backend();
        be1.setAlive(true);
        be1.setId(1);
        Backend be2 = new Backend();
        be2.setAlive(true);
        be2.setId(2);
        new Expectations(service) {
            {
                service.getBackends();
                result = ImmutableList.of(be1, be2);
            }
            {
                service.getBackend(be2.getId());
                result = be2;
            }
        };

        // Case 1: enable query_queue, but min_concurrency and min_mem_used_pct not effective.
        GlobalVariable.setQueryQueueConcurrencyHardLimit(0);
        GlobalVariable.setQueryQueueMemUsedPctHardLimit(0);
        GlobalVariable.setQueryQueueCpuUsedPermilleHardLimit(0);
        Assert.assertTrue(manager.canRunMore());

        GlobalVariable.setQueryQueueConcurrencyHardLimit(3);
        GlobalVariable.setQueryQueueMemUsedPctHardLimit(0.3);
        GlobalVariable.setQueryQueueCpuUsedPermilleHardLimit(400);
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
    public void testCoordinatorNeedCheckQueue(@Mocked SchemaScanNode schemaScanNode,
                                              @Mocked OlapScanNode olapScanNode,
                                              @Mocked PlanFragment fragment,
                                              @Mocked OlapTableSink olapTableSink,
                                              @Mocked MysqlTableSink mysqlTableSink,
                                              @Mocked ExportSink exportSink) {
        QueryQueueManager manager = QueryQueueManager.getInstance();

        // 1. Load type.
        new Expectations(coordinator) {
            {
                coordinator.isLoadType();
                result = true;
            }
        };
        Assert.assertFalse(manager.needCheckQueue(coordinator));

        // 2. ScanNodes is empty.
        List<ScanNode> scanNodes = new ArrayList<>();
        new Expectations(coordinator) {
            {
                coordinator.isLoadType();
                result = false;
            }

            {
                coordinator.getScanNodes();
                result = scanNodes;
            }
        };
        Assert.assertFalse(manager.needCheckQueue(coordinator));

        // 3. ScanNodes only contain SchemaNode.
        scanNodes.add(schemaScanNode);
        Assert.assertFalse(manager.needCheckQueue(coordinator));

        // 4. Insert to MySQL table.
        scanNodes.add(olapScanNode);
        new Expectations(coordinator, fragment) {
            {
                coordinator.getFragments();
                result = ImmutableList.of(fragment);
            }
            {
                fragment.getSink();
                result = mysqlTableSink;
            }
        };
        GlobalVariable.setQueryQueueInsertEnable(false);
        Assert.assertFalse(manager.needCheckQueue(coordinator));
        GlobalVariable.setQueryQueueInsertEnable(true);
        Assert.assertTrue(manager.needCheckQueue(coordinator));

        // 5. Insert to OLAP table.
        new Expectations(coordinator, fragment) {
            {
                fragment.getSink();
                result = olapTableSink;
            }
        };
        GlobalVariable.setQueryQueueInsertEnable(false);
        Assert.assertFalse(manager.needCheckQueue(coordinator));
        GlobalVariable.setQueryQueueInsertEnable(true);
        Assert.assertTrue(manager.needCheckQueue(coordinator));

        // 6. Query for select.
        PlanNodeId nodeId = new PlanNodeId(0);
        ResultSink queryResultSink = new ResultSink(nodeId, TResultSinkType.MYSQL_PROTOCAL);
        new Expectations(coordinator, fragment) {
            {
                fragment.getSink();
                result = queryResultSink;
            }
        };
        GlobalVariable.setQueryQueueSelectEnable(false);
        Assert.assertFalse(manager.needCheckQueue(coordinator));
        GlobalVariable.setQueryQueueSelectEnable(true);
        Assert.assertTrue(manager.needCheckQueue(coordinator));

        // 7. Query for statistic.
        ResultSink statisticResultSink = new ResultSink(nodeId, TResultSinkType.STATISTIC);
        new Expectations(coordinator, fragment) {
            {
                fragment.getSink();
                result = statisticResultSink;
            }
        };
        GlobalVariable.setQueryQueueStatisticEnable(false);
        Assert.assertFalse(manager.needCheckQueue(coordinator));
        GlobalVariable.setQueryQueueStatisticEnable(true);
        Assert.assertTrue(manager.needCheckQueue(coordinator));

        // 8. Query for outfile.
        ResultSink fileResultSink = new ResultSink(nodeId, TResultSinkType.FILE);
        new Expectations(coordinator, fragment) {
            {
                fragment.getSink();
                result = fileResultSink;
            }
        };
        Assert.assertFalse(manager.needCheckQueue(coordinator));

        // 9. Export.
        new Expectations(coordinator, fragment) {
            {
                fragment.getSink();
                result = exportSink;
            }
        };
        Assert.assertFalse(manager.needCheckQueue(coordinator));
    }
}
