// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.UserException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class QueryQueueManagerTest {

    private boolean queryQueueEnable;
    private int queryQueueConcurrencyHardLimit;
    private double queryQueueMemUsedPctHardLimit;
    private int queryQueuePendingTimeoutSecond;
    private int queryQueueMaxQueuedQueries;

    @Before
    public void before() {
        queryQueueEnable = GlobalVariable.isQueryQueueEnable();
        queryQueueConcurrencyHardLimit = GlobalVariable.getQueryQueueConcurrencyHardLimit();
        queryQueueMemUsedPctHardLimit = GlobalVariable.getQueryQueueMemUsedPctHardLimit();
        queryQueuePendingTimeoutSecond = GlobalVariable.getQueryQueuePendingTimeoutSecond();
        queryQueueMaxQueuedQueries = GlobalVariable.getQueryQueueMaxQueuedQueries();
    }

    @After
    public void after() {
        // Reset query queue configs.
        GlobalVariable.setQueryQueueEnable(queryQueueEnable);
        GlobalVariable.setQueryQueueConcurrencyHardLimit(queryQueueConcurrencyHardLimit);
        GlobalVariable.setQueryQueueMemUsedPctHardLimit(queryQueueMemUsedPctHardLimit);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(queryQueuePendingTimeoutSecond);
        GlobalVariable.setQueryQueueMaxQueuedQueries(queryQueueMaxQueuedQueries);
    }

    private void mockResourceOverloaded() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        new Expectations(manager) {
            {
                manager.needWait();
                result = true;
            }
        };
    }

    private void mockResourceNotOverloaded() {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        new Expectations(manager) {
            {
                manager.needWait();
                result = false;
            }
        };
    }

    @Test
    public void testNotWait() throws UserException, InterruptedException {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        ConnectContext connectCtx = new ConnectContext();

        // Case 1: Disable query_queue.
        GlobalVariable.setQueryQueueEnable(false);
        manager.maybeWait(connectCtx);
        Assert.assertEquals(0, connectCtx.getAuditEventBuilder().build().pendingTimeMs);
        Assert.assertEquals(0, manager.numPendingQueries());

        // Case 2: Enable query_queue but resource isn't overloaded.
        GlobalVariable.setQueryQueueEnable(true);
        mockResourceNotOverloaded();
        manager.maybeWait(connectCtx);
        Assert.assertEquals(0, connectCtx.getAuditEventBuilder().build().pendingTimeMs);
        Assert.assertEquals(0, manager.numPendingQueries());
    }

    @Test
    public void testWait() throws InterruptedException {
        QueryQueueManager manager = QueryQueueManager.getInstance();
        ConnectContext connectCtx1 = new ConnectContext();
        ConnectContext connectCtx2 = new ConnectContext();

        mockResourceOverloaded();

        // Case 1: Pending timeout.
        GlobalVariable.setQueryQueueEnable(true);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(1);
        Thread thread = new Thread(() -> {
            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> {
                Assert.assertThrows("Pending timeout", UserException.class, () -> manager.maybeWait(connectCtx1));
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
            Assert.assertThrows("Cancelled", UserException.class, () -> manager.maybeWait(connectCtx1));
            Assert.assertEquals(0, manager.numPendingQueries());
        });
        thread.start();

        // Case 2: exceed query queue capacity.
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(connectCtx1::isPending);
        Assert.assertThrows("Need pend but exceed query queue capacity", UserException.class,
                () -> manager.maybeWait(connectCtx2));

        // Case 3: Cancel the query, when it is pending.
        manager.cancelQuery(connectCtx1);
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> !connectCtx1.isPending());

        // Case 4: wait first, and then wakeup and don't wait anymore after resource isn't overloaded.
        thread = new Thread(() -> {
            try {
                manager.maybeWait(connectCtx1);
                Assert.assertEquals(0, manager.numPendingQueries());
            } catch (UserException | InterruptedException e) {
                Assert.fail("Unexpected exception");
            }
        });
        thread.start();
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(connectCtx1::isPending);
        mockResourceNotOverloaded();
        manager.maybeNotify();
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> !connectCtx1.isPending());
        // noneffective.
        manager.maybeNotify();
    }

    @Test
    public void testNeedWait() {
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

        // Case 1: disable query_queue.
        GlobalVariable.setQueryQueueEnable(false);
        Assert.assertFalse(manager.needWait());

        // Case 2: enable query_queue, but min_concurrency and min_mem_used_pct not effective.
        GlobalVariable.setQueryQueueEnable(true);
        GlobalVariable.setQueryQueueConcurrencyHardLimit(0);
        GlobalVariable.setQueryQueueMemUsedPctHardLimit(0);
        Assert.assertFalse(manager.needWait());

        GlobalVariable.setQueryQueueConcurrencyHardLimit(3);
        GlobalVariable.setQueryQueueMemUsedPctHardLimit(0.3);
        // Case 3: exceed concurrency threshold.
        manager.updateResourceUsage(be2.getId(), 3, 10, 0);
        Assert.assertTrue(manager.needWait());

        // Case 4: exceed memory threshold.
        manager.updateResourceUsage(be2.getId(), 0, 10, 4);
        Assert.assertTrue(manager.needWait());

        // Case 5: BE isn't alive after resource overload.
        be2.setAlive(false);
        Assert.assertFalse(manager.needWait());

        // Case 6: don't exceed concurrency and memory threshold.
        be2.setAlive(true);
        manager.updateResourceUsage(be2.getId(), 2, 10, 2);
        Assert.assertFalse(manager.needWait());
    }
}
