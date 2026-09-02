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

import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GracefulExitFlag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectSchedulerTest {

    @BeforeEach
    public void setUp() throws Exception {
        resetGracefulExitFlag();
    }

    @AfterEach
    public void tearDown() throws Exception {
        resetGracefulExitFlag();
    }

    private static void resetGracefulExitFlag() throws Exception {
        Field flagField = GracefulExitFlag.class.getDeclaredField("GRACEFUL_EXIT");
        flagField.setAccessible(true);
        ((AtomicBoolean) flagField.get(null)).set(false);
        Field beginField = GracefulExitFlag.class.getDeclaredField("BEGIN_NANO");
        beginField.setAccessible(true);
        ((AtomicLong) beginField.get(null)).set(0L);
    }

    // Simulate that the graceful-exit drain window (accept-new window + min wait) has elapsed.
    private static void markDrainWindowElapsed() throws Exception {
        GracefulExitFlag.markGracefulExit();
        long windowNanos = TimeUnit.MILLISECONDS.toNanos(Config.graceful_exit_accept_new_window_ms);
        long minNanos = TimeUnit.SECONDS.toNanos(Config.min_graceful_exit_time_second);
        Field beginField = GracefulExitFlag.class.getDeclaredField("BEGIN_NANO");
        beginField.setAccessible(true);
        ((AtomicLong) beginField.get(null)).set(System.nanoTime() - windowNanos - minNanos - 1L);
    }

    // Real ConnectContext subclass backed by overridden collaborators, so closeAllIdleConnection
    // exercises the real inActiveExplicitTransaction/isIdleLastFor dispatch without network access.
    private static class TestContext extends ConnectContext {
        private final boolean inExplicitTxn;
        private final boolean idle;
        private final boolean pendingTasks;
        private final Runnable cleanupAction;

        TestContext(long connectionId, boolean inExplicitTxn, boolean idle, Runnable cleanupAction) {
            this(connectionId, inExplicitTxn, idle, false, cleanupAction);
        }

        TestContext(long connectionId, boolean inExplicitTxn, boolean idle, boolean pendingTasks,
                    Runnable cleanupAction) {
            this.inExplicitTxn = inExplicitTxn;
            this.idle = idle;
            this.pendingTasks = pendingTasks;
            this.cleanupAction = cleanupAction;
            setConnectionId((int) connectionId);
        }

        @Override
        public boolean inActiveExplicitTransaction() {
            return inExplicitTxn;
        }

        @Override
        public boolean isIdleLastFor(long milliSeconds) {
            return idle;
        }

        @Override
        public boolean hasPendingTasks() {
            return pendingTasks;
        }

        @Override
        public synchronized void cleanup() {
            if (cleanupAction != null) {
                cleanupAction.run();
            }
        }
    }

    @Test
    public void testCloseAllIdleConnectionCleansOnlyIdleNonTxnContexts() {
        ConnectScheduler scheduler = new ConnectScheduler(10);
        Map<Long, ConnectContext> connectionMap = Deencapsulation.getField(scheduler, "connectionMap");

        int[] cleanups = {0};
        connectionMap.put(1L, new TestContext(1L, false, true, () -> cleanups[0]++));
        connectionMap.put(2L, new TestContext(2L, false, false, () -> cleanups[0]++));
        connectionMap.put(3L, new TestContext(3L, true, true, () -> cleanups[0]++));

        scheduler.closeAllIdleConnection();

        // Only the idle, non-transaction context must be cleaned up. Connections with an active
        // explicit transaction are skipped while the drain window is still open so their
        // ExplicitTxnState is not stranded. Busy (non-idle) connections are also kept.
        Assertions.assertEquals(1, cleanups[0]);
    }

    @Test
    public void testCloseAllIdleConnectionCleansIdleExplicitTxnAfterDrainWindow() throws Exception {
        // Once the graceful-exit drain window has elapsed, an idle connection holding an explicit
        // transaction must also be cleaned up (disconnecting rolls the txn back) so totalConns can
        // reach 0 and graceful shutdown finishes instead of hitting the hard timeout.
        markDrainWindowElapsed();

        ConnectScheduler scheduler = new ConnectScheduler(10);
        Map<Long, ConnectContext> connectionMap = Deencapsulation.getField(scheduler, "connectionMap");

        int[] cleanups = {0};
        connectionMap.put(1L, new TestContext(1L, true, true, () -> cleanups[0]++));
        connectionMap.put(2L, new TestContext(2L, true, false, () -> cleanups[0]++));

        scheduler.closeAllIdleConnection();

        Assertions.assertEquals(1, cleanups[0]);
    }

    @Test
    public void testCloseAllIdleConnectionSkipsContextWithPendingTasks() {
        ConnectScheduler scheduler = new ConnectScheduler(10);
        Map<Long, ConnectContext> connectionMap = Deencapsulation.getField(scheduler, "connectionMap");

        int[] cleanups = {0};
        connectionMap.put(1L, new TestContext(1L, false, true, true, () -> cleanups[0]++));

        scheduler.closeAllIdleConnection();

        // A queued packet has already been admitted to a worker but has not yet reached dispatch;
        // its context still looks idle and must not be cleaned up.
        Assertions.assertEquals(0, cleanups[0]);
    }

    @Test
    public void testIsDrainedRequiresAcceptWindowElapsedAndEmptyMap() throws Exception {
        ConnectScheduler scheduler = new ConnectScheduler(10);
        Assertions.assertFalse(scheduler.isDrained());

        markDrainWindowElapsed();
        Assertions.assertTrue(scheduler.isDrained());

        Map<Long, ConnectContext> connectionMap = Deencapsulation.getField(scheduler, "connectionMap");
        connectionMap.put(1L, new TestContext(1L, false, false, () -> { }));
        Assertions.assertFalse(scheduler.isDrained());
    }

    @Test
    public void testCloseAllIdleConnectionSkipsEmptyMap() {
        ConnectScheduler scheduler = new ConnectScheduler(10);
        // Must not throw when there are no connections registered.
        scheduler.closeAllIdleConnection();
    }
    @Test
    public void testCloseAllIdleConnectionSkipsContextThatBecameActiveAfterSelection() {
        ConnectScheduler scheduler = new ConnectScheduler(10);
        Map<Long, ConnectContext> connectionMap = Deencapsulation.getField(scheduler, "connectionMap");

        int[] cleanups = {0};
        // The lock-phase check reports idle so the connection is selected as a candidate, but the
        // pre-cleanup recheck reports busy, simulating a new statement arriving after the lock was
        // released (TOCTOU). Such a connection must not be cleaned up mid-statement.
        connectionMap.put(1L, new TestContext(1L, false, true, () -> cleanups[0]++) {
            private boolean firstCheck = true;

            @Override
            public boolean isIdleLastFor(long milliSeconds) {
                if (firstCheck) {
                    firstCheck = false;
                    return true;
                }
                return false;
            }
        });

        scheduler.closeAllIdleConnection();

        Assertions.assertEquals(0, cleanups[0]);
    }
}
