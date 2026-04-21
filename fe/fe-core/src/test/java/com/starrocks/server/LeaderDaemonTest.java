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

// Placed in com.starrocks.server to reach the package-private leader-activation/demotion entrypoints
// on GlobalStateMgr used by this test.
package com.starrocks.server;

import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.ha.FrontendNodeType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LeaderDaemonTest {

    /**
     * Always-ready GlobalStateMgr for tests so the daemon loop does not block on isReady().
     * All leader-lease state uses the real superclass logic.
     */
    private static class TestGlobalStateMgr extends GlobalStateMgr {
        TestGlobalStateMgr() {
            super(new NodeMgr());
        }

        @Override
        public boolean isReady() {
            return true;
        }
    }

    private static TestGlobalStateMgr activeLeader() {
        TestGlobalStateMgr gsm = new TestGlobalStateMgr();
        gsm.beginLeaderActivation();
        gsm.setFrontendNodeType(FrontendNodeType.LEADER);
        gsm.publishLeaderLease(42L);
        return gsm;
    }

    private static class CountingDaemon extends LeaderDaemon {
        final GlobalStateMgr gsm;
        final AtomicInteger cycles = new AtomicInteger();
        final AtomicBoolean beforeStopCalled = new AtomicBoolean();
        final AtomicBoolean afterStopCalled = new AtomicBoolean();
        final CountDownLatch firstCycle = new CountDownLatch(1);

        CountingDaemon(String name, long intervalMs, GlobalStateMgr gsm) {
            super(name, intervalMs);
            this.gsm = gsm;
        }

        @Override
        protected GlobalStateMgr getGlobalStateMgr() {
            return gsm;
        }

        @Override
        protected void runAfterLeaseValid() throws InterruptedException {
            cycles.incrementAndGet();
            firstCycle.countDown();
        }

        @Override
        protected void onBeforeStop() {
            beforeStopCalled.set(true);
        }

        @Override
        protected void onAfterStop() {
            afterStopCalled.set(true);
        }
    }

    @Test
    public void testRestartAfterStopGracefully() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        CountingDaemon d = new CountingDaemon("restart-test", 5L, gsm);

        d.start();
        Assertions.assertTrue(d.firstCycle.await(3, TimeUnit.SECONDS), "first cycle must run");
        Assertions.assertTrue(d.isRunning());

        d.stopGracefully(2000L);
        Assertions.assertTrue(d.beforeStopCalled.get());
        Assertions.assertTrue(d.afterStopCalled.get());
        Assertions.assertFalse(d.isRunning());
        Assertions.assertTrue(d.isStopped());

        // Reset observables and restart the same instance - mimics a re-elected leader reusing
        // the singleton Mgr across leader sessions.
        d.beforeStopCalled.set(false);
        d.afterStopCalled.set(false);
        int cyclesAfterFirstStop = d.cycles.get();

        d.start();
        Assertions.assertTrue(d.isRunning());
        long deadline = System.currentTimeMillis() + 2000L;
        while (System.currentTimeMillis() < deadline && d.cycles.get() <= cyclesAfterFirstStop) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(d.cycles.get() > cyclesAfterFirstStop, "worker should resume producing cycles");

        d.stopGracefully(2000L);
        Assertions.assertFalse(d.isRunning());
    }

    @Test
    public void testSelfStopWhenLeaseInvalidated() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        CountingDaemon d = new CountingDaemon("self-stop-test", 5L, gsm);

        d.start();
        Assertions.assertTrue(d.firstCycle.await(3, TimeUnit.SECONDS));
        Assertions.assertTrue(d.isRunning());

        // Demotion happens externally; the daemon must notice on its next loop iteration and exit on its own.
        gsm.beginLeaderDemotion(FrontendNodeType.FOLLOWER);

        long deadline = System.currentTimeMillis() + 3000L;
        while (System.currentTimeMillis() < deadline && d.isRunning()) {
            Thread.sleep(10);
        }
        Assertions.assertFalse(d.isRunning(), "daemon must self-stop after lease becomes invalid");
        Assertions.assertTrue(d.isStopped());
    }

    @Test
    public void testStopGracefullyInvokesHooksInOrder() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        StringBuilder order = new StringBuilder();
        LeaderDaemon d = new LeaderDaemon("hook-order-test", 5L) {
            @Override
            protected GlobalStateMgr getGlobalStateMgr() {
                return gsm;
            }

            @Override
            protected void runAfterLeaseValid() throws InterruptedException {
                Thread.sleep(5);
            }

            @Override
            protected void onBeforeStop() {
                synchronized (order) {
                    order.append("before;");
                }
            }

            @Override
            protected void onAfterStop() {
                synchronized (order) {
                    order.append("after;");
                }
            }
        };

        d.start();
        Thread.sleep(100);
        d.stopGracefully(2000L);
        Assertions.assertEquals("before;after;", order.toString());
    }

    @Test
    public void testStopGracefullyReInterruptsOnTimeout() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        CountDownLatch inWork = new CountDownLatch(1);
        AtomicBoolean reachedAfterStop = new AtomicBoolean();
        // Worker that swallows interrupts and busy-loops, forcing the timeout path.
        LeaderDaemon d = new LeaderDaemon("timeout-test", 0L) {
            @Override
            protected GlobalStateMgr getGlobalStateMgr() {
                return gsm;
            }

            @Override
            protected void runAfterLeaseValid() {
                inWork.countDown();
                long until = System.currentTimeMillis() + 500L;
                while (System.currentTimeMillis() < until) {
                    // Clear the interrupt flag so LeaderDaemon.loop cannot observe stop promptly.
                    Thread.interrupted();
                }
            }

            @Override
            protected void onAfterStop() {
                reachedAfterStop.set(true);
            }
        };

        d.start();
        Assertions.assertTrue(inWork.await(2, TimeUnit.SECONDS));

        long start = System.currentTimeMillis();
        d.stopGracefully(100L);
        long elapsed = System.currentTimeMillis() - start;

        // stopGracefully must return after the join timeout rather than blocking for the full busy span.
        Assertions.assertTrue(elapsed < 2000L, "stopGracefully should honor the join timeout; elapsed=" + elapsed);
        Assertions.assertTrue(reachedAfterStop.get(), "onAfterStop must run even after timeout");
        Assertions.assertTrue(d.isStopped());
    }
}
