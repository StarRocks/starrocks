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
        final AtomicBoolean stoppedCalled = new AtomicBoolean();
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
        protected void onStopped() {
            stoppedCalled.set(true);
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
        Assertions.assertTrue(d.stoppedCalled.get());
        Assertions.assertFalse(d.isRunning());
        Assertions.assertTrue(d.isStopped());

        // Reset observables and restart the same instance - mimics a re-elected leader reusing
        // the singleton Mgr across leader sessions.
        d.stoppedCalled.set(false);
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
    public void testStopGracefullyRunsOnStoppedAfterWorkerExits() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        AtomicBoolean workerExited = new AtomicBoolean();
        AtomicBoolean onStoppedObservedWorkerExit = new AtomicBoolean();
        LeaderDaemon d = new LeaderDaemon("hook-order-test", 5L) {
            @Override
            protected GlobalStateMgr getGlobalStateMgr() {
                return gsm;
            }

            @Override
            protected void runAfterLeaseValid() throws InterruptedException {
                try {
                    Thread.sleep(5);
                } finally {
                    workerExited.set(true);
                }
            }

            @Override
            protected void onStopped() {
                onStoppedObservedWorkerExit.set(workerExited.get());
            }
        };

        d.start();
        Thread.sleep(100);
        d.stopGracefully(2000L);
        Assertions.assertTrue(onStoppedObservedWorkerExit.get(),
                "onStopped must run after the worker has exited runAfterLeaseValid");
        Assertions.assertFalse(d.isRunning());
    }

    @Test
    public void testJoinTimeoutInvokesFailureHookAndKeepsStateFenced() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        CountDownLatch inWork = new CountDownLatch(1);
        AtomicBoolean joinTimeoutHook = new AtomicBoolean();
        AtomicBoolean onStoppedCalled = new AtomicBoolean();
        // Worker that swallows interrupts and busy-loops, forcing the join-timeout path.
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
            protected void onStopped() {
                onStoppedCalled.set(true);
            }

            @Override
            protected void onJoinTimeout() {
                // Test override: record the call instead of terminating the JVM.
                joinTimeoutHook.set(true);
            }
        };

        d.start();
        Assertions.assertTrue(inWork.await(2, TimeUnit.SECONDS));

        long start = System.currentTimeMillis();
        d.stopGracefully(100L);
        long elapsed = System.currentTimeMillis() - start;

        // Must honor the join timeout rather than blocking for the full busy span.
        Assertions.assertTrue(elapsed < 2000L, "stopGracefully should honor the join timeout; elapsed=" + elapsed);
        Assertions.assertTrue(joinTimeoutHook.get(), "onJoinTimeout must fire when the worker doesn't exit in time");
        Assertions.assertFalse(onStoppedCalled.get(), "onStopped must NOT run on the timeout path");
        Assertions.assertTrue(d.isStopped());
    }

    @Test
    public void testDefaultIntervalConstructorAndGetters() {
        LeaderDaemon d = new LeaderDaemon("default-ctor") {
            @Override
            protected void runAfterLeaseValid() {
            }
        };
        Assertions.assertEquals("default-ctor", d.getName());
        Assertions.assertEquals(30L * 1000L, d.getInterval());
        d.setInterval(123L);
        Assertions.assertEquals(123L, d.getInterval());
    }

    @Test
    public void testStartIsIdempotent() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        CountingDaemon d = new CountingDaemon("idempotent-start", 5L, gsm);
        d.start();
        Assertions.assertTrue(d.firstCycle.await(3, TimeUnit.SECONDS));
        // Second start() while running must be a no-op - no second worker thread.
        d.start();
        Assertions.assertTrue(d.isRunning());
        d.stopGracefully(2000L);
    }

    @Test
    public void testSetStopIsIdempotent() {
        TestGlobalStateMgr gsm = activeLeader();
        CountingDaemon d = new CountingDaemon("idempotent-setstop", 5L, gsm);
        d.start();
        d.setStop();
        // Second setStop() is a no-op and must not throw even after worker is already scheduled to exit.
        d.setStop();
        Assertions.assertTrue(d.isStopped());
        d.stopGracefully(2000L);
    }

    @Test
    public void testOnStoppedThrowableIsSwallowed() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        AtomicBoolean stateReset = new AtomicBoolean();
        LeaderDaemon d = new LeaderDaemon("onstopped-throw", 5L) {
            @Override
            protected GlobalStateMgr getGlobalStateMgr() {
                return gsm;
            }

            @Override
            protected void runAfterLeaseValid() throws InterruptedException {
                Thread.sleep(5);
            }

            @Override
            protected void onStopped() {
                throw new RuntimeException("boom");
            }
        };
        d.start();
        Thread.sleep(50);
        // A throwing onStopped must not prevent stopGracefully from completing its state reset.
        d.stopGracefully(2000L);
        Assertions.assertFalse(d.isRunning(), "state reset must still run after onStopped throws");
        stateReset.set(!d.isRunning());
        Assertions.assertTrue(stateReset.get());
    }

    @Test
    public void testRunAfterLeaseValidThrowableDoesNotKillLoop() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch afterFailures = new CountDownLatch(3);
        LeaderDaemon d = new LeaderDaemon("runtime-error", 5L) {
            @Override
            protected GlobalStateMgr getGlobalStateMgr() {
                return gsm;
            }

            @Override
            protected void runAfterLeaseValid() {
                if (attempts.incrementAndGet() <= 3) {
                    afterFailures.countDown();
                    throw new RuntimeException("transient");
                }
            }
        };
        d.start();
        // Loop must keep running past RuntimeExceptions from runAfterLeaseValid.
        Assertions.assertTrue(afterFailures.await(3, TimeUnit.SECONDS),
                "loop must survive runtime exceptions and keep iterating");
        d.stopGracefully(2000L);
    }

    @Test
    public void testStopWhileNotReadyExitsCleanly() throws Exception {
        // GlobalStateMgr that never reports ready - worker gets stuck in the !isReady() sleep loop.
        GlobalStateMgr neverReady = new GlobalStateMgr(new NodeMgr()) {
            @Override
            public boolean isReady() {
                return false;
            }
        };
        AtomicBoolean ranBody = new AtomicBoolean();
        LeaderDaemon d = new LeaderDaemon("never-ready", 5L) {
            @Override
            protected GlobalStateMgr getGlobalStateMgr() {
                return neverReady;
            }

            @Override
            protected void runAfterLeaseValid() {
                ranBody.set(true);
            }
        };
        d.start();
        Thread.sleep(150);
        d.stopGracefully(2000L);
        // Body must never have been invoked because GSM stayed unready; the worker must still have exited.
        Assertions.assertFalse(ranBody.get());
        Assertions.assertFalse(d.isRunning());
        Assertions.assertTrue(d.isStopped());
    }

    @Test
    public void testSetStopBeforeStartIsSafe() {
        CountingDaemon d = new CountingDaemon("setstop-before-start", 5L, activeLeader());
        // Before start(), there is no worker to interrupt. setStop() must still flip isStopped.
        d.setStop();
        Assertions.assertTrue(d.isStopped());
        // stopGracefully without a worker thread must be a no-op that returns cleanly.
        d.stopGracefully(100L);
        Assertions.assertFalse(d.isRunning());
    }
}
