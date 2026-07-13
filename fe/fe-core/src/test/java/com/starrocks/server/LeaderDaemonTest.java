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

import java.util.List;
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

    /** Fire-and-forget stop is async; wait for the worker to run onStopped() and clear isRunning. */
    private static void awaitQuiesced(LeaderDaemon d) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000L;
        while (System.currentTimeMillis() < deadline && d.isRunning()) {
            Thread.sleep(10);
        }
    }

    private static void stopAndAwait(LeaderDaemon d) throws InterruptedException {
        d.stopBestEffort();
        awaitQuiesced(d);
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
    public void testRestartAfterStop() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        CountingDaemon d = new CountingDaemon("restart-test", 5L, gsm);

        d.start();
        Assertions.assertTrue(d.firstCycle.await(3, TimeUnit.SECONDS), "first cycle must run");
        Assertions.assertTrue(d.isRunning());

        // Fire-and-forget stop; the worker self-cleans and clears isRunning on its own exit.
        d.stopBestEffort();
        awaitQuiesced(d);
        Assertions.assertTrue(d.stoppedCalled.get());
        Assertions.assertFalse(d.isRunning());
        Assertions.assertTrue(d.isStopRequested());

        // Reset observables and restart the same instance - mimics a re-elected leader reusing the
        // singleton Mgr across leader sessions. start() CASes on isRunning==false, which is why we
        // waited for the worker to fully quiesce above.
        d.stoppedCalled.set(false);
        int cyclesAfterFirstStop = d.cycles.get();

        d.start();
        Assertions.assertTrue(d.isRunning());
        long deadline = System.currentTimeMillis() + 2000L;
        while (System.currentTimeMillis() < deadline && d.cycles.get() <= cyclesAfterFirstStop) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(d.cycles.get() > cyclesAfterFirstStop, "worker should resume producing cycles");

        stopAndAwait(d);
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
        Assertions.assertTrue(d.isStopRequested());
    }

    @Test
    public void testStopBestEffortRunsOnStoppedAfterWorkerExits() throws Exception {
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
        d.stopBestEffort();
        awaitQuiesced(d);
        Assertions.assertTrue(onStoppedObservedWorkerExit.get(),
                "onStopped must run after the worker has exited runAfterLeaseValid");
        Assertions.assertFalse(d.isRunning());
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
        stopAndAwait(d);
    }

    @Test
    public void testSetStopIsIdempotent() throws Exception {
        TestGlobalStateMgr gsm = activeLeader();
        CountingDaemon d = new CountingDaemon("idempotent-setstop", 5L, gsm);
        d.start();
        d.setStop();
        // Second setStop() is a no-op and must not throw even after worker is already scheduled to exit.
        d.setStop();
        Assertions.assertTrue(d.isStopRequested());
        stopAndAwait(d);
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
        // A throwing onStopped must not prevent the worker from completing its state reset (isRunning=false).
        d.stopBestEffort();
        awaitQuiesced(d);
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
        stopAndAwait(d);
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
        d.stopBestEffort();
        awaitQuiesced(d);
        // Body must never have been invoked because GSM stayed unready; the worker must still have exited.
        Assertions.assertFalse(ranBody.get());
        Assertions.assertFalse(d.isRunning());
        Assertions.assertTrue(d.isStopRequested());
    }

    @Test
    public void testSetStopBeforeStartIsSafe() {
        CountingDaemon d = new CountingDaemon("setstop-before-start", 5L, activeLeader());
        // Before start(), there is no worker to interrupt. setStop() must still flip the stop request.
        d.setStop();
        Assertions.assertTrue(d.isStopRequested());
        // stopBestEffort without a worker thread must be a no-op that returns cleanly.
        d.stopBestEffort();
        Assertions.assertFalse(d.isRunning());
    }

    @Test
    public void testIsAgentTaskDispatchDisallowed() {
        // The agent-task enqueue guard and the AgentBatchTask dispatch fence share this predicate: a
        // node must not push BE agent tasks unless it is an active, non-demoting leader.
        TestGlobalStateMgr leader = activeLeader();
        Assertions.assertFalse(leader.isAgentTaskDispatchDisallowed(), "an active leader may dispatch agent tasks");

        // Demoting: disallowed even though feType is still LEADER (leaderRoleState == DEMOTING).
        leader.beginLeaderDemotion(FrontendNodeType.FOLLOWER);
        Assertions.assertTrue(leader.isAgentTaskDispatchDisallowed(),
                "a demoting node must not dispatch even while feType is still LEADER");

        // Follower (not demoting): disallowed via feType.
        TestGlobalStateMgr follower = new TestGlobalStateMgr();
        follower.setFrontendNodeType(FrontendNodeType.FOLLOWER);
        Assertions.assertTrue(follower.isAgentTaskDispatchDisallowed(), "a follower must not dispatch agent tasks");
    }

    @Test
    public void testFindLeaderSessionStragglersIgnoresFreshRunningPools() {
        // Regression: a freshly-constructed (running, never-shut-down) leader-session pool must NOT be
        // flagged as a straggler by the re-activation gate. The gate keys on isShutdown()-but-not-
        // terminated, not merely !isTerminated(); a running pool has isShutdown()==false. Before the fix
        // this false-positived and System.exit'd the process on a normal (re-)activation with live pools.
        TestGlobalStateMgr gsm = new TestGlobalStateMgr();
        List<String> stragglers = gsm.findLeaderSessionStragglers();
        Assertions.assertFalse(stragglers.contains("loadingLoadTaskScheduler(pool)"),
                "a fresh running load pool must not be flagged as a straggler");
        Assertions.assertFalse(stragglers.contains("pendingLoadTaskScheduler(pool)"),
                "a fresh running load pool must not be flagged as a straggler");
        Assertions.assertFalse(stragglers.contains("exportChecker(pools)"),
                "fresh/absent export pools must not be flagged as stragglers");
        Assertions.assertFalse(stragglers.contains("taskManager(schedulers)"),
                "fresh running task-manager schedulers must not be flagged as stragglers");
    }
}
