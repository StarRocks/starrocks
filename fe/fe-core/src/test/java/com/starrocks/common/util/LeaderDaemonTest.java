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

package com.starrocks.common.util;

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LeaderLease;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LeaderDaemonTest {
    @Test
    public void testStopBestEffortDoesNotInterruptBusinessCode(@Mocked GlobalStateMgr globalStateMgr)
            throws Exception {
        // A stop request cannot interrupt business code (including JE and committed WAL apply).
        mockValidLeaderLease(globalStateMgr);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        AtomicBoolean stopped = new AtomicBoolean(false);
        TestLeaderDaemon daemon = new TestLeaderDaemon(globalStateMgr, entered, release, interrupted, stopped);

        daemon.start();
        Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));

        // Requesting stop returns immediately, without joining or interrupting.
        daemon.stopBestEffort();
        Assertions.assertTrue(daemon.isStopRequested());
        Thread.sleep(200);
        Assertions.assertFalse(interrupted.get());
        Assertions.assertTrue(daemon.isRunning(), "worker keeps running until it bails cooperatively");

        release.countDown();
        awaitQuiesced(daemon);
        Assertions.assertFalse(daemon.isRunning());
        Assertions.assertFalse(interrupted.get());
        Assertions.assertTrue(stopped.get());
    }

    @Test
    public void testSetStopAlsoLeavesBusinessCodeUninterrupted(@Mocked GlobalStateMgr globalStateMgr)
            throws Exception {
        mockValidLeaderLease(globalStateMgr);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        AtomicBoolean stopped = new AtomicBoolean(false);
        TestLeaderDaemon daemon = new TestLeaderDaemon(globalStateMgr, entered, release, interrupted, stopped);

        daemon.start();
        Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));

        daemon.setStop();

        try {
            Assertions.assertTrue(daemon.isRunning());
            Assertions.assertTrue(daemon.isStopRequested());
            Assertions.assertFalse(interrupted.get());
        } finally {
            release.countDown();
            awaitQuiesced(daemon);
        }
        Assertions.assertFalse(interrupted.get());
        Assertions.assertTrue(stopped.get());
    }

    @Test
    public void testZeroIntervalKeepsLoopingInsteadOfHangingAfterOneCycle(@Mocked GlobalStateMgr globalStateMgr)
            throws Exception {
        mockValidLeaderLease(globalStateMgr);
        // An interval==0 LeaderDaemon is a "tight drain loop" (report-handler / resource-report-handler /
        // routine-load-task-scheduler): runAfterLeaseValid() self-paces via a blocking poll/sleep and the
        // outer loop must call it again immediately. The regression was that loop() ran
        // stopSignal.wait(intervalMs) == wait(0), which blocks forever, so the daemon ran exactly one cycle
        // then hung. This test requires the daemon to run several cycles.
        CountDownLatch cycles = new CountDownLatch(3);
        LeaderDaemon daemon = new LeaderDaemon("zero-interval-daemon", 0L) {
            @Override
            protected GlobalStateMgr getGlobalStateMgr() {
                return globalStateMgr;
            }

            @Override
            protected void runAfterLeaseValid() throws InterruptedException {
                cycles.countDown();
                Thread.sleep(10);
            }
        };

        daemon.start();
        try {
            Assertions.assertTrue(cycles.await(5, TimeUnit.SECONDS),
                    "interval=0 daemon must keep looping, not hang after a single cycle");
        } finally {
            daemon.stopBestEffort();
        }
        awaitQuiesced(daemon);
        Assertions.assertFalse(daemon.isRunning());
    }

    @Test
    public void testStopBestEffortDoesNotJoinAndWorkerSelfCleansAndDeregisters(@Mocked GlobalStateMgr globalStateMgr)
            throws Exception {
        // Each stop request returns without joining, so demotion can notify every daemon first.
        // The registry must track actual body and cleanup completion.
        mockValidLeaderLease(globalStateMgr);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        AtomicBoolean stopped = new AtomicBoolean(false);
        TestLeaderDaemon daemon = new TestLeaderDaemon(globalStateMgr, entered, release, interrupted, stopped);

        daemon.start();
        Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
        Assertions.assertTrue(LeaderDaemon.getRunningInstances().contains(daemon),
                "a started daemon must appear in the running-instances registry");

        daemon.stopBestEffort();
        Assertions.assertTrue(daemon.isStopRequested());

        Assertions.assertTrue(daemon.isRunning());
        release.countDown();
        awaitQuiesced(daemon);
        Assertions.assertFalse(daemon.isRunning());
        Assertions.assertFalse(LeaderDaemon.getRunningInstances().contains(daemon),
                "worker must deregister from the running-instances registry on exit");
        Assertions.assertTrue(stopped.get(), "worker must run onStopped() on its own exit");
        Assertions.assertFalse(interrupted.get());
    }

    @Test
    public void testAwaitQuiescedWaitsForWorkerExitAndTimesOutOnStragglers(@Mocked GlobalStateMgr globalStateMgr)
            throws Exception {
        // Demotion orders specific daemons' onStopped() (journal-visible state resets) BEFORE the
        // follower replayer starts via awaitQuiesced(list, timeout): it must block while the worker
        // is alive, throw on a straggler (the stage runner turns that into a process exit), and
        // return only after the worker fully exited - onStopped() included.
        mockValidLeaderLease(globalStateMgr);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        AtomicBoolean stopped = new AtomicBoolean(false);
        TestLeaderDaemon daemon = new TestLeaderDaemon(globalStateMgr, entered, release, interrupted, stopped);

        daemon.start();
        try {
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(IllegalStateException.class,
                    () -> LeaderDaemon.awaitQuiesced(List.of(daemon), 150L),
                    "a still-running daemon must fail the bounded wait, not be skipped");
        } finally {
            daemon.stopBestEffort();
            release.countDown();
        }
        LeaderDaemon.awaitQuiesced(List.of(daemon), 5000L);
        Assertions.assertFalse(daemon.isRunning());
        Assertions.assertTrue(stopped.get(), "quiesced implies onStopped() has completed");
    }

    @Test
    public void testStopWakesBusinessDelayWithoutInterrupting(@Mocked GlobalStateMgr globalStateMgr) throws Exception {
        mockValidLeaderLease(globalStateMgr);
        CountDownLatch entered = new CountDownLatch(1);
        AtomicBoolean stopped = new AtomicBoolean();
        LeaderDaemon daemon = new LeaderDaemon("cooperative-business-delay", 0L) {
            @Override
            protected GlobalStateMgr getGlobalStateMgr() {
                return globalStateMgr;
            }

            @Override
            protected void runAfterLeaseValid() throws InterruptedException {
                entered.countDown();
                sleepUntilNextStep(TimeUnit.HOURS.toMillis(1));
                stopped.set(shouldStop());
            }
        };
        daemon.start();
        try {
            Assertions.assertTrue(entered.await(3, TimeUnit.SECONDS));
        } finally {
            daemon.stopBestEffort();
            LeaderDaemon.awaitQuiesced(List.of(daemon), 3000L);
        }
        Assertions.assertTrue(stopped.get());
    }

    @Test
    public void testScheduledShutdownDiscardsDelayedWorkButDrainsRunningBody() throws Exception {
        java.util.concurrent.ScheduledThreadPoolExecutor pool = new java.util.concurrent.ScheduledThreadPoolExecutor(1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicBoolean delayedRan = new AtomicBoolean();
        pool.execute(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        java.util.concurrent.Future<?> delayed = pool.schedule(() -> delayedRan.set(true), 1, TimeUnit.HOURS);
        try {
            Assertions.assertTrue(entered.await(3, TimeUnit.SECONDS));
            LeaderDaemon.shutdownLeaderExecutor(pool);
            Assertions.assertTrue(delayed.isCancelled());
            Assertions.assertFalse(pool.isTerminated());
            Assertions.assertFalse(interrupted.get());
        } finally {
            release.countDown();
            LeaderDaemon.shutdownAndAwaitTermination("test-scheduled-pool", pool);
        }
        Assertions.assertFalse(delayedRan.get());
        Assertions.assertFalse(interrupted.get());
    }

    @Test
    public void testInterruptedCycleDoesNotSpinOrRequestStop(@Mocked GlobalStateMgr globalStateMgr) throws Exception {
        mockValidLeaderLease(globalStateMgr);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch repeated = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        LeaderDaemon daemon = new LeaderDaemon("interrupted-cycle", TimeUnit.HOURS.toMillis(1)) {
            @Override
            protected GlobalStateMgr getGlobalStateMgr() {
                return globalStateMgr;
            }

            @Override
            protected void runAfterLeaseValid() throws InterruptedException {
                if (first.compareAndSet(true, false)) {
                    entered.countDown();
                } else {
                    repeated.countDown();
                }
                throw new InterruptedException("business wait failed");
            }
        };
        daemon.start();
        try {
            Assertions.assertTrue(entered.await(3, TimeUnit.SECONDS));
            Assertions.assertFalse(repeated.await(200, TimeUnit.MILLISECONDS),
                    "reasserting a cycle interrupt must not turn the interval wait into a busy loop");
            Assertions.assertFalse(daemon.isStopRequested());
            Assertions.assertTrue(daemon.isRunning());
        } finally {
            daemon.stopBestEffort();
            LeaderDaemon.awaitQuiesced(List.of(daemon), 3000L);
        }
    }

    private static void awaitQuiesced(LeaderDaemon daemon) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline
                && (daemon.isRunning() || LeaderDaemon.getRunningInstances().contains(daemon))) {
            Thread.sleep(10);
        }
    }

    private void mockValidLeaderLease(GlobalStateMgr globalStateMgr) {
        LeaderLease lease = new LeaderLease(1L, 1L);
        new Expectations() {
            {
                globalStateMgr.isReady();
                result = true;
                minTimes = 0;

                globalStateMgr.captureLeaderLease();
                result = lease;
                minTimes = 0;

                globalStateMgr.isLeaderLeaseValid(lease);
                result = true;
                minTimes = 0;
            }
        };
    }

    private static class TestLeaderDaemon extends LeaderDaemon {
        private final GlobalStateMgr globalStateMgr;
        private final CountDownLatch entered;
        private final CountDownLatch release;
        private final AtomicBoolean interrupted;
        private final AtomicBoolean stopped;
        TestLeaderDaemon(GlobalStateMgr globalStateMgr, CountDownLatch entered, CountDownLatch release,
                         AtomicBoolean interrupted, AtomicBoolean stopped) {
            super("test-leader-daemon", 1000L);
            this.globalStateMgr = globalStateMgr;
            this.entered = entered;
            this.release = release;
            this.interrupted = interrupted;
            this.stopped = stopped;
        }

        @Override
        protected void runAfterLeaseValid() throws InterruptedException {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            }
        }

        @Override
        protected GlobalStateMgr getGlobalStateMgr() {
            return globalStateMgr;
        }

        @Override
        protected void onStopped() {
            stopped.set(true);
        }
    }
}
