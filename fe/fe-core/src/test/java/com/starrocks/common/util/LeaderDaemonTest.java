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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LeaderDaemonTest {
    @Test
    public void testStopGracefullyInterruptsRunningCycleByDefault(@Mocked GlobalStateMgr globalStateMgr)
            throws Exception {
        // Default contract: stopGracefully() interrupts the worker so a cycle blocked in an
        // interruptible primitive (here CountDownLatch.await) unblocks at once, without the test
        // ever releasing the latch.
        mockValidLeaderLease(globalStateMgr);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        AtomicBoolean stopped = new AtomicBoolean(false);
        TestLeaderDaemon daemon = new TestLeaderDaemon(globalStateMgr, entered, release, interrupted, stopped);

        daemon.start();
        Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));

        daemon.stopGracefully(5000L);

        Assertions.assertTrue(interrupted.get());
        Assertions.assertTrue(daemon.isStopped());
        Assertions.assertFalse(daemon.isRunning());
        Assertions.assertTrue(stopped.get());
    }

    @Test
    public void testStopGracefullyDoesNotInterruptWhenOptedOut(@Mocked GlobalStateMgr globalStateMgr)
            throws Exception {
        // Interrupt-unsafe daemons override interruptOnStop() to false (e.g. CheckpointController,
        // which calls BDBJE directly). stopGracefully() then must NOT interrupt the worker; it
        // relies on the cycle finishing / cooperative bail. Here the daemon exits only once the
        // test releases the latch, and the worker is never interrupted.
        mockValidLeaderLease(globalStateMgr);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        AtomicBoolean stopped = new AtomicBoolean(false);
        TestLeaderDaemon daemon = new TestLeaderDaemon(globalStateMgr, entered, release, interrupted, stopped);
        daemon.interruptOnStopFlag = false;

        daemon.start();
        Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));

        Thread stopper = new Thread(() -> daemon.stopGracefully(5000L));
        stopper.start();
        Thread.sleep(200);

        Assertions.assertTrue(daemon.isStopped());
        Assertions.assertFalse(interrupted.get());

        release.countDown();
        stopper.join(5000L);
        Assertions.assertFalse(stopper.isAlive());
        Assertions.assertFalse(daemon.isRunning());
        Assertions.assertFalse(interrupted.get());
        Assertions.assertTrue(stopped.get());
    }

    @Test
    public void testSetStopStillInterruptsRunningCycle(@Mocked GlobalStateMgr globalStateMgr)
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

        for (int i = 0; i < 50 && !interrupted.get(); i++) {
            Thread.sleep(20);
        }
        release.countDown();

        Assertions.assertTrue(interrupted.get());
        Assertions.assertTrue(daemon.isStopped());
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

            @Override
            protected void onJoinTimeout() {
                // tests only: never terminate the JVM
            }
        };

        daemon.start();
        try {
            Assertions.assertTrue(cycles.await(5, TimeUnit.SECONDS),
                    "interval=0 daemon must keep looping, not hang after a single cycle");
        } finally {
            daemon.stopGracefully(5000L);
        }
        Assertions.assertFalse(daemon.isRunning());
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
        // Whether stopGracefully() may interrupt this daemon's worker. Default true (the framework
        // default); the opt-out test sets it false to exercise the interrupt-unsafe daemon path.
        boolean interruptOnStopFlag = true;

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
        protected boolean interruptOnStop() {
            return interruptOnStopFlag;
        }

        @Override
        protected void onStopped() {
            stopped.set(true);
        }

        @Override
        protected void onJoinTimeout() {
            Assertions.fail("test daemon should stop before timeout");
        }
    }
}
