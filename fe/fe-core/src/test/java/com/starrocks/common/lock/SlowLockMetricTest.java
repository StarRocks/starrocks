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

package com.starrocks.common.lock;

import com.codahale.metrics.Histogram;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.PrometheusMetricVisitor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class SlowLockMetricTest {

    /**
     * Reset the static throttle gates inside {@link LockManager} so each test starts from a clean
     * window. The gates are private statics; reflection keeps the test focused without poking a
     * test-only setter into production code.
     */
    private static void resetLockManagerThrottleGates() throws Exception {
        Class<?> cls = LockManager.class;
        for (String name : new String[] {"LAST_EVENT_LOG_MS", "LAST_STACK_PRINT_MS"}) {
            Field f = cls.getDeclaredField(name);
            f.setAccessible(true);
            ((AtomicLong) f.get(null)).set(0L);
        }
    }

    @BeforeEach
    public void resetGatesBeforeEach() throws Exception {
        // Both the existing test and the new throttle tests in this class touch the static
        // gates inside LockManager. Reset to a known-empty state per test so JUnit ordering
        // cannot leave a prior test's gate stamp blocking the next test's first event.
        resetLockManagerThrottleGates();
    }

    /**
     * Acquire-release contention helper: spawn a holder thread, take an exclusive lock on the
     * other side after a sync point, then release. Returns after the wait/hold has happened so
     * the caller can sample the relevant histogram counts.
     */
    private void triggerSlowLock(long rid, long holdMs) throws InterruptedException {
        CountDownLatch syncPoint1 = new CountDownLatch(1);
        CountDownLatch syncPoint2 = new CountDownLatch(1);
        Thread holder = createLockThread(rid, syncPoint1, syncPoint2, holdMs);
        holder.start();
        syncPoint1.await();
        Locker locker = new Locker();
        syncPoint2.countDown();
        locker.lockDatabase(rid, LockType.WRITE);
        locker.unLockDatabase(rid, LockType.WRITE);
        holder.join();
    }

    private Thread createLockThread(long rid, CountDownLatch syncPoint1, CountDownLatch syncPoint2, long sleepMs) {
        return new Thread(() -> {
            Locker lock = new Locker();
            lock.lockDatabase(rid, LockType.WRITE);
            try {
                syncPoint1.countDown();
                syncPoint2.await();
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                // Ignore Exception
            } finally {
                lock.unLockDatabase(rid, LockType.WRITE);
            }
        });
    }

    @Test
    public void testSlowLockHistogramUpdateOnLockContention() {
        Histogram histoLockHeld = MetricRepo.HISTO_SLOW_LOCK_HELD_TIME_MS;
        String histoLockHeldName = "slow_lock_held_time_ms";
        Histogram histoLockWait = MetricRepo.HISTO_SLOW_LOCK_WAIT_TIME_MS;
        String histoLockWaitName = "slow_lock_wait_time_ms";

        long slowLockConfig = Config.slow_lock_threshold_ms;
        long rid = 1127;

        Config.slow_lock_threshold_ms = 500; // 500 ms
        CountDownLatch syncPoint1 = new CountDownLatch(1);
        CountDownLatch syncPoint2 = new CountDownLatch(1);
        try {
            // check metric
            long slowHeldBeforeCount = histoLockHeld.getCount();
            long slowWaitBeforeCount = histoLockWait.getCount();

            Thread lockThread = createLockThread(rid, syncPoint1, syncPoint2, 1000L);
            lockThread.start();

            syncPoint1.await();

            Locker locker = new Locker();
            syncPoint2.countDown();

            // expect a slow lock here
            locker.lockDatabase(rid, LockType.WRITE);
            locker.unLockDatabase(rid, LockType.WRITE);

            // check metric again
            long slowHeldAfterCount = histoLockHeld.getCount();
            Assertions.assertEquals(slowHeldBeforeCount + 1, slowHeldAfterCount);
            long slowWaitAfterCount = histoLockWait.getCount();
            Assertions.assertEquals(slowWaitBeforeCount + 1, slowWaitAfterCount);

            PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("starrocks_fe");
            visitor.visitHistogram(histoLockHeldName, histoLockHeld);
            visitor.visitHistogram(histoLockWaitName, histoLockWait);
            String output = visitor.build();

            /*
            # HELP starrocks_fe_slow_lock_held_time_ms
            # TYPE starrocks_fe_slow_lock_held_time_ms summary
            starrocks_fe_slow_lock_held_time_ms{quantile="0.75"} 506.0
            starrocks_fe_slow_lock_held_time_ms{quantile="0.95"} 506.0
            starrocks_fe_slow_lock_held_time_ms{quantile="0.98"} 506.0
            starrocks_fe_slow_lock_held_time_ms{quantile="0.99"} 506.0
            starrocks_fe_slow_lock_held_time_ms{quantile="0.999"} 506.0
            starrocks_fe_slow_lock_held_time_ms_sum 506.0
            starrocks_fe_slow_lock_held_time_ms_count 1
            # HELP starrocks_fe_slow_lock_wait_time_ms
            # TYPE starrocks_fe_slow_lock_wait_time_ms summary
            starrocks_fe_slow_lock_wait_time_ms{quantile="0.75"} 1007.0
            starrocks_fe_slow_lock_wait_time_ms{quantile="0.95"} 1007.0
            starrocks_fe_slow_lock_wait_time_ms{quantile="0.98"} 1007.0
            starrocks_fe_slow_lock_wait_time_ms{quantile="0.99"} 1007.0
            starrocks_fe_slow_lock_wait_time_ms{quantile="0.999"} 1007.0
            starrocks_fe_slow_lock_wait_time_ms_sum 1007.0
            starrocks_fe_slow_lock_wait_time_ms_count 1
             */
            // Compare against the post-event counts rather than hardcoded "1": other tests in
            // this class share the same global histograms, so the absolute count is not 1 once
            // any sibling test has run earlier in the JVM.
            Assertions.assertTrue(output.contains(
                            "starrocks_fe_slow_lock_held_time_ms_count " + slowHeldAfterCount), output);
            Assertions.assertTrue(output.contains("starrocks_fe_slow_lock_held_time_ms{quantile=\"0.99\"}"), output);
            Assertions.assertTrue(output.contains(
                            "starrocks_fe_slow_lock_wait_time_ms_count " + slowWaitAfterCount), output);
            Assertions.assertTrue(output.contains("starrocks_fe_slow_lock_wait_time_ms{quantile=\"0.99\"}"), output);

            lockThread.join();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            Assertions.fail("Test interrupted: " + exception.getMessage());
        } finally {
            Config.slow_lock_threshold_ms = slowLockConfig;
        }
    }

    @Test
    public void testEventLevelThrottleSuppressesSecondSlowLockHeldMetric() throws Exception {
        // The held-time histogram is updated inside the slow-lock-trace event gate, so a second
        // event within slow_lock_log_every_ms must not bump the count, while the wait-time
        // histogram (recorded outside the gate) must bump on every event.
        resetLockManagerThrottleGates();
        long origThreshold = Config.slow_lock_threshold_ms;
        long origEvery = Config.slow_lock_log_every_ms;
        Config.slow_lock_threshold_ms = 500L;
        Config.slow_lock_log_every_ms = 60_000L;
        try {
            Histogram heldHisto = MetricRepo.HISTO_SLOW_LOCK_HELD_TIME_MS;
            Histogram waitHisto = MetricRepo.HISTO_SLOW_LOCK_WAIT_TIME_MS;
            long heldBefore = heldHisto.getCount();
            long waitBefore = waitHisto.getCount();

            triggerSlowLock(11270L, 1000L);
            long heldAfterFirst = heldHisto.getCount();
            long waitAfterFirst = waitHisto.getCount();
            Assertions.assertEquals(heldBefore + 1, heldAfterFirst,
                    "first slow-lock event should bump held-time count");
            Assertions.assertEquals(waitBefore + 1, waitAfterFirst,
                    "first slow-lock event should bump wait-time count");

            triggerSlowLock(11271L, 1000L);
            long heldAfterSecond = heldHisto.getCount();
            long waitAfterSecond = waitHisto.getCount();
            Assertions.assertEquals(heldAfterFirst, heldAfterSecond,
                    "second slow-lock event within slow_lock_log_every_ms must be suppressed for held-time");
            Assertions.assertEquals(waitAfterFirst + 1, waitAfterSecond,
                    "wait-time metric is recorded outside the event gate and must still bump");
        } finally {
            Config.slow_lock_threshold_ms = origThreshold;
            Config.slow_lock_log_every_ms = origEvery;
        }
    }

    @Test
    public void testEventLevelThrottleDisabledWhenIntervalNonPositive() throws Exception {
        resetLockManagerThrottleGates();
        long origThreshold = Config.slow_lock_threshold_ms;
        long origEvery = Config.slow_lock_log_every_ms;
        Config.slow_lock_threshold_ms = 500L;
        Config.slow_lock_log_every_ms = 0L; // disable the event gate
        try {
            Histogram heldHisto = MetricRepo.HISTO_SLOW_LOCK_HELD_TIME_MS;
            long heldBefore = heldHisto.getCount();
            triggerSlowLock(11272L, 1000L);
            triggerSlowLock(11273L, 1000L);
            long heldAfter = heldHisto.getCount();
            Assertions.assertEquals(heldBefore + 2, heldAfter,
                    "both events should bump held-time count when slow_lock_log_every_ms is 0");
        } finally {
            Config.slow_lock_threshold_ms = origThreshold;
            Config.slow_lock_log_every_ms = origEvery;
        }
    }
}
