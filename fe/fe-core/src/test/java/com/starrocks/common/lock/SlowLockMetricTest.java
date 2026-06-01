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
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.PrometheusMetricVisitor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

public class SlowLockMetricTest {

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
        long origStackInterval = Config.slow_lock_log_l1_stack_interval_ms;
        long origLogEvery = Config.slow_lock_log_l2_info_interval_ms;
        long rid = 1127;

        Config.slow_lock_threshold_ms = 500; // 500 ms
        // Disable the slow-lock log throttles (interval <= 0 always admits, bypassing the gates)
        // so this metric integration test is deterministic regardless of the global gate state
        // left by sibling tests in the same JVM — the slow event always reaches L1 and bumps the
        // held-time histogram.
        Config.slow_lock_log_l1_stack_interval_ms = 0;
        Config.slow_lock_log_l2_info_interval_ms = 0;
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

            // Compare against the post-event counts rather than a hardcoded "1": other tests in
            // this JVM share the same global histograms, so the absolute count is not necessarily 1.
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
            Config.slow_lock_log_l1_stack_interval_ms = origStackInterval;
            Config.slow_lock_log_l2_info_interval_ms = origLogEvery;
        }
    }
}
