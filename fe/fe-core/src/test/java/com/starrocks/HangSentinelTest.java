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

package com.starrocks;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

/**
 * TEMPORARY - do not merge.
 * <p>
 * Wedges its surefire fork the same way the real FE UT hang does: two threads take two monitors in
 * opposite order, so the test thread parks on `synchronized` forever. That block is not
 * interruptible, so the 300s JUnit timeout cannot end it (junit-jupiter 5.8.2 ignores
 * `timeout.thread.mode.default = separate_thread` and only sends a best-effort interrupt), the test
 * set never completes, and surefire discards the fork's buffered stdout - exactly the situation this
 * PR is meant to make diagnosable.
 * <p>
 * The two latches make the deadlock structural rather than timing-dependent: neither thread can
 * reach its second monitor until the other one provably holds the first. That matters because FE UT
 * runs 18 forks in parallel, so a sleep-based handshake could lose its race on a loaded runner and
 * leave the fork merely failing instead of hanging.
 * <p>
 * Revert this file together with the temporary `timeout-minutes` reduction in ci-pipeline.yml once
 * the `Clean ECI` dump has been shown to work.
 */
public class HangSentinelTest {
    private static final Object LOCK_A = new Object();
    private static final Object LOCK_B = new Object();

    @Test
    public void testNonInterruptibleHang() {
        CountDownLatch lockAHeld = new CountDownLatch(1);
        CountDownLatch lockBHeld = new CountDownLatch(1);

        Thread worker = new Thread(() -> {
            synchronized (LOCK_B) {
                lockBHeld.countDown();
                awaitQuietly(lockAHeld);
                synchronized (LOCK_A) {
                    throw new IllegalStateException("unreachable: LOCK_A is held by the test thread");
                }
            }
        }, "hang-sentinel-worker");
        worker.setDaemon(true);
        worker.start();

        synchronized (LOCK_A) {
            lockAHeld.countDown();
            awaitQuietly(lockBHeld);
            synchronized (LOCK_B) {
                throw new IllegalStateException("unreachable: LOCK_B is held by hang-sentinel-worker");
            }
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
