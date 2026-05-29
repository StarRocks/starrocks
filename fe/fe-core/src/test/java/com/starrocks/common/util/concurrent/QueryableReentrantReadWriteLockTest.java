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

package com.starrocks.common.util.concurrent;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class QueryableReentrantReadWriteLockTest {

    private boolean origPrintStack;
    private long origStackInterval;
    private long origLogEvery;
    private long origBreadcrumbEvery;
    private int origMaxWaiter;

    @BeforeEach
    public void setUp() {
        origPrintStack = Config.slow_lock_print_stack;
        origStackInterval = Config.slow_lock_stack_print_interval_ms;
        origLogEvery = Config.slow_lock_log_every_ms;
        origBreadcrumbEvery = Config.slow_lock_breadcrumb_every_ms;
        origMaxWaiter = Config.slow_lock_max_waiter_count_to_log;
        Config.slow_lock_print_stack = true;
        Config.slow_lock_stack_print_interval_ms = 30000L;
        Config.slow_lock_log_every_ms = 3000L;
        Config.slow_lock_breadcrumb_every_ms = 1000L;
        Config.slow_lock_max_waiter_count_to_log = 30;
    }

    @AfterEach
    public void tearDown() {
        Config.slow_lock_print_stack = origPrintStack;
        Config.slow_lock_stack_print_interval_ms = origStackInterval;
        Config.slow_lock_log_every_ms = origLogEvery;
        Config.slow_lock_breadcrumb_every_ms = origBreadcrumbEvery;
        Config.slow_lock_max_waiter_count_to_log = origMaxWaiter;
    }

    /** A decision with captureStack=true, produced from fresh gates (L1). */
    private static SlowLockLogDecision captureDecision() {
        return SlowLockLogDecision.decide(true, freshGate(), freshGate(), freshGate(), 1_000_000_000L);
    }

    private static AtomicLong freshGate() {
        return new AtomicLong(SlowLockLogDecision.GATE_INIT_SENTINEL);
    }

    // ---- rendering: getLockInfoToJson only reads decision.captureStack ----

    @Test
    public void testExclusiveOwnerIncludesStackWhenCaptureStackTrue() {
        SlowLockLogDecision capture = captureDecision();
        Assertions.assertTrue(capture.captureStack, "precondition: decision should capture stack");

        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        lock.exclusiveLock();
        try {
            JsonObject holder = lock.getLockInfoToJson(null, capture).getAsJsonObject("holderInfo");
            Assertions.assertEquals("exclusive", holder.get("status").getAsString());
            Assertions.assertTrue(holder.has("stack"));
        } finally {
            lock.exclusiveUnlock();
        }
    }

    @Test
    public void testExclusiveOwnerOmitsStackWhenCaptureStackFalse() {
        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        lock.exclusiveLock();
        try {
            // SUPPRESS carries captureStack=false; rendering must omit the stack field.
            JsonObject holder = lock.getLockInfoToJson(null, SlowLockLogDecision.SUPPRESS)
                    .getAsJsonObject("holderInfo");
            Assertions.assertEquals("exclusive", holder.get("status").getAsString());
            Assertions.assertFalse(holder.has("stack"));
        } finally {
            lock.exclusiveUnlock();
        }
    }

    @Test
    public void testCurrentThreadHolderStackGating() {
        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        JsonObject withStack = lock.getLockInfoToJson(Thread.currentThread(), captureDecision())
                .getAsJsonObject("holderInfo");
        Assertions.assertTrue(withStack.has("stack"));

        JsonObject noStack = lock.getLockInfoToJson(Thread.currentThread(), SlowLockLogDecision.SUPPRESS)
                .getAsJsonObject("holderInfo");
        Assertions.assertFalse(noStack.has("stack"));
    }

    @Test
    public void testEmptySnapshotRendersNoStackAndDoesNotThrow() {
        // No owner, no reader. getLockInfoToJson must not touch any gate (it only reads the
        // decision) and renders an empty oldestReader.
        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        JsonObject holder = lock.getLockInfoToJson(null, captureDecision()).getAsJsonObject("holderInfo");
        Assertions.assertEquals("shared", holder.get("status").getAsString());
        Assertions.assertEquals(0, holder.getAsJsonObject("oldestReader").size());
    }

    // ---- decision integration: decideSlowLockLog touches the per-instance gates ----

    @Test
    public void testDecideDegradesWithinStackWindow() {
        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        lock.exclusiveLock();
        try {
            SlowLockLogDecision first = lock.decideSlowLockLog(true);
            Assertions.assertEquals(SlowLockTier.L1_STACK_INFO, first.tier,
                    "first event should hit L1");
            SlowLockLogDecision second = lock.decideSlowLockLog(true);
            Assertions.assertNotEquals(SlowLockTier.L1_STACK_INFO, second.tier,
                    "a second event within the stack window must degrade below L1");
        } finally {
            lock.exclusiveUnlock();
        }
    }

    @Test
    public void testDecidePerInstanceIsolation() {
        QueryableReentrantReadWriteLock lockA = new QueryableReentrantReadWriteLock(true);
        QueryableReentrantReadWriteLock lockB = new QueryableReentrantReadWriteLock(true);
        lockA.exclusiveLock();
        lockB.exclusiveLock();
        try {
            Assertions.assertEquals(SlowLockTier.L1_STACK_INFO, lockA.decideSlowLockLog(true).tier);
            // lockA consumed its own L1 quota; lockB must be unaffected (per-instance gates).
            Assertions.assertEquals(SlowLockTier.L1_STACK_INFO, lockB.decideSlowLockLog(true).tier,
                    "lockB must not be throttled by lockA's quota");
        } finally {
            lockB.exclusiveUnlock();
            lockA.exclusiveUnlock();
        }
    }

    @Test
    public void testDecideHasHolderFalseDoesNotConsumeStackGate() {
        // A no-holder decision (which falls to L2) must not burn the per-instance stack gate,
        // so a subsequent real (hasHolder) event in the same window can still reach L1.
        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        SlowLockLogDecision noHolder = lock.decideSlowLockLog(false);
        Assertions.assertNotEquals(SlowLockTier.L1_STACK_INFO, noHolder.tier,
                "no holder → not L1");
        SlowLockLogDecision withHolder = lock.decideSlowLockLog(true);
        Assertions.assertEquals(SlowLockTier.L1_STACK_INFO, withHolder.tier,
                "stack gate must still be available — the no-holder call must not have consumed it");
    }

    // ---- waiter cap ----

    @Test
    public void testWaiterCapApplied() throws InterruptedException {
        Config.slow_lock_max_waiter_count_to_log = 3;

        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        lock.exclusiveLock();
        int waiterCount = 6;
        CountDownLatch started = new CountDownLatch(waiterCount);
        List<Thread> waiters = new ArrayList<>();
        try {
            for (int i = 0; i < waiterCount; i++) {
                Thread t = new Thread(() -> {
                    started.countDown();
                    lock.sharedLock();
                    lock.sharedUnlock();
                }, "queued-reader-" + i);
                t.start();
                waiters.add(t);
            }
            started.await();
            long deadlineNs = System.nanoTime() + 2_000_000_000L;
            while (lock.getQueuedReaderThreads().size() < waiterCount && System.nanoTime() < deadlineNs) {
                Thread.sleep(20);
            }
            Assertions.assertEquals(waiterCount, lock.getQueuedReaderThreads().size(),
                    "test prerequisite: all reader waiters should be queued");

            JsonArray queued = lock.getLockInfoToJson(null, SlowLockLogDecision.SUPPRESS)
                    .getAsJsonArray("queuedReaders");
            Assertions.assertEquals(4, queued.size(), "expected cap=3 entries + 1 omitted trailer, got: " + queued);
            JsonObject trailer = queued.get(3).getAsJsonObject();
            Assertions.assertTrue(trailer.has("omitted"));
            Assertions.assertEquals("remain 3 waiters omitted", trailer.get("omitted").getAsString());
        } finally {
            lock.exclusiveUnlock();
            for (Thread t : waiters) {
                t.join();
            }
        }
    }

    @Test
    public void testWaiterCapDisabledIncludesAll() throws InterruptedException {
        Config.slow_lock_max_waiter_count_to_log = 0;

        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        lock.exclusiveLock();
        int waiterCount = 4;
        CountDownLatch started = new CountDownLatch(waiterCount);
        List<Thread> waiters = new ArrayList<>();
        try {
            for (int i = 0; i < waiterCount; i++) {
                Thread t = new Thread(() -> {
                    started.countDown();
                    lock.sharedLock();
                    lock.sharedUnlock();
                }, "queued-reader-cap0-" + i);
                t.start();
                waiters.add(t);
            }
            started.await();
            long deadlineNs = System.nanoTime() + 2_000_000_000L;
            while (lock.getQueuedReaderThreads().size() < waiterCount && System.nanoTime() < deadlineNs) {
                Thread.sleep(20);
            }

            JsonArray queued = lock.getLockInfoToJson(null, SlowLockLogDecision.SUPPRESS)
                    .getAsJsonArray("queuedReaders");
            Assertions.assertEquals(waiterCount, queued.size(), "cap=0 must serialize every waiter with no trailer");
            for (int i = 0; i < queued.size(); i++) {
                Assertions.assertFalse(queued.get(i).getAsJsonObject().has("omitted"));
            }
        } finally {
            lock.exclusiveUnlock();
            for (Thread t : waiters) {
                t.join();
            }
        }
    }
}
