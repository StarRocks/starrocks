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

public class QueryableReentrantReadWriteLockTest {

    private boolean origPrintStack;
    private long origStackInterval;
    private long origLogEvery;
    private int origMaxWaiter;

    @BeforeEach
    public void setUp() {
        origPrintStack = Config.slow_lock_print_stack;
        origStackInterval = Config.slow_lock_stack_print_interval_ms;
        origLogEvery = Config.slow_lock_log_every_ms;
        origMaxWaiter = Config.slow_lock_max_waiter_count_to_log;
    }

    @AfterEach
    public void tearDown() {
        Config.slow_lock_print_stack = origPrintStack;
        Config.slow_lock_stack_print_interval_ms = origStackInterval;
        Config.slow_lock_log_every_ms = origLogEvery;
        Config.slow_lock_max_waiter_count_to_log = origMaxWaiter;
    }

    @Test
    public void testGetLockInfoWithExclusiveOwnerIncludesStackWhenEnabled() {
        Config.slow_lock_print_stack = true;
        Config.slow_lock_stack_print_interval_ms = 0; // disable throttle

        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        lock.exclusiveLock();
        try {
            JsonObject info = lock.getLockInfoToJson(null);
            JsonObject holder = info.getAsJsonObject("holderInfo");
            Assertions.assertEquals("exclusive", holder.get("status").getAsString());
            Assertions.assertTrue(holder.has("stack"), "stack should be captured when switch on and not throttled");
        } finally {
            lock.exclusiveUnlock();
        }
    }

    @Test
    public void testGetLockInfoWithExclusiveOwnerOmitsStackWhenSwitchOff() {
        Config.slow_lock_print_stack = false;

        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        lock.exclusiveLock();
        try {
            JsonObject info = lock.getLockInfoToJson(null);
            JsonObject holder = info.getAsJsonObject("holderInfo");
            Assertions.assertEquals("exclusive", holder.get("status").getAsString());
            Assertions.assertFalse(holder.has("stack"), "stack must be omitted when slow_lock_print_stack=false");
        } finally {
            lock.exclusiveUnlock();
        }
    }

    @Test
    public void testStackThrottleSuppressesSecondCallWithinWindow() {
        Config.slow_lock_print_stack = true;
        Config.slow_lock_stack_print_interval_ms = 60_000L; // 1 min

        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        lock.exclusiveLock();
        try {
            JsonObject first = lock.getLockInfoToJson(null);
            Assertions.assertTrue(first.getAsJsonObject("holderInfo").has("stack"),
                    "first call within an empty window must capture stack");
            JsonObject second = lock.getLockInfoToJson(null);
            Assertions.assertFalse(second.getAsJsonObject("holderInfo").has("stack"),
                    "second call within the throttle window must omit stack");
        } finally {
            lock.exclusiveUnlock();
        }
    }

    @Test
    public void testPerInstanceThrottleDoesNotStarveOtherLocks() {
        Config.slow_lock_print_stack = true;
        Config.slow_lock_stack_print_interval_ms = 60_000L;

        QueryableReentrantReadWriteLock lockA = new QueryableReentrantReadWriteLock(true);
        QueryableReentrantReadWriteLock lockB = new QueryableReentrantReadWriteLock(true);
        lockA.exclusiveLock();
        lockB.exclusiveLock();
        try {
            JsonObject a1 = lockA.getLockInfoToJson(null);
            JsonObject a2 = lockA.getLockInfoToJson(null);
            JsonObject b1 = lockB.getLockInfoToJson(null);
            Assertions.assertTrue(a1.getAsJsonObject("holderInfo").has("stack"));
            Assertions.assertFalse(a2.getAsJsonObject("holderInfo").has("stack"),
                    "lockA second call should be throttled");
            Assertions.assertTrue(b1.getAsJsonObject("holderInfo").has("stack"),
                    "lockB first call must not be affected by lockA's throttle quota (per-instance scope)");
        } finally {
            lockB.exclusiveUnlock();
            lockA.exclusiveUnlock();
        }
    }

    @Test
    public void testCurrentThreadHolderInfoStackGating() {
        Config.slow_lock_print_stack = true;
        Config.slow_lock_stack_print_interval_ms = 0;

        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        JsonObject info = lock.getLockInfoToJson(Thread.currentThread());
        Assertions.assertTrue(info.getAsJsonObject("holderInfo").has("stack"));

        Config.slow_lock_print_stack = false;
        JsonObject info2 = lock.getLockInfoToJson(Thread.currentThread());
        Assertions.assertFalse(info2.getAsJsonObject("holderInfo").has("stack"));
    }

    @Test
    public void testWaiterCapApplied() throws InterruptedException {
        Config.slow_lock_print_stack = false; // don't care about stacks here
        Config.slow_lock_max_waiter_count_to_log = 3;

        QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);
        lock.exclusiveLock();
        // Spin up readers that will queue behind the exclusive holder.
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
            // Give threads a moment to enter the queue.
            long deadlineNs = System.nanoTime() + 2_000_000_000L;
            while (lock.getQueuedReaderThreads().size() < waiterCount
                    && System.nanoTime() < deadlineNs) {
                Thread.sleep(20);
            }
            Assertions.assertEquals(waiterCount, lock.getQueuedReaderThreads().size(),
                    "test prerequisite: all reader waiters should be queued");

            JsonObject info = lock.getLockInfoToJson(null);
            JsonArray queued = info.getAsJsonArray("queuedReaders");
            // 3 listed + 1 omitted trailer
            Assertions.assertEquals(4, queued.size(),
                    "expected cap=3 entries + 1 omitted trailer, got: " + queued);
            JsonObject trailer = queued.get(3).getAsJsonObject();
            Assertions.assertTrue(trailer.has("omitted"));
            Assertions.assertEquals("remain 3 waiters omitted",
                    trailer.get("omitted").getAsString());
        } finally {
            lock.exclusiveUnlock();
            for (Thread t : waiters) {
                t.join();
            }
        }
    }

    @Test
    public void testWaiterCapDisabledIncludesAll() throws InterruptedException {
        Config.slow_lock_print_stack = false;
        Config.slow_lock_max_waiter_count_to_log = 0; // disable cap

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
            while (lock.getQueuedReaderThreads().size() < waiterCount
                    && System.nanoTime() < deadlineNs) {
                Thread.sleep(20);
            }

            JsonObject info = lock.getLockInfoToJson(null);
            JsonArray queued = info.getAsJsonArray("queuedReaders");
            Assertions.assertEquals(waiterCount, queued.size(),
                    "cap=0 must serialize every waiter with no trailer");
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
