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

import mockit.Expectations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for slow lock detection in QueryableReentrantReadWriteLock.
 * Tests verify that slow lock detection properly triggers and captures
 * lock info when lock acquisition exceeds the configured threshold.
 */
public class QueryableReentrantReadWriteLockSlowLockTest {
    private static final long SLOW_LOCK_THRESHOLD_MS = 500;
    private static final long LOCK_HOLD_TIME_MS = 1500;

    private QueryableReentrantReadWriteLock lock;

    @BeforeEach
    public void setUp() {
        lock = new QueryableReentrantReadWriteLock(true);
    }

    /**
     * Scenario: A separate thread holds the exclusive lock for a long time, then releases it.
     * The main thread attempts to acquire a shared lock and should capture lock info about slow lock detection.
     */
    @Test
    public void testSharedLockDetectingSlowLock() throws Exception {
        new Expectations(lock) {
            {
                lock.getLockInfoToJson(null);
                minTimes = 1;
            }
        };

        // Start a background thread that holds the exclusive lock
        AtomicBoolean lockAcquired = new AtomicBoolean(false);
        Thread writerThread = new Thread(() -> {
            lock.exclusiveLock();
            lockAcquired.set(true);
            try {
                // Hold the lock for longer than the threshold
                Thread.sleep(LOCK_HOLD_TIME_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.exclusiveUnlock();
            }
        }, "WriterThread");

        writerThread.start();

        // Wait for the writer thread to acquire the lock
        while (!lockAcquired.get()) {
            Thread.sleep(1);
        }
        assertTrue(lockAcquired.get(), "Writer thread should have acquired the lock");

        // Main thread attempts to acquire shared lock with slow lock detection
        // This will timeout and trigger slow lock detection
        long startTime = System.currentTimeMillis();
        lock.sharedLockDetectingSlowLock(SLOW_LOCK_THRESHOLD_MS, TimeUnit.MILLISECONDS);
        long elapsedTime = System.currentTimeMillis() - startTime;

        // Verify that we waited for the lock to be released
        // elapseTime in [LOCK_HOLD_TIME_MS - 100, LOCK_HOLD_TIME_MS + 100]
        assertTrue(elapsedTime >= LOCK_HOLD_TIME_MS - 100,
                "Should have waited for the writer thread to release the lock. Elapsed: " + elapsedTime);
        assertTrue(elapsedTime <= LOCK_HOLD_TIME_MS + 100,
                "Should have waited for the writer thread to release the lock. Elapsed: " + elapsedTime);

        // Ensure the reader now holds the lock and can clean up
        lock.sharedUnlock();

        // Wait for writer thread to finish
        writerThread.join();
    }

    /**
     * Scenario: A separate thread holds the exclusive lock for a long time, then releases it.
     * Another thread attempts to acquire an exclusive lock and should capture lock info about slow lock detection.
     */
    @Test
    public void testExclusiveLockDetectingSlowLock() throws Exception {
        new Expectations(lock) {
            {
                lock.getLockInfoToJson(null);
                minTimes = 1;
            }
        };

        // Start a background thread that holds the exclusive lock
        AtomicBoolean lockAcquired = new AtomicBoolean(false);
        Thread writerThread1 = new Thread(() -> {
            lock.exclusiveLock();
            lockAcquired.set(true);
            try {
                // Hold the lock for longer than the threshold
                Thread.sleep(LOCK_HOLD_TIME_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.exclusiveUnlock();
            }
        }, "WriterThread1");

        writerThread1.start();

        // Wait for the writer thread to acquire the lock
        while (!lockAcquired.get()) {
            Thread.sleep(1);
        }
        assertTrue(lockAcquired.get(), "Writer thread should have acquired the lock");

        // Main thread attempts to acquire exclusive lock with slow lock detection
        // This will timeout and trigger slow lock detection
        long startTime = System.currentTimeMillis();
        lock.exclusiveLockDetectingSlowLock(SLOW_LOCK_THRESHOLD_MS, TimeUnit.MILLISECONDS);
        long elapsedTime = System.currentTimeMillis() - startTime;

        // Verify that we waited for the lock to be released
        // elapseTime in [LOCK_HOLD_TIME_MS - 100, LOCK_HOLD_TIME_MS + 100]
        assertTrue(elapsedTime >= LOCK_HOLD_TIME_MS - 100,
                "Should have waited for the writer thread to release the lock. Elapsed: " + elapsedTime);
        assertTrue(elapsedTime <= LOCK_HOLD_TIME_MS + 100,
                "Should have waited for the writer thread to release the lock. Elapsed: " + elapsedTime);

        // Ensure the current thread now holds the lock and can clean up
        lock.exclusiveUnlock();

        // Wait for writer thread to finish
        writerThread1.join();
    }

    /**
     * Test that sharedLockDetectingSlowLock() does NOT capture lock info when lock is acquired within threshold.
     * Scenario: No contention on the lock, acquisition should be fast.
     */
    @Test
    public void testSharedLockNoSlowLockWhenFast() {
        new Expectations(lock) {
            {
                lock.getLockInfoToJson(null);
                maxTimes = 0;
                minTimes = 0;
            }
        };

        // Directly acquire shared lock with no contention
        lock.sharedLockDetectingSlowLock(SLOW_LOCK_THRESHOLD_MS, TimeUnit.MILLISECONDS);

        // Verify the lock is held
        assertTrue(lock.isReadLockHeldByCurrentThread(), "Current thread should hold the read lock");

        // Clean up
        lock.sharedUnlock();
    }

    /**
     * Test that exclusiveLockDetectingSlowLock() does NOT capture lock info when lock is acquired within threshold.
     * Scenario: No contention on the lock, acquisition should be fast.
     */
    @Test
    public void testExclusiveLockNoSlowLockWhenFast() {
        new Expectations(lock) {
            {
                lock.getLockInfoToJson(null);
                maxTimes = 0;
                minTimes = 0;
            }
        };

        // Directly acquire exclusive lock with no contention
        lock.exclusiveLockDetectingSlowLock(SLOW_LOCK_THRESHOLD_MS, TimeUnit.MILLISECONDS);

        // Verify the lock is held
        assertTrue(lock.isWriteLockHeldByCurrentThread(), "Current thread should hold the write lock");

        // Clean up
        lock.exclusiveUnlock();
    }

    /**
     * Test concurrent scenario: Multiple threads contending for shared lock while exclusive lock is held.
     * Verifies that all waiting threads properly detect and capture the slow lock.
     */
    @Test
    public void testMultipleReadersDetectSlowLock() throws Exception {
        new Expectations(lock) {
            {
                lock.getLockInfoToJson(null);
                minTimes = 2;
            }
        };

        // Start a writer thread holding the lock
        AtomicBoolean writerLockAcquired = new AtomicBoolean(false);
        Thread writerThread = new Thread(() -> {
            lock.exclusiveLock();
            writerLockAcquired.set(true);
            try {
                Thread.sleep(LOCK_HOLD_TIME_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.exclusiveUnlock();
            }
        }, "Writer");

        writerThread.start();
        while (!writerLockAcquired.get()) {
            Thread.sleep(1);
        }
        assertTrue(writerLockAcquired.get(), "Writer should have acquired the lock");

        // Start multiple reader threads that will wait for the shared lock
        Thread reader1 = new Thread(() -> {
            lock.sharedLockDetectingSlowLock(SLOW_LOCK_THRESHOLD_MS, TimeUnit.MILLISECONDS);
            lock.sharedUnlock();
        }, "Reader1");

        Thread reader2 = new Thread(() -> {
            lock.sharedLockDetectingSlowLock(SLOW_LOCK_THRESHOLD_MS, TimeUnit.MILLISECONDS);
            lock.sharedUnlock();
        }, "Reader2");

        reader1.start();
        reader2.start();

        // Wait for all threads to complete
        writerThread.join();
        reader1.join();
        reader2.join();
    }
}

