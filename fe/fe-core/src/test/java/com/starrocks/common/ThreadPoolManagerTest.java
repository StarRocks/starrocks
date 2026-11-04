// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public class ThreadPoolManagerTest {

    @Test
    public void testCheckpointThreadDoesNotBufferMetrics() throws Exception {
        Field checkpointThreadIdField = GlobalStateMgr.class.getDeclaredField("checkpointThreadId");
        checkpointThreadIdField.setAccessible(true);
        long previousCheckpointThreadId = checkpointThreadIdField.getLong(null);

        Field poolMapField = ThreadPoolManager.class.getDeclaredField("nameToThreadPoolMap");
        poolMapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, ThreadPoolExecutor> poolMap =
                (Map<String, ThreadPoolExecutor>) poolMapField.get(null);

        String daemonPoolName = "checkpoint-daemon-pool";
        String priorityPoolName = "checkpoint-priority-pool";
        String scheduledPoolName = "checkpoint-scheduled-pool";
        ThreadPoolExecutor daemonPool = null;
        PriorityThreadPoolExecutor priorityPool = null;
        ScheduledThreadPoolExecutor scheduledPool = null;
        try {
            checkpointThreadIdField.setLong(null, Thread.currentThread().getId());
            daemonPool = ThreadPoolManager.newDaemonCacheThreadPool(1, daemonPoolName, true);
            priorityPool = ThreadPoolManager.newDaemonFixedPriorityThreadPool(1, 1, priorityPoolName, true);
            scheduledPool = ThreadPoolManager.newDaemonScheduledThreadPool(1, scheduledPoolName, true);

            Assertions.assertFalse(poolMap.containsKey(daemonPoolName));
            Assertions.assertFalse(poolMap.containsKey(priorityPoolName));
            Assertions.assertFalse(poolMap.containsKey(scheduledPoolName));
        } finally {
            checkpointThreadIdField.setLong(null, previousCheckpointThreadId);
            poolMap.remove(daemonPoolName);
            poolMap.remove(priorityPoolName);
            poolMap.remove(scheduledPoolName);
            if (daemonPool != null) {
                daemonPool.shutdownNow();
            }
            if (priorityPool != null) {
                priorityPool.shutdownNow();
            }
            if (scheduledPool != null) {
                scheduledPool.shutdownNow();
            }
        }
    }

    @Test
    public void testNormal() throws InterruptedException {
        ThreadPoolExecutor testCachedPool = ThreadPoolManager.newDaemonCacheThreadPool(2, "test_cache_pool", true);
        ThreadPoolExecutor testFixedThreaddPool = ThreadPoolManager.newDaemonFixedThreadPool(2, 2,
                "test_fixed_thread_pool", true);

        ThreadPoolManager.registerThreadPoolMetric("test_cache_pool", testCachedPool);
        ThreadPoolManager.registerThreadPoolMetric("test_fixed_thread_pool", testFixedThreaddPool);

        List<Metric> metricList = MetricRepo.getMetricsByName("thread_pool");

        Assertions.assertEquals(8, metricList.size());
        Assertions.assertEquals(ThreadPoolManager.LogDiscardPolicy.class,
                testCachedPool.getRejectedExecutionHandler().getClass());
        Assertions.assertEquals(ThreadPoolManager.BlockedPolicy.class,
                testFixedThreaddPool.getRejectedExecutionHandler().getClass());

        Runnable task = () -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        for (int i = 0; i < 4; i++) {
            testCachedPool.submit(task);
        }

        Thread.sleep(200);

        Assertions.assertEquals(2, testCachedPool.getPoolSize());
        Assertions.assertTrue(2 >= testCachedPool.getActiveCount());
        Assertions.assertEquals(0, testCachedPool.getQueue().size());
        Assertions.assertTrue(0 <= testCachedPool.getCompletedTaskCount());

        Thread.sleep(1500);

        Assertions.assertEquals(2, testCachedPool.getPoolSize());
        Assertions.assertEquals(0, testCachedPool.getActiveCount());
        Assertions.assertEquals(0, testCachedPool.getQueue().size());
        Assertions.assertEquals(2, testCachedPool.getCompletedTaskCount());

        for (int i = 0; i < 4; i++) {
            testFixedThreaddPool.submit(task);
        }
        Thread.sleep(200);

        Assertions.assertEquals(2, testFixedThreaddPool.getPoolSize());
        Assertions.assertEquals(2, testFixedThreaddPool.getActiveCount());
        Assertions.assertEquals(2, testFixedThreaddPool.getQueue().size());
        Assertions.assertEquals(0, testFixedThreaddPool.getCompletedTaskCount());

        Thread.sleep(4500);

        Assertions.assertEquals(2, testFixedThreaddPool.getPoolSize());
        Assertions.assertEquals(0, testFixedThreaddPool.getActiveCount());
        Assertions.assertEquals(0, testFixedThreaddPool.getQueue().size());
        Assertions.assertEquals(4, testFixedThreaddPool.getCompletedTaskCount());

    }

    @Test
    public void testSetCacheThreadPoolSize() {
        ThreadPoolExecutor executor =
                ThreadPoolManager.newDaemonCacheThreadPool(1024, "testExecutor1", false);
        Assertions.assertEquals(0, executor.getCorePoolSize());
        Assertions.assertEquals(1024, executor.getMaximumPoolSize());

        ThreadPoolManager.setCacheThreadPoolSize(executor, 4096);
        Assertions.assertEquals(0, executor.getCorePoolSize());
        Assertions.assertEquals(4096, executor.getMaximumPoolSize());

        ThreadPoolManager.setCacheThreadPoolSize(executor, 1);
        Assertions.assertEquals(0, executor.getCorePoolSize());
        Assertions.assertEquals(1, executor.getMaximumPoolSize());

        ThreadPoolManager.setCacheThreadPoolSize(executor, -1);
        Assertions.assertEquals(0, executor.getCorePoolSize());
        Assertions.assertEquals(1, executor.getMaximumPoolSize());

        ThreadPoolExecutor fixedExecutor =
                ThreadPoolManager.newDaemonFixedThreadPool(10, 100, "testExecutor2", false);
        ThreadPoolManager.setCacheThreadPoolSize(fixedExecutor, 1);
        Assertions.assertEquals(10, fixedExecutor.getCorePoolSize());
        Assertions.assertEquals(10, fixedExecutor.getMaximumPoolSize());
    }

    @Test
    public void testFixedThreadPoolWithAbortPolicy() {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPoolWithAbortPolicy(
                1, 1, "test_abort_policy_pool", false);
        CountDownLatch occupied = new CountDownLatch(1);
        try {
            Assertions.assertEquals(ThreadPoolManager.FastAbortPolicy.class,
                    executor.getRejectedExecutionHandler().getClass());

            // the single thread takes the first task, the single queue slot takes the second,
            // and the third has nowhere to go: it must fail the submission rather than wait
            executor.execute(() -> {
                try {
                    occupied.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            executor.execute(() -> {
            });
            Assertions.assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> {
            }));
        } finally {
            occupied.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testSetFixedThreadPoolSize() {
        int expectedPoolSize = 2;
        ThreadPoolExecutor testPool =
                ThreadPoolManager.newDaemonFixedThreadPool(expectedPoolSize, 4096, "testPool", false);
        Assertions.assertEquals(expectedPoolSize, testPool.getCorePoolSize());
        Assertions.assertEquals(expectedPoolSize, testPool.getMaximumPoolSize());

        { // increase the pool size, no problem
            expectedPoolSize = 10;
            int poolSize = expectedPoolSize;
            ExceptionChecker.expectThrowsNoException(
                    () -> ThreadPoolManager.setFixedThreadPoolSize(testPool, poolSize));
            Assertions.assertEquals(expectedPoolSize, testPool.getCorePoolSize());
            Assertions.assertEquals(expectedPoolSize, testPool.getMaximumPoolSize());
        }

        { // decrease the pool size, no problem
            expectedPoolSize = 5;
            int poolSize = expectedPoolSize;
            ExceptionChecker.expectThrowsNoException(
                    () -> ThreadPoolManager.setFixedThreadPoolSize(testPool, poolSize));
            Assertions.assertEquals(expectedPoolSize, testPool.getCorePoolSize());
            Assertions.assertEquals(expectedPoolSize, testPool.getMaximumPoolSize());
        }

        // can't set to <= 0
        Assertions.assertThrows(IllegalArgumentException.class, () -> ThreadPoolManager.setFixedThreadPoolSize(testPool, 0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ThreadPoolManager.setFixedThreadPoolSize(testPool, -1));
    }

    @Test
    public void testCacheThreadPoolWithQueueRunsTasksConcurrently() throws InterruptedException {
        int numThreads = 4;
        ThreadPoolExecutor executor =
                ThreadPoolManager.newDaemonCacheThreadPool(numThreads, 16, "test_cache_queue_pool", false);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Assertions.assertEquals(numThreads, executor.getCorePoolSize());
            Assertions.assertEquals(numThreads, executor.getMaximumPoolSize());
            Assertions.assertTrue(executor.allowsCoreThreadTimeOut());

            // Tasks block until released, so all of them can only be running at once if the pool
            // starts a thread per task instead of queueing behind a single worker.
            CountDownLatch started = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                executor.execute(() -> {
                    started.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(numThreads, executor.getActiveCount());
            Assertions.assertEquals(0, executor.getQueue().size());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testScalingThreadPoolHandsTaskToIdleThread() throws InterruptedException {
        ThreadPoolExecutor executor =
                ThreadPoolManager.newDaemonScalingThreadPool(1, 4, "test_scaling_idle_pool", false);
        ThreadPoolManager.ScalingQueue queue = (ThreadPoolManager.ScalingQueue) executor.getQueue();
        try {
            for (int i = 0; i < 20; i++) {
                CountDownLatch done = new CountDownLatch(1);
                executor.execute(done::countDown);
                Assertions.assertTrue(done.await(10, TimeUnit.SECONDS));
                // the next task is submitted only once the thread waits for it again
                waitUntil(queue::hasWaitingConsumer);
            }
            // a fixed pool would have started a thread per task up to its size; this one reused the idle one
            Assertions.assertEquals(1, executor.getLargestPoolSize());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testScalingThreadPoolGrowsToMaxThenQueues() throws InterruptedException {
        int maxThreads = 4;
        int numTasks = maxThreads + 3;
        ThreadPoolExecutor executor =
                ThreadPoolManager.newDaemonScalingThreadPool(1, maxThreads, "test_scaling_max_pool", false);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Assertions.assertEquals(ThreadPoolManager.ForceQueuePolicy.class,
                    executor.getRejectedExecutionHandler().getClass());
            CountDownLatch started = new CountDownLatch(maxThreads);
            CountDownLatch done = new CountDownLatch(numTasks);
            Runnable blockingTask = () -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                done.countDown();
            };

            // blocking tasks each get a thread of their own, up to the maximum
            for (int i = 0; i < maxThreads; i++) {
                executor.execute(blockingTask);
            }
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(maxThreads, executor.getPoolSize());

            // with every thread busy, more tasks neither block the submitter nor get rejected: they wait
            for (int i = maxThreads; i < numTasks; i++) {
                executor.execute(blockingTask);
            }
            Assertions.assertEquals(maxThreads, executor.getPoolSize());
            Assertions.assertEquals(numTasks - maxThreads, executor.getQueue().size());

            release.countDown();
            Assertions.assertTrue(done.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(maxThreads, executor.getLargestPoolSize());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testScalingThreadPoolShrinksToCoreWhenIdle() throws InterruptedException {
        int maxThreads = 4;
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonScalingThreadPool(1, maxThreads,
                100, TimeUnit.MILLISECONDS, "test_scaling_shrink_pool", false);
        CountDownLatch release = new CountDownLatch(1);
        try {
            CountDownLatch started = new CountDownLatch(maxThreads);
            for (int i = 0; i < maxThreads; i++) {
                executor.execute(() -> {
                    started.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            release.countDown();

            // the threads started for the burst exit once idle for the keep-alive time; the core thread
            // stays, so a task queued at the maximum always has a thread to run it
            waitUntil(() -> executor.getPoolSize() == 1);
            Assertions.assertFalse(executor.allowsCoreThreadTimeOut());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testScalingThreadPoolRejectsAfterShutdown() {
        ThreadPoolExecutor executor =
                ThreadPoolManager.newDaemonScalingThreadPool(1, 4, "test_scaling_shutdown_pool", false);
        executor.shutdown();
        Assertions.assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> {
        }));
        Assertions.assertEquals(0, executor.getQueue().size());
    }

    @Test
    public void testSetScalingThreadPoolSize() {
        ThreadPoolExecutor executor =
                ThreadPoolManager.newDaemonScalingThreadPool(4, 16, "test_scaling_resize_pool", false);
        try {
            ThreadPoolManager.setScalingThreadPoolSize(executor, 4, 64);
            Assertions.assertEquals(4, executor.getCorePoolSize());
            Assertions.assertEquals(64, executor.getMaximumPoolSize());

            // a maximum below the core size takes the core size down with it
            ThreadPoolManager.setScalingThreadPoolSize(executor, 4, 2);
            Assertions.assertEquals(2, executor.getCorePoolSize());
            Assertions.assertEquals(2, executor.getMaximumPoolSize());

            ThreadPoolManager.setScalingThreadPoolSize(executor, 4, 8);
            Assertions.assertEquals(4, executor.getCorePoolSize());
            Assertions.assertEquals(8, executor.getMaximumPoolSize());

            // a non-positive maximum is ignored rather than thrown from the config refresh listener
            ThreadPoolManager.setScalingThreadPoolSize(executor, 4, 0);
            ThreadPoolManager.setScalingThreadPoolSize(executor, 4, -1);
            Assertions.assertEquals(4, executor.getCorePoolSize());
            Assertions.assertEquals(8, executor.getMaximumPoolSize());

            // the pool keeps at least one core thread
            ThreadPoolManager.setScalingThreadPoolSize(executor, 0, 8);
            Assertions.assertEquals(1, executor.getCorePoolSize());
            Assertions.assertEquals(8, executor.getMaximumPoolSize());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testScalingThreadPoolNeedsACoreThread() {
        // with no core thread, a task queued at the maximum could be left with no thread to run it
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ThreadPoolManager.newDaemonScalingThreadPool(0, 4, "test_scaling_no_core_pool", false));
    }

    @Test
    public void testRaisingScalingThreadPoolMaxStartsQueuedTasks() throws InterruptedException {
        int oldMax = 2;
        int newMax = 5;
        ThreadPoolExecutor executor =
                ThreadPoolManager.newDaemonScalingThreadPool(1, oldMax, "test_scaling_raise_pool", false);
        CountDownLatch release = new CountDownLatch(1);
        try {
            CountDownLatch started = new CountDownLatch(newMax);
            for (int i = 0; i < newMax; i++) {
                executor.execute(() -> {
                    started.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            waitUntil(() -> started.getCount() == newMax - oldMax);
            Assertions.assertEquals(newMax - oldMax, executor.getQueue().size());

            // nothing else is submitted: the tasks queued at the old maximum must start on their own
            ThreadPoolManager.setScalingThreadPoolSize(executor, 1, newMax);
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(newMax, executor.getPoolSize());
            Assertions.assertEquals(0, executor.getQueue().size());
            Assertions.assertEquals(1, executor.getCorePoolSize());
            Assertions.assertEquals(newMax, executor.getMaximumPoolSize());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testUnchangedResizeLetsIdleThreadsExit() throws InterruptedException {
        int maxThreads = 4;
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonScalingThreadPool(1, maxThreads,
                500, TimeUnit.MILLISECONDS, "test_scaling_refresh_pool", false);
        CountDownLatch release = new CountDownLatch(1);
        try {
            CountDownLatch started = new CountDownLatch(maxThreads);
            for (int i = 0; i < maxThreads; i++) {
                executor.execute(() -> {
                    started.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            release.countDown();

            // The config refresh resizes the pool to the same size every 10 s. Done every 50 ms here, it must not
            // keep the threads above the core alive: each still exits after its 500 ms keep-alive.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (executor.getPoolSize() > 1 && System.nanoTime() < deadline) {
                ThreadPoolManager.setScalingThreadPoolSize(executor, 1, maxThreads);
                Thread.sleep(50);
            }
            Assertions.assertEquals(1, executor.getPoolSize());
            Assertions.assertEquals(1, executor.getCorePoolSize());
            Assertions.assertEquals(maxThreads, executor.getMaximumPoolSize());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            Assertions.assertTrue(System.nanoTime() < deadline, "condition not met in 10 seconds");
            Thread.sleep(1);
        }
    }
}
