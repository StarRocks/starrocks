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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

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
}
