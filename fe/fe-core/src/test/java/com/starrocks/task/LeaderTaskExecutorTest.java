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

package com.starrocks.task;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LeaderTaskExecutorTest {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderTaskExecutorTest.class);
    private static final int THREAD_NUM = 1;
    private static final long SLEEP_MS = 10L;

    private LeaderTaskExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new LeaderTaskExecutor("master_task_executor_test", THREAD_NUM, false);
        executor.start();
    }

    @AfterEach
    public void tearDown() {
        if (executor != null) {
            executor.close();
        }
    }

    @Test
    public void testSubmit() {
        // submit task
        LeaderTask task1 = new TestLeaderTask(1L);
        Assertions.assertTrue(executor.submit(task1));
        Assertions.assertEquals(1, executor.getTaskNum());
        // submit same running task error
        Assertions.assertFalse(executor.submit(task1));
        Assertions.assertEquals(1, executor.getTaskNum());

        // submit another task
        LeaderTask task2 = new TestLeaderTask(2L);
        Assertions.assertTrue(executor.submit(task2));
        Assertions.assertEquals(2, executor.getTaskNum());

        // wait for tasks run to end
        try {
            // checker thread interval is 1s
            // sleep 3s
            Thread.sleep(SLEEP_MS * 300);
            Assertions.assertEquals(0, executor.getTaskNum());
        } catch (InterruptedException e) {
            LOG.error("error", e);
        }
    }

    @Test
    public void testPoolSize() {
        int size = executor.getCorePoolSize();
        executor.setPoolSize(size + 1);
        Assertions.assertEquals(size + 1, executor.getCorePoolSize());
        executor.setPoolSize(size);
        Assertions.assertEquals(size, executor.getCorePoolSize());
    }

    @Test
    public void testStartAfterCloseRebuildsPoolsForReuse() {
        // close() shuts down both pools so a singleton instance (pendingLoadTaskScheduler,
        // loadingLoadTaskScheduler in GlobalStateMgr) can be used by the demotion drain. A
        // subsequent start() on the SAME instance must rebuild both pools so the next leader
        // session does not get RejectedExecutionException when scheduling tasks.
        LeaderTaskExecutor target = new LeaderTaskExecutor("leader_task_executor_reuse_test", 1, 10, false);
        target.start();
        ThreadPoolExecutor originalExecutor = target.executor;
        ScheduledThreadPoolExecutor originalSched = target.scheduledThreadPool;

        target.close(5000L);
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(originalExecutor::isTerminated);

        target.start();

        Assertions.assertNotSame(originalExecutor, target.executor, "executor must be rebuilt on restart");
        Assertions.assertNotSame(originalSched, target.scheduledThreadPool,
                "scheduledThreadPool must be rebuilt on restart");
        Assertions.assertFalse(target.executor.isShutdown());
        Assertions.assertFalse(target.scheduledThreadPool.isShutdown());

        // The rebuilt scheduler must accept new work; submit a no-op task and verify it does
        // not raise RejectedExecutionException.
        Assertions.assertTrue(target.submit(new TestLeaderTask(99L)));

        target.close(5000L);
    }

    @Test
    public void testCloseWithTimeoutLogsWhenExecutorRefusesToTerminate() {
        // close(awaitMillis) returning false from awaitTermination triggers a LOG.warn for
        // each pool that did not drain. Submit an uninterruptible task and use a very short
        // timeout budget to hit both LOG.warn branches.
        LeaderTaskExecutor target =
                new LeaderTaskExecutor("leader_task_executor_timeout_test", 1, 10, false);
        target.start();
        target.executor.execute(() -> {
            long deadline = System.currentTimeMillis() + 1500L;
            while (System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                    // simulate uninterruptible work
                }
            }
        });

        target.close(50L);

        Assertions.assertTrue(target.executor.isShutdown());
        target.executor.shutdownNow();
    }

    @Test
    public void testStartRefusesToRestartBeforePoolTerminates() {
        // Mirror of BatchWriteMgr / AlterHandler restart guard. If close() returns but the
        // underlying executor has not yet terminated (in-flight task ignoring interrupt),
        // start() must throw IllegalStateException instead of spinning up a parallel pool.
        LeaderTaskExecutor target = new LeaderTaskExecutor("leader_task_executor_refuse_test", 1, 10, false);
        target.start();
        ThreadPoolExecutor blockedPool = new java.util.concurrent.ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new java.util.concurrent.ArrayBlockingQueue<>(1));
        blockedPool.execute(() -> {
            try {
                Thread.sleep(30_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        blockedPool.shutdown();
        target.executor = blockedPool;

        try {
            Assertions.assertThrows(IllegalStateException.class, target::start);
        } finally {
            blockedPool.shutdownNow();
        }
    }

    private class TestLeaderTask extends LeaderTask {

        public TestLeaderTask(long signature) {
            this.signature = signature;
        }

        @Override
        protected void exec() {
            LOG.info("run exec. signature: {}", signature);
            try {
                Thread.sleep(SLEEP_MS);
            } catch (InterruptedException e) {
                LOG.error("error", e);
            }
        }

    }
}
