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


package com.starrocks.task;

import com.starrocks.common.PriorityThreadPoolExecutor;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PriorityLeaderTaskExecutorTest {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityLeaderTaskExecutorTest.class);
    private static final int THREAD_NUM = 1;
    private static final long SLEEP_MS = 200L;

    private static List<Long> SEQ = new ArrayList<>();

    private PriorityLeaderTaskExecutor executor;

    @BeforeEach
    public void setUp() {
        SEQ.clear();
        executor = new PriorityLeaderTaskExecutor("priority_task_executor_test", THREAD_NUM, 100, false);
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
        PriorityLeaderTask task1 = new TestLeaderTask(1L);
        Assertions.assertTrue(executor.submit(task1));
        Assertions.assertEquals(1, executor.getTaskNum());
        // submit same running task error
        Assertions.assertFalse(executor.submit(task1));
        Assertions.assertEquals(1, executor.getTaskNum());

        // submit another task
        PriorityLeaderTask task2 = new TestLeaderTask(2L);
        Assertions.assertTrue(executor.submit(task2));
        Assertions.assertEquals(2, executor.getTaskNum());

        // submit priority task
        PriorityLeaderTask task3 = new TestLeaderTask(3L, 1);
        Assertions.assertTrue(executor.submit(task3));
        Assertions.assertEquals(3, executor.getTaskNum());

        // submit priority task
        PriorityLeaderTask task4 = new TestLeaderTask(4L);
        Assertions.assertTrue(executor.submit(task4));
        Assertions.assertEquals(4, executor.getTaskNum());

        Assertions.assertTrue(executor.updatePriority(4L, 5));

        // wait for tasks run to end
        try {
            Thread.sleep(2000);
            Assertions.assertEquals(0, executor.getTaskNum());
        } catch (InterruptedException e) {
            LOG.error("error", e);
        }

        Assertions.assertEquals(4, SEQ.size());
        Assertions.assertEquals(1L, SEQ.get(0).longValue());
        Assertions.assertEquals(4L, SEQ.get(1).longValue());
        Assertions.assertEquals(3L, SEQ.get(2).longValue());
        Assertions.assertEquals(2L, SEQ.get(3).longValue());
    }

    @Test
    public void testUpdatePoolSize() {
        PriorityThreadPoolExecutor priorityExecutor = executor.executor;
        Assertions.assertEquals(THREAD_NUM, executor.getCorePoolSize());
        Assertions.assertEquals(THREAD_NUM, priorityExecutor.getMaximumPoolSize());

        // set from 1 to 2
        int newThreadNum = THREAD_NUM + 1;
        executor.setPoolSize(newThreadNum);
        Assertions.assertEquals(newThreadNum, executor.getCorePoolSize());
        Assertions.assertEquals(newThreadNum, priorityExecutor.getMaximumPoolSize());

        // set from 2 to 1
        executor.setPoolSize(THREAD_NUM);
        Assertions.assertEquals(THREAD_NUM, executor.getCorePoolSize());
        Assertions.assertEquals(THREAD_NUM, priorityExecutor.getMaximumPoolSize());
    }

    @Test
    public void testStartAfterCloseRebuildsPoolsForReuse() {
        // close() shuts down both pools so a singleton instance can be used by the demotion
        // drain. A subsequent start() on the SAME instance must rebuild both pools so the next
        // leader session does not get RejectedExecutionException when scheduling tasks.
        PriorityLeaderTaskExecutor target =
                new PriorityLeaderTaskExecutor("priority_task_executor_reuse_test", 1, 100, false);
        target.start();
        PriorityThreadPoolExecutor originalExecutor = target.executor;
        ScheduledThreadPoolExecutor originalSched = target.scheduledThreadPool;

        target.close(5000L);
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(originalExecutor::isTerminated);

        target.start();

        Assertions.assertNotSame(originalExecutor, target.executor, "executor must be rebuilt on restart");
        Assertions.assertNotSame(originalSched, target.scheduledThreadPool,
                "scheduledThreadPool must be rebuilt on restart");
        Assertions.assertFalse(target.executor.isShutdown());
        Assertions.assertFalse(target.scheduledThreadPool.isShutdown());

        // The rebuilt executor must accept new work; submit a no-op task and verify it does
        // not raise RejectedExecutionException.
        Assertions.assertTrue(target.submit(new TestLeaderTask(99L)));

        target.close(5000L);
    }

    @Test
    public void testStartRefusesToRestartBeforePoolTerminates() {
        // Mirror of BatchWriteMgr / AlterHandler restart guard. If close() returns but the
        // underlying executor has not yet terminated, start() must throw IllegalStateException
        // instead of spinning up a parallel pool against the same runningTasks map.
        PriorityLeaderTaskExecutor target =
                new PriorityLeaderTaskExecutor("priority_task_executor_refuse_test", 1, 100, false);
        target.start();
        PriorityThreadPoolExecutor blockedPool = com.starrocks.common.ThreadPoolManager
                .newDaemonFixedPriorityThreadPool(
                        1, 100, "priority_task_executor_refuse_test_blocked_pool", false);
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

    @Test
    public void testCloseWithTimeoutAwaitsTermination() throws Exception {
        // close(awaitMillis) must block until BOTH pools have actually terminated, so a
        // re-elected leader does not race a still-alive worker from the previous session.
        PriorityLeaderTaskExecutor target =
                new PriorityLeaderTaskExecutor("priority_task_executor_close_test", 1, 100, false);
        target.start();
        PriorityThreadPoolExecutor inner = target.executor;
        ScheduledThreadPoolExecutor sched = target.scheduledThreadPool;

        target.close(5000L);

        Assertions.assertTrue(inner.isShutdown(), "executor must be shutdown");
        Assertions.assertTrue(inner.isTerminated(), "executor must be terminated after close(awaitMillis)");
        Assertions.assertTrue(sched.isShutdown(), "scheduledThreadPool must be shutdown");
        Assertions.assertTrue(sched.isTerminated(),
                "scheduledThreadPool must be terminated after close(awaitMillis)");
    }

    private class TestLeaderTask extends PriorityLeaderTask {

        public TestLeaderTask(long signature) {
            this.signature = signature;
        }

        public TestLeaderTask(long signature, int priority) {
            super(priority);
            this.signature = signature;
        }

        @Override
        protected void exec() {
            LOG.info("run exec. signature: {}, priority: {}", signature, getPriority());
            SEQ.add(signature);
            try {
                Thread.sleep(SLEEP_MS);
            } catch (InterruptedException e) {
                LOG.error("error", e);
            }
        }

    }
}
