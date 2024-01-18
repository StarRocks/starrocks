// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.task;

import com.starrocks.common.PriorityThreadPoolExecutor;
import com.starrocks.common.jmockit.Deencapsulation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PriorityLeaderTaskExecutorTest {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityLeaderTaskExecutorTest.class);
    private static final int THREAD_NUM = 1;
    private static final long SLEEP_MS = 200L;

    private static List<Long> SEQ = new ArrayList<>();

    private PriorityLeaderTaskExecutor executor;

    @Before
    public void setUp() {
        executor = new PriorityLeaderTaskExecutor("priority_task_executor_test", THREAD_NUM, 100, false);
        executor.start();
    }

    @After
    public void tearDown() {
        if (executor != null) {
            executor.close();
        }
    }

    @Test
    public void testSubmit() {
        // submit task
        PriorityLeaderTask task1 = new TestLeaderTask(1L);
        Assert.assertTrue(executor.submit(task1));
        Assert.assertEquals(1, executor.getTaskNum());
        // submit same running task error
        Assert.assertFalse(executor.submit(task1));
        Assert.assertEquals(1, executor.getTaskNum());

        // submit another task
        PriorityLeaderTask task2 = new TestLeaderTask(2L);
        Assert.assertTrue(executor.submit(task2));
        Assert.assertEquals(2, executor.getTaskNum());

        // submit priority task
        PriorityLeaderTask task3 = new TestLeaderTask(3L, 1);
        Assert.assertTrue(executor.submit(task3));
        Assert.assertEquals(3, executor.getTaskNum());

        // submit priority task
        PriorityLeaderTask task4 = new TestLeaderTask(4L);
        Assert.assertTrue(executor.submit(task4));
        Assert.assertEquals(4, executor.getTaskNum());

        Assert.assertTrue(executor.updatePriority(4L, 5));

        // wait for tasks run to end
        try {
            Thread.sleep(2000);
            Assert.assertEquals(0, executor.getTaskNum());
        } catch (InterruptedException e) {
            LOG.error("error", e);
        }

        Assert.assertEquals(4, SEQ.size());
        Assert.assertEquals(1L, SEQ.get(0).longValue());
        Assert.assertEquals(4L, SEQ.get(1).longValue());
        Assert.assertEquals(3L, SEQ.get(2).longValue());
        Assert.assertEquals(2L, SEQ.get(3).longValue());
    }

    @Test
    public void testUpdatePoolSize() {
        PriorityThreadPoolExecutor priorityExecutor = Deencapsulation.getField(executor, "executor");
        Assert.assertEquals(THREAD_NUM, executor.getCorePoolSize());
        Assert.assertEquals(THREAD_NUM, priorityExecutor.getMaximumPoolSize());

        // set from 1 to 2
        int newThreadNum = THREAD_NUM + 1;
        executor.setPoolSize(newThreadNum);
        Assert.assertEquals(newThreadNum, executor.getCorePoolSize());
        Assert.assertEquals(newThreadNum, priorityExecutor.getMaximumPoolSize());

        // set from 2 to 1
        executor.setPoolSize(THREAD_NUM);
        Assert.assertEquals(THREAD_NUM, executor.getCorePoolSize());
        Assert.assertEquals(THREAD_NUM, priorityExecutor.getMaximumPoolSize());
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
