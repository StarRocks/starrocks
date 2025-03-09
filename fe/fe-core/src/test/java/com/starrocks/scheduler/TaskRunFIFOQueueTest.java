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


package com.starrocks.scheduler;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class TaskRunFIFOQueueTest {

    private static final int N = 100;
    private static final int M = 5;
    private static ConnectContext connectContext;

    @Before
    public void setUp() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getNextId();
                minTimes = 0;
                returns(100L, 101L, 102L, 103L, 104L, 105L);

            }
        };
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync) {
        return makeExecuteOption(isMergeRedundant, isSync, 0);
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority) {
        ExecuteOption executeOption = new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(), isMergeRedundant,
                Maps.newHashMap());
        executeOption.setSync(isSync);
        executeOption.setPriority(priority);
        return executeOption;
    }

    private TaskRun makeTaskRun(long taskId, Task task, ExecuteOption executeOption) {
        return makeTaskRun(taskId, task, executeOption, -1);
    }

    private TaskRun makeTaskRun(long taskId, Task task, ExecuteOption executeOption, long createTime) {
        TaskRun taskRun = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(executeOption)
                .build();
        taskRun.setTaskId(taskId);
        // submitTaskRun needs task run status is empty
        if (createTime >= 0) {
            taskRun.initStatus("1", createTime);
            taskRun.getStatus().setPriority(executeOption.getPriority());
        }
        return taskRun;
    }

    @Test
    public void testTaskRunsWithDifferentCreateTime() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;
        List<TaskRun> taskRuns = Lists.newArrayList();
        TaskRunFIFOQueue queue = new TaskRunFIFOQueue();
        for (int i = 0; i < N; i++) {
            TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, 0), i);
            taskRuns.add(taskRun);
            queue.add(taskRun);
        }
        Assert.assertTrue(queue.size() == N);
        Assert.assertTrue(!queue.isEmpty());
        List<TaskRun> pendingTaskRuns = queue.getCopiedPendingTaskRuns();
        for (int i = 0; i < N; i++) {
            TaskRun taskRun = queue.poll(Predicates.alwaysTrue());
            Assert.assertTrue(taskRun.equals(pendingTaskRuns.get(i)));
            Assert.assertTrue(taskRun.equals(taskRuns.get(i)));
        }
        Assert.assertTrue(queue.isEmpty());
    }

    @Test
    public void testTaskRunsWithDifferentCreatePriority() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;
        List<TaskRun> taskRuns = Lists.newArrayList();
        TaskRunFIFOQueue queue = new TaskRunFIFOQueue();
        for (int i = 0; i < N; i++) {
            TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, i), i);
            taskRuns.add(taskRun);
            queue.add(taskRun);
        }
        Assert.assertTrue(queue.size() == N);
        Assert.assertTrue(!queue.isEmpty());
        List<TaskRun> pendingTaskRuns = queue.getCopiedPendingTaskRuns();
        for (int i = 0; i < N; i++) {
            TaskRun taskRun = queue.poll(Predicates.alwaysTrue());
            Assert.assertTrue(taskRun.equals(pendingTaskRuns.get(i)));
            Assert.assertTrue(taskRun.equals(taskRuns.get(N - 1 - i)));
        }
        Assert.assertTrue(queue.isEmpty());
    }

    @Test
    public void testGetAndPollWithMultiThreads() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;
        Set<TaskRun> taskRuns = Sets.newConcurrentHashSet();
        Set<TaskRun> result = Sets.newConcurrentHashSet();
        TaskRunFIFOQueue queue = new TaskRunFIFOQueue();
        {
            List<Thread> threads = Lists.newArrayList();
            for (int i = 0; i < M; i++) {
                Thread t = new Thread(() -> {
                    for (int j = 0; j < N / M; j++) {
                        TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, j), j);
                        queue.add(taskRun);
                        taskRuns.add(taskRun);
                    }
                });
                threads.add(t);
            }
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Assert.fail("join failed");
                }
            }
            Assert.assertTrue(queue.size() == taskRuns.size());
        }

        {
            List<Thread> threads = Lists.newArrayList();
            for (int i = 0; i < M; i++) {
                Thread t = new Thread(() -> {
                    while (!queue.isEmpty()) {
                        TaskRun taskRun = queue.poll(Predicates.alwaysTrue());
                        if (taskRun == null) {
                            continue;
                        }
                        Assert.assertTrue(taskRuns.contains(taskRun));
                        result.add(taskRun);
                    }
                });
                threads.add(t);
            }
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Assert.fail("join failed");
                }
            }
            Assert.assertTrue(result.size() == taskRuns.size());
            Assert.assertTrue(queue.isEmpty());
        }
    }
}