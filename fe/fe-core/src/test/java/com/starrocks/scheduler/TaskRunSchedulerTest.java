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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class TaskRunSchedulerTest {

    private static final int N = 100;
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
        ExecuteOption executeOption = new ExecuteOption(isMergeRedundant);
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
    public void testTaskRunSchedulerWithDifferentPriority() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;
        List<TaskRun> taskRuns = Lists.newArrayList();

        TaskRunScheduler scheduler = new TaskRunScheduler();
        for (int i = 0; i < N; i++) {
            TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, i), 1);
            taskRuns.add(taskRun);
            scheduler.addPendingTaskRun(taskRun);
        }
        Assert.assertTrue(scheduler.getCopiedPendingTaskRuns().size() == N);

        List<TaskRun> queue = scheduler.getCopiedPendingTaskRuns();
        Assert.assertEquals(N, queue.size());

        List<TaskRun> pendingTaskRuns = scheduler.getCopiedPendingTaskRuns();
        for (int i = 0; i < N; i++) {
            int j = i;
            scheduler.scheduledPendingTaskRun(taskRun -> {
                Assert.assertTrue(taskRun.equals(taskRuns.get(N - 1 - j)));
                Assert.assertTrue(taskRun.equals(pendingTaskRuns.get(j)));
            });
        }
    }

    @Test
    public void testTaskRunSchedulerWithDifferentCreateTime() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;
        List<TaskRun> taskRuns = Lists.newArrayList();

        TaskRunScheduler scheduler = new TaskRunScheduler();
        for (int i = 0; i < N; i++) {
            TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, 1), i);
            taskRuns.add(taskRun);
            scheduler.addPendingTaskRun(taskRun);
        }
        Assert.assertTrue(scheduler.getCopiedPendingTaskRuns().size() == N);

        List<TaskRun> queue = scheduler.getCopiedPendingTaskRuns();
        Assert.assertEquals(N, queue.size());

        for (int i = 0; i < N; i++) {
            TaskRun taskRun = queue.get(i);
            Assert.assertTrue(taskRun.equals(taskRuns.get(i)));
        }
    }

    @Test
    public void testScheduledPendingTaskRun() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        List<TaskRun> taskRuns = Lists.newArrayList();
        TaskRunScheduler scheduler = new TaskRunScheduler();
        for (int i = 0; i < N; i++) {
            TaskRun taskRun = makeTaskRun(i, task, makeExecuteOption(true, false, 1), i);
            taskRuns.add(taskRun);
            scheduler.addPendingTaskRun(taskRun);
        }

        Set<TaskRun> runningTaskRuns = Sets.newHashSet(taskRuns.subList(0, Config.task_runs_concurrency));
        scheduler.scheduledPendingTaskRun(taskRun -> {
            Assert.assertTrue(runningTaskRuns.contains(taskRun));
        });
        Assert.assertTrue(scheduler.getRunningTaskCount() == Config.task_runs_concurrency);
        Assert.assertTrue(scheduler.getPendingQueueCount() == N - Config.task_runs_concurrency);
        for (int i = 0; i < Config.task_runs_concurrency; i++) {
            Assert.assertTrue(scheduler.getRunnableTaskRun(i).equals(taskRuns.get(i)));
        }
        for (int i = Config.task_runs_concurrency; i < N; i++) {
            Assert.assertTrue(scheduler.getRunnableTaskRun(i).equals(taskRuns.get(i)));
        }
    }

    @Test
    public void testScheduledPendingTaskRunWithSameTaskId() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        List<TaskRun> taskRuns = Lists.newArrayList();
        TaskRunScheduler scheduler = new TaskRunScheduler();
        for (int i = 0; i < 10; i++) {
            TaskRun taskRun = makeTaskRun(1, task, makeExecuteOption(true, false, 1), i);
            taskRuns.add(taskRun);
            Assert.assertTrue(scheduler.addPendingTaskRun(taskRun));
        }

        Set<TaskRun> runningTaskRuns = Sets.newHashSet(taskRuns.subList(0, 1));
        scheduler.scheduledPendingTaskRun(taskRun -> {
            Assert.assertTrue(runningTaskRuns.contains(taskRun));
        });
        // running queue only support one task with same task id
        Assert.assertTrue(scheduler.getRunningTaskCount() == 1);
        Assert.assertTrue(scheduler.getPendingQueueCount() == 9);

        System.out.println(scheduler);
        for (int i = 0; i < 1; i++) {
            Assert.assertTrue(scheduler.getRunnableTaskRun(1).equals(taskRuns.get(i)));
        }
        List<TaskRun> pendingTaskRuns = scheduler.getCopiedPendingTaskRuns();
        for (int i = 1; i < 10; i++) {
            int j = i;
            scheduler.scheduledPendingTaskRun(taskRun -> {
                Assert.assertTrue(taskRun.equals(taskRuns.get(j)));
                Assert.assertTrue(taskRun.equals(pendingTaskRuns.get(j - 1)));
            });
        }
    }

    @Test
    public void testScheduledToString() {
        String str = null;
        {
            Task task = new Task("test");
            task.setDefinition("select 1");
            List<TaskRun> taskRuns = Lists.newArrayList();
            TaskRunScheduler scheduler = new TaskRunScheduler();
            for (int i = 0; i < 10; i++) {
                TaskRun taskRun = makeTaskRun(i, task, makeExecuteOption(true, false, 1), i);
                taskRuns.add(taskRun);
                scheduler.addPendingTaskRun(taskRun);
            }
            Set<TaskRun> runningTaskRuns = Sets.newHashSet(taskRuns.subList(0, Config.task_runs_concurrency));
            scheduler.scheduledPendingTaskRun(taskRun -> {
                Assert.assertTrue(runningTaskRuns.contains(taskRun));
            });
            str = scheduler.toString();
        }

        // test json result
        {
            TaskRunScheduler scheduler = GsonUtils.GSON.fromJson(str, TaskRunScheduler.class);
            Assert.assertTrue(scheduler.getRunningTaskCount() == 4);
            Assert.assertTrue(scheduler.getPendingQueueCount() == 6);

            Set<Long> expPendingTaskIds = ImmutableSet.of(4L, 5L, 6L, 7L, 8L, 9L);
            for (TaskRun taskRun : scheduler.getCopiedPendingTaskRuns()) {
                Assert.assertTrue(taskRun != null);
                Assert.assertTrue(expPendingTaskIds.contains(taskRun.getTaskId()));
            }

            Set<Long> expRunningTaskIds = ImmutableSet.of(0L, 1L, 2L, 3L);
            for (TaskRun taskRun : scheduler.getCopiedRunningTaskRuns()) {
                Assert.assertTrue(taskRun != null);
                Assert.assertTrue(expRunningTaskIds.contains(taskRun.getTaskId()));
            }
        }
    }

    @Test
    public void testTaskSchedulerWithDifferentTaskIds() {
        TaskManager tm = new TaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        for (int i = 0; i < N; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(i, task, makeExecuteOption(true, false));
            taskRun.setProcessor(new MockTaskRunProcessor());
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        long runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();
        Assert.assertEquals(N, pendingTaskRunsCount + runningTaskRunsCount);
    }

    @Test
    public void testTaskSchedulerWithSameTaskIdsAndMergeable() {
        TaskManager tm = new TaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        for (int i = 0; i < N; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(1, task, makeExecuteOption(true, false));
            taskRun.setProcessor(new MockTaskRunProcessor());
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        long runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();
        Assert.assertEquals(1, pendingTaskRunsCount + runningTaskRunsCount);
    }

    @Test
    public void testTaskSchedulerWithSameTaskIdsAndNoMergeable() {
        TaskManager tm = new TaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        for (int i = 0; i < N; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(1, task, makeExecuteOption(false, false));
            taskRun.setProcessor(new MockTaskRunProcessor());
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        long runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();
        System.out.println(taskRunScheduler);
        Assert.assertEquals(N, pendingTaskRunsCount + runningTaskRunsCount);
    }
}