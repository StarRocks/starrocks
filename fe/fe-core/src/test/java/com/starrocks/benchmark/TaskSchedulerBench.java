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

package com.starrocks.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.starrocks.common.Config;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

@Ignore
public class TaskSchedulerBench extends MVTestBase {

    // private static final int TASK_NUM = Config.task_runs_queue_length;
    private static final int TASK_NUM = 10;

    @Rule
    public TestRule benchRun = new BenchmarkRule();

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        Config.task_runs_concurrency = TASK_NUM;
        LOG.info("prepared {} tasks", TASK_NUM);
    }

    @Before
    public void before() {
        starRocksAssert.getCtx().getSessionVariable().setEnableQueryDump(false);
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync) {
        ExecuteOption executeOption = new ExecuteOption(isMergeRedundant);
        executeOption.setSync(isSync);
        return executeOption;
    }

    private TaskRun makeTaskRun(long taskId, Task task, ExecuteOption executeOption) {
        TaskRun taskRun = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(executeOption)
                .build();
        taskRun.setTaskId(taskId);
        return taskRun;
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 1)
    public void testTaskSchedulerWithDifferentTaskIds() {
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        for (int i = 0; i < TASK_NUM; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(i, task, makeExecuteOption(true, false));
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        long runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();

        while (pendingTaskRunsCount != 0 || runningTaskRunsCount != 0) {
            pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
            runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();
        }
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 1)
    public void testTaskSchedulerWithSameTaskIdsAndMergeable() {
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        for (int i = 0; i < TASK_NUM; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(1, task, makeExecuteOption(true, false));
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        long runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();

        while (pendingTaskRunsCount != 0 || runningTaskRunsCount != 0) {
            pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
            runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();
        }
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 1)
    public void testTaskSchedulerWithSameTaskIdsAndNoMergeable() {
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        for (int i = 0; i < TASK_NUM; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(1, task, makeExecuteOption(false, false));
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        long runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();

        while (pendingTaskRunsCount != 0 || runningTaskRunsCount != 0) {
            pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
            runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();
        }
    }
}
