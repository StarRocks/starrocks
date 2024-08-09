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

import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class TaskRunExecutorTest {
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

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority) {
        ExecuteOption executeOption = new ExecuteOption(isMergeRedundant);
        executeOption.setSync(isSync);
        executeOption.setPriority(priority);
        return executeOption;
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

    private void mockExecuteTaskRun(TaskRun taskRun, List<Integer> failedAttempts) {
        new MockUp<TaskRun>() {
            /**
             * @see TaskRun#executeTaskRun() throws Exception
             */
            @Mock
            public boolean executeTaskRun() throws Exception {
                if (failedAttempts.contains(taskRun.getTaskRunAttempt())) {
                    return false;
                }
                return true;
            }
        };
    }

    @Test
    public void testTaskRunExecutorWithoutRetryAttempt() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;
        TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, 0), 1);
        taskRun.setTaskRetryAttempts(3);
        String queryId = UUIDUtil.genUUID().toString();
        long created = System.currentTimeMillis();
        taskRun.initStatus(queryId, created);
        TaskRunScheduler scheduler = new TaskRunScheduler();
        scheduler.addPendingTaskRun(taskRun);
        mockExecuteTaskRun(taskRun, List.of(0));
        scheduler.scheduledPendingTaskRun(t -> {
            Assert.assertEquals(taskRun, t);
            Assert.assertEquals(3, t.getTaskRetryAttempts());
            // 1th attemp run failed.
            try {
                boolean isSuccess = t.executeTaskRun();
                Assert.assertFalse(isSuccess);
                Assert.assertEquals(0, t.getTaskRunAttempt());
            } catch (Exception e) {
                Assert.fail();
            }
        });
    }

    @Test
    public void testTaskRunExecutorWithRetryAttempt() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;
        TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, 0), 1);
        taskRun.setTaskRetryAttempts(3);
        String queryId = UUIDUtil.genUUID().toString();
        long created = System.currentTimeMillis();
        taskRun.initStatus(queryId, created);

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        TaskRunScheduler scheduler = taskManager.getTaskRunScheduler();
        scheduler.addPendingTaskRun(taskRun);
        mockExecuteTaskRun(taskRun, List.of(0, 1));

        TaskRunManager taskRunManager = taskManager.getTaskRunManager();
        taskRunManager.scheduledPendingTaskRun();
        taskRun.getFuture().join();
        TaskRunStatus status = taskRun.getStatus();
        System.out.println(status);
        Assert.assertEquals(Constants.TaskRunState.SUCCESS, status.getState());
        Assert.assertEquals(0, status.getErrorCode());
        Assert.assertEquals(null, status.getErrorMessage());
        // 0, 1th attempt run failed.
        Assert.assertEquals(2, status.getTaskRunAttempt());
    }

    @Test
    public void testTaskRunExecutorWithAllRetryAttemptsFailed() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;
        TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, 0), 1);
        taskRun.setTaskRetryAttempts(3);
        String queryId = UUIDUtil.genUUID().toString();
        long created = System.currentTimeMillis();
        taskRun.initStatus(queryId, created);

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        TaskRunScheduler scheduler = taskManager.getTaskRunScheduler();
        scheduler.addPendingTaskRun(taskRun);
        mockExecuteTaskRun(taskRun, List.of(0, 1, 2));

        TaskRunManager taskRunManager = taskManager.getTaskRunManager();
        taskRunManager.scheduledPendingTaskRun();
        taskRun.getFuture().join();
        TaskRunStatus status = taskRun.getStatus();
        System.out.println(status);
        Assert.assertEquals(Constants.TaskRunState.FAILED, status.getState());
        Assert.assertEquals(-1, status.getErrorCode());
        Assert.assertEquals(null, status.getErrorMessage());
        // 0, 1, 2th attempt run all failed.
        Assert.assertEquals(2, status.getTaskRunAttempt());
    }
}