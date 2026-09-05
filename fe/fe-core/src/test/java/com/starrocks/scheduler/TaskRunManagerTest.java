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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class TaskRunManagerTest {

    private static final int N = 100;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        globalStateMgr.getWarehouseMgr().addWarehouse(new DefaultWarehouse(1, "w1"));
        globalStateMgr.getWarehouseMgr().addWarehouse(new DefaultWarehouse(2, "w2"));
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority) {
        return makeExecuteOption(isMergeRedundant, isSync, priority, Maps.newHashMap());
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority,
                                                   Map<String, String> properties) {
        ExecuteOption executeOption = new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(), isMergeRedundant,
                properties);
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
    public void testKillTaskRun() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        List<TaskRun> taskRuns = Lists.newArrayList();
        long taskId = 100;

        TaskRunScheduler scheduler = new TaskRunScheduler();
        TaskRunManager taskRunManager = new TaskRunManager(scheduler);

        boolean[] forces = {false, true};
        for (boolean force : forces) {
            for (int i = 0; i < N; i++) {
                TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, 1));
                taskRuns.add(taskRun);
                scheduler.addPendingTaskRun(taskRun);
            }

            scheduler.scheduledPendingTaskRun(taskRun -> Assertions.assertTrue(taskRun.getTaskId() == taskId));

            Assertions.assertTrue(scheduler.getRunningTaskRun(taskId) != null);
            Assertions.assertTrue(scheduler.getRunnableTaskRun(taskId) != null);
            Assertions.assertTrue(scheduler.getPendingTaskRunsByTaskId(taskId).size() == N - 1);

            // no matter whether force is true or not, we always clear running and pending task run
            taskRunManager.killTaskRun(taskId, force);

            Assertions.assertTrue(CollectionUtils.isEmpty(scheduler.getPendingTaskRunsByTaskId(taskId)));
            if (force) {
                Assertions.assertTrue(scheduler.getRunningTaskRun(taskId) == null);
            } else {
                Assertions.assertTrue(scheduler.getRunningTaskRun(taskId) != null);
                scheduler.removeRunningTask(taskId);
            }
        }
    }

    private Map<String, String> makeTaskRunProperties(String partitionStart,
                                                      String partitionEnd,
                                                      boolean isForce) {
        Map<String, String> result = Maps.newHashMap();
        result.put(TaskRun.PARTITION_START, partitionStart);
        result.put(TaskRun.PARTITION_END, partitionEnd);
        result.put(TaskRun.FORCE, String.valueOf(isForce));
        return result;
    }

    private Map<String, String> makeMVTaskRunProperties(String partitionStart,
                                                        String partitionEnd,
                                                        boolean isForce) {
        Map<String, String> result = Maps.newHashMap();
        result.put(TaskRun.PARTITION_START, partitionStart);
        result.put(TaskRun.PARTITION_END, partitionEnd);
        result.put(TaskRun.MV_ID, "1");
        result.put(TaskRun.FORCE, String.valueOf(isForce));
        return result;
    }

    @Test
    public void testExecutionOption() {
        {
            ExecuteOption option1 = makeExecuteOption(true, false, 1);
            ExecuteOption option2 = makeExecuteOption(true, false, 10);
            Assertions.assertTrue(option1.isMergeableWith(option2));
        }
        {
            ExecuteOption option1 = makeExecuteOption(true, false, 1);
            ExecuteOption option2 = makeExecuteOption(false, false, 10);
            Assertions.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assertions.assertTrue(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeTaskRunProperties("2023-01-01", "2023-01-02", true);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assertions.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assertions.assertTrue(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", true);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assertions.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            prop1.put("a", "a");
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop11 = option1.getTaskRunComparableProperties();
            Assertions.assertTrue(prop11.size() == 4);

            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            prop2.put("a", "b");
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assertions.assertTrue(option1.isMergeableWith(option2));
        }
    }

    @Test
    public void testReplayCreateTaskRunWithExpiredFinishState() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setState(Constants.TaskRunState.SUCCESS);
        status.setExpireTime(System.currentTimeMillis() - 1000); // Expired

        // Should return early without processing
        taskManager.replayCreateTaskRun(status);

        // Verify no task runs were added
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());
    }

    @Test
    public void testReplayCreateTaskRunPendingState() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000); // Not expired

        taskManager.replayCreateTaskRun(status);

        // Verify task run was added to pending queue
        Assertions.assertEquals(1, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());
    }

    @Test
    public void testReplayCreateTaskRunPendingStateWithNullTask() {
        TaskManager taskManager = new TaskManager();
        // Don't create the task

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("non_existent_task");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);

        taskManager.replayCreateTaskRun(status);

        // Should not add anything since task doesn't exist
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
    }

    @Test
    public void testReplayCreateTaskRunRunningState() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.RUNNING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);

        taskManager.replayCreateTaskRun(status);

        // Should be marked as FAILED and added to history
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        // Verify it was added to history with FAILED state
        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.FAILED, history.get(0).getState());
    }

    @Test
    public void testReplayCreateTaskRunFailedState() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.FAILED);
        status.setExpireTime(System.currentTimeMillis() + 3600000);

        taskManager.replayCreateTaskRun(status);

        // Should be added to history
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.FAILED, history.get(0).getState());
    }

    @Test
    public void testReplayCreateTaskRunMergedState() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.MERGED);
        status.setExpireTime(System.currentTimeMillis() + 3600000);

        taskManager.replayCreateTaskRun(status);

        // Should be added to history with progress 100
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.MERGED, history.get(0).getState());
        Assertions.assertEquals(100, history.get(0).getProgress());
    }

    @Test
    public void testReplayCreateTaskRunSuccessState() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.SUCCESS);
        status.setExpireTime(System.currentTimeMillis() + 3600000);

        taskManager.replayCreateTaskRun(status);

        // Should be added to history with progress 100
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, history.get(0).getState());
        Assertions.assertEquals(100, history.get(0).getProgress());
    }

    @Test
    public void testReplayCreateTaskRunSkippedState() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.SKIPPED);
        status.setExpireTime(System.currentTimeMillis() + 3600000);

        taskManager.replayCreateTaskRun(status);

        // Should be added to history with progress 0
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, history.get(0).getState());
        Assertions.assertEquals(0, history.get(0).getProgress());
    }

    @Test
    public void testReplayUpdateTaskRunPendingToRunning() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // First create a pending task run
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);
        taskManager.replayCreateTaskRun(status);

        // Now update it from PENDING to RUNNING
        TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
        taskManager.replayUpdateTaskRun(change);

        // Should be moved from pending to running
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(1, taskManager.getTaskRunScheduler().getRunningTaskCount());
    }

    @Test
    public void testReplayUpdateTaskRunPendingToMerged() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // First create a pending task run
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);
        taskManager.replayCreateTaskRun(status);

        // Now update it from PENDING to MERGED
        TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.MERGED);
        change.setErrorMessage("Merged by other task");
        change.setErrorCode(0);
        change.setFinishTime(System.currentTimeMillis());
        taskManager.replayUpdateTaskRun(change);

        // Should be removed from pending and added to history
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.MERGED, history.get(0).getState());
        Assertions.assertEquals(100, history.get(0).getProgress());
        Assertions.assertEquals("Merged by other task", history.get(0).getErrorMessage());
    }

    @Test
    public void testReplayUpdateTaskRunPendingToFailed() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // First create a pending task run
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);
        taskManager.replayCreateTaskRun(status);

        // Now update it from PENDING to FAILED
        TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.FAILED);
        change.setErrorMessage("Task failed");
        change.setErrorCode(-1);
        taskManager.replayUpdateTaskRun(change);

        // Should be removed from pending and added to history
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.FAILED, history.get(0).getState());
        Assertions.assertEquals("Task failed", history.get(0).getErrorMessage());
        Assertions.assertEquals(-1, history.get(0).getErrorCode());
    }

    @Test
    public void testReplayUpdateTaskRunPendingToSuccess() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // First create a pending task run
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);
        taskManager.replayCreateTaskRun(status);

        // Now update it from PENDING to SUCCESS
        TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.SUCCESS);
        taskManager.replayUpdateTaskRun(change);

        // Should be removed from pending and added to history
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, history.get(0).getState());
    }

    @Test
    public void testReplayUpdateTaskRunRunningToFailed() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setSource(Constants.TaskSource.MV);

        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // First create a pending task run and move it to running
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setSource(Constants.TaskSource.MV);
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);
        taskManager.replayCreateTaskRun(status);

        TaskRunStatusChange changeToRunning = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
        taskManager.replayUpdateTaskRun(changeToRunning);

        // Now update it from RUNNING to FAILED
        TaskRunStatusChange changeToFailed = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.RUNNING, Constants.TaskRunState.FAILED);
        changeToFailed.setErrorMessage("Task failed during execution");
        changeToFailed.setErrorCode(-2);
        changeToFailed.setFinishTime(System.currentTimeMillis());
        MVTaskRunExtraMessage mvTaskRunExtraMessage = new MVTaskRunExtraMessage();
        String extraMessage = mvTaskRunExtraMessage.toString();
        changeToFailed.setExtraMessage(extraMessage);
        taskManager.replayUpdateTaskRun(changeToFailed);

        // Should be removed from running and added to history
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());

        TaskRunStatus taskRunStatus = history.get(0);
        Assertions.assertEquals(Constants.TaskSource.MV, taskRunStatus.getSource());
        Assertions.assertEquals(Constants.TaskRunState.FAILED, taskRunStatus.getState());
        Assertions.assertEquals("Task failed during execution", taskRunStatus.getErrorMessage());
        Assertions.assertEquals(-2, taskRunStatus.getErrorCode());
        Assertions.assertEquals(extraMessage, taskRunStatus.getExtraMessage());
        Assertions.assertEquals(100, taskRunStatus.getProgress());
    }

    @Test
    public void testReplayUpdateTaskRunRunningToSuccess() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // First create a pending task run and move it to running
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);
        taskManager.replayCreateTaskRun(status);

        TaskRunStatusChange changeToRunning = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
        taskManager.replayUpdateTaskRun(changeToRunning);

        // Now update it from RUNNING to SUCCESS
        TaskRunStatusChange changeToSuccess = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.RUNNING, Constants.TaskRunState.SUCCESS);
        changeToSuccess.setFinishTime(System.currentTimeMillis());
        changeToSuccess.setExtraMessage("Task completed successfully");
        taskManager.replayUpdateTaskRun(changeToSuccess);

        // Should be removed from running and added to history
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, history.get(0).getState());
        Assertions.assertEquals("", history.get(0).getExtraMessage());
        Assertions.assertEquals(100, history.get(0).getProgress());
    }

    @Test
    public void testReplayUpdateTaskRunWithNonMatchingQueryId() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // First create a pending task run
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);
        taskManager.replayCreateTaskRun(status);

        // Create a different status with different query ID
        TaskRunStatus differentStatus = new TaskRunStatus();
        differentStatus.setTaskName("test");
        differentStatus.setQueryId("different_query_id");
        differentStatus.setCreateTime(System.currentTimeMillis());
        differentStatus.setState(Constants.TaskRunState.PENDING);

        // Try to update with non-matching query ID
        TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), differentStatus,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
        taskManager.replayUpdateTaskRun(change);

        // Should not be moved to running due to query ID mismatch
        Assertions.assertEquals(1, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());
    }

    @Test
    public void testReplayUpdateTaskRunWithNonExistentPendingTask() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // Don't create any pending task run

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);

        // Try to update non-existent pending task
        TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
        taskManager.replayUpdateTaskRun(change);

        // Should not affect anything
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());
    }

    @Test
    public void testReplayUpdateTaskRunRunningToFinishedWithHistoryLookup() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // Create a task run status and add it to history first
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.RUNNING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);
        taskManager.getTaskRunHistory().addHistory(status);

        // Now update it from RUNNING to SUCCESS (simulating a task that was running before restart)
        TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.RUNNING, Constants.TaskRunState.SUCCESS);
        change.setFinishTime(System.currentTimeMillis());
        change.setExtraMessage("Task completed after restart");
        taskManager.replayUpdateTaskRun(change);

        // Should update the existing history entry
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(1, history.size());
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, history.get(0).getState());
        Assertions.assertEquals("", history.get(0).getExtraMessage());
    }

    @Test
    public void testReplayUpdateTaskRunIllegalStateTransition() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);

        // Try illegal transition: PENDING to PENDING
        TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.PENDING);
        taskManager.replayUpdateTaskRun(change);

        // Should not affect anything due to illegal transition
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());
    }

    @Test
    public void testReplayUpdateTaskRunFromRunningToNonFinishState() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // First create a pending task run and move it to running
        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("query_123");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.PENDING);
        status.setExpireTime(System.currentTimeMillis() + 3600000);
        taskManager.replayCreateTaskRun(status);

        TaskRunStatusChange changeToRunning = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
        taskManager.replayUpdateTaskRun(changeToRunning);

        // Try illegal transition: RUNNING to PENDING
        TaskRunStatusChange illegalChange = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.RUNNING, Constants.TaskRunState.PENDING);
        taskManager.replayUpdateTaskRun(illegalChange);

        // Should not affect anything due to illegal transition
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(1, taskManager.getTaskRunScheduler().getRunningTaskCount());
    }

    @Test
    public void testReplayUpdateTaskRunWithNullHistoryEntry() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        TaskRunStatus status = new TaskRunStatus();
        status.setTaskName("test");
        status.setQueryId("non_existent_query");
        status.setCreateTime(System.currentTimeMillis());
        status.setState(Constants.TaskRunState.RUNNING);

        // Try to update a running task that doesn't exist in history
        TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), status,
                Constants.TaskRunState.RUNNING, Constants.TaskRunState.SUCCESS);
        change.setFinishTime(System.currentTimeMillis());
        change.setExtraMessage("Should not be updated");
        taskManager.replayUpdateTaskRun(change);

        // Should not affect anything since the task doesn't exist in history
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());
        Assertions.assertEquals(0, taskManager.getTaskRunHistory().getInMemoryHistory().size());
    }

    @Test
    public void testReplayCreateTaskRunWithMultipleStates() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // Test multiple task runs with different states
        Constants.TaskRunState[] states = {
                Constants.TaskRunState.PENDING,
                Constants.TaskRunState.RUNNING,
                Constants.TaskRunState.FAILED,
                Constants.TaskRunState.SUCCESS,
                Constants.TaskRunState.MERGED,
                Constants.TaskRunState.SKIPPED
        };

        for (int i = 0; i < states.length; i++) {
            TaskRunStatus status = new TaskRunStatus();
            status.setTaskName("test");
            status.setQueryId("query_" + i);
            status.setCreateTime(System.currentTimeMillis());
            status.setState(states[i]);
            status.setExpireTime(System.currentTimeMillis() + 3600000);

            taskManager.replayCreateTaskRun(status);
        }

        // Only PENDING state should be in pending queue
        Assertions.assertEquals(1, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        // All finish states should be in history
        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(5, history.size()); // RUNNING, FAILED, SUCCESS, MERGED, SKIPPED
    }

    @Test
    public void testReplayUpdateTaskRunComplexScenario() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);

        // Create multiple pending task runs
        for (int i = 0; i < 3; i++) {
            TaskRunStatus status = new TaskRunStatus();
            status.setTaskName("test");
            status.setQueryId("query_" + i);
            status.setCreateTime(System.currentTimeMillis());
            status.setState(Constants.TaskRunState.PENDING);
            status.setExpireTime(System.currentTimeMillis() + 3600000);
            taskManager.replayCreateTaskRun(status);
        }

        Assertions.assertEquals(3, taskManager.getTaskRunScheduler().getPendingQueueCount());

        // Move first one to running
        TaskRunStatus firstStatus = new TaskRunStatus();
        firstStatus.setTaskName("test");
        firstStatus.setQueryId("query_0");
        TaskRunStatusChange changeToRunning = new TaskRunStatusChange(task.getId(), firstStatus,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
        taskManager.replayUpdateTaskRun(changeToRunning);

        Assertions.assertEquals(2, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(1, taskManager.getTaskRunScheduler().getRunningTaskCount());

        // Move first one to success
        TaskRunStatusChange changeToSuccess = new TaskRunStatusChange(task.getId(), firstStatus,
                Constants.TaskRunState.RUNNING, Constants.TaskRunState.SUCCESS);
        changeToSuccess.setFinishTime(System.currentTimeMillis());
        taskManager.replayUpdateTaskRun(changeToSuccess);

        Assertions.assertEquals(2, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        // Move second one to failed
        TaskRunStatus secondStatus = new TaskRunStatus();
        secondStatus.setTaskName("test");
        secondStatus.setQueryId("query_1");
        TaskRunStatusChange changeToFailed = new TaskRunStatusChange(task.getId(), secondStatus,
                Constants.TaskRunState.PENDING, Constants.TaskRunState.FAILED);
        changeToFailed.setErrorMessage("Task failed");
        changeToFailed.setErrorCode(-1);
        taskManager.replayUpdateTaskRun(changeToFailed);

        Assertions.assertEquals(1, taskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, taskManager.getTaskRunScheduler().getRunningTaskCount());

        // Verify history
        List<TaskRunStatus> history = taskManager.getTaskRunHistory().getInMemoryHistory();
        Assertions.assertEquals(2, history.size()); // SUCCESS and FAILED
    }
}
