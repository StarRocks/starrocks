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

import com.starrocks.persist.AlterTaskInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.scheduler.persist.DropTasksLog;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TaskManagerEditLogTest {
    private TaskManager masterTaskManager;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create TaskManager instance
        masterTaskManager = new TaskManager();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testCreateTaskNormalCase() throws Exception {
        // 1. Prepare test data
        String taskName = "test_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");

        // 2. Verify initial state
        Assertions.assertNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(0, masterTaskManager.filterTasks(null).size());

        // 3. Execute createTask operation (master side)
        masterTaskManager.createTask(task);

        // 4. Verify master state
        Assertions.assertNotNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(1, masterTaskManager.filterTasks(null).size());
        
        // Verify task details
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(taskName, createdTask.getName());
        Assertions.assertEquals(Constants.TaskType.MANUAL, createdTask.getType());
        Assertions.assertEquals("SELECT 1", createdTask.getDefinition());
        Assertions.assertEquals("test_db", createdTask.getDbName());
        Assertions.assertEquals("test_catalog", createdTask.getCatalogName());
        Assertions.assertTrue(createdTask.getId() > 0); // ID should be assigned

        // 5. Test follower replay functionality
        TaskManager followerTaskManager = new TaskManager();
        
        // Verify follower initial state
        Assertions.assertNull(followerTaskManager.getTask(taskName));
        Assertions.assertEquals(0, followerTaskManager.filterTasks(null).size());

        // Create a copy of the task for replay testing
        Task replayTask = (Task) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_TASK);
        
        // Execute follower replay
        followerTaskManager.replayCreateTask(replayTask);

        // 6. Verify follower state is consistent with master
        Assertions.assertNotNull(followerTaskManager.getTask(taskName));
        Assertions.assertEquals(1, followerTaskManager.filterTasks(null).size());
        
        Task followerTask = followerTaskManager.getTask(taskName);
        Assertions.assertNotNull(followerTask);
        Assertions.assertEquals(taskName, followerTask.getName());
        Assertions.assertEquals(Constants.TaskType.MANUAL, followerTask.getType());
        Assertions.assertEquals("SELECT 1", followerTask.getDefinition());
        Assertions.assertEquals("test_db", followerTask.getDbName());
        Assertions.assertEquals("test_catalog", followerTask.getCatalogName());
        Assertions.assertEquals(createdTask.getId(), followerTask.getId());
    }

    @Test
    public void testCreateTaskEditLogException() throws Exception {
        // 1. Prepare test data
        String taskName = "exception_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");

        // 2. Create a separate TaskManager for exception testing
        TaskManager exceptionTaskManager = new TaskManager();
        EditLog spyEditLog = spy(new EditLog(null));
        
        // 3. Mock EditLog.logCreateTask to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateTask(any(Task.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertNull(exceptionTaskManager.getTask(taskName));
        Assertions.assertEquals(0, exceptionTaskManager.filterTasks(null).size());
        
        // Save initial state snapshot
        int initialTaskCount = exceptionTaskManager.filterTasks(null).size();

        // 4. Execute createTask operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionTaskManager.createTask(task);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertNull(exceptionTaskManager.getTask(taskName));
        Assertions.assertEquals(initialTaskCount, exceptionTaskManager.filterTasks(null).size());
        
        // Verify no task was accidentally added
        Assertions.assertNull(exceptionTaskManager.getTask(taskName));
    }

    @Test
    public void testCreateTaskDuplicateName() throws Exception {
        // 1. First create a task
        String taskName = "duplicate_test_task";
        Task task1 = new Task(taskName);
        task1.setType(Constants.TaskType.MANUAL);
        task1.setDefinition("SELECT 1");
        task1.setDbName("test_db");
        task1.setCatalogName("test_catalog");
        
        masterTaskManager.createTask(task1);
        Assertions.assertNotNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(1, masterTaskManager.filterTasks(null).size());

        // 2. Try to create another task with the same name
        Task task2 = new Task(taskName);
        task2.setType(Constants.TaskType.MANUAL);
        task2.setDefinition("SELECT 2");
        task2.setDbName("test_db2");
        task2.setCatalogName("test_catalog2");

        // 3. Expect DdlException to be thrown
        try {
            masterTaskManager.createTask(task2);
            Assertions.fail("Expected DdlException to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e.toString().contains("already exists"));
        }

        // 4. Verify state remains unchanged (only the original task)
        Assertions.assertEquals(1, masterTaskManager.filterTasks(null).size());
        Task existingTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(existingTask);
        Assertions.assertEquals("SELECT 1", existingTask.getDefinition()); // Original definition preserved
        Assertions.assertEquals("test_db", existingTask.getDbName()); // Original db name preserved
    }

    @Test
    public void testCreateTaskPeriodicalWithoutSchedule() throws Exception {
        // 1. Prepare test data for periodical task without schedule
        String taskName = "periodical_task_no_schedule";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        // Note: No schedule is set

        // 2. Verify initial state
        Assertions.assertNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(0, masterTaskManager.filterTasks(null).size());

        // 3. Execute createTask operation and expect DdlException
        try {
            masterTaskManager.createTask(task);
            Assertions.fail("Expected DdlException to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e.toString().contains("has no scheduling information"));
        }

        // 4. Verify state remains unchanged
        Assertions.assertNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(0, masterTaskManager.filterTasks(null).size());
    }

    @Test
    public void testCreateTaskPeriodicalWithSchedule() throws Exception {
        // 1. Prepare test data for periodical task with schedule
        String taskName = "periodical_task_with_schedule";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        
        // Set up schedule
        TaskSchedule schedule = new TaskSchedule();
        schedule.setStartTime(System.currentTimeMillis() / 1000);
        schedule.setPeriod(3600); // 1 hour
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);

        // 2. Verify initial state
        Assertions.assertNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(0, masterTaskManager.filterTasks(null).size());

        // 3. Execute createTask operation (master side)
        masterTaskManager.createTask(task);

        // 4. Verify master state
        Assertions.assertNotNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(1, masterTaskManager.filterTasks(null).size());
        Assertions.assertNotNull(masterTaskManager.getPeriodFutureMap().get(task.getId()));
        
        // Verify task details
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(taskName, createdTask.getName());
        Assertions.assertEquals(Constants.TaskType.PERIODICAL, createdTask.getType());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, createdTask.getState()); // Should be set to ACTIVE
        Assertions.assertNotNull(createdTask.getSchedule());
        Assertions.assertEquals(3600, createdTask.getSchedule().getPeriod());
        Assertions.assertEquals(TimeUnit.SECONDS, createdTask.getSchedule().getTimeUnit());
        Assertions.assertTrue(createdTask.getId() > 0); // ID should be assigned

        // 5. Test follower replay functionality
        TaskManager followerTaskManager = new TaskManager();
        
        // Verify follower initial state
        Assertions.assertNull(followerTaskManager.getTask(taskName));
        Assertions.assertEquals(0, followerTaskManager.filterTasks(null).size());

        // Create a copy of the task for replay testing
        Task replayTask = (Task) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_TASK);
        
        // Execute follower replay
        followerTaskManager.replayCreateTask(replayTask);

        // 6. Verify follower state is consistent with master
        Assertions.assertNotNull(followerTaskManager.getTask(taskName));
        Assertions.assertEquals(1, followerTaskManager.filterTasks(null).size());
        
        Task followerTask = followerTaskManager.getTask(taskName);
        Assertions.assertNotNull(followerTask);
        Assertions.assertEquals(taskName, followerTask.getName());
        Assertions.assertEquals(Constants.TaskType.PERIODICAL, followerTask.getType());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, followerTask.getState());
        Assertions.assertNotNull(followerTask.getSchedule());
        Assertions.assertEquals(3600, followerTask.getSchedule().getPeriod());
        Assertions.assertEquals(TimeUnit.SECONDS, followerTask.getSchedule().getTimeUnit());
        Assertions.assertEquals(createdTask.getId(), followerTask.getId());
    }

    @Test
    public void testCreateTaskWithNonZeroId() throws Exception {
        // 1. Prepare test data with non-zero ID (should be rejected)
        String taskName = "task_with_id";
        Task task = new Task(taskName);
        task.setId(123L); // Set non-zero ID
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");

        // 2. Verify initial state
        Assertions.assertNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(0, masterTaskManager.filterTasks(null).size());

        // 3. Execute createTask operation and expect IllegalArgumentException
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            masterTaskManager.createTask(task);
        });
        Assertions.assertTrue(exception.getMessage().contains("TaskId should be assigned by the framework"));

        // 4. Verify state remains unchanged
        Assertions.assertNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(0, masterTaskManager.filterTasks(null).size());
    }

    @Test
    public void testDropTasksNormalCase() throws Exception {
        // 1. Prepare test data - create multiple tasks first
        String taskName1 = "test_task_1";
        String taskName2 = "test_task_2";
        String taskName3 = "test_task_3";
        
        Task task1 = new Task(taskName1);
        task1.setType(Constants.TaskType.MANUAL);
        task1.setDefinition("SELECT 1");
        task1.setDbName("test_db");
        task1.setCatalogName("test_catalog");
        
        Task task2 = new Task(taskName2);
        task2.setType(Constants.TaskType.MANUAL);
        task2.setDefinition("SELECT 2");
        task2.setDbName("test_db");
        task2.setCatalogName("test_catalog");
        
        Task task3 = new Task(taskName3);
        task3.setType(Constants.TaskType.MANUAL);
        task3.setDefinition("SELECT 3");
        task3.setDbName("test_db");
        task3.setCatalogName("test_catalog");

        // 2. Create tasks
        masterTaskManager.createTask(task1);
        masterTaskManager.createTask(task2);
        masterTaskManager.createTask(task3);

        // 3. Verify initial state
        Assertions.assertNotNull(masterTaskManager.getTask(taskName1));
        Assertions.assertNotNull(masterTaskManager.getTask(taskName2));
        Assertions.assertNotNull(masterTaskManager.getTask(taskName3));
        Assertions.assertEquals(3, masterTaskManager.filterTasks(null).size());

        // 4. Prepare task IDs to drop (drop task1 and task3, keep task2)
        List<Long> taskIdsToDrop = new ArrayList<>();
        taskIdsToDrop.add(task1.getId());
        taskIdsToDrop.add(task3.getId());

        // 5. Execute dropTasks operation (master side)
        masterTaskManager.dropTasks(taskIdsToDrop);

        // 6. Verify master state after dropping
        Assertions.assertNull(masterTaskManager.getTask(taskName1));
        Assertions.assertNotNull(masterTaskManager.getTask(taskName2)); // Should still exist
        Assertions.assertNull(masterTaskManager.getTask(taskName3));
        Assertions.assertEquals(1, masterTaskManager.filterTasks(null).size());

        // 7. Test follower replay functionality
        TaskManager followerTaskManager = new TaskManager();
        
        // First create the same tasks in follower
        Task followerTask1 = new Task(taskName1);
        followerTask1.setId(task1.getId());
        followerTask1.setType(Constants.TaskType.MANUAL);
        followerTask1.setDefinition("SELECT 1");
        followerTask1.setDbName("test_db");
        followerTask1.setCatalogName("test_catalog");
        followerTask1.setCreateTime(task1.getCreateTime());
        
        Task followerTask2 = new Task(taskName2);
        followerTask2.setId(task2.getId());
        followerTask2.setType(Constants.TaskType.MANUAL);
        followerTask2.setDefinition("SELECT 2");
        followerTask2.setDbName("test_db");
        followerTask2.setCatalogName("test_catalog");
        followerTask2.setCreateTime(task2.getCreateTime());
        
        Task followerTask3 = new Task(taskName3);
        followerTask3.setId(task3.getId());
        followerTask3.setType(Constants.TaskType.MANUAL);
        followerTask3.setDefinition("SELECT 3");
        followerTask3.setDbName("test_db");
        followerTask3.setCatalogName("test_catalog");
        followerTask3.setCreateTime(task3.getCreateTime());
        
        followerTaskManager.replayCreateTask(followerTask1);
        followerTaskManager.replayCreateTask(followerTask2);
        followerTaskManager.replayCreateTask(followerTask3);
        
        Assertions.assertEquals(3, followerTaskManager.filterTasks(null).size());

        // Replay the drop operation
        DropTasksLog dropTasksLog = (DropTasksLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_TASKS);
        followerTaskManager.replayDropTasks(dropTasksLog.getTaskIdList());

        // 8. Verify follower state is consistent with master
        Assertions.assertNull(followerTaskManager.getTask(taskName1));
        Assertions.assertNotNull(followerTaskManager.getTask(taskName2)); // Should still exist
        Assertions.assertNull(followerTaskManager.getTask(taskName3));
        Assertions.assertEquals(1, followerTaskManager.filterTasks(null).size());
    }

    @Test
    public void testDropTasksEditLogException() throws Exception {
        // 1. Prepare test data - create tasks first
        String taskName1 = "drop_exception_task_1";
        String taskName2 = "drop_exception_task_2";
        
        Task task1 = new Task(taskName1);
        task1.setType(Constants.TaskType.MANUAL);
        task1.setDefinition("SELECT 1");
        task1.setDbName("test_db");
        task1.setCatalogName("test_catalog");
        
        Task task2 = new Task(taskName2);
        task2.setType(Constants.TaskType.MANUAL);
        task2.setDefinition("SELECT 2");
        task2.setDbName("test_db");
        task2.setCatalogName("test_catalog");

        // Create a separate TaskManager for exception testing
        TaskManager exceptionTaskManager = new TaskManager();
        
        // Create tasks first
        exceptionTaskManager.createTask(task1);
        exceptionTaskManager.createTask(task2);
        Assertions.assertEquals(2, exceptionTaskManager.filterTasks(null).size());

        // 2. Prepare task IDs to drop
        List<Long> taskIdsToDrop = new ArrayList<>();
        taskIdsToDrop.add(task1.getId());

        // 3. Mock EditLog.logDropTasks to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropTasks(any(DropTasksLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        int initialTaskCount = exceptionTaskManager.filterTasks(null).size();

        // 4. Execute dropTasks operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionTaskManager.dropTasks(taskIdsToDrop);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertNotNull(exceptionTaskManager.getTask(taskName1));
        Assertions.assertNotNull(exceptionTaskManager.getTask(taskName2));
        Assertions.assertEquals(initialTaskCount, exceptionTaskManager.filterTasks(null).size());
        
        // Verify both tasks still exist
        Task existingTask1 = exceptionTaskManager.getTask(taskName1);
        Task existingTask2 = exceptionTaskManager.getTask(taskName2);
        Assertions.assertNotNull(existingTask1);
        Assertions.assertNotNull(existingTask2);
    }

    @Test
    public void testDropTasksWithPeriodicalTask() throws Exception {
        // 1. Prepare test data - create periodical task
        String taskName = "periodical_drop_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        
        // Set up schedule
        TaskSchedule schedule = new TaskSchedule();
        schedule.setStartTime(System.currentTimeMillis() / 1000);
        schedule.setPeriod(3600); // 1 hour
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);

        // 2. Create periodical task
        masterTaskManager.createTask(task);

        // 3. Verify initial state
        Assertions.assertNotNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(1, masterTaskManager.filterTasks(null).size());
        Assertions.assertNotNull(masterTaskManager.getPeriodFutureMap().get(task.getId()));

        // 4. Prepare task ID to drop
        List<Long> taskIdsToDrop = new ArrayList<>();
        taskIdsToDrop.add(task.getId());

        // 5. Execute dropTasks operation
        masterTaskManager.dropTasks(taskIdsToDrop);

        // 6. Verify task is dropped and scheduler is stopped
        Assertions.assertNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(0, masterTaskManager.filterTasks(null).size());
        Assertions.assertNull(masterTaskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testDropTasksNonExistentTaskId() throws Exception {
        // 1. Prepare test data - create one task
        String taskName = "existing_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        
        masterTaskManager.createTask(task);
        Assertions.assertEquals(1, masterTaskManager.filterTasks(null).size());

        // 2. Prepare task IDs to drop (including non-existent ID)
        List<Long> taskIdsToDrop = new ArrayList<>();
        taskIdsToDrop.add(task.getId());
        taskIdsToDrop.add(99999L); // Non-existent task ID

        // 3. Execute dropTasks operation
        masterTaskManager.dropTasks(taskIdsToDrop);

        // 4. Verify existing task is dropped, non-existent ID is ignored
        Assertions.assertNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(0, masterTaskManager.filterTasks(null).size());
    }

    @Test
    public void testDropTasksEmptyList() throws Exception {
        // 1. Prepare test data - create one task
        String taskName = "existing_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        
        masterTaskManager.createTask(task);
        Assertions.assertEquals(1, masterTaskManager.filterTasks(null).size());

        // 2. Prepare empty task ID list
        List<Long> taskIdsToDrop = new ArrayList<>();

        // 3. Execute dropTasks operation with empty list
        masterTaskManager.dropTasks(taskIdsToDrop);

        // 4. Verify no tasks are dropped
        Assertions.assertNotNull(masterTaskManager.getTask(taskName));
        Assertions.assertEquals(1, masterTaskManager.filterTasks(null).size());
    }

    @Test
    public void testClearUnfinishedTaskRunNormalCase() throws Exception {
        // 1. Prepare test data - create a task first
        String taskName = "clear_unfinished_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        
        masterTaskManager.createTask(task);
        long taskId = task.getId();
        Assertions.assertNotNull(masterTaskManager.getTask(taskName));

        // 2. Create pending TaskRun
        TaskRun pendingTaskRun = TaskRunBuilder.newBuilder(task).build();
        pendingTaskRun.setTaskId(taskId);
        pendingTaskRun.initStatus("pending_query_1", System.currentTimeMillis());
        pendingTaskRun.getStatus().setState(Constants.TaskRunState.PENDING);
        masterTaskManager.getTaskRunScheduler().addPendingTaskRun(pendingTaskRun);
        Assertions.assertEquals(1, masterTaskManager.getTaskRunScheduler().getPendingQueueCount());

        // 3. Create running TaskRun (simulate by adding to running map)
        TaskRun runningTaskRun = TaskRunBuilder.newBuilder(task).build();
        runningTaskRun.setTaskId(taskId);
        runningTaskRun.initStatus("running_query_1", System.currentTimeMillis());
        runningTaskRun.getStatus().setState(Constants.TaskRunState.RUNNING);
        masterTaskManager.getTaskRunScheduler().addRunningTaskRun(runningTaskRun);
        Assertions.assertEquals(1, masterTaskManager.getTaskRunScheduler().getRunningTaskCount());

        // 4. Execute clearUnfinishedTaskRun operation
        masterTaskManager.clearUnfinishedTaskRun();

        // 5. Verify pending and running task runs are cleared
        Assertions.assertEquals(0, masterTaskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, masterTaskManager.getTaskRunScheduler().getRunningTaskCount());

        // 6. Test follower replay functionality
        TaskManager followerTaskManager = new TaskManager();
        
        // First create the task in follower
        Task followerTask = new Task(taskName);
        followerTask.setId(taskId);
        followerTask.setType(Constants.TaskType.MANUAL);
        followerTask.setDefinition("SELECT 1");
        followerTask.setDbName("test_db");
        followerTask.setCatalogName("test_catalog");
        followerTask.setCreateTime(task.getCreateTime());
        followerTaskManager.replayCreateTask(followerTask);

        // Create pending TaskRun in follower
        TaskRun followerPendingTaskRun = TaskRunBuilder.newBuilder(followerTask).build();
        followerPendingTaskRun.setTaskId(taskId);
        followerPendingTaskRun.initStatus("pending_query_1", System.currentTimeMillis());
        followerPendingTaskRun.getStatus().setState(Constants.TaskRunState.PENDING);
        followerTaskManager.getTaskRunScheduler().addPendingTaskRun(followerPendingTaskRun);
        
        // Create running TaskRun in follower
        TaskRun followerRunningTaskRun = TaskRunBuilder.newBuilder(followerTask).build();
        followerRunningTaskRun.setTaskId(taskId);
        followerRunningTaskRun.initStatus("running_query_1", System.currentTimeMillis());
        followerRunningTaskRun.getStatus().setState(Constants.TaskRunState.RUNNING);
        followerTaskManager.getTaskRunScheduler().addRunningTaskRun(followerRunningTaskRun);
        
        Assertions.assertEquals(1, followerTaskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(1, followerTaskManager.getTaskRunScheduler().getRunningTaskCount());

        // Replay the first status change (for pending TaskRun)
        TaskRunStatusChange statusChange1 = (TaskRunStatusChange) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_TASK_RUN);
        followerTaskManager.replayUpdateTaskRun(statusChange1);

        // Replay the second status change (for running TaskRun)
        TaskRunStatusChange statusChange2 = (TaskRunStatusChange) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_TASK_RUN);
        followerTaskManager.replayUpdateTaskRun(statusChange2);

        // 7. Verify follower state is consistent with master
        Assertions.assertEquals(0, followerTaskManager.getTaskRunScheduler().getPendingQueueCount());
        Assertions.assertEquals(0, followerTaskManager.getTaskRunScheduler().getRunningTaskCount());
    }

    @Test
    public void testClearUnfinishedTaskRunEditLogException() throws Exception {
        // 1. Prepare test data - create a task first
        String taskName = "clear_unfinished_exception_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");

        // Create a separate TaskManager for exception testing
        TaskManager exceptionTaskManager = new TaskManager();
        
        exceptionTaskManager.createTask(task);
        long taskId = task.getId();
        Assertions.assertNotNull(exceptionTaskManager.getTask(taskName));

        // 2. Create pending TaskRun
        TaskRun pendingTaskRun = TaskRunBuilder.newBuilder(task).build();
        pendingTaskRun.setTaskId(taskId);
        pendingTaskRun.initStatus("pending_query_1", System.currentTimeMillis());
        pendingTaskRun.getStatus().setState(Constants.TaskRunState.PENDING);
        exceptionTaskManager.getTaskRunScheduler().addPendingTaskRun(pendingTaskRun);
        Assertions.assertEquals(1, exceptionTaskManager.getTaskRunScheduler().getPendingQueueCount());

        // 3. Mock EditLog.logUpdateTaskRun to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logUpdateTaskRun(any(TaskRunStatusChange.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        long initialPendingCount = exceptionTaskManager.getTaskRunScheduler().getPendingQueueCount();

        // 4. Execute clearUnfinishedTaskRun operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionTaskManager.clearUnfinishedTaskRun();
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(initialPendingCount, exceptionTaskManager.getTaskRunScheduler().getPendingQueueCount());
        
        // Verify pending task run still exists
        Assertions.assertEquals(1, exceptionTaskManager.getTaskRunScheduler().getPendingQueueCount());
    }

    @Test
    public void testAlterTaskNormalCase() throws Exception {
        // 1. Prepare test data - create a manual task first
        String taskName = "alter_task_test";
        Task currentTask = new Task(taskName);
        currentTask.setType(Constants.TaskType.MANUAL);
        currentTask.setDefinition("SELECT 1");
        currentTask.setDbName("test_db");
        currentTask.setCatalogName("test_catalog");
        
        masterTaskManager.createTask(currentTask);
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(Constants.TaskType.MANUAL, createdTask.getType());

        // 2. Prepare changed task - change from MANUAL to PERIODICAL
        Task changedTask = new Task(taskName);
        changedTask.setType(Constants.TaskType.PERIODICAL);
        changedTask.setDefinition("SELECT 1");
        changedTask.setDbName("test_db");
        changedTask.setCatalogName("test_catalog");
        
        // Set up schedule for periodical task
        TaskSchedule schedule = new TaskSchedule();
        schedule.setStartTime(System.currentTimeMillis() / 1000);
        schedule.setPeriod(3600); // 1 hour
        schedule.setTimeUnit(TimeUnit.SECONDS);
        changedTask.setSchedule(schedule);

        // 3. Execute alterTask operation (master side)
        masterTaskManager.alterTask(createdTask, changedTask);

        // 4. Verify master state after alteration
        Task alteredTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(alteredTask);
        Assertions.assertEquals(Constants.TaskType.PERIODICAL, alteredTask.getType());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, alteredTask.getState());
        Assertions.assertNotNull(alteredTask.getSchedule());
        Assertions.assertEquals(3600, alteredTask.getSchedule().getPeriod());
        Assertions.assertNotNull(masterTaskManager.getPeriodFutureMap().get(alteredTask.getId()));

        // 5. Test follower replay functionality
        TaskManager followerTaskManager = new TaskManager();
        
        // First create the task in follower
        Task followerTask = new Task(taskName);
        followerTask.setId(createdTask.getId());
        followerTask.setType(Constants.TaskType.MANUAL);
        followerTask.setDefinition("SELECT 1");
        followerTask.setDbName("test_db");
        followerTask.setCatalogName("test_catalog");
        followerTask.setCreateTime(createdTask.getCreateTime());
        followerTaskManager.replayCreateTask(followerTask);
        
        Assertions.assertEquals(Constants.TaskType.MANUAL, followerTaskManager.getTask(taskName).getType());

        // Replay the alter operation
        AlterTaskInfo alterTaskInfo = (AlterTaskInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TASK);
        
        followerTaskManager.replayAlterTask(alterTaskInfo);

        // 6. Verify follower state is consistent with master
        Task followerAlteredTask = followerTaskManager.getTask(taskName);
        Assertions.assertNotNull(followerAlteredTask);
        Assertions.assertEquals(Constants.TaskType.PERIODICAL, followerAlteredTask.getType());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, followerAlteredTask.getState());
        Assertions.assertNotNull(followerAlteredTask.getSchedule());
        Assertions.assertEquals(3600, followerAlteredTask.getSchedule().getPeriod());
    }

    @Test
    public void testAlterTaskEditLogException() throws Exception {
        // 1. Prepare test data - create a manual task first
        String taskName = "alter_exception_task";
        Task currentTask = new Task(taskName);
        currentTask.setType(Constants.TaskType.MANUAL);
        currentTask.setDefinition("SELECT 1");
        currentTask.setDbName("test_db");
        currentTask.setCatalogName("test_catalog");

        // Create a separate TaskManager for exception testing
        TaskManager exceptionTaskManager = new TaskManager();
        
        exceptionTaskManager.createTask(currentTask);
        Task createdTask = exceptionTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(Constants.TaskType.MANUAL, createdTask.getType());

        // 2. Prepare changed task - change from MANUAL to PERIODICAL
        Task changedTask = new Task(taskName);
        changedTask.setType(Constants.TaskType.PERIODICAL);
        changedTask.setDefinition("SELECT 1");
        changedTask.setDbName("test_db");
        changedTask.setCatalogName("test_catalog");
        
        TaskSchedule schedule = new TaskSchedule();
        schedule.setStartTime(System.currentTimeMillis() / 1000);
        schedule.setPeriod(3600);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        changedTask.setSchedule(schedule);

        // 3. Mock EditLog.logAlterTask to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterTask(any(AlterTaskInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        Constants.TaskType initialType = createdTask.getType();

        // 4. Execute alterTask operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionTaskManager.alterTask(createdTask, changedTask);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Task unchangedTask = exceptionTaskManager.getTask(taskName);
        Assertions.assertNotNull(unchangedTask);
        Assertions.assertEquals(initialType, unchangedTask.getType());
        Assertions.assertNull(unchangedTask.getSchedule());
    }

    @Test
    public void testAlterTaskFromPeriodicalToManual() throws Exception {
        // 1. Prepare test data - create a periodical task first
        String taskName = "alter_periodical_to_manual_task";
        Task currentTask = new Task(taskName);
        currentTask.setType(Constants.TaskType.PERIODICAL);
        currentTask.setDefinition("SELECT 1");
        currentTask.setDbName("test_db");
        currentTask.setCatalogName("test_catalog");
        
        TaskSchedule schedule = new TaskSchedule();
        schedule.setStartTime(System.currentTimeMillis() / 1000);
        schedule.setPeriod(3600);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        currentTask.setSchedule(schedule);
        
        masterTaskManager.createTask(currentTask);
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(Constants.TaskType.PERIODICAL, createdTask.getType());
        Assertions.assertNotNull(masterTaskManager.getPeriodFutureMap().get(createdTask.getId()));
        
        // Save schedule before alteration (since alterTask will set it to null)
        TaskSchedule originalSchedule = createdTask.getSchedule();
        Assertions.assertNotNull(originalSchedule);

        // 2. Prepare changed task - change from PERIODICAL to MANUAL
        Task changedTask = new Task(taskName);
        changedTask.setType(Constants.TaskType.MANUAL);
        changedTask.setDefinition("SELECT 1");
        changedTask.setDbName("test_db");
        changedTask.setCatalogName("test_catalog");

        // 3. Execute alterTask operation
        masterTaskManager.alterTask(createdTask, changedTask);

        // 4. Verify master state after alteration
        Task alteredTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(alteredTask);
        Assertions.assertEquals(Constants.TaskType.MANUAL, alteredTask.getType());
        Assertions.assertEquals(Constants.TaskState.UNKNOWN, alteredTask.getState());
        Assertions.assertNull(alteredTask.getSchedule());
        Assertions.assertNull(masterTaskManager.getPeriodFutureMap().get(alteredTask.getId()));

        // 5. Test follower replay functionality
        TaskManager followerTaskManager = new TaskManager();
        
        // First create the periodical task in follower (use original schedule before alteration)
        Task followerTask = new Task(taskName);
        followerTask.setId(createdTask.getId());
        followerTask.setType(Constants.TaskType.PERIODICAL);
        followerTask.setState(Constants.TaskState.ACTIVE);
        followerTask.setDefinition("SELECT 1");
        followerTask.setDbName("test_db");
        followerTask.setCatalogName("test_catalog");
        followerTask.setCreateTime(createdTask.getCreateTime());
        followerTask.setSchedule(originalSchedule);
        followerTaskManager.replayCreateTask(followerTask);
        
        Task followerTaskAfterCreate = followerTaskManager.getTask(taskName);
        Assertions.assertNotNull(followerTaskAfterCreate, "Follower task should be created");
        Assertions.assertEquals(Constants.TaskType.PERIODICAL, followerTaskAfterCreate.getType());

        // Replay the alter operation
        AlterTaskInfo alterTaskInfo = (AlterTaskInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TASK);
        
        followerTaskManager.replayAlterTask(alterTaskInfo);

        // 6. Verify follower state is consistent with master
        Task followerAlteredTask = followerTaskManager.getTask(taskName);
        Assertions.assertNotNull(followerAlteredTask);
        Assertions.assertEquals(Constants.TaskType.MANUAL, followerAlteredTask.getType());
        Assertions.assertEquals(Constants.TaskState.UNKNOWN, followerAlteredTask.getState());
        Assertions.assertNull(followerAlteredTask.getSchedule());
    }
}
