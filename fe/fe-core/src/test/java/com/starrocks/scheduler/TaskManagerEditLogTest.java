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


import com.starrocks.persist.OperationType;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

<<<<<<< HEAD
=======
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
>>>>>>> 6c86502392 ([BugFix] Stop MV scheduler after inactive (#71265))
import java.util.concurrent.TimeUnit;

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
    public void testAlterTaskFromEventTriggeredToPeriodical() throws Exception {
        // This test verifies the fix for the MV scheduler bug when changing from 
        // REFRESH ASYNC (EVENT_TRIGGERED) to REFRESH ASYNC EVERY(interval) (PERIODICAL)
        
        // 1. Prepare test data - create an event triggered task first (simulating REFRESH ASYNC)
        String taskName = "alter_event_triggered_to_periodical";
        Task currentTask = new Task(taskName);
        currentTask.setType(Constants.TaskType.EVENT_TRIGGERED);
        currentTask.setDefinition("SELECT 1");
        currentTask.setDbName("test_db");
        currentTask.setCatalogName("test_catalog");
        
        masterTaskManager.createTask(currentTask, false);
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(Constants.TaskType.EVENT_TRIGGERED, createdTask.getType());
        // Event triggered tasks should not have a scheduler entry
        Assertions.assertNull(masterTaskManager.getPeriodFutureMap().get(createdTask.getId()));

        // 2. Prepare changed task - change from EVENT_TRIGGERED to PERIODICAL
        // (simulating ALTER MV to REFRESH ASYNC EVERY(2 MINUTE))
        Task changedTask = new Task(taskName);
        changedTask.setType(Constants.TaskType.PERIODICAL);
        changedTask.setDefinition("SELECT 1");
        changedTask.setDbName("test_db");
        changedTask.setCatalogName("test_catalog");
        
        // Set up schedule for periodical task
        TaskSchedule schedule = new TaskSchedule();
        schedule.setStartTime(System.currentTimeMillis() / 1000);
        schedule.setPeriod(120); // 2 minutes
        schedule.setTimeUnit(TimeUnit.SECONDS);
        changedTask.setSchedule(schedule);

        // 3. Execute alterTask operation (master side)
        masterTaskManager.alterTask(createdTask, changedTask, false);

        // 4. Verify master state after alteration
        Task alteredTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(alteredTask);
        Assertions.assertEquals(Constants.TaskType.PERIODICAL, alteredTask.getType());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, alteredTask.getState());
        Assertions.assertNotNull(alteredTask.getSchedule());
        Assertions.assertEquals(120, alteredTask.getSchedule().getPeriod());
        Assertions.assertEquals(TimeUnit.SECONDS, alteredTask.getSchedule().getTimeUnit());
        // This is the key assertion: the scheduler should be registered
        Assertions.assertNotNull(masterTaskManager.getPeriodFutureMap().get(alteredTask.getId()),
                "Scheduler should be registered in periodFutureMap after altering from EVENT_TRIGGERED to PERIODICAL");

        // 5. Test follower replay functionality
        TaskManager followerTaskManager = new TaskManager();
        
        // First create the task in follower
        Task followerTask = new Task(taskName);
        followerTask.setId(createdTask.getId());
        followerTask.setType(Constants.TaskType.EVENT_TRIGGERED);
        followerTask.setDefinition("SELECT 1");
        followerTask.setDbName("test_db");
        followerTask.setCatalogName("test_catalog");
        followerTask.setCreateTime(createdTask.getCreateTime());
        followerTaskManager.replayCreateTask(followerTask);
        
        Assertions.assertEquals(Constants.TaskType.EVENT_TRIGGERED, followerTaskManager.getTask(taskName).getType());

        // Replay the alter operation
        Task alterTaskInfo = (Task) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TASK);
        
        followerTaskManager.replayAlterTask(alterTaskInfo);

        // 6. Verify follower state is consistent with master
        Task followerAlteredTask = followerTaskManager.getTask(taskName);
        Assertions.assertNotNull(followerAlteredTask);
        Assertions.assertEquals(Constants.TaskType.PERIODICAL, followerAlteredTask.getType());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, followerAlteredTask.getState());
        Assertions.assertNotNull(followerAlteredTask.getSchedule());
        Assertions.assertEquals(120, followerAlteredTask.getSchedule().getPeriod());
    }
<<<<<<< HEAD
=======

    @Test
    public void testSuspendTaskNormalCase() throws Exception {
        // 1. Prepare test data - create a periodical task first
        String taskName = "suspend_task_test";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        task.setState(Constants.TaskState.ACTIVE);
        
        TaskSchedule schedule = new TaskSchedule();
        schedule.setStartTime(System.currentTimeMillis() / 1000);
        schedule.setPeriod(3600);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        
        masterTaskManager.createTask(task);
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(Constants.TaskState.ACTIVE, createdTask.getState());
        ScheduledFuture<?> scheduledFuture = masterTaskManager.getPeriodFutureMap().get(createdTask.getId());
        Assertions.assertNotNull(scheduledFuture);

        // 2. Execute suspendTask operation (master side)
        masterTaskManager.suspendTask(createdTask, false);

        // 3. Verify master state after suspension
        Task suspendedTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(suspendedTask);
        Assertions.assertEquals(Constants.TaskState.PAUSE, suspendedTask.getState());
        Assertions.assertTrue(scheduledFuture.isCancelled(),
                "Suspending a periodical task should cancel the registered scheduler future");

        // 4. Test follower replay functionality
        TaskManager followerTaskManager = new TaskManager();
        
        // First create the task in follower
        Task followerTask = new Task(taskName);
        followerTask.setId(createdTask.getId());
        followerTask.setType(Constants.TaskType.PERIODICAL);
        followerTask.setState(Constants.TaskState.ACTIVE);
        followerTask.setDefinition("SELECT 1");
        followerTask.setDbName("test_db");
        followerTask.setCatalogName("test_catalog");
        followerTask.setCreateTime(createdTask.getCreateTime());
        followerTask.setSchedule(schedule);
        followerTaskManager.replayCreateTask(followerTask);
        
        Assertions.assertEquals(Constants.TaskState.ACTIVE, followerTaskManager.getTask(taskName).getState());

        // Replay the suspend operation
        AlterTaskInfo alterTaskInfo = (AlterTaskInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TASK);
        
        followerTaskManager.replayAlterTask(alterTaskInfo);

        // 5. Verify follower state is consistent with master
        Task followerSuspendedTask = followerTaskManager.getTask(taskName);
        Assertions.assertNotNull(followerSuspendedTask);
        Assertions.assertEquals(Constants.TaskState.PAUSE, followerSuspendedTask.getState());
    }

    @Test
    public void testResumeTaskNormalCase() throws Exception {
        // 1. Prepare test data - create a suspended periodical task first
        String taskName = "resume_task_test";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        task.setState(Constants.TaskState.PAUSE);

        TaskSchedule schedule = new TaskSchedule();
        schedule.setStartTime(System.currentTimeMillis() / 1000);
        schedule.setPeriod(3600);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);

        masterTaskManager.createTask(task);
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        // Set state to PAUSE after creation
        masterTaskManager.suspendTask(createdTask, false);

        Task suspendedTask = masterTaskManager.getTask(taskName);
        Assertions.assertEquals(Constants.TaskState.PAUSE, suspendedTask.getState());

        // 2. Execute resumeTask operation (master side)
        masterTaskManager.resumeTask(suspendedTask, false);

        // 3. Verify master state after resumption
        Task resumedTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(resumedTask);
        Assertions.assertEquals(Constants.TaskState.ACTIVE, resumedTask.getState());

        // 4. Test follower replay functionality
        TaskManager followerTaskManager = new TaskManager();

        // First create the task in follower
        Task followerTask = new Task(taskName);
        followerTask.setId(createdTask.getId());
        followerTask.setType(Constants.TaskType.PERIODICAL);
        followerTask.setState(Constants.TaskState.PAUSE);
        followerTask.setDefinition("SELECT 1");
        followerTask.setDbName("test_db");
        followerTask.setCatalogName("test_catalog");
        followerTask.setCreateTime(createdTask.getCreateTime());
        followerTask.setSchedule(schedule);
        followerTaskManager.replayCreateTask(followerTask);

        // The follower task is created with PAUSE state to simulate the state after suspend.
        // In the master, the sequence is: create (ACTIVE) -> suspend (PAUSE) -> resume (ACTIVE).
        // So there are two OP_ALTER_TASK journals to replay: suspend and resume.
        Task followerTaskBeforeReplay = followerTaskManager.getTask(taskName);
        Assertions.assertNotNull(followerTaskBeforeReplay);
        Assertions.assertEquals(Constants.TaskState.PAUSE, followerTaskBeforeReplay.getState());

        // Replay both alter journals: first suspend, then resume
        // 1. Replay the suspend operation (changes state from ACTIVE to PAUSE)
        {
            AlterTaskInfo alterTaskInfo = (AlterTaskInfo) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TASK);
            followerTaskManager.replayAlterTask(alterTaskInfo);
            Task followerSuspendedTask = followerTaskManager.getTask(taskName);
            Assertions.assertNotNull(followerSuspendedTask);
            Assertions.assertEquals(Constants.TaskState.PAUSE, followerSuspendedTask.getState());
        }
        // 2. Replay the resume operation (changes state from PAUSE to ACTIVE)
        {
            AlterTaskInfo alterTaskInfo = (AlterTaskInfo) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TASK);
            followerTaskManager.replayAlterTask(alterTaskInfo);
            Task followerResumedTask = followerTaskManager.getTask(taskName);
            Assertions.assertNotNull(followerResumedTask);
            Assertions.assertEquals(Constants.TaskState.ACTIVE, followerResumedTask.getState());
        }
    }

    @Test
    public void testUpdateTaskPropertiesNormalCase() throws Exception {
        // 1. Prepare test data - create a task first
        String taskName = "update_properties_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        
        masterTaskManager.createTask(task);
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertNull(createdTask.getProperties());

        // 2. Prepare properties to update
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        // 3. Execute updateTaskProperties operation (master side)
        masterTaskManager.updateTaskProperties(createdTask, properties);

        // 4. Verify master state after update
        Task updatedTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(updatedTask);
        Assertions.assertNotNull(updatedTask.getProperties());
        Assertions.assertEquals("value1", updatedTask.getProperties().get("key1"));
        Assertions.assertEquals("value2", updatedTask.getProperties().get("key2"));

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
        
        Assertions.assertNull(followerTaskManager.getTask(taskName).getProperties());

        // Replay the update properties operation
        AlterTaskInfo alterTaskInfo = (AlterTaskInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TASK);
        
        followerTaskManager.replayAlterTask(alterTaskInfo);

        // 6. Verify follower state is consistent with master
        Task followerUpdatedTask = followerTaskManager.getTask(taskName);
        Assertions.assertNotNull(followerUpdatedTask);
        Assertions.assertNotNull(followerUpdatedTask.getProperties());
        Assertions.assertEquals("value1", followerUpdatedTask.getProperties().get("key1"));
        Assertions.assertEquals("value2", followerUpdatedTask.getProperties().get("key2"));
    }

    @Test
    public void testUpdateTaskPropertiesEditLogException() throws Exception {
        // 1. Prepare test data - create a task first
        String taskName = "update_properties_exception_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");

        // Create a separate TaskManager for exception testing
        TaskManager exceptionTaskManager = new TaskManager();
        exceptionTaskManager.createTask(task);
        Task createdTask = exceptionTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Map<String, String> initialProperties = createdTask.getProperties();

        // 2. Mock EditLog.logAlterTask to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterTask(any(AlterTaskInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute updateTaskProperties operation and expect exception
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key1", "value1");
        
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionTaskManager.updateTaskProperties(createdTask, properties);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Task unchangedTask = exceptionTaskManager.getTask(taskName);
        Assertions.assertNotNull(unchangedTask);
        Assertions.assertEquals(initialProperties, unchangedTask.getProperties());
    }

    @Test
    public void testSuspendTaskEditLogException() throws Exception {
        // 1. Prepare test data - create a periodical task
        String taskName = "suspend_exception_task";
        Task task = new Task(taskName);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        task.setState(Constants.TaskState.ACTIVE);
        
        TaskSchedule schedule = new TaskSchedule();
        schedule.setStartTime(System.currentTimeMillis() / 1000);
        schedule.setPeriod(3600);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        
        // Create a separate TaskManager for exception testing
        TaskManager exceptionTaskManager = new TaskManager();
        exceptionTaskManager.createTask(task);
        Task createdTask = exceptionTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(Constants.TaskState.ACTIVE, createdTask.getState());

        // 2. Mock EditLog.logAlterTask to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterTask(any(AlterTaskInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        Constants.TaskState initialState = createdTask.getState();

        // 3. Execute suspendTask operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionTaskManager.suspendTask(createdTask, false);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Task unchangedTask = exceptionTaskManager.getTask(taskName);
        Assertions.assertNotNull(unchangedTask);
        Assertions.assertEquals(initialState, unchangedTask.getState());
    }
>>>>>>> 6c86502392 ([BugFix] Stop MV scheduler after inactive (#71265))
}
