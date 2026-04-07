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

import java.util.concurrent.ScheduledFuture;
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

    @Test
    public void testSuspendTaskNormalCase() throws Exception {
        // 1. Create a periodical task
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

        masterTaskManager.createTask(task, false);
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(Constants.TaskState.ACTIVE, createdTask.getState());
        ScheduledFuture<?> scheduledFuture = masterTaskManager.getPeriodFutureMap().get(createdTask.getId());
        Assertions.assertNotNull(scheduledFuture);

        // 2. Suspend the task
        masterTaskManager.suspendTask(createdTask);

        // 3. Verify the scheduler future was cancelled
        Task suspendedTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(suspendedTask);
        Assertions.assertEquals(Constants.TaskState.PAUSE, suspendedTask.getState());
        Assertions.assertTrue(scheduledFuture.isCancelled(),
                "Suspending a periodical task should cancel the registered scheduler future");
    }

    @Test
    public void testResumeTaskNormalCase() throws Exception {
        // 1. Create and suspend a periodical task
        String taskName = "resume_task_test";
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

        masterTaskManager.createTask(task, false);
        Task createdTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(createdTask);

        masterTaskManager.suspendTask(createdTask);
        Task suspendedTask = masterTaskManager.getTask(taskName);
        Assertions.assertEquals(Constants.TaskState.PAUSE, suspendedTask.getState());

        // 2. Resume the task
        masterTaskManager.resumeTask(suspendedTask);

        // 3. Verify state and scheduler re-registration
        Task resumedTask = masterTaskManager.getTask(taskName);
        Assertions.assertNotNull(resumedTask);
        Assertions.assertEquals(Constants.TaskState.ACTIVE, resumedTask.getState());
        Assertions.assertNotNull(masterTaskManager.getPeriodFutureMap().get(resumedTask.getId()),
                "Resuming a periodical task should re-register the scheduler future");
    }
}
