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

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.history.TaskRunHistory;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodName.class)
public class TaskManagerTest {

    private static final Logger LOG = LogManager.getLogger(TaskManagerTest.class);

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static final ExecuteOption DEFAULT_MERGE_OPTION = makeExecuteOption(true, false);
    private static final ExecuteOption DEFAULT_NO_MERGE_OPTION = makeExecuteOption(false, false);
    private final TaskRunScheduler taskRunScheduler = new TaskRunScheduler();
    private TaskRun taskRun;

    @BeforeEach
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
        taskRun = new TaskRun();
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
    }

    @Test
    public void submitTaskRegularTest() throws Exception {

        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
        String submitSQL = "submit task as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL, ctx);

        Task task = TaskBuilder.buildTask(submitTaskStmt, ctx);
        String dbName = UUIDUtil.genUUID().toString();
        task.setDbName(dbName);

        String realDbName = task.getDbName();
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();

        taskManager.createTask(task, true);
        TaskRunManager taskRunManager = taskManager.getTaskRunManager();
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.setProcessor(new MockTaskRunProcessor());
        taskRunManager.submitTaskRun(taskRun, new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(),
                false, Maps.newHashMap()));
        List<TaskRunStatus> taskRuns = null;
        Constants.TaskRunState state = null;

        int retryCount = 0;
        int maxRetry = 30;
        TGetTasksParams getTasksParams = new TGetTasksParams();
        getTasksParams.setDb(realDbName);
        while (retryCount < maxRetry) {
            taskRuns = taskManager.getMatchedTaskRunStatus(getTasksParams);
            if (taskRuns.size() > 0) {
                state = taskRuns.get(0).getState();
            }
            retryCount++;
            ThreadUtil.sleepAtLeastIgnoreInterrupts(1000L);
            if (state == Constants.TaskRunState.FAILED || state == Constants.TaskRunState.SUCCESS) {
                break;
            }
            LOG.info("SubmitTaskRegularTest is waiting for TaskRunState retryCount:" + retryCount);
        }
        assertEquals(Constants.TaskRunState.SUCCESS, state);
    }

    @Test
    public void testTaskRunPriority() {
        Queue<TaskRun> queue = Queues.newPriorityBlockingQueue();
        long now = System.currentTimeMillis();
        Task task = new Task("test");

        TaskRun taskRun1 = TaskRunBuilder.newBuilder(task).build();
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder.newBuilder(task).build();
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setPriority(10);

        TaskRun taskRun3 = TaskRunBuilder.newBuilder(task).build();
        taskRun3.initStatus("3", now + 100);
        taskRun3.getStatus().setPriority(5);

        TaskRun taskRun4 = TaskRunBuilder.newBuilder(task).build();
        taskRun4.initStatus("4", now);
        taskRun4.getStatus().setPriority(5);

        queue.offer(taskRun1);
        queue.offer(taskRun2);
        queue.offer(taskRun3);
        queue.offer(taskRun4);

        TaskRunStatus get1 = queue.poll().getStatus();
        assertEquals(10, get1.getPriority());
        TaskRunStatus get2 = queue.poll().getStatus();
        assertEquals(5, get2.getPriority());
        assertEquals(now, get2.getCreateTime());
        TaskRunStatus get3 = queue.poll().getStatus();
        assertEquals(5, get3.getPriority());
        assertEquals(now + 100, get3.getCreateTime());
        TaskRunStatus get4 = queue.poll().getStatus();
        assertEquals(0, get4.getPriority());

    }

    @Test
    public void testTaskRunMergePriorityFirst() {

        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        Task task = new Task("test");
        task.setDefinition("select 1");

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(makeExecuteOption(true, false))
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setPriority(10);

        taskRunManager.arrangeTaskRun(taskRun1, false);
        taskRunManager.arrangeTaskRun(taskRun2, false);

        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();
        List<TaskRun> taskRuns = Lists.newArrayList(taskRunScheduler.getPendingTaskRunsByTaskId(taskId));
        Assertions.assertTrue(taskRuns != null);
        assertEquals(1, taskRuns.size());
        assertEquals(10, taskRuns.get(0).getStatus().getPriority());
    }

    @Test
    public void testTaskRunMergePriorityFirst2() {

        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        Task task = new Task("test");
        task.setDefinition("select 1");

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setPriority(10);

        taskRunManager.arrangeTaskRun(taskRun2, false);
        taskRunManager.arrangeTaskRun(taskRun1, false);

        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();
        List<TaskRun> taskRuns = Lists.newArrayList(taskRunScheduler.getPendingTaskRunsByTaskId(taskId));
        Assertions.assertTrue(taskRuns != null);
        assertEquals(1, taskRuns.size());
        assertEquals(10, taskRuns.get(0).getStatus().getPriority());

    }

    @Test
    public void testTaskRunMergeTimeFirst() {

        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        Task task = new Task("test");
        task.setDefinition("select 1");

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now + 10);
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setPriority(0);

        taskRunManager.arrangeTaskRun(taskRun1, false);
        taskRunManager.arrangeTaskRun(taskRun2, false);

        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();
        List<TaskRun> taskRuns = Lists.newArrayList(taskRunScheduler.getPendingTaskRunsByTaskId(taskId));
        Assertions.assertTrue(taskRuns != null);
        assertEquals(1, taskRuns.size());
        TaskRun taskRun = taskRuns.get(0);
        assertEquals(now, taskRun.getStatus().getCreateTime());
    }

    @Test
    public void testTaskRunMergeTimeFirst2() {

        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        Task task = new Task("test");
        task.setDefinition("select 1");

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now + 10);
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setPriority(0);

        taskRunManager.arrangeTaskRun(taskRun2, false);
        taskRunManager.arrangeTaskRun(taskRun1, false);

        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();
        List<TaskRun> taskRuns = Lists.newArrayList(taskRunScheduler.getPendingTaskRunsByTaskId(taskId));
        Assertions.assertTrue(taskRuns != null);
        assertEquals(1, taskRuns.size());
        TaskRun taskRun = taskRuns.get(0);
        assertEquals(now, taskRun.getStatus().getCreateTime());
    }

    @Test
    public void testTaskRunNotMerge() {

        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        Task task = new Task("test");
        task.setDefinition("select 1");

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_NO_MERGE_OPTION)
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_NO_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setPriority(10);

        TaskRun taskRun3 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_NO_MERGE_OPTION)
                .build();
        taskRun3.setTaskId(taskId);
        taskRun3.initStatus("3", now + 10);
        taskRun3.getStatus().setPriority(10);

        taskRunManager.arrangeTaskRun(taskRun2, false);
        taskRunManager.arrangeTaskRun(taskRun1, false);
        taskRunManager.arrangeTaskRun(taskRun3, false);

        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();
        Collection<TaskRun> taskRuns = taskRunScheduler.getPendingTaskRunsByTaskId(taskId);
        Assertions.assertTrue(taskRuns != null);
        assertEquals(3, taskRuns.size());
    }

    @Test
    public void testReplayUpdateTaskRunOutOfOrder() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);
        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder.newBuilder(task).build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);

        TaskRun taskRun2 = TaskRunBuilder.newBuilder(task).build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskManager.replayCreateTaskRun(taskRun2.getStatus());
        taskManager.replayCreateTaskRun(taskRun1.getStatus());

        TaskRunStatusChange change1 = new TaskRunStatusChange(task.getId(), taskRun2.getStatus(),
                Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
        taskManager.replayUpdateTaskRun(change1);

        TaskRunScheduler taskRunScheduler = taskManager.getTaskRunScheduler();
        assertEquals(1, taskRunScheduler.getRunningTaskCount());
    }

    @Test
    public void testReplayUpdateTaskRun1() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        taskManager.replayCreateTask(task);
        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder.newBuilder(task).build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);

        TaskRun taskRun2 = TaskRunBuilder.newBuilder(task).build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskManager.replayCreateTaskRun(taskRun2.getStatus());
        taskManager.replayCreateTaskRun(taskRun1.getStatus());

        TaskRunScheduler taskRunScheduler = taskManager.getTaskRunScheduler();
        {
            // task run 2
            TaskRunStatusChange change1 = new TaskRunStatusChange(task.getId(), taskRun2.getStatus(),
                    Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
            taskManager.replayUpdateTaskRun(change1);
            assertEquals(1, taskRunScheduler.getRunningTaskCount());
            assertEquals(1, taskRunScheduler.getPendingQueueCount());
        }

        {
            // task run 2
            TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), taskRun2.getStatus(),
                    Constants.TaskRunState.RUNNING, Constants.TaskRunState.FAILED);
            taskManager.replayUpdateTaskRun(change);
            assertEquals(0, taskRunScheduler.getRunningTaskCount());
            assertEquals(1, taskRunScheduler.getPendingQueueCount());
        }

        {
            // task run 1
            TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), taskRun1.getStatus(),
                    Constants.TaskRunState.PENDING, Constants.TaskRunState.FAILED);
            taskManager.replayUpdateTaskRun(change);
            assertEquals(0, taskRunScheduler.getRunningTaskCount());
            assertEquals(0, taskRunScheduler.getPendingQueueCount());
        }
    }

    @Test
    public void testForceGC() {
        Config.enable_task_history_archive = false;
        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        for (int i = 0; i < 100; i++) {
            TaskRunStatus taskRunStatus = new TaskRunStatus();
            taskRunStatus.setQueryId("test" + i);
            taskRunStatus.setTaskName("test" + i);
            taskRunManager.getTaskRunHistory().addHistory(taskRunStatus);
        }
        Config.task_runs_max_history_number = 20;
        taskRunManager.getTaskRunHistory().forceGC();
        assertEquals(20, taskRunManager.getTaskRunHistory().getInMemoryHistory().size());
        Config.task_runs_max_history_number = 10000;
        Config.enable_task_history_archive = true;
    }

    @Test
    public void testForceGC2() {
        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        for (int i = 0; i < 10; i++) {
            TaskRunStatus taskRunStatus = new TaskRunStatus();
            taskRunStatus.setQueryId("test" + i);
            taskRunStatus.setTaskName("test" + i);
            taskRunManager.getTaskRunHistory().addHistory(taskRunStatus);
        }
        Config.task_runs_max_history_number = 20;
        taskRunManager.getTaskRunHistory().forceGC();
        assertEquals(10, taskRunManager.getTaskRunHistory().getInMemoryHistory().size());
        Config.task_runs_max_history_number = 10000;
    }

    private LocalDateTime parseLocalDateTime(String str) throws Exception {
        Date date = TimeUtils.parseDate(str, PrimitiveType.DATETIME);
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    @Test
    public void testGetInitialDelayTime1() throws Exception {
        assertEquals(50, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-04-18 19:08:50"),
                parseLocalDateTime("2023-04-18 20:00:00")));
        assertEquals(30, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-04-18 19:08:30"),
                parseLocalDateTime("2023-04-18 20:00:00")));
        assertEquals(20, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-04-18 19:08:30"),
                parseLocalDateTime("2023-04-18 20:00:10")));
        assertEquals(0, TaskManager.getInitialDelayTime(20, parseLocalDateTime("2023-04-18 19:08:30"),
                parseLocalDateTime("2023-04-18 21:00:10")));
    }

    @Test
    public void testGetInitialDelayTime2() throws Exception {
        assertEquals(23, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-12-29 19:50:00"),
                LocalDateTime.parse("2024-01-30T15:27:37.342356010")));
        assertEquals(50, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-12-29 19:50:00"),
                LocalDateTime.parse("2024-01-30T15:27:10.342356010")));
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync) {
        ExecuteOption executeOption = new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(),
                isMergeRedundant, Maps.newHashMap());
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
    public void testTaskRunMergeRedundant1() {
        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;

        TaskRun taskRun1 = makeTaskRun(taskId, task, makeExecuteOption(true, false));
        TaskRun taskRun2 = makeTaskRun(taskId, task, makeExecuteOption(true, true));

        // If it's a sync refresh, no merge redundant anyway
        SubmitResult result = taskRunManager.submitTaskRun(taskRun1, taskRun1.getExecuteOption());
        Assertions.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
        result = taskRunManager.submitTaskRun(taskRun2, taskRun2.getExecuteOption());
        Assertions.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);

        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();
        Collection<TaskRun> taskRuns = taskRunScheduler.getPendingTaskRunsByTaskId(taskId);
        Assertions.assertTrue(taskRuns != null);
        assertEquals(2, taskRunScheduler.getPendingQueueCount());
        assertEquals(2, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());

        // If it's a sync refresh, no merge redundant anyway
        TaskRun taskRun3 = makeTaskRun(taskId, task, makeExecuteOption(false, true));
        result = taskRunManager.submitTaskRun(taskRun3, taskRun3.getExecuteOption());
        Assertions.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
        assertEquals(3, taskRunScheduler.getPendingQueueCount());
        assertEquals(3, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());
        // merge it
        TaskRun taskRun4 = makeTaskRun(taskId, task, makeExecuteOption(true, false));
        result = taskRunManager.submitTaskRun(taskRun4, taskRun4.getExecuteOption());
        Assertions.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);

        assertEquals(3, taskRunScheduler.getPendingQueueCount());
        assertEquals(3, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());

        // no merge it
        TaskRun taskRun5 = makeTaskRun(taskId, task, makeExecuteOption(false, false));
        result = taskRunManager.submitTaskRun(taskRun5, taskRun5.getExecuteOption());
        Assertions.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
        assertEquals(4, taskRunScheduler.getPendingQueueCount());
        assertEquals(4, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());

        for (int i = 4; i < Config.task_runs_queue_length; i++) {
            TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(false, false));
            result = taskRunManager.submitTaskRun(taskRun, taskRun.getExecuteOption());
            Assertions.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
            assertEquals(i + 1, taskRunScheduler.getPendingQueueCount());
            assertEquals(i + 1, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());
        }
        // no assign it: exceed queue's size
        TaskRun taskRun6 = makeTaskRun(taskId, task, makeExecuteOption(false, false));
        result = taskRunManager.submitTaskRun(taskRun6, taskRun6.getExecuteOption());
        Assertions.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.REJECTED);
        assertEquals(Config.task_runs_queue_length, taskRunScheduler.getPendingQueueCount());
        assertEquals(Config.task_runs_queue_length, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());

        // no assign it: exceed queue's size
        TaskRun taskRun7 = makeTaskRun(taskId, task, makeExecuteOption(false, false));
        result = taskRunManager.submitTaskRun(taskRun7, taskRun7.getExecuteOption());
        Assertions.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.REJECTED);
        assertEquals(Config.task_runs_queue_length, taskRunScheduler.getPendingQueueCount());
        assertEquals(Config.task_runs_queue_length, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());
    }


    @Test
    public void testTaskEquality() {
        Task task1 = new Task("test");
        task1.setDefinition("select 1");
        task1.setId(1);

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task1)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        {
            long now = System.currentTimeMillis();
            taskRun1.setTaskId(task1.getId());
            taskRun1.initStatus("1", now + 10);
            taskRun1.getStatus().setPriority(0);
        }

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task1)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        {
            long now = System.currentTimeMillis();
            taskRun2.setTaskId(task1.getId());
            taskRun2.initStatus("1", now + 10);
            taskRun2.getStatus().setPriority(0);
            Assertions.assertFalse(taskRun1.equals(taskRun2));
        }

        {
            long now = System.currentTimeMillis();
            taskRun2.setTaskId(task1.getId());
            taskRun2.initStatus("2", now + 10);
            taskRun2.getStatus().setPriority(10);
            Assertions.assertFalse(taskRun1.equals(taskRun2));
        }
        {
            long now = System.currentTimeMillis();
            taskRun2.setTaskId(task1.getId());
            taskRun2.initStatus("2", now + 10);
            taskRun2.getStatus().setPriority(10);
            taskRun2.setExecuteOption(DEFAULT_NO_MERGE_OPTION);
            Assertions.assertFalse(taskRun1.equals(taskRun2));
        }

        {
            long now = System.currentTimeMillis();
            taskRun2.setTaskId(2);
            taskRun2.initStatus("2", now + 10);
            taskRun2.getStatus().setPriority(10);
            taskRun2.setExecuteOption(DEFAULT_NO_MERGE_OPTION);
            Assertions.assertFalse(taskRun1.equals(taskRun2));
        }

        {
            long now = System.currentTimeMillis();
            taskRun2.setTaskId(task1.getId());
            taskRun2.initStatus("2", now + 10);
            taskRun2.getStatus().setPriority(10);
            try {
                Field taskRunId = taskRun2.getClass().getDeclaredField("taskRunId");
                taskRunId.setAccessible(true);
                taskRunId.set(taskRun2, taskRun1.getTaskRunId());
            } catch (Exception e) {
                Assertions.fail();
            }
            Assertions.assertTrue(taskRun1.equals(taskRun2));
        }

        {
            Map<Long, TaskRun> map1 = Maps.newHashMap();
            map1.put(task1.getId(), taskRun1);
            Map<Long, TaskRun> map2 = Maps.newHashMap();
            map2.put(task1.getId(), taskRun1);
            Assertions.assertTrue(map1.equals(map2));
            Map<Long, TaskRun> map3 = ImmutableMap.copyOf(map1);
            Assertions.assertTrue(map1.equals(map3));
            Assertions.assertTrue(map1.get(task1.getId()).equals(map3.get(task1.getId())));
        }
    }

    @Test
    public void testSyncRefreshWithoutMergeable() {
        Config.enable_mv_refresh_sync_refresh_mergeable = false;
        TaskManager tm = new TaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        for (int i = 0; i < 10; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(1, task, makeExecuteOption(true, true));
            taskRun.setProcessor(new MockTaskRunProcessor(5000));
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        assertEquals(pendingTaskRunsCount, 10);
    }

    @Test
    public void testSyncRefreshWithMergeable1() {
        TaskManager tm = new TaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        Config.enable_mv_refresh_sync_refresh_mergeable = true;
        for (int i = 0; i < 10; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(1, task, makeExecuteOption(true, true));
            taskRun.setProcessor(new MockTaskRunProcessor(5000));
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        Assertions.assertTrue(pendingTaskRunsCount == 1);
        Config.enable_mv_refresh_sync_refresh_mergeable = false;
    }

    @Test
    public void testSyncRefreshWithMergeable2() {
        TaskManager tm = new TaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        Config.enable_mv_refresh_sync_refresh_mergeable = true;
        for (int i = 0; i < 10; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(1, task, makeExecuteOption(true, true));
            taskRun.setProcessor(new MockTaskRunProcessor(5000));
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
            taskRunScheduler.scheduledPendingTaskRun(t -> {
                try {
                    t.getProcessor().postTaskRun(null);
                } catch (Exception e) {
                    Assertions.fail("Process task run failed:" + e);
                }
            });
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        Assertions.assertTrue(pendingTaskRunsCount == 1);
        Config.enable_mv_refresh_sync_refresh_mergeable = false;
    }

    @Test
    public void testKillTaskRun() {
        TaskManager tm = new TaskManager();
        TaskRunScheduler taskRunScheduler = tm.getTaskRunScheduler();
        for (int i = 0; i < 10; i++) {
            Task task = new Task("test");
            task.setDefinition("select 1");
            TaskRun taskRun = makeTaskRun(1, task, makeExecuteOption(true, false));
            taskRun.setProcessor(new MockTaskRunProcessor(5000));
            tm.getTaskRunManager().submitTaskRun(taskRun, taskRun.getExecuteOption());
        }
        taskRunScheduler.scheduledPendingTaskRun(taskRun -> {
            try {
                taskRun.getProcessor().postTaskRun(null);
            } catch (Exception e) {
                Assertions.fail("Process task run failed:" + e);
            }
        });
        long runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();
        assertEquals(1, runningTaskRunsCount);

        new MockUp<TaskRun>() {
            @Mock
            public ConnectContext getRunCtx() {
                return null;
            }
        };
        // running task run will not be removed if force kill is false
        TaskRunManager taskRunManager = tm.getTaskRunManager();
        taskRunManager.killTaskRun(1L, false);
        assertEquals(1, taskRunScheduler.getRunningTaskCount());
        taskRunManager.killTaskRun(1L, true);
        assertEquals(0, taskRunScheduler.getRunningTaskCount());
    }

    @Test
    public void testTaskRunDefinition() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;
        TaskRun taskRun = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        long now = System.currentTimeMillis();
        taskRun.setTaskId(taskId);
        taskRun.initStatus("1", now + 10);
        taskRun.getStatus().setPriority(0);
        TaskRunStatus taskRunStatus = taskRun.getStatus();
        assertEquals(taskRunStatus.getDefinition(), "select 1");
    }

    @Test
    public void testTaskRunWithLargeDefinition1() {
        Task task = new Task("test");
        StringBuilder sb = new StringBuilder("select ");
        for (int i = 0; i < SystemTable.MAX_FIELD_VARCHAR_LENGTH; i++) {
            sb.append("\n ");
        }
        sb.append(" 1");
        task.setDefinition(sb.toString());

        long taskId = 1;
        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(makeExecuteOption(true, false))
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setPriority(0);

        Assertions.assertTrue(taskRun1.getStatus().getDefinition().equals("select 1"));
    }

    @Test
    public void testTaskRunWithLargeDefinition2() {
        Task task = new Task("test");
        StringBuilder sb = new StringBuilder("select ");
        for (int i = 0; i < SystemTable.MAX_FIELD_VARCHAR_LENGTH; i++) {
            sb.append("'a', \n ");
        }
        sb.append(" 1");
        task.setDefinition(sb.toString());

        long taskId = 1;
        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(makeExecuteOption(true, false))
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setPriority(0);

        String definition = taskRun1.getStatus().getDefinition();
        Assertions.assertTrue(definition.length() == SystemTable.MAX_FIELD_VARCHAR_LENGTH / 4);
    }

    @Test
    public void testTaskRunWithLargeDefinition3() {
        Task task = new Task("test");
        task.setDefinition(null);

        long taskId = 1;
        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(makeExecuteOption(true, false))
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setPriority(0);

        String definition = taskRun1.getStatus().getDefinition();
        Assertions.assertTrue(definition == null);
    }

    @Test
    public void saveTasksV2SkipsSkippedTaskRunStatuses() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        {
            TaskManager taskManager = new TaskManager();
            ImageWriter imageWriter = image.getImageWriter();

            Task task = new Task("task");
            task.setId(1L);
            taskManager.replayCreateTask(task);

            TaskRunStatus skippedStatus = new TaskRunStatus();
            skippedStatus.setTaskId(1);
            skippedStatus.setQueryId("task_run_1");
            skippedStatus.setTaskName("task_run_1");
            skippedStatus.setState(Constants.TaskRunState.SKIPPED);
            skippedStatus.setExpireTime(System.currentTimeMillis() + 1000000);
            taskManager.replayCreateTaskRun(skippedStatus);

            TaskRunStatus validStatus = new TaskRunStatus();
            validStatus.setTaskId(2);
            validStatus.setQueryId("task_run_2");
            validStatus.setTaskName("task_run_2");
            validStatus.setState(Constants.TaskRunState.SUCCESS);
            validStatus.setExpireTime(System.currentTimeMillis() + 1000000);
            taskManager.replayCreateTaskRun(validStatus);

            TaskRunHistory taskRunHistory = taskManager.getTaskRunHistory();
            assertEquals(2, taskRunHistory.getTaskRunCount());

            taskManager.saveTasksV2(imageWriter);
        }

        SRMetaBlockReader imageReader = image.getMetaBlockReader();
        {
            TaskManager taskManager = new TaskManager();
            taskManager.loadTasksV2(imageReader);
            TaskRunHistory taskRunHistory = taskManager.getTaskRunHistory();
            assertEquals(2, taskRunHistory.getTaskRunCount());

            Set<Constants.TaskRunState> expectedStates = ImmutableSet.of(
                    Constants.TaskRunState.SUCCESS, Constants.TaskRunState.SKIPPED);
            taskRunHistory.getInMemoryHistory()
                    .stream()
                    .forEach(status -> Assertions.assertTrue(
                            expectedStates.contains(status.getState()),
                            "Unexpected task run state: " + status.getState()));
        }
    }

    @Test
    public void replayCreateTaskRunHandlesNullStatusGracefully() {
        TaskManager taskManager = new TaskManager();
        taskManager.replayCreateTaskRun(null);
        // No exception should be thrown, and no log entry should indicate a failure.
    }

    @Test
    public void replayCreateTaskRunHandlesInvalidState() {
        TaskManager taskManager = new TaskManager();
        TaskRunStatus invalidStatus = new TaskRunStatus();
        invalidStatus.setState(null);
        invalidStatus.setTaskName("invalidTask");
        taskManager.replayCreateTaskRun(invalidStatus);
        // No exception should be thrown, and no log entry should indicate a failure.
    }

    @Test
    public void replayCreateTaskRunSkipsExpiredFinishedTaskRun() {
        TaskManager taskManager = new TaskManager();
        TaskRunStatus expiredStatus = new TaskRunStatus();
        expiredStatus.setState(Constants.TaskRunState.SUCCESS);
        expiredStatus.setTaskName("expiredTask");
        expiredStatus.setExpireTime(System.currentTimeMillis() - 1000);
        taskManager.replayCreateTaskRun(expiredStatus);
        // The expired task run should be skipped without errors.
    }

    @Test
    public void replayCreateTaskRunProcessesValidPendingTaskRun() {
        TaskManager taskManager = new TaskManager();
        TaskRunStatus validStatus = new TaskRunStatus();
        validStatus.setState(Constants.TaskRunState.PENDING);
        validStatus.setTaskName("validTask");
        validStatus.setExpireTime(System.currentTimeMillis() + 100000);
        taskManager.replayCreateTaskRun(validStatus);
        // The valid task run should be processed without errors.
    }

    @Test
    public void removeExpiredTaskRunsShouldCancelLongRunningTasks() {
        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        TaskRun taskRun = new TaskRun();
        TaskRunStatus status = new TaskRunStatus();
        status.setCreateTime(System.currentTimeMillis() - 5000);
        new MockUp<TaskRun>() {
            @Mock
            public TaskRunStatus getStatus() {
                return status;
            }

            @Mock
            public int getExecuteTimeoutS() {
                return 3;
            }
        };
        new MockUp<TaskRunScheduler>() {
            @Mock
            public Set<TaskRun> getCopiedRunningTaskRuns() {
                return ImmutableSet.of(taskRun);
            }
        };
        new MockUp<TaskRunManager>() {
            @Mock
            void killRunningTaskRun(TaskRun taskRun, boolean force) {
                assertEquals(status, taskRun.getStatus());
            }
        };

        TaskManager taskManager = new TaskManager();
        taskManager.removeExpiredTaskRuns(false);
    }

    @Test
    public void removeExpiredTaskRunsShouldNotCancelTasksWithoutTimeout() {
        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        TaskRun taskRun = new TaskRun();
        TaskRunStatus status = new TaskRunStatus();
        status.setCreateTime(System.currentTimeMillis() - 5000);

        new MockUp<TaskRun>() {
            @Mock
            public TaskRunStatus getStatus() {
                return status;
            }

            @Mock
            public int getExecuteTimeoutS() {
                return 0;
            }
        };
        new MockUp<TaskRunScheduler>() {
            @Mock
            public Set<TaskRun> getCopiedRunningTaskRuns() {
                return ImmutableSet.of(taskRun);
            }
        };
        new MockUp<TaskRunManager>() {
            @Mock
            void killRunningTaskRun(TaskRun taskRun, boolean force) {
                Assertions.fail("Task without timeout should not be canceled");
            }
        };

        TaskManager taskManager = new TaskManager();
        taskManager.removeExpiredTaskRuns(false);
    }

    @Test
    public void removeExpiredTaskRunsShouldNotCancelNonExpiredTasks() {
        TaskRunManager taskRunManager = new TaskRunManager(taskRunScheduler);
        TaskRun taskRun = new TaskRun();
        TaskRunStatus status = new TaskRunStatus();
        status.setCreateTime(System.currentTimeMillis() - 1000);
        new MockUp<TaskRun>() {
            @Mock
            public TaskRunStatus getStatus() {
                return status;
            }

            @Mock
            public int getExecuteTimeoutS() {
                return 5;
            }
        };
        new MockUp<TaskRunScheduler>() {
            @Mock
            public Set<TaskRun> getCopiedRunningTaskRuns() {
                return ImmutableSet.of(taskRun);
            }
        };
        new MockUp<TaskRunManager>() {
            @Mock
            void killRunningTaskRun(TaskRun taskRun, boolean force) {
                Assertions.fail("Non-expired task should not be canceled");
            }
        };

        TaskManager taskManager = new TaskManager();
        taskManager.removeExpiredTaskRuns(false);
    }

    @Test
    void testInitialDelayPositive() {
        long periodSeconds = 60;
        LocalDateTime taskStartTime = LocalDateTime.of(2023, 4, 18, 20, 0, 0);
        LocalDateTime currentDateTime = LocalDateTime.of(2023, 4, 18, 19, 8, 50);
        long delay = TaskManager.getInitialDelayTime(periodSeconds, taskStartTime, currentDateTime);
        assertEquals(3070, delay);
    }

    @Test
    void testInitialDelayNegativeWithNano() {
        long periodSeconds = 60;
        LocalDateTime taskStartTime = LocalDateTime.of(2023, 4, 18, 19, 0, 0);
        LocalDateTime currentDateTime = LocalDateTime.of(2023, 4, 18, 20, 0, 1, 1); // 有nano
        long delay = TaskManager.getInitialDelayTime(periodSeconds, taskStartTime, currentDateTime);
        assertEquals(59, delay);
    }

    @Test
    void testInitialDelayNegativeWithoutNano() {
        long periodSeconds = 60;
        LocalDateTime taskStartTime = LocalDateTime.of(2023, 4, 18, 19, 0, 0);
        LocalDateTime currentDateTime = LocalDateTime.of(2023, 4, 18, 20, 0, 1, 0); // nano = 0
        long delay = TaskManager.getInitialDelayTime(periodSeconds, taskStartTime, currentDateTime);
        // initialDelay = -3601, extra = 0
        // ((-3601 % 60) + 60) % 60 = (-1 + 60) % 60 = 59 % 60 = 59
        assertEquals(59, delay);
    }

    @Test
    void testInitialDelayWithNullLastScheduleTime() {
        long periodSeconds = 20;
        LocalDateTime taskStartTime = LocalDateTime.of(2023, 4, 18, 21, 0, 10);
        LocalDateTime currentDateTime = LocalDateTime.of(2023, 4, 18, 19, 8, 30);
        long delay = TaskManager.getInitialDelayTime(periodSeconds, taskStartTime, currentDateTime);
        assertEquals(1 * 3600 + 51 * 60 + 40, delay); // 1小时51分40秒
    }

    // Java
    @Test
    public void testRegisterSchedulerSetsNextScheduleTimeAndSchedulesTask() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        TaskSchedule schedule = new TaskSchedule();
        long now = System.currentTimeMillis() / 1000 * 1000;
        schedule.setStartTime(now);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        taskManager.registerScheduler(task);
        long nextScheduleTime = task.getNextScheduleTime();
        Assertions.assertTrue(nextScheduleTime > 0);
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId())
        );
    }

    @Test
    public void testRegisterSchedulerWithLastScheduleTime() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        TaskSchedule schedule = new TaskSchedule();
        long now = System.currentTimeMillis() / 1000 * 1000;
        schedule.setStartTime(now - 3600); // 1小时前
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(now - 60);
        taskManager.registerScheduler(task);
        long nextScheduleTime = task.getNextScheduleTime();
        Assertions.assertTrue(nextScheduleTime > 0);
        Assertions.assertNotNull(
                taskManager.getPeriodFutureMap().get(task.getId())
        );
    }

    @Test
    public void testRegisterSchedulerNonPeriodicalTask() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setType(Constants.TaskType.MANUAL);
        
        // Register scheduler should return early for non-periodical tasks
        taskManager.registerScheduler(task);
        
        // Should not create a scheduled future for manual tasks
        Assertions.assertNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerEventTriggeredTask() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        task.setType(Constants.TaskType.EVENT_TRIGGERED);
        
        // Register scheduler should return early for event triggered tasks
        taskManager.registerScheduler(task);
        
        // Should not create a scheduled future for event triggered tasks
        Assertions.assertNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerMVTaskTriggerImmediately() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_mv");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = now - 180000; // 3 minutes ago
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60); // 1 minute period
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(1L);
        
        // Mock executeTask to verify it's called
        final boolean[] executeTaskCalled = {false};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCalled[0] = true;
                assertEquals("test_mv", taskName);
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // Should trigger immediately because lastScheduleTime + period < currentTime
        Assertions.assertTrue(executeTaskCalled[0], "Task should be executed immediately");
        Assertions.assertTrue(task.getLastScheduleTime() >= now, "LastScheduleTime should be updated");
        Assertions.assertNotNull(spyTaskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerMVTaskNoImmediateTriggerWhenLastScheduleBeforeStart() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_mv");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = now - 7200000; // 2 hours ago (before start time)
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(2L);
        
        // Mock executeTask to verify it's NOT called
        final boolean[] executeTaskCalled = {false};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCalled[0] = true;
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // Should not trigger immediately because lastScheduleTime is before taskStartTime
        Assertions.assertFalse(executeTaskCalled[0], "Task should not be executed immediately");
        Assertions.assertNotNull(spyTaskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerMVTaskNoImmediateTriggerWhenNotExpired() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_mv");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = now - 30000; // 30 seconds ago
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60); // 1 minute period
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(3L);
        
        // Mock executeTask to verify it's NOT called
        final boolean[] executeTaskCalled = {false};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCalled[0] = true;
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // Should not trigger immediately because lastScheduleTime + period is in the future
        Assertions.assertFalse(executeTaskCalled[0], "Task should not be executed immediately");
        Assertions.assertNotNull(spyTaskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerNonMVTaskNoImmediateTrigger() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_ctas");
        task.setSource(Constants.TaskSource.CTAS);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = now - 180000; // 3 minutes ago
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60); // 1 minute period
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(4L);
        
        // Mock executeTask to verify it's NOT called
        final boolean[] executeTaskCalled = {false};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCalled[0] = true;
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // Should not trigger immediately because source is not MV
        Assertions.assertFalse(executeTaskCalled[0], "Non-MV task should not be executed immediately");
        Assertions.assertNotNull(spyTaskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerWithMinutesTimeUnit() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_minutes");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(5); // 5 minutes
        schedule.setTimeUnit(TimeUnit.MINUTES);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(5L);
        
        taskManager.registerScheduler(task);
        
        long nextScheduleTime = task.getNextScheduleTime();
        Assertions.assertTrue(nextScheduleTime > 0);
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerWithHoursTimeUnit() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_hours");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(2); // 2 hours
        schedule.setTimeUnit(TimeUnit.HOURS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(6L);
        
        taskManager.registerScheduler(task);
        
        long nextScheduleTime = task.getNextScheduleTime();
        Assertions.assertTrue(nextScheduleTime > 0);
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerWithDaysTimeUnit() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_days");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(1); // 1 day
        schedule.setTimeUnit(TimeUnit.DAYS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(7L);
        
        taskManager.registerScheduler(task);
        
        long nextScheduleTime = task.getNextScheduleTime();
        Assertions.assertTrue(nextScheduleTime > 0);
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerNextScheduleTimeCalculation() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_calculation");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(8L);
        
        taskManager.registerScheduler(task);
        
        long nextScheduleTime = task.getNextScheduleTime();
        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        
        // nextScheduleTime should be within reasonable range (0-60 seconds from now for a 60-second period)
        Assertions.assertTrue(nextScheduleTime > currentTimeSeconds);
        Assertions.assertTrue(nextScheduleTime <= currentTimeSeconds + 60);
    }

    @Test
    public void testRegisterSchedulerExecuteTaskExceptionHandling() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_exception");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = now - 180000; // 3 minutes ago
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(9L);
        
        // Mock executeTask to throw exception
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                throw new DdlException("Test exception");
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        
        // Should not throw exception, should handle it gracefully
        Assertions.assertDoesNotThrow(() -> spyTaskManager.registerScheduler(task));
        
        // Should still register the scheduler despite the exception
        Assertions.assertNotNull(spyTaskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerMultipleTasks() {
        TaskManager taskManager = new TaskManager();
        
        // Register multiple tasks
        for (int i = 0; i < 5; i++) {
            Task task = new Task("test_task_" + i);
            TaskSchedule schedule = new TaskSchedule();
            
            long now = System.currentTimeMillis();
            schedule.setStartTime(now);
            schedule.setPeriod(60 + i * 10); // Different periods
            schedule.setTimeUnit(TimeUnit.SECONDS);
            task.setSchedule(schedule);
            task.setType(Constants.TaskType.PERIODICAL);
            task.setState(Constants.TaskState.ACTIVE);
            task.setLastScheduleTime(-1);
            task.setId(10L + i);
            
            taskManager.registerScheduler(task);
        }
        
        // All tasks should be registered
        assertEquals(5, taskManager.getPeriodFutureMap().size());
        
        // Verify each task has a future
        for (int i = 0; i < 5; i++) {
            Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(10L + i));
        }
    }

    @Test
    public void testRegisterSchedulerWithPipeSource() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_pipe");
        task.setSource(Constants.TaskSource.PIPE);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000;
        long lastScheduleTime = now - 180000;
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(15L);
        
        final boolean[] executeTaskCalled = {false};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCalled[0] = true;
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // PIPE source should not trigger immediately (only MV does)
        Assertions.assertFalse(executeTaskCalled[0]);
        Assertions.assertNotNull(spyTaskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerWithInsertSource() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_insert");
        task.setSource(Constants.TaskSource.INSERT);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(120);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(16L);
        
        taskManager.registerScheduler(task);
        
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
        Assertions.assertTrue(task.getNextScheduleTime() > 0);
    }

    @Test
    public void testRegisterSchedulerWithDatacacheSelectSource() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_datacache");
        task.setSource(Constants.TaskSource.DATACACHE_SELECT);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(300);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(17L);
        
        taskManager.registerScheduler(task);
        
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
        Assertions.assertTrue(task.getNextScheduleTime() > 0);
    }

    @Test
    public void testRegisterSchedulerMVTaskImmediateTriggerBoundaryExactly() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_mv_boundary");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long periodMs = 60000; // 1 minute in milliseconds
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = now - periodMs - 1000; // exactly period + 1 second ago
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(18L);
        
        final boolean[] executeTaskCalled = {false};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCalled[0] = true;
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // Should trigger immediately
        Assertions.assertTrue(executeTaskCalled[0], "Task should be executed immediately at boundary");
    }

    @Test
    public void testRegisterSchedulerMVTaskImmediateTriggerNotAtBoundary() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_mv_not_boundary");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long periodMs = 60000; // 1 minute in milliseconds
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = now - periodMs + 1000; // period - 1 second ago (not yet expired)
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(19L);
        
        final boolean[] executeTaskCalled = {false};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCalled[0] = true;
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // Should NOT trigger immediately
        Assertions.assertFalse(executeTaskCalled[0], "Task should not be executed before period expires");
    }

    @Test
    public void testRegisterSchedulerWithVeryShortPeriod() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_short_period");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(1); // 1 second
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(20L);
        
        taskManager.registerScheduler(task);
        
        long nextScheduleTime = task.getNextScheduleTime();
        Assertions.assertTrue(nextScheduleTime > 0);
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerWithVeryLongPeriod() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_long_period");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(7); // 7 days
        schedule.setTimeUnit(TimeUnit.DAYS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(21L);
        
        taskManager.registerScheduler(task);
        
        long nextScheduleTime = task.getNextScheduleTime();
        Assertions.assertTrue(nextScheduleTime > 0);
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerWithTaskProperties() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_with_properties");
        TaskSchedule schedule = new TaskSchedule();
        
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key1", "value1");
        properties.put("key2", "value2");
        task.setProperties(properties);
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(22L);
        
        taskManager.registerScheduler(task);
        
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
        Assertions.assertEquals(properties, task.getProperties());
    }

    @Test
    public void testRegisterSchedulerWithNullProperties() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_null_properties");
        TaskSchedule schedule = new TaskSchedule();
        
        task.setProperties(null);
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(23L);
        
        // Should not throw exception
        Assertions.assertDoesNotThrow(() -> taskManager.registerScheduler(task));
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerReRegistrationSameTask() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_reregister");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(24L);
        
        // First registration
        taskManager.registerScheduler(task);
        ScheduledFuture<?> firstFuture = taskManager.getPeriodFutureMap().get(task.getId());
        Assertions.assertNotNull(firstFuture);
        
        // Second registration (should replace the first one)
        taskManager.registerScheduler(task);
        ScheduledFuture<?> secondFuture = taskManager.getPeriodFutureMap().get(task.getId());
        Assertions.assertNotNull(secondFuture);
        
        // Should have a future in the map
        assertEquals(1, taskManager.getPeriodFutureMap().size());
    }

    @Test
    public void testRegisterSchedulerMVTaskWithLastScheduleTimeEqualToStart() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_mv_equal_start");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = startTime; // exactly at start time
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(25L);
        
        final boolean[] executeTaskCalled = {false};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCalled[0] = true;
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // lastScheduleTime is not after taskStartTime, so should not trigger
        Assertions.assertFalse(executeTaskCalled[0]);
    }

    @Test
    public void testRegisterSchedulerMVTaskWithLastScheduleTimeJustAfterStart() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_mv_just_after_start");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = startTime + 1000; // 1 second after start time
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(26L);
        
        final boolean[] executeTaskCalled = {false};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCalled[0] = true;
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // lastScheduleTime is after taskStartTime and expired, should trigger
        Assertions.assertTrue(executeTaskCalled[0]);
    }

    @Test
    public void testRegisterSchedulerWithStartTimeInFuture() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_future_start");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long futureStartTime = now + 3600000; // 1 hour in future
        
        schedule.setStartTime(futureStartTime);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(27L);
        
        taskManager.registerScheduler(task);
        
        long nextScheduleTime = task.getNextScheduleTime();
        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        
        // nextScheduleTime should be in the future
        Assertions.assertTrue(nextScheduleTime > currentTimeSeconds);
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerLastScheduleTimeUpdatedOnImmediateTrigger() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_last_schedule_update");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = now - 180000; // 3 minutes ago
        long originalLastScheduleTime = lastScheduleTime;
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(60); // 1 minute period
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(28L);
        
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                // Do nothing
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // lastScheduleTime should be updated to current time
        Assertions.assertTrue(task.getLastScheduleTime() > originalLastScheduleTime);
        Assertions.assertTrue(task.getLastScheduleTime() >= now);
    }

    @Test
    public void testRegisterSchedulerWithMillisecondsTimeUnit() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_milliseconds");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(5000); // 5000 milliseconds = 5 seconds
        schedule.setTimeUnit(TimeUnit.MILLISECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(29L);
        
        taskManager.registerScheduler(task);
        
        long nextScheduleTime = task.getNextScheduleTime();
        Assertions.assertTrue(nextScheduleTime > 0);
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerWithPausedTaskState() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_paused");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.PAUSE); // Paused state
        task.setLastScheduleTime(-1);
        task.setId(30L);
        
        // Should still register even if paused
        taskManager.registerScheduler(task);
        
        Assertions.assertNotNull(taskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerNextScheduleTimeInSeconds() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_next_schedule_seconds");
        TaskSchedule schedule = new TaskSchedule();
        
        long nowMs = System.currentTimeMillis();
        long nowSeconds = nowMs / 1000;
        
        schedule.setStartTime(nowMs);
        schedule.setPeriod(120);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(31L);
        
        taskManager.registerScheduler(task);
        
        long nextScheduleTime = task.getNextScheduleTime();
        
        // nextScheduleTime should be in seconds, not milliseconds
        Assertions.assertTrue(nextScheduleTime < nowMs); // Should be smaller than milliseconds
        Assertions.assertTrue(nextScheduleTime > nowSeconds - 1); // Should be close to current time in seconds
        Assertions.assertTrue(nextScheduleTime <= nowSeconds + 120); // Within the period
    }

    @Test
    public void testRegisterSchedulerMVTaskMultipleConditionsForNoTrigger() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_mv_no_trigger_multi");
        task.setSource(Constants.TaskSource.MV);
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        long startTime = now - 3600000; // 1 hour ago
        long lastScheduleTime = now - 90000; // 1.5 minutes ago
        
        schedule.setStartTime(startTime);
        schedule.setPeriod(120); // 2 minute period (not expired yet)
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(lastScheduleTime);
        task.setId(32L);
        
        final int[] executeTaskCallCount = {0};
        TaskManager spyTaskManager = new TaskManager() {
            @Override
            public void executeTask(String taskName) throws DdlException {
                executeTaskCallCount[0]++;
            }
        };
        
        spyTaskManager.replayCreateTask(task);
        spyTaskManager.registerScheduler(task);
        
        // Should not trigger because period hasn't expired
        assertEquals(0, executeTaskCallCount[0], "Task should not be executed");
        Assertions.assertNotNull(spyTaskManager.getPeriodFutureMap().get(task.getId()));
    }

    @Test
    public void testRegisterSchedulerVerifySchedulerIsActive() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test_scheduler_active");
        TaskSchedule schedule = new TaskSchedule();
        
        long now = System.currentTimeMillis();
        schedule.setStartTime(now);
        schedule.setPeriod(60);
        schedule.setTimeUnit(TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setType(Constants.TaskType.PERIODICAL);
        task.setState(Constants.TaskState.ACTIVE);
        task.setLastScheduleTime(-1);
        task.setId(33L);
        
        taskManager.registerScheduler(task);
        
        ScheduledFuture<?> future = taskManager.getPeriodFutureMap().get(task.getId());
        Assertions.assertNotNull(future);
        
        // Verify the future is not cancelled or done
        Assertions.assertFalse(future.isCancelled(), "Scheduled future should not be cancelled");
        Assertions.assertFalse(future.isDone(), "Scheduled future should not be done immediately");
    }
}