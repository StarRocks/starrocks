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
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TaskManagerTest {

    private static final Logger LOG = LogManager.getLogger(TaskManagerTest.class);

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static final ExecuteOption DEFAULT_MERGE_OPTION = makeExecuteOption(true, false);
    private static final ExecuteOption DEFAULT_NO_MERGE_OPTION = makeExecuteOption(false, false);

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
        Assert.assertEquals(Constants.TaskRunState.SUCCESS, state);
    }

    @Test
    public void taskSerializeTest() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String submitSQL = "submit task as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL, ctx);
        Task task = TaskBuilder.buildTask(submitTaskStmt, ctx);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        task.write(dataOutputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        Task readTask = Task.read(dataInputStream);
        // upgrade should default task type to manual
        Assert.assertEquals(readTask.getType(), Constants.TaskType.MANUAL);
        Assert.assertEquals(readTask.getState(), Constants.TaskState.UNKNOWN);
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
        Assert.assertEquals(10, get1.getPriority());
        TaskRunStatus get2 = queue.poll().getStatus();
        Assert.assertEquals(5, get2.getPriority());
        Assert.assertEquals(now, get2.getCreateTime());
        TaskRunStatus get3 = queue.poll().getStatus();
        Assert.assertEquals(5, get3.getPriority());
        Assert.assertEquals(now + 100, get3.getCreateTime());
        TaskRunStatus get4 = queue.poll().getStatus();
        Assert.assertEquals(0, get4.getPriority());

    }

    @Test
    public void testTaskRunMergePriorityFirst() {

        TaskRunManager taskRunManager = new TaskRunManager();
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
        Assert.assertTrue(taskRuns != null);
        Assert.assertEquals(1, taskRuns.size());
        Assert.assertEquals(10, taskRuns.get(0).getStatus().getPriority());
    }

    @Test
    public void testTaskRunMergePriorityFirst2() {

        TaskRunManager taskRunManager = new TaskRunManager();
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
        Assert.assertTrue(taskRuns != null);
        Assert.assertEquals(1, taskRuns.size());
        Assert.assertEquals(10, taskRuns.get(0).getStatus().getPriority());

    }

    @Test
    public void testTaskRunMergeTimeFirst() {

        TaskRunManager taskRunManager = new TaskRunManager();
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
        Assert.assertTrue(taskRuns != null);
        Assert.assertEquals(1, taskRuns.size());
        TaskRun taskRun = taskRuns.get(0);
        Assert.assertEquals(now, taskRun.getStatus().getCreateTime());
    }

    @Test
    public void testTaskRunMergeTimeFirst2() {

        TaskRunManager taskRunManager = new TaskRunManager();
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
        Assert.assertTrue(taskRuns != null);
        Assert.assertEquals(1, taskRuns.size());
        TaskRun taskRun = taskRuns.get(0);
        Assert.assertEquals(now, taskRun.getStatus().getCreateTime());
    }

    @Test
    public void testTaskRunNotMerge() {

        TaskRunManager taskRunManager = new TaskRunManager();
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
        Assert.assertTrue(taskRuns != null);
        Assert.assertEquals(3, taskRuns.size());
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
        Assert.assertEquals(1, taskRunScheduler.getRunningTaskCount());
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
            Assert.assertEquals(1, taskRunScheduler.getRunningTaskCount());
            Assert.assertEquals(1, taskRunScheduler.getPendingQueueCount());
        }

        {
            // task run 2
            TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), taskRun2.getStatus(),
                    Constants.TaskRunState.RUNNING, Constants.TaskRunState.FAILED);
            taskManager.replayUpdateTaskRun(change);
            Assert.assertEquals(0, taskRunScheduler.getRunningTaskCount());
            Assert.assertEquals(1, taskRunScheduler.getPendingQueueCount());
        }

        {
            // task run 1
            TaskRunStatusChange change = new TaskRunStatusChange(task.getId(), taskRun1.getStatus(),
                    Constants.TaskRunState.PENDING, Constants.TaskRunState.FAILED);
            taskManager.replayUpdateTaskRun(change);
            Assert.assertEquals(0, taskRunScheduler.getRunningTaskCount());
            Assert.assertEquals(0, taskRunScheduler.getPendingQueueCount());
        }
    }

    @Test
    public void testForceGC() {
        Config.enable_task_history_archive = false;
        TaskRunManager taskRunManager = new TaskRunManager();
        for (int i = 0; i < 100; i++) {
            TaskRunStatus taskRunStatus = new TaskRunStatus();
            taskRunStatus.setQueryId("test" + i);
            taskRunStatus.setTaskName("test" + i);
            taskRunManager.getTaskRunHistory().addHistory(taskRunStatus);
        }
        Config.task_runs_max_history_number = 20;
        taskRunManager.getTaskRunHistory().forceGC();
        Assert.assertEquals(20, taskRunManager.getTaskRunHistory().getInMemoryHistory().size());
        Config.task_runs_max_history_number = 10000;
        Config.enable_task_history_archive = true;
    }

    @Test
    public void testForceGC2() {
        TaskRunManager taskRunManager = new TaskRunManager();
        for (int i = 0; i < 10; i++) {
            TaskRunStatus taskRunStatus = new TaskRunStatus();
            taskRunStatus.setQueryId("test" + i);
            taskRunStatus.setTaskName("test" + i);
            taskRunManager.getTaskRunHistory().addHistory(taskRunStatus);
        }
        Config.task_runs_max_history_number = 20;
        taskRunManager.getTaskRunHistory().forceGC();
        Assert.assertEquals(10, taskRunManager.getTaskRunHistory().getInMemoryHistory().size());
        Config.task_runs_max_history_number = 10000;
    }

    private LocalDateTime parseLocalDateTime(String str) throws Exception {
        Date date = TimeUtils.parseDate(str, PrimitiveType.DATETIME);
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    @Test
    public void testGetInitialDelayTime1() throws Exception {
        Assert.assertEquals(50, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-04-18 19:08:50"),
                parseLocalDateTime("2023-04-18 20:00:00")));
        Assert.assertEquals(30, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-04-18 19:08:30"),
                parseLocalDateTime("2023-04-18 20:00:00")));
        Assert.assertEquals(20, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-04-18 19:08:30"),
                parseLocalDateTime("2023-04-18 20:00:10")));
        Assert.assertEquals(0, TaskManager.getInitialDelayTime(20, parseLocalDateTime("2023-04-18 19:08:30"),
                parseLocalDateTime("2023-04-18 21:00:10")));
    }

    @Test
    public void testGetInitialDelayTime2() throws Exception {
        Assert.assertEquals(23, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-12-29 19:50:00"),
                LocalDateTime.parse("2024-01-30T15:27:37.342356010")));
        Assert.assertEquals(50, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-12-29 19:50:00"),
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
        TaskRunManager taskRunManager = new TaskRunManager();
        Task task = new Task("test");
        task.setDefinition("select 1");
        long taskId = 1;

        TaskRun taskRun1 = makeTaskRun(taskId, task, makeExecuteOption(true, false));
        TaskRun taskRun2 = makeTaskRun(taskId, task, makeExecuteOption(true, true));

        // If it's a sync refresh, no merge redundant anyway
        SubmitResult result = taskRunManager.submitTaskRun(taskRun1, taskRun1.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
        result = taskRunManager.submitTaskRun(taskRun2, taskRun2.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);

        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();
        Collection<TaskRun> taskRuns = taskRunScheduler.getPendingTaskRunsByTaskId(taskId);
        Assert.assertTrue(taskRuns != null);
        Assert.assertEquals(2, taskRunScheduler.getPendingQueueCount());
        Assert.assertEquals(2, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());

        // If it's a sync refresh, no merge redundant anyway
        TaskRun taskRun3 = makeTaskRun(taskId, task, makeExecuteOption(false, true));
        result = taskRunManager.submitTaskRun(taskRun3, taskRun3.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
        Assert.assertEquals(3, taskRunScheduler.getPendingQueueCount());
        Assert.assertEquals(3, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());
        // merge it
        TaskRun taskRun4 = makeTaskRun(taskId, task, makeExecuteOption(true, false));
        result = taskRunManager.submitTaskRun(taskRun4, taskRun4.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);

        Assert.assertEquals(3, taskRunScheduler.getPendingQueueCount());
        Assert.assertEquals(3, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());

        // no merge it
        TaskRun taskRun5 = makeTaskRun(taskId, task, makeExecuteOption(false, false));
        result = taskRunManager.submitTaskRun(taskRun5, taskRun5.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
        Assert.assertEquals(4, taskRunScheduler.getPendingQueueCount());
        Assert.assertEquals(4, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());

        for (int i = 4; i < Config.task_runs_queue_length; i++) {
            TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(false, false));
            result = taskRunManager.submitTaskRun(taskRun, taskRun.getExecuteOption());
            Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
            Assert.assertEquals(i + 1, taskRunScheduler.getPendingQueueCount());
            Assert.assertEquals(i + 1, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());
        }
        // no assign it: exceed queue's size
        TaskRun taskRun6 = makeTaskRun(taskId, task, makeExecuteOption(false, false));
        result = taskRunManager.submitTaskRun(taskRun6, taskRun6.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.REJECTED);
        Assert.assertEquals(Config.task_runs_queue_length, taskRunScheduler.getPendingQueueCount());
        Assert.assertEquals(Config.task_runs_queue_length, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());

        // no assign it: exceed queue's size
        TaskRun taskRun7 = makeTaskRun(taskId, task, makeExecuteOption(false, false));
        result = taskRunManager.submitTaskRun(taskRun7, taskRun7.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.REJECTED);
        Assert.assertEquals(Config.task_runs_queue_length, taskRunScheduler.getPendingQueueCount());
        Assert.assertEquals(Config.task_runs_queue_length, taskRunScheduler.getPendingTaskRunsByTaskId(taskId).size());
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
            Assert.assertFalse(taskRun1.equals(taskRun2));
        }

        {
            long now = System.currentTimeMillis();
            taskRun2.setTaskId(task1.getId());
            taskRun2.initStatus("2", now + 10);
            taskRun2.getStatus().setPriority(10);
            Assert.assertFalse(taskRun1.equals(taskRun2));
        }
        {
            long now = System.currentTimeMillis();
            taskRun2.setTaskId(task1.getId());
            taskRun2.initStatus("2", now + 10);
            taskRun2.getStatus().setPriority(10);
            taskRun2.setExecuteOption(DEFAULT_NO_MERGE_OPTION);
            Assert.assertFalse(taskRun1.equals(taskRun2));
        }

        {
            long now = System.currentTimeMillis();
            taskRun2.setTaskId(2);
            taskRun2.initStatus("2", now + 10);
            taskRun2.getStatus().setPriority(10);
            taskRun2.setExecuteOption(DEFAULT_NO_MERGE_OPTION);
            Assert.assertFalse(taskRun1.equals(taskRun2));
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
                Assert.fail();
            }
            Assert.assertTrue(taskRun1.equals(taskRun2));
        }

        {
            Map<Long, TaskRun> map1 = Maps.newHashMap();
            map1.put(task1.getId(), taskRun1);
            Map<Long, TaskRun> map2 = Maps.newHashMap();
            map2.put(task1.getId(), taskRun1);
            Assert.assertTrue(map1.equals(map2));
            Map<Long, TaskRun> map3 = ImmutableMap.copyOf(map1);
            Assert.assertTrue(map1.equals(map3));
            Assert.assertTrue(map1.get(task1.getId()).equals(map3.get(task1.getId())));
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
        Assert.assertEquals(pendingTaskRunsCount, 10);
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
        Assert.assertTrue(pendingTaskRunsCount == 1);
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
                    Assert.fail("Process task run failed:" + e);
                }
            });
        }
        long pendingTaskRunsCount = taskRunScheduler.getPendingQueueCount();
        Assert.assertTrue(pendingTaskRunsCount == 1);
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
                Assert.fail("Process task run failed:" + e);
            }
        });
        long runningTaskRunsCount = taskRunScheduler.getRunningTaskCount();
        Assert.assertEquals(1, runningTaskRunsCount);

        new MockUp<TaskRun>() {
            @Mock
            public ConnectContext getRunCtx() {
                return null;
            }
        };
        // running task run will not be removed if force kill is false
        TaskRunManager taskRunManager = tm.getTaskRunManager();
        taskRunManager.killTaskRun(1L, false);
        Assert.assertEquals(1, taskRunScheduler.getRunningTaskCount());
        taskRunManager.killTaskRun(1L, true);
        Assert.assertEquals(0, taskRunScheduler.getRunningTaskCount());
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
        Assert.assertEquals(taskRunStatus.getDefinition(), "select 1");
    }
}