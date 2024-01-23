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
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

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
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();

        taskManager.createTask(task, true);
        // taskManager.executeTask(taskList.get(0).getName());
        TaskRunManager taskRunManager = taskManager.getTaskRunManager();
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.setProcessor(new MockTaskRunProcessor());
        taskRunManager.submitTaskRun(taskRun, new ExecuteOption());
        List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);
        Constants.TaskRunState state = null;

        int retryCount = 0;
        int maxRetry = 5;
        while (retryCount < maxRetry) {
            if (taskRuns.size() > 0) {
                state = taskRuns.get(0).getState();
            }
            retryCount++;
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
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
        PriorityBlockingQueue<TaskRun> queue = Queues.newPriorityBlockingQueue();
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

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(makeExecuteOption(true, false))
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(10);

        taskRunManager.arrangeTaskRun(taskRun1);
        taskRunManager.arrangeTaskRun(taskRun2);

        Map<Long, PriorityBlockingQueue<TaskRun>> pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(1, pendingTaskRunMap.get(taskId).size());
        PriorityBlockingQueue<TaskRun> taskRuns = pendingTaskRunMap.get(taskId);
        TaskRun taskRun = taskRuns.poll();
        Assert.assertEquals(10, taskRun.getStatus().getPriority());

    }

    @Test
    public void testTaskRunMergePriorityFirst2() {

        TaskRunManager taskRunManager = new TaskRunManager();
        Task task = new Task("test");

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(10);

        taskRunManager.arrangeTaskRun(taskRun2);
        taskRunManager.arrangeTaskRun(taskRun1);

        Map<Long, PriorityBlockingQueue<TaskRun>> pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(1, pendingTaskRunMap.get(taskId).size());
        PriorityBlockingQueue<TaskRun> taskRuns = pendingTaskRunMap.get(taskId);
        TaskRun taskRun = taskRuns.poll();
        Assert.assertEquals(10, taskRun.getStatus().getPriority());

    }

    @Test
    public void testTaskRunMergeTimeFirst() {

        TaskRunManager taskRunManager = new TaskRunManager();
        Task task = new Task("test");

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now + 10);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(0);

        taskRunManager.arrangeTaskRun(taskRun1);
        taskRunManager.arrangeTaskRun(taskRun2);

        Map<Long, PriorityBlockingQueue<TaskRun>> pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(1, pendingTaskRunMap.get(taskId).size());
        PriorityBlockingQueue<TaskRun> taskRuns = pendingTaskRunMap.get(taskId);
        TaskRun taskRun = taskRuns.poll();
        Assert.assertEquals(now + 10, taskRun.getStatus().getCreateTime());

    }

    @Test
    public void testTaskRunMergeTimeFirst2() {

        TaskRunManager taskRunManager = new TaskRunManager();
        Task task = new Task("test");

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now + 10);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(0);

        taskRunManager.arrangeTaskRun(taskRun2);
        taskRunManager.arrangeTaskRun(taskRun1);

        Map<Long, PriorityBlockingQueue<TaskRun>> pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(1, pendingTaskRunMap.get(taskId).size());
        PriorityBlockingQueue<TaskRun> taskRuns = pendingTaskRunMap.get(taskId);
        TaskRun taskRun = taskRuns.poll();
        Assert.assertEquals(now + 10, taskRun.getStatus().getCreateTime());

    }

    @Test
    public void testTaskRunNotMerge() {

        TaskRunManager taskRunManager = new TaskRunManager();
        Task task = new Task("test");

        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_NO_MERGE_OPTION)
                .build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_NO_MERGE_OPTION)
                .build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(10);

        TaskRun taskRun3 = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(DEFAULT_NO_MERGE_OPTION)
                .build();
        taskRun3.setTaskId(taskId);
        taskRun3.initStatus("3", now + 10);
        taskRun3.getStatus().setDefinition("select 1");
        taskRun3.getStatus().setPriority(10);

        taskRunManager.arrangeTaskRun(taskRun2);
        taskRunManager.arrangeTaskRun(taskRun1);
        taskRunManager.arrangeTaskRun(taskRun3);

        Map<Long, PriorityBlockingQueue<TaskRun>> pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(3, pendingTaskRunMap.get(taskId).size());
    }

    @Test
    public void testReplayUpdateTaskRunOutOfOrder() {
        TaskManager taskManager = new TaskManager();
        Task task = new Task("test");
        taskManager.replayCreateTask(task);
        long taskId = 1;

        TaskRun taskRun1 = TaskRunBuilder.newBuilder(task).build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setDefinition("select 1");

        TaskRun taskRun2 = TaskRunBuilder.newBuilder(task).build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskManager.replayCreateTaskRun(taskRun2.getStatus());
        taskManager.replayCreateTaskRun(taskRun1.getStatus());

        TaskRunStatusChange change1 = new TaskRunStatusChange(task.getId(), taskRun2.getStatus(),
                Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
        taskManager.replayUpdateTaskRun(change1);

        Map<Long, TaskRun> runningTaskRunMap = taskManager.getTaskRunManager().getRunningTaskRunMap();
        Assert.assertEquals(1, runningTaskRunMap.values().size());

    }

    @Test
    public void testForceGC() {
        TaskRunManager taskRunManager = new TaskRunManager();
        for (int i = 0; i < 100; i++) {
            TaskRunStatus taskRunStatus = new TaskRunStatus();
            taskRunStatus.setQueryId("test" + i);
            taskRunStatus.setTaskName("test" + i);
            taskRunManager.getTaskRunHistory().addHistory(taskRunStatus);
        }
        Config.task_runs_max_history_number = 20;
        taskRunManager.getTaskRunHistory().forceGC();
        Assert.assertEquals(20, taskRunManager.getTaskRunHistory().getAllHistory().size());
        Config.task_runs_max_history_number = 10000;
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
        Assert.assertEquals(10, taskRunManager.getTaskRunHistory().getAllHistory().size());
        Config.task_runs_max_history_number = 10000;
    }

    private LocalDateTime parseLocalDateTime(String str) throws Exception {
        Date date = TimeUtils.parseDate(str, PrimitiveType.DATETIME);
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    @Test
    public void testGetInitialDelayTime() throws Exception {
        Assert.assertEquals(50, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-04-18 19:08:50"),
                parseLocalDateTime("2023-04-18 20:00:00")));
        Assert.assertEquals(30, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-04-18 19:08:30"),
                parseLocalDateTime("2023-04-18 20:00:00")));
        Assert.assertEquals(20, TaskManager.getInitialDelayTime(60, parseLocalDateTime("2023-04-18 19:08:30"),
                parseLocalDateTime("2023-04-18 20:00:10")));
        Assert.assertEquals(0, TaskManager.getInitialDelayTime(20, parseLocalDateTime("2023-04-18 19:08:30"),
                parseLocalDateTime("2023-04-18 21:00:10")));
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync) {
        ExecuteOption executeOption = new ExecuteOption();
        executeOption.setMergeRedundant(isMergeRedundant);
        executeOption.setSync(isSync);
        return  executeOption;
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

        Map<Long, PriorityBlockingQueue<TaskRun>> pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(2, pendingTaskRunMap.get(taskId).size());

        // If it's a sync refresh, no merge redundant anyway
        TaskRun taskRun3 = makeTaskRun(taskId, task, makeExecuteOption(false, true));
        result = taskRunManager.submitTaskRun(taskRun3, taskRun3.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);

        pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(3, pendingTaskRunMap.get(taskId).size());

        // merge it
        TaskRun taskRun4 = makeTaskRun(taskId, task, makeExecuteOption(true, false));
        result = taskRunManager.submitTaskRun(taskRun4, taskRun4.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);

        pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(3, pendingTaskRunMap.get(taskId).size());

        // no merge it
        TaskRun taskRun5 = makeTaskRun(taskId, task, makeExecuteOption(false, false));
        result = taskRunManager.submitTaskRun(taskRun5, taskRun5.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
        pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(4, pendingTaskRunMap.get(taskId).size());

        for (int i = 4; i < Config.task_runs_queue_length; i++) {
            TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(false, false));
            result = taskRunManager.submitTaskRun(taskRun, taskRun.getExecuteOption());
            Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.SUBMITTED);
            pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
            Assert.assertEquals(i + 1, pendingTaskRunMap.get(taskId).size());
        }
        // no assign it: exceed queue's size
        TaskRun taskRun6 = makeTaskRun(taskId, task, makeExecuteOption(false, false));
        result = taskRunManager.submitTaskRun(taskRun6, taskRun6.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.REJECTED);
        pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(Config.task_runs_queue_length, pendingTaskRunMap.get(taskId).size());

        // no assign it: exceed queue's size
        TaskRun taskRun7 = makeTaskRun(taskId, task, makeExecuteOption(false, false));
        result = taskRunManager.submitTaskRun(taskRun7, taskRun7.getExecuteOption());
        Assert.assertTrue(result.getStatus() == SubmitResult.SubmitStatus.REJECTED);
        pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(Config.task_runs_queue_length, pendingTaskRunMap.get(taskId).size());
    }
}
