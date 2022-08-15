// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.starrocks.analysis.DmlStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.Coordinator;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.spark_project.guava.collect.MinMaxPriorityQueue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TaskManagerTest {

    private static final Logger LOG = LogManager.getLogger(TaskManagerTest.class);

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

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
        Config.enable_experimental_mv = true;
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
    public void SubmitTaskRegularTest() throws Exception {

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

        int retryCount = 0, maxRetry = 5;
        while (retryCount < maxRetry) {
            state = taskRuns.get(0).getState();
            retryCount ++;
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            if (state == Constants.TaskRunState.FAILED || state == Constants.TaskRunState.SUCCESS) {
                break;
            }
            LOG.info("SubmitTaskRegularTest is waiting for TaskRunState retryCount:" + retryCount);
        }

        Assert.assertEquals(Constants.TaskRunState.SUCCESS, state);

    }

    // This test is temporarily removed because it is unstable,
    // and it will be added back when the cause of the problem is found and fixed.
    public void SubmitMvTaskTest() {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {}
        };
        String sql = "create materialized view test.mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(k2)\n" +
                "refresh manual\n" +
                "properties('replication_num' = '1')\n" +
                "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;";
        Database testDb = null;
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            testDb = GlobalStateMgr.getCurrentState().getDb("test");
            MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv1"));
            Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            taskManager.createTask(task, true);
            taskManager.executeTask(task.getName());
            List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);

            Constants.TaskRunState state = null;
            int retryCount = 0, maxRetry = 5;
            while (retryCount < maxRetry) {
                state = taskRuns.get(0).getState();
                retryCount ++;
                ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
                if (state == Constants.TaskRunState.FAILED || state == Constants.TaskRunState.SUCCESS) {
                    break;
                }
                LOG.info("SubmitMvTaskTest is waiting for TaskRunState retryCount:" + retryCount);
            }
            Assert.assertEquals(Constants.TaskRunState.SUCCESS, state);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (testDb != null) {
                testDb.dropTable("mv1");
            }
        }
    }

    @Test
    public void SubmitMvAsyncTaskTest() {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };
        String sql = "create materialized view test.mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(k2)\n" +
                "refresh async every(interval 20 second)\n" +
                "properties('replication_num' = '1')\n" +
                "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;";
        Database testDb = null;
        try {
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            TaskRunHistory taskRunHistory = taskManager.getTaskRunManager().getTaskRunHistory();
            taskRunHistory.getAllHistory().clear();

            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            testDb = GlobalStateMgr.getCurrentState().getDb("test");

            // at least 2 times = schedule 1 times + execute 1 times
            List<TaskRunStatus> taskRuns = null;
            int retryCount = 0, maxRetry = 5;
            while (retryCount < maxRetry) {
                ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
                taskRuns = taskManager.showTaskRunStatus(null);
                if(taskRuns.size() == 2) {
                    Set<Constants.TaskRunState> taskRunStates =
                            taskRuns.stream().map(TaskRunStatus::getState).collect(Collectors.toSet());
                    if (taskRunStates.size() == 1 && (taskRunStates.contains(Constants.TaskRunState.FAILED) ||
                            taskRunStates.contains(Constants.TaskRunState.SUCCESS))) {
                        break;
                    }
                }
                retryCount++;
                LOG.info("SubmitMvAsyncTaskTest is waiting for TaskRunState retryCount:" + retryCount);
            }
            for (TaskRunStatus taskRun : taskRuns) {
                Assert.assertEquals(Constants.TaskRunState.SUCCESS, taskRun.getState());
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (testDb != null) {
                testDb.dropTable("mv1");
            }
        }
    }

    @Test
    public void periodicalTaskRegularTest(@Mocked RowBatch rowBatch) throws DdlException {
        new MockUp<Coordinator>() {
            @Mock
            public void exec() throws Exception {}
        };

        LocalDateTime now = LocalDateTime.now();

        Task task = new Task("test_periodical");
        task.setCreateTime(System.currentTimeMillis());
        task.setDbName("test");
        task.setDefinition("select 1");
        task.setExpireTime(0L);
        long startTime = Utils.getLongFromDateTime(LocalDateTime.now().plusSeconds(3));
        TaskSchedule taskSchedule = new TaskSchedule(startTime, 5, TimeUnit.SECONDS);
        task.setSchedule(taskSchedule);
        task.setType(Constants.TaskType.PERIODICAL);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        LOG.info("start time is :" + now);
        taskManager.createTask(task, false);
        TaskRunHistory taskRunHistory = taskManager.getTaskRunManager().getTaskRunHistory();
        taskRunHistory.getAllHistory().clear();

        ThreadUtil.sleepAtLeastIgnoreInterrupts(10000L);
        taskManager.dropTasks(ImmutableList.of(task.getId()), true);

        Deque<TaskRunStatus> allHistory = taskRunHistory.getAllHistory();
        for (TaskRunStatus taskRunStatus : allHistory) {
            LOG.info(taskRunStatus);
        }

        Assert.assertEquals(2, allHistory.size());
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

        TaskRun taskRun1 = TaskRunBuilder.newBuilder(task).build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder.newBuilder(task).build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(10);

        taskRunManager.arrangeTaskRun(taskRun1, true);
        taskRunManager.arrangeTaskRun(taskRun2, true);

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

        TaskRun taskRun1 = TaskRunBuilder.newBuilder(task).build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder.newBuilder(task).build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(10);

        taskRunManager.arrangeTaskRun(taskRun2, true);
        taskRunManager.arrangeTaskRun(taskRun1, true);

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

        TaskRun taskRun1 = TaskRunBuilder.newBuilder(task).build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now + 10);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder.newBuilder(task).build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(0);

        taskRunManager.arrangeTaskRun(taskRun1, true);
        taskRunManager.arrangeTaskRun(taskRun2, true);

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

        TaskRun taskRun1 = TaskRunBuilder.newBuilder(task).build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now + 10);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder.newBuilder(task).build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(0);

        taskRunManager.arrangeTaskRun(taskRun2, true);
        taskRunManager.arrangeTaskRun(taskRun1, true);

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

        TaskRun taskRun1 = TaskRunBuilder.newBuilder(task).build();
        long now = System.currentTimeMillis();
        taskRun1.setTaskId(taskId);
        taskRun1.initStatus("1", now);
        taskRun1.getStatus().setDefinition("select 1");
        taskRun1.getStatus().setPriority(0);

        TaskRun taskRun2 = TaskRunBuilder.newBuilder(task).build();
        taskRun2.setTaskId(taskId);
        taskRun2.initStatus("2", now);
        taskRun2.getStatus().setDefinition("select 1");
        taskRun2.getStatus().setPriority(10);

        TaskRun taskRun3 = TaskRunBuilder.newBuilder(task).build();
        taskRun3.setTaskId(taskId);
        taskRun3.initStatus("3", now + 10);
        taskRun3.getStatus().setDefinition("select 1");
        taskRun3.getStatus().setPriority(10);

        taskRunManager.arrangeTaskRun(taskRun2, false);
        taskRunManager.arrangeTaskRun(taskRun1, false);
        taskRunManager.arrangeTaskRun(taskRun3, false);

        Map<Long, PriorityBlockingQueue<TaskRun>> pendingTaskRunMap = taskRunManager.getPendingTaskRunMap();
        Assert.assertEquals(3, pendingTaskRunMap.get(taskId).size());

    }


}
