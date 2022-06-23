// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.DmlStmt;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.persist.TaskSchedule;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.plan.ExecPlan;
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
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
                "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view test.mv1\n" +
                "partition by date_trunc('month',tbl1.k1)\n" +
                "distributed by hash(k2)\n" +
                "refresh manual\n" +
                "properties('replication_num' = '1')\n" +
                "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;");
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
        taskRunManager.submitTaskRun(taskRun);

        ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);

        List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);

        Constants.TaskRunState state = taskRuns.get(0).getState();

        int retryCount = 0, maxRetry = 5;
        while (retryCount < maxRetry) {
            retryCount ++;
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            if (state == Constants.TaskRunState.FAILED || state == Constants.TaskRunState.SUCCESS) {
                break;
            }
        }

        Assert.assertEquals(Constants.TaskRunState.SUCCESS, state);

    }

    @Test
    public void SubmitMvTaskTest() throws DdlException {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {}
        };

        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv1"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        taskManager.createTask(task, true);
        taskManager.executeTask(task.getName());

        ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);

        List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);

        Constants.TaskRunState state = taskRuns.get(0).getState();

        int retryCount = 0, maxRetry = 5;
        while (retryCount < maxRetry) {
            retryCount ++;
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            if (state == Constants.TaskRunState.FAILED || state == Constants.TaskRunState.SUCCESS) {
                break;
            }
        }

        Assert.assertEquals(Constants.TaskRunState.SUCCESS, state);
    }

    @Test
    public void periodicalTaskRegularTest() throws DdlException {

        LocalDateTime now = LocalDateTime.now();

        Task task = new Task();
        task.setName("test_periodical");
        task.setCreateTime(System.currentTimeMillis());
        task.setDbName("test");
        task.setDefinition("select 1");
        task.setExpireTime(0L);
        long startTime = now.plusSeconds(3).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
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
        // upgrade should default task type to normal
        Assert.assertEquals(readTask.getType(), Constants.TaskType.NORMAL);
    }

}
