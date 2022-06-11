// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.persist.Schedule;
import com.starrocks.scheduler.persist.TaskRunStatus;
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
import java.time.ZoneOffset;
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
        taskRunManager.submitTaskRun(taskRun);

        ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);

        List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);
        Assert.assertEquals(Constants.TaskRunState.SUCCESS, taskRuns.get(0).getState());

    }

    @Test
    public void periodicalTaskRegularTest() {

        LocalDateTime now = LocalDateTime.now();

        Task task = new Task();
        task.setId(1L);
        task.setName("test_periodical");
        task.setCreateTime(System.currentTimeMillis());
        task.setDbName("test");
        task.setDefinition("select 1");
        task.setExpireTime(0L);
        long startTime = now.plusSeconds(3).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        Schedule schedule = new Schedule(startTime, 5, TimeUnit.SECONDS);
        task.setSchedule(schedule);
        task.setTaskType(Constants.TaskType.PERIODICAL);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        LOG.info("start time is :" + now);
        taskManager.createTask(task, true);

        ThreadUtil.sleepAtLeastIgnoreInterrupts(10000L);

        TaskRunHistory taskRunHistory = taskManager.getTaskRunManager().getTaskRunHistory();
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
        Assert.assertEquals(readTask.getTaskType(), Constants.TaskType.MANUAL);
    }

}
