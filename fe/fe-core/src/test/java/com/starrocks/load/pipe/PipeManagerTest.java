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

package com.starrocks.load.pipe;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.pipe.filelist.FileListRepo;
import com.starrocks.load.pipe.filelist.FileListTableRepo;
import com.starrocks.load.pipe.filelist.RepoAccessor;
import com.starrocks.load.pipe.filelist.RepoExecutor;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.SubmitResult;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.analyzer.PipeAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.AlterPipeClauseRetry;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TListPipeFilesParams;
import com.starrocks.thrift.TListPipeFilesResult;
import com.starrocks.thrift.TListPipesParams;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.util.Strings;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PipeManagerTest {

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;
    private static final String PIPE_TEST_DB = "pipe_test_db";

    @BeforeClass
    public static void setup() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(ctx);
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);

        // create database
        starRocksAssert.withDatabase(PIPE_TEST_DB);
        ctx.setDatabase(PIPE_TEST_DB);

        // create table
        starRocksAssert.withTable(
                "create table tbl (col_int int, col_string string) properties('replication_num'='1') ");

        starRocksAssert.withTable(
                "create table tbl1 (col_int int, col_string string) properties('replication_num'='1') ");

        // Disable global scheduler
        GlobalStateMgr.getCurrentState().getPipeListener().setStop();
        GlobalStateMgr.getCurrentState().getPipeScheduler().setStop();
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @After
    public void after() {
        long dbId = ctx.getGlobalStateMgr().getDb(PIPE_TEST_DB).getId();
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        pm.dropPipesOfDb(PIPE_TEST_DB, dbId);
    }

    private void createPipe(String sql) throws Exception {
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        CreatePipeStmt createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.createPipe(createStmt);
    }

    private void alterPipe(String sql) throws Exception {
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        AlterPipeStmt createStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.alterPipe(createStmt);
    }

    private void dropPipe(String name) throws Exception {
        String sql = "drop pipe " + name;
        DropPipeStmt dropStmt = (DropPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        pm.dropPipe(dropStmt);
    }

    private Pipe getPipe(String name) {
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        return pm.mayGetPipe(new PipeName(PIPE_TEST_DB, name)).get();
    }

    private void resumePipe(String name) throws Exception {
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        String sql = "alter pipe " + name + " resume";
        AlterPipeStmt alterStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.alterPipe(alterStmt);
    }

    private void suspendPipe(String name) throws Exception {
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        String sql = "alter pipe " + name + " suspend";
        AlterPipeStmt alterStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.alterPipe(alterStmt);
    }

    private void waitPipeTaskFinish(String name) {
        Pipe pipe = getPipe(name);
        Stopwatch watch = Stopwatch.createStarted();
        while (pipe.getState() != Pipe.State.FINISHED) {
            if (watch.elapsed(TimeUnit.SECONDS) > 60) {
                Assert.fail("wait for pipe but failed: elapsed " + watch.elapsed(TimeUnit.SECONDS));
            }
            pipe.schedule();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Assert.fail("wait for pipe but failed: " + e);
            }
        }
    }

    @Test
    public void testPipeWithWarehouse() throws Exception {
        // not exists
        String sql = "create pipe p_warehouse properties('warehouse' = 'w1') " +
                "as insert into tbl select * from files('path'='fake://pipe', 'format'='parquet')";
        Exception e = Assert.assertThrows(AnalysisException.class, () -> createPipe(sql));
        Assert.assertEquals("Getting analyzing error. Detail message: Invalid parameter w1.", e.getMessage());

        // mock the warehouse
        new MockUp<PipeAnalyzer>() {
            @Mock
            public void analyzeWarehouseProperty(String warehouseName) {
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public boolean warehouseExists(String warehouseName) {
                return true;
            }
        };

        createPipe(sql);
        Pipe pipe = getPipe("p_warehouse");
        Assert.assertTrue(pipe.getTaskProperties().toString(),
                pipe.getTaskProperties().containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
        Assert.assertEquals("('warehouse'='w1')", pipe.getPropertiesString());

        // alter pipe
        alterPipe("alter pipe p_warehouse set('warehouse' = 'w2') ");
        Assert.assertEquals(pipe.getTaskProperties().toString(),
                "w2", pipe.getTaskProperties().get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
    }

    @Test
    public void persistPipe() throws Exception {
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        pm.clear();

        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        long dbId = ctx.getGlobalStateMgr().getDb(PIPE_TEST_DB).getId();
        pm.dropPipesOfDb(PIPE_TEST_DB, dbId);

        // create pipe 1
        String sql = "create pipe p1 as insert into tbl select * from files('path'='fake://pipe', 'format'='parquet')";
        CreatePipeStmt createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.createPipe(createStmt);
        UtFrameUtils.PseudoImage image1 = new UtFrameUtils.PseudoImage();
        pm.getRepo().saveImage(image1.getDataOutputStream(), 123);

        // restore from image
        PipeManager pm1 = new PipeManager();
        pm1.getRepo().loadImage(image1.getDataInputStream(), 123);
        Assert.assertEquals(pm.getPipesUnlock(), pm1.getPipesUnlock());

        // create pipe 2
        // pause pipe 1
        sql = "create pipe p2 as insert into tbl select * from files('path'='fake://pipe', 'format'='parquet')";
        CreatePipeStmt createStmt1 = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.createPipe(createStmt1);
        sql = "alter pipe p1 suspend";
        AlterPipeStmt alterPipeStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.alterPipe(alterPipeStmt);
        UtFrameUtils.PseudoImage image2 = new UtFrameUtils.PseudoImage();
        pm.getRepo().saveImage(image2.getDataOutputStream(), 123);

        // restore and check
        PipeManager pm2 = new PipeManager();
        pm2.getRepo().loadImage(image2.getDataInputStream(), 123);
        Assert.assertEquals(pm.getPipesUnlock(), pm2.getPipesUnlock());
        Pipe p1 = pm2.mayGetPipe(new PipeName(PIPE_TEST_DB, "p1")).get();
        Assert.assertEquals(Pipe.State.SUSPEND, p1.getState());

        // replay journal at follower
        PipeManager follower = new PipeManager();
        PipeOpEntry opEntry = (PipeOpEntry) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        follower.getRepo().replay(opEntry);
        Assert.assertEquals(pm1.getPipesUnlock(), follower.getPipesUnlock());
        opEntry = (PipeOpEntry) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        follower.getRepo().replay(opEntry);
        opEntry = (PipeOpEntry) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        follower.getRepo().replay(opEntry);
        Assert.assertEquals(pm2.getPipesUnlock(), follower.getPipesUnlock());

        // Validate pipe execution
        Pipe p2 = follower.mayGetPipe(new PipeName(PIPE_TEST_DB, "p2")).get();
        p2.poll();
        p2.schedule();
        p1 = follower.mayGetPipe(new PipeName(PIPE_TEST_DB, "p1")).get();
        Assert.assertEquals(Pipe.State.SUSPEND, p1.getState());
    }

    private void mockTaskLongRunning(long runningSecs, Constants.TaskRunState result) {
        new MockUp<TaskRunExecutor>() {
            @Mock
            public void executeTaskRun(TaskRun taskRun) {

                ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
                executorService.schedule(() -> {
                    taskRun.getFuture().complete(result);
                }, runningSecs, TimeUnit.SECONDS);
            }
        };
    }

    private void mockTaskExecutor(Supplier<Constants.TaskRunState> runnable) {

        new MockUp<TaskRunExecutor>() {
            @Mock
            public void executeTaskRun(TaskRun taskRun) {
                try {
                    Constants.TaskRunState result = runnable.get();
                    taskRun.getFuture().complete(result);
                } catch (Exception e) {
                    taskRun.getFuture().completeExceptionally(e);
                }

            }
        };
    }

    private void mockTaskExecution(Constants.TaskRunState executionState) {
        new MockUp<TaskManager>() {
            @Mock
            public SubmitResult executeTaskAsync(Task task, ExecuteOption option) {
                CompletableFuture<Constants.TaskRunState> future = new CompletableFuture<>();
                future.complete(executionState);
                return new SubmitResult("queryid", SubmitResult.SubmitStatus.SUBMITTED, future);
            }
        };
    }

    private void mockPollError(int errorCount) {
        // poll error
        MockUp<HdfsUtil> mockHdfs = new MockUp<HdfsUtil>() {
            private int count = 0;

            @Mock
            public List<FileStatus> listFileMeta(String path, BrokerDesc brokerDesc) throws UserException {
                count++;
                if (count <= errorCount) {
                    throw new UserException("network connection error");
                } else {
                    List<FileStatus> res = new ArrayList<>();
                    res.add(new FileStatus(1024, false, 1, 1, 1, new Path("file1")));
                    return res;
                }
            }
        };
    }

    public static void mockRepoExecutorDML() {
        new MockUp<RepoExecutor>() {
            @Mock
            public void executeDML(String sql) {
            }

            @Mock
            public List<TResultBatch> executeDQL(String sql) {
                return Lists.newArrayList();
            }

            @Mock
            public void executeDDL(String sql) {
            }
        };
    }

    private void mockRepoExecutor() {
        new MockUp<RepoExecutor>() {
            @Mock
            public void executeDML(String sql) {
            }

            @Mock
            public List<TResultBatch> executeDQL(String sql) {
                return Lists.newArrayList();
            }

            @Mock
            public void executeDDL(String sql) {
            }
        };

        new MockUp<FileListTableRepo>() {
            private List<PipeFileRecord> records = new ArrayList<>();

            @Mock
            public void updateFileState(List<PipeFileRecord> files, FileListRepo.PipeFileState state, String label) {
                for (PipeFileRecord file : files) {
                    PipeFileRecord record = records.stream().filter(x -> x.equals(file)).findFirst().get();
                    record.loadState = state;
                    record.insertLabel = label;
                }
            }

            @Mock
            public List<PipeFileRecord> listFilesByState(FileListRepo.PipeFileState state, long limit) {
                return records.stream().filter(x -> x.getLoadState().equals(state)).collect(Collectors.toList());
            }

            @Mock
            public PipeFileRecord listFilesByPath(String path) {
                return records.stream().filter(x -> x.getFileName().equals(path)).findFirst().orElse(null);
            }

            @Mock
            public void stageFiles(List<PipeFileRecord> records) {
                this.records.addAll(records);
            }

        };
    }

    @Test
    public void pollPipe() throws Exception {
        final String pipeName = "p3";
        String sql = "create pipe p3 properties('poll_interval' = '1') as " +
                "insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createPipe(sql);

        Pipe p3 = getPipe(pipeName);
        Assert.assertEquals(0, p3.getLastPolledTime());
        p3.poll();

        Thread.sleep(1000);
        p3.poll();
        long timePoint = System.currentTimeMillis() / 1000;
        long diff = timePoint - p3.getLastPolledTime();
        Assert.assertTrue("Time diff: " + diff + " should less than 10 seconds", diff >= 0 && diff <= 10);

        p3.poll();
        diff = p3.getLastPolledTime() - timePoint;
        Assert.assertTrue("Time diff: " + diff + " should less than 10 seconds", diff >= 0 && diff <= 10);
    }

    @Test
    public void executePipe() throws Exception {
        mockRepoExecutor();
        String sql = "create pipe p3 as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        CreatePipeStmt createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        pm.createPipe(createStmt);

        mockTaskExecution(Constants.TaskRunState.SUCCESS);
        Pipe p1 = pm.mayGetPipe(new PipeName(PIPE_TEST_DB, "p3")).get();
        p1.poll();
        p1.schedule();
        p1.schedule();
        FilePipeSource source = (FilePipeSource) p1.getPipeSource();

        FileListRepo repo = source.getFileListRepo();
        Assert.assertEquals(1, repo.listFilesByState(FileListRepo.PipeFileState.FINISHED, 0).size());
    }

    @Test
    @Ignore("flaky test")
    public void testExecuteTaskSubmitFailed() throws Exception {
        mockRepoExecutor();
        final String pipeName = "p3";
        String sql = "create pipe p3 as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createPipe(sql);

        // poll error
        mockPollError(1);

        Pipe p3 = getPipe(pipeName);
        p3.poll();
        Assert.assertEquals(Pipe.State.ERROR, p3.getState());

        // clear the error and resume the pipe
        resumePipe(pipeName);
        p3.setLastPolledTime(0);
        Assert.assertEquals(Pipe.State.RUNNING, p3.getState());
        p3.poll();
        p3.schedule();
        Assert.assertEquals(Pipe.State.RUNNING, p3.getState());
        Assert.assertEquals(1, p3.getRunningTasks().size());

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        new mockit.Expectations(taskManager) {
            {
                // submit error
                taskManager.executeTaskAsync((Task) any, (ExecuteOption) any);
                result = new SubmitResult("queryid", SubmitResult.SubmitStatus.FAILED);

            }
        };

        Thread.sleep(1000);
        Assert.assertEquals(1, p3.getRunningTasks().size());
        // retry several times, until failed
        for (int i = 0; i < Pipe.FAILED_TASK_THRESHOLD; i++) {
            p3.schedule();
            Assert.assertEquals(Pipe.State.RUNNING, p3.getState());
            Assert.assertEquals(1, p3.getRunningTasks().size());
            Assert.assertTrue(String.format("iteration %d: %s", i, p3.getRunningTasks()),
                    p3.getRunningTasks().stream().allMatch(PipeTaskDesc::isError));

            p3.schedule();
            Assert.assertEquals(Pipe.State.RUNNING, p3.getState());
            Assert.assertTrue(String.format("iteration %d: %s", i, p3.getRunningTasks()),
                    p3.getRunningTasks().stream().allMatch(PipeTaskDesc::isRunnable));
        }
        p3.schedule();
        Assert.assertEquals(Pipe.FAILED_TASK_THRESHOLD + 1, p3.getFailedTaskExecutionCount());
        Assert.assertEquals(Pipe.State.ERROR, p3.getState());

        // retry all
        {
            AlterPipeStmt alter = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser("alter pipe p3 retry all", ctx);
            p3.retry((AlterPipeClauseRetry) alter.getAlterPipeClause());
            List<PipeFileRecord> unloadedFiles =
                    p3.getPipeSource().getFileListRepo().listFilesByState(FileListRepo.PipeFileState.UNLOADED, 0);
            Assert.assertEquals(1, unloadedFiles.size());
        }
    }

    private void pipeRetryFailedTask(Pipe p3, boolean retryAll) throws Exception {
        // retry several times, until failed
        for (int i = 0; i < Pipe.FAILED_TASK_THRESHOLD; i++) {
            // submit task, turn into running
            p3.schedule();
            Assert.assertEquals(String.format("iteration %d", i), Pipe.State.RUNNING, p3.getState());
            Assert.assertTrue(String.format("iteration %d: %s", i, p3.getRunningTasks()),
                    p3.getRunningTasks().stream().allMatch(PipeTaskDesc::isRunning));

            // task execution failed, turn into error
            p3.schedule();
            Assert.assertEquals(String.format("iteration %d", i), Pipe.State.RUNNING, p3.getState());
            Assert.assertTrue(String.format("iteration %d: %s", i, p3.getRunningTasks()),
                    p3.getRunningTasks().stream().allMatch(PipeTaskDesc::isError));

            // cleanup error state, and turn into runnable
            p3.schedule();
            Assert.assertEquals(String.format("iteration %d", i), Pipe.State.RUNNING, p3.getState());
            Assert.assertTrue(String.format("iteration %d: %s", i, p3.getRunningTasks()),
                    p3.getRunningTasks().stream().allMatch(PipeTaskDesc::isRunnable));
        }
        p3.schedule();
        p3.schedule();
        Assert.assertEquals(Pipe.FAILED_TASK_THRESHOLD + 1, p3.getFailedTaskExecutionCount());
        Assert.assertEquals(Pipe.State.ERROR, p3.getState());

        // retry all
        if (retryAll) {
            String sql = String.format("alter pipe %s retry all", p3.getName());
            AlterPipeStmt alter = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            p3.retry((AlterPipeClauseRetry) alter.getAlterPipeClause());
            List<PipeFileRecord> unloadedFiles =
                    p3.getPipeSource().getFileListRepo().listFilesByState(FileListRepo.PipeFileState.UNLOADED, 0);
            Assert.assertEquals(1, unloadedFiles.size());
            Assert.assertEquals(Pipe.State.RUNNING, p3.getState());
        } else {
            List<PipeFileRecord> errorFiles =
                    p3.getPipeSource().getFileListRepo().listFilesByState(FileListRepo.PipeFileState.ERROR, 0);
            for (PipeFileRecord file : errorFiles) {
                String sql =
                        String.format("alter pipe %s retry file %s", p3.getName(), Strings.quote(file.getFileName()));
                AlterPipeStmt alter = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
                p3.retry((AlterPipeClauseRetry) alter.getAlterPipeClause());
            }
            Assert.assertEquals(Pipe.State.RUNNING, p3.getState());
            List<PipeFileRecord> unloadedFiles =
                    p3.getPipeSource().getFileListRepo().listFilesByState(FileListRepo.PipeFileState.UNLOADED, 0);
            Assert.assertEquals(1, unloadedFiles.size());
        }
    }

    private Pipe preparePipe(String pipeName) throws Exception {
        String sql = String.format("create pipe %s as insert into tbl1 " +
                "select * from files('path'='fake://pipe', 'format'='parquet')", pipeName);
        createPipe(sql);

        // poll the pipe to generate tasks
        Pipe p3 = getPipe(pipeName);
        p3.poll();
        p3.setLastPolledTime(0);
        Assert.assertEquals(Pipe.State.RUNNING, p3.getState());
        return p3;
    }

    @Test
    public void testExecuteFailed() throws Exception {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        mockRepoExecutor();

        // mock execution failed
        for (boolean retryAll : Lists.newArrayList(true, false)) {
            final String pipeName = "p3";
            Pipe p3 = preparePipe(pipeName);
            new mockit.Expectations(taskManager) {
                {
                    taskManager.executeTaskAsync((Task) any, (ExecuteOption) any);
                    SubmitResult submit = new SubmitResult("queryid", SubmitResult.SubmitStatus.SUBMITTED);
                    FutureTask<Constants.TaskRunState> future = new FutureTask<>(() -> Constants.TaskRunState.FAILED);
                    submit.setFuture(future);
                    future.run();
                    result = submit;
                }
            };
            Assert.assertEquals(0, p3.getRunningTasks().size());
            pipeRetryFailedTask(p3, retryAll);
            dropPipe(pipeName);
        }

        // mock execution cancelled
        for (boolean retryAll : Lists.newArrayList(true, false)) {
            final String pipeName = "p4";
            Pipe p4 = preparePipe(pipeName);
            new mockit.Expectations(taskManager) {
                {
                    taskManager.executeTaskAsync((Task) any, (ExecuteOption) any);
                    SubmitResult submit = new SubmitResult("queryid", SubmitResult.SubmitStatus.SUBMITTED);
                    FutureTask<Constants.TaskRunState> future = new FutureTask<>(() -> Constants.TaskRunState.FAILED);
                    submit.setFuture(future);
                    future.cancel(true);
                    result = submit;
                }
            };
            Assert.assertEquals(0, p4.getRunningTasks().size());
            pipeRetryFailedTask(p4, retryAll);
            dropPipe(pipeName);
        }
    }

    @Test
    public void testTaskExecution() {
        PipeTaskDesc task = new PipeTaskDesc(1, "task", "test", "sql", null);

        // normal success
        {
            CompletableFuture<Constants.TaskRunState> future = new CompletableFuture<>();
            future.complete(Constants.TaskRunState.SUCCESS);
            task.setFuture(future);
            Assert.assertFalse(task.isFinished());
            Assert.assertFalse(task.isTaskRunning());
        }

        // exceptional
        {
            CompletableFuture<Constants.TaskRunState> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("task failure"));
            task.setFuture(future);
            Assert.assertFalse(task.isFinished());
            Assert.assertFalse(task.isTaskRunning());
        }

        // running
        {
            CompletableFuture<Constants.TaskRunState> future = new CompletableFuture<>();
            task.setFuture(future);
            Assert.assertFalse(task.isFinished());
            Assert.assertTrue(task.isTaskRunning());
        }
    }

    @Test
    public void resumeAfterError() throws Exception {
        final String pipeName = "p3";
        String sql = "create pipe p3 as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createPipe(sql);

        mockPollError(1);
        Pipe p3 = getPipe(pipeName);
        p3.poll();

        // get error
        Assert.assertEquals(Pipe.State.ERROR, p3.getState());

        // resume after error
        resumePipe(pipeName);
        Assert.assertEquals(Pipe.State.RUNNING, p3.getState());
        Assert.assertEquals(0, p3.getFailedTaskExecutionCount());
    }

    /**
     * The suspend operation could either interrupt the normal execution of task, or
     */
    @Test
    public void testSuspend() throws Exception {
        mockRepoExecutor();
        final String name = "p_suspend";
        String sql = "create pipe p_suspend " +
                "properties('auto_ingest'='false') " +
                "as " +
                "insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createPipe(sql);

        // normal execution of task, will retry after interruption
        mockTaskLongRunning(10, Constants.TaskRunState.SUCCESS);
        Pipe p = getPipe(name);
        p.poll();
        p.schedule();

        // suspend make the pipe-task enter RUNNABLE state
        suspendPipe(name);
        Assert.assertEquals(1, p.getRunningTasks().size());
        Assert.assertEquals(PipeTaskDesc.PipeTaskState.RUNNABLE, p.getRunningTasks().get(0).getState());

        // Throw the LabelAlreadyUsed exception
        // But Pipe could finish since this exception is acceptable
        mockTaskExecutor(() -> {
            throw new RuntimeException(new LabelAlreadyUsedException("h"));
        });
        resumePipe(name);
        waitPipeTaskFinish(name);

        dropPipe(name);
    }

    @Test
    public void executeAutoIngest() throws Exception {
        mockRepoExecutor();
        mockTaskExecution(Constants.TaskRunState.SUCCESS);
        // auto_ingest=false
        String pipeP3 = "p3";
        String p3Sql = "create pipe p3 properties('auto_ingest'='false') as " +
                "insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createPipe(p3Sql);
        Pipe pipe = getPipe(pipeP3);
        Assert.assertEquals(Pipe.State.RUNNING, pipe.getState());
        pipe.poll();
        pipe.schedule();
        pipe.schedule();
        pipe.poll();
        // schedule task
        pipe.schedule();
        // finalize task
        pipe.schedule();
        // trigger eos
        pipe.schedule();
        Assert.assertTrue(pipe.getPipeSource().eos());
        Assert.assertEquals(Pipe.State.FINISHED, pipe.getState());

        // auto_ingest=true
        String pipeP4 = "p4";
        String p4Sql = "create pipe p4 properties('auto_ingest'='true') as " +
                "insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createPipe(p4Sql);
        pipe = getPipe(pipeP4);
        Assert.assertEquals(Pipe.State.RUNNING, pipe.getState());
        pipe.poll();
        pipe.schedule();
        pipe.poll();
        pipe.schedule();
        Assert.assertFalse(pipe.getPipeSource().eos());
        Assert.assertEquals(Pipe.State.RUNNING, pipe.getState());
    }

    @Test
    public void pipeCRUD() throws Exception {
        mockRepoExecutor();

        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        pm.clear();
        PipeName name = new PipeName(PIPE_TEST_DB, "p_crud");

        // create
        String sql =
                "create pipe p_crud as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        CreatePipeStmt createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.createPipe(createStmt);

        Pipe pipe = pm.mayGetPipe(name).get();
        Assert.assertEquals(Pipe.State.RUNNING, pipe.getState());

        // create if not exists
        CreatePipeStmt createAgain = createStmt;
        Assert.assertThrows(SemanticException.class, () -> pm.createPipe(createAgain));
        sql = "create pipe if not exists p_crud as insert into tbl1 " +
                "select * from files('path'='fake://pipe', 'format'='parquet')";
        createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.createPipe(createStmt);

        // create or replace
        String createOrReplaceSql = "create or replace pipe p_crud as insert into tbl1 " +
                "select * from files('path'='fake://pipe', 'format'='parquet')";
        CreatePipeStmt createOrReplace = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(createOrReplaceSql, ctx);
        long previousId = getPipe("p_crud").getId();
        pm.createPipe(createOrReplace);
        Assert.assertNotEquals(previousId, getPipe("p_crud").getId());
        pipe = pm.mayGetPipe(name).get();

        // create or replace when not exists
        previousId = pipe.getId();
        dropPipe(name.getPipeName());
        pm.createPipe(createOrReplace);
        pipe = pm.mayGetPipe(name).get();
        Assert.assertNotEquals(previousId, pipe.getId());

        // pause
        sql = "alter pipe p_crud suspend";
        AlterPipeStmt pauseStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.alterPipe(pauseStmt);
        pm.alterPipe(pauseStmt);
        pm.alterPipe(pauseStmt);
        Assert.assertEquals(Pipe.State.SUSPEND, pipe.getState());

        // resume
        sql = "alter pipe p_crud resume";
        AlterPipeStmt resumeStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.alterPipe(resumeStmt);
        pm.alterPipe(resumeStmt);
        pm.alterPipe(resumeStmt);
        Assert.assertEquals(Pipe.State.RUNNING, pipe.getState());

        // alter property
        sql = "alter pipe p_crud set ('auto_ingest'='false', 'BATCH_SIZE'='10GB') ";
        AlterPipeStmt alterStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.alterPipe(alterStmt);
        pipe = getPipe("p_crud");
        Assert.assertEquals("{\"auto_ingest\":\"false\",\"BATCH_SIZE\":\"10GB\"}", pipe.getPropertiesJson());

        // drop
        sql = "drop pipe p_crud";
        DropPipeStmt dropStmt = (DropPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.dropPipe(dropStmt);
        Assert.assertFalse(pm.mayGetPipe(name).isPresent());

        // drop not existed
        DropPipeStmt finalDropStmt = dropStmt;
        Assert.assertThrows(SemanticException.class, () -> pm.dropPipe(finalDropStmt));
        sql = "drop pipe if exists p_crud";
        dropStmt = (DropPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.dropPipe(dropStmt);

        // drop database
        sql = "create pipe p_crud as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.createPipe(createStmt);
        sql = "create pipe p_crud1 as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.createPipe(createStmt);
        long dbId = ctx.getGlobalStateMgr().getDb(PIPE_TEST_DB).getId();
        pm.dropPipesOfDb(PIPE_TEST_DB, dbId);
        Assert.assertEquals(0, pm.getPipesUnlock().size());
    }

    @Test
    public void showPipes() throws Exception {
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();

        String createSql =
                "create pipe show_1 as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        CreatePipeStmt createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        pm.createPipe(createStmt);

        createSql =
                "create pipe show_2 as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        pm.createPipe(createStmt);

        // show
        String sql = "show pipes";
        ShowPipeStmt showPipeStmt = (ShowPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowExecutor showExecutor = new ShowExecutor(ctx, showPipeStmt);
        ShowResultSet result = showExecutor.execute();
        Assert.assertEquals(
                Arrays.asList("show_1", "RUNNING", "pipe_test_db.tbl1",
                        "{\"loadedFiles\":0,\"loadedBytes\":0,\"loadingFiles\":0}", null),
                result.getResultRows().get(0).subList(2, result.numColumns() - 1));
        Assert.assertEquals(
                Arrays.asList("show_2", "RUNNING", "pipe_test_db.tbl1",
                        "{\"loadedFiles\":0,\"loadedBytes\":0,\"loadingFiles\":0}", null),
                result.getResultRows().get(1).subList(2, result.numColumns() - 1));

        // desc
        sql = "desc pipe show_1";
        DescPipeStmt descPipeStmt = (DescPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        showExecutor = new ShowExecutor(ctx, descPipeStmt);
        result = showExecutor.execute();
        Assert.assertEquals(
                Arrays.asList("show_1", "FILE", "pipe_test_db.tbl1", "FILE_SOURCE(path=fake://pipe)",
                        "insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')", ""),
                result.getResultRows().get(0).subList(2, result.numColumns())
        );
    }

    @Test
    public void testListPipes() throws Exception {
        mockRepoExecutor();
        ExecuteEnv env = Mockito.mock(ExecuteEnv.class);
        FrontendServiceImpl impl = new FrontendServiceImpl(env);
        TListPipesParams params = new TListPipesParams();

        // without identity
        Assert.assertThrows(TException.class, () -> impl.listPipes(params));
        TUserIdentity identity = new TUserIdentity();
        identity.setUsername("root");
        params.setUser_ident(identity);

        // normal
        PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
        String createSql =
                "create pipe list_p1 as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        CreatePipeStmt createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        pm.createPipe(createStmt);

        Assert.assertFalse(impl.listPipes(params).pipes.isEmpty());

        String dropSql = "drop pipe list_p1";
        DropPipeStmt dropPipeStmt = (DropPipeStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);
        pm.dropPipe(dropPipeStmt);
    }

    @Test
    public void testListPipeFiles() throws Exception {
        ExecuteEnv env = Mockito.mock(ExecuteEnv.class);
        FrontendServiceImpl impl = new FrontendServiceImpl(env);

        PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
        String createSql =
                "create pipe list_p2 as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        CreatePipeStmt createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        pm.createPipe(createStmt);

        List<PipeFileRecord> records = Arrays.asList(
                new PipeFileRecord(1, "file1", "version1", 1024),
                new PipeFileRecord(1, "file2", "version1", 1024),
                new PipeFileRecord(1, "file3", "version1", 1024)
        );
        new Expectations(RepoAccessor.getInstance()) {
            {
                RepoAccessor.getInstance().listAllFiles();
                result = records;
            }
        };

        TListPipeFilesParams params = new TListPipeFilesParams();

        // without identify
        Assert.assertThrows(TException.class, () -> impl.listPipeFiles(params));

        // normal
        TUserIdentity identity = new TUserIdentity();
        identity.setUsername("root");
        params.setUser_ident(identity);
        TListPipeFilesResult result = impl.listPipeFiles(params);
        Assert.assertFalse(result.pipe_files.isEmpty());
    }

    @Test
    public void testProperty() throws Exception {
        createPipe("create pipe p_batch_size properties('batch_size'='10GB') " +
                " as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')");
        createPipe("create pipe p_batch_files properties('batch_files'='100') " +
                " as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')");
        createPipe("create pipe p_poll_interval properties('poll_interval'='100') " +
                " as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')");
        createPipe("create pipe p_auto_ingest properties('auto_ingest'='false') " +
                " as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')");
    }

    @Test
    public void testTaskProperties() throws Exception {
        mockRepoExecutor();
        String pipeName = "p_task_properties";
        createPipe("create pipe p_task_properties properties('task.query_timeout'='20') " +
                " as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')");
        Pipe pipe = getPipe(pipeName);
        Assert.assertEquals("{\"task.query_timeout\":\"20\"}", pipe.getPropertiesJson());
        Assert.assertEquals(ImmutableMap.of("query_timeout", "20"), pipe.getTaskProperties());
        dropPipe(pipeName);

        // default task execution variables
        createPipe("create pipe p_task_properties " +
                " as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')");
        pipe = getPipe(pipeName);
        Assert.assertEquals(ImmutableMap.of("query_timeout", "3600"), pipe.getTaskProperties());
    }

    @Test
    public void testInsertSql() throws Exception {
        mockRepoExecutor();
        String pipeName = "p_insert_sql";

        // select *
        {
            createPipe("create pipe p_insert_sql properties('batch_size'='10GB') " +
                    " as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')");
            Pipe pipe = getPipe(pipeName);
            FilePipePiece piece = new FilePipePiece();
            piece.addFile(new PipeFileRecord(pipe.getId(), "a.parquet", "v1", 1));
            piece.addFile(new PipeFileRecord(pipe.getId(), "b.parquet", "v1", 1));
            String sql = FilePipeSource.buildInsertSql(pipe, piece, "insert_label");
            Assert.assertEquals("INSERT INTO `tbl1` WITH LABEL `insert_label` SELECT *\n" +
                    "FROM FILES('format'='parquet','path'='a.parquet,b.parquet')", sql);
            dropPipe(pipeName);
        }

        // select col
        {
            createPipe("create pipe p_insert_sql properties('batch_size'='10GB') " +
                    " as insert into tbl1 select col_int, col_string from files('path'='fake://pipe', 'format'='parquet')");
            Pipe pipe = getPipe(pipeName);
            FilePipePiece piece = new FilePipePiece();
            piece.addFile(new PipeFileRecord(pipe.getId(), "a.parquet", "v1", 1));
            piece.addFile(new PipeFileRecord(pipe.getId(), "b.parquet", "v1", 1));
            String sql = FilePipeSource.buildInsertSql(pipe, piece, "insert_label");
            Assert.assertEquals("INSERT INTO `tbl1` WITH LABEL `insert_label` SELECT `col_int`, `col_string`\n" +
                    "FROM FILES('format'='parquet','path'='a.parquet,b.parquet')", sql);
            dropPipe(pipeName);
        }

        // specify target columns
        {
            createPipe("create pipe p_insert_sql properties('batch_size'='10GB') " +
                    " as insert into tbl1 (col_int) select col_int from files('path'='fake://pipe', 'format'='parquet')");
            Pipe pipe = getPipe(pipeName);
            FilePipePiece piece = new FilePipePiece();
            piece.addFile(new PipeFileRecord(pipe.getId(), "a.parquet", "v1", 1));
            piece.addFile(new PipeFileRecord(pipe.getId(), "b.parquet", "v1", 1));
            String sql = FilePipeSource.buildInsertSql(pipe, piece, "insert_label");
            Assert.assertEquals("INSERT INTO `tbl1` " +
                    "WITH LABEL `insert_label` " +
                    "(`col_int`) SELECT `col_int`\n" +
                    "FROM FILES('format'='parquet','path'='a.parquet,b.parquet')", sql);
            SqlParser.parse(sql, new SessionVariable());
            dropPipe(pipeName);
        }
    }

    @Test
    public void testRecovery() throws Exception {
        mockRepoExecutor();
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
        pm.clear();

        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        long dbId = ctx.getGlobalStateMgr().getDb(PIPE_TEST_DB).getId();
        pm.dropPipesOfDb(PIPE_TEST_DB, dbId);

        // create pipe 1
        String sql =
                "create pipe p_crash as insert into tbl select * from files('path'='fake://pipe', 'format'='parquet')";
        createPipe(sql);
        UtFrameUtils.PseudoImage image1 = new UtFrameUtils.PseudoImage();
        pm.getRepo().saveImage(image1.getDataOutputStream(), 123);

        // loading file and crash
        String name = "p_crash";
        Pipe pipe = getPipe(name);
        pipe.poll();
        pipe.schedule();
        Assert.assertEquals(1, pipe.getRunningTasks().size());
        Assert.assertTrue(StringUtils.isNotEmpty(pipe.getRunningTasks().get(0).getUniqueTaskName()));

        // recover when transaction failed
        {
            PipeManager pm1 = new PipeManager();
            FileListRepo repo = pipe.getPipeSource().getFileListRepo();
            pm1.getRepo().loadImage(image1.getDataInputStream(), 123);
            Assert.assertEquals(pm.getPipesUnlock(), pm1.getPipesUnlock());
            pipe = pm1.mayGetPipe(new PipeName(PIPE_TEST_DB, name)).get();
            Assert.assertFalse(pipe.isRecovered());
            Assert.assertFalse(pipe.isRunnable());

            pipe.recovery();
            Assert.assertEquals(1, repo.listFilesByState(FileListRepo.PipeFileState.ERROR, 0).size());
            Assert.assertTrue(pipe.isRecovered());
            Assert.assertTrue(pipe.isRunnable());
        }

        // recover when transaction committed
        {
            FileListRepo repo = pipe.getPipeSource().getFileListRepo();
            repo.updateFileState(repo.listFilesByState(FileListRepo.PipeFileState.ERROR, 0),
                    FileListRepo.PipeFileState.LOADING, "insert-label");
            new MockUp<GlobalTransactionMgr>() {
                @Mock
                public TransactionStatus getLabelStatus(long dbId, String label) {
                    return TransactionStatus.COMMITTED;
                }
            };

            PipeManager pm1 = new PipeManager();
            pm1.getRepo().loadImage(image1.getDataInputStream(), 123);
            Assert.assertEquals(pm.getPipesUnlock(), pm1.getPipesUnlock());
            pipe = pm1.mayGetPipe(new PipeName(PIPE_TEST_DB, name)).get();
            Assert.assertFalse(pipe.isRecovered());
            Assert.assertFalse(pipe.isRunnable());

            pipe.recovery();
            Assert.assertEquals(1, repo.listFilesByState(FileListRepo.PipeFileState.FINISHED, 0).size());
            Assert.assertTrue(pipe.isRecovered());
            Assert.assertTrue(pipe.isRunnable());
        }
    }

    @Test
    public void testInspectPipes() throws Exception {
        ConnectContext newCtx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        newCtx.setDatabase(PIPE_TEST_DB);
        newCtx.setThreadLocalInfo();
        createPipe("create pipe p_inspect as insert into tbl " +
                "select * from files('path'='fake://pipe', 'format'='parquet')");

        String sql = "select inspect_all_pipes()";
        String plan = UtFrameUtils.getFragmentPlan(newCtx, sql);
        Assert.assertTrue(plan.contains("p_inspect"));
    }
}