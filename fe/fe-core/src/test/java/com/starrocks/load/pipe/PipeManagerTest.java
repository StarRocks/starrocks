// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.pipe;

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.UserException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.SubmitResult;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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

    @Test
    public void persistPipe() throws Exception {
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();

        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();

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
        sql = "alter pipe p1 pause";
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
            public void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
                    throws UserException {
                count++;
                if (count <= errorCount) {
                    throw new UserException("network connection error");
                } else {
                    fileStatuses.add(new TBrokerFileStatus("file1", false, 1024, false));
                }
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
        long current = System.currentTimeMillis() / 1000;
        Assert.assertEquals(current, p3.getLastPolledTime());
        p3.poll();
        Assert.assertEquals(current, p3.getLastPolledTime());
    }

    @Test
    public void executePipe() throws Exception {
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
        /*
        FileListRepoInMemory repo = source.getFileListRepo();
        Assert.assertEquals(1, repo.size());
        List<PipeFile> files = repo.listFiles();
        Assert.assertEquals(
                Lists.newArrayList(new PipeFile("file1", 1024, FileListRepo.PipeFileState.LOADED)),
                files);
         */
    }

    @Ignore("flaky")
    @Test
    public void executeError() throws Exception {
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

        // execute error
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        new mockit.Expectations(taskManager) {
            {
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

    @Test
    public void executeAutoIngest() throws Exception {
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
        pipe.schedule();
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
        PipeManager pm = ctx.getGlobalStateMgr().getPipeManager();
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
        sql =
                "create pipe if not exists p_crud as insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')";
        createStmt = (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        pm.createPipe(createStmt);

        // pause
        sql = "alter pipe p_crud pause";
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
                Arrays.asList("show_1", "pipe_test_db.tbl1", "RUNNING", "0", "0", "0"),
                result.getResultRows().get(0).subList(2, result.numColumns()));
        Assert.assertEquals(
                Arrays.asList("show_2", "pipe_test_db.tbl1", "RUNNING", "0", "0", "0"),
                result.getResultRows().get(1).subList(2, result.numColumns()));

        // desc
        sql = "desc pipe show_1";
        DescPipeStmt descPipeStmt = (DescPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        showExecutor = new ShowExecutor(ctx, descPipeStmt);
        result = showExecutor.execute();
        Assert.assertEquals(
                Arrays.asList("show_1", "FILE", "pipe_test_db.tbl1", "FILE_SOURCE(path=fake://pipe)",
                        "insert into tbl1 select * from files('path'='fake://pipe', 'format'='parquet')"),
                result.getResultRows().get(0).subList(2, result.numColumns())
        );
    }

}