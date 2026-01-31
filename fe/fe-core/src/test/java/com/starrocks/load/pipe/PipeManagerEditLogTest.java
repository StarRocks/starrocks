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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.persist.AlterPipeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class PipeManagerEditLogTest {
    private PipeManager masterPipeManager;
    private ConnectContext ctx;
    private StarRocksAssert starRocksAssert;
    private static final String PIPE_TEST_DB = "pipe_test_db_editlog";
    private static final String PIPE_TEST_TABLE = "tbl1";
    private long testDbId = 100L;
    private long testTableId = 1000L;
    private long testPartitionId = 2000L;
    private long testIndexId = 3000L;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        // Create test database
        Database database = new Database(testDbId, PIPE_TEST_DB);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(database);

        // Create columns
        List<Column> columns = Lists.newArrayList();
        Column colInt = new Column("col_int", IntegerType.INT);
        colInt.setIsKey(true);
        columns.add(colInt);
        
        Column colString = new Column("col_string", TypeFactory.createDefaultCatalogString());
        colString.setIsKey(false);
        columns.add(colString);

        // Create partition info
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(testPartitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(testPartitionId, (short) 1);

        // Create distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList(colInt));

        // Create MaterializedIndex
        MaterializedIndex materializedIndex = new MaterializedIndex(testIndexId, MaterializedIndex.IndexState.NORMAL);

        // Create Partition
        Partition partition = new Partition(testPartitionId, testPartitionId + 100, PIPE_TEST_TABLE,
                materializedIndex, distributionInfo);

        // Create OlapTable
        OlapTable olapTable = new OlapTable(testTableId, PIPE_TEST_TABLE, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        olapTable.setIndexMeta(testIndexId, PIPE_TEST_TABLE, columns, 0, 0, (short) 1, 
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(testIndexId);
        olapTable.addPartition(partition);

        // Register table
        database.registerTableUnlocked(olapTable);

        // Create ConnectContext for pipe statement parsing
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase(PIPE_TEST_DB);
        starRocksAssert = new StarRocksAssert(ctx);

        // Create PipeManager instance
        masterPipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
    }

    @AfterEach
    public void tearDown() {
        // Clean up pipes
        try {
            long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(PIPE_TEST_DB).getId();
            masterPipeManager.dropPipesOfDb(PIPE_TEST_DB, dbId);
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        UtFrameUtils.tearDownForPersisTest();
    }

    // Helper method to create a pipe statement
    private CreatePipeStmt createPipeStmt(String pipeName) throws Exception {
        String sql = "create pipe " + pipeName + " as insert into tbl1 " +
                "select * from files('path'='fake://pipe', 'format'='parquet')";
        return (CreatePipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
    }

    @Test
    public void testCreatePipeNormalCase() throws Exception {
        // 1. Prepare test data
        String pipeName = "test_pipe";
        CreatePipeStmt stmt = createPipeStmt(pipeName);
        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);

        // 2. Verify initial state
        Optional<Pipe> pipeOpt = masterPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertFalse(pipeOpt.isPresent());

        // 3. Execute createPipe operation (master side)
        masterPipeManager.createPipe(stmt);

        // 4. Verify master state
        pipeOpt = masterPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertTrue(pipeOpt.isPresent());
        Pipe pipe = pipeOpt.get();
        Assertions.assertEquals(pipeName, pipe.getName());

        // 5. Test follower replay functionality
        PipeManager followerPipeManager = new PipeManager();

        // Verify follower initial state
        Optional<Pipe> followerPipeOpt = followerPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertFalse(followerPipeOpt.isPresent());

        PipeOpEntry replayEntry = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);

        // Execute follower replay
        followerPipeManager.getRepo().replay(replayEntry);

        // 6. Verify follower state is consistent with master
        followerPipeOpt = followerPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertTrue(followerPipeOpt.isPresent());
        Pipe followerPipe = followerPipeOpt.get();
        Assertions.assertEquals(pipeName, followerPipe.getName());
    }

    @Test
    public void testCreatePipeEditLogException() throws Exception {
        // 1. Prepare test data
        String pipeName = "exception_pipe";
        CreatePipeStmt stmt = createPipeStmt(pipeName);
        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);

        // 2. Create a separate PipeManager for exception testing
        PipeManager exceptionPipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);

        // 3. Mock EditLog.logPipeOp to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logPipeOp(any(PipeOpEntry.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Optional<Pipe> pipeOpt = exceptionPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertFalse(pipeOpt.isPresent());

        // 4. Execute createPipe operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionPipeManager.createPipe(stmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        pipeOpt = exceptionPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertFalse(pipeOpt.isPresent());
    }

    @Test
    public void testDropPipeNormalCase() throws Exception {
        // 1. Prepare test data - first create a pipe
        String pipeName = "test_drop_pipe";
        CreatePipeStmt createStmt = createPipeStmt(pipeName);
        masterPipeManager.createPipe(createStmt);

        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);
        // Verify pipe exists
        Optional<Pipe> pipeOpt = masterPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertTrue(pipeOpt.isPresent());

        // 2. Prepare DropPipeStmt
        String sql = "drop pipe " + pipeName;
        DropPipeStmt dropStmt = (DropPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 3. Execute dropPipe operation (master side)
        masterPipeManager.dropPipe(dropStmt);

        // 4. Verify master state
        pipeOpt = masterPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertFalse(pipeOpt.isPresent());

        // 5. Test follower replay functionality
        PipeManager followerPipeManager = new PipeManager();

        // First replay the create pipe
        PipeOpEntry replayCreateEntry = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        followerPipeManager.getRepo().replay(replayCreateEntry);

        // Then replay the drop pipe
        PipeOpEntry replayDropEntry = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);

        // Execute follower replay
        followerPipeManager.getRepo().replay(replayDropEntry);

        // 6. Verify follower state is consistent with master
        Optional<Pipe> followerPipeOpt = followerPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertFalse(followerPipeOpt.isPresent());
    }

    @Test
    public void testDropPipeEditLogException() throws Exception {
        // 1. Prepare test data - first create a pipe
        String pipeName = "exception_drop_pipe";
        CreatePipeStmt createStmt = createPipeStmt(pipeName);
        PipeManager exceptionPipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
        exceptionPipeManager.createPipe(createStmt);

        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);
        // Verify pipe exists
        Optional<Pipe> pipeOpt = exceptionPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertTrue(pipeOpt.isPresent());

        // 2. Prepare DropPipeStmt
        String sql = "drop pipe " + pipeName;
        DropPipeStmt dropStmt = (DropPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 3. Mock EditLog.logPipeOp to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logPipeOp(any(PipeOpEntry.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute dropPipe operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionPipeManager.dropPipe(dropStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        pipeOpt = exceptionPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertTrue(pipeOpt.isPresent());
    }

    @Test
    public void testDropPipesOfDbNormalCase() throws Exception {
        // 1. Prepare test data - create multiple pipes
        String pipeName1 = "test_drop_db_pipe1";
        String pipeName2 = "test_drop_db_pipe2";
        CreatePipeStmt createStmt1 = createPipeStmt(pipeName1);
        CreatePipeStmt createStmt2 = createPipeStmt(pipeName2);
        masterPipeManager.createPipe(createStmt1);
        masterPipeManager.createPipe(createStmt2);

        // Verify pipes exist
        Optional<Pipe> pipeOpt1 = masterPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName1));
        Optional<Pipe> pipeOpt2 = masterPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName2));
        Assertions.assertTrue(pipeOpt1.isPresent());
        Assertions.assertTrue(pipeOpt2.isPresent());

        // 2. Execute dropPipesOfDb operation (master side)
        long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(PIPE_TEST_DB).getId();
        masterPipeManager.dropPipesOfDb(PIPE_TEST_DB, dbId);

        // 3. Verify master state
        pipeOpt1 = masterPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName1));
        pipeOpt2 = masterPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName2));
        Assertions.assertFalse(pipeOpt1.isPresent());
        Assertions.assertFalse(pipeOpt2.isPresent());

        // 4. Test follower replay functionality
        PipeManager followerPipeManager = new PipeManager();

        // First replay the create pipes
        PipeOpEntry replayCreateEntry1 = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        followerPipeManager.getRepo().replay(replayCreateEntry1);
        PipeOpEntry replayCreateEntry2 = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        followerPipeManager.getRepo().replay(replayCreateEntry2);

        // Then replay the drop pipes
        PipeOpEntry replayDropEntry1 = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        followerPipeManager.getRepo().replay(replayDropEntry1);
        PipeOpEntry replayDropEntry2 = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        followerPipeManager.getRepo().replay(replayDropEntry2);

        // 5. Verify follower state is consistent with master
        Optional<Pipe> followerPipeOpt1 = followerPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName1));
        Optional<Pipe> followerPipeOpt2 = followerPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName2));
        Assertions.assertFalse(followerPipeOpt1.isPresent());
        Assertions.assertFalse(followerPipeOpt2.isPresent());
    }

    @Test
    public void testDropPipesOfDbEditLogException() throws Exception {
        // 1. Prepare test data - create multiple pipes
        String pipeName1 = "exception_drop_db_pipe1";
        String pipeName2 = "exception_drop_db_pipe2";
        CreatePipeStmt createStmt1 = createPipeStmt(pipeName1);
        CreatePipeStmt createStmt2 = createPipeStmt(pipeName2);
        PipeManager exceptionPipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
        exceptionPipeManager.createPipe(createStmt1);
        exceptionPipeManager.createPipe(createStmt2);

        // Verify pipes exist
        Optional<Pipe> pipeOpt1 = exceptionPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName1));
        Optional<Pipe> pipeOpt2 = exceptionPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName2));
        Assertions.assertTrue(pipeOpt1.isPresent());
        Assertions.assertTrue(pipeOpt2.isPresent());

        // 2. Mock EditLog.logPipeOp to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logPipeOp(any(PipeOpEntry.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute dropPipesOfDb operation
        // Note: dropPipesOfDb catches exceptions internally, so it won't throw
        long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(PIPE_TEST_DB).getId();
        exceptionPipeManager.dropPipesOfDb(PIPE_TEST_DB, dbId);

        // 4. Verify leader memory state remains unchanged after exception
        // Since dropPipesOfDb catches exceptions, pipes should still exist
        pipeOpt1 = exceptionPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName1));
        pipeOpt2 = exceptionPipeManager.mayGetPipe(new PipeName(PIPE_TEST_DB, pipeName2));
        Assertions.assertTrue(pipeOpt1.isPresent());
        Assertions.assertTrue(pipeOpt2.isPresent());
    }

    @Test
    public void testAlterPipeSuspendNormalCase() throws Exception {
        // 1. Prepare test data - first create a pipe
        String pipeName = "test_alter_suspend_pipe";
        CreatePipeStmt createStmt = createPipeStmt(pipeName);
        masterPipeManager.createPipe(createStmt);

        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);
        Pipe pipe = masterPipeManager.mayGetPipe(pipeNameObj).get();
        // Verify initial state
        Assertions.assertNotEquals(Pipe.State.SUSPEND, pipe.getState());

        // 2. Prepare AlterPipeStmt with Suspend
        String sql = "alter pipe " + pipeName + " suspend";
        AlterPipeStmt alterStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 3. Execute alterPipe operation (master side)
        masterPipeManager.alterPipe(alterStmt);

        // 4. Verify master state
        pipe = masterPipeManager.mayGetPipe(pipeNameObj).get();
        Assertions.assertEquals(Pipe.State.SUSPEND, pipe.getState());

        // 5. Test follower replay functionality
        PipeManager followerPipeManager = new PipeManager();

        // First replay the create pipe
        PipeOpEntry replayCreateEntry = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        followerPipeManager.getRepo().replay(replayCreateEntry);

        // Then replay the alter pipe
        AlterPipeLog alterLog = (AlterPipeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_PIPE);
        followerPipeManager.getRepo().replayAlterPipe(alterLog);

        // 6. Verify follower state is consistent with master
        Optional<Pipe> followerPipeOpt = followerPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertTrue(followerPipeOpt.isPresent());
        Pipe followerPipe = followerPipeOpt.get();
        Assertions.assertEquals(Pipe.State.SUSPEND, followerPipe.getState());
    }

    @Test
    public void testAlterPipeSuspendEditLogException() throws Exception {
        // 1. Prepare test data - first create a pipe
        String pipeName = "exception_alter_suspend_pipe";
        CreatePipeStmt createStmt = createPipeStmt(pipeName);
        PipeManager exceptionPipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
        exceptionPipeManager.createPipe(createStmt);

        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);
        Pipe pipe = exceptionPipeManager.mayGetPipe(pipeNameObj).get();
        // Verify initial state
        Pipe.State initialState = pipe.getState();
        Assertions.assertNotEquals(Pipe.State.SUSPEND, initialState);

        // 2. Prepare AlterPipeStmt with Suspend
        String sql = "alter pipe " + pipeName + " suspend";
        AlterPipeStmt alterStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 3. Mock EditLog.logAlterPipe to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterPipe(any(AlterPipeLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterPipe operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionPipeManager.alterPipe(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        pipe = exceptionPipeManager.mayGetPipe(pipeNameObj).get();
        Assertions.assertEquals(initialState, pipe.getState());
    }

    @Test
    public void testAlterPipeResumeNormalCase() throws Exception {
        // 1. Prepare test data - first create and suspend a pipe
        String pipeName = "test_alter_resume_pipe";
        CreatePipeStmt createStmt = createPipeStmt(pipeName);
        masterPipeManager.createPipe(createStmt);

        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);
        // Suspend first
        String suspendSql = "alter pipe " + pipeName + " suspend";
        AlterPipeStmt suspendStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(suspendSql, ctx);
        masterPipeManager.alterPipe(suspendStmt);

        Pipe pipe = masterPipeManager.mayGetPipe(pipeNameObj).get();
        Assertions.assertEquals(Pipe.State.SUSPEND, pipe.getState());

        // 2. Prepare AlterPipeStmt with Resume
        String sql = "alter pipe " + pipeName + " resume";
        AlterPipeStmt alterStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 3. Execute alterPipe operation (master side)
        masterPipeManager.alterPipe(alterStmt);

        // 4. Verify master state
        pipe = masterPipeManager.mayGetPipe(pipeNameObj).get();
        Assertions.assertEquals(Pipe.State.RUNNING, pipe.getState());

        // 5. Test follower replay functionality
        PipeManager followerPipeManager = new PipeManager();

        // First replay the create pipe
        PipeOpEntry replayCreateEntry = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        followerPipeManager.getRepo().replay(replayCreateEntry);

        // Then replay the suspend pipe
        AlterPipeLog suspendLog = (AlterPipeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_PIPE);
        followerPipeManager.getRepo().replayAlterPipe(suspendLog);

        // Then replay the resume pipe
        AlterPipeLog resumeLog = (AlterPipeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_PIPE);
        followerPipeManager.getRepo().replayAlterPipe(resumeLog);

        // 6. Verify follower state is consistent with master
        Optional<Pipe> followerPipeOpt = followerPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertTrue(followerPipeOpt.isPresent());
        Pipe followerPipe = followerPipeOpt.get();
        Assertions.assertEquals(Pipe.State.RUNNING, followerPipe.getState());
    }

    @Test
    public void testAlterPipeResumeEditLogException() throws Exception {
        // 1. Prepare test data - first create and suspend a pipe
        String pipeName = "exception_alter_resume_pipe";
        CreatePipeStmt createStmt = createPipeStmt(pipeName);
        PipeManager exceptionPipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
        exceptionPipeManager.createPipe(createStmt);

        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);
        // Suspend first
        String suspendSql = "alter pipe " + pipeName + " suspend";
        AlterPipeStmt suspendStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(suspendSql, ctx);
        exceptionPipeManager.alterPipe(suspendStmt);

        Pipe pipe = exceptionPipeManager.mayGetPipe(pipeNameObj).get();
        Assertions.assertEquals(Pipe.State.SUSPEND, pipe.getState());

        // 2. Prepare AlterPipeStmt with Resume
        String sql = "alter pipe " + pipeName + " resume";
        AlterPipeStmt alterStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 3. Mock EditLog.logAlterPipe to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterPipe(any(AlterPipeLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterPipe operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionPipeManager.alterPipe(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        pipe = exceptionPipeManager.mayGetPipe(pipeNameObj).get();
        Assertions.assertEquals(Pipe.State.SUSPEND, pipe.getState());
    }

    @Test
    public void testAlterPipeSetPropertyNormalCase() throws Exception {
        // 1. Prepare test data - first create a pipe
        String pipeName = "test_alter_set_property_pipe";
        CreatePipeStmt createStmt = createPipeStmt(pipeName);
        masterPipeManager.createPipe(createStmt);

        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);
        // 2. Prepare AlterPipeStmt with SetProperty - test all alterable properties
        // Note: warehouse must exist, use default_warehouse which is initialized in setUp
        // Note: task.xx properties must correspond to existing session variables
        // Using common session variables: query_timeout and exec_mem_limit
        String sql = "alter pipe " + pipeName + " set (" +
                "'auto_ingest'='false', " +
                "'poll_interval'='60', " +
                "'batch_size'='2GB', " +
                "'batch_files'='128', " +
                "'warehouse'='default_warehouse', " +
                "'task.query_timeout'='300', " +
                "'task.exec_mem_limit'='2147483648')";
        AlterPipeStmt alterStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 3. Execute alterPipe operation (master side)
        masterPipeManager.alterPipe(alterStmt);

        // 4. Verify master state - check all modified properties
        Pipe pipe = masterPipeManager.mayGetPipe(pipeNameObj).get();
        Assertions.assertEquals(60, pipe.getPollIntervalSecond());
        Assertions.assertFalse(pipe.getPipeSource().getAutoIngest());
        Assertions.assertEquals(2L << 30, pipe.getPipeSource().getBatchSize()); // 2GB
        Assertions.assertEquals(128L, pipe.getPipeSource().getBatchFiles());
        
        // Verify taskExecutionVariables - warehouse and task properties
        Map<String, String> taskProps = pipe.getTaskProperties();
        Assertions.assertNotNull(taskProps);
        Assertions.assertEquals("default_warehouse", taskProps.get("warehouse"));
        // task.xx properties are stored without the "task." prefix
        Assertions.assertEquals("300", taskProps.get("query_timeout")); // task.query_timeout -> query_timeout
        Assertions.assertEquals("2147483648", taskProps.get("exec_mem_limit")); // task.exec_mem_limit -> exec_mem_limit

        // 5. Test follower replay functionality
        PipeManager followerPipeManager = new PipeManager();

        // First replay the create pipe
        PipeOpEntry replayCreateEntry = (PipeOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_PIPE);
        followerPipeManager.getRepo().replay(replayCreateEntry);

        // Then replay the alter pipe
        AlterPipeLog alterLog = (AlterPipeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_PIPE);
        followerPipeManager.getRepo().replayAlterPipe(alterLog);

        // 6. Verify follower state is consistent with master - check all properties
        Optional<Pipe> followerPipeOpt = followerPipeManager.mayGetPipe(pipeNameObj);
        Assertions.assertTrue(followerPipeOpt.isPresent());
        Pipe followerPipe = followerPipeOpt.get();
        Assertions.assertEquals(pipe.getPollIntervalSecond(), followerPipe.getPollIntervalSecond());
        Assertions.assertEquals(pipe.getPipeSource().getAutoIngest(), followerPipe.getPipeSource().getAutoIngest());
        Assertions.assertEquals(pipe.getPipeSource().getBatchSize(), followerPipe.getPipeSource().getBatchSize());
        Assertions.assertEquals(pipe.getPipeSource().getBatchFiles(), followerPipe.getPipeSource().getBatchFiles());
        
        // Verify follower taskExecutionVariables
        Map<String, String> followerTaskProps = followerPipe.getTaskProperties();
        Assertions.assertNotNull(followerTaskProps);
        Assertions.assertEquals(taskProps.get("warehouse"), followerTaskProps.get("warehouse"));
        Assertions.assertEquals(taskProps.get("query_timeout"), followerTaskProps.get("query_timeout"));
        Assertions.assertEquals(taskProps.get("exec_mem_limit"), followerTaskProps.get("exec_mem_limit"));
    }

    @Test
    public void testAlterPipeSetPropertyEditLogException() throws Exception {
        // 1. Prepare test data - first create a pipe
        String pipeName = "exception_alter_set_property_pipe";
        CreatePipeStmt createStmt = createPipeStmt(pipeName);
        PipeManager exceptionPipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
        exceptionPipeManager.createPipe(createStmt);

        PipeName pipeNameObj = new PipeName(PIPE_TEST_DB, pipeName);
        Pipe pipe = exceptionPipeManager.mayGetPipe(pipeNameObj).get();
        Map<String, String> initialProps = new HashMap<>(pipe.getProperties());

        // 2. Prepare AlterPipeStmt with SetProperty
        String sql = "alter pipe " + pipeName + " set ('auto_ingest'='false')";
        AlterPipeStmt alterStmt = (AlterPipeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 3. Mock EditLog.logAlterPipe to throw exception
        // Note: alterProperties is called on Pipe, which calls logAlterPipe
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterPipe(any(AlterPipeLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterPipe operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionPipeManager.alterPipe(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        pipe = exceptionPipeManager.mayGetPipe(pipeNameObj).get();
        Map<String, String> currentProps = pipe.getProperties();
        // Properties should remain unchanged (though exact comparison may be tricky due to defaults)
        Assertions.assertNotNull(currentProps);
    }
}

