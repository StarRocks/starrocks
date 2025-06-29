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

package com.starrocks.transaction;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TUniqueId;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class ExplicitTxnTest {
    @BeforeClass
    public static void init() throws DdlException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MetricRepo.init();

        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(globalStateMgr);

        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        MockedLocalMetaStore localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        globalStateMgr.setLocalMetastore(localMetastore);

        MockedMetadataMgr mockedMetadataMgr = new MockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(mockedMetadataMgr);

        localMetastore.createDb("db1");
        String createTable = "create table db1.tbl1 (c1 bigint, c2 bigint, c3 bigint)";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) SqlParser.parseSingleStatement(createTable, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(createTableStmt, context);
        localMetastore.createTable(createTableStmt);

        localMetastore.createDb("db2");
        createTable = "create table db2.tbl1 (c1 bigint, c2 bigint, c3 bigint)";
        createTableStmt =
                (CreateTableStmt) SqlParser.parseSingleStatement(createTable, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(createTableStmt, context);
        localMetastore.createTable(createTableStmt);
    }

    @Test
    public void testNotSupportStmt() throws IOException, DdlException {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        context.setTxnId(1);

        //Init ConnectProcessor
        MetricRepo.init();
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(3);
        serializer.writeEofString("select * from a");
        ByteBuffer queryPacket = serializer.toByteBuffer();
        ByteBuffer finalQueryPacket1 = queryPacket;
        new MockUp<MysqlChannel>() {
            @Mock
            public ByteBuffer fetchOnePacket() throws IOException {
                return finalQueryPacket1;
            }

            @Mock
            public void sendAndFlush(ByteBuffer packet) throws IOException {
            }
        };

        ConnectProcessor processor = new ConnectProcessor(context);
        processor.processOnce();

        Assert.assertTrue(context.getState().isError());
        Assert.assertEquals(ErrorCode.ERR_EXPLICIT_TXN_NOT_SUPPORT_STMT, context.getState().getErrorCode());

        serializer.reset();
        serializer.writeInt1(3);
        serializer.writeEofString("insert overwrite t values(1,2,3,4)");
        queryPacket = serializer.toByteBuffer();
        ByteBuffer finalQueryPacket = queryPacket;
        new MockUp<MysqlChannel>() {
            @Mock
            public ByteBuffer fetchOnePacket() throws IOException {
                return finalQueryPacket;
            }

            @Mock
            public void sendAndFlush(ByteBuffer packet) throws IOException {
            }
        };

        processor = new ConnectProcessor(context);
        processor.processOnce();

        Assert.assertTrue(context.getState().isError());
        Assert.assertEquals(ErrorCode.ERR_EXPLICIT_TXN_NOT_SUPPORT_STMT, context.getState().getErrorCode());
    }

    @Test
    public void testInsertSameTable() throws IOException, DdlException {
        new MockUp<DefaultCoordinator>() {
            @Mock
            public void exec() throws StarRocksException, RpcException, InterruptedException {
            }

            @Mock
            public boolean join(int timeoutSecond) {
                return true;
            }

            @Mock
            public boolean isDone() {
                return true;
            }

            @Mock
            public Status getExecStatus() {
                return Status.OK;
            }

            @Mock
            public Map<String, String> getLoadCounters() {
                Map<String, String> counters = new HashMap<String, String>();
                counters.put(LoadEtlTask.DPP_NORMAL_ALL, "0");
                counters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "0");
                counters.put(LoadJob.LOADED_BYTES, "0");

                return counters;
            }
        };

        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("db1", "tbl1");

        context.setQualifiedUser("u1");
        context.setCurrentUserIdentity(new UserIdentity("u1", "%"));

        TUniqueId queryId = new TUniqueId(2, 3);
        context.setExecutionId(queryId);
        UUID lastQueryId = new UUID(4L, 5L);
        context.setLastQueryId(lastQueryId);

        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO));

        String sql = "insert into db1.tbl1 values(1,2,3)";
        DmlStmt stmt = (DmlStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);

        TransactionStmtExecutor.loadData(database, olapTable, new ExecPlan(), (DmlStmt) stmt, stmt.getOrigStmt(), context);
        Assert.assertFalse(context.getState().isError());
        try {
            TransactionStmtExecutor.loadData(database, olapTable, new ExecPlan(), (DmlStmt) stmt, stmt.getOrigStmt(), context);
            Assert.fail();
        } catch (ErrorReportException e) {
            Assert.assertEquals(ErrorCode.ERR_TXN_IMPORT_SAME_TABLE, e.getErrorCode());
        }
    }

    @Test
    public void testInsertSameTable2() throws IOException, DdlException {
        new MockUp<DefaultCoordinator>() {
            @Mock
            public void exec() throws StarRocksException, RpcException, InterruptedException {
            }

            @Mock
            public boolean join(int timeoutSecond) {
                return true;
            }

            @Mock
            public boolean isDone() {
                return true;
            }

            @Mock
            public Status getExecStatus() {
                return Status.OK;
            }

            @Mock
            public Map<String, String> getLoadCounters() {
                Map<String, String> counters = new HashMap<String, String>();
                counters.put(LoadEtlTask.DPP_NORMAL_ALL, "10");
                counters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "5");
                counters.put(LoadJob.LOADED_BYTES, "0");

                return counters;
            }
        };

        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("db1", "tbl1");

        context.setQualifiedUser("u1");
        context.setCurrentUserIdentity(new UserIdentity("u1", "%"));

        TUniqueId queryId = new TUniqueId(4, 4);
        context.setExecutionId(queryId);
        UUID lastQueryId = new UUID(4L, 5L);
        context.setLastQueryId(lastQueryId);

        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO));

        String sql = "insert into db1.tbl1 values(1,2,3)";
        DmlStmt stmt = (DmlStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);

        TransactionStmtExecutor.loadData(database, olapTable, new ExecPlan(), (DmlStmt) stmt, stmt.getOrigStmt(), context);

        ExplicitTxnState explicitTxnState =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getExplicitTxnState(context.getTxnId());
        String label = explicitTxnState.getTransactionState().getLabel();
        LoadMgr loadMgr = GlobalStateMgr.getCurrentState().getLoadMgr();
        LoadJob loadJob = loadMgr.getLoadJobs(label).get(0);
        Assert.assertEquals(JobState.CANCELLED, loadJob.getState());
    }

    @Test
    public void testBegin() throws IOException, DdlException {
        ConnectContext context = new ConnectContext();

        long transactionId = 1;
        TransactionState transactionState = new TransactionState(transactionId, "test-label", null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                context.getExecTimeout() * 1000L);

        ExplicitTxnState explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(transactionState);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        globalTransactionMgr.addTransactionState(transactionId, explicitTxnState);

        context.setTxnId(transactionId);
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO));

        Assert.assertFalse(context.getState().isError());
        Assert.assertEquals("{'label':'test-label', 'status':'PREPARE', 'txnId':'1'}", context.getState().getInfoMessage());
    }

    @Test
    public void testCommitEmptyInsert() {
        ConnectContext context = new ConnectContext();
        //Commit txn not exist
        context.setTxnId(12345);
        TransactionStmtExecutor.commitStmt(context, new CommitStmt(NodePosition.ZERO));

        // Commit transaction not insert data
        long transactionId = 1;
        TransactionState transactionState = new TransactionState(transactionId, "test-label", null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                context.getExecTimeout() * 1000L);

        ExplicitTxnState explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(transactionState);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        globalTransactionMgr.addTransactionState(transactionId, explicitTxnState);

        context.setTxnId(transactionId);
        TransactionStmtExecutor.commitStmt(context, new CommitStmt(NodePosition.ZERO));
        Assert.assertEquals(0, context.getTxnId());
        Assert.assertEquals("{'label':'test-label', 'status':'VISIBLE', 'txnId':'1'}", context.getState().getInfoMessage());
        Assert.assertNull(globalTransactionMgr.getExplicitTxnState(transactionId));

        // Rollback transaction not insert data
        transactionId = 2;
        transactionState = new TransactionState(transactionId, "test-label-2", null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                context.getExecTimeout() * 1000L);

        explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(transactionState);

        globalTransactionMgr.addTransactionState(transactionId, explicitTxnState);

        context.setTxnId(transactionId);
        TransactionStmtExecutor.rollbackStmt(context, new RollbackStmt(NodePosition.ZERO));
        Assert.assertEquals(0, context.getTxnId());
        Assert.assertEquals("{'label':'test-label-2', 'status':'ABORTED', 'txnId':'2'}", context.getState().getInfoMessage());
        Assert.assertNull(globalTransactionMgr.getExplicitTxnState(transactionId));
    }

    @Test
    public void testCommitDatabaseNotExist() {
        ConnectContext context = new ConnectContext();
        long transactionId = 1;
        TransactionState transactionState = new TransactionState(transactionId, "test-label", null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                context.getExecTimeout() * 1000L);

        ExplicitTxnState explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(transactionState);

        ExplicitTxnState.ExplicitTxnStateItem explicitTxnStateItem = new ExplicitTxnState.ExplicitTxnStateItem();
        explicitTxnStateItem.setTabletCommitInfos(List.of());
        explicitTxnStateItem.setTabletFailInfos(List.of());

        explicitTxnState.addTransactionItem(explicitTxnStateItem);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        globalTransactionMgr.addTransactionState(transactionId, explicitTxnState);

        context.setTxnId(transactionId);
        TransactionStmtExecutor.commitStmt(context, new CommitStmt(NodePosition.ZERO));
        Assert.assertEquals(0, context.getTxnId());
        Assert.assertNull(globalTransactionMgr.getExplicitTxnState(transactionId));
        Assert.assertEquals("database 0 is not found", context.getState().getErrorMessage());

        transactionId = 2;
        transactionState = new TransactionState(transactionId, "test-label-2", null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                context.getExecTimeout() * 1000L);

        explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(transactionState);

        explicitTxnStateItem = new ExplicitTxnState.ExplicitTxnStateItem();
        explicitTxnStateItem.setTabletCommitInfos(List.of());
        explicitTxnStateItem.setTabletFailInfos(List.of());

        explicitTxnState.addTransactionItem(explicitTxnStateItem);

        globalTransactionMgr.addTransactionState(transactionId, explicitTxnState);

        context.setTxnId(transactionId);
        TransactionStmtExecutor.rollbackStmt(context, new RollbackStmt(NodePosition.ZERO));
        Assert.assertEquals(0, context.getTxnId());
        Assert.assertNull(globalTransactionMgr.getExplicitTxnState(transactionId));
        Assert.assertEquals("database 0 is not found", context.getState().getErrorMessage());
    }
}
