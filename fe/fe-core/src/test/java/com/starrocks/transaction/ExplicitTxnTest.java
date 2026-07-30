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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MockedLocalMetaStore;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QueryWarning;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExplicitTxnTest {

    private TransactionState addExplicitState(GlobalTransactionMgr mgr, long txnId, String label, long timeoutMs) {
        TransactionState state = new TransactionState(
                txnId, label, null, TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(
                        TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                timeoutMs);
        state.setPrepareTime(System.currentTimeMillis());
        ExplicitTxnState explicit = new ExplicitTxnState();
        explicit.setTransactionState(state);
        mgr.addTransactionState(txnId, explicit);
        return state;
    }

    private void cleanupExplicitState(GlobalTransactionMgr mgr, TransactionState state) throws StarRocksException {
        try {
            if (state.getDbId() != 0 && state.isRunning()) {
                mgr.abortTransaction(state.getDbId(), state.getTransactionId(), "ExplicitTxnTest cleanup");
            }
        } finally {
            mgr.clearExplicitTxnState(state.getTransactionId());
        }
    }

    private void abortRunningTransactions(GlobalTransactionMgr mgr, long dbId) throws StarRocksException {
        DatabaseTransactionMgr dbTxnMgr = mgr.getDatabaseTransactionMgr(dbId);
        Long txnId;
        while ((txnId = dbTxnMgr.getMinActiveTxnId().orElse(null)) != null) {
            mgr.abortTransaction(dbId, txnId, "ExplicitTxnTest isolation");
        }
    }

    private static void awaitLatch(CountDownLatch latch, String message) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS), message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @AfterAll
    public static void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeAll
    public static void init() throws DdlException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MetricRepo.init();

        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(globalStateMgr);

        UtFrameUtils.setUpForPersistTest();

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
        serializer.writeEofString("select 1");
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

        Assertions.assertNotEquals(ErrorCode.ERR_EXPLICIT_TXN_NOT_SUPPORT_STMT, context.getState().getErrorCode());

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

        Assertions.assertTrue(context.getState().isError());
        Assertions.assertEquals(ErrorCode.ERR_EXPLICIT_TXN_NOT_SUPPORT_STMT, context.getState().getErrorCode());
    }

    @Test
    public void testShowStmtAllowedInExplicitTxn() {
        // SHOW statements are read-only metadata queries and must be allowed inside an
        // explicit transaction (regression for ERR_EXPLICIT_TXN_NOT_SUPPORT_STMT on SHOW GRANTS / SHOW WAREHOUSES).
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setTxnId(1);

        ShowGrantsStmt showGrants = new ShowGrantsStmt(new UserRef("u1", "%"), NodePosition.ZERO);
        Assertions.assertDoesNotThrow(() -> ExplicitTxnStatementValidator.validate(showGrants, context));

        ShowWarehousesStmt showWarehouses = new ShowWarehousesStmt(null);
        Assertions.assertDoesNotThrow(() -> ExplicitTxnStatementValidator.validate(showWarehouses, context));
    }

    @Test
    public void testInsertSameTable() throws IOException, DdlException {
        AtomicInteger activationCount = new AtomicInteger();
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public ExplicitTxnState activateExplicitTransactionTable(
                    Invocation invocation, long txnId, long dbId, long tableId) throws StarRocksException {
                activationCount.incrementAndGet();
                return invocation.proceed(txnId, dbId, tableId);
            }
        };
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
        Assertions.assertFalse(context.getState().isError());
        Assertions.assertEquals(1, activationCount.get());
        try {
            TransactionStmtExecutor.loadData(database, olapTable, new ExecPlan(), (DmlStmt) stmt, stmt.getOrigStmt(), context);
            Assertions.fail();
        } catch (ErrorReportException e) {
            Assertions.assertEquals(ErrorCode.ERR_TXN_IMPORT_SAME_TABLE, e.getErrorCode());
        }
        Assertions.assertEquals(1, activationCount.get());
    }

    @Test
    public void testExplicitTxnFirstTableGatedByPerTableLimit() throws Exception {
        // With the per-table cap enabled, an explicit transaction's FIRST INSERT into a table that is already
        // at the cap must be rejected at admission. An explicit transaction registers before any table is
        // attached, so without passing the target table explicitly the check sees an empty table list and
        // gates nothing, admitting the load regardless of how loaded that table already is.
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
        long dbId = database.getId();
        long tblId = olapTable.getId();

        context.setQualifiedUser("u1");
        context.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        context.setExecutionId(new TUniqueId(10, 11));
        context.setLastQueryId(new UUID(12L, 13L));

        DatabaseTransactionMgr dbTxnMgr =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getDatabaseTransactionMgr(dbId);
        TransactionState occupy = new TransactionState(dbId, Lists.newArrayList(tblId), 90011L, "occupy_tbl1", null,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localhost"), -1, 3600000L);
        occupy.setTransactionStatus(TransactionStatus.PREPARE);

        int savedTable = Config.max_running_txn_num_per_table;
        int savedDb = Config.max_running_txn_num_per_db;
        try {
            Config.max_running_txn_num_per_db = 1000;
            Config.max_running_txn_num_per_table = 1;
            // Occupy tbl1's single per-table slot with a running transaction.
            Deencapsulation.invoke(dbTxnMgr, "unprotectUpsertTransactionState", occupy);

            TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO));
            String sql = "insert into db1.tbl1 values(1,2,3)";
            DmlStmt stmt = (DmlStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
            Analyzer.analyze(stmt, context);

            TransactionStmtExecutor.loadData(database, olapTable, new ExecPlan(), stmt, stmt.getOrigStmt(), context);

            // tbl1 is already at its per-table cap, so the explicit transaction's first INSERT into it is rejected.
            Assertions.assertTrue(context.getState().isError());
        } finally {
            occupy.setTransactionStatus(TransactionStatus.ABORTED);
            Deencapsulation.invoke(dbTxnMgr, "unprotectUpsertTransactionState", occupy);
            Config.max_running_txn_num_per_table = savedTable;
            Config.max_running_txn_num_per_db = savedDb;
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
        Assertions.assertEquals(JobState.CANCELLED, loadJob.getState());
    }

    @Test
    public void testFilteredRowsProduceSessionWarning() throws IOException, DdlException {
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
            public String getTrackingUrl() {
                return "http://be:8040/api/_load_error_log";
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
        // let the 5 filtered rows pass the ratio check instead of cancelling the load
        context.getSessionVariable().setInsertMaxFilterRatio(1);

        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("db1", "tbl1");

        context.setQualifiedUser("u1");
        context.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        context.setExecutionId(new TUniqueId(20, 21));
        context.setLastQueryId(new UUID(22L, 23L));

        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO));

        String sql = "insert into db1.tbl1 values(1,2,3)";
        DmlStmt stmt = (DmlStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        TransactionStmtExecutor.loadData(database, olapTable, new ExecPlan(), stmt, stmt.getOrigStmt(), context);

        // The INSERT succeeds inside the transaction and the filtered rows are surfaced as a
        // session warning, matching the autocommit path (SHOW WARNINGS reads it back before COMMIT).
        Assertions.assertFalse(context.getState().isError());
        Assertions.assertEquals(1, context.getWarnings().size());
        QueryWarning warning = context.getWarnings().get(0);
        Assertions.assertEquals("Warning", warning.getLevel());
        Assertions.assertEquals("1265", warning.getCode());
        Assertions.assertEquals("5 row(s) filtered or substituted to NULL during load; "
                + "tracking_url=http://be:8040/api/_load_error_log", warning.getMessage());
    }

    @Test
    public void testPartialUpdateOnModifiedTableRejected() throws IOException, DdlException {
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
        context.setExecutionId(new TUniqueId(10, 11));
        context.setLastQueryId(new UUID(12L, 13L));

        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO));

        // Statement 1: a normal write marks tbl1 as modified in this explicit transaction.
        String sql = "insert into db1.tbl1 values(1,2,3)";
        DmlStmt stmt = (DmlStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        TransactionStmtExecutor.loadData(database, olapTable, new ExecPlan(), stmt, stmt.getOrigStmt(), context);
        Assertions.assertFalse(context.getState().isError());

        // Statement 2: a partial-update INSERT targeting the already-modified tbl1 must be rejected
        // with ERR_EXPLICIT_TXN_PARTIAL_UPDATE_ON_MODIFIED_TABLE (5308).
        InsertStmt partialUpdate = mock(InsertStmt.class);
        when(partialUpdate.usePartialUpdate()).thenReturn(true);
        when(partialUpdate.getTargetTable()).thenReturn(olapTable);

        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> ExplicitTxnStatementValidator.validate(partialUpdate, context));
        Assertions.assertTrue(e.getMessage().contains("Partial update cannot be applied to table"),
                "unexpected message: " + e.getMessage());

        // A partial update targeting a different, not-yet-modified table is allowed.
        OlapTable otherTable =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("db2", "tbl1");
        InsertStmt partialUpdateOther = mock(InsertStmt.class);
        when(partialUpdateOther.usePartialUpdate()).thenReturn(true);
        when(partialUpdateOther.getTargetTable()).thenReturn(otherTable);
        Assertions.assertDoesNotThrow(() -> ExplicitTxnStatementValidator.validate(partialUpdateOther, context));
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

        Assertions.assertFalse(context.getState().isError());
        Assertions.assertEquals("{'label':'test-label', 'status':'PREPARE', 'txnId':'1'}", context.getState().getInfoMessage());
    }

    @Test
    public void testBeginWithLabel() throws IOException, DdlException {
        // Test BEGIN with user-specified label
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        TUniqueId queryId = new TUniqueId(100, 200);
        context.setExecutionId(queryId);

        // Test parsing BEGIN with label
        String sql = "BEGIN WITH LABEL my_custom_label";
        BeginStmt beginStmt = (BeginStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Assertions.assertEquals("my_custom_label", beginStmt.getLabel());

        // Test parsing START TRANSACTION with label
        sql = "START TRANSACTION WITH LABEL another_label";
        beginStmt = (BeginStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Assertions.assertEquals("another_label", beginStmt.getLabel());

        // Test execution with user-specified label
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "user_txn_label"));

        Assertions.assertFalse(context.getState().isError());
        String infoMessage = context.getState().getInfoMessage();
        Assertions.assertTrue(infoMessage.contains("'label':'user_txn_label'"));
        Assertions.assertTrue(infoMessage.contains("'status':'PREPARE'"));

        // Cleanup
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().clearExplicitTxnState(context.getTxnId());
        context.setTxnId(0);
    }

    @Test
    public void testBeginWithoutLabel() throws IOException, DdlException {
        // Test BEGIN without label (should use executionId as default)
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        TUniqueId queryId = new TUniqueId(300, 400);
        context.setExecutionId(queryId);

        // Test parsing BEGIN without label
        String sql = "BEGIN";
        BeginStmt beginStmt = (BeginStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Assertions.assertNull(beginStmt.getLabel());

        // Test parsing START TRANSACTION without label
        sql = "START TRANSACTION";
        beginStmt = (BeginStmt) SqlParser.parseSingleStatement(sql, context.getSessionVariable().getSqlMode());
        Assertions.assertNull(beginStmt.getLabel());

        // Test execution without label (should generate default label)
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO));

        Assertions.assertFalse(context.getState().isError());
        String infoMessage = context.getState().getInfoMessage();
        // Default label is generated from executionId
        Assertions.assertTrue(infoMessage.contains("'status':'PREPARE'"));

        // Cleanup
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().clearExplicitTxnState(context.getTxnId());
        context.setTxnId(0);
    }

    @Test
    public void testBeginWithInvalidLabel() {
        // Test BEGIN with invalid label format (contains spaces or special characters)
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        TUniqueId queryId = new TUniqueId(500, 600);
        context.setExecutionId(queryId);

        // Test label with spaces - should throw SemanticException
        Assertions.assertThrows(SemanticException.class, () -> {
            TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "label with spaces"));
        });

        // Test label with special characters - should throw SemanticException
        Assertions.assertThrows(SemanticException.class, () -> {
            TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "label@special#chars"));
        });

        // Test label exceeds max length (128 chars) - should throw SemanticException
        String longLabel = "a".repeat(129);
        Assertions.assertThrows(SemanticException.class, () -> {
            TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, longLabel));
        });

        // Verify valid labels still work (alphanumeric with underscores and hyphens)
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "valid_label-123"));
        Assertions.assertFalse(context.getState().isError());

        // Cleanup
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().clearExplicitTxnState(context.getTxnId());
        context.setTxnId(0);
    }

    @Test
    public void testBeginWithDifferentLabelWhenTxnExists() {
        // Test that BEGIN WITH LABEL throws error when transaction already exists with different label
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        TUniqueId queryId = new TUniqueId(700, 800);
        context.setExecutionId(queryId);

        // First BEGIN with a label
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "first_label"));
        Assertions.assertFalse(context.getState().isError());
        String infoMessage = context.getState().getInfoMessage();
        Assertions.assertTrue(infoMessage.contains("'label':'first_label'"));

        // Second BEGIN with a different label should throw SemanticException
        Assertions.assertThrows(SemanticException.class, () -> {
            TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "different_label"));
        });

        // Second BEGIN with the same label should succeed (return existing transaction)
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "first_label"));
        Assertions.assertFalse(context.getState().isError());
        infoMessage = context.getState().getInfoMessage();
        Assertions.assertTrue(infoMessage.contains("'label':'first_label'"));

        // Second BEGIN without label should also succeed (return existing transaction)
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO));
        Assertions.assertFalse(context.getState().isError());

        // Cleanup
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().clearExplicitTxnState(context.getTxnId());
        context.setTxnId(0);
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
        Assertions.assertEquals(0, context.getTxnId());
        Assertions.assertEquals("{'label':'test-label', 'status':'VISIBLE', 'txnId':'1'}", context.getState().getInfoMessage());
        Assertions.assertNull(globalTransactionMgr.getExplicitTxnState(transactionId));

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
        Assertions.assertEquals(0, context.getTxnId());
        Assertions.assertEquals("{'label':'test-label-2', 'status':'ABORTED', 'txnId':'2'}", context.getState().getInfoMessage());
        Assertions.assertNull(globalTransactionMgr.getExplicitTxnState(transactionId));
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
        Assertions.assertEquals(0, context.getTxnId());
        Assertions.assertNull(globalTransactionMgr.getExplicitTxnState(transactionId));
        Assertions.assertEquals("database 0 is not found", context.getState().getErrorMessage());

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
        Assertions.assertEquals(0, context.getTxnId());
        Assertions.assertNull(globalTransactionMgr.getExplicitTxnState(transactionId));
        Assertions.assertEquals("database 0 is not found", context.getState().getErrorMessage());
    }

    @Test
    public void testCommitTimeoutPassedInMilliseconds() {
        // Regression: commitStmt() reads query_timeout (seconds) and previously passed it
        // directly to GlobalTransactionMgr.retryCommitOnRateLimitExceeded(..., long timeoutMs),
        // causing a 300s query_timeout to become a 300ms lock wait.
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        int queryTimeoutS = 300;
        context.getSessionVariable().setQueryTimeoutS(queryTimeoutS);

        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Assertions.assertNotNull(db1);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long transactionId = globalTransactionMgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState transactionState = new TransactionState(transactionId, "timeout-unit-test", null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                context.getExecTimeout() * 1000L);
        transactionState.setDbId(db1.getId());

        ExplicitTxnState explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(transactionState);

        ExplicitTxnState.ExplicitTxnStateItem item = new ExplicitTxnState.ExplicitTxnStateItem();
        item.setTabletCommitInfos(List.of());
        item.setTabletFailInfos(List.of());
        explicitTxnState.addTransactionItem(item);

        globalTransactionMgr.addTransactionState(transactionId, explicitTxnState);
        context.setTxnId(transactionId);

        long[] capturedTimeoutMs = {-1L};
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public VisibleStateWaiter retryCommitOnRateLimitExceeded(
                    Database db, long txnId, List<TabletCommitInfo> commitInfos, List<TabletFailInfo> failInfos,
                    TxnCommitAttachment attachment, long timeoutMs) throws LockTimeoutException {
                capturedTimeoutMs[0] = timeoutMs;
                // Short-circuit the rest of commitStmt; the exception is caught and reported as error.
                throw new LockTimeoutException(
                        "get database write lock timeout, database=" + db.getFullName() + ", timeout=" + timeoutMs + "ms");
            }
        };

        TransactionStmtExecutor.commitStmt(context, new CommitStmt(NodePosition.ZERO));

        Assertions.assertEquals((long) queryTimeoutS * 1000L, capturedTimeoutMs[0],
                "query_timeout (" + queryTimeoutS + "s) must be passed to retryCommitOnRateLimitExceeded "
                        + "as milliseconds, got " + capturedTimeoutMs[0]);
    }

    @Test
    public void testCommitWithLostTransactionState() {
        // When txnId is set but explicitTxnState is null (e.g., FE leader switch),
        // commitStmt should report an error instead of silently succeeding.
        ConnectContext context = new ConnectContext();
        context.setTxnId(99999);

        TransactionStmtExecutor.commitStmt(context, new CommitStmt(NodePosition.ZERO));

        Assertions.assertEquals(0, context.getTxnId());
        Assertions.assertTrue(context.getState().isError());
        Assertions.assertTrue(context.getState().getErrorMessage().contains("Transaction state not found"));
    }

    @Test
    public void testRollbackWithLostTransactionState() {
        // When txnId is set but explicitTxnState is null (e.g., FE leader switch),
        // rollbackStmt should report an error instead of silently succeeding.
        ConnectContext context = new ConnectContext();
        context.setTxnId(99998);

        TransactionStmtExecutor.rollbackStmt(context, new RollbackStmt(NodePosition.ZERO));

        Assertions.assertEquals(0, context.getTxnId());
        Assertions.assertTrue(context.getState().isError());
        Assertions.assertTrue(context.getState().getErrorMessage().contains("Transaction state not found"));
    }

    @Test
    public void testBeginWithLostTransactionState() {
        // When txnId is set but explicitTxnState was cleared (e.g., timeout cleanup),
        // beginStmt should reset and create a new transaction instead of NPE.
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        TUniqueId queryId = new TUniqueId(900, 901);
        context.setExecutionId(queryId);

        // Simulate stale txnId without matching explicitTxnState
        context.setTxnId(88888);

        // BEGIN should recover by creating a new transaction
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "recovery_label"));
        Assertions.assertFalse(context.getState().isError());
        Assertions.assertNotEquals(88888, context.getTxnId());
        Assertions.assertTrue(context.getState().getInfoMessage().contains("'label':'recovery_label'"));

        // Cleanup
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().clearExplicitTxnState(context.getTxnId());
        context.setTxnId(0);
    }

    @Test
    public void testCleanupClearsExplicitTxnState() {
        // Test that ConnectContext.cleanup() properly clears explicitTxnStateMap entries
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        TUniqueId queryId = new TUniqueId(950, 951);
        context.setExecutionId(queryId);

        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "cleanup_test_label"));
        long txnId = context.getTxnId();
        Assertions.assertNotEquals(0, txnId);
        Assertions.assertNotNull(
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getExplicitTxnState(txnId));

        // Simulate connection disconnect
        context.cleanup();

        // Verify explicitTxnState was cleaned up
        Assertions.assertNull(
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getExplicitTxnState(txnId));
    }

    @Test
    public void testReshardPlanningReservations() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        Database db2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db2");
        Table table2 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db2.getFullName(), "tbl1");
        abortRunningTransactions(mgr, db1.getId());
        abortRunningTransactions(mgr, db2.getId());
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "reshard-planning-reservation", 60_000L);
        try {
            Assertions.assertTrue(mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
            Assertions.assertTrue(mgr.isPreviousTransactionsFinished(
                    txnId, db1.getId(), List.of(table1.getId())));

            Assertions.assertSame(state, mgr.reserveExplicitTransactionLayout(
                    txnId, db1.getId(), table1.getId()));
            Assertions.assertEquals(0, state.getDbId());
            Assertions.assertTrue(state.getTableIdList().isEmpty());
            Assertions.assertFalse(mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
            Assertions.assertTrue(mgr.isPreviousTransactionsFinished(
                    txnId, db1.getId(), List.of(table1.getId())));

            Assertions.assertSame(state, mgr.reserveExplicitTransactionLayout(
                    txnId, db2.getId(), table2.getId()));
            Assertions.assertSame(state, mgr.registerExplicitTransactionState(txnId, db2.getId()));
            Assertions.assertTrue(mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
            Assertions.assertFalse(mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db2.getId(), List.of(table2.getId()), Set.of()));
            Assertions.assertTrue(state.getTableIdList().isEmpty());
            Assertions.assertSame(state, mgr.registerExplicitTransactionState(txnId, db2.getId()));

            ErrorReportException exception = Assertions.assertThrows(ErrorReportException.class,
                    () -> mgr.reserveExplicitTransactionLayout(txnId, db1.getId(), table1.getId()));
            Assertions.assertEquals(ErrorCode.ERR_TXN_FORBID_CROSS_DB, exception.getErrorCode());
            Assertions.assertEquals(ErrorCode.ERR_TXN_FORBID_CROSS_DB.formatErrorMsg(), exception.getMessage());
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testRegistrationFailureRestoresUnboundStateAndReservation() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        abortRunningTransactions(mgr, db1.getId());
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "registration-failure", 60_000L);
        try {
            mgr.reserveExplicitTransactionLayout(txnId, db1.getId(), table1.getId());
            new MockUp<DatabaseTransactionMgr>() {
                @Mock
                public void upsertTransactionState(TransactionState transactionState) throws AnalysisException {
                    throw new AnalysisException("injected upsert failure");
                }
            };

            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> mgr.registerExplicitTransactionState(txnId, db1.getId()));
            Assertions.assertEquals("injected upsert failure", exception.getMessage());
            Assertions.assertEquals(0, state.getDbId());
            Assertions.assertNull(mgr.getDatabaseTransactionMgr(db1.getId()).getTransactionState(txnId));
            Assertions.assertFalse(mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testRegisteredTransactionReservesNewTableDuringPlanning() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        abortRunningTransactions(mgr, db1.getId());
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long secondTableId = table1.getId() + 1;
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "registered-new-table", 60_000L);
        try {
            mgr.registerExplicitTransactionState(txnId, db1.getId());
            state.addTableIdList(table1.getId());
            mgr.reserveExplicitTransactionLayout(txnId, db1.getId(), secondTableId);

            Assertions.assertFalse(state.getTableIdList().contains(secondTableId));
            Assertions.assertFalse(mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(secondTableId), Set.of()));
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testDatabaseLoadDataRegistrationFailureRestoresUnboundState() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "database-load-registration-failure", 60_000L);
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setTxnId(txnId);
        context.setExecutionId(new TUniqueId(txnId, txnId));
        context.setQualifiedUser("u1");
        context.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        try {
            new MockUp<DatabaseTransactionMgr>() {
                @Mock
                public void upsertTransactionState(TransactionState transactionState,
                        List<Long> admissionTableIds) throws AnalysisException {
                    throw new AnalysisException("injected upsert failure");
                }
            };

            TransactionStmtExecutor.loadData(db1, table1, new ExecPlan(), mock(DmlStmt.class),
                    new OriginStatement("insert"), context);
            Assertions.assertTrue(context.getState().isError());
            Assertions.assertTrue(context.getState().getErrorMessage().contains("injected upsert failure"));
            Assertions.assertEquals(0, state.getDbId());
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testStreamLoadDataRegistrationFailureRestoresUnboundState() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "stream-load-registration-failure", 60_000L);
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setTxnId(txnId);
        context.setExecutionId(new TUniqueId(txnId, txnId));
        context.setQualifiedUser("u1");
        context.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        try {
            new MockUp<DatabaseTransactionMgr>() {
                @Mock
                public void upsertTransactionState(TransactionState transactionState,
                        List<Long> admissionTableIds) throws AnalysisException {
                    throw new AnalysisException("injected upsert failure");
                }
            };

            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> TransactionStmtExecutor.loadData(db1.getId(), table1.getId(),
                            new ExplicitTxnState.ExplicitTxnStateItem(), context));
            Assertions.assertEquals("injected upsert failure", exception.getMessage());
            Assertions.assertEquals(0, state.getDbId());
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testDatabaseLoadDataRejectsMissingExplicitState() {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setTxnId(txnId);
        context.setExecutionId(new TUniqueId(txnId, txnId));
        context.setQualifiedUser("u1");
        context.setCurrentUserIdentity(new UserIdentity("u1", "%"));

        Assertions.assertNull(mgr.reserveExplicitTransactionLayout(txnId, db1.getId(), table1.getId()));
        TransactionStmtExecutor.loadData(db1, table1, new ExecPlan(), mock(DmlStmt.class),
                new OriginStatement("insert"), context);

        Assertions.assertTrue(context.getState().isError());
        Assertions.assertTrue(context.getState().getErrorMessage().contains(Long.toString(txnId)));
    }

    @Test
    public void testStreamLoadDataRejectsMissingExplicitState() {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setTxnId(txnId);

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> TransactionStmtExecutor.loadData(db1.getId(), table1.getId(),
                        new ExplicitTxnState.ExplicitTxnStateItem(), context));
        Assertions.assertTrue(exception.getMessage().contains(Long.toString(txnId)));
    }

    @Test
    public void testDatabaseLoadDataRejectsStateRemovedBeforeActivation() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "database-load-removed-state", 60_000L);
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setTxnId(txnId);
        context.setExecutionId(new TUniqueId(txnId, txnId));
        context.setQualifiedUser("u1");
        context.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        try {
            new MockUp<GlobalTransactionMgr>() {
                @Mock
                public ExplicitTxnState activateExplicitTransactionTable(
                        Invocation invocation, long transactionId, long dbId, long tableId) throws StarRocksException {
                    mgr.clearExplicitTxnState(transactionId);
                    return invocation.proceed(transactionId, dbId, tableId);
                }
            };

            TransactionStmtExecutor.loadData(db1, table1, new ExecPlan(), mock(DmlStmt.class),
                    new OriginStatement("insert"), context);
            Assertions.assertTrue(context.getState().isError());
            Assertions.assertTrue(context.getState().getErrorMessage().contains(Long.toString(txnId)));
            Assertions.assertFalse(state.getTableIdList().contains(table1.getId()));
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testStreamLoadDataRejectsStateRemovedBeforeActivation() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "stream-load-removed-state", 60_000L);
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setTxnId(txnId);
        try {
            new MockUp<GlobalTransactionMgr>() {
                @Mock
                public ExplicitTxnState activateExplicitTransactionTable(
                        Invocation invocation, long transactionId, long dbId, long tableId) throws StarRocksException {
                    mgr.clearExplicitTxnState(transactionId);
                    return invocation.proceed(transactionId, dbId, tableId);
                }
            };

            StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                    () -> TransactionStmtExecutor.loadData(db1.getId(), table1.getId(),
                            new ExplicitTxnState.ExplicitTxnStateItem(), context));
            Assertions.assertTrue(exception.getMessage().contains(Long.toString(txnId)));
            Assertions.assertFalse(state.getTableIdList().contains(table1.getId()));
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testExplicitTableActivationRejectsStateClearedAfterRegistration() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        abortRunningTransactions(mgr, db1.getId());
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "activation-after-clear", 60_000L);
        try {
            mgr.registerExplicitTransactionState(txnId, db1.getId());
            mgr.clearExplicitTxnState(txnId);

            StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                    () -> mgr.activateExplicitTransactionTable(txnId, db1.getId(), table1.getId()));
            Assertions.assertTrue(exception.getMessage().contains(Long.toString(txnId)));
            Assertions.assertFalse(state.getTableIdList().contains(table1.getId()));
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testDatabaseTableActivationRejectsMissingTransaction() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();

        TransactionNotFoundException exception = Assertions.assertThrows(TransactionNotFoundException.class,
                () -> mgr.getDatabaseTransactionMgr(db1.getId()).activateTransactionTable(txnId, 1L));
        Assertions.assertTrue(exception.getMessage().contains(Long.toString(txnId)));
    }

    @Test
    public void testDatabaseTableActivationRejectsFinishedTransaction() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        abortRunningTransactions(mgr, db1.getId());
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "activation-after-finish", 60_000L);
        try {
            mgr.registerExplicitTransactionState(txnId, db1.getId());
            mgr.abortTransaction(db1.getId(), txnId, "finish before activation");

            Assertions.assertThrows(TransactionNotFoundException.class,
                    () -> mgr.getDatabaseTransactionMgr(db1.getId())
                            .activateTransactionTable(txnId, table1.getId()));
            Assertions.assertFalse(state.getTableIdList().contains(table1.getId()));
        } finally {
            mgr.clearExplicitTxnState(txnId);
        }
    }

    @Test
    public void testDatabaseTableActivationIsIdempotent() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        abortRunningTransactions(mgr, db1.getId());
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "idempotent-table-activation", 60_000L);
        try {
            mgr.registerExplicitTransactionState(txnId, db1.getId());
            DatabaseTransactionMgr dbTxnMgr = mgr.getDatabaseTransactionMgr(db1.getId());

            Assertions.assertSame(state, dbTxnMgr.activateTransactionTable(txnId, table1.getId()));
            Assertions.assertSame(state, dbTxnMgr.activateTransactionTable(txnId, table1.getId()));
            Assertions.assertEquals(1, state.getTableIdList().stream()
                    .filter(tableId -> tableId.equals(table1.getId())).count());
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testExplicitTableActivationSerializesWatermarkAndRemoval() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        abortRunningTransactions(mgr, db1.getId());
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "serialize-table-activation", 60_000L);
        CountDownLatch activationEntered = new CountDownLatch(1);
        CountDownLatch releaseActivation = new CountDownLatch(1);
        CountDownLatch watermarkStarted = new CountDownLatch(1);
        CountDownLatch removalStarted = new CountDownLatch(1);
        AtomicBoolean blockActivation = new AtomicBoolean(true);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            mgr.registerExplicitTransactionState(txnId, db1.getId());
            ExplicitTxnState explicit = mgr.getExplicitTxnState(txnId);
            Assertions.assertNotNull(explicit);
            new MockUp<TransactionState>() {
                @Mock
                public void addTableIdList(Invocation invocation, Long tableId) {
                    TransactionState invokedState = invocation.getInvokedInstance();
                    if (invokedState == state && tableId.equals(table1.getId())
                            && blockActivation.compareAndSet(true, false)) {
                        activationEntered.countDown();
                        awaitLatch(releaseActivation, "activation was not released");
                    }
                    invocation.proceed(tableId);
                }
            };

            Future<ExplicitTxnState> activation = executor.submit(
                    () -> mgr.activateExplicitTransactionTable(txnId, db1.getId(), table1.getId()));
            Assertions.assertTrue(activationEntered.await(10, TimeUnit.SECONDS));
            Future<Boolean> watermark = executor.submit(() -> {
                watermarkStarted.countDown();
                return mgr.isPreviousTransactionsFinishedForReshard(
                        txnId, db1.getId(), List.of(table1.getId()), Set.of());
            });
            Future<?> removal = executor.submit(() -> {
                removalStarted.countDown();
                mgr.clearExplicitTxnState(txnId);
            });
            Assertions.assertTrue(watermarkStarted.await(10, TimeUnit.SECONDS));
            Assertions.assertTrue(removalStarted.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(watermark.isDone());
            Assertions.assertFalse(removal.isDone());

            releaseActivation.countDown();
            Assertions.assertSame(explicit, activation.get(10, TimeUnit.SECONDS));
            removal.get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(watermark.get(10, TimeUnit.SECONDS));
            Assertions.assertNull(mgr.getExplicitTxnState(txnId));
            Assertions.assertTrue(state.getTableIdList().contains(table1.getId()));
        } finally {
            releaseActivation.countDown();
            executor.shutdownNow();
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testDatabaseWatermarkSerializesTableActivation() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        abortRunningTransactions(mgr, db1.getId());
        long secondTableId = table1.getId() + 1;
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "serialize-db-table-list", 60_000L);
        CountDownLatch watermarkEntered = new CountDownLatch(1);
        CountDownLatch releaseWatermark = new CountDownLatch(1);
        CountDownLatch activationStarted = new CountDownLatch(1);
        AtomicBoolean blockWatermark = new AtomicBoolean(true);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            mgr.registerExplicitTransactionState(txnId, db1.getId());
            DatabaseTransactionMgr dbTxnMgr = mgr.getDatabaseTransactionMgr(db1.getId());
            dbTxnMgr.activateTransactionTable(txnId, table1.getId());
            new MockUp<TransactionState>() {
                @Mock
                public List<Long> getTableIdList(Invocation invocation) {
                    TransactionState invokedState = invocation.getInvokedInstance();
                    if (invokedState == state && blockWatermark.compareAndSet(true, false)) {
                        watermarkEntered.countDown();
                        awaitLatch(releaseWatermark, "watermark was not released");
                    }
                    return invocation.proceed();
                }
            };

            Future<Boolean> watermark = executor.submit(() -> dbTxnMgr.isPreviousTransactionsFinished(
                    txnId, List.of(secondTableId), Set.of()));
            Assertions.assertTrue(watermarkEntered.await(10, TimeUnit.SECONDS));
            Future<TransactionState> activation = executor.submit(() -> {
                activationStarted.countDown();
                return dbTxnMgr.activateTransactionTable(txnId, secondTableId);
            });
            Assertions.assertTrue(activationStarted.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(activation.isDone());

            releaseWatermark.countDown();
            Assertions.assertTrue(watermark.get(10, TimeUnit.SECONDS));
            Assertions.assertSame(state, activation.get(10, TimeUnit.SECONDS));
            Assertions.assertFalse(dbTxnMgr.isPreviousTransactionsFinished(
                    txnId, List.of(secondTableId), Set.of()));
        } finally {
            releaseWatermark.countDown();
            executor.shutdownNow();
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testReshardWatermarkSerializesRegistration() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "serialize-registration", 60_000L);
        CountDownLatch watermarkEntered = new CountDownLatch(1);
        CountDownLatch releaseWatermark = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            mgr.reserveExplicitTransactionLayout(txnId, db1.getId(), table1.getId());
            new MockUp<DatabaseTransactionMgr>() {
                @Mock
                public boolean isPreviousTransactionsFinished(long endTransactionId, List<Long> tableIds,
                        Set<Long> excludeTransactionIds) throws InterruptedException {
                    watermarkEntered.countDown();
                    Assertions.assertTrue(releaseWatermark.await(10, TimeUnit.SECONDS));
                    return true;
                }
            };

            Future<Boolean> watermark = executor.submit(() -> mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
            Assertions.assertTrue(watermarkEntered.await(10, TimeUnit.SECONDS));
            Future<TransactionState> registration = executor.submit(
                    () -> mgr.registerExplicitTransactionState(txnId, db1.getId()));
            Assertions.assertFalse(registration.isDone());
            releaseWatermark.countDown();
            Assertions.assertFalse(watermark.get(10, TimeUnit.SECONDS));
            Assertions.assertSame(state, registration.get(10, TimeUnit.SECONDS));
        } finally {
            releaseWatermark.countDown();
            executor.shutdownNow();
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testReshardWatermarkSerializesExplicitRemoval() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "serialize-removal", 60_000L);
        CountDownLatch watermarkEntered = new CountDownLatch(1);
        CountDownLatch releaseWatermark = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            mgr.reserveExplicitTransactionLayout(txnId, db1.getId(), table1.getId());
            new MockUp<DatabaseTransactionMgr>() {
                @Mock
                public boolean isPreviousTransactionsFinished(long endTransactionId, List<Long> tableIds,
                        Set<Long> excludeTransactionIds) throws InterruptedException {
                    watermarkEntered.countDown();
                    Assertions.assertTrue(releaseWatermark.await(10, TimeUnit.SECONDS));
                    return true;
                }
            };

            Future<Boolean> watermark = executor.submit(() -> mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
            Assertions.assertTrue(watermarkEntered.await(10, TimeUnit.SECONDS));
            Future<?> removal = executor.submit(() -> mgr.clearExplicitTxnState(txnId));
            Assertions.assertFalse(removal.isDone());
            releaseWatermark.countDown();
            Assertions.assertFalse(watermark.get(10, TimeUnit.SECONDS));
            removal.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
        } finally {
            releaseWatermark.countDown();
            executor.shutdownNow();
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testRegistrationSerializesReshardWatermark() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "registration-serializes-watermark", 60_000L);
        CountDownLatch upsertEntered = new CountDownLatch(1);
        CountDownLatch releaseUpsert = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            mgr.reserveExplicitTransactionLayout(txnId, db1.getId(), table1.getId());
            new MockUp<DatabaseTransactionMgr>() {
                @Mock
                public void upsertTransactionState(Invocation invocation, TransactionState transactionState)
                        throws Exception {
                    upsertEntered.countDown();
                    Assertions.assertTrue(releaseUpsert.await(10, TimeUnit.SECONDS));
                    invocation.proceed(transactionState);
                }
            };

            Future<TransactionState> registration = executor.submit(
                    () -> mgr.registerExplicitTransactionState(txnId, db1.getId()));
            Assertions.assertTrue(upsertEntered.await(10, TimeUnit.SECONDS));
            Future<Boolean> watermark = executor.submit(() -> mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
            Assertions.assertFalse(watermark.isDone());
            releaseUpsert.countDown();
            Assertions.assertSame(state, registration.get(10, TimeUnit.SECONDS));
            Assertions.assertFalse(watermark.get(10, TimeUnit.SECONDS));
        } finally {
            releaseUpsert.countDown();
            executor.shutdownNow();
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testClearReservedExplicitStateReleasesReshardWatermark() throws Exception {
        GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        abortRunningTransactions(mgr, db1.getId());
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db1.getFullName(), "tbl1");
        long txnId = mgr.getTransactionIDGenerator().getNextTransactionId();
        TransactionState state = addExplicitState(mgr, txnId, "clear-reserved-state", 60_000L);
        try {
            mgr.reserveExplicitTransactionLayout(txnId, db1.getId(), table1.getId());
            Assertions.assertFalse(mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
            mgr.clearExplicitTxnState(txnId);
            Assertions.assertTrue(mgr.isPreviousTransactionsFinishedForReshard(
                    txnId, db1.getId(), List.of(table1.getId()), Set.of()));
        } finally {
            cleanupExplicitState(mgr, state);
        }
    }

    @Test
    public void testAbortTimeoutTxnsCleanupExplicitTxnState() throws Exception {
        // Test that abortTimeoutTxns() cleans up timed-out explicit transaction states
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        abortRunningTransactions(globalTransactionMgr, db1.getId());

        // Create a transaction state with a very short timeout (already expired)
        long transactionId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionIDGenerator().getNextTransactionId();
        TransactionState transactionState = new TransactionState(transactionId, "timeout_test_label", null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                        FrontendOptions.getLocalHostAddress()),
                1L); // 1ms timeout
        transactionState.setPrepareTime(System.currentTimeMillis() - 10000); // Started 10 seconds ago

        ExplicitTxnState explicitTxnState = new ExplicitTxnState();
        explicitTxnState.setTransactionState(transactionState);
        globalTransactionMgr.addTransactionState(transactionId, explicitTxnState);
        try {
            Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db1.getFullName(), "tbl1");
            globalTransactionMgr.reserveExplicitTransactionLayout(transactionId, db1.getId(), table1.getId());

            Assertions.assertNotNull(globalTransactionMgr.getExplicitTxnState(transactionId));
            Assertions.assertFalse(globalTransactionMgr.isPreviousTransactionsFinishedForReshard(
                    transactionId, db1.getId(), List.of(table1.getId()), Set.of()));

            // Run timeout cleanup
            globalTransactionMgr.abortTimeoutTxns();

            // Verify the timed-out state was cleaned up
            Assertions.assertNull(globalTransactionMgr.getExplicitTxnState(transactionId));
            Assertions.assertTrue(globalTransactionMgr.isPreviousTransactionsFinishedForReshard(
                    transactionId, db1.getId(), List.of(table1.getId()), Set.of()));
        } finally {
            cleanupExplicitState(globalTransactionMgr, transactionState);
        }
    }

    @Test
    public void testAbortTimeoutTxnsCleanupOrphanedNullState() {
        // Test that abortTimeoutTxns() also cleans up entries where transactionState is null
        // (orphaned entries from lost state, e.g., after FE leader switch)
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        long orphanTxnId = globalTransactionMgr.getTransactionIDGenerator().getNextTransactionId();
        ExplicitTxnState orphanState = new ExplicitTxnState();
        // transactionState is null by default - simulates orphaned entry
        globalTransactionMgr.addTransactionState(orphanTxnId, orphanState);

        Assertions.assertNotNull(globalTransactionMgr.getExplicitTxnState(orphanTxnId));

        // Run timeout cleanup
        globalTransactionMgr.abortTimeoutTxns();

        // Verify the orphaned null-state entry was cleaned up
        Assertions.assertNull(globalTransactionMgr.getExplicitTxnState(orphanTxnId));
    }

    @Test
    public void testBeginWithStaleExplicitTxnStateClearsEntry() {
        // Test that beginStmt clears the stale map entry when explicitTxnState exists
        // but transactionState is null
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        TUniqueId queryId = new TUniqueId(960, 961);
        context.setExecutionId(queryId);

        // Add a stale entry with null transactionState
        long staleTxnId = globalTransactionMgr.getTransactionIDGenerator().getNextTransactionId();
        ExplicitTxnState staleState = new ExplicitTxnState();
        globalTransactionMgr.addTransactionState(staleTxnId, staleState);
        context.setTxnId(staleTxnId);

        // beginStmt should detect the lost state, clean up stale entry, and start fresh
        TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, "stale_cleanup_label"));

        // Stale entry should be removed from the map
        Assertions.assertNull(globalTransactionMgr.getExplicitTxnState(staleTxnId));
        // A new transaction should have been started
        Assertions.assertNotEquals(0, context.getTxnId());
        Assertions.assertNotEquals(staleTxnId, context.getTxnId());
        Assertions.assertFalse(context.getState().isError());

        // Cleanup
        globalTransactionMgr.clearExplicitTxnState(context.getTxnId());
        context.setTxnId(0);
    }

    @Test
    public void testBeginWithLabelAlreadyUsedByAnotherSession() {
        // BEGIN WITH LABEL must be rejected when another session already holds an explicit transaction
        // with the same label
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        ConnectContext first = new ConnectContext();
        first.setThreadLocalInfo();
        first.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        first.setExecutionId(new TUniqueId(970, 971));
        TransactionStmtExecutor.beginStmt(first, new BeginStmt(NodePosition.ZERO, "duplicated_label"));
        Assertions.assertFalse(first.getState().isError());
        Assertions.assertNotEquals(0, first.getTxnId());

        ConnectContext second = new ConnectContext();
        second.setThreadLocalInfo();
        second.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        second.setExecutionId(new TUniqueId(972, 973));
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> TransactionStmtExecutor.beginStmt(second, new BeginStmt(NodePosition.ZERO, "duplicated_label")));
        Assertions.assertTrue(e.getMessage().contains("has already been used"), e.getMessage());
        // The rejected session must not be left inside a transaction
        Assertions.assertEquals(0, second.getTxnId());

        // Cleanup
        globalTransactionMgr.clearExplicitTxnState(first.getTxnId());
        first.setTxnId(0);
    }

    @Test
    public void testConcurrentBeginWithSameLabel() throws Exception {
        // Regression test for the label uniqueness race: several sessions running
        // `BEGIN WITH LABEL <same label>` at the same time must not all succeed. The uniqueness check and the
        // registration of the explicit transaction state have to happen atomically, otherwise every session
        // passes the check before any of them publishes its state.
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        final int concurrency = 8;
        final int rounds = 5;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        try {
            for (int round = 0; round < rounds; round++) {
                final String label = "concurrent_label_" + round;
                CyclicBarrier barrier = new CyclicBarrier(concurrency);
                AtomicInteger succeeded = new AtomicInteger();
                AtomicInteger rejected = new AtomicInteger();
                Queue<Long> succeededTxnIds = new ConcurrentLinkedQueue<>();

                List<Future<?>> futures = new ArrayList<>(concurrency);
                for (int i = 0; i < concurrency; i++) {
                    final int seq = i;
                    futures.add(executor.submit(() -> {
                        ConnectContext context = new ConnectContext();
                        context.setThreadLocalInfo();
                        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
                        context.setExecutionId(new TUniqueId(1000 + seq, 2000 + seq));

                        // Line up all sessions so that they hit the label check together
                        barrier.await(30, TimeUnit.SECONDS);
                        try {
                            TransactionStmtExecutor.beginStmt(context, new BeginStmt(NodePosition.ZERO, label));
                            succeeded.incrementAndGet();
                            succeededTxnIds.add(context.getTxnId());
                        } catch (SemanticException e) {
                            Assertions.assertTrue(e.getMessage().contains("has already been used"), e.getMessage());
                            rejected.incrementAndGet();
                        }
                        return null;
                    }));
                }
                for (Future<?> future : futures) {
                    future.get(60, TimeUnit.SECONDS);
                }

                Assertions.assertEquals(1, succeeded.get(),
                        "exactly one BEGIN WITH LABEL " + label + " should succeed");
                Assertions.assertEquals(concurrency - 1, rejected.get());

                for (Long txnId : succeededTxnIds) {
                    globalTransactionMgr.clearExplicitTxnState(txnId);
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
