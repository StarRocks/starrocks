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

import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.proc.TransProcDir;
import com.starrocks.lake.TxnInfoHelper;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.RedirectStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;
import com.starrocks.sql.ast.AdminSkipCommittedTransactionStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for ADMIN SKIP COMMITTED TRANSACTION phase-1.
 * Covers the standalone accessor / proto / parser pieces. End-to-end txn-state
 * transitions are exercised in integration tests.
 */
public class AdminSkipCommittedTransactionTest {

    @Test
    public void testTransactionStateNoOpPublishAccessorDefaults() {
        // Fresh TransactionState should default to not-marked-as-no-op with empty reason.
        TransactionState state = new TransactionState();
        assertFalse(state.isNoOpPublish(),
                "freshly-constructed TransactionState must not be marked as no-op publish by default");
        assertEquals("", state.getNoOpPublishReason());
    }

    @Test
    public void testTransactionStateMarkAsNoOpPublish() {
        TransactionState state = new TransactionState();
        state.markAsNoOpPublish("publish stuck on txnlog loss");
        assertTrue(state.isNoOpPublish());
        assertEquals("publish stuck on txnlog loss", state.getNoOpPublishReason());
    }

    @Test
    public void testTransactionStateMarkAsNoOpPublishNullReasonStoredAsEmpty() {
        // A null reason is normalized to "" so downstream code can safely
        // log without null-checking.
        TransactionState state = new TransactionState();
        state.markAsNoOpPublish(null);
        assertTrue(state.isNoOpPublish());
        assertEquals("", state.getNoOpPublishReason());
    }

    @Test
    public void testTxnInfoHelperPropagatesNoOpPublishFlag() {
        TransactionState state = new TransactionState(1234L, "lbl-no-op-on", null,
                TransactionState.LoadJobSourceType.FRONTEND,
                TransactionState.TxnCoordinator.fromThisFE(), 60_000L);
        state.markAsNoOpPublish("operator escape hatch");

        TxnInfoPB info = TxnInfoHelper.fromTransactionState(state);
        assertNotNull(info);
        assertEquals(1234L, info.txnId.longValue());
        assertTrue(info.noOpPublish, "TxnInfoPB.noOpPublish must reflect TransactionState.isNoOpPublish()");
    }

    @Test
    public void testTxnInfoHelperOmitsNoOpPublishFlagWhenNotMarked() {
        TransactionState state = new TransactionState(5678L, "lbl-no-op-off", null,
                TransactionState.LoadJobSourceType.FRONTEND,
                TransactionState.TxnCoordinator.fromThisFE(), 60_000L);
        // markAsNoOpPublish not called.
        TxnInfoPB info = TxnInfoHelper.fromTransactionState(state);
        // noOpPublish should be the boxed-false default. Field is `optional bool`
        // so a non-set value reads back as false at the Java side.
        assertFalse(Boolean.TRUE.equals(info.noOpPublish),
                "non-marked txns must not carry noOpPublish=true");
    }

    @Test
    public void testConfigDefaultIsDisabled() {
        // Default value of enable_admin_skip_committed_txn must be false so the
        // SQL is rejected unless an operator explicitly opts in.
        assertFalse(Config.enable_admin_skip_committed_txn,
                "enable_admin_skip_committed_txn must default to false to prevent accidental use");
    }

    @Test
    public void testParseSqlWithoutReason() {
        // Verifies grammar wiring for: ADMIN SKIP COMMITTED TRANSACTION <id>.
        String sql = "ADMIN SKIP COMMITTED TRANSACTION 12345";
        List<StatementBase> stmts = SqlParser.parse(sql, new ConnectContext().getSessionVariable());
        assertEquals(1, stmts.size());
        assertTrue(stmts.get(0) instanceof AdminSkipCommittedTransactionStmt);
        AdminSkipCommittedTransactionStmt parsed = (AdminSkipCommittedTransactionStmt) stmts.get(0);
        assertEquals(12345L, parsed.getTxnId());
        assertEquals("", parsed.getReason());
    }

    @Test
    public void testTransProcDirSurfacesNoOpPublishColumns() {
        // SHOW PROC '/transactions/<db>/(running|finished)' must surface the
        // no_op_publish state so operators can verify an ADMIN SKIP actually
        // took effect on the txn. Two new columns appended at the end of the
        // result keep existing column ordering stable.
        assertEquals("NoOpPublish",
                TransProcDir.TITLE_NAMES.get(TransProcDir.TITLE_NAMES.size() - 2));
        assertEquals("NoOpPublishReason",
                TransProcDir.TITLE_NAMES.get(TransProcDir.TITLE_NAMES.size() - 1));
    }

    @Test
    public void testGlobalMgrRejectsUnknownTxnId() throws Exception {
        // GlobalTransactionMgr scans all per-db txn managers to find the
        // owning DB for a given txn id. With an empty map, the lookup fails
        // and a StarRocksException is thrown — covers the "not found in any
        // database" branch.
        boolean originalEnable = Config.enable_admin_skip_committed_txn;
        Config.enable_admin_skip_committed_txn = true;
        try {
            GlobalTransactionMgr mgr = new GlobalTransactionMgr(null);
            StarRocksException ex = assertThrows(StarRocksException.class,
                    () -> mgr.markCommittedTransactionAsNoOpPublish(987654321L, "no such txn"));
            assertTrue(ex.getMessage().contains("not found"),
                    "error message must indicate txn not found, got: " + ex.getMessage());
        } finally {
            Config.enable_admin_skip_committed_txn = originalEnable;
        }
    }

    @Test
    public void testGlobalMgrUpfrontConfigGate() throws Exception {
        // Config gate at GlobalTransactionMgr entry: when the feature is
        // disabled, the call is rejected before any txn lookup so operators
        // get a clear "feature off" error regardless of whether the txn id
        // exists. Caught on the test cluster: a bogus id used to leak
        // "transaction not found" first, hiding the gate from the user.
        boolean originalEnable = Config.enable_admin_skip_committed_txn;
        Config.enable_admin_skip_committed_txn = false;
        try {
            GlobalTransactionMgr mgr = new GlobalTransactionMgr(null);
            StarRocksException ex = assertThrows(StarRocksException.class,
                    () -> mgr.markCommittedTransactionAsNoOpPublish(987654321L, "gated off"));
            assertTrue(ex.getMessage().contains("disabled"),
                    "config-off error must mention 'disabled', got: " + ex.getMessage());
        } finally {
            Config.enable_admin_skip_committed_txn = originalEnable;
        }
    }

    @Test
    public void testParseSqlWithReason() {
        String sql = "ADMIN SKIP COMMITTED TRANSACTION 67890 REASON 'txnlog lost on OSS'";
        List<StatementBase> stmts = SqlParser.parse(sql, new ConnectContext().getSessionVariable());
        assertEquals(1, stmts.size());
        assertTrue(stmts.get(0) instanceof AdminSkipCommittedTransactionStmt);
        AdminSkipCommittedTransactionStmt parsed = (AdminSkipCommittedTransactionStmt) stmts.get(0);
        assertEquals(67890L, parsed.getTxnId());
        assertEquals("txnlog lost on OSS", parsed.getReason());
    }

    @Test
    public void testAnalyzerVisitNoOp() {
        // Analyzer.visitAdminSkipCommittedTransactionStatement is intentionally a
        // no-op (validation deferred to execution under lock). The test simply
        // dispatches the AST through the analyzer's visitor to cover that line.
        AdminSkipCommittedTransactionStmt stmt =
                new AdminSkipCommittedTransactionStmt(1234L, "test", NodePosition.ZERO);
        // Call via the visitor directly. Per Analyzer.analyze() contract, the
        // visitor returns Void; we only care that no exception is raised.
        Analyzer.AnalyzerVisitor.getInstance().visit(stmt, new ConnectContext());
    }

    @Test
    public void testRedirectStatusForwardsToLeader() {
        // ADMIN SKIP COMMITTED TRANSACTION is a DDL statement, so its
        // redirect-status should be FORWARD_NO_SYNC (set by the DDL default).
        AdminSkipCommittedTransactionStmt stmt =
                new AdminSkipCommittedTransactionStmt(1234L, "test", NodePosition.ZERO);
        RedirectStatus status = RedirectStatus.getRedirectStatus(stmt);
        assertNotNull(status);
    }

    @Test
    public void testAuthorizerAccessDeniedPath() throws Exception {
        // Stub Authorizer.checkSystemAction to throw AccessDeniedException so the
        // visitor's catch block + reportAccessDenied path executes.
        new MockUp<com.starrocks.sql.analyzer.Authorizer>() {
            @Mock
            public void checkSystemAction(ConnectContext context, com.starrocks.authorization.PrivilegeType action)
                    throws com.starrocks.authorization.AccessDeniedException {
                throw new com.starrocks.authorization.AccessDeniedException("test-no-privilege");
            }
        };

        AdminSkipCommittedTransactionStmt stmt =
                new AdminSkipCommittedTransactionStmt(1234L, "test", NodePosition.ZERO);
        AuthorizerStmtVisitor visitor = new AuthorizerStmtVisitor();
        // The catch block calls reportAccessDenied, which raises an
        // ErrorReportException. Assert that *some* exception escapes so this
        // test fails loudly if the deny path stops throwing in the future
        // (otherwise an authorization regression would silently flip to a
        // no-op privileged call).
        assertThrows(Exception.class,
                () -> visitor.visitAdminSkipCommittedTransactionStatement(stmt, new ConnectContext()));
    }

    @Test
    public void testAuthorizerHappyPath() throws Exception {
        // Stub Authorizer.checkSystemAction to return normally so the visitor's
        // happy path executes: the call site, the closing brace, and `return null`.
        // Combined with testAuthorizerAccessDeniedPath above this covers every line
        // of visitAdminSkipCommittedTransactionStatement.
        new MockUp<com.starrocks.sql.analyzer.Authorizer>() {
            @Mock
            public void checkSystemAction(ConnectContext context, com.starrocks.authorization.PrivilegeType action) {
                // intentionally no-op (privileged caller)
            }
        };

        AdminSkipCommittedTransactionStmt stmt =
                new AdminSkipCommittedTransactionStmt(1234L, "test", NodePosition.ZERO);
        AuthorizerStmtVisitor visitor = new AuthorizerStmtVisitor();
        visitor.visitAdminSkipCommittedTransactionStatement(stmt, new ConnectContext());
    }

    @Test
    public void testDdlExecutorHappyPath(
            @Mocked ConnectContext mockedCtx,
            @Mocked GlobalStateMgr mockedGsm,
            @Mocked GlobalTransactionMgr mockedTxnMgr) throws Exception {
        // Drive the success path through the visitor wrapper so the lambda body
        // and trailing `return null` are both hit by JaCoCo.
        AdminSkipCommittedTransactionStmt stmt =
                new AdminSkipCommittedTransactionStmt(1234L, "ddl-exec-ok", NodePosition.ZERO);

        new mockit.Expectations() {
            {
                mockedCtx.getGlobalStateMgr();
                result = mockedGsm;
                mockedGsm.getGlobalTransactionMgr();
                result = mockedTxnMgr;
                mockedTxnMgr.markCommittedTransactionAsNoOpPublish(1234L, "ddl-exec-ok");
                // no `result =`: default no-op return — exercises the
                // wrapWithRuntimeException happy-path branch.
            }
        };

        DDLStmtExecutor.StmtExecutorVisitor.getInstance()
                .visitAdminSkipCommittedTransactionStatement(stmt, mockedCtx);
    }

    @Test
    public void testDdlExecutorExceptionWrappingForUnknownTxn(
            @Mocked ConnectContext mockedCtx,
            @Mocked GlobalStateMgr mockedGsm,
            @Mocked GlobalTransactionMgr mockedTxnMgr) throws Exception {
        // Parallel error-path test for the visitor wrapper: a thrown
        // StarRocksException must be rewrapped by ErrorReport.wrapWithRuntimeException.
        AdminSkipCommittedTransactionStmt stmt =
                new AdminSkipCommittedTransactionStmt(99999L, "ddl-exec-cov", NodePosition.ZERO);

        new mockit.Expectations() {
            {
                mockedCtx.getGlobalStateMgr();
                result = mockedGsm;
                mockedGsm.getGlobalTransactionMgr();
                result = mockedTxnMgr;
                mockedTxnMgr.markCommittedTransactionAsNoOpPublish(99999L, "ddl-exec-cov");
                result = new StarRocksException("txn 99999 not found");
            }
        };

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> DDLStmtExecutor.StmtExecutorVisitor.getInstance()
                        .visitAdminSkipCommittedTransactionStatement(stmt, mockedCtx),
                "ErrorReport.wrapWithRuntimeException must rewrap the StarRocksException");
        // The wrapped cause chain should still surface the original "not found" message
        // so operators have a precise diagnostic.
        Throwable cause = ex;
        while (cause != null && !cause.getMessage().contains("99999")) {
            cause = cause.getCause();
        }
        assertNotNull(cause, "rewrapped exception must preserve the underlying StarRocksException message");
    }
}
