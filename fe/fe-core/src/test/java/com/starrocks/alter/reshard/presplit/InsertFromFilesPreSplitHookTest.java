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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StatementBase.ExplainLevel;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRelation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * Detection-side coverage for {@link InsertFromFilesPreSplitHook}. Each test
 * drives a different early-return branch and asserts the hook never reaches
 * {@link TabletPreSplitCoordinator#submitAsynchronously}. The eligible
 * delegation path needs a full FE fixture (catalog, partitions, tablet
 * inverted index, ConnectContext-bound compute resource) and is left to
 * integration coverage.
 */
public class InsertFromFilesPreSplitHookTest {

    private boolean savedConfigInsertFromFiles;

    @BeforeEach
    public void setUp() {
        savedConfigInsertFromFiles = Config.enable_tablet_pre_split_for_insert_from_files;
        Config.enable_tablet_pre_split_for_insert_from_files = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_tablet_pre_split_for_insert_from_files = savedConfigInsertFromFiles;
    }

    @Test
    public void testConfigFlagOffShortCircuits() throws Exception {
        Config.enable_tablet_pre_split_for_insert_from_files = false;
        InsertStmt stmt = simpleFilesInsertStmt();

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    @Test
    public void testNonInsertStatementShortCircuits() throws Exception {
        StatementBase stmt = mock(StatementBase.class);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    @Test
    public void testReadOnlyExplainShortCircuits() throws Exception {
        // EXPLAIN INSERT INTO t SELECT ... FROM FILES(...) — must not mutate tablet metadata.
        InsertStmt stmt = simpleFilesInsertStmt();
        when(stmt.isExplain()).thenReturn(true);
        when(stmt.getExplainLevel()).thenReturn(ExplainLevel.NORMAL);

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    @Test
    public void testInsertOverwriteFirstPassShortCircuits() throws Exception {
        // INSERT OVERWRITE's first planning pass has no overwrite-job yet; the
        // overwrite handler will re-plan with one. Submitting now would race
        // the overwrite's own table-state changes.
        InsertStmt stmt = simpleFilesInsertStmt();
        when(stmt.isOverwrite()).thenReturn(true);
        when(stmt.hasOverwriteJob()).thenReturn(false);

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    @Test
    public void testExplicitSessionTransactionShortCircuits() throws Exception {
        InsertStmt stmt = simpleFilesInsertStmt();
        ConnectContext context = mock(ConnectContext.class);
        when(context.getTxnId()).thenReturn(42L);

        assertHookDoesNotDelegate(() -> InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, context));
    }

    @Test
    public void testPreSetStmtTxnIdShortCircuits() throws Exception {
        // InsertOverwriteJobRunner re-plans with stmt.txnId already set; the hook
        // must not double-submit pre-split for that path.
        InsertStmt stmt = simpleFilesInsertStmt();
        when(stmt.getTxnId()).thenReturn(99L);

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    @Test
    public void testNullQueryStatementShortCircuits() throws Exception {
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTxnId()).thenReturn(DmlStmt.INVALID_TXN_ID);
        when(stmt.getQueryStatement()).thenReturn(null);

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    @Test
    public void testCteInQueryRelationShortCircuits() throws Exception {
        // INSERT INTO t WITH cte AS (...) SELECT * FROM FILES(...) — CTE means
        // more than one source could feed the insert; reject.
        SelectRelation selectRelation = mock(SelectRelation.class);
        when(selectRelation.getCteRelations()).thenReturn(List.of(mock(CTERelation.class)));

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    @Test
    public void testSetOperationQueryRelationShortCircuits() throws Exception {
        // INSERT INTO t SELECT ... UNION ALL SELECT ... — non-SelectRelation queryRelation.
        SetOperationRelation setOp = mock(SetOperationRelation.class);
        when(setOp.getCteRelations()).thenReturn(List.of());
        InsertStmt stmt = insertStmtWithQueryRelation(setOp);

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    @Test
    public void testNonFilesFromShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM other_olap_table — FROM is a plain TableRelation,
        // not FileTableFunctionRelation. Out of scope for pre-split.
        SelectRelation selectRelation = mock(SelectRelation.class);
        when(selectRelation.getCteRelations()).thenReturn(List.of());
        when(selectRelation.getRelation()).thenReturn(mock(TableRelation.class));

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    @Test
    public void testTableRefNormalizationFailureShortCircuits() throws Exception {
        // AnalyzerUtils.normalizedTableRef throwing SemanticException is the
        // documented failure mode when the session has no current db; the hook
        // must catch the throw and no-op rather than abort the planning thread.
        InsertStmt stmt = simpleFilesInsertStmt();
        TableRef tableRef = mock(TableRef.class);
        when(stmt.getTableRef()).thenReturn(tableRef);

        assertHookDoesNotDelegate(() -> {
            try (MockedStatic<AnalyzerUtils> analyzer = Mockito.mockStatic(AnalyzerUtils.class)) {
                analyzer.when(() -> AnalyzerUtils.normalizedTableRef(eq(tableRef), any()))
                        .thenThrow(new SemanticException("simulated: no current database"));
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class));
            }
        });
    }

    @Test
    public void testInternalThrowIsSwallowed() throws Exception {
        // Drive the outer try/catch by passing an InsertStmt whose accessors
        // throw. The hook must not let the throw escape — it runs before
        // planning, so any escape would abort an otherwise-valid INSERT.
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTxnId()).thenThrow(new RuntimeException("simulated stmt failure"));

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mock(ConnectContext.class)));
    }

    /**
     * Wraps {@code invocation} with a {@code MockedStatic} so the test can
     * assert the hook never reached
     * {@link TabletPreSplitCoordinator#submitAsynchronously}. "No throw" alone
     * is too weak — the public hook always swallows throws.
     */
    private static void assertHookDoesNotDelegate(HookInvocation invocation) throws Exception {
        try (MockedStatic<TabletPreSplitCoordinator> coordinator =
                     Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            invocation.run();
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
        }
    }

    @FunctionalInterface
    private interface HookInvocation {
        void run() throws Exception;
    }

    private static InsertStmt simpleFilesInsertStmt() {
        // A minimal Insert-from-FILES shape so the Config-flag and txn checks
        // are the only meaningful gates. The selectRelation's getRelation()
        // returns a FileTableFunctionRelation but the hook never reaches that
        // line in these tests — the gate it's testing fires first.
        SelectRelation selectRelation = mock(SelectRelation.class);
        when(selectRelation.getCteRelations()).thenReturn(List.of());
        when(selectRelation.getRelation()).thenReturn(mock(FileTableFunctionRelation.class));
        return insertStmtWithQueryRelation(selectRelation);
    }

    private static InsertStmt insertStmtWithQueryRelation(QueryRelation queryRelation) {
        QueryStatement queryStatement = mock(QueryStatement.class);
        when(queryStatement.getQueryRelation()).thenReturn(queryRelation);
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTxnId()).thenReturn(DmlStmt.INVALID_TXN_ID);
        when(stmt.getQueryStatement()).thenReturn(queryStatement);
        return stmt;
    }
}
