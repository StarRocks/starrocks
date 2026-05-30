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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
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

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.assertHookDoesNotDelegate;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.mockConnectContextWithSessionPreSplit;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testSessionOptOutShortCircuits() throws Exception {
        // SET enable_tablet_pre_split=false on the session must short-circuit
        // after the cheap AST-shape filters (cost: zero) but before FILES
        // schema resolution + target resolution (cost: real), AND record the
        // eligibility-skip counter under disabled_by_session — operators rely
        // on that bvar to observe per-session opt-outs.
        //
        // To reach the session check, the stmt must clear qualifyingInsertStmt
        // and extractSingleFilesSource. The fresh-txn check needs getTxnId()
        // == INVALID_TXN_ID (Mockito's default 0 would otherwise be treated
        // as a pre-set txn id).
        InsertStmt stmt = simpleFilesInsertStmt();
        when(stmt.getTxnId()).thenReturn(DmlStmt.INVALID_TXN_ID);

        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.DISABLED_BY_SESSION.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(false)));

            org.junit.jupiter.api.Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "session opt-out must bump the disabled_by_session bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void testNonInsertStatementShortCircuits() throws Exception {
        StatementBase stmt = mock(StatementBase.class);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testReadOnlyExplainShortCircuits() throws Exception {
        // EXPLAIN INSERT INTO t SELECT ... FROM FILES(...) — must not mutate tablet metadata.
        InsertStmt stmt = simpleFilesInsertStmt();
        when(stmt.isExplain()).thenReturn(true);
        when(stmt.getExplainLevel()).thenReturn(ExplainLevel.NORMAL);

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
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
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testExplicitSessionTransactionShortCircuits() throws Exception {
        InsertStmt stmt = simpleFilesInsertStmt();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
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
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testNullQueryStatementShortCircuits() throws Exception {
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTxnId()).thenReturn(DmlStmt.INVALID_TXN_ID);
        when(stmt.getQueryStatement()).thenReturn(null);

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testCteInQueryRelationShortCircuits() throws Exception {
        // INSERT INTO t WITH cte AS (...) SELECT * FROM FILES(...) — CTE means
        // more than one source could feed the insert; reject.
        SelectRelation selectRelation = mock(SelectRelation.class);
        when(selectRelation.getCteRelations()).thenReturn(List.of(mock(CTERelation.class)));

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testSetOperationQueryRelationShortCircuits() throws Exception {
        // INSERT INTO t SELECT ... UNION ALL SELECT ... — non-SelectRelation queryRelation.
        SetOperationRelation setOp = mock(SetOperationRelation.class);
        when(setOp.getCteRelations()).thenReturn(List.of());
        InsertStmt stmt = insertStmtWithQueryRelation(setOp);

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testNonFilesFromShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM other_olap_table — FROM is a plain TableRelation,
        // not FileTableFunctionRelation. Out of scope for pre-split.
        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(mock(TableRelation.class)));
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testTargetColumnListShortCircuits() throws Exception {
        // INSERT INTO t (a, b) SELECT * FROM FILES(...) — the explicit column
        // list reorders/subsets the target's columns; the sampler reads source
        // columns matching the target's sort-key names, so the sampled column
        // would mismatch what the load writes.
        InsertStmt stmt = simpleFilesInsertStmt();
        when(stmt.getTargetColumnNames()).thenReturn(List.of("a", "b"));

        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testExpressionProjectionShortCircuits() throws Exception {
        // INSERT INTO t SELECT col + 1 FROM FILES(...) — expression projection
        // changes the inserted values; the sampler reads source columns
        // verbatim and would observe different values than the load writes.
        SelectListItem exprItem = mock(SelectListItem.class);
        when(exprItem.isStar()).thenReturn(false);

        InsertStmt stmt = insertStmtWithQueryRelation(
                filesSelectRelationWithSelectList(selectListOf(exprItem)));
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testMultipleSelectItemsShortCircuits() throws Exception {
        // INSERT INTO t SELECT a, b FROM FILES(...) — even when each item is
        // a plain column, naming a subset of FILES' columns produces a
        // different inserted row shape than a bare `SELECT *`.
        SelectList twoColumnSelectList = selectListOf(mock(SelectListItem.class), mock(SelectListItem.class));

        InsertStmt stmt = insertStmtWithQueryRelation(filesSelectRelationWithSelectList(twoColumnSelectList));
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testQualifiedStarShortCircuits() throws Exception {
        // INSERT INTO t SELECT files.* FROM FILES(...) — qualified star may
        // expand to a different column-set than the sampler's verbatim FILES()
        // read once multi-source joins are introduced.
        SelectListItem qualifiedStar = mock(SelectListItem.class);
        when(qualifiedStar.isStar()).thenReturn(true);
        when(qualifiedStar.getTblName()).thenReturn(new TableName(null, "files"));

        InsertStmt stmt = insertStmtWithQueryRelation(
                filesSelectRelationWithSelectList(selectListOf(qualifiedStar)));
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testStarWithExcludeShortCircuits() throws Exception {
        // INSERT INTO t SELECT * EXCLUDE (col_x) FROM FILES(...) — the
        // exclusion drops a column from the inserted row, mismatching the
        // sampler's verbatim FILES() read.
        SelectListItem starExclude = mock(SelectListItem.class);
        when(starExclude.isStar()).thenReturn(true);
        when(starExclude.getExcludedColumns()).thenReturn(List.of("col_x"));

        InsertStmt stmt = insertStmtWithQueryRelation(
                filesSelectRelationWithSelectList(selectListOf(starExclude)));
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testDistinctShortCircuits() throws Exception {
        // INSERT INTO t SELECT DISTINCT * FROM FILES(...) — DISTINCT collapses
        // duplicate rows; the sampler does not, so the sampled row-set differs
        // from what the load writes.
        SelectListItem starItem = mock(SelectListItem.class);
        when(starItem.isStar()).thenReturn(true);
        SelectList distinctSelectList = selectListOf(starItem);
        when(distinctSelectList.isDistinct()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(filesSelectRelationWithSelectList(distinctSelectList));
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testWhereClauseShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM FILES(...) WHERE x > 10 — the sampler
        // ignores the predicate, so it would observe rows the load filters out.
        SelectRelation selectRelation = bareStarSelectRelationOver(mock(FileTableFunctionRelation.class));
        when(selectRelation.hasWhereClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testGroupByClauseShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM FILES(...) GROUP BY ... — grouping
        // changes the row count; the sampler would observe ungrouped rows.
        SelectRelation selectRelation = bareStarSelectRelationOver(mock(FileTableFunctionRelation.class));
        when(selectRelation.hasGroupByClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testHavingClauseShortCircuits() throws Exception {
        // HAVING without GROUP BY is rare but parser-legal; the sampler
        // ignores it, so the row-set diverges.
        SelectRelation selectRelation = bareStarSelectRelationOver(mock(FileTableFunctionRelation.class));
        when(selectRelation.hasHavingClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testOrderByClauseShortCircuits() throws Exception {
        // ORDER BY combined with LIMIT picks a deterministic subset; ORDER BY
        // alone does not change the row-set but pairs with LIMIT, so we reject
        // either to keep the bare-`SELECT *` invariant simple.
        SelectRelation selectRelation = bareStarSelectRelationOver(mock(FileTableFunctionRelation.class));
        when(selectRelation.hasOrderByClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testLimitClauseShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM FILES(...) LIMIT 100 — LIMIT caps the
        // load row count; the sampler ignores it and would sample beyond the
        // cap, biasing the boundaries toward unwritten data.
        SelectRelation selectRelation = bareStarSelectRelationOver(mock(FileTableFunctionRelation.class));
        when(selectRelation.hasLimit()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
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
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true));
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
                InsertFromFilesPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testSchemasAlignWhenByPositionNamesMatch() {
        InsertStmt stmt = byPositionInsertStmt();
        OlapTable target = olapTableWithColumns("k", "v");
        TableFunctionTable source = tableFunctionTableWithColumns("k", "v");

        assertTrue(InsertFromFilesPreSplitHook.schemasAlignForByPositionInsert(stmt, target, source));
    }

    @Test
    public void testSchemasMisalignedWhenByPositionNamesDifferAtOrdinal() {
        // Target is (k, v) but FILES is (v, k). The load writes file column v
        // into target column k while the sampler reads file column k by name.
        InsertStmt stmt = byPositionInsertStmt();
        OlapTable target = olapTableWithColumns("k", "v");
        TableFunctionTable source = tableFunctionTableWithColumns("v", "k");

        assertFalse(InsertFromFilesPreSplitHook.schemasAlignForByPositionInsert(stmt, target, source));
    }

    @Test
    public void testSchemasMisalignedWhenArityDiffers() {
        InsertStmt stmt = byPositionInsertStmt();
        OlapTable target = olapTableWithColumns("k", "v");
        TableFunctionTable source = tableFunctionTableWithColumns("k", "v", "extra");

        assertFalse(InsertFromFilesPreSplitHook.schemasAlignForByPositionInsert(stmt, target, source));
    }

    @Test
    public void testSchemasAlignWhenByNameMappingEvenWithReorderedSource() {
        // By-name mapping pairs target column k with FILES column k regardless of
        // position, so the by-name sampler read matches what the load writes.
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.isColumnMatchByName()).thenReturn(true);
        OlapTable target = olapTableWithColumns("k", "v");
        TableFunctionTable source = tableFunctionTableWithColumns("v", "k");

        assertTrue(InsertFromFilesPreSplitHook.schemasAlignForByPositionInsert(stmt, target, source));
    }

    private static InsertStmt byPositionInsertStmt() {
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.isColumnMatchByName()).thenReturn(false);
        return stmt;
    }

    private static OlapTable olapTableWithColumns(String... columnNames) {
        // Resolve inner column mocks first; Mockito's per-thread stubbing state
        // does not allow nested mock()/when() inside another when() argument.
        List<Column> columns = columnsNamed(columnNames);
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseSchemaWithoutGeneratedColumn()).thenReturn(columns);
        return table;
    }

    private static TableFunctionTable tableFunctionTableWithColumns(String... columnNames) {
        List<Column> columns = columnsNamed(columnNames);
        TableFunctionTable table = mock(TableFunctionTable.class);
        when(table.getFullSchema()).thenReturn(columns);
        return table;
    }

    private static List<Column> columnsNamed(String... columnNames) {
        List<Column> columns = new ArrayList<>(columnNames.length);
        for (String name : columnNames) {
            Column column = mock(Column.class);
            when(column.getName()).thenReturn(name);
            columns.add(column);
        }
        return columns;
    }

    private static InsertStmt simpleFilesInsertStmt() {
        // A minimal `INSERT INTO t SELECT * FROM FILES(...)` shape that passes
        // every cheap pre-filter inside extractSingleFilesSource. Tests that
        // want to exercise a downstream branch (tableRef normalization, txn
        // checks) start from here.
        return insertStmtWithQueryRelation(bareStarSelectRelationOver(mock(FileTableFunctionRelation.class)));
    }

    private static SelectRelation bareStarSelectRelationOver(Relation from) {
        // Match the production AST invariant: excludedColumns is final and
        // always non-null. Without this stub Mockito returns null and the
        // production `!isEmpty()` check would NPE.
        SelectListItem starItem = mock(SelectListItem.class);
        when(starItem.isStar()).thenReturn(true);
        when(starItem.getExcludedColumns()).thenReturn(List.of());
        return filesSelectRelationWithSelectList(selectListOf(starItem), from);
    }

    private static SelectRelation filesSelectRelationWithSelectList(SelectList selectList) {
        return filesSelectRelationWithSelectList(selectList, mock(FileTableFunctionRelation.class));
    }

    private static SelectRelation filesSelectRelationWithSelectList(SelectList selectList, Relation from) {
        SelectRelation selectRelation = mock(SelectRelation.class);
        when(selectRelation.getCteRelations()).thenReturn(List.of());
        when(selectRelation.getRelation()).thenReturn(from);
        when(selectRelation.getSelectList()).thenReturn(selectList);
        return selectRelation;
    }

    private static SelectList selectListOf(SelectListItem... items) {
        SelectList selectList = mock(SelectList.class);
        when(selectList.getItems()).thenReturn(List.of(items));
        return selectList;
    }

    private static InsertStmt insertStmtWithQueryRelation(QueryRelation queryRelation) {
        QueryStatement queryStatement = mock(QueryStatement.class);
        when(queryStatement.getQueryRelation()).thenReturn(queryRelation);
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTxnId()).thenReturn(DmlStmt.INVALID_TXN_ID);
        when(stmt.getQueryStatement()).thenReturn(queryStatement);
        // Mockito's default for List<String> returns an empty list (not null);
        // explicitly stub null here so simpleFilesInsertStmt() actually clears
        // the "target column list present" pre-filter. Tests that want the
        // column-list-present branch override this with thenReturn(List.of(...)).
        when(stmt.getTargetColumnNames()).thenReturn(null);
        return stmt;
    }
}
