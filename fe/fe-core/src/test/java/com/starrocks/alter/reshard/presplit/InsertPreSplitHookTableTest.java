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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.mv.RowIdStrategy;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StatementBase.ExplainLevel;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.assertHookDoesNotDelegate;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.generatedColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.mockConnectContextWithSessionPreSplit;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.stubGeneratedSchema;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Detection-side and source coverage for the OLAP-table path of
 * {@link InsertPreSplitHook}. The detection tests drive each early-return branch
 * and assert the hook never reaches
 * {@link TabletPreSplitCoordinator#submitAsynchronously}. The source-content
 * tests exercise {@link TablePreSplitSource#prepare} directly and assert the
 * built {@link InsertFromTableScanContext} carries the resolved source columns /
 * WHERE predicate. The shared single/multi-partition routing now lives in
 * {@link PreSplitFlow} and is covered by {@link PreSplitFlowTest}. End-to-end
 * coverage (catalog, partitions, tablet inverted index, compute-resource-bound
 * ConnectContext) lives in the TSP regression suite.
 */
public class InsertPreSplitHookTableTest {

    private static final long BASE_INDEX_META_ID = 10L;

    private static final Column GEN_K = bigintColumn("k");
    private static final Column GEN_V = bigintColumn("v");
    private static final Column GEN_DT = new Column("dt", DateType.DATETIME, true);
    private static final Column GEN_DT_MONTH =
            generatedColumn("dt_month", DateType.DATETIME, "date_trunc('month', dt)", List.of(GEN_DT));
    // Captured before any fixture mocks GlobalStateMgr: analysis resolves built-in functions in the real registry, the
    // user's session variables are read by name through the real VariableMgr, and rendered literals are re-parsed with
    // the real parser.
    private static final com.starrocks.server.GlobalStateMgr REAL_GLOBAL_STATE =
            com.starrocks.server.GlobalStateMgr.getCurrentState();
    private static final Expr ABS_V = SqlParser.parseSqlToExpr("abs(v)", SqlModeHelper.MODE_DEFAULT);
    private static final Expr WHERE_UPPER_V_V = SqlParser.parseSqlToExpr("upper(v, v) = 'a'", SqlModeHelper.MODE_DEFAULT);
    private static final Expr WHERE_ON_GENERATED_DT_MONTH =
            SqlParser.parseSqlToExpr("date_trunc('month', dt_month) >= '2026-01-01 00:00:00'", SqlModeHelper.MODE_DEFAULT);
    // Folds to CAST(1.5 AS DECIMAL64(10,1)) in every sql_mode.
    private static final Expr WHERE_FOLDED_DECIMAL =
            SqlParser.parseSqlToExpr("v < CAST(concat('1', '.5') AS DECIMAL(10, 1))", SqlModeHelper.MODE_DEFAULT);

    private boolean savedConfigInsertFromTable;

    @BeforeEach
    public void setUp() {
        savedConfigInsertFromTable = Config.enable_tablet_pre_split_for_insert_from_table;
        Config.enable_tablet_pre_split_for_insert_from_table = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_tablet_pre_split_for_insert_from_table = savedConfigInsertFromTable;
    }

    // ---------- qualifyingInsertStmt pre-filters ----------

    @Test
    public void testConfigFlagOffShortCircuits() throws Exception {
        // Cluster-wide opt-out must short-circuit before the coordinator AND
        // record the eligibility-skip counter under disabled_by_config.
        Config.enable_tablet_pre_split_for_insert_from_table = false;
        InsertStmt stmt = simpleTableInsertStmt();

        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.DISABLED_BY_CONFIG.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));

            Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "config opt-out must bump the disabled_by_config bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void testNonInsertStatementShortCircuits() throws Exception {
        StatementBase stmt = mock(StatementBase.class);
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testReadOnlyExplainShortCircuits() throws Exception {
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.isExplain()).thenReturn(true);
        when(stmt.getExplainLevel()).thenReturn(ExplainLevel.NORMAL);

        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testInsertOverwriteFirstPassShortCircuits() throws Exception {
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.isOverwrite()).thenReturn(true);
        when(stmt.hasOverwriteJob()).thenReturn(false);

        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testExplicitSessionTransactionShortCircuits() throws Exception {
        InsertStmt stmt = simpleTableInsertStmt();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getTxnId()).thenReturn(42L);

        assertHookDoesNotDelegate(() -> InsertPreSplitHook.maybeRunPreSplit(stmt, context));
    }

    @Test
    public void testPreSetStmtTxnIdShortCircuits() throws Exception {
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.getTxnId()).thenReturn(99L);

        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testTargetPartitionSpecBuildsRestrictedTemporaryScope() {
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.isSpecifyPartitionNames()).thenReturn(true);
        when(stmt.getTargetPartitionNames()).thenReturn(
                new PartitionRef(List.of("p1_temp"), true, NodePosition.ZERO));

        PreSplitPartitionScope scope = PreSplitPartitionScope.fromInsert(stmt);

        Assertions.assertTrue(scope.isSpecified());
        Assertions.assertTrue(scope.isTemporary());
        Assertions.assertEquals("p1_temp", scope.mappedCatalogName("P1_TEMP"));
    }

    @Test
    public void testStaticKeyPartitionInsertShortCircuits() throws Exception {
        // INSERT INTO t PARTITION(k=1) SELECT ... is static key-partition syntax;
        // same concern: the load targets a fixed partition set.
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.isStaticKeyPartitionInsert()).thenReturn(true);

        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testShadowRewriteInsertRecordsNothing() throws Exception {
        // The online range rewrite copies a partition onto itself (INSERT INTO t PARTITION (p) SELECT ... FROM t
        // PARTITION (p)) while its job owns the target's tablets. Driven against an eligible table and against a view
        // (whose rewrite runs as a system statement): neither may reach the flow or record a skip.
        for (boolean viewTarget : List.of(false, true)) {
            try (SourceFixture fixture = sourceFixture();
                    MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
                if (viewTarget) {
                    fixture.withMaterializedViewTarget(null);
                    when(fixture.insertStmt.isSystem()).thenReturn(true);
                }
                fixture.restrictSourceTo(List.of("p1"));
                fixture.sourcePartition("p1", 1L, 1000L, 100L, null);
                when(fixture.insertStmt.isSpecifyPartitionNames()).thenReturn(true);
                when(fixture.insertStmt.getTargetPartitionNames())
                        .thenReturn(new PartitionRef(List.of("p1"), false, NodePosition.ZERO));
                when(fixture.insertStmt.isShadowRewrite()).thenReturn(true);

                long skips = recordedSkipsOfAnyReason(
                        () -> InsertPreSplitHook.maybeRunPreSplit(fixture.insertStmt, fixture.context));

                flow.verifyNoInteractions();
                Assertions.assertEquals(0L, skips, "viewTarget=" + viewTarget);
            }
        }
    }

    @Test
    public void testInsertWithLoadPropertiesDispatches() throws Exception {
        // A PROPERTIES(...) clause no longer disqualifies the statement. No INSERT load property can
        // move a row to a different tablet: the live ones (strict_mode, max_filter_ratio,
        // merge_condition) only remove rows, and the rest are shared load vocabulary an INSERT
        // ignores. So the sampled boundaries stay valid and the statement must reach the flow.
        // Driven with the mix the requirement asks for plus two keys the removed gate rejected
        // (partial_update, merge_condition) and a max_filter_ratio the analyzer will itself reject,
        // so re-adding any arm of that gate makes this fail.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            when(fixture.insertStmt.getProperties()).thenReturn(Map.of(
                    "strict_mode", "true", "max_filter_ratio", "2.0",
                    "partial_update", "true", "merge_condition", "v"));

            InsertPreSplitHook.maybeRunPreSplit(fixture.insertStmt, fixture.context);

            flow.verify(() -> PreSplitFlow.dispatch(
                    any(Database.class), eq(fixture.targetTable), any(PreSplitFlow.Prepared.class),
                    eq(LoadKind.INSERT_FROM_TABLE), any(), eq(fixture.context),
                    any(PreSplitPartitionScope.class)), times(1));
        }
    }

    @Test
    public void testNullQueryStatementShortCircuits() throws Exception {
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTxnId()).thenReturn(DmlStmt.INVALID_TXN_ID);
        when(stmt.getQueryStatement()).thenReturn(null);

        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testCteInQueryRelationShortCircuits() throws Exception {
        SelectRelation selectRelation = mock(SelectRelation.class);
        when(selectRelation.getCteRelations()).thenReturn(List.of(mock(CTERelation.class)));

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testSetOperationQueryRelationShortCircuits() throws Exception {
        // INSERT INTO t SELECT ... UNION ALL SELECT ... — non-SelectRelation queryRelation.
        SetOperationRelation setOp = mock(SetOperationRelation.class);
        when(setOp.getCteRelations()).thenReturn(List.of());
        InsertStmt stmt = insertStmtWithQueryRelation(setOp);

        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testJoinSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM a JOIN b — the FROM is not a single TableRelation.
        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(mock(Relation.class)));
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testFilesSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM FILES(...) — handled by the FILES hook, not this one.
        InsertStmt stmt = insertStmtWithQueryRelation(
                bareStarSelectRelationOver(mock(FileTableFunctionRelation.class)));
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testExpressionProjectionMatchesTableSourceShape() {
        // Expressions are accepted at the shape gate. prepare() later verifies
        // that every key needed by sampling is still a direct source-column ref.
        SelectListItem exprItem = mock(SelectListItem.class);
        when(exprItem.isStar()).thenReturn(false);
        when(exprItem.getExpr()).thenReturn(mock(FunctionCallExpr.class));

        SelectRelation selectRelation = selectRelationWithSelectList(
                selectListOf(exprItem), plainTableRelation());

        Assertions.assertTrue(new TablePreSplitSource().matches(mock(InsertStmt.class), selectRelation));
    }

    @Test
    public void testDistinctShortCircuits() throws Exception {
        SelectListItem starItem = mock(SelectListItem.class);
        when(starItem.isStar()).thenReturn(true);
        SelectList distinctSelectList = selectListOf(starItem);
        when(distinctSelectList.isDistinct()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(
                selectRelationWithSelectList(distinctSelectList, plainTableRelation()));
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testGroupByClauseShortCircuits() throws Exception {
        SelectRelation selectRelation = bareStarSelectRelationOver(plainTableRelation());
        when(selectRelation.hasGroupByClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testHavingClauseShortCircuits() throws Exception {
        SelectRelation selectRelation = bareStarSelectRelationOver(plainTableRelation());
        when(selectRelation.hasHavingClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testOrderByClauseShortCircuits() throws Exception {
        SelectRelation selectRelation = bareStarSelectRelationOver(plainTableRelation());
        when(selectRelation.hasOrderByClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testLimitClauseShortCircuits() throws Exception {
        SelectRelation selectRelation = bareStarSelectRelationOver(plainTableRelation());
        when(selectRelation.hasLimit()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    // ---------- isPlainTableReference: source-slice modifiers ----------

    @Test
    public void testTemporaryPartitionSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src TEMPORARY PARTITION(p1): the sampler cannot name a temporary partition.
        TableRelation sourceRelation = plainTableRelation();
        when(sourceRelation.getPartitionNames())
                .thenReturn(new PartitionRef(List.of("p1"), true, NodePosition.ZERO));

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testKeyPartitionSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src PARTITION(k=1): a key-valued slice, not a partition-name list.
        TableRelation sourceRelation = plainTableRelation();
        PartitionRef keyRef = mock(PartitionRef.class);
        when(keyRef.isTemp()).thenReturn(false);
        when(keyRef.isKeyPartitionNames()).thenReturn(true);
        when(keyRef.getPartitionNames()).thenReturn(List.of());
        when(sourceRelation.getPartitionNames()).thenReturn(keyRef);

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testEmptyPartitionListSourceDoesNotMatch() {
        // An empty list would render as PARTITION () in the sampler's own scan.
        try (SourceFixture fixture = sourceFixture()) {
            fixture.restrictSourceTo(List.of());

            Assertions.assertFalse(new TablePreSplitSource().matches(fixture.insertStmt,
                    (SelectRelation) fixture.insertStmt.getQueryStatement().getQueryRelation()));
        }
    }

    @Test
    public void testPartitionPredicateSourceShortCircuits() throws Exception {
        // A partition predicate restricts the scan to rows the sampler would not read.
        try (SourceFixture fixture = sourceFixture()) {
            TableRelation relation = (TableRelation) ((SelectRelation) fixture.insertStmt.getQueryStatement()
                    .getQueryRelation()).getRelation();
            SelectRelation selectRelation = (SelectRelation) fixture.insertStmt.getQueryStatement().getQueryRelation();
            Assertions.assertTrue(new TablePreSplitSource().matches(fixture.insertStmt, selectRelation));

            when(relation.getPartitionPredicate()).thenReturn(mock(Expr.class));

            Assertions.assertFalse(new TablePreSplitSource().matches(fixture.insertStmt, selectRelation));
        }
    }

    @Test
    public void testPartitionRestrictedSourceIsSampled() throws Exception {
        try (SourceFixture fixture = sourceFixture()) {
            fixture.restrictSourceTo(List.of("p1", "p2"));
            // p1 carries a rollup: Partition#getRowCount sums every visible index, so a wrong
            // implementation would report 100 + 40 + 300 here instead of the base-index rows.
            Partition p1 = fixture.sourcePartition("p1", 1L, 1000L, 100L, 40L);
            Partition p2 = fixture.sourcePartition("p2", 2L, 2000L, 300L, null);
            when(p1.getRowCount()).thenReturn(140L);
            when(p2.getRowCount()).thenReturn(300L);

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext);
            Assertions.assertEquals("`src_t`.`src_db`.`src_t` PARTITION (`p1`, `p2`)", scanContext.sourceFromSql());
            Assertions.assertEquals(3000L, scanContext.sourceTotalBytes());
            Assertions.assertEquals(400L, scanContext.sourceTotalRows());
        }
    }

    @Test
    public void testPartitionRestrictedSourceKeepsAliasAfterPartitionList() throws Exception {
        try (SourceFixture fixture = sourceFixture()) {
            fixture.restrictSourceTo(List.of("p`1"));
            fixture.sourcePartition("p`1", 1L, 1000L, 100L, null);
            TableRelation relation = (TableRelation) ((SelectRelation) fixture.insertStmt.getQueryStatement()
                    .getQueryRelation()).getRelation();
            TableName alias = mock(TableName.class);
            when(alias.getTbl()).thenReturn("s");
            when(relation.getAlias()).thenReturn(alias);

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext);
            Assertions.assertEquals("`src_t`.`src_db`.`src_t` PARTITION (`p``1`) `s`", scanContext.sourceFromSql());
        }
    }

    @Test
    public void testRepeatedSourcePartitionIsCountedOnce() throws Exception {
        try (SourceFixture fixture = sourceFixture()) {
            fixture.restrictSourceTo(List.of("p1", "p1"));
            fixture.sourcePartition("p1", 1L, 1000L, 100L, null);

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext);
            Assertions.assertEquals(1000L, scanContext.sourceTotalBytes());
            Assertions.assertEquals(100L, scanContext.sourceTotalRows());
        }
    }

    @Test
    public void testPartitionRestrictedExternalSourceIsDeclined() throws Exception {
        // The planner drops a partition list on a non-OLAP scan (a materialized view refresh gives one to a JDBC
        // base table), so the sampler would read the whole external table. Sized, so only the list can decline it.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            fixture.withExternalSource(connectorStatistics(1_000_000d, 8d, Statistics.StatsSource.TABLE_METADATA));
            fixture.restrictSourceTo(List.of("p1"));

            InsertPreSplitHook.maybeRunPreSplit(fixture.insertStmt, fixture.context);

            flow.verifyNoInteractions();
            verify(fixture.metadataMgr, never())
                    .getTableStatistics(any(), any(), any(), any(), any(), any(), anyLong(), any());
        }
    }

    @Test
    public void testTabletModifierOnSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src TABLET(123) — explicit tablet slice.
        TableRelation sourceRelation = plainTableRelation();
        when(sourceRelation.getTabletIds()).thenReturn(List.of(123L));

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testTableSampleModifierOnSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src TABLESAMPLE(...) — sampling slice.
        TableRelation sourceRelation = plainTableRelation();
        when(sourceRelation.getSampleClause())
                .thenReturn(mock(com.starrocks.sql.ast.TableSampleClause.class));

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testTemporalModifierOnSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src FOR VERSION AS OF ... — time-travel slice.
        TableRelation sourceRelation = plainTableRelation();
        when(sourceRelation.getQueryPeriod())
                .thenReturn(mock(com.starrocks.sql.ast.QueryPeriod.class));

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    // ---------- WHERE gate (downstream of source resolution) ----------

    @Test
    public void testNonDeterministicWhereRandShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src WHERE rand() < 0.5 — rand() resolves
        // differently in the ROOT sampling context than in the user's INSERT.
        assertNonDeterministicWhereSkips(new FunctionCallExpr("rand", List.of()));
    }

    @Test
    public void testNonDeterministicWhereNowShortCircuits() throws Exception {
        assertNonDeterministicWhereSkips(new FunctionCallExpr("now", List.of()));
    }

    @Test
    public void testNonDeterministicWhereCurrentUserShortCircuits() throws Exception {
        // current_user() parses as an InformationFunction node, not FunctionCallExpr.
        assertNonDeterministicWhereSkips(new InformationFunction("CURRENT_USER"));
    }

    // ---------- resolveEligibleTable: MV target ----------

    @Test
    public void testMaterializedViewTargetShortCircuits() throws Exception {
        // MaterializedView extends OlapTable, so resolveOlapTarget would accept it.
        // The hook must reject it before submitting any reshard job: user INSERT into
        // an MV is rejected by InsertAnalyzer, but the hook runs before the analyzer.
        // The view is otherwise fully eligible and has no row id -- the same view a system
        // refresh's static overwrite samples -- so only the route can decline it.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            fixture.withMaterializedViewTarget(null);

            long skips = recordedSkips(SkipReason.MATERIALIZED_VIEW_TARGET,
                    () -> InsertPreSplitHook.maybeRunPreSplit(fixture.insertStmt, fixture.context));

            flow.verifyNoInteractions();
            Assertions.assertEquals(1L, skips, "a user INSERT into a view must record materialized_view_target");
        }
    }

    @Test
    public void testMaterializedViewTargetRecordsItsSkipReason() {
        // The sampled sources cannot map an MV's hidden row-id sort key back to a source column, so
        // they must decline -- but declining silently left an operator unable to tell this apart from
        // the statement never having been a pre-split candidate at all.
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try (SourceFixture fixture = sourceFixture()) {
            MaterializedView mv = mock(MaterializedView.class);
            when(mv.getName()).thenReturn("mv_target");
            fixture.metaUtils.when(() -> MetaUtils.getSessionAwareTable(
                            any(), Mockito.argThat(db -> db != fixture.sourceDb), any()))
                    .thenReturn(mv);

            String label = SkipReason.MATERIALIZED_VIEW_TARGET.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue();

            fixture.assertNoSubmit();

            Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "an MV target on a sampled path must bump the materialized_view_target bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    // ---------- Static overwrite: MV target ----------

    @Test
    public void testSystemStaticOverwriteOfPctViewIsSampled() throws Exception {
        // A view without a row id is keyed by ordinary columns, and its refresh is a static
        // INSERT OVERWRITE the system issues, so it is sampled like a table.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            MaterializedView view = fixture.withMaterializedViewTarget(null);

            long skips = recordedSkips(SkipReason.MATERIALIZED_VIEW_TARGET, () -> fixture.runStaticOverwrite(true));

            flow.verify(() -> PreSplitFlow.runStaticOverwriteFlow(
                    any(Database.class), eq(view), any(PreSplitFlow.Prepared.class),
                    eq(LoadKind.INSERT_FROM_TABLE), any(), eq(fixture.context),
                    any(PreSplitPartitionScope.class)), times(1));
            Assertions.assertEquals(0L, skips, "a sampled view must not be reported as a declined view");
        }
    }

    @Test
    public void testNonSystemStaticOverwriteOfPctViewIsDeclined() throws Exception {
        // Only the view's own refresh is a system statement; anything else writing a view stays declined.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            fixture.withMaterializedViewTarget(null);

            long skips = recordedSkips(SkipReason.MATERIALIZED_VIEW_TARGET, () -> fixture.runStaticOverwrite(false));

            flow.verifyNoInteractions();
            Assertions.assertEquals(1L, skips, "a non-system overwrite of a view must record materialized_view_target");
        }
    }

    @Test
    public void testSystemInsertIntoPctViewIsDeclined() throws Exception {
        // A system statement admits a view only on the static-overwrite route. A system INSERT INTO a view (an online
        // rewrite of it, for one) reaches the pre-analysis route, which must keep declining every view.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            fixture.withMaterializedViewTarget(null);
            when(fixture.insertStmt.isSystem()).thenReturn(true);

            long skips = recordedSkips(SkipReason.MATERIALIZED_VIEW_TARGET,
                    () -> InsertPreSplitHook.maybeRunPreSplit(fixture.insertStmt, fixture.context));

            flow.verifyNoInteractions();
            Assertions.assertEquals(1L, skips, "a system INSERT INTO a view must record materialized_view_target");
        }
    }

    @Test
    public void testSystemStaticOverwriteOfPctViewOverExternalSourceIsDeclined() throws Exception {
        // A refresh samples only an internal OLAP base table: an external one would add a whole-source external scan
        // to every refresh batch. The source is sized and unrestricted, so only the view target can decline it.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            fixture.withMaterializedViewTarget(null);
            fixture.withExternalSource(connectorStatistics(1_000_000d, 8d, Statistics.StatsSource.TABLE_METADATA));

            fixture.runStaticOverwrite(true);

            flow.verifyNoInteractions();
        }
    }

    @Test
    public void testSystemStaticOverwriteOfPctViewSkipsTargetAuthorization() throws Exception {
        // The refresh already required privileges on the view, the INSERT check never applies to a view,
        // and a denial here would be swallowed into a silent no-op. Every target-side check is stubbed to
        // deny, so reaching the flow proves none of them ran; the source SELECT re-check still must.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            MaterializedView view = fixture.withMaterializedViewTarget(null);
            when(fixture.context.isBypassAuthorizerCheck()).thenReturn(false);
            Warehouse otherWarehouse = mock(Warehouse.class);
            when(otherWarehouse.getId()).thenReturn(WarehouseManager.DEFAULT_WAREHOUSE_ID + 1);
            when(otherWarehouse.getName()).thenReturn("other_warehouse");
            when(GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(anyLong()))
                    .thenReturn(otherWarehouse);
            fixture.authorizer.when(() -> Authorizer.checkTableAction(
                            any(ConnectContext.class), any(String.class), any(String.class), eq(PrivilegeType.INSERT)))
                    .thenThrow(new AccessDeniedException("simulated: no INSERT on the view"));
            fixture.authorizer.when(() -> Authorizer.checkWarehouseAction(
                            any(ConnectContext.class), any(String.class), any(PrivilegeType.class)))
                    .thenThrow(new AccessDeniedException("simulated: no USAGE on the warehouse"));

            fixture.runStaticOverwrite(true);

            flow.verify(() -> PreSplitFlow.runStaticOverwriteFlow(
                    any(Database.class), eq(view), any(PreSplitFlow.Prepared.class),
                    eq(LoadKind.INSERT_FROM_TABLE), any(), eq(fixture.context),
                    any(PreSplitPartitionScope.class)), times(1));
            fixture.authorizer.verify(() -> Authorizer.checkWarehouseAction(any(), any(), any()), never());
            fixture.authorizer.verify(() -> Authorizer.checkTableAction(
                    any(ConnectContext.class), any(String.class), any(String.class), eq(PrivilegeType.INSERT)), never());
            fixture.authorizer.verify(() -> Authorizer.checkTableAction(
                    any(ConnectContext.class), any(String.class), any(String.class), any(String.class),
                    eq(PrivilegeType.SELECT)), times(1));
        }
    }

    @Test
    public void testSystemStaticOverwriteOfIncrementalViewDeclinedByDerivedTier() throws Exception {
        // An incremental view is keyed by its hidden row id, which no sampled source can map back to a
        // column. One the derived tier cannot key either (here: a sort key that is not the row id) must
        // stay declined rather than reach the sampled path a view without a row id now takes.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            fixture.withMaterializedViewTarget(RowIdStrategy.AUTO_INCREMENT);

            long skips = recordedSkips(SkipReason.MATERIALIZED_VIEW_TARGET, () -> fixture.runStaticOverwrite(true));

            flow.verifyNoInteractions();
            Assertions.assertEquals(1L, skips,
                    "an incremental view the derived tier cannot key must record materialized_view_target");
        }
    }

    /** Runs {@code invocation} with metrics enabled and returns how many times it recorded {@code reason}. */
    private static long recordedSkips(SkipReason reason, PresplitTestSupport.HookInvocation invocation)
            throws Exception {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = reason.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue();
            invocation.run();
            return MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue() - baseline;
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    /** Runs {@code invocation} with metrics enabled and returns how many skips it recorded, whatever the reason. */
    private static long recordedSkipsOfAnyReason(PresplitTestSupport.HookInvocation invocation) throws Exception {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            long baseline = totalRecordedSkips();
            invocation.run();
            return totalRecordedSkips() - baseline;
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    private static long totalRecordedSkips() {
        long total = 0L;
        for (SkipReason reason : SkipReason.values()) {
            total += MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(reason.name().toLowerCase()).getValue();
        }
        return total;
    }

    // ---------- tryRunPreSplit: partition-branch gate ----------

    @Test
    public void testManuallyPartitionedTargetShortCircuits() throws Exception {
        // A partitioned target that does NOT support automatic partitioning (e.g. a plain
        // list/range partition table) must not reach runMultiPartitionFlow: pre-creating
        // partitions from sampled values would fabricate system partitions outside the
        // user-defined partition set.
        try (SourceFixture fixture = sourceFixture()) {
            // Override the default (isPartitioned=false) to make it partitioned but non-automatic.
            PartitionInfo manualPartitionInfo = mock(PartitionInfo.class);
            when(manualPartitionInfo.isPartitioned()).thenReturn(true);
            when(manualPartitionInfo.getPartitionColumns(any())).thenReturn(List.of());
            when(fixture.target().getPartitionInfo()).thenReturn(manualPartitionInfo);
            when(fixture.target().supportedAutomaticPartition()).thenReturn(false);

            fixture.assertNoSubmit();
        }
    }

    // ---------- Source authorization + policy gate ----------

    @Test
    public void testUserLacksSelectOnSourceShortCircuits() throws Exception {
        // AccessDeniedException from the source SELECT check is swallowed by the
        // outer try/catch — no submit, no propagation.
        try (SourceFixture fixture = sourceFixture()) {
            when(fixture.context.isBypassAuthorizerCheck()).thenReturn(false);
            fixture.authorizer.when(() -> Authorizer.checkTableAction(
                            any(ConnectContext.class), any(String.class), any(String.class),
                            any(String.class), eq(com.starrocks.authorization.PrivilegeType.SELECT)))
                    .thenThrow(new AccessDeniedException("simulated: no SELECT on source"));

            fixture.assertNoSubmit();
        }
    }

    @Test
    public void testSourceRowAccessPolicyShortCircuits() throws Exception {
        try (SourceFixture fixture = sourceFixture()) {
            fixture.authorizer.when(() -> Authorizer.getRowAccessPolicy(any(), any()))
                    .thenReturn(mock(Expr.class));

            fixture.assertNoSubmit();
        }
    }

    @Test
    public void testSourceColumnMaskingPolicyShortCircuits() throws Exception {
        try (SourceFixture fixture = sourceFixture()) {
            fixture.authorizer.when(() -> Authorizer.getColumnMaskingPolicy(any(), any(), any()))
                    .thenReturn(Map.of("k", mock(Expr.class)));

            fixture.assertNoSubmit();
        }
    }

    // ---------- Source resolution / mapping ----------

    @Test
    public void testSourceNotOlapTableShortCircuits() throws Exception {
        // The source resolves to a non-OLAP table — pre-split is out of scope.
        try (SourceFixture fixture = sourceFixture()) {
            fixture.metaUtils.when(() -> MetaUtils.getSessionAwareTable(
                            any(), eq(fixture.sourceDb), any()))
                    .thenReturn(mock(Table.class));

            fixture.assertNoSubmit();
        }
    }

    @Test
    public void testIcebergSourceIsSupportedAndUsesSnapshotTotals() {
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        org.apache.iceberg.Snapshot snapshot = mock(org.apache.iceberg.Snapshot.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        when(snapshot.summary()).thenReturn(Map.of(
                "total-files-size", "872000000000",
                "total-records", "3500000000"));

        when(icebergTable.getCatalogName()).thenReturn("iceberg_catalog");
        when(icebergTable.getType()).thenReturn(Table.TableType.ICEBERG);

        Assertions.assertTrue(TablePreSplitSource.isSupportedSourceTable(icebergTable));
        // A usable snapshot summary answers without consulting the table statistics at all, so no
        // ConnectContext / MetadataMgr is needed here.
        Assertions.assertEquals(new Estimates(872000000000L, 3500000000L),
                TablePreSplitSource.sourceEstimates(icebergTable, List.of(), null));
    }

    @Test
    public void testExternalCatalogBaseTablesAreSupportedSources() {
        for (Table.TableType type : List.of(Table.TableType.HIVE, Table.TableType.PAIMON,
                Table.TableType.DELTALAKE, Table.TableType.HUDI, Table.TableType.JDBC,
                Table.TableType.ELASTICSEARCH)) {
            Assertions.assertTrue(TablePreSplitSource.isSupportedSourceTable(externalTable("ext_catalog", type)),
                    "an external-catalog " + type + " table must be a supported source");
        }
    }

    @Test
    public void testViewsMetadataAndInternalNonOlapTablesAreNotSupportedSources() {
        // Views would need the projection mapped through the view definition.
        for (Table.TableType type : List.of(Table.TableType.HIVE_VIEW, Table.TableType.ICEBERG_VIEW,
                Table.TableType.PAIMON_VIEW)) {
            Table view = externalTable("ext_catalog", type);
            when(view.isView()).thenReturn(true);
            Assertions.assertFalse(TablePreSplitSource.isSupportedSourceTable(view), type + " must be excluded");
        }
        Table metadataTable = externalTable("ext_catalog", Table.TableType.METADATA);
        when(metadataTable.isMetadataTable()).thenReturn(true);
        Assertions.assertFalse(TablePreSplitSource.isSupportedSourceTable(metadataTable));
        Assertions.assertFalse(TablePreSplitSource.isSupportedSourceTable(
                externalTable("ext_catalog", Table.TableType.SCHEMA)));
        // A non-OLAP table of the internal catalog (an old-style MySQL / ES external table, a view)
        // keeps being declined.
        Assertions.assertFalse(TablePreSplitSource.isSupportedSourceTable(
                externalTable("default_catalog", Table.TableType.MYSQL)));
        Assertions.assertFalse(TablePreSplitSource.isSupportedSourceTable(null));
    }

    private static Table externalTable(String catalogName, Table.TableType type) {
        Table table = mock(Table.class);
        when(table.getCatalogName()).thenReturn(catalogName);
        when(table.getType()).thenReturn(type);
        when(table.getCatalogDBName()).thenReturn("ext_db");
        when(table.getName()).thenReturn("ext_t");
        when(table.isUnPartitioned()).thenReturn(true);
        when(table.getBaseSchema()).thenReturn(List.of(bigintColumn("k"), bigintColumn("v")));
        when(table.getFullVisibleSchema()).thenReturn(List.of(bigintColumn("k"), bigintColumn("v")));
        return table;
    }

    /** Stubs the fixture-less MetadataMgr the connector-statistics estimate reads. */
    private static MockedStatic<com.starrocks.server.GlobalStateMgr> mockStatistics(Statistics statistics) {
        MockedStatic<com.starrocks.server.GlobalStateMgr> globalStateMgr =
                Mockito.mockStatic(com.starrocks.server.GlobalStateMgr.class);
        com.starrocks.server.GlobalStateMgr globalState = mock(com.starrocks.server.GlobalStateMgr.class);
        com.starrocks.server.MetadataMgr metadataMgr = mock(com.starrocks.server.MetadataMgr.class);
        when(globalState.getMetadataMgr()).thenReturn(metadataMgr);
        globalStateMgr.when(com.starrocks.server.GlobalStateMgr::getCurrentState).thenReturn(globalState);
        stubStatistics(metadataMgr, statistics);
        return globalStateMgr;
    }

    private static void stubStatistics(com.starrocks.server.MetadataMgr metadataMgr, Statistics statistics) {
        when(metadataMgr.getTableVersionRange(any(), any(), any(), any())).thenReturn(TvrTableSnapshot.empty());
        when(metadataMgr.getTableStatistics(any(), any(), any(), any(), any(), any(), anyLong(), any()))
                .thenReturn(statistics);
    }

    private static Statistics connectorStatistics(double rows, double columnWidth,
                                                  Statistics.StatsSource statsSource) {
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.BIGINT, "k", true);
        ColumnRefOperator v = new ColumnRefOperator(2, IntegerType.BIGINT, "v", true);
        ColumnStatistic width = ColumnStatistic.builder().setAverageRowSize(columnWidth).build();
        return Statistics.builder().setOutputRowCount(rows)
                .addColumnStatistic(k, width).addColumnStatistic(v, width)
                .setStatsSource(statsSource).build();
    }

    @Test
    public void testExternalSourceIsSizedFromConnectorStatistics() {
        // The rows are what the connector reports; the bytes are those rows at the statistics' width
        // (two 8-byte columns here), since the optimizer's statistics carry no stored-file size.
        try (MockedStatic<com.starrocks.server.GlobalStateMgr> ignored = mockStatistics(
                connectorStatistics(1_000_000d, 8d, Statistics.StatsSource.TABLE_METADATA))) {
            Assertions.assertEquals(new Estimates(16_000_000L, 1_000_000L),
                    TablePreSplitSource.sourceEstimates(externalTable("hive_catalog", Table.TableType.HIVE),
                            List.of(), mockConnectContextWithSessionPreSplit(true)));
        }
    }

    @Test
    public void testPlaceholderConnectorStatisticsAreNotAnEstimate() {
        // StatsSource.NONE is what the optimizer returns when it knows nothing -- e.g. a JDBC table
        // whose statistics are still loading, or a catalog with stats-from-metadata turned off. Its
        // row count is Config.default_statistics_output_row_count, not a measurement.
        try (MockedStatic<com.starrocks.server.GlobalStateMgr> ignored = mockStatistics(
                connectorStatistics(1d, 8d, Statistics.StatsSource.NONE))) {
            Assertions.assertEquals(Estimates.ZERO,
                    TablePreSplitSource.sourceEstimates(externalTable("jdbc_catalog", Table.TableType.JDBC),
                            List.of(), mockConnectContextWithSessionPreSplit(true)));
        }
    }

    @Test
    public void testConnectorStatisticsFailureIsNotAnEstimate() {
        try (MockedStatic<com.starrocks.server.GlobalStateMgr> globalStateMgr = mockStatistics(null)) {
            com.starrocks.server.MetadataMgr metadataMgr =
                    com.starrocks.server.GlobalStateMgr.getCurrentState().getMetadataMgr();
            when(metadataMgr.getTableStatistics(any(), any(), any(), any(), any(), any(), anyLong(), any()))
                    .thenThrow(new RuntimeException("simulated metastore outage"));
            Assertions.assertEquals(Estimates.ZERO,
                    TablePreSplitSource.sourceEstimates(externalTable("hive_catalog", Table.TableType.HIVE),
                            List.of(), mockConnectContextWithSessionPreSplit(true)));
        }
    }

    @Test
    public void testIcebergSourceWithoutSummaryTotalsFallsBackToConnectorStatistics() {
        IcebergTable icebergTable = mockIcebergTableWithSummary(Map.of("operation", "append"));
        when(icebergTable.getCatalogName()).thenReturn("iceberg_catalog");
        when(icebergTable.isUnPartitioned()).thenReturn(true);
        when(icebergTable.getBaseSchema()).thenReturn(List.of(bigintColumn("k"), bigintColumn("v")));
        try (MockedStatic<com.starrocks.server.GlobalStateMgr> ignored = mockStatistics(
                connectorStatistics(500d, 8d, Statistics.StatsSource.TABLE_METADATA))) {
            Assertions.assertEquals(new Estimates(8_000L, 500L),
                    TablePreSplitSource.sourceEstimates(icebergTable, List.of(),
                            mockConnectContextWithSessionPreSplit(true)));
        }
    }

    // The snapshot-summary keys below are written by whichever engine produced the snapshot, not by
    // StarRocks, so a source table can legitimately arrive without them. These lock down what
    // pre-split then does, because the degradation is silent at the SQL level.

    private static IcebergTable mockIcebergTableWithSummary(Map<String, String> summary) {
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        if (summary == null) {
            when(nativeTable.currentSnapshot()).thenReturn(null);
            return icebergTable;
        }
        org.apache.iceberg.Snapshot snapshot = mock(org.apache.iceberg.Snapshot.class);
        when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        when(snapshot.summary()).thenReturn(summary);
        return icebergTable;
    }

    @Test
    public void testIcebergSourceWithoutCurrentSnapshotEstimatesZero() {
        // An empty (never-written) Iceberg table has no current snapshot.
        Assertions.assertEquals(Estimates.ZERO,
                TablePreSplitSource.icebergSnapshotEstimates(mockIcebergTableWithSummary(null)));
    }

    @Test
    public void testIcebergSourceMissingSummaryTotalsEstimatesZero() {
        // The case most likely to bite in production: a snapshot exists and the scan works, but the
        // writer never recorded the totals, so pre-split has no size to work from.
        Assertions.assertEquals(Estimates.ZERO,
                TablePreSplitSource.icebergSnapshotEstimates(
                        mockIcebergTableWithSummary(Map.of("operation", "append"))));
    }

    @Test
    public void testIcebergSourceWithUnparseableSummaryTotalsEstimatesZero() {
        // Never propagate a partially-parsed size: one bad key must not be combined with a good one
        // into a ratio that over- or under-splits.
        Assertions.assertEquals(Estimates.ZERO,
                TablePreSplitSource.icebergSnapshotEstimates(mockIcebergTableWithSummary(
                        Map.of("total-files-size", "not-a-number", "total-records", "-17"))));
    }

    @Test
    public void testZeroSourceEstimateRemovesTheSamplingRateLimit() {
        // Why an unsized external source is declined rather than sampled: a zero byte estimate makes
        // the Bernoulli rate 1.0, so every predicate-matching row reaches the ORDER BY rand() LIMIT
        // rather than a small sample. On a large lake table that turns a cheap sample into a top-N
        // over the whole filtered input.
        Assertions.assertEquals(1.0, AbstractSqlSampleSubqueryExecutor.pickSamplingRate(0L));
        Assertions.assertTrue(AbstractSqlSampleSubqueryExecutor.pickSamplingRate(872000000000L) < 1.0e-4,
                "a sized snapshot must still be sampled at a small rate");
    }

    @Test
    public void prepareSizesAnExternalSourceFromItsStatistics() throws Exception {
        try (SourceFixture fixture = sourceFixture()) {
            fixture.withExternalSource(connectorStatistics(1_000_000d, 8d, Statistics.StatsSource.TABLE_METADATA));
            when(fixture.context.getSessionVariable().getTimeZone()).thenReturn("America/Los_Angeles");

            PreSplitFlow.Prepared prepared = fixture.prepare();

            Assertions.assertNotNull(prepared, "a sized external source must be pre-split");
            InsertFromTableScanContext scanContext = (InsertFromTableScanContext) prepared.scanContext();
            Assertions.assertEquals(16_000_000L, scanContext.sourceTotalBytes());
            Assertions.assertEquals(1_000_000L, scanContext.sourceTotalRows());
            Assertions.assertEquals("America/Los_Angeles", scanContext.loadTimeZone());
            Assertions.assertEquals(16_000_000L, prepared.estimatedBytes());
            Assertions.assertEquals(Map.of("k", "k", "v", "v"), scanContext.targetToSourceColumnNames());
        }
    }

    @Test
    public void prepareDeclinesAnUnsizedExternalSourceWithEstimateUnavailable() throws Exception {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try (SourceFixture fixture = sourceFixture()) {
            fixture.withExternalSource(connectorStatistics(1d, 8d, Statistics.StatsSource.NONE));
            String label = SkipReason.ESTIMATE_UNAVAILABLE.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue();

            Assertions.assertNull(fixture.prepare(), "an unsized external source must not be sampled");
            Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue());
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void prepareStillSamplesAnUnsizedOlapSource() throws Exception {
        // The fixture's OLAP source reports zero bytes; that keeps today's unrated sampling.
        try (SourceFixture fixture = sourceFixture()) {
            Assertions.assertNotNull(fixture.prepare());
        }
    }

    @Test
    public void prepareDoesNotSizeAnExternalSourceTheUserCannotSelect() throws Exception {
        // The SELECT re-check and the policy gate still guard a new source kind, and they run before
        // the connector statistics are read.
        try (SourceFixture fixture = sourceFixture()) {
            fixture.withExternalSource(connectorStatistics(1_000_000d, 8d, Statistics.StatsSource.TABLE_METADATA));
            fixture.authorizer.when(() -> Authorizer.getRowAccessPolicy(any(), any()))
                    .thenReturn(mock(Expr.class));

            Assertions.assertNull(fixture.prepare());
            verify(fixture.metadataMgr, never())
                    .getTableStatistics(any(), any(), any(), any(), any(), any(), anyLong(), any());
        }
    }

    @Test
    public void testUnmappableSortKeyShortCircuits() throws Exception {
        // Target sort key references a column the source projection cannot supply
        // (target has column "missing" that does not exist in the source).
        try (SourceFixture fixture = sourceFixture(List.of("k", "missing"), List.of("k", "v"))) {
            fixture.assertNoSubmit();
        }
    }

    @Test
    public void testSourceGeneratedColumnWithStarShortCircuits() throws Exception {
        // A visible generated source column adds an output the by-position mapping
        // cannot see; SELECT * over such a source is rejected.
        try (SourceFixture fixture = sourceFixture()) {
            when(fixture.sourceTable.hasGeneratedColumn()).thenReturn(true);
            fixture.assertNoSubmit();
        }
    }

    // ---------- Outer try/catch ----------

    @Test
    public void testInternalThrowIsSwallowed() {
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTxnId()).thenThrow(new RuntimeException("simulated stmt failure"));

        Assertions.assertDoesNotThrow(() ->
                        InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)),
                "hook must never propagate a throw");
    }

    // ---------- TablePreSplitSource.prepare: scan-context content (re-homed) ----------

    @Test
    public void prepareBuildsScanContextWithResolvedSourceColumns() throws Exception {
        // The shared single/multi routing is covered by PreSplitFlowTest; what is
        // source-specific here is the scan-context content TablePreSplitSource.prepare
        // builds. Drive prepare directly against the wired resolve path: the captured
        // InsertFromTableScanContext must carry the source column resolved for the
        // target sort key (here target "k" maps by-position to source "k"), an empty
        // partition source-column list (unpartitioned target), and a null WHERE.
        try (SourceFixture fixture = sourceFixture()) {
            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext, "prepare must build a scan context for the eligible source");
            Assertions.assertEquals(Map.of("k", "k", "v", "v"), scanContext.targetToSourceColumnNames(),
                    "scan context must carry the full resolved target->source column map");
            Assertions.assertNull(scanContext.wherePredicateSql(),
                    "no WHERE clause must yield a null predicate SQL");
            Assertions.assertSame(fixture.sourceTable, scanContext.sourceTable(),
                    "scan context must carry the resolved OLAP source table");
        }
    }

    @Test
    public void prepareCarriesTheUsersSessionSemanticsIntoTheScanContext() throws Exception {
        SessionVariable userSession = new SessionVariable();
        userSession.setSqlMode(SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_PIPES_AS_CONCAT);
        userSession.setTimeZone("Asia/Shanghai");
        userSession.setCboEqBaseType("varchar");
        SampleSessionSemantics expected = SampleSessionSemantics.capture(userSession);

        try (SourceFixture fixture = sourceFixture()) {
            when(fixture.context.getSessionVariable()).thenReturn(userSession);

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext);
            Assertions.assertEquals(expected, scanContext.sessionSemantics());
        }
    }

    @Test
    public void prepareAllowsExpressionOnNonKeyColumn() throws Exception {
        // target [k, v]; SELECT k, parse_json(v) FROM src. The sampler only needs
        // target key k, so the value expression is intentionally absent from the map.
        try (SourceFixture fixture = sourceFixture()) {
            SelectRelation selectRelation =
                    (SelectRelation) fixture.insertStmt.getQueryStatement().getQueryRelation();
            SelectListItem expression = mock(SelectListItem.class);
            when(expression.isStar()).thenReturn(false);
            when(expression.getExpr()).thenReturn(mock(FunctionCallExpr.class));
            SelectList projection = selectListOf(bareColumnItem("k"), expression);
            when(selectRelation.getSelectList()).thenReturn(projection);

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext);
            Assertions.assertEquals(Map.of("k", "k"), scanContext.targetToSourceColumnNames());
        }
    }

    @Test
    public void prepareAcceptsPartialTargetColumnList() throws Exception {
        // target base (k, v, extra); INSERT INTO t (k, v) SELECT * FROM src(k, v). This path used
        // to decline every non-identity list; the shared targetColumnListIsPreSplitSafe gate (see
        // InsertPreSplitHookColumnListTest) is now the only column-list gate, and the omitted
        // "extra" -- defaulted by the load -- simply never enters the map.
        try (SourceFixture fixture = sourceFixture(List.of("k", "v", "extra"), List.of("k", "v"))) {
            when(fixture.insertStmt.getTargetColumnNames()).thenReturn(List.of("k", "v"));

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext, "a partial target column list must not skip pre-split");
            Assertions.assertEquals(Map.of("k", "k", "v", "v"), scanContext.targetToSourceColumnNames());
        }
    }

    @Test
    public void prepareAcceptsReorderedTargetColumnList() throws Exception {
        // INSERT INTO t (v, k) SELECT k, v FROM src -- outputs pair against the list as written,
        // so the target sort key k is sampled from source column v, not from source column k.
        try (SourceFixture fixture = sourceFixture()) {
            SelectRelation selectRelation =
                    (SelectRelation) fixture.insertStmt.getQueryStatement().getQueryRelation();
            SelectList projection = selectListOf(bareColumnItem("k"), bareColumnItem("v"));
            when(selectRelation.getSelectList()).thenReturn(projection);
            when(fixture.insertStmt.getTargetColumnNames()).thenReturn(List.of("v", "k"));

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext, "a reordered target column list must not skip pre-split");
            Assertions.assertEquals(Map.of("k", "v", "v", "k"), scanContext.targetToSourceColumnNames());
        }
    }

    @Test
    public void prepareThreadsWherePredicateSqlIntoScanContext() throws Exception {
        // A deterministic, safe WHERE clause must be rendered by SamplingPredicateGate.toSql
        // and threaded verbatim into the scan context. Mock the WHERE Expr so the gate's
        // safety walk finds nothing unsafe, and stub toSql to a sentinel so the assertion
        // does not couple to AstToSQLBuilder's exact rendering (the gate itself is unit-tested
        // separately in SamplingPredicateGateTest).
        try (SourceFixture fixture = sourceFixture()) {
            Expr where = mock(Expr.class);
            QueryRelation queryRelation = fixture.insertStmt.getQueryStatement().getQueryRelation();
            when(((SelectRelation) queryRelation).getWhereClause()).thenReturn(where);

            try (MockedStatic<SamplingPredicateGate> gate =
                         Mockito.mockStatic(SamplingPredicateGate.class, Mockito.CALLS_REAL_METHODS)) {
                gate.when(() -> SamplingPredicateGate.toSql(where)).thenReturn("`a` > 10");

                InsertFromTableScanContext scanContext = fixture.prepareScanContext();

                Assertions.assertNotNull(scanContext, "prepare must build a scan context");
                Assertions.assertEquals("`a` > 10", scanContext.wherePredicateSql(),
                        "the rendered WHERE predicate SQL must be threaded into the scan context");
            }
        }
    }

    @Test
    public void prepareRendersTheFoldedWherePredicate() throws Exception {
        // prepare must hand the sampler the predicate folded in the caller's context, not the
        // parsed one: the parsed date_sub(current_date(), 7) would be evaluated as ROOT.
        try (SourceFixture fixture = sourceFixture()) {
            Expr parsed = mock(Expr.class);
            Expr folded = mock(Expr.class);
            QueryRelation queryRelation = fixture.insertStmt.getQueryStatement().getQueryRelation();
            when(((SelectRelation) queryRelation).getWhereClause()).thenReturn(parsed);

            try (MockedStatic<SamplingPredicateGate> gate =
                         Mockito.mockStatic(SamplingPredicateGate.class, Mockito.CALLS_REAL_METHODS)) {
                gate.when(() -> SamplingPredicateGate.foldPlanTimeConstants(eq(parsed), any()))
                        .thenReturn(folded);
                gate.when(() -> SamplingPredicateGate.toSql(folded)).thenReturn("`dt` >= '2026-09-16'");

                InsertFromTableScanContext scanContext = fixture.prepareScanContext();

                Assertions.assertNotNull(scanContext);
                Assertions.assertEquals("`dt` >= '2026-09-16'", scanContext.wherePredicateSql());
            }
        }
    }

    @Test
    public void prepareSkipsWhenTheWherePredicateDoesNotFold() throws Exception {
        try (SourceFixture fixture = sourceFixture()) {
            Expr parsed = mock(Expr.class);
            QueryRelation queryRelation = fixture.insertStmt.getQueryStatement().getQueryRelation();
            when(((SelectRelation) queryRelation).getWhereClause()).thenReturn(parsed);

            try (MockedStatic<SamplingPredicateGate> gate =
                         Mockito.mockStatic(SamplingPredicateGate.class, Mockito.CALLS_REAL_METHODS)) {
                gate.when(() -> SamplingPredicateGate.foldPlanTimeConstants(eq(parsed), any()))
                        .thenReturn(null);

                Assertions.assertNull(fixture.prepareScanContext());
            }
        }
    }

    @Test
    public void prepareSkipsAWhereWhoseCallBindsNoBuiltin() throws Exception {
        // upper is allowlisted by name, but no built-in upper takes two arguments.
        try (SourceFixture fixture = sourceFixture()) {
            QueryRelation queryRelation = fixture.insertStmt.getQueryStatement().getQueryRelation();
            when(((SelectRelation) queryRelation).getWhereClause()).thenReturn(WHERE_UPPER_V_V);

            Assertions.assertNull(fixture.prepareScanContext());
        }
    }

    @Test
    public void prepareChecksTheWhereAgainstTheSourcesGeneratedColumnsToo() throws Exception {
        // A WHERE clause may read a generated source column, which the projection mapping never sees.
        try (SourceFixture fixture = sourceFixture()) {
            when(fixture.sourceTable.getBaseSchema()).thenReturn(List.of(GEN_K, GEN_V, GEN_DT, GEN_DT_MONTH));
            SelectRelation selectRelation =
                    (SelectRelation) fixture.insertStmt.getQueryStatement().getQueryRelation();
            SelectList selectList = selectListOf(bareColumnItem("k"), bareColumnItem("v"));
            when(selectRelation.getSelectList()).thenReturn(selectList);
            when(selectRelation.getWhereClause()).thenReturn(WHERE_ON_GENERATED_DT_MONTH);

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext, "a WHERE clause may read a generated source column");
            Assertions.assertEquals("(date_trunc('month', `dt_month`)) >= '2026-01-01 00:00:00'",
                    scanContext.wherePredicateSql());
        }
    }

    @Test
    public void prepareSkipsAWhereWithADecimalLiteralTheSamplerWouldReadAsADouble() throws Exception {
        // The check runs on the folded WHERE clause in the user's sql_mode: the folded 1.5 reads back as itself without
        // DOUBLE_LITERAL and as a DOUBLE under it.
        try (SourceFixture fixture = sourceFixture()) {
            SelectRelation selectRelation =
                    (SelectRelation) fixture.insertStmt.getQueryStatement().getQueryRelation();
            when(selectRelation.getWhereClause()).thenReturn(WHERE_FOLDED_DECIMAL);
            InsertFromTableScanContext scanContext = fixture.prepareScanContext();
            Assertions.assertNotNull(scanContext);
            Assertions.assertEquals("`v` < (CAST(1.5 AS DECIMAL64(10,1)))", scanContext.wherePredicateSql());

            when(fixture.context.getSessionVariable().getSqlMode())
                    .thenReturn(SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL);
            Assertions.assertNull(fixture.prepareScanContext());
        }
    }

    @Test
    public void prepareSourceEqualsTargetStillProducesScanContext() throws Exception {
        // INSERT INTO t SELECT * FROM t -- the source resolves to the SAME OlapTable
        // instance as the target. The hook holds no locks, so source == target is NOT
        // rejected: prepare must still resolve the source and build a scan context
        // (the prior flow-seam test asserted only submit+await, which PreSplitFlowTest
        // now covers; the source-specific signal that survives is "source == target is
        // accepted at prepare").
        try (SourceFixture fixture = sourceFixture()) {
            fixture.resolveSourceToTarget();

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext, "source == target must still build a scan context");
            Assertions.assertSame(fixture.target(), scanContext.sourceTable(),
                    "scan context must carry the target table as its source when source == target");
            Assertions.assertEquals(Map.of("k", "k", "v", "v"), scanContext.targetToSourceColumnNames());
        }
    }

    @Test
    public void prepareBuildsSecondaryIndexSpecsForRollup() throws Exception {
        // A range target carrying a rollup: prepare must emit one SecondaryIndexSpec per
        // visible rollup, carrying that rollup's TARGET sort-key columns (the remap to
        // source names happens later, in the executor, via the scan-context map).
        try (SourceFixture fixture = sourceFixture()) {
            fixture.withVisibleRollup(/*rollupMetaId=*/ 202L, List.of("k"));

            PreSplitFlow.Prepared prepared = fixture.prepare();

            Assertions.assertNotNull(prepared, "prepare must build a Prepared for the rollup target");
            Assertions.assertEquals(1, prepared.secondaryIndexSpecs().size(),
                    "one visible rollup -> one secondary index spec");
            SecondaryIndexSpec spec = prepared.secondaryIndexSpecs().get(0);
            Assertions.assertEquals(202L, spec.indexMetaId());
            Assertions.assertEquals(List.of("k"),
                    spec.sortKey().stream().map(Column::getName).collect(java.util.stream.Collectors.toList()),
                    "spec carries the rollup's TARGET sort-key columns");
        }
    }

    @Test
    public void prepareSkipsWhenRollupSortKeyUnmappable() throws Exception {
        // A rollup whose sort-key column has no source mapping (e.g. a range DUP rollup whose
        // ORDER BY promotes a generated column -- absent from the non-generated base-schema map)
        // must skip pre-split cleanly (prepare returns null), not build a spec that later fails
        // the sample. Here "g" is not among the fixture's non-generated base columns {k, v}.
        try (SourceFixture fixture = sourceFixture()) {
            fixture.withVisibleRollup(/*rollupMetaId=*/ 203L, List.of("g"));

            Assertions.assertNull(fixture.prepare(),
                    "a rollup sort-key column with no source mapping must skip pre-split");
        }
    }

    @Test
    public void prepareComputesAGeneratedPartitionColumnFromTheSourceColumnItReads() throws Exception {
        try (SourceFixture fixture = sourceFixture()) {
            fixture.withGeneratedPartitionColumn();

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext, "a generated partition column over a source column must be sampled");
            Assertions.assertEquals(Map.of("dt_month", "date_trunc('month', CAST(`dt` AS DATETIME))"),
                    scanContext.targetToExpressionSql());
            Assertions.assertEquals(Map.of("k", "k", "v", "v", "dt", "dt"), scanContext.targetToSourceColumnNames());
        }
    }

    @Test
    public void prepareDeclinesAGeneratedPartitionColumnWhoseInputTheColumnListOmits() throws Exception {
        // INSERT INTO t (k, v) SELECT k, v FROM src: dt is defaulted, so dt_month cannot be sampled. The
        // skip must name the generated column in the metric, the log and the profile.
        try (SourceFixture fixture = sourceFixture()) {
            fixture.withGeneratedPartitionColumn();
            SelectRelation selectRelation =
                    (SelectRelation) fixture.insertStmt.getQueryStatement().getQueryRelation();
            SelectList projection = selectListOf(bareColumnItem("k"), bareColumnItem("v"));
            when(selectRelation.getSelectList()).thenReturn(projection);
            when(fixture.insertStmt.getTargetColumnNames()).thenReturn(List.of("k", "v"));
            PreSplitProfile profile = new PreSplitProfile();

            boolean savedHasInit = MetricRepo.hasInit;
            MetricRepo.hasInit = true;
            try (PreSplitProfile.Scope ignored = PreSplitProfile.startAttempt(profile, LoadKind.INSERT_FROM_TABLE)) {
                String reason = SkipReason.UNSUPPORTED_GENERATED_COLUMN.name().toLowerCase();
                long before = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(reason).getValue();

                Assertions.assertNull(fixture.prepare());

                Assertions.assertEquals(before + 1L,
                        MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(reason).getValue().longValue());
            } finally {
                MetricRepo.hasInit = savedHasInit;
            }
            Assertions.assertEquals("SKIPPED: UNSUPPORTED_GENERATED_COLUMN (dt_month)",
                    profile.toRuntimeProfile().getInfoString("Outcomes"));
        }
    }

    @Test
    public void prepareAdmitsASafeComputedPartitionColumn() throws Exception {
        // SELECT k, abs(v) FROM src into PARTITION BY (v): the table path evaluates a safe computed key
        // over the source rows, as the FILES path does.
        try (SourceFixture fixture = sourceFixture()) {
            when(fixture.target().getPartitionInfo().getPartitionColumns(any())).thenReturn(List.of(bigintColumn("v")));
            SelectRelation selectRelation =
                    (SelectRelation) fixture.insertStmt.getQueryStatement().getQueryRelation();
            SelectListItem computed = mock(SelectListItem.class);
            when(computed.isStar()).thenReturn(false);
            when(computed.getExpr()).thenReturn(ABS_V);
            SelectList projection = selectListOf(bareColumnItem("k"), computed);
            when(selectRelation.getSelectList()).thenReturn(projection);

            InsertFromTableScanContext scanContext = fixture.prepareScanContext();

            Assertions.assertNotNull(scanContext, "a safe computed partition column must be sampled");
            Assertions.assertEquals(Map.of("v", "abs(`v`)"), scanContext.targetToExpressionSql());
        }
    }

    @Test
    public void prepareSkipsTemporaryTableSource() throws Exception {
        // The sampler runs as ROOT in a fresh statistics context that cannot see the user's
        // session temporary tables, so a temp-table source would be sampled as the shadowed
        // permanent table (or fail). prepare must skip (return null) when the resolved source
        // is temporary.
        try (SourceFixture fixture = sourceFixture()) {
            when(fixture.sourceTable.isTemporaryTable()).thenReturn(true);

            Assertions.assertNull(fixture.prepareScanContext(),
                    "a temporary-table source must skip pre-split (ROOT sampler cannot see it)");
        }
    }

    @Test
    public void dynamicOverwriteHookDispatchesWithTransactionId() throws Exception {
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            when(fixture.insertStmt.isDynamicOverwrite()).thenReturn(true);
            when(fixture.insertStmt.hasOverwriteJob()).thenReturn(true);

            InsertPreSplitHook.maybeRunDynamicOverwritePreSplit(fixture.insertStmt, fixture.context, 42L);

            flow.verify(() -> PreSplitFlow.runDynamicOverwriteFlow(
                    any(Database.class), eq(fixture.targetTable), any(PreSplitFlow.Prepared.class),
                    eq(LoadKind.INSERT_FROM_TABLE), any(), eq(fixture.context), eq(42L)), times(1));
        }
    }

    @Test
    public void dynamicOverwriteHookDispatchesWithAnalyzerFilledProperties() throws Exception {
        // This hook is the one pre-split entry point that runs AFTER analysis, so getProperties()
        // has already been filled with the session defaults InsertAnalyzer#analyzeProperties adds
        // to every INSERT. Gating on that map instead of the parse-time key set made the whole
        // dynamic-overwrite path a silent no-op on a real cluster.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            when(fixture.insertStmt.isDynamicOverwrite()).thenReturn(true);
            when(fixture.insertStmt.hasOverwriteJob()).thenReturn(true);
            when(fixture.insertStmt.getProperties()).thenReturn(Map.of(
                    "strict_mode", "true", "max_filter_ratio", "0.0", "timeout", "14400"));

            InsertPreSplitHook.maybeRunDynamicOverwritePreSplit(fixture.insertStmt, fixture.context, 42L);

            flow.verify(() -> PreSplitFlow.runDynamicOverwriteFlow(
                    any(Database.class), eq(fixture.targetTable), any(PreSplitFlow.Prepared.class),
                    eq(LoadKind.INSERT_FROM_TABLE), any(), eq(fixture.context), eq(42L)), times(1));
        }
    }

    @Test
    public void dynamicOverwriteHookDispatchesWithUserSpecifiedProperties() throws Exception {
        // Parity with the normal entry point, which the properties gate used to break separately:
        // this hook is the one that runs AFTER analysis, so it sees a getProperties() the analyzer
        // has already filled. Neither the parse-time key set nor that map may disqualify it.
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            when(fixture.insertStmt.isDynamicOverwrite()).thenReturn(true);
            when(fixture.insertStmt.hasOverwriteJob()).thenReturn(true);
            when(fixture.insertStmt.getProperties()).thenReturn(Map.of(
                    "strict_mode", "true", "max_filter_ratio", "0.1", "timeout", "14400",
                    "timezone", "Asia/Shanghai"));

            InsertPreSplitHook.maybeRunDynamicOverwritePreSplit(fixture.insertStmt, fixture.context, 42L);

            flow.verify(() -> PreSplitFlow.runDynamicOverwriteFlow(
                    any(Database.class), eq(fixture.targetTable), any(PreSplitFlow.Prepared.class),
                    eq(LoadKind.INSERT_FROM_TABLE), any(), eq(fixture.context), eq(42L)), times(1));
        }
    }

    @Test
    public void dynamicOverwriteHookSkipsNonDynamicStatement() throws Exception {
        assertDynamicOverwriteHookSkips(42L, stmt -> {
            when(stmt.isDynamicOverwrite()).thenReturn(false);
            when(stmt.hasOverwriteJob()).thenReturn(true);
        });
    }

    @Test
    public void dynamicOverwriteHookSkipsWithoutOverwriteJob() throws Exception {
        assertDynamicOverwriteHookSkips(42L, stmt -> {
            when(stmt.isDynamicOverwrite()).thenReturn(true);
            when(stmt.hasOverwriteJob()).thenReturn(false);
        });
    }

    @Test
    public void dynamicOverwriteHookSkipsWithoutTransactionId() throws Exception {
        // The caller has not opened the overwrite transaction yet, so there is no txn prefix to
        // name the temporary partitions after.
        assertDynamicOverwriteHookSkips(0L, stmt -> {
            when(stmt.isDynamicOverwrite()).thenReturn(true);
            when(stmt.hasOverwriteJob()).thenReturn(true);
        });
    }

    @Test
    public void dynamicOverwriteHookSkipsExplain() throws Exception {
        assertDynamicOverwriteHookSkips(42L, stmt -> {
            when(stmt.isDynamicOverwrite()).thenReturn(true);
            when(stmt.hasOverwriteJob()).thenReturn(true);
            when(stmt.isExplain()).thenReturn(true);
            when(stmt.getExplainLevel()).thenReturn(ExplainLevel.COSTS);
        });
    }

    @Test
    public void dynamicOverwriteHookSkipsPreSetStatementTransaction() throws Exception {
        assertDynamicOverwriteHookSkips(42L, stmt -> {
            when(stmt.isDynamicOverwrite()).thenReturn(true);
            when(stmt.hasOverwriteJob()).thenReturn(true);
            when(stmt.getTxnId()).thenReturn(99L);
        });
    }

    @Test
    public void dynamicOverwriteHookSkipsTargetPartitionSpec() throws Exception {
        assertDynamicOverwriteHookSkips(42L, stmt -> {
            when(stmt.isDynamicOverwrite()).thenReturn(true);
            when(stmt.hasOverwriteJob()).thenReturn(true);
            when(stmt.isSpecifyPartitionNames()).thenReturn(true);
        });
        assertDynamicOverwriteHookSkips(42L, stmt -> {
            when(stmt.isDynamicOverwrite()).thenReturn(true);
            when(stmt.hasOverwriteJob()).thenReturn(true);
            when(stmt.isStaticKeyPartitionInsert()).thenReturn(true);
        });
    }

    @Test
    public void dynamicOverwriteHookSkipsExplicitSessionTransaction() throws Exception {
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            when(fixture.insertStmt.isDynamicOverwrite()).thenReturn(true);
            when(fixture.insertStmt.hasOverwriteJob()).thenReturn(true);
            when(fixture.context.getTxnId()).thenReturn(7L);

            InsertPreSplitHook.maybeRunDynamicOverwritePreSplit(fixture.insertStmt, fixture.context, 42L);

            flow.verifyNoInteractions();
        }
    }

    @Test
    public void dynamicOverwriteHookSwallowsUnexpectedFailure() throws Exception {
        // Fail-safe contract: the load must still run when the hook throws.
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.isDynamicOverwrite()).thenThrow(new IllegalStateException("boom"));

        Assertions.assertDoesNotThrow(() -> InsertPreSplitHook.maybeRunDynamicOverwritePreSplit(
                stmt, mockConnectContextWithSessionPreSplit(true), 42L));
    }

    /** Applies {@code stubs} to the fixture's statement and asserts the flow is never entered. */
    private static void assertDynamicOverwriteHookSkips(
            long overwriteTransactionId, Consumer<InsertStmt> stubs) throws Exception {
        try (SourceFixture fixture = sourceFixture();
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            stubs.accept(fixture.insertStmt);

            InsertPreSplitHook.maybeRunDynamicOverwritePreSplit(
                    fixture.insertStmt, fixture.context, overwriteTransactionId);

            flow.verifyNoInteractions();
        }
    }

    // ---------- Shared fixtures ----------

    /**
     * Wires the full resolve path so the hook reaches the WHERE gate / mapping:
     * a single-tier target (k, v), a SELECT * over an OLAP source (k, v), and the
     * static metadata + authorizer mocks needed to resolve both. Tests close it
     * in a finally block.
     */
    private static SourceFixture sourceFixture() {
        return sourceFixture(List.of("k", "v"), List.of("k", "v"));
    }

    private static SourceFixture sourceFixture(List<String> targetColumns, List<String> sourceColumns) {
        return new SourceFixture(targetColumns, sourceColumns);
    }

    /**
     * Bundles the target / source catalog mocks and the open MockedStatic scopes
     * (GlobalStateMgr, MetaUtils, AnalyzerUtils, Authorizer, TabletPreSplitCoordinator)
     * the resolve path needs. The static scopes stay open across the hook
     * invocation; {@link #close()} releases them, so tests open the fixture in a
     * try-with-resources block. The Authorizer scope defaults to no row / column
     * policy; tests override it to drive the policy gates.
     */
    private static final class SourceFixture implements AutoCloseable {
        private final InsertStmt insertStmt;
        private final ConnectContext context;
        private final Database sourceDb;
        private final OlapTable sourceTable;
        private final OlapTable targetTable;
        private final MockedStatic<MetaUtils> metaUtils;
        private final MockedStatic<Authorizer> authorizer;
        private final MockedStatic<TabletPreSplitCoordinator> coordinator;
        private final MockedStatic<com.starrocks.server.GlobalStateMgr> globalStateMgr;
        private final MockedStatic<com.starrocks.sql.analyzer.AnalyzerUtils> analyzerUtils;
        private final com.starrocks.server.MetadataMgr metadataMgr;

        OlapTable target() {
            return targetTable;
        }

        private SourceFixture(List<String> targetColumns, List<String> sourceColumns) {
            this.context = mockConnectContextWithSessionPreSplit(true);
            when(context.isBypassAuthorizerCheck()).thenReturn(true);

            OlapTable target = mock(OlapTable.class);
            this.targetTable = target;

            // Source: OLAP, columns supplied; no generated column by default.
            this.sourceTable = mock(OlapTable.class);
            when(sourceTable.getVisibleColumnsWithoutGeneratedColumn()).thenReturn(columnsOf(sourceColumns));
            when(sourceTable.getBaseSchema()).thenReturn(columnsOf(sourceColumns));
            when(sourceTable.hasGeneratedColumn()).thenReturn(false);
            when(sourceTable.getDataSize()).thenReturn(0L);

            this.sourceDb = mock(Database.class);
            Database targetDb = mock(Database.class);
            when(targetDb.getFullName()).thenReturn("target_db");

            // Source relation: SELECT * FROM src_db.src_t (no alias, no modifier).
            TableRelation sourceRelation = plainTableRelation();
            when(sourceRelation.getName()).thenReturn(new TableName("src_t", "src_db", "src_t"));
            when(sourceRelation.getAlias()).thenReturn(null);
            this.insertStmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
            when(insertStmt.isColumnMatchByName()).thenReturn(false);
            com.starrocks.sql.ast.TableRef tableRef = mock(com.starrocks.sql.ast.TableRef.class);
            when(insertStmt.getTableRef()).thenReturn(tableRef);

            this.globalStateMgr = Mockito.mockStatic(com.starrocks.server.GlobalStateMgr.class);
            this.metaUtils = Mockito.mockStatic(MetaUtils.class);
            this.analyzerUtils = Mockito.mockStatic(com.starrocks.sql.analyzer.AnalyzerUtils.class);
            this.authorizer = Mockito.mockStatic(Authorizer.class);
            this.coordinator = Mockito.mockStatic(TabletPreSplitCoordinator.class);

            com.starrocks.server.GlobalStateMgr globalState = mock(com.starrocks.server.GlobalStateMgr.class);
            com.starrocks.server.MetadataMgr metadataMgr = mock(com.starrocks.server.MetadataMgr.class);
            this.metadataMgr = metadataMgr;
            when(globalState.getMetadataMgr()).thenReturn(metadataMgr);
            globalStateMgr.when(com.starrocks.server.GlobalStateMgr::getCurrentState).thenReturn(globalState);
            when(globalState.getFunction(any(), any())).thenAnswer(invocation ->
                    REAL_GLOBAL_STATE.getFunction(invocation.getArgument(0), invocation.getArgument(1)));
            when(globalState.getVariableMgr()).thenReturn(REAL_GLOBAL_STATE.getVariableMgr());
            when(globalState.getSqlParser()).thenReturn(REAL_GLOBAL_STATE.getSqlParser());
            // Target db resolves first, then source db (both via getDb).
            when(metadataMgr.getDb(any(), any(), eq("target_db"))).thenReturn(targetDb);
            when(metadataMgr.getDb(any(), any(), eq("src_db"))).thenReturn(sourceDb);

            // Wire a default-warehouse compute resource so the target-auth block
            // (reached only when the bypass flag is off) does not NPE and skips
            // the non-default-warehouse USAGE check.
            ComputeResource computeResource = mock(ComputeResource.class);
            when(computeResource.getWarehouseId())
                    .thenReturn(com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID);
            when(context.getCurrentComputeResource()).thenReturn(computeResource);
            com.starrocks.server.WarehouseManager warehouseManager =
                    mock(com.starrocks.server.WarehouseManager.class);
            com.starrocks.warehouse.Warehouse defaultWarehouse = mock(com.starrocks.warehouse.Warehouse.class);
            when(defaultWarehouse.getId()).thenReturn(com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID);
            when(warehouseManager.getWarehouse(anyLong())).thenReturn(defaultWarehouse);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);

            com.starrocks.sql.ast.TableRef normalizedRef = mock(com.starrocks.sql.ast.TableRef.class);
            when(normalizedRef.getCatalogName()).thenReturn("default_catalog");
            when(normalizedRef.getDbName()).thenReturn("target_db");
            analyzerUtils.when(() -> com.starrocks.sql.analyzer.AnalyzerUtils.normalizedTableRef(any(), any()))
                    .thenReturn(normalizedRef);

            stubEligibleTarget(target, "target_t", targetColumns);
            metaUtils.when(() -> MetaUtils.getSessionAwareTable(any(), eq(targetDb), any())).thenReturn(target);
            metaUtils.when(() -> MetaUtils.getSessionAwareTable(any(), eq(sourceDb), any())).thenReturn(sourceTable);

            // Default: source is authorized with no row / column policy attached.
            authorizer.when(() -> Authorizer.getRowAccessPolicy(any(), any())).thenReturn(null);
            authorizer.when(() -> Authorizer.getColumnMaskingPolicy(any(), any(), any())).thenReturn(Map.of());
        }

        /** Restricts the fixture's source relation to the named (formal) partitions. */
        private void restrictSourceTo(List<String> partitionNames) {
            TableRelation relation = (TableRelation) ((SelectRelation) insertStmt.getQueryStatement()
                    .getQueryRelation()).getRelation();
            when(relation.getPartitionNames())
                    .thenReturn(new PartitionRef(partitionNames, false, NodePosition.ZERO));
        }

        /**
         * Registers a formal source partition with one physical partition whose base index holds
         * {@code baseRows} rows and, when {@code rollupRows} is non-null, a rollup holding that many.
         */
        private Partition sourcePartition(String name, long id, long dataSize, long baseRows, Long rollupRows) {
            Partition partition = mock(Partition.class);
            when(partition.getId()).thenReturn(id);
            when(partition.getDataSize()).thenReturn(dataSize);
            PhysicalPartition physicalPartition = mock(PhysicalPartition.class);
            MaterializedIndex baseIndex = mock(MaterializedIndex.class);
            when(baseIndex.getRowCount()).thenReturn(baseRows);
            when(physicalPartition.getQueryableBaseIndex()).thenReturn(baseIndex);
            when(partition.getSubPartitions()).thenReturn(List.of(physicalPartition));
            when(sourceTable.getPartition(name, false)).thenReturn(partition);
            return partition;
        }

        /** Stubs {@code table} as an eligible target: single-tier, range-distribution, NORMAL, one index, scalar sort key. */
        private void stubEligibleTarget(OlapTable table, String name, List<String> columnNames) {
            when(table.getName()).thenReturn(name);
            when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
            when(table.isRangeDistribution()).thenReturn(true);
            when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
            com.starrocks.catalog.MaterializedIndexMeta baseMeta = mock(com.starrocks.catalog.MaterializedIndexMeta.class);
            when(baseMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
            when(table.getVisibleIndexMetas()).thenReturn(List.of(baseMeta));
            when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
            when(table.getBaseSchemaWithoutGeneratedColumn()).thenReturn(columnsOf(columnNames));
            PartitionInfo partitionInfo = mock(PartitionInfo.class);
            when(partitionInfo.isPartitioned()).thenReturn(false);
            when(partitionInfo.getPartitionColumns(any())).thenReturn(List.of());
            when(table.getPartitionInfo()).thenReturn(partitionInfo);
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(bigintColumn("k")));
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(eq(table), eq(BASE_INDEX_META_ID)))
                    .thenReturn(List.of(bigintColumn("k")));
        }

        /**
         * Re-points the target resolution at a materialized view with the same eligible shape as the
         * default target, so only its being a view -- and its row-id strategy -- can decline it.
         */
        private MaterializedView withMaterializedViewTarget(RowIdStrategy rowIdStrategy) {
            MaterializedView view = mock(MaterializedView.class);
            stubEligibleTarget(view, "mv_target", List.of("k", "v"));
            when(view.getRowIdStrategy()).thenReturn(rowIdStrategy);
            metaUtils.when(() -> MetaUtils.getSessionAwareTable(any(), Mockito.argThat(db -> db != sourceDb), any()))
                    .thenReturn(view);
            return view;
        }

        /** Drives the static-overwrite entry point the way InsertOverwriteJobRunner does for this statement. */
        private void runStaticOverwrite(boolean system) {
            when(insertStmt.isOverwrite()).thenReturn(true);
            when(insertStmt.hasOverwriteJob()).thenReturn(true);
            when(insertStmt.isSystem()).thenReturn(system);
            InsertPreSplitHook.maybeRunStaticOverwritePreSplit(insertStmt, context,
                    List.of("mv_target"), List.of("tmp_mv_target"), Estimates.ZERO);
        }

        /**
         * Drives the public entry point (so the fail-safe try/catch applies)
         * and asserts the hook never delegated to the coordinator through
         * either the single-partition ({@code submitAsynchronously}) or the
         * multi-partition ({@code submitForPartitionsCombined}) path.
         */
        private void assertNoSubmit() {
            InsertPreSplitHook.maybeRunPreSplit(insertStmt, context);
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt(), any()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any(), any(), any()), never());
        }

        /**
         * Drives {@link TablePreSplitSource#prepare} directly against the wired
         * resolve path and returns the built {@link InsertFromTableScanContext}
         * (or {@code null} when prepare skipped). The {@code database} argument is
         * unused by {@code prepare} (it resolves the source db itself), so a bare
         * mock suffices.
         */
        private InsertFromTableScanContext prepareScanContext() throws AccessDeniedException {
            PreSplitFlow.Prepared prepared = prepare();
            return prepared == null ? null : (InsertFromTableScanContext) prepared.scanContext();
        }

        /**
         * Drives {@link TablePreSplitSource#prepare} directly and returns the full
         * {@link PreSplitFlow.Prepared} (or {@code null} when prepare skipped), so a test
         * can inspect the built {@code secondaryIndexSpecs}.
         */
        private PreSplitFlow.Prepared prepare() throws AccessDeniedException {
            SelectRelation selectRelation =
                    (SelectRelation) insertStmt.getQueryStatement().getQueryRelation();
            return new TablePreSplitSource().prepare(
                    insertStmt, selectRelation, targetTable, mock(Database.class), context);
        }

        /**
         * Re-points the source at an external-catalog Hive table with the fixture's (k, v) schema
         * and makes the connector statistics answer with {@code statistics}.
         */
        private void withExternalSource(Statistics statistics) {
            Table external = externalTable("hive_catalog", Table.TableType.HIVE);
            metaUtils.when(() -> MetaUtils.getSessionAwareTable(any(), eq(sourceDb), any())).thenReturn(external);
            stubStatistics(metadataMgr, statistics);
        }

        /**
         * Adds one visible rollup ({@code rollupMetaId}) with the given sort-key columns to
         * the target's visible-index set, alongside the base index, so prepare builds a
         * secondary index spec for it.
         */
        private void withVisibleRollup(long rollupMetaId, List<String> rollupSortKeyColumnNames) {
            com.starrocks.catalog.MaterializedIndexMeta baseMeta =
                    mock(com.starrocks.catalog.MaterializedIndexMeta.class);
            when(baseMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
            com.starrocks.catalog.MaterializedIndexMeta rollupMeta =
                    mock(com.starrocks.catalog.MaterializedIndexMeta.class);
            when(rollupMeta.getIndexMetaId()).thenReturn(rollupMetaId);
            when(targetTable.getVisibleIndexMetas()).thenReturn(List.of(baseMeta, rollupMeta));
            when(targetTable.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(eq(targetTable), eq(rollupMetaId)))
                    .thenReturn(columnsOf(rollupSortKeyColumnNames));
        }

        /**
         * Target (k, v, dt DATETIME) plus dt_month AS date_trunc('month', dt), PARTITION BY (dt_month), from source
         * (k, v, dt DATETIME). The columns are built at class load: parsing needs the GlobalStateMgr this fixture mocks.
         */
        private void withGeneratedPartitionColumn() {
            stubGeneratedSchema(targetTable, List.of(GEN_K, GEN_V, GEN_DT), GEN_DT_MONTH);
            when(targetTable.getPartitionInfo().getPartitionColumns(any())).thenReturn(List.of(GEN_DT_MONTH));
            when(sourceTable.getVisibleColumnsWithoutGeneratedColumn()).thenReturn(List.of(GEN_K, GEN_V, GEN_DT));
            when(sourceTable.getBaseSchema()).thenReturn(List.of(GEN_K, GEN_V, GEN_DT));
        }

        /**
         * Re-points the source resolution so the source table resolves to the SAME
         * instance as the target (INSERT INTO t SELECT * FROM t), stubbing the
         * source-side accessors prepare reads on the target.
         */
        private void resolveSourceToTarget() {
            when(targetTable.getVisibleColumnsWithoutGeneratedColumn())
                    .thenReturn(columnsOf(List.of("k", "v")));
            when(targetTable.getBaseSchema()).thenReturn(columnsOf(List.of("k", "v")));
            when(targetTable.hasGeneratedColumn()).thenReturn(false);
            when(targetTable.getDataSize()).thenReturn(0L);
            metaUtils.when(() -> MetaUtils.getSessionAwareTable(any(), eq(sourceDb), any()))
                    .thenReturn(targetTable);
        }

        @Override
        public void close() {
            coordinator.close();
            authorizer.close();
            analyzerUtils.close();
            metaUtils.close();
            globalStateMgr.close();
        }
    }

    private void assertNonDeterministicWhereSkips(Expr whereClause) throws Exception {
        try (SourceFixture fixture = sourceFixture()) {
            QueryRelation queryRelation = fixture.insertStmt.getQueryStatement().getQueryRelation();
            when(((SelectRelation) queryRelation).getWhereClause()).thenReturn(whereClause);
            fixture.assertNoSubmit();
        }
    }

    private static List<Column> columnsOf(List<String> names) {
        return names.stream().map(PresplitTestSupport::bigintColumn).collect(java.util.stream.Collectors.toList());
    }

    // ---------- AST-shape builders (mirror InsertPreSplitHookFilesTest) ----------

    private static InsertStmt simpleTableInsertStmt() {
        return insertStmtWithQueryRelation(bareStarSelectRelationOver(plainTableRelation()));
    }

    private static TableRelation plainTableRelation() {
        TableRelation relation = mock(TableRelation.class);
        when(relation.getPartitionNames()).thenReturn(null);
        when(relation.getTabletIds()).thenReturn(List.of());
        when(relation.getReplicaIds()).thenReturn(List.of());
        when(relation.getTableHints()).thenReturn(java.util.Set.of());
        when(relation.getSampleClause()).thenReturn(null);
        when(relation.getQueryPeriod()).thenReturn(null);
        when(relation.getQueryPeriodString()).thenReturn(null);
        when(relation.getTvrVersionRange()).thenReturn(null);
        when(relation.getGtid()).thenReturn(0L);
        return relation;
    }

    private static SelectRelation bareStarSelectRelationOver(Relation from) {
        SelectListItem starItem = mock(SelectListItem.class);
        when(starItem.isStar()).thenReturn(true);
        when(starItem.getExcludedColumns()).thenReturn(List.of());
        return selectRelationWithSelectList(selectListOf(starItem), from);
    }

    private static SelectRelation selectRelationWithSelectList(SelectList selectList, Relation from) {
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

    private static SelectListItem bareColumnItem(String columnName) {
        SlotRef slotRef = mock(SlotRef.class);
        when(slotRef.getColName()).thenReturn(columnName);
        SelectListItem item = mock(SelectListItem.class);
        when(item.isStar()).thenReturn(false);
        when(item.getExpr()).thenReturn(slotRef);
        return item;
    }

    private static InsertStmt insertStmtWithQueryRelation(QueryRelation queryRelation) {
        QueryStatement queryStatement = mock(QueryStatement.class);
        when(queryStatement.getQueryRelation()).thenReturn(queryRelation);
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTxnId()).thenReturn(DmlStmt.INVALID_TXN_ID);
        when(stmt.getQueryStatement()).thenReturn(queryStatement);
        when(stmt.getTargetColumnNames()).thenReturn(null);
        return stmt;
    }
}
