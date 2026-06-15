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

import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
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
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.assertHookDoesNotDelegate;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.mockConnectContextWithSessionPreSplit;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Detection-side and flow coverage for {@link InsertFromTablePreSplitHook}. The
 * detection tests drive each early-return branch and assert the hook never
 * reaches {@link TabletPreSplitCoordinator#submitAsynchronously}. The flow tests
 * drive the private single/multi-partition methods via reflection (mirroring
 * {@link InsertFromFilesPreSplitHookPartitionedTest}) and assert delegation to
 * the coordinator with arg-captors. End-to-end coverage (catalog, partitions,
 * tablet inverted index, compute-resource-bound ConnectContext) lives in the TSP
 * regression suite.
 */
public class InsertFromTablePreSplitHookTest {

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
                    InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));

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
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testReadOnlyExplainShortCircuits() throws Exception {
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.isExplain()).thenReturn(true);
        when(stmt.getExplainLevel()).thenReturn(ExplainLevel.NORMAL);

        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testInsertOverwriteFirstPassShortCircuits() throws Exception {
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.isOverwrite()).thenReturn(true);
        when(stmt.hasOverwriteJob()).thenReturn(false);

        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testExplicitSessionTransactionShortCircuits() throws Exception {
        InsertStmt stmt = simpleTableInsertStmt();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getTxnId()).thenReturn(42L);

        assertHookDoesNotDelegate(() -> InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, context));
    }

    @Test
    public void testPreSetStmtTxnIdShortCircuits() throws Exception {
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.getTxnId()).thenReturn(99L);

        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testTargetPartitionSpecShortCircuits() throws Exception {
        // INSERT INTO t PARTITION(p1) SELECT * FROM src restricts the load to
        // specific target partitions. Pre-split must not sample the whole source
        // and pre-create partitions outside that set.
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.isSpecifyPartitionNames()).thenReturn(true);

        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testStaticKeyPartitionInsertShortCircuits() throws Exception {
        // INSERT INTO t PARTITION(k=1) SELECT ... is static key-partition syntax;
        // same concern: the load targets a fixed partition set.
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.isStaticKeyPartitionInsert()).thenReturn(true);

        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    // ---------- extractSingleTableSource: query-shape filters ----------

    @Test
    public void testNullQueryStatementShortCircuits() throws Exception {
        InsertStmt stmt = mock(InsertStmt.class);
        when(stmt.getTxnId()).thenReturn(DmlStmt.INVALID_TXN_ID);
        when(stmt.getQueryStatement()).thenReturn(null);

        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testCteInQueryRelationShortCircuits() throws Exception {
        SelectRelation selectRelation = mock(SelectRelation.class);
        when(selectRelation.getCteRelations()).thenReturn(List.of(mock(CTERelation.class)));

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testSetOperationQueryRelationShortCircuits() throws Exception {
        // INSERT INTO t SELECT ... UNION ALL SELECT ... — non-SelectRelation queryRelation.
        SetOperationRelation setOp = mock(SetOperationRelation.class);
        when(setOp.getCteRelations()).thenReturn(List.of());
        InsertStmt stmt = insertStmtWithQueryRelation(setOp);

        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testJoinSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM a JOIN b — the FROM is not a single TableRelation.
        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(mock(Relation.class)));
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testFilesSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM FILES(...) — handled by the FILES hook, not this one.
        InsertStmt stmt = insertStmtWithQueryRelation(
                bareStarSelectRelationOver(mock(FileTableFunctionRelation.class)));
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testTargetColumnListShortCircuits() throws Exception {
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.getTargetColumnNames()).thenReturn(List.of("a", "b"));

        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testExpressionProjectionShortCircuits() throws Exception {
        // INSERT INTO t SELECT col + 1 FROM src — a non-SlotRef expression item.
        SelectListItem exprItem = mock(SelectListItem.class);
        when(exprItem.isStar()).thenReturn(false);
        when(exprItem.getExpr()).thenReturn(mock(FunctionCallExpr.class));

        InsertStmt stmt = insertStmtWithQueryRelation(
                selectRelationWithSelectList(selectListOf(exprItem), mock(TableRelation.class)));
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
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
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testGroupByClauseShortCircuits() throws Exception {
        SelectRelation selectRelation = bareStarSelectRelationOver(plainTableRelation());
        when(selectRelation.hasGroupByClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testHavingClauseShortCircuits() throws Exception {
        SelectRelation selectRelation = bareStarSelectRelationOver(plainTableRelation());
        when(selectRelation.hasHavingClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testOrderByClauseShortCircuits() throws Exception {
        SelectRelation selectRelation = bareStarSelectRelationOver(plainTableRelation());
        when(selectRelation.hasOrderByClause()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testLimitClauseShortCircuits() throws Exception {
        SelectRelation selectRelation = bareStarSelectRelationOver(plainTableRelation());
        when(selectRelation.hasLimit()).thenReturn(true);

        InsertStmt stmt = insertStmtWithQueryRelation(selectRelation);
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    // ---------- isPlainTableReference: source-slice modifiers ----------

    @Test
    public void testPartitionModifierOnSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src PARTITION(p1) — explicit partition slice.
        TableRelation sourceRelation = plainTableRelation();
        when(sourceRelation.getPartitionNames())
                .thenReturn(mock(com.starrocks.sql.ast.PartitionRef.class));

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testTabletModifierOnSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src TABLET(123) — explicit tablet slice.
        TableRelation sourceRelation = plainTableRelation();
        when(sourceRelation.getTabletIds()).thenReturn(List.of(123L));

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testTableSampleModifierOnSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src TABLESAMPLE(...) — sampling slice.
        TableRelation sourceRelation = plainTableRelation();
        when(sourceRelation.getSampleClause())
                .thenReturn(mock(com.starrocks.sql.ast.TableSampleClause.class));

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    @Test
    public void testTemporalModifierOnSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src FOR VERSION AS OF ... — time-travel slice.
        TableRelation sourceRelation = plainTableRelation();
        when(sourceRelation.getQueryPeriod())
                .thenReturn(mock(com.starrocks.sql.ast.QueryPeriod.class));

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
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
        try (SourceFixture fixture = sourceFixture()) {
            // Override the target resolution to return a MaterializedView instead of OlapTable.
            MaterializedView mv = mock(MaterializedView.class);
            when(mv.getName()).thenReturn("mv_target");
            fixture.metaUtils.when(() -> MetaUtils.getSessionAwareTable(
                            any(), Mockito.argThat(db -> db != fixture.sourceDb), any()))
                    .thenReturn(mv);

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
                        InsertFromTablePreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)),
                "hook must never propagate a throw");
    }

    // ---------- Single-partition flow (driven via reflection) ----------

    @Test
    public void runSinglePartitionFlowSubmittedDrivesAwaitOnce() throws Exception {
        // A Submitted outcome MUST drive awaitFinishedAllowingFallback exactly
        // once. The pipeline factory + coordinator are stubbed; the await helper
        // semantics are covered by InsertFromFilesPreSplitHookPartitionedTest.
        Database database = mock(Database.class);
        OlapTable table = mockSingleTierTarget();
        OlapTable sourceTable = mockSourceOlapTable();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

        InsertSelectSourceColumns mapping = mappingOf(List.of("k"), List.of());
        PreSplitTargets.EligibleTarget eligibleTarget =
                new PreSplitTargets.EligibleTarget(database, table, /*partitionId*/ 5L, /*oldTabletId*/ 9L);
        DefaultPreSplitPipeline pipeline = mock(DefaultPreSplitPipeline.class);
        PreSplitPipeline.PreparedReshardJob preparedJob = mock(PreSplitPipeline.PreparedReshardJob.class);

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<PreSplitTargets> targets = Mockito.mockStatic(PreSplitTargets.class);
                MockedStatic<DefaultPreSplitPipeline> pipelineFactory =
                        Mockito.mockStatic(DefaultPreSplitPipeline.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            targets.when(() -> PreSplitTargets.findEligibleTarget(database, table)).thenReturn(eligibleTarget);
            pipelineFactory.when(() -> DefaultPreSplitPipeline.forLoadKind(
                            any(), any(), anyLong(), anyLong(), eq(LoadKind.INSERT_FROM_TABLE)))
                    .thenReturn(pipeline);
            coordinator.when(() -> TabletPreSplitCoordinator.submitAsynchronously(
                            any(), any(), anyLong(), any(), eq(LoadKind.INSERT_FROM_TABLE), any(), anyInt()))
                    .thenReturn(new PreSplitOutcome.Submitted(preparedJob));

            invokeRunSinglePartitionFlow(database, table, sourceTable, "`db`.`src`", null, mapping, context);

            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), eq(LoadKind.INSERT_FROM_TABLE), any(), anyInt()), times(1));
            coordinator.verify(() -> TabletPreSplitCoordinator.awaitFinishedAllowingFallback(
                    eq(LoadKind.INSERT_FROM_TABLE), eq(table), eq(pipeline), eq(preparedJob), any()), times(1));
        }
    }

    @Test
    public void runSinglePartitionFlowCarriesScanContextSourceColumns() throws Exception {
        // Reordered bare columns: the captured scan context must carry the source
        // column the mapping resolved (not the target column's name).
        Database database = mock(Database.class);
        OlapTable table = mockSingleTierTarget();
        OlapTable sourceTable = mockSourceOlapTable();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

        // Target sort key "k" maps to source column "k_src" after reordering.
        InsertSelectSourceColumns mapping = mappingOf(List.of("k_src"), List.of("p_src"));
        PreSplitTargets.EligibleTarget eligibleTarget =
                new PreSplitTargets.EligibleTarget(database, table, 5L, 9L);
        AtomicReference<InsertFromTableScanContext> capturedScan = new AtomicReference<>();

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<PreSplitTargets> targets = Mockito.mockStatic(PreSplitTargets.class);
                MockedStatic<DefaultPreSplitPipeline> pipelineFactory =
                        Mockito.mockStatic(DefaultPreSplitPipeline.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            targets.when(() -> PreSplitTargets.findEligibleTarget(database, table)).thenReturn(eligibleTarget);
            pipelineFactory.when(() -> DefaultPreSplitPipeline.forLoadKind(
                            any(), any(), anyLong(), anyLong(), eq(LoadKind.INSERT_FROM_TABLE)))
                    .thenReturn(mock(DefaultPreSplitPipeline.class));
            coordinator.when(() -> TabletPreSplitCoordinator.submitAsynchronously(
                            any(), any(), anyLong(), any(), any(), any(), anyInt()))
                    .thenAnswer(invocation -> {
                        capturedScan.set(invocation.getArgument(3));
                        return new PreSplitOutcome.Skipped(SkipReason.NO_USEFUL_CUTS);
                    });

            invokeRunSinglePartitionFlow(
                    database, table, sourceTable, "`db`.`src`", "`a` > 10", mapping, context);

            Assertions.assertNotNull(capturedScan.get(), "scan context must be captured at submit");
            Assertions.assertEquals(List.of("k_src"), capturedScan.get().sortKeySourceColumnNames(),
                    "scan context must carry the resolved source sort-key column");
            Assertions.assertEquals(List.of("p_src"), capturedScan.get().partitionSourceColumnNames());
            Assertions.assertEquals("`a` > 10", capturedScan.get().wherePredicateSql());
        }
    }

    @Test
    public void runSinglePartitionFlowNoEligibleTargetDoesNotSubmit() throws Exception {
        Database database = mock(Database.class);
        OlapTable table = mockSingleTierTarget();
        OlapTable sourceTable = mockSourceOlapTable();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);

        try (MockedStatic<PreSplitTargets> targets = Mockito.mockStatic(PreSplitTargets.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            targets.when(() -> PreSplitTargets.findEligibleTarget(database, table)).thenReturn(null);

            invokeRunSinglePartitionFlow(
                    database, table, sourceTable, "`db`.`src`", null, mappingOf(List.of("k"), List.of()), context);

            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
        }
    }

    @Test
    public void runSinglePartitionFlowSourceEqualsTargetStillSubmits() throws Exception {
        // INSERT INTO t SELECT * FROM t WHERE ... — the source resolves to the
        // SAME OlapTable instance as the target. The hook holds no locks, so
        // there is no deadlock to exercise; this confirms the single-partition
        // flow still submits + awaits normally when source == target.
        Database database = mock(Database.class);
        OlapTable sameTable = mockSingleTierTarget();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

        InsertSelectSourceColumns mapping = mappingOf(List.of("k"), List.of());
        PreSplitTargets.EligibleTarget eligibleTarget =
                new PreSplitTargets.EligibleTarget(database, sameTable, /*partitionId*/ 5L, /*oldTabletId*/ 9L);
        DefaultPreSplitPipeline pipeline = mock(DefaultPreSplitPipeline.class);
        PreSplitPipeline.PreparedReshardJob preparedJob = mock(PreSplitPipeline.PreparedReshardJob.class);

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<PreSplitTargets> targets = Mockito.mockStatic(PreSplitTargets.class);
                MockedStatic<DefaultPreSplitPipeline> pipelineFactory =
                        Mockito.mockStatic(DefaultPreSplitPipeline.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            targets.when(() -> PreSplitTargets.findEligibleTarget(database, sameTable)).thenReturn(eligibleTarget);
            pipelineFactory.when(() -> DefaultPreSplitPipeline.forLoadKind(
                            any(), any(), anyLong(), anyLong(), eq(LoadKind.INSERT_FROM_TABLE)))
                    .thenReturn(pipeline);
            coordinator.when(() -> TabletPreSplitCoordinator.submitAsynchronously(
                            any(), any(), anyLong(), any(), eq(LoadKind.INSERT_FROM_TABLE), any(), anyInt()))
                    .thenReturn(new PreSplitOutcome.Submitted(preparedJob));

            // source table == target table, with a WHERE predicate.
            invokeRunSinglePartitionFlow(
                    database, sameTable, sameTable, "`db`.`t`", "`k` > 100", mapping, context);

            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), eq(LoadKind.INSERT_FROM_TABLE), any(), anyInt()), times(1));
            coordinator.verify(() -> TabletPreSplitCoordinator.awaitFinishedAllowingFallback(
                    eq(LoadKind.INSERT_FROM_TABLE), eq(sameTable), eq(pipeline), eq(preparedJob), any()), times(1));
        }
    }

    // ---------- Multi-partition flow (driven via reflection) ----------

    @Test
    public void runMultiPartitionFlowSubmittedDrivesAwaitOnce() throws Exception {
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);
        OlapTable table = mockPartitionedTarget();
        OlapTable sourceTable = mockSourceOlapTable();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

        InsertSelectSourceColumns mapping = mappingOf(List.of("k"), List.of("p"));
        SampleSet samples = new SampleSet(List.of(), List.of(), Estimates.ZERO);
        TabletReshardJob combinedJob = mock(TabletReshardJob.class);

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenReturn(samples))) {
            grouper.when(() -> PartitionSampleGrouper.group(
                            any(SampleSet.class), any(OlapTable.class), any(ConnectContext.class),
                            anyLong(), anyLong()))
                    .thenReturn(List.of(mock(PartitionSamples.class)));
            coordinator.when(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                            any(), any(), anyList(), anyInt(), any()))
                    .thenReturn(new PreSplitOutcome.SubmittedCombined(combinedJob, List.of()));

            invokeRunMultiPartitionFlow(
                    database, table, sourceTable, "`db`.`src`", null, mapping,
                    List.of(bigintColumn("k")), List.of(bigintColumn("p")), context);

            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), times(1));
            coordinator.verify(() -> TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                    eq(LoadKind.INSERT_FROM_TABLE), eq(table), eq(combinedJob), any()), times(1));
        }
    }

    @Test
    public void runMultiPartitionFlowSamplerThrowDoesNotPropagate() throws Exception {
        // The sampler throwing inside the flow must not propagate: runDataTierSampler
        // catches it, records SAMPLE_FAILED, returns null, and the flow short-circuits
        // before the grouper / coordinator.
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);
        OlapTable table = mockPartitionedTarget();
        OlapTable sourceTable = mockSourceOlapTable();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

        InsertSelectSourceColumns mapping = mappingOf(List.of("k"), List.of("p"));

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class)))
                                .thenThrow(new RuntimeException("synthetic sample failure")))) {
            invokeRunMultiPartitionFlow(
                    database, table, sourceTable, "`db`.`src`", null, mapping,
                    List.of(bigintColumn("k")), List.of(bigintColumn("p")), context);

            grouper.verify(() -> PartitionSampleGrouper.group(
                    any(), any(), any(), anyLong(), anyLong()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), never());
        }
    }

    // ---------- Reflection drivers for the private flow methods ----------

    private static void invokeRunSinglePartitionFlow(
            Database database, OlapTable table, OlapTable sourceTable, String sourceFromSql,
            String wherePredicateSql, InsertSelectSourceColumns mapping, ConnectContext context) throws Exception {
        InsertFromTableScanContext scanContext = scanContextOf(sourceTable, sourceFromSql, wherePredicateSql, mapping,
                context);
        Method method = InsertFromTablePreSplitHook.class.getDeclaredMethod(
                "runSinglePartitionFlow", Database.class, OlapTable.class, OlapTable.class,
                InsertFromTableScanContext.class, ConnectContext.class);
        method.setAccessible(true);
        try {
            method.invoke(null, database, table, sourceTable, scanContext, context);
        } catch (InvocationTargetException invocationFailure) {
            Throwable cause = invocationFailure.getCause();
            throw cause instanceof Exception ? (Exception) cause : new RuntimeException(cause);
        }
    }

    private static void invokeRunMultiPartitionFlow(
            Database database, OlapTable table, OlapTable sourceTable, String sourceFromSql,
            String wherePredicateSql, InsertSelectSourceColumns mapping,
            List<Column> sortKeyColumns, List<Column> partitionColumns, ConnectContext context) throws Exception {
        InsertFromTableScanContext scanContext = scanContextOf(sourceTable, sourceFromSql, wherePredicateSql, mapping,
                context);
        Method method = InsertFromTablePreSplitHook.class.getDeclaredMethod(
                "runMultiPartitionFlow", Database.class, OlapTable.class, OlapTable.class,
                InsertFromTableScanContext.class, List.class, List.class, ConnectContext.class);
        method.setAccessible(true);
        try {
            method.invoke(null, database, table, sourceTable, scanContext,
                    sortKeyColumns, partitionColumns, context);
        } catch (InvocationTargetException invocationFailure) {
            Throwable cause = invocationFailure.getCause();
            throw cause instanceof Exception ? (Exception) cause : new RuntimeException(cause);
        }
    }

    /**
     * Builds the shared scan context the way {@code tryRunPreSplit} does before
     * the partition branch, so the reflection drivers can exercise the flow
     * methods against their post-cleanup signatures. Falls back to a fresh
     * compute-resource mock when the context does not stub one (the scan-context
     * record requires a non-null compute resource).
     */
    private static InsertFromTableScanContext scanContextOf(
            OlapTable sourceTable, String sourceFromSql, String wherePredicateSql,
            InsertSelectSourceColumns mapping, ConnectContext context) {
        ComputeResource computeResource = context.getCurrentComputeResource();
        if (computeResource == null) {
            computeResource = mock(ComputeResource.class);
        }
        return new InsertFromTableScanContext(
                sourceTable, sourceFromSql, mapping.sortKeySourceColumnNames(),
                mapping.partitionSourceColumnNames(), wherePredicateSql, computeResource);
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
        private final MockedStatic<MetaUtils> metaUtils;
        private final MockedStatic<Authorizer> authorizer;
        private final MockedStatic<TabletPreSplitCoordinator> coordinator;
        private final MockedStatic<com.starrocks.server.GlobalStateMgr> globalStateMgr;
        private final MockedStatic<com.starrocks.sql.analyzer.AnalyzerUtils> analyzerUtils;

        private SourceFixture(List<String> targetColumns, List<String> sourceColumns) {
            this.context = mockConnectContextWithSessionPreSplit(true);
            when(context.isBypassAuthorizerCheck()).thenReturn(true);

            // Target: single-tier, range-distribution, NORMAL, one index, scalar sort key.
            OlapTable target = mock(OlapTable.class);
            when(target.getName()).thenReturn("target_t");
            when(target.isCloudNativeTableOrMaterializedView()).thenReturn(true);
            when(target.isRangeDistribution()).thenReturn(true);
            when(target.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
            when(target.getVisibleIndexMetas()).thenReturn(List.of(mock(com.starrocks.catalog.MaterializedIndexMeta.class)));
            when(target.getBaseSchemaWithoutGeneratedColumn()).thenReturn(columnsOf(targetColumns));
            PartitionInfo targetPartitionInfo = mock(PartitionInfo.class);
            when(targetPartitionInfo.isPartitioned()).thenReturn(false);
            when(targetPartitionInfo.getPartitionColumns(any())).thenReturn(List.of());
            when(target.getPartitionInfo()).thenReturn(targetPartitionInfo);

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
            when(globalState.getMetadataMgr()).thenReturn(metadataMgr);
            globalStateMgr.when(com.starrocks.server.GlobalStateMgr::getCurrentState).thenReturn(globalState);
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

            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target))
                    .thenReturn(List.of(bigintColumn("k")));
            metaUtils.when(() -> MetaUtils.getSessionAwareTable(any(), eq(targetDb), any())).thenReturn(target);
            metaUtils.when(() -> MetaUtils.getSessionAwareTable(any(), eq(sourceDb), any())).thenReturn(sourceTable);

            // Default: source is authorized with no row / column policy attached.
            authorizer.when(() -> Authorizer.getRowAccessPolicy(any(), any())).thenReturn(null);
            authorizer.when(() -> Authorizer.getColumnMaskingPolicy(any(), any(), any())).thenReturn(Map.of());
        }

        /**
         * Drives the public entry point (so the fail-safe try/catch applies)
         * and asserts the hook never delegated to the coordinator through
         * either the single-partition ({@code submitAsynchronously}) or the
         * multi-partition ({@code submitForPartitionsCombined}) path.
         */
        private void assertNoSubmit() {
            InsertFromTablePreSplitHook.maybeRunPreSplit(insertStmt, context);
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), never());
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

    private static OlapTable mockSingleTierTarget() {
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("target_t");
        return table;
    }

    private static OlapTable mockPartitionedTarget() {
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("partitioned_target_t");
        return table;
    }

    private static OlapTable mockSourceOlapTable() {
        OlapTable sourceTable = mock(OlapTable.class);
        when(sourceTable.getDataSize()).thenReturn(0L);
        return sourceTable;
    }

    private static InsertSelectSourceColumns mappingOf(
            List<String> sortKeySourceColumns, List<String> partitionSourceColumns) throws Exception {
        java.lang.reflect.Constructor<InsertSelectSourceColumns> constructor =
                InsertSelectSourceColumns.class.getDeclaredConstructor(List.class, List.class);
        constructor.setAccessible(true);
        return constructor.newInstance(sortKeySourceColumns, partitionSourceColumns);
    }

    private static List<Column> columnsOf(List<String> names) {
        return names.stream().map(PresplitTestSupport::bigintColumn).collect(java.util.stream.Collectors.toList());
    }

    // ---------- AST-shape builders (mirror InsertFromFilesPreSplitHookTest) ----------

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
