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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
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
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

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
    public void testInsertWithLoadPropertiesShortCircuits() throws Exception {
        // INSERT PROPERTIES(strict_mode=true) ... — load properties are only validated by
        // InsertAnalyzer.analyzeProperties after this hook, so pre-splitting for a statement that
        // may fail property validation is wrong. Skip conservatively when any property is present.
        // The gate reads the parse-time key set, not getProperties(), which the analyzer overwrites.
        InsertStmt stmt = simpleTableInsertStmt();
        when(stmt.getUserSpecifiedPropertyKeys()).thenReturn(Set.of("strict_mode"));

        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
    }

    // ---------- extractSingleTableSource: query-shape filters ----------

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
    public void testPartitionModifierOnSourceShortCircuits() throws Exception {
        // INSERT INTO t SELECT * FROM src PARTITION(p1) — explicit partition slice.
        TableRelation sourceRelation = plainTableRelation();
        when(sourceRelation.getPartitionNames())
                .thenReturn(mock(com.starrocks.sql.ast.PartitionRef.class));

        InsertStmt stmt = insertStmtWithQueryRelation(bareStarSelectRelationOver(sourceRelation));
        assertHookDoesNotDelegate(() ->
                InsertPreSplitHook.maybeRunPreSplit(stmt, mockConnectContextWithSessionPreSplit(true)));
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

        Assertions.assertTrue(TablePreSplitSource.isSupportedSourceTable(icebergTable));
        Assertions.assertEquals(new Estimates(872000000000L, 3500000000L),
                TablePreSplitSource.sourceEstimates(icebergTable));
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
                TablePreSplitSource.sourceEstimates(mockIcebergTableWithSummary(null)));
    }

    @Test
    public void testIcebergSourceMissingSummaryTotalsEstimatesZero() {
        // The case most likely to bite in production: a snapshot exists and the scan works, but the
        // writer never recorded the totals, so pre-split has no size to work from.
        Assertions.assertEquals(Estimates.ZERO,
                TablePreSplitSource.sourceEstimates(mockIcebergTableWithSummary(Map.of("operation", "append"))));
    }

    @Test
    public void testIcebergSourceWithUnparseableSummaryTotalsEstimatesZero() {
        // Never propagate a partially-parsed size: one bad key must not be combined with a good one
        // into a ratio that over- or under-splits.
        Assertions.assertEquals(Estimates.ZERO,
                TablePreSplitSource.sourceEstimates(mockIcebergTableWithSummary(
                        Map.of("total-files-size", "not-a-number", "total-records", "-17"))));
    }

    @Test
    public void testZeroSourceEstimateRemovesTheSamplingRateLimit() {
        // The consequence of the three cases above, asserted end-to-end so it cannot regress
        // unnoticed: a zero byte estimate makes the Bernoulli rate 1.0, so every predicate-matching
        // row reaches the ORDER BY rand() LIMIT rather than a small sample. On a large Iceberg
        // snapshot that turns a cheap sample into a top-N over the whole filtered input.
        Assertions.assertEquals(1.0, AbstractSqlSampleSubqueryExecutor.pickSamplingRate(0L));
        Assertions.assertTrue(AbstractSqlSampleSubqueryExecutor.pickSamplingRate(872000000000L) < 1.0e-4,
                "a sized snapshot must still be sampled at a small rate");
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
            when(fixture.insertStmt.getUserSpecifiedPropertyKeys()).thenReturn(Set.of());

            InsertPreSplitHook.maybeRunDynamicOverwritePreSplit(fixture.insertStmt, fixture.context, 42L);

            flow.verify(() -> PreSplitFlow.runDynamicOverwriteFlow(
                    any(Database.class), eq(fixture.targetTable), any(PreSplitFlow.Prepared.class),
                    eq(LoadKind.INSERT_FROM_TABLE), any(), eq(fixture.context), eq(42L)), times(1));
        }
    }

    @Test
    public void dynamicOverwriteHookSkipsUserSpecifiedProperties() throws Exception {
        assertDynamicOverwriteHookSkips(42L, stmt -> {
            when(stmt.isDynamicOverwrite()).thenReturn(true);
            when(stmt.hasOverwriteJob()).thenReturn(true);
            when(stmt.getUserSpecifiedPropertyKeys()).thenReturn(Set.of("max_filter_ratio"));
        });
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

        OlapTable target() {
            return targetTable;
        }

        private SourceFixture(List<String> targetColumns, List<String> sourceColumns) {
            this.context = mockConnectContextWithSessionPreSplit(true);
            when(context.isBypassAuthorizerCheck()).thenReturn(true);

            // Target: single-tier, range-distribution, NORMAL, one index, scalar sort key.
            OlapTable target = mock(OlapTable.class);
            this.targetTable = target;
            when(target.getName()).thenReturn("target_t");
            when(target.isCloudNativeTableOrMaterializedView()).thenReturn(true);
            when(target.isRangeDistribution()).thenReturn(true);
            when(target.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
            com.starrocks.catalog.MaterializedIndexMeta baseMeta = mock(com.starrocks.catalog.MaterializedIndexMeta.class);
            when(baseMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
            when(target.getVisibleIndexMetas()).thenReturn(List.of(baseMeta));
            when(target.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
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
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(eq(target), eq(BASE_INDEX_META_ID)))
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
            InsertPreSplitHook.maybeRunPreSplit(insertStmt, context);
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
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
