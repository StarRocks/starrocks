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

<<<<<<< HEAD
=======
import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.catalog.Column;
>>>>>>> e982e19 ([BugFix] Pre-split loads whose partition or sort-key column is a generated column (#80390))
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.ACTIVITY_DATE;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.MONTH_SQL;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.activityMonth;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.assertHookDoesNotDelegate;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.mockConnectContextWithSessionPreSplit;
<<<<<<< HEAD
import static org.mockito.Mockito.mock;
=======
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
>>>>>>> e982e19 ([BugFix] Pre-split loads whose partition or sort-key column is a generated column (#80390))
import static org.mockito.Mockito.when;

/**
 * Detection-side coverage for {@link BrokerLoadPreSplitHook}: each early-return
 * branch is exercised and asserted via {@code MockedStatic} to never reach
 * {@link TabletPreSplitCoordinator#submitAsynchronously}. The eligible-
 * delegation path needs a full FE fixture (catalog, tablet inverted index,
 * compute-resource warehouse) and is left to integration coverage.
 */
public class BrokerLoadPreSplitHookTest {

    private static final long BASE_INDEX_META_ID = 200L;

    private boolean savedConfigBrokerLoad;

    @BeforeEach
    public void setUp() {
        savedConfigBrokerLoad = Config.enable_tablet_pre_split_for_broker_load;
        Config.enable_tablet_pre_split_for_broker_load = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_tablet_pre_split_for_broker_load = savedConfigBrokerLoad;
    }

    @Test
    public void testConfigFlagOffShortCircuits() throws Exception {
        // Cluster-wide opt-out must short-circuit before the coordinator AND
        // record the eligibility-skip counter under disabled_by_config — the
        // hook returns ahead of the coordinator, so checkConfigAndSession would
        // otherwise never bump the bucket.
        Config.enable_tablet_pre_split_for_broker_load = false;
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.DISABLED_BY_CONFIG.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    invokeHook(singlePartitionOlapTable(), List.of(), List.of()));

            org.junit.jupiter.api.Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "config opt-out must bump the disabled_by_config bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void testSessionOptOutShortCircuits() throws Exception {
        // SET enable_tablet_pre_split=false on the session must short-circuit
        // before the eligibility-target walk AND record the eligibility-skip
        // counter under disabled_by_session. The hook now takes the
        // ConnectContext directly (parameter-threaded), so we
        // pass an opted-out context rather than stubbing a static.
        ConnectContext optedOutContext = mockConnectContextWithSessionPreSplit(false);
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.DISABLED_BY_SESSION.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    invokeHook(optedOutContext, singlePartitionOlapTable(), List.of(), List.of()));

            org.junit.jupiter.api.Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "session opt-out must bump the disabled_by_session bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void testResolvedOptOutShortCircuitsThoughTheContextSaysOtherwise() throws Exception {
        // BrokerLoadJob fires this hook from a scheduler thread against a ConnectContext that,
        // outside an FE failover, is the submitter's own live session. A SET issued after the
        // statement was accepted must not re-decide the load, so the value the caller resolved
        // wins over the one on the context.
        ConnectContext liveOptedInContext = mockConnectContextWithSessionPreSplit(true);
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.DISABLED_BY_SESSION.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    invokeHook(liveOptedInContext, singlePartitionOlapTable(), List.of(), List.of(), false));

            Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "the resolved opt-out must bump the disabled_by_session bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void testResolvedOptInIsNotUndoneByAnOptedOutContext() throws Exception {
        // The mirror case, which is what an FE-failover replay of a load submitted with pre-split
        // ON relies on: null file groups short-circuit AFTER the session gate, so reaching that
        // return without bumping disabled_by_session proves the gate opened.
        ConnectContext liveOptedOutContext = mockConnectContextWithSessionPreSplit(false);
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.DISABLED_BY_SESSION.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    invokeHook(liveOptedOutContext, singlePartitionOlapTable(), null, List.of(), true));

            Assertions.assertEquals(baseline,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "a resolved opt-in must not take the session-opt-out branch");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void testNullFileGroupsShortCircuits() throws Exception {
        assertHookDoesNotDelegate(() ->
                invokeHook(singlePartitionOlapTable(), null, List.of()));
    }

    @Test
    public void testNullFileStatusesShortCircuits() throws Exception {
        assertHookDoesNotDelegate(() ->
                invokeHook(singlePartitionOlapTable(), List.of(), null));
    }

    @Test
    public void testMultiPartitionOlapTableShortCircuits() throws Exception {
        OlapTable target = mock(OlapTable.class);
        when(target.getPhysicalPartitions()).thenReturn(
                List.of(mock(PhysicalPartition.class), mock(PhysicalPartition.class)));

        assertHookDoesNotDelegate(() ->
                invokeHook(target, List.of(mock(BrokerFileGroup.class)), List.of()));
    }

    @Test
    public void testSinglePartitionWithMultipleBaseTabletsRecordsSkip() throws Exception {
        // Table clears the table-level gate but its single base index already
        // holds multiple tablets (the re-load-after-split case). The hook must
        // record multiple_base_index_tablets and not delegate — the
        // coordinator's maybeAct, which would otherwise record it, is never
        // reached on the single-partition resolve-failure path.
        OlapTable target = tablePassingTableLevelGate();
        MaterializedIndex baseIndex = mock(MaterializedIndex.class);
        when(baseIndex.getTablets()).thenReturn(List.of(mock(Tablet.class), mock(Tablet.class)));
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);
        when(target.getPhysicalPartitions()).thenReturn(List.of(partition));

        assertSinglePartitionResolveRecordsSkip(target, SkipReason.MULTIPLE_BASE_INDEX_TABLETS);
    }

    @Test
    public void testMissingBaseIndexRecordsSkip() throws Exception {
        // Table clears the table-level gate but the base-index lookup returns
        // null — e.g. an alter changed the base-index id mid-load. The hook
        // must record metadata_not_resolved and not delegate.
        OlapTable target = tablePassingTableLevelGate();
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(null);
        when(target.getPhysicalPartitions()).thenReturn(List.of(partition));

        assertSinglePartitionResolveRecordsSkip(target, SkipReason.METADATA_NOT_RESOLVED);
    }

    @Test
    public void testInternalThrowIsSwallowed() throws Exception {
        // Drive the outer try/catch by passing an OlapTable whose accessor
        // throws. The hook must not let the throw escape — Broker Load would
        // otherwise abort an already-running pending-task callback.
        OlapTable target = mock(OlapTable.class);
        when(target.getPhysicalPartitions()).thenThrow(new RuntimeException("simulated table failure"));

        assertHookDoesNotDelegate(() ->
                invokeHook(target, List.of(mock(BrokerFileGroup.class)), List.of()));
    }

    /**
     * Invokes {@code BrokerLoadPreSplitHook.maybeRunPreSplit} with default
     * mocks for {@code Database}, {@code BrokerDesc}, and {@code ComputeResource}
     * — none of which the early-return branches consult. Tests pass distinct
     * arguments for the three fields the hook actually inspects on the
     * short-circuit paths: target table, file groups, file statuses. The
     * default context has {@code enable_tablet_pre_split=true} so the
     * session opt-out branch is NOT taken.
     */
    private static void invokeHook(
            OlapTable target, List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses) {
        invokeHook(mockConnectContextWithSessionPreSplit(true), target, fileGroups, fileStatuses);
    }

    /**
     * Overload for tests that need to drive a specific {@link ConnectContext}
     * (e.g. the session opt-out test).
     */
    private static void invokeHook(
            ConnectContext context, OlapTable target,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses) {
        BrokerLoadPreSplitHook.maybeRunPreSplit(
                context, mock(Database.class), target, mock(BrokerDesc.class),
                fileGroups, fileStatuses, mock(ComputeResource.class), () -> false);
    }

    /**
     * Overload for tests that drive the opt-out a deferred load resolved for itself, which is
     * not necessarily what {@code context} carries by the time the hook fires.
     */
    private static void invokeHook(
            ConnectContext context, OlapTable target,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            Boolean sessionPreSplitEnabled) {
        BrokerLoadPreSplitHook.maybeRunPreSplit(
                context, mock(Database.class), target, mock(BrokerDesc.class),
                fileGroups, fileStatuses, mock(ComputeResource.class), () -> false,
                sessionPreSplitEnabled);
    }

    private static OlapTable singlePartitionOlapTable() {
        MaterializedIndex baseIndex = mock(MaterializedIndex.class);
        when(baseIndex.getTablets()).thenReturn(List.of(mock(Tablet.class)));
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(table.getPhysicalPartitions()).thenReturn(List.of(partition));
        return table;
    }

    /**
     * Builds an unpartitioned {@link OlapTable} that clears the table-level
     * eligibility gate ({@link PreSplitTargets#findEligibleTable}) so the hook
     * proceeds into the single-partition flow. Per-partition shape (physical
     * partitions, base index, tablets) is left to the caller to stub. The
     * sort-key column is supplied by {@link #assertSinglePartitionResolveRecordsSkip}
     * via a {@code MockedStatic<MetaUtils>}.
     */
    private static OlapTable tablePassingTableLevelGate() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(mock(MaterializedIndexMeta.class)));
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(false);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);
        return table;
    }

    /**
     * Invokes the hook against a table that passes the table-level gate but
     * fails single-partition target resolution, and asserts it (a) never
     * delegates to the coordinator and (b) bumps the {@code eligibility_skipped}
     * counter under {@code expectedReason} exactly once. A
     * {@code MockedStatic<MetaUtils>} supplies a scalar sort key so the
     * table-level gate's sort-key check passes.
     */
    private static void assertSinglePartitionResolveRecordsSkip(
            OlapTable target, SkipReason expectedReason) throws Exception {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target))
                    .thenReturn(List.of(PresplitTestSupport.bigintColumn("k")));
            String label = expectedReason.name().toLowerCase();
            long baseline = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(label).getValue();

            assertHookDoesNotDelegate(() ->
                    invokeHook(target, List.of(mock(BrokerFileGroup.class)),
                            List.of(List.<TBrokerFileStatus>of())));

            Assertions.assertEquals(baseline + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                            .getMetric(label).getValue().longValue(),
                    "single-partition resolve failure must bump the " + label + " bucket");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    // ---- generated sampled columns ----

    private static final Column ACCOUNT_ID = PresplitTestSupport.bigintColumn("account_id");
    private static final Column ACTIVITY_MONTH = activityMonth();

    /** Stubs (account_id, activity_date) + activity_date_month AS date_trunc('month', activity_date). */
    private static void stubGeneratedSchema(OlapTable target) {
        when(target.getName()).thenReturn("t");
        PresplitTestSupport.stubGeneratedSchema(target, List.of(ACCOUNT_ID, ACTIVITY_DATE), ACTIVITY_MONTH);
    }

    private static BrokerFileGroup fileGroupWithColumns(ImportColumnDesc... columns) {
        BrokerFileGroup fileGroup = mock(BrokerFileGroup.class);
        when(fileGroup.getColumnExprList()).thenReturn(List.of(columns));
        return fileGroup;
    }

    @Test
    public void noGeneratedSampledColumnLeavesTheMappingEmpty() {
        InsertSelectSourceColumns.Resolved resolved = BrokerLoadPreSplitHook.resolveGeneratedSampledColumns(
                mock(OlapTable.class), List.of(mock(BrokerFileGroup.class)), List.of(ACCOUNT_ID),
                mockConnectContextWithSessionPreSplit(true));

        Assertions.assertTrue(resolved.targetToExpressionSql().isEmpty());
        Assertions.assertTrue(resolved.targetToConstantSql().isEmpty());
    }

    @Test
    public void generatedSampledColumnWhoseInputTheLoadDoesNotReadFromTheFileIsDeclined() {
        // COLUMNS (account_id): the load defaults activity_date, so its generated month is not the file's.
        assertGeneratedColumnDeclined(fileGroupWithColumns(new ImportColumnDesc("account_id")));
        // SET (activity_date = <expr>): the load writes the mapped value, not the file's column.
        assertGeneratedColumnDeclined(fileGroupWithColumns(new ImportColumnDesc("account_id"),
                new ImportColumnDesc("activity_date", mock(Expr.class))));
        // COLUMNS (account_id, activity_date) SET (activity_date = <expr>): Load applies the SET after the bare
        // entry, so the load writes the mapped value even though the bare name is listed.
        assertGeneratedColumnDeclined(fileGroupWithColumns(new ImportColumnDesc("account_id"),
                new ImportColumnDesc("activity_date"), new ImportColumnDesc("ACTIVITY_DATE", mock(Expr.class))));
    }

    @Test
    public void generatedSampledColumnOfASetOnlyListIsComputedFromTheFileColumnItReads() {
        // SET (account_id = <expr>) alone names no file field, so Load reads activity_date by its own name.
        OlapTable target = mock(OlapTable.class);
        stubGeneratedSchema(target);

        InsertSelectSourceColumns.Resolved resolved = BrokerLoadPreSplitHook.resolveGeneratedSampledColumns(
                target, List.of(fileGroupWithColumns(new ImportColumnDesc("account_id", mock(Expr.class)))),
                List.of(ACCOUNT_ID, ACTIVITY_MONTH), mockConnectContextWithSessionPreSplit(true));

        Assertions.assertNotNull(resolved);
        Assertions.assertEquals(Map.of("activity_date_month", MONTH_SQL), resolved.targetToExpressionSql());
    }

    // ---- PARTITION(...) scope ----

    private static Partition partition(OlapTable target, long id, String name, boolean temporary) {
        Partition partition = mock(Partition.class);
        when(partition.getId()).thenReturn(id);
        when(partition.getName()).thenReturn(name);
        when(target.getPartition(id)).thenReturn(partition);
        when(target.getPartition(name, false)).thenReturn(temporary ? null : partition);
        return partition;
    }

    private static OlapTable partitionedTarget(boolean partitioned) {
        OlapTable target = mock(OlapTable.class);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(partitioned);
        when(target.getPartitionInfo()).thenReturn(partitionInfo);
        return target;
    }

    private static BrokerFileGroup fileGroupNamingPartitions(Long... partitionIds) {
        BrokerFileGroup fileGroup = mock(BrokerFileGroup.class);
        when(fileGroup.isSpecifyPartition()).thenReturn(true);
        when(fileGroup.getPartitionIds()).thenReturn(List.of(partitionIds));
        return fileGroup;
    }

    @Test
    public void loadNamingNoPartitionIsUnrestricted() {
        Assertions.assertFalse(BrokerLoadPreSplitHook.partitionScopeOf(
                partitionedTarget(true), List.of(mock(BrokerFileGroup.class))).isSpecified());
    }

    @Test
    public void partitionListLimitsTheScopeToThosePartitions() {
        OlapTable target = partitionedTarget(true);
        partition(target, 1L, "p1", false);
        partition(target, 2L, "p2", false);

        PreSplitPartitionScope scope = BrokerLoadPreSplitHook.partitionScopeOf(
                target, List.of(fileGroupNamingPartitions(1L, 2L)));

        Assertions.assertTrue(scope.isSpecified());
        Assertions.assertFalse(scope.isTemporary());
        Assertions.assertEquals(Set.of("p1", "p2"), Set.copyOf(scope.catalogPartitionNames()));
        Assertions.assertEquals("p1", scope.mappedCatalogName("P1"));
        Assertions.assertNull(scope.mappedCatalogName("p3"));
    }

    @Test
    public void groupNamingNoPartitionMayWriteEveryExistingPartition() {
        // Automatic partition creation is off for the whole load once any group names partitions, so the
        // unrestricted group may still write any existing partition -- but no new one.
        OlapTable target = partitionedTarget(true);
        Partition p1 = partition(target, 1L, "p1", false);
        Partition p2 = partition(target, 2L, "p2", false);
        when(target.getPartitions()).thenReturn(List.of(p1, p2));

        PreSplitPartitionScope scope = BrokerLoadPreSplitHook.partitionScopeOf(
                target, List.of(fileGroupNamingPartitions(1L), mock(BrokerFileGroup.class)));

        Assertions.assertEquals(Set.of("p1", "p2"), Set.copyOf(scope.catalogPartitionNames()));
    }

    @Test
    public void unpartitionedTargetIsUnrestrictedEvenWhenAPartitionIsNamed() {
        // An unpartitioned table's single partition carries the table's name; a scope there would only make
        // PreSplitFlow skip a load it pre-splits today.
        Assertions.assertFalse(BrokerLoadPreSplitHook.partitionScopeOf(
                partitionedTarget(false), List.of(fileGroupNamingPartitions(1L))).isSpecified());
    }

    /** What the hook dispatched, which it must do exactly once. */
    private record Dispatched(PreSplitFlow.Prepared prepared, PreSplitPartitionScope scope) {
        BrokerLoadScanContext scanContext() {
            return (BrokerLoadScanContext) prepared.scanContext();
        }
    }

    private static Dispatched dispatched(ConnectContext context, OlapTable target, List<BrokerFileGroup> fileGroups) {
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target)).thenReturn(List.of(ACCOUNT_ID));
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target, BASE_INDEX_META_ID))
                    .thenReturn(List.of(ACCOUNT_ID));

            invokeHook(context, target, fileGroups, List.of(List.<TBrokerFileStatus>of()), /*sessionPreSplitEnabled*/ true);

            ArgumentCaptor<PreSplitFlow.Prepared> prepared = ArgumentCaptor.forClass(PreSplitFlow.Prepared.class);
            ArgumentCaptor<PreSplitPartitionScope> scope = ArgumentCaptor.forClass(PreSplitPartitionScope.class);
            flow.verify(() -> PreSplitFlow.dispatch(any(), eq(target), prepared.capture(), eq(LoadKind.BROKER_LOAD),
                    any(), any(), scope.capture()), times(1));
            return new Dispatched(prepared.getValue(), scope.getValue());
        }
    }

    @Test
    public void hookCarriesTheGeneratedColumnIntoTheScanContext() {
        OlapTable target = tablePassingTableLevelGate();
        stubGeneratedSchema(target);
        when(target.getPartitionInfo().getPartitionColumns(any())).thenReturn(List.of(ACTIVITY_MONTH));

        BrokerLoadScanContext scanContext = dispatched(mockConnectContextWithSessionPreSplit(true), target,
                List.of(mock(BrokerFileGroup.class))).scanContext();
        Assertions.assertEquals(Map.of("activity_date_month", MONTH_SQL), scanContext.targetToExpressionSql());
        Assertions.assertEquals(List.of(ACTIVITY_DATE), scanContext.generatedColumnInputs());
    }

    @Test
    public void hookCarriesTheJobSessionsSemanticsIntoTheScanContext() {
        OlapTable target = tablePassingTableLevelGate();
        when(target.getName()).thenReturn("t");
        SessionVariable jobSession = new SessionVariable();
        jobSession.setTimeZone("Asia/Shanghai");
        jobSession.setCboEqBaseType("varchar");
        ConnectContext context = mock(ConnectContext.class);
        when(context.getSessionVariable()).thenReturn(jobSession);

        Assertions.assertEquals(SampleSessionSemantics.capture(jobSession),
                dispatched(context, target, List.of(mock(BrokerFileGroup.class))).scanContext().sessionSemantics());
    }

    @Test
    public void hookScopesALoadThatNamesPartitionsToThem() {
        OlapTable target = tablePassingTableLevelGate();
        when(target.getName()).thenReturn("t");
        when(target.getPartitionInfo().isPartitioned()).thenReturn(true);
        partition(target, 1L, "p1", false);
        partition(target, 2L, "p2", false);

        PreSplitPartitionScope scope = dispatched(mockConnectContextWithSessionPreSplit(true), target,
                List.of(fileGroupNamingPartitions(1L, 2L))).scope();
        Assertions.assertTrue(scope.isSpecified());
        Assertions.assertEquals(Set.of("p1", "p2"), Set.copyOf(scope.catalogPartitionNames()));
    }

    @Test
    public void hookSkipsALoadWhosePartitionsNoScopeDescribes() throws Exception {
        // One group names a normal partition and another a temporary one, which no single scope describes.
        OlapTable target = tablePassingTableLevelGate();
        when(target.getName()).thenReturn("t");
        when(target.getPartitionInfo().isPartitioned()).thenReturn(true);
        partition(target, 1L, "p1", false);
        partition(target, 9L, "tp1", true);
        PreSplitProfile profile = new PreSplitProfile();

        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PreSplitFlow> flow = Mockito.mockStatic(PreSplitFlow.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target)).thenReturn(List.of(ACCOUNT_ID));
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target, BASE_INDEX_META_ID))
                    .thenReturn(List.of(ACCOUNT_ID));

            BrokerLoadPreSplitHook.maybeRunPreSplit(mockConnectContextWithSessionPreSplit(true), mock(Database.class),
                    target, mock(BrokerDesc.class), List.of(fileGroupNamingPartitions(1L), fileGroupNamingPartitions(9L)),
                    List.of(List.<TBrokerFileStatus>of(), List.<TBrokerFileStatus>of()), mock(ComputeResource.class),
                    () -> false, profile, /*sessionPreSplitEnabled*/ null);

            flow.verify(() -> PreSplitFlow.dispatch(any(), any(), any(), any(), any(), any(), any()), never());
            Assertions.assertEquals("SKIPPED: UNSUPPORTED_PARTITION_SCOPE",
                    profile.toRuntimeProfile().getInfoString("Outcomes"));
        }
    }

    private static void assertGeneratedColumnDeclined(BrokerFileGroup fileGroup) {
        OlapTable target = mock(OlapTable.class);
        stubGeneratedSchema(target);
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String reason = SkipReason.UNSUPPORTED_GENERATED_COLUMN.name().toLowerCase();
            long before = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(reason).getValue();

            Assertions.assertNull(BrokerLoadPreSplitHook.resolveGeneratedSampledColumns(target, List.of(fileGroup),
                    List.of(ACCOUNT_ID, ACTIVITY_MONTH), mockConnectContextWithSessionPreSplit(true)));

            Assertions.assertEquals(before + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(reason).getValue().longValue());
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }
}
