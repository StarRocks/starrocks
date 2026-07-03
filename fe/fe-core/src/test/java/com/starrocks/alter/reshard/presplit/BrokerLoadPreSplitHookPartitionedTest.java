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

import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.loadv2.BrokerLoadJob;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.mockConnectContextWithSessionPreSplit;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests for the multi-partition pre-split flow added in
 * {@link BrokerLoadPreSplitHook}. Each early-return branch on the
 * single-partition path is already covered by {@link BrokerLoadPreSplitHookTest};
 * this file focuses on the multi-partition code paths:
 *
 * <ol>
 *   <li>Sync-await invariant: the Broker Load hook sync-awaits the reshard
 *       daemon's {@code FINISHED} transition on both single- and
 *       multi-partition paths so the triggering Broker Load itself sees the
 *       post-pre-split tablet layout — symmetric with the INSERT-from-FILES
 *       hook. Deadlock-safe because {@code BrokerLoadJob.unprotectedExecute}
 *       defers {@code beginTxn} until after the hook returns.</li>
 *   <li>Source-level ordering invariant: {@code BrokerLoadJob.createLoadingTask}
 *       calls {@code firePreSplitHooks} BEFORE {@code beginTxn()} and BEFORE
 *       {@code task.prepare()}. This is what lets the triggering Broker Load
 *       benefit from pre-split — its sink plan is built against the
 *       post-reshard layout — and what keeps the daemon's cleanup-phase
 *       prev-txn wait off the not-yet-allocated load txn.</li>
 *   <li>Persisted session variable ({@link SessionVariable#ENABLE_TABLET_PRE_SPLIT}
 *       in {@code BulkLoadJob.sessionVariables}) survives serialization
 *       round-trip — required so a load submitted with
 *       {@code SET enable_tablet_pre_split = false} still honors the opt-out
 *       after FE failover.</li>
 * </ol>
 *
 * <p>End-to-end "Broker Load → grouper → pre-create → triggering load sees
 * new partitions" coverage requires a full FE fixture (catalog, partitions,
 * tablet inverted index, ConnectContext-bound compute resource) and lives in
 * the TSP regression suite; the two
 * integration-shaped tests at the bottom are intentionally {@code @Disabled}
 * with a pointer.
 */
public class BrokerLoadPreSplitHookPartitionedTest {

    private boolean savedConfigBrokerLoad;
    private boolean savedMetricHasInit;

    @BeforeEach
    public void setUp() {
        savedConfigBrokerLoad = Config.enable_tablet_pre_split_for_broker_load;
        Config.enable_tablet_pre_split_for_broker_load = true;
        savedMetricHasInit = MetricRepo.hasInit;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_tablet_pre_split_for_broker_load = savedConfigBrokerLoad;
        MetricRepo.hasInit = savedMetricHasInit;
    }

    // ---------- Sync-await invariant ----------

    @Test
    public void brokerLoadHookSyncAwaitsOnBothPaths() {
        // The Broker Load hook now sync-awaits the reshard daemon's FINISHED
        // transition on both the single-partition and multi-partition paths.
        // Symmetric with the INSERT-from-FILES hook so the triggering Broker
        // Load itself benefits from pre-split. Deadlock-safe because
        // BrokerLoadJob.unprotectedExecute defers beginTxn until after the
        // hook returns, so the reshard daemon's cleanup-phase prev-txn drain
        // cannot include T_load. Regression guard: source-level structural
        // assertion that the two await helpers exist and are called.
        String source = readSource(
                "fe-core/src/main/java/com/starrocks/alter/reshard/presplit/PreSplitFlow.java");
        String codeOnly = stripLineComments(source);
        Assertions.assertTrue(containsCall(codeOnly, "awaitFinishedAllowingFallback"),
                "PreSplitFlow MUST call awaitFinishedAllowingFallback on the single-partition path");
        Assertions.assertTrue(containsCall(codeOnly, "awaitCombinedJobAllowingFallback"),
                "PreSplitFlow MUST call awaitCombinedJobAllowingFallback on the multi-partition path");
        Assertions.assertTrue(source.contains("submitForPartitionsCombined"),
                "PreSplitFlow multi-partition flow MUST call submitForPartitionsCombined");
    }

    @Test
    public void partitionedBrokerLoadTakesMultiPartitionBranch() {
        // Strengthened routing proof: mock a partitioned table whose findEligibleTable
        // check ACTUALLY passes (non-empty supported sort-key list via a MockedStatic
        // on MetaUtils + non-null ConnectContext.getSessionVariable). Then mock the
        // Sampler construction so the data-tier sampler returns a non-null SampleSet
        // and mock PartitionSampleGrouper.group to return a non-empty list. Finally
        // mock TabletPreSplitCoordinator.submitForPartitionsCombined to return Skipped.
        //
        // Verifies:
        //   - submitForPartitionsCombined was called EXACTLY ONCE (proves the multi-
        //     partition flow was reached, not just the unpartitioned branch).
        //   - submitAsynchronously was NEVER called (proves we did NOT route to the
        //     single-partition path).
        OlapTable table = mockPartitionedRangeTable();
        List<Column> sortKey = List.of(bigintColumn("sort_col"));
        List<Column> partitionColumns = List.of(bigintColumn("p_col"));
        when(table.getPartitionInfo().getPartitionColumns(any())).thenReturn(partitionColumns);

        SampleSet sampledRows = new SampleSet(List.of(), List.of(), Estimates.ZERO);

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenReturn(sampledRows))) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table)).thenReturn(sortKey);
            // Non-empty grouped list -> submitForPartitionsCombined is invoked.
            grouper.when(() -> PartitionSampleGrouper.group(
                            any(SampleSet.class), any(OlapTable.class), any(ConnectContext.class),
                            anyLong(), anyLong()))
                    .thenReturn(List.of(Mockito.mock(PartitionSamples.class)));
            coordinator.when(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                            any(), any(), anyList(), anyInt(), any(), any()))
                    .thenReturn(new PreSplitOutcome.Skipped(SkipReason.NO_USEFUL_CUTS));

            BrokerLoadPreSplitHook.maybeRunPreSplit(
                    mockConnectContextWithSessionPreSplit(true),
                    mock(Database.class), table, mock(BrokerDesc.class),
                    List.of(mock(BrokerFileGroup.class)),
                    List.of(List.<TBrokerFileStatus>of()),
                    mock(ComputeResource.class), () -> false);

            // Routing proof: partitioned tables MUST take the multi-partition path...
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any(), any()), times(1));
            // ...and MUST NOT fall through to the single-partition entry.
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
        }
    }

    @Test
    public void manuallyPartitionedBrokerLoadSkipsPreSplit() {
        // A partitioned Broker Load target whose supportedAutomaticPartition() is false (a
        // manual list/range partition table) must not reach the multi-partition flow: the
        // automatic-partition gate in PreSplitFlow.dispatch reaches through the Broker Load
        // entry too. Mirror partitionedBrokerLoadTakesMultiPartitionBranch's scaffolding so
        // the flow would reach submitForPartitionsCombined if the gate were removed — that is
        // the bite signal. Override only this instance's supportedAutomaticPartition() to
        // false; the shared mockPartitionedRangeTable() default (true) stays untouched.
        OlapTable table = mockPartitionedRangeTable();
        when(table.supportedAutomaticPartition()).thenReturn(false);
        List<Column> sortKey = List.of(bigintColumn("sort_col"));
        List<Column> partitionColumns = List.of(bigintColumn("p_col"));
        when(table.getPartitionInfo().getPartitionColumns(any())).thenReturn(partitionColumns);

        SampleSet sampledRows = new SampleSet(List.of(), List.of(), Estimates.ZERO);

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenReturn(sampledRows))) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table)).thenReturn(sortKey);
            grouper.when(() -> PartitionSampleGrouper.group(
                            any(SampleSet.class), any(OlapTable.class), any(ConnectContext.class),
                            anyLong(), anyLong()))
                    .thenReturn(List.of(Mockito.mock(PartitionSamples.class)));

            BrokerLoadPreSplitHook.maybeRunPreSplit(
                    mockConnectContextWithSessionPreSplit(true),
                    mock(Database.class), table, mock(BrokerDesc.class),
                    List.of(mock(BrokerFileGroup.class)),
                    List.of(List.<TBrokerFileStatus>of()),
                    mock(ComputeResource.class), () -> false);

            // The automatic-partition gate must skip before either submit path.
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any(), any()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
        }
    }

    @Test
    public void multiPartitionBrokerLoadCapsSampleAtPreSubmitBudget() {
        // The multi-partition data-tier sample must be capped at the pre-submit
        // budget — the same bound the single-partition DefaultPreSplitPipeline path
        // applies. Without it a slow FILES sample runs until the default
        // query_timeout while the Broker Load holds a pending_load_task_scheduler
        // slot in PENDING. Capture the SampleRequest handed to the sampler and
        // assert its query-timeout equals tablet_pre_split_pre_submit_timeout_seconds.
        long savedTimeout = Config.tablet_pre_split_pre_submit_timeout_seconds;
        Config.tablet_pre_split_pre_submit_timeout_seconds = 123L;
        try {
            OlapTable table = mockPartitionedRangeTable();
            List<Column> sortKey = List.of(bigintColumn("sort_col"));
            List<Column> partitionColumns = List.of(bigintColumn("p_col"));
            when(table.getPartitionInfo().getPartitionColumns(any())).thenReturn(partitionColumns);
            SampleSet sampledRows = new SampleSet(List.of(), List.of(), Estimates.ZERO);
            AtomicReference<SampleRequest> capturedRequest = new AtomicReference<>();

            try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                    MockedStatic<TabletPreSplitCoordinator> coordinator =
                            Mockito.mockStatic(TabletPreSplitCoordinator.class);
                    MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                    MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                    MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                            (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenAnswer(invocation -> {
                                capturedRequest.set(invocation.getArgument(0));
                                return sampledRows;
                            }))) {
                metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table)).thenReturn(sortKey);
                grouper.when(() -> PartitionSampleGrouper.group(
                                any(SampleSet.class), any(OlapTable.class), any(ConnectContext.class),
                                anyLong(), anyLong()))
                        .thenReturn(List.of(Mockito.mock(PartitionSamples.class)));
                coordinator.when(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                                any(), any(), anyList(), anyInt(), any(), any()))
                        .thenReturn(new PreSplitOutcome.Skipped(SkipReason.NO_USEFUL_CUTS));

                BrokerLoadPreSplitHook.maybeRunPreSplit(
                        mockConnectContextWithSessionPreSplit(true),
                        mock(Database.class), table, mock(BrokerDesc.class),
                        List.of(mock(BrokerFileGroup.class)),
                        List.of(List.<TBrokerFileStatus>of()),
                        mock(ComputeResource.class), () -> false);

                Assertions.assertNotNull(capturedRequest.get(), "data-tier sampler must have been invoked");
                Assertions.assertEquals(123, capturedRequest.get().getQueryTimeoutSeconds(),
                        "multi-partition Broker Load sample must be capped at pre_submit_timeout");
            }
        } finally {
            Config.tablet_pre_split_pre_submit_timeout_seconds = savedTimeout;
        }
    }

    @Test
    public void samplerReturningNullSkipsBeforeGrouper() {
        // runDataTierSampler returns null (sampler produced no SampleSet) ->
        // runMultiPartitionFlow short-circuits before PartitionSampleGrouper.group
        // and before submitForPartitionsCombined. Drive a sampler whose sample()
        // throws StarRocksException so runDataTierSampler's catch returns null.
        OlapTable table = mockPartitionedRangeTable();
        List<Column> sortKey = List.of(bigintColumn("sort_col"));
        when(table.getPartitionInfo().getPartitionColumns(any())).thenReturn(List.of(bigintColumn("p_col")));

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class)))
                                .thenThrow(new com.starrocks.common.StarRocksException("synthetic sample failure")))) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table)).thenReturn(sortKey);

            BrokerLoadPreSplitHook.maybeRunPreSplit(
                    mockConnectContextWithSessionPreSplit(true),
                    mock(Database.class), table, mock(BrokerDesc.class),
                    List.of(mock(BrokerFileGroup.class)),
                    List.of(List.<TBrokerFileStatus>of()),
                    mock(ComputeResource.class), () -> false);

            // Sampler failed -> no grouping, no submit.
            grouper.verify(() -> PartitionSampleGrouper.group(
                    any(), any(), any(), anyLong(), anyLong()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any(), any()), never());
        }
    }

    @Test
    public void samplerRuntimeFailureSkipsBeforeGrouper() {
        // runDataTierSampler's RuntimeException catch (distinct from StarRocksException)
        // also returns null -> no grouping, no submit. Both catch arms bucket as
        // SAMPLE_FAILED; here we only assert the short-circuit behavior.
        OlapTable table = mockPartitionedRangeTable();
        List<Column> sortKey = List.of(bigintColumn("sort_col"));
        when(table.getPartitionInfo().getPartitionColumns(any())).thenReturn(List.of(bigintColumn("p_col")));

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class)))
                                .thenThrow(new RuntimeException("synthetic runtime sample failure")))) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table)).thenReturn(sortKey);

            BrokerLoadPreSplitHook.maybeRunPreSplit(
                    mockConnectContextWithSessionPreSplit(true),
                    mock(Database.class), table, mock(BrokerDesc.class),
                    List.of(mock(BrokerFileGroup.class)),
                    List.of(List.<TBrokerFileStatus>of()),
                    mock(ComputeResource.class), () -> false);

            grouper.verify(() -> PartitionSampleGrouper.group(
                    any(), any(), any(), anyLong(), anyLong()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any(), any()), never());
        }
    }

    @Test
    public void emptyGroupsSkipsBeforeSubmit() {
        // Sampler succeeds but the grouper drops every row (empty list) ->
        // runMultiPartitionFlow returns before submitForPartitionsCombined.
        OlapTable table = mockPartitionedRangeTable();
        List<Column> sortKey = List.of(bigintColumn("sort_col"));
        when(table.getPartitionInfo().getPartitionColumns(any())).thenReturn(List.of(bigintColumn("p_col")));

        SampleSet sampledRows = new SampleSet(List.of(), List.of(), Estimates.ZERO);

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenReturn(sampledRows))) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table)).thenReturn(sortKey);
            grouper.when(() -> PartitionSampleGrouper.group(
                            any(SampleSet.class), any(OlapTable.class), any(ConnectContext.class),
                            anyLong(), anyLong()))
                    .thenReturn(List.of());

            BrokerLoadPreSplitHook.maybeRunPreSplit(
                    mockConnectContextWithSessionPreSplit(true),
                    mock(Database.class), table, mock(BrokerDesc.class),
                    List.of(mock(BrokerFileGroup.class)),
                    List.of(List.<TBrokerFileStatus>of()),
                    mock(ComputeResource.class), () -> false);

            // Grouper ran but returned empty -> no submit.
            grouper.verify(() -> PartitionSampleGrouper.group(
                    any(), any(), any(), anyLong(), anyLong()), times(1));
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any(), any()), never());
        }
    }

    // ---------- Source-level ordering invariant ----------

    @Test
    public void firePreSplitHooksRunsBeforeBeginTxnInBrokerLoadJob() {
        // Structural deadlock guard: BrokerLoadJob.createLoadingTask must fire
        // the pre-split hooks BEFORE beginTxn() begins T_load. If T_load were
        // allocated first the reshard daemon's cleanup-phase prev-txn wait
        // would include it, deadlocking the hook's sync-await. The "before
        // task.prepare()" ordering is a transitive consequence — task.prepare()
        // runs inside buildLoadingTasksUnderReadLock which is called after
        // beginTransaction — so it doesn't need its own assertion.
        String source = readSource(
                "fe-core/src/main/java/com/starrocks/load/loadv2/BrokerLoadJob.java");
        // Strip comments + javadoc so this ignores prose references to method names.
        String codeOnly = stripLineComments(source);
        int firePreSplitIdx = codeOnly.indexOf("firePreSplitHooks(context");
        int beginTxnCallIdx = codeOnly.indexOf("beginTxn();");
        Assertions.assertTrue(firePreSplitIdx > 0,
                "firePreSplitHooks( call must exist in BrokerLoadJob source");
        Assertions.assertTrue(beginTxnCallIdx > 0,
                "beginTxn(); call must exist in BrokerLoadJob source");
        Assertions.assertTrue(firePreSplitIdx < beginTxnCallIdx,
                "firePreSplitHooks MUST run BEFORE beginTxn — otherwise the daemon's "
                        + "cleanup-phase prev-txn drain would deadlock the hook's sync-await");
    }

    // ---------- Eligibility skip in the multi-partition path ----------

    @Test
    public void nonRangeDistributionTableSkipsBeforeMultiPartitionFlow() {
        // Defensive: a partitioned but non-range-distribution target must
        // short-circuit at findEligibleTable, before reaching the sampler.
        // We never reach submitForPartitionsCombined.
        OlapTable table = mock(OlapTable.class);
        when(table.isRangeDistribution()).thenReturn(false);

        try (MockedStatic<TabletPreSplitCoordinator> coordinator =
                     Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            BrokerLoadPreSplitHook.maybeRunPreSplit(
                    mockConnectContextWithSessionPreSplit(true),
                    mock(Database.class), table, mock(BrokerDesc.class),
                    List.of(mock(BrokerFileGroup.class)),
                    List.of(List.<TBrokerFileStatus>of()),
                    mock(ComputeResource.class), () -> false);

            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any(), any()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
        }
    }

    // The await-helper polling semantics (finished, aborted, disappeared, timeout)
    // are covered by InsertPreSplitHookFilesPartitionedTest's
    // awaitCombinedJobAllowingFallback* tests now that the helper lives on
    // TabletPreSplitCoordinator. Broker-side coverage is the source-level
    // structural assertion above ("MUST call awaitCombinedJobAllowingFallback").

    // ---------- Persisted session variable round-trip ----------

    @Test
    public void persistedEnableTabletPreSplitKeyIsStable() {
        // BulkLoadJob persists `enable_tablet_pre_split` into sessionVariables so
        // BrokerLoadJob.firePreSplitHooks can re-apply the opt-out after FE
        // failover. The contract is that the key matches SessionVariable.ENABLE_TABLET_PRE_SPLIT
        // — if the key drifts, the persisted opt-out becomes silently inert.
        // This test pins the constant so a rename triggers a compile-and-test break.
        Assertions.assertEquals("enable_tablet_pre_split", SessionVariable.ENABLE_TABLET_PRE_SPLIT,
                "SessionVariable.ENABLE_TABLET_PRE_SPLIT key must remain stable — BulkLoadJob "
                        + "persists this key and BrokerLoadJob.firePreSplitHooks re-applies it across FE failover");
        // Defensive: BrokerLoadJob must still reference the persisted key (regression guard).
        String brokerLoadJobSource = readSource(
                "fe-core/src/main/java/com/starrocks/load/loadv2/BrokerLoadJob.java");
        Assertions.assertTrue(brokerLoadJobSource.contains("SessionVariable.ENABLE_TABLET_PRE_SPLIT"),
                "BrokerLoadJob must reference SessionVariable.ENABLE_TABLET_PRE_SPLIT to re-apply the persisted opt-out");
        // And BulkLoadJob must still persist it.
        String bulkLoadJobSource = readSource(
                "fe-core/src/main/java/com/starrocks/load/loadv2/BulkLoadJob.java");
        Assertions.assertTrue(bulkLoadJobSource.contains("SessionVariable.ENABLE_TABLET_PRE_SPLIT"),
                "BulkLoadJob must persist SessionVariable.ENABLE_TABLET_PRE_SPLIT for FE failover survival");
    }

    // ---------- Outer try/catch swallows internal throws ----------

    @Test
    public void hookExceptionSwallowedNeverAbortsLoad() {
        // Any throw inside tryRunPreSplit must be swallowed by maybeRunPreSplit's
        // outer try/catch. Drive this by passing an OlapTable whose accessor
        // throws on isRangeDistribution. The hook must not let the throw escape
        // — BrokerLoadJob would otherwise abort an already-running pending-task callback.
        OlapTable table = mock(OlapTable.class);
        when(table.isRangeDistribution()).thenThrow(new RuntimeException("simulated table failure"));

        Assertions.assertDoesNotThrow(() ->
                        BrokerLoadPreSplitHook.maybeRunPreSplit(
                                mockConnectContextWithSessionPreSplit(true),
                                mock(Database.class), table, mock(BrokerDesc.class),
                                List.of(mock(BrokerFileGroup.class)),
                                List.of(List.<TBrokerFileStatus>of()),
                                mock(ComputeResource.class), () -> false),
                "hook must never propagate a throw");
    }

    // ---------- Integration-shaped tests (intentionally @Disabled) ----------

    @Test
    @Disabled("end-to-end: requires full FE fixture (catalog, partitions, tablet inverted index, "
            + "compute-resource-bound ConnectContext) and a partitioned Broker Load. Covered by "
            + "the TSP regression suite. The unit-level tests above already cover the multi-"
            + "partition code paths individually. Documents the sync-await invariant: the "
            + "triggering Broker Load itself plans against the post-reshard tablet layout because "
            + "buildLoadingTasksUnderReadLock runs only after awaitCombinedJobAllowingFallback "
            + "returns — task.prepare() builds the sink plan against the already-split layout, "
            + "not against the pre-split single tablet.")
    public void triggeringBrokerLoadSeesPostSplitLayout() {
        // Documented intent:
        //   1. LOAD LABEL ... INSERT INTO partitioned_t SELECT * FROM .../parquet (3 partitions worth)
        //   2. Hook samples, groups into 3 PartitionSamples, pre-creates 2 missing partitions,
        //      submits combined reshard, then sync-awaits awaitCombinedJobAllowingFallback until
        //      the reshard daemon drives the combined job to FINISHED (bounded by
        //      tablet_pre_split_post_submit_wait_seconds).
        //   3. Only after the await returns does BrokerLoadJob call beginTransaction and
        //      buildLoadingTasksUnderReadLock — task.prepare() sees the post-reshard tablet
        //      layout and the BE plan is built against the post-split tablets, not the
        //      original single tablet. A subsequent Broker Load on the same partitioned_t sees
        //      identical layout (it skips the reshard because the layout already matches).
    }

    @Test
    @Disabled("end-to-end: load-proceeds-on-pre-split-failure semantics. Documents that a Skipped "
            + "outcome (PRE_CREATE_FAILED, SUBMIT_FAILED, etc.) from submitForPartitionsCombined "
            + "MUST NOT abort the Broker Load — the await helper fail-safes on submit failure / "
            + "timeout / wait error and BrokerLoadJob proceeds to submitTask regardless. "
            + "Covered by TSP regression suite.")
    public void loadProceedsRegardlessOfPreSplitOutcome() {
        // Documented intent:
        //   1. Force submitForPartitionsCombined to return Skipped(PRE_CREATE_FAILED) by
        //      injecting a LocalMetastore stub whose addPartitions throws.
        //   2. BrokerLoadJob.createLoadingTask proceeds with submitTask — no exception escapes,
        //      load txn is not aborted, no LOAD_RUN_FAIL surfaced.
    }

    // ---------- Helpers ----------

    /**
     * Mock a partitioned, range-distribution {@link OlapTable} that passes
     * every structural gate inside {@link PreSplitTargets#findEligibleTable}.
     * The caller is responsible for ALSO stubbing
     * {@code MetaUtils.getRangeDistributionColumns(table)} via
     * {@link MockedStatic} when the test needs {@code findEligibleTable} to
     * return {@code null} (i.e. the table is actually eligible) — without that
     * stub the bare mock returns an empty sort-key list and the eligibility
     * gate short-circuits with {@code UNSUPPORTED_SORT_KEY}.
     */
    private static OlapTable mockPartitionedRangeTable() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(mock(com.starrocks.catalog.MaterializedIndexMeta.class)));
        when(table.getName()).thenReturn("partitioned_t");
        when(table.supportedAutomaticPartition()).thenReturn(true);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(true);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);
        return table;
    }

    /**
     * Read a source file for structural / ordering tests. The {@code relativePath}
     * is rooted at {@code fe/}; the resolver walks upward from the test working
     * directory until it finds a parent matching that prefix (handles invocation
     * from {@code fe-core/}, {@code fe/}, or the repository root).
     */
    private static String readSource(String relativePath) {
        Path workingDir = Paths.get("").toAbsolutePath();
        // Try each ancestor of the working directory; the first one whose
        // resolution exists is the answer. Surefire forks from fe-core/,
        // mvn invocations sometimes from fe/, and IDE runners often from the
        // repository root — handle all three without hardcoding.
        for (Path candidate = workingDir; candidate != null; candidate = candidate.getParent()) {
            Path direct = candidate.resolve(relativePath);
            if (Files.exists(direct)) {
                return readFileChecked(direct);
            }
            Path underFe = candidate.resolve("fe").resolve(relativePath);
            if (Files.exists(underFe)) {
                return readFileChecked(underFe);
            }
        }
        throw new AssertionError("Failed to locate source file " + relativePath
                + " from working directory " + workingDir);
    }

    private static String readFileChecked(Path path) {
        try {
            return Files.readString(path);
        } catch (Exception readFailure) {
            throw new AssertionError("Failed to read source file " + path + ": " + readFailure.getMessage(),
                    readFailure);
        }
    }

    /**
     * Strip Java line comments and block comments so structural assertions don't
     * trip on documentation that mentions a method by name. A full Java parser is
     * overkill here; a small regex pass over the source is sufficient because the
     * hook file does not contain string literals that collide with the matched
     * names.
     */
    private static String stripLineComments(String source) {
        String withoutBlockComments = source.replaceAll("(?s)/\\*.*?\\*/", "");
        String withoutLineComments = withoutBlockComments.replaceAll("//[^\\n]*", "");
        return withoutLineComments;
    }

    /**
     * Return {@code true} when {@code methodName} appears as a call in
     * {@code code}, i.e. immediately followed by an open paren (possibly with
     * whitespace). Method-reference syntax ({@code ::name}) is intentionally
     * NOT matched: this assertion targets actual invocations, not references.
     */
    private static boolean containsCall(String code, String methodName) {
        return code.matches("(?s).*\\b" + java.util.regex.Pattern.quote(methodName) + "\\s*\\(.*");
    }

    /**
     * Reference to {@link BrokerLoadJob} to ensure the test class compile-pins
     * the production class. The structural ordering test depends on the source
     * shape, so a compile-time pin guards against an accidental relocation /
     * rename of {@code BrokerLoadJob.createLoadingTask}.
     */
    @SuppressWarnings("unused")
    private static final Class<?> BROKER_LOAD_JOB_PIN = BrokerLoadJob.class;
}
