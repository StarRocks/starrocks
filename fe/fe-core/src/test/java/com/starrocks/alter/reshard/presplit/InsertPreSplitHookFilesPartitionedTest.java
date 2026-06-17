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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.Config;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.mockConnectContextWithSessionPreSplit;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Multi-partition support coverage for the FILES path of {@link InsertPreSplitHook}.
 * The single/multi routing and the data-tier sampler branches now live in
 * {@link PreSplitFlow} and are covered by {@link PreSplitFlowTest}; this file
 * retains the FILES-specific building blocks those branches rely on:
 *
 * <ol>
 *   <li>{@link DefaultPreSplitPipeline#forLoadKind} bypasses the meta tier (Parquet
 *       metadata) when the target table is partitioned, falling through to
 *       the data tier (sub-query) — and leaves the meta tier in place for unpartitioned
 *       targets.</li>
 *   <li>The hook's outer {@code maybeRunPreSplit} swallows any throw so an
 *       analyzer failure cannot abort the triggering INSERT.</li>
 *   <li>{@link TabletPreSplitCoordinator#awaitCombinedJobAllowingFallback}
 *       polls {@code TabletReshardJobMgr} for the combined job's terminal
 *       state, bumps {@code tablet_pre_split_post_submit_hard_cap} exactly
 *       once on timeout, and never aborts the load on timeout / abort /
 *       disappearance.</li>
 * </ol>
 *
 * <p>End-to-end "FILES → grouper → pre-create → planner sees new partitions"
 * coverage requires a full FE fixture (catalog, partitions, tablet inverted
 * index, ConnectContext-bound compute resource) and lives in the TSP regression
 * suite; the two integration-shaped
 * tests at the bottom are intentionally {@code @Disabled} with a pointer.
 */
public class InsertPreSplitHookFilesPartitionedTest {

    private boolean savedConfigInsertFromFiles;
    private boolean savedMetricHasInit;
    private long savedPostSubmitWaitSeconds;

    @BeforeEach
    public void setUp() {
        savedConfigInsertFromFiles = Config.enable_tablet_pre_split_for_insert_from_files;
        Config.enable_tablet_pre_split_for_insert_from_files = true;
        savedMetricHasInit = MetricRepo.hasInit;
        savedPostSubmitWaitSeconds = Config.tablet_pre_split_post_submit_wait_seconds;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_tablet_pre_split_for_insert_from_files = savedConfigInsertFromFiles;
        MetricRepo.hasInit = savedMetricHasInit;
        Config.tablet_pre_split_post_submit_wait_seconds = savedPostSubmitWaitSeconds;
    }

    // ---------- DefaultPreSplitPipeline.forLoadKind branching ----------

    @Test
    public void partitionedTableForcesDataTierInPipelineFactory() throws Exception {
        // Force the meta-tier sampler installed by forLoadKind to throw
        // MetaTierUnavailableException — this routes the pipeline to the data tier
        // without ever touching the Parquet footer reader. The pipeline's
        // sample-and-plan path then proceeds against the data-tier sampler only.
        Database database = mock(Database.class);
        OlapTable table = mock(OlapTable.class);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(true);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(mock(TabletReshardJobMgr.class));
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                    database, table, /*oldTabletId*/ 99L, /*fileTotalBytes*/ 1024L,
                    LoadKind.INSERT_FROM_FILES);

            // Drive the partitioned meta-tier sampler directly via reflection over the
            // package-private field. Then verify it raises MetaTierUnavailableException
            // (the signal that routes the pipeline to data tier) regardless of inputs.
            java.lang.reflect.Field metaField = DefaultPreSplitPipeline.class.getDeclaredField("metaTierSampler");
            metaField.setAccessible(true);
            MetaTierSampler installedMetaSampler = (MetaTierSampler) metaField.get(pipeline);

            Assertions.assertThrows(MetaTierUnavailableException.class,
                    () -> installedMetaSampler.tryPlan(mock(SampleRequest.class), /*K*/ 3),
                    "partitioned-table meta-tier sampler must signal data-tier fallback");
        }
    }

    @Test
    public void unpartitionedTableUsesProductionMetaTierSampler() throws Exception {
        // For unpartitioned tables, forLoadKind installs the production
        // ParquetMetadataSampler — verified by checking the installed sampler is
        // a method reference whose underlying class is ParquetMetadataSampler.
        // (Functional-interface lambdas don't preserve target identity, so we
        // probe behavior instead: the no-op partitioned lambda throws
        // MetaTierUnavailableException unconditionally; the production sampler
        // requires a real RowGroupStatisticsProvider and does NOT throw that
        // exception for arbitrary inputs.)
        Database database = mock(Database.class);
        OlapTable table = mock(OlapTable.class);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(false);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(mock(TabletReshardJobMgr.class));
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                    database, table, /*oldTabletId*/ 99L, /*fileTotalBytes*/ 1024L,
                    LoadKind.INSERT_FROM_FILES);

            java.lang.reflect.Field metaField = DefaultPreSplitPipeline.class.getDeclaredField("metaTierSampler");
            metaField.setAccessible(true);
            MetaTierSampler installedMetaSampler = (MetaTierSampler) metaField.get(pipeline);

            // The production sampler is a method reference on ParquetMetadataSampler.
            // Its declaring class name contains "ParquetMetadataSampler" via the
            // captured this$0 — but we can't easily introspect a method reference.
            // Instead we assert the inverse of the partitioned test: calling tryPlan
            // does NOT raise MetaTierUnavailableException with this signal-only
            // message (the production sampler may throw OTHER exceptions when
            // given a mock request, but never the partitioned-table sentinel).
            try {
                installedMetaSampler.tryPlan(mock(SampleRequest.class), /*K*/ 3);
            } catch (MetaTierUnavailableException unexpectedFallback) {
                // The production sampler may also raise MetaTierUnavailableException
                // for legitimate reasons (no row groups, threshold exceeded). What
                // we need to confirm is that the message is NOT the partitioned
                // sentinel installed by the partitioned branch.
                Assertions.assertFalse(unexpectedFallback.getMessage().contains("partitioned table forces"),
                        "unpartitioned table must not install the partitioned-table meta-tier sentinel");
            } catch (Throwable ignored) {
                // Any other exception path is acceptable here — the production
                // sampler legitimately throws on a mock request. The assertion
                // we need is that MetaTierUnavailableException with the sentinel
                // message did NOT escape; we confirmed that in the catch above.
            }
        }
    }

    // ---------- Automatic-partition gate (PreSplitFlow.dispatch) ----------

    @Test
    public void skipsManuallyPartitionedFilesTarget() {
        // A partitioned FILES target whose supportedAutomaticPartition() is false (a manual
        // list/range partition table) must not reach runMultiPartitionFlow: pre-creating
        // partitions from sampled values would fabricate system partitions outside the
        // user-defined set. The automatic-partition gate lives in PreSplitFlow.dispatch and
        // reaches through every INSERT entry; this drives the FILES load kind directly.
        //
        // The full multi-partition scaffolding (CN count, sampler, grouper) is wired even
        // though the gate short-circuits before any of it runs: were the gate removed, the
        // flow would progress all the way to submitForPartitionsCombined and FAIL on the
        // never() verification below — a clean bite signal rather than an NPE inside
        // computeNodeCount on uninitialized global state.
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);

        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("manual_partitioned_files_t");
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(true);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);
        when(table.supportedAutomaticPartition()).thenReturn(false);

        PreSplitFlow.Prepared prepared = filesPrepared();
        SampleSet samples = new SampleSet(List.of(), List.of(), Estimates.ZERO);

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

            PreSplitFlow.dispatch(database, table, prepared, LoadKind.INSERT_FROM_FILES,
                    () -> false, mock(ConnectContext.class));

            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
        }
    }

    /**
     * Build a {@link PreSplitFlow.Prepared} bundle carrying an
     * {@link InsertFromFilesScanContext} so the dispatched flow is genuinely FILES-flavored.
     * The sort key / partition columns are the ones the data-tier sampler would project.
     */
    private static PreSplitFlow.Prepared filesPrepared() {
        InsertFromFilesScanContext scanContext =
                new InsertFromFilesScanContext(mock(TableFunctionTable.class), mock(ComputeResource.class));
        List<Column> sortKey = List.of(bigintColumn("sort_col"));
        List<Column> partitionColumns = List.of(bigintColumn("p_col"));
        return new PreSplitFlow.Prepared(scanContext, sortKey, partitionColumns,
                /*estimatedBytes*/ 0L, mock(ComputeResource.class));
    }

    // ---------- Outer try/catch swallows internal throws ----------

    @Test
    public void hookExceptionSwallowedNeverAbortsInsert() {
        // Any throw inside tryRunPreSplit must be swallowed by maybeRunPreSplit's
        // outer try/catch. Drive this by passing a StatementBase that throws on
        // every accessor; the hook must not let the throw escape because it runs
        // before the planner — escaping would abort an otherwise-valid INSERT.
        StatementBase parsedStmt = mock(StatementBase.class);
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);

        // No assertion needed beyond "no throw escapes" — Assertions.assertDoesNotThrow
        // is the contract.
        Assertions.assertDoesNotThrow(() ->
                InsertPreSplitHook.maybeRunPreSplit(parsedStmt, context),
                "hook must never propagate a throw");
    }

    // ---------- awaitCombinedJobAllowingFallback semantics ----------

    @Test
    public void awaitReturnsImmediatelyWhenJobAlreadyFinishedSinglePoll() {
        // Job is FINISHED on the first poll -> helper returns without bumping any
        // counter. The single-poll assertion (times(1) on getTabletReshardJob)
        // is the per-call-of-the-helper proof. The "ONCE per combined-job
        // submission, NOT once per PartitionSamples" structural invariant is
        // enforced by the single call site in PreSplitFlow.runMultiPartitionFlow.
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t_finished");

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        when(combinedJob.getJobId()).thenReturn(1001L);

        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);
        TabletReshardJob latest = mock(TabletReshardJob.class);
        when(latest.getJobState()).thenReturn(TabletReshardJob.JobState.FINISHED);
        when(reshardMgr.getTabletReshardJob(1001L)).thenReturn(latest);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardMgr);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            Assertions.assertDoesNotThrow(() ->
                    TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                            LoadKind.INSERT_FROM_FILES, table, combinedJob, () -> false));

            verify(reshardMgr, times(1)).getTabletReshardJob(1001L);
        }
    }

    @Test
    public void awaitReturnsOnAbortedTerminalState() {
        // Job is ABORTED on the first poll -> helper logs and returns without
        // throwing or bumping the hard-cap counter (timeout is the only path
        // that bumps hard-cap; aborted is a terminal-but-not-timeout path).
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t_aborted");

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        when(combinedJob.getJobId()).thenReturn(1002L);

        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);
        TabletReshardJob latest = mock(TabletReshardJob.class);
        when(latest.getJobState()).thenReturn(TabletReshardJob.JobState.ABORTED);
        when(latest.getErrorMessage()).thenReturn("synthetic abort");
        when(reshardMgr.getTabletReshardJob(1002L)).thenReturn(latest);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardMgr);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            Assertions.assertDoesNotThrow(() ->
                    TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                            LoadKind.INSERT_FROM_FILES, table, combinedJob, () -> false));
        }
    }

    @Test
    public void awaitReturnsWhenJobDisappearsFromManager() {
        // The reshard daemon may evict a finished job from its in-memory map
        // before the helper polls. The helper logs and returns; never throws.
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t_disappeared");

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        when(combinedJob.getJobId()).thenReturn(1003L);

        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);
        when(reshardMgr.getTabletReshardJob(1003L)).thenReturn(null);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardMgr);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            Assertions.assertDoesNotThrow(() ->
                    TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                            LoadKind.INSERT_FROM_FILES, table, combinedJob, () -> false));
        }
    }

    @Test
    public void awaitTimeoutDoesNotAbortInsertAndBumpsHardCapBvar() {
        // Pin the wait to 0 seconds; first deadline check after the first poll
        // expires immediately. Helper logs the timeout, bumps the hard-cap
        // counter, and returns — never throws.
        Config.tablet_pre_split_post_submit_wait_seconds = 0L;

        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t_timeout");

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        when(combinedJob.getJobId()).thenReturn(1004L);

        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);
        TabletReshardJob latest = mock(TabletReshardJob.class);
        // Job stuck in PENDING — deadline must fire on the first iteration.
        when(latest.getJobState()).thenReturn(TabletReshardJob.JobState.PENDING);
        when(reshardMgr.getTabletReshardJob(1004L)).thenReturn(latest);

        // Wire every pre-split metric the helper touches with MetricRepo.hasInit=true
        // so we can assert the post-submit-hard-cap bump without NPE-ing on the
        // sibling histogram update. Same pattern as TabletPreSplitCoordinatorTest's
        // withCoordinatorMetricsWired helper.
        withPreSplitMetricsWired(hardCapCounter -> {
            long baseline = hardCapCounter.getValue();

            try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
                GlobalStateMgr globalState = mock(GlobalStateMgr.class);
                when(globalState.getTabletReshardJobMgr()).thenReturn(reshardMgr);
                gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

                Assertions.assertDoesNotThrow(() ->
                        TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                                LoadKind.INSERT_FROM_FILES, table, combinedJob, () -> false),
                        "timeout must NEVER abort the INSERT — log + return only");
            }

            Assertions.assertEquals(baseline + 1L, hardCapCounter.getValue().longValue(),
                    "post-submit-hard-cap bvar must bump exactly once on timeout");
        });
    }

    @Test
    public void awaitPollsAgainAfterNonTerminalStateThenReturnsOnFinished() {
        // First poll: PENDING (non-terminal, before the 300s deadline) -> the helper
        // sleeps DEFAULT_POLL_INTERVAL and loops. Second poll: FINISHED -> returns.
        // Drives the sleep + second-iteration body (the loop continuation path that a
        // single-poll test cannot reach).
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t_two_polls");

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        when(combinedJob.getJobId()).thenReturn(2001L);

        TabletReshardJob pending = mock(TabletReshardJob.class);
        when(pending.getJobState()).thenReturn(TabletReshardJob.JobState.PENDING);
        TabletReshardJob finished = mock(TabletReshardJob.class);
        when(finished.getJobState()).thenReturn(TabletReshardJob.JobState.FINISHED);

        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);
        // PENDING on the first poll, FINISHED on the second.
        when(reshardMgr.getTabletReshardJob(2001L)).thenReturn(pending, finished);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardMgr);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            Assertions.assertDoesNotThrow(() ->
                    TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                            LoadKind.INSERT_FROM_FILES, table, combinedJob, () -> false));

            // Two polls: PENDING then FINISHED. Proves the loop continued past the
            // first non-terminal state (sleep + re-poll body).
            verify(reshardMgr, times(2)).getTabletReshardJob(2001L);
        }
    }

    @Test
    public void awaitReturnsImmediatelyWhenShouldAbortReturnsTrue() {
        // Cancel-aware path: a supplier that returns true short-circuits the
        // helper before it ever polls TabletReshardJobMgr. The helper logs
        // the abort and returns without throwing or bumping the hard-cap
        // counter (the load is cancelled by the caller, not timed out here).
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t_caller_abort");

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        when(combinedJob.getJobId()).thenReturn(3001L);

        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardMgr);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            Assertions.assertDoesNotThrow(() ->
                    TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                            LoadKind.INSERT_FROM_FILES, table, combinedJob, () -> true));

            // shouldAbort fires before any TabletReshardJobMgr poll.
            verify(reshardMgr, never()).getTabletReshardJob(3001L);
        }
    }

    @Test
    public void awaitReturnsWhenInterruptedAndPreservesInterruptFlag() {
        // Pre-set the current thread's interrupt flag so Thread.sleep throws
        // InterruptedException on the first non-terminal poll. The helper must
        // re-assert the interrupt flag and return without throwing.
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t_interrupted");

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        when(combinedJob.getJobId()).thenReturn(2002L);

        TabletReshardJob pending = mock(TabletReshardJob.class);
        when(pending.getJobState()).thenReturn(TabletReshardJob.JobState.PENDING);

        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);
        when(reshardMgr.getTabletReshardJob(2002L)).thenReturn(pending);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardMgr);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            Thread.currentThread().interrupt();
            try {
                Assertions.assertDoesNotThrow(() ->
                        TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                                LoadKind.INSERT_FROM_FILES, table, combinedJob, () -> false),
                        "interrupt must be handled internally — never propagate");
                Assertions.assertTrue(Thread.currentThread().isInterrupted(),
                        "helper must re-assert the interrupt flag before returning");
            } finally {
                // Clear the interrupt flag so it does not leak into sibling tests.
                Thread.interrupted();
            }
        }
    }

    /**
     * Wire the post-submit-hard-cap counter and the post-submit-wait histogram
     * the helper touches when {@link MetricRepo#hasInit} is {@code true}. The
     * {@code body} consumer receives the wired hard-cap counter so the test can
     * assert its bump. Restores previous state in a finally block so other tests
     * stay isolated. Mirrors {@code TabletPreSplitCoordinatorTest.withCoordinatorMetricsWired}.
     */
    private static void withPreSplitMetricsWired(java.util.function.Consumer<LongCounterMetric> body) {
        LongCounterMetric hardCap = new LongCounterMetric(
                "tablet_pre_split_post_submit_hard_cap", MetricUnit.REQUESTS, "hard-cap events");
        MetricRegistry localRegistry = new MetricRegistry();

        LongCounterMetric savedHardCap = MetricRepo.COUNTER_TABLET_PRE_SPLIT_POST_SUBMIT_HARD_CAP;
        Histogram savedPostSubmitHistogram = MetricRepo.HISTO_TABLET_PRE_SPLIT_POST_SUBMIT_WAIT_MS;
        boolean savedHasInit = MetricRepo.hasInit;

        MetricRepo.COUNTER_TABLET_PRE_SPLIT_POST_SUBMIT_HARD_CAP = hardCap;
        MetricRepo.HISTO_TABLET_PRE_SPLIT_POST_SUBMIT_WAIT_MS = localRegistry.histogram("post_submit_wait_ms");
        MetricRepo.hasInit = true;
        try {
            body.accept(hardCap);
        } finally {
            MetricRepo.hasInit = savedHasInit;
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_POST_SUBMIT_HARD_CAP = savedHardCap;
            MetricRepo.HISTO_TABLET_PRE_SPLIT_POST_SUBMIT_WAIT_MS = savedPostSubmitHistogram;
        }
    }

    // ---------- Integration-shaped tests (intentionally @Disabled) ----------

    @Test
    @Disabled("end-to-end: requires full FE fixture (catalog, partitions, tablet inverted index, "
            + "compute-resource-bound ConnectContext). Covered by the TSP regression suite. "
            + "The unit-level boundary tests above already cover "
            + "the multi-partition code paths individually.")
    public void preCreatedPartitionsVisibleToPlannerAfterHook() {
        // Documented intent:
        //   1. INSERT INTO partitioned_t SELECT * FROM FILES(... 3 partitions worth of data ...)
        //   2. Hook samples, groups into 3 PartitionSamples, pre-creates the 2 missing ones
        //   3. After awaitCombinedJobAllowingFallback returns, the planner reads the catalog
        //      and observes ALL 3 partitions plus their split tablet layouts
    }

    @Test
    @Disabled("end-to-end: ALTER TABLE ADD PARTITION semantics. Documents the known leak — "
            + "pre-created empty partitions stay in catalog if the INSERT itself fails. "
            + "Aligned with manual `ALTER TABLE ... ADD PARTITION` behavior; cleanup is "
            + "operator-driven. Covered by the design doc § Pre-Create Failure Modes.")
    public void preCreatedPartitionsLeakedOnInsertFailure() {
        // Documented intent:
        //   1. Hook runs, pre-creates 2 new partitions, submits combined reshard
        //   2. The INSERT itself fails post-hook (analyzer error, txn abort, etc.)
        //   3. The 2 pre-created partitions remain in the catalog (no cleanup)
        // This is intentional behavior — ALTER TABLE ADD PARTITION has the same
        // semantic. Cleanup is operator-driven, not hook-driven.
    }
}
