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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricRepo;
import com.starrocks.planner.LoadScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
 * Tests for the multi-partition pre-split flow added in
 * {@link InsertFromFilesPreSplitHook}. The hook itself is heavily coupled to
 * {@code StmtExecutor}-time analyzer/authorizer/catalog state — each early
 * branch already has dedicated coverage in {@link InsertFromFilesPreSplitHookTest},
 * which exhaustively drives the AST-shape filters. This file focuses on the
 * multi-partition code paths:
 *
 * <ol>
 *   <li>{@link DefaultPreSplitPipeline#forLoadKind} bypasses the meta tier (Parquet
 *       metadata) when the target table is partitioned, falling through to
 *       the data tier (sub-query) — and leaves the meta tier in place for unpartitioned
 *       targets.</li>
 *   <li>The hook's outer {@code maybeRunPreSplit} swallows any throw from the
 *       partitioned-branch resolution so an analyzer failure cannot abort the
 *       triggering INSERT.</li>
 *   <li>{@link InsertFromFilesPreSplitHook#awaitCombinedJobAllowingFallback}
 *       polls {@code TabletReshardJobMgr} for the combined job's terminal
 *       state, bumps {@code tablet_pre_split_post_submit_hard_cap} exactly
 *       once on timeout, and never aborts the load on timeout / abort /
 *       disappearance. The "ONCE per combined-job submission, NOT once per
 *       PartitionSamples" structural invariant is enforced by the single
 *       call site in {@code runMultiPartitionFlow} (see the call-site
 *       comment in the production source).</li>
 * </ol>
 *
 * <p>End-to-end "FILES → grouper → pre-create → planner sees new partitions"
 * coverage requires a full FE fixture (catalog, partitions, tablet inverted
 * index, ConnectContext-bound compute resource) and lives in the TSP regression
 * suite; the two integration-shaped
 * tests at the bottom are intentionally {@code @Disabled} with a pointer.
 */
public class InsertFromFilesPreSplitHookPartitionedTest {

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
                InsertFromFilesPreSplitHook.maybeRunPreSplit(parsedStmt, context),
                "hook must never propagate a throw");
    }

    // ---------- awaitCombinedJobAllowingFallback semantics ----------

    @Test
    public void awaitReturnsImmediatelyWhenJobAlreadyFinishedSinglePoll() {
        // Job is FINISHED on the first poll -> helper returns without bumping any
        // counter. The single-poll assertion (times(1) on getTabletReshardJob)
        // is the per-call-of-the-helper proof. The "ONCE per combined-job
        // submission, NOT once per PartitionSamples" structural invariant is
        // enforced by the single call site in runMultiPartitionFlow (see the
        // call-site comment above InsertFromFilesPreSplitHook line 246).
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
                    InsertFromFilesPreSplitHook.awaitCombinedJobAllowingFallback(table, combinedJob));

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
                    InsertFromFilesPreSplitHook.awaitCombinedJobAllowingFallback(table, combinedJob));
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
                    InsertFromFilesPreSplitHook.awaitCombinedJobAllowingFallback(table, combinedJob));
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
                        InsertFromFilesPreSplitHook.awaitCombinedJobAllowingFallback(table, combinedJob),
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
                    InsertFromFilesPreSplitHook.awaitCombinedJobAllowingFallback(table, combinedJob));

            // Two polls: PENDING then FINISHED. Proves the loop continued past the
            // first non-terminal state (sleep + re-poll body).
            verify(reshardMgr, times(2)).getTabletReshardJob(2001L);
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
                        InsertFromFilesPreSplitHook.awaitCombinedJobAllowingFallback(table, combinedJob),
                        "interrupt must be handled internally — never propagate");
                Assertions.assertTrue(Thread.currentThread().isInterrupted(),
                        "helper must re-assert the interrupt flag before returning");
            } finally {
                // Clear the interrupt flag so it does not leak into sibling tests.
                Thread.interrupted();
            }
        }
    }

    // ---------- runMultiPartitionFlow body (driven via reflection) ----------

    @Test
    public void runMultiPartitionFlowSubmittedDrivesAwaitOnce() throws Exception {
        // INSERT path: a SubmittedCombined outcome MUST drive
        // awaitCombinedJobAllowingFallback exactly once (the single await call site).
        // Sampler returns a non-empty SampleSet, grouper returns a non-empty list,
        // and the coordinator returns SubmittedCombined whose combined job is already
        // FINISHED (so the await returns on the first poll).
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);
        OlapTable table = mockPartitionedSamplerTable();
        TableFunctionTable sourceTable = mockSourceTable();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

        SampleSet samples = new SampleSet(List.of(), List.of(), Estimates.ZERO);

        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        when(combinedJob.getJobId()).thenReturn(3001L);
        TabletReshardJob finishedLatest = mock(TabletReshardJob.class);
        when(finishedLatest.getJobState()).thenReturn(TabletReshardJob.JobState.FINISHED);
        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);
        when(reshardMgr.getTabletReshardJob(3001L)).thenReturn(finishedLatest);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<LoadScanNode> loadScanNode = Mockito.mockStatic(LoadScanNode.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenReturn(samples))) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardMgr);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);
            loadScanNode.when(() -> LoadScanNode.getAvailableComputeNodes(any())).thenReturn(List.of());

            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(bigintColumn("sort_col")));
            grouper.when(() -> PartitionSampleGrouper.group(
                            any(SampleSet.class), any(OlapTable.class), any(ConnectContext.class),
                            anyLong(), anyLong()))
                    .thenReturn(List.of(mock(PartitionSamples.class)));
            coordinator.when(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                            any(), any(), anyList(), anyInt(), any()))
                    .thenReturn(new PreSplitOutcome.SubmittedCombined(combinedJob, List.of()));

            invokeRunMultiPartitionFlow(database, table, sourceTable, context);

            // The combined submit ran once...
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), times(1));
            // ...and the await loop polled the combined job (SubmittedCombined branch).
            verify(reshardMgr, times(1)).getTabletReshardJob(3001L);
        }
    }

    @Test
    public void runMultiPartitionFlowSkippedDoesNotAwait() throws Exception {
        // A Skipped outcome (e.g. NO_USEFUL_CUTS) must NOT enter the await branch:
        // there is no combined job to poll. Verify the reshard manager is never asked
        // for a job.
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);
        OlapTable table = mockPartitionedSamplerTable();
        TableFunctionTable sourceTable = mockSourceTable();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

        SampleSet samples = new SampleSet(List.of(), List.of(), Estimates.ZERO);
        TabletReshardJobMgr reshardMgr = mock(TabletReshardJobMgr.class);

        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<LoadScanNode> loadScanNode = Mockito.mockStatic(LoadScanNode.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenReturn(samples))) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getTabletReshardJobMgr()).thenReturn(reshardMgr);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);
            loadScanNode.when(() -> LoadScanNode.getAvailableComputeNodes(any())).thenReturn(List.of());

            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(bigintColumn("sort_col")));
            grouper.when(() -> PartitionSampleGrouper.group(
                            any(SampleSet.class), any(OlapTable.class), any(ConnectContext.class),
                            anyLong(), anyLong()))
                    .thenReturn(List.of(mock(PartitionSamples.class)));
            coordinator.when(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                            any(), any(), anyList(), anyInt(), any()))
                    .thenReturn(new PreSplitOutcome.Skipped(SkipReason.NO_USEFUL_CUTS));

            invokeRunMultiPartitionFlow(database, table, sourceTable, context);

            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), times(1));
            // No SubmittedCombined -> no await -> reshard manager never polled.
            verify(reshardMgr, never()).getTabletReshardJob(anyLong());
        }
    }

    @Test
    public void runMultiPartitionFlowNullSamplesSkipsBeforeGrouper() throws Exception {
        // runDataTierSampler returns null (sampler threw) -> runMultiPartitionFlow
        // returns before the grouper / coordinator are ever touched.
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);
        OlapTable table = mockPartitionedSamplerTable();
        TableFunctionTable sourceTable = mockSourceTable();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

        try (MockedStatic<LoadScanNode> loadScanNode = Mockito.mockStatic(LoadScanNode.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class)))
                                .thenThrow(new StarRocksException("synthetic sample failure")))) {
            loadScanNode.when(() -> LoadScanNode.getAvailableComputeNodes(any())).thenReturn(List.of());
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(bigintColumn("sort_col")));

            invokeRunMultiPartitionFlow(database, table, sourceTable, context);

            grouper.verify(() -> PartitionSampleGrouper.group(
                    any(), any(), any(), anyLong(), anyLong()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), never());
        }
    }

    @Test
    public void runMultiPartitionFlowEmptyGroupsSkipsBeforeSubmit() throws Exception {
        // Sampler succeeds but grouper returns an empty list -> short-circuit before
        // submitForPartitionsCombined (grouper already recorded its own skip bvar).
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);
        OlapTable table = mockPartitionedSamplerTable();
        TableFunctionTable sourceTable = mockSourceTable();
        ConnectContext context = mockConnectContextWithSessionPreSplit(true);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));

        SampleSet samples = new SampleSet(List.of(), List.of(), Estimates.ZERO);

        try (MockedStatic<LoadScanNode> loadScanNode = Mockito.mockStatic(LoadScanNode.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<PartitionSampleGrouper> grouper = Mockito.mockStatic(PartitionSampleGrouper.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenReturn(samples))) {
            loadScanNode.when(() -> LoadScanNode.getAvailableComputeNodes(any())).thenReturn(List.of());
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(bigintColumn("sort_col")));
            grouper.when(() -> PartitionSampleGrouper.group(
                            any(SampleSet.class), any(OlapTable.class), any(ConnectContext.class),
                            anyLong(), anyLong()))
                    .thenReturn(List.of());

            invokeRunMultiPartitionFlow(database, table, sourceTable, context);

            grouper.verify(() -> PartitionSampleGrouper.group(
                    any(), any(), any(), anyLong(), anyLong()), times(1));
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), never());
        }
    }

    // ---------- runDataTierSampler (driven via reflection) ----------

    @Test
    public void runDataTierSamplerReturnsSampleSetOnSuccess() throws Exception {
        // The data-tier sampler succeeds -> the SampleSet is returned unchanged.
        OlapTable table = mockPartitionedSamplerTable();
        TableFunctionTable sourceTable = mockSourceTable();
        SampleSet expected = new SampleSet(List.of(), List.of(), Estimates.ZERO);

        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenReturn(expected))) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(bigintColumn("sort_col")));

            Object result = invokeRunDataTierSampler(table, sourceTable, mock(ComputeResource.class));
            Assertions.assertSame(expected, result, "successful sample must be returned unchanged");
        }
    }

    @Test
    public void runDataTierSamplerReturnsNullOnStarRocksException() throws Exception {
        // StarRocksException from the sampler -> caught, SAMPLE_FAILED bvar recorded, null returned.
        OlapTable table = mockPartitionedSamplerTable();
        TableFunctionTable sourceTable = mockSourceTable();

        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class)))
                                .thenThrow(new StarRocksException("synthetic checked sample failure")))) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(bigintColumn("sort_col")));

            Assertions.assertNull(invokeRunDataTierSampler(table, sourceTable, mock(ComputeResource.class)),
                    "StarRocksException sampler failure must yield null");
        }
    }

    @Test
    public void runDataTierSamplerReturnsNullOnRuntimeException() throws Exception {
        // RuntimeException from the sampler -> caught by the second catch arm, null returned.
        OlapTable table = mockPartitionedSamplerTable();
        TableFunctionTable sourceTable = mockSourceTable();

        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                        (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class)))
                                .thenThrow(new RuntimeException("synthetic runtime sample failure")))) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(bigintColumn("sort_col")));

            Assertions.assertNull(invokeRunDataTierSampler(table, sourceTable, mock(ComputeResource.class)),
                    "RuntimeException sampler failure must yield null");
        }
    }

    // ---------- Reflection helpers for the private multi-partition methods ----------

    /**
     * Mock a partitioned {@link OlapTable} whose partition-info answers the
     * sampler's {@code getPartitionColumns} call. The sort-key list is supplied
     * separately by the per-test {@code MetaUtils.getRangeDistributionColumns}
     * stub. The hook resolves partition source columns via
     * {@code table.getPartitionInfo().getPartitionColumns(table.getIdToColumn())}.
     */
    private static OlapTable mockPartitionedSamplerTable() {
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("partitioned_insert_t");
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(true);
        List<Column> partitionColumns = List.of(bigintColumn("p_col"));
        when(partitionInfo.getPartitionColumns(any())).thenReturn(partitionColumns);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);
        return table;
    }

    /** Mock a FILES source table with one zero-byte file so {@code sumFileBytes} is harmless. */
    private static TableFunctionTable mockSourceTable() {
        TableFunctionTable sourceTable = mock(TableFunctionTable.class);
        when(sourceTable.loadFileList()).thenReturn(List.<TBrokerFileStatus>of());
        return sourceTable;
    }

    private static void invokeRunMultiPartitionFlow(
            Database database, OlapTable table, TableFunctionTable sourceTable, ConnectContext context)
            throws Exception {
        Method method = InsertFromFilesPreSplitHook.class.getDeclaredMethod(
                "runMultiPartitionFlow", Database.class, OlapTable.class, TableFunctionTable.class,
                ConnectContext.class);
        method.setAccessible(true);
        try {
            method.invoke(null, database, table, sourceTable, context);
        } catch (InvocationTargetException invocationFailure) {
            // Surface the real cause so a production throw is not masked by reflection.
            Throwable cause = invocationFailure.getCause();
            throw cause instanceof Exception ? (Exception) cause : new RuntimeException(cause);
        }
    }

    private static Object invokeRunDataTierSampler(
            OlapTable table, TableFunctionTable sourceTable, ComputeResource computeResource) throws Exception {
        Method method = InsertFromFilesPreSplitHook.class.getDeclaredMethod(
                "runDataTierSampler", OlapTable.class, TableFunctionTable.class, ComputeResource.class);
        method.setAccessible(true);
        try {
            return method.invoke(null, table, sourceTable, computeResource);
        } catch (InvocationTargetException invocationFailure) {
            Throwable cause = invocationFailure.getCause();
            throw cause instanceof Exception ? (Exception) cause : new RuntimeException(cause);
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
