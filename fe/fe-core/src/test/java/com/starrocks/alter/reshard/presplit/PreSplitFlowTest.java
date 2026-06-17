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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
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
 * Unit tests for the shared {@link PreSplitFlow} core extracted from the
 * per-load-kind entry hooks. The methods under test are package-private, so the
 * tests call them directly (no reflection). {@link LoadKind#INSERT_FROM_FILES}
 * is used as the representative kind; the flow itself is load-kind-agnostic
 * past the {@code Prepared} bundle.
 *
 * <p>Coverage mirrors the per-hook tests' Mockito patterns
 * ({@link InsertPreSplitHookFilesPartitionedTest}): {@code MockedStatic} on
 * {@link TabletPreSplitCoordinator} / {@link PreSplitTargets} /
 * {@link PartitionSampleGrouper} / {@link DefaultPreSplitPipeline} and a
 * {@code MockedConstruction<ReservoirSampler>} for the direct data-tier
 * sampler.
 */
public class PreSplitFlowTest {

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

    // ---------- dispatch routing ----------

    @Test
    public void dispatchRoutesUnpartitionedToSingle() {
        // Unpartitioned target -> single-partition flow -> submitAsynchronously runs,
        // submitForPartitionsCombined never.
        Database database = mock(Database.class);
        OlapTable table = mockTable(/*partitioned*/ false, /*automatic*/ false);
        PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<PreSplitTargets> targets = Mockito.mockStatic(PreSplitTargets.class);
                MockedStatic<DefaultPreSplitPipeline> pipelineStatic =
                        Mockito.mockStatic(DefaultPreSplitPipeline.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            stubEligibleTarget(targets, database, table);
            stubPipelineFactory(pipelineStatic);
            coordinator.when(() -> TabletPreSplitCoordinator.submitAsynchronously(
                            any(), any(), anyLong(), any(), any(), any(), anyInt()))
                    .thenReturn(new PreSplitOutcome.Skipped(SkipReason.NO_USEFUL_CUTS));

            PreSplitFlow.dispatch(database, table, prepared, LoadKind.INSERT_FROM_FILES,
                    () -> false, mock(ConnectContext.class));

            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), times(1));
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), never());
        }
    }

    @Test
    public void dispatchRoutesPartitionedAutomaticToMulti() {
        // Partitioned + supportedAutomaticPartition() == TRUE -> multi-partition flow ->
        // submitForPartitionsCombined runs, submitAsynchronously never.
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);
        OlapTable table = mockTable(/*partitioned*/ true, /*automatic*/ true);
        PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));
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
            coordinator.when(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                            any(), any(), anyList(), anyInt(), any()))
                    .thenReturn(new PreSplitOutcome.Skipped(SkipReason.NO_USEFUL_CUTS));

            PreSplitFlow.dispatch(database, table, prepared, LoadKind.INSERT_FROM_FILES,
                    () -> false, mock(ConnectContext.class));

            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), times(1));
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
        }
    }

    @Test
    public void dispatchSkipsManuallyPartitioned() {
        // Partitioned + supportedAutomaticPartition() returns false (manual list/range
        // partitions) -> the hoisted automatic-partition gate skips before either submit.
        //
        // The full multi-partition scaffolding (CN-count, sampler, grouper) is wired even
        // though the gate should short-circuit before any of it runs. This is deliberate:
        // were the gate ever removed, the flow would progress all the way to
        // submitForPartitionsCombined and FAIL on the never() verification below — a clean
        // regression signal — instead of NPE-ing inside computeNodeCount on uninitialized
        // global state.
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);
        OlapTable table = mockTable(/*partitioned*/ true, /*automatic*/ false);
        PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));
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

            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.submitForPartitionsCombined(
                    any(), any(), anyList(), anyInt(), any()), never());
        }
    }

    // ---------- single-partition flow await ----------

    @Test
    public void singleFlowAwaitsOnSubmitted() {
        // submitAsynchronously returns Submitted -> awaitFinishedAllowingFallback invoked
        // once, threading through the passed shouldAbort supplier.
        Database database = mock(Database.class);
        OlapTable table = mockTable(/*partitioned*/ false, /*automatic*/ false);
        PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));

        PreSplitPipeline.PreparedReshardJob preparedJob = mock(PreSplitPipeline.PreparedReshardJob.class);
        BooleanSupplierMarker shouldAbort = new BooleanSupplierMarker();

        try (MockedStatic<TabletReshardUtils> reshardUtils = PresplitTestSupport.stubComputeNodeCount(1);
                MockedStatic<PreSplitTargets> targets = Mockito.mockStatic(PreSplitTargets.class);
                MockedStatic<DefaultPreSplitPipeline> pipelineStatic =
                        Mockito.mockStatic(DefaultPreSplitPipeline.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            stubEligibleTarget(targets, database, table);
            DefaultPreSplitPipeline pipeline = stubPipelineFactory(pipelineStatic);
            coordinator.when(() -> TabletPreSplitCoordinator.submitAsynchronously(
                            any(), any(), anyLong(), any(), any(), any(), anyInt()))
                    .thenReturn(new PreSplitOutcome.Submitted(preparedJob));

            PreSplitFlow.runSinglePartitionFlow(database, table, prepared,
                    LoadKind.INSERT_FROM_FILES, shouldAbort);

            coordinator.verify(() -> TabletPreSplitCoordinator.awaitFinishedAllowingFallback(
                    eq(LoadKind.INSERT_FROM_FILES), eq(table), eq(pipeline), eq(preparedJob), eq(shouldAbort)),
                    times(1));
        }
    }

    @Test
    public void singleFlowSkipsWhenNoEligibleTarget() {
        // PreSplitTargets.findEligibleTarget returns null -> the guard short-circuits
        // before the pipeline factory and the coordinator: no submit, no await.
        Database database = mock(Database.class);
        OlapTable table = mockTable(/*partitioned*/ false, /*automatic*/ false);
        PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));

        try (MockedStatic<PreSplitTargets> targets = Mockito.mockStatic(PreSplitTargets.class);
                MockedStatic<TabletPreSplitCoordinator> coordinator =
                        Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            targets.when(() -> PreSplitTargets.findEligibleTarget(database, table)).thenReturn(null);

            PreSplitFlow.runSinglePartitionFlow(database, table, prepared,
                    LoadKind.INSERT_FROM_FILES, () -> false);

            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
            coordinator.verify(() -> TabletPreSplitCoordinator.awaitFinishedAllowingFallback(
                    any(), any(), any(), any(), any()), never());
        }
    }

    // ---------- multi-partition flow await ----------

    @Test
    public void multiFlowAwaitsCombinedOnSubmitted() {
        // submitForPartitionsCombined returns SubmittedCombined -> awaitCombinedJobAllowingFallback
        // invoked once on the combined job, threading through the passed shouldAbort supplier.
        Database database = mock(Database.class);
        when(database.getId()).thenReturn(7L);
        OlapTable table = mockTable(/*partitioned*/ true, /*automatic*/ true);
        PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));
        SampleSet samples = new SampleSet(List.of(), List.of(), Estimates.ZERO);
        TabletReshardJob combinedJob = mock(TabletReshardJob.class);
        BooleanSupplierMarker shouldAbort = new BooleanSupplierMarker();

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

            PreSplitFlow.runMultiPartitionFlow(database, table, prepared,
                    LoadKind.INSERT_FROM_FILES, shouldAbort, mock(ConnectContext.class));

            coordinator.verify(() -> TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                    eq(LoadKind.INSERT_FROM_FILES), eq(table), eq(combinedJob), eq(shouldAbort)), times(1));
        }
    }

    // ---------- runDataTierSampler ----------

    @Test
    public void runDataTierSamplerReturnsSampleSetOnSuccess() {
        // The data-tier sampler succeeds -> the SampleSet is returned unchanged.
        OlapTable table = mockTable(/*partitioned*/ true, /*automatic*/ true);
        PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));
        SampleSet expected = new SampleSet(List.of(), List.of(), Estimates.ZERO);

        try (MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class))).thenReturn(expected))) {
            SampleSet result = PreSplitFlow.runDataTierSampler(table, prepared, LoadKind.INSERT_FROM_FILES);
            Assertions.assertSame(expected, result, "successful sample must be returned unchanged");
        }
    }

    @Test
    public void runDataTierSamplerCapsQueryTimeout() {
        // The data-tier sample must be capped at the pre-submit budget. Capture the
        // SampleRequest and assert its query-timeout equals (int) the config seconds.
        long savedTimeout = Config.tablet_pre_split_pre_submit_timeout_seconds;
        Config.tablet_pre_split_pre_submit_timeout_seconds = 123L;
        try {
            OlapTable table = mockTable(/*partitioned*/ true, /*automatic*/ true);
            PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));
            SampleSet samples = new SampleSet(List.of(), List.of(), Estimates.ZERO);
            ArgumentCaptor<SampleRequest> captor = ArgumentCaptor.forClass(SampleRequest.class);

            try (MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                    (sampler, ctx) -> when(sampler.sample(captor.capture())).thenReturn(samples))) {
                PreSplitFlow.runDataTierSampler(table, prepared, LoadKind.INSERT_FROM_FILES);

                Assertions.assertEquals(123, captor.getValue().getQueryTimeoutSeconds(),
                        "data-tier sample must be capped at (int) tablet_pre_split_pre_submit_timeout_seconds");
            }
        } finally {
            Config.tablet_pre_split_pre_submit_timeout_seconds = savedTimeout;
        }
    }

    @Test
    public void runDataTierSamplerReturnsNullOnStarRocksException() {
        // StarRocksException from the sampler -> caught, null returned.
        OlapTable table = mockTable(/*partitioned*/ true, /*automatic*/ true);
        PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));

        try (MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class)))
                        .thenThrow(new StarRocksException("synthetic checked sample failure")))) {
            Assertions.assertNull(
                    PreSplitFlow.runDataTierSampler(table, prepared, LoadKind.INSERT_FROM_FILES),
                    "StarRocksException sampler failure must yield null");
        }
    }

    @Test
    public void runDataTierSamplerReturnsNullOnRuntimeException() {
        // RuntimeException from the sampler -> caught by the second catch arm, null returned.
        OlapTable table = mockTable(/*partitioned*/ true, /*automatic*/ true);
        PreSplitFlow.Prepared prepared = preparedFor(mock(ScanContext.class));

        try (MockedConstruction<ReservoirSampler> ignored = Mockito.mockConstruction(ReservoirSampler.class,
                (sampler, ctx) -> when(sampler.sample(any(SampleRequest.class)))
                        .thenThrow(new RuntimeException("synthetic runtime sample failure")))) {
            Assertions.assertNull(
                    PreSplitFlow.runDataTierSampler(table, prepared, LoadKind.INSERT_FROM_FILES),
                    "RuntimeException sampler failure must yield null");
        }
    }

    // ---------- helpers ----------

    /**
     * Mock an {@link OlapTable} with the partitioned + automatic-partition shape the
     * dispatch gate inspects. The sort key / partition columns the flow uses come from
     * the {@link PreSplitFlow.Prepared} bundle, not the table, so they are not stubbed here.
     */
    private static OlapTable mockTable(boolean partitioned, boolean automatic) {
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("presplit_flow_t");
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(partitioned);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);
        when(table.supportedAutomaticPartition()).thenReturn(automatic);
        return table;
    }

    private static PreSplitFlow.Prepared preparedFor(ScanContext scanContext) {
        List<Column> sortKey = List.of(bigintColumn("sort_col"));
        return new PreSplitFlow.Prepared(scanContext, sortKey, List.of(),
                /*estimatedBytes*/ 0L, mock(ComputeResource.class));
    }

    /** Stub PreSplitTargets.findEligibleTarget to resolve a single-partition target for {@code table}. */
    private static void stubEligibleTarget(
            MockedStatic<PreSplitTargets> targets, Database database, OlapTable table) {
        targets.when(() -> PreSplitTargets.findEligibleTarget(database, table))
                .thenReturn(new PreSplitTargets.EligibleTarget(
                        database, table, /*partitionId*/ 11L, /*oldTabletId*/ 22L));
    }

    /** Stub DefaultPreSplitPipeline.forLoadKind to return a mock pipeline, returning it for verification. */
    private static DefaultPreSplitPipeline stubPipelineFactory(MockedStatic<DefaultPreSplitPipeline> pipelineStatic) {
        DefaultPreSplitPipeline pipeline = mock(DefaultPreSplitPipeline.class);
        pipelineStatic.when(() -> DefaultPreSplitPipeline.forLoadKind(
                        any(), any(), anyLong(), anyLong(), any()))
                .thenReturn(pipeline);
        return pipeline;
    }

    /**
     * Identity-stable {@link java.util.function.BooleanSupplier} so a test can assert the
     * exact same supplier reference is threaded into the await helper via {@code eq(...)}.
     */
    private static final class BooleanSupplierMarker implements java.util.function.BooleanSupplier {
        @Override
        public boolean getAsBoolean() {
            return false;
        }
    }
}
