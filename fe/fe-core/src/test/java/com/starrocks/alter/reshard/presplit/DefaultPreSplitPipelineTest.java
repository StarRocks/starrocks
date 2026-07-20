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
import com.starrocks.alter.reshard.SplitTabletJob;
import com.starrocks.alter.reshard.SplitTabletJobFactory;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.DUMMY_CONTEXT;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintTuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultPreSplitPipelineTest {

    private static final long OLD_TABLET_ID = 9000L;
    private static final long ROLLUP_TABLET_ID = 9100L;
    private static final long BASE_INDEX_META_ID = 1L;
    private static final long ROLLUP_INDEX_META_ID = 2L;
    private static final List<Column> BASE_SORT_KEY = List.of(bigintColumn("ts"));
    private static final List<Column> ROLLUP_SORT_KEY = List.of(bigintColumn("ts2"));
    private static final long FILE_TOTAL_BYTES = 1024L;
    private static final int ACTIVE_COMPUTE_NODES = 3;
    private static final Duration PRE_SUBMIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POST_SUBMIT_TIMEOUT = Duration.ofSeconds(300);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(1);

    private long savedConfigTargetSize;
    private int savedConfigMaxSplitCount;
    private Database database;
    private OlapTable table;
    private TabletReshardJobMgr tabletReshardJobManager;
    private SampleRequest sampleRequest;

    @BeforeEach
    public void setUp() {
        savedConfigTargetSize = Config.tablet_reshard_target_size;
        savedConfigMaxSplitCount = Config.tablet_reshard_max_split_count;
        Config.tablet_reshard_target_size = 10L * DebugUtil.GIGABYTE;
        Config.tablet_reshard_max_split_count = 1024;

        database = mock(Database.class);
        when(database.getId()).thenReturn(500L);
        table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t");
        when(table.getId()).thenReturn(600L);
        tabletReshardJobManager = mock(TabletReshardJobMgr.class);

        sampleRequest = new SampleRequest(DUMMY_CONTEXT, BASE_SORT_KEY, 16L * DebugUtil.MEGABYTE, 0L);
        // Default single-index shape (matches singleBaseTarget()); tests covering a base+rollup
        // target list restub this via stubVisibleIndexMetas.
        stubVisibleIndexMetas(BASE_INDEX_META_ID);
    }

    @AfterEach
    public void tearDown() {
        Config.tablet_reshard_target_size = savedConfigTargetSize;
        Config.tablet_reshard_max_split_count = savedConfigMaxSplitCount;
    }

    private DefaultPreSplitPipeline newPipeline(MetaTierSampler metaTierSampler, Sampler dataTierSampler, Clock clock) {
        return newPipeline(singleBaseTarget(), metaTierSampler, dataTierSampler, clock);
    }

    private DefaultPreSplitPipeline newPipeline(List<IndexPreSplitTarget> indexTargets,
            MetaTierSampler metaTierSampler, Sampler dataTierSampler, Clock clock) {
        return new DefaultPreSplitPipeline(
                metaTierSampler, dataTierSampler, tabletReshardJobManager,
                database, table, indexTargets, FILE_TOTAL_BYTES,
                POLL_INTERVAL, clock, null);
    }

    private static List<IndexPreSplitTarget> singleBaseTarget() {
        return List.of(new IndexPreSplitTarget(BASE_INDEX_META_ID, OLD_TABLET_ID, BASE_SORT_KEY));
    }

    private static List<IndexPreSplitTarget> baseAndRollupTargets() {
        return List.of(
                new IndexPreSplitTarget(BASE_INDEX_META_ID, OLD_TABLET_ID, BASE_SORT_KEY),
                new IndexPreSplitTarget(ROLLUP_INDEX_META_ID, ROLLUP_TABLET_ID, ROLLUP_SORT_KEY));
    }

    /** Stubs {@code table.getVisibleIndexMetas()} to the given index-meta ids, base first. */
    private void stubVisibleIndexMetas(long... indexMetaIds) {
        List<MaterializedIndexMeta> metas = new ArrayList<>();
        for (long indexMetaId : indexMetaIds) {
            MaterializedIndexMeta meta = mock(MaterializedIndexMeta.class);
            when(meta.getIndexMetaId()).thenReturn(indexMetaId);
            metas.add(meta);
        }
        when(table.getVisibleIndexMetas()).thenReturn(metas);
    }

    @Test
    public void testMetaTierReturnsBoundariesProducesPreparedJobAndSkipsDataTier() throws Exception {
        BoundaryPlannerResult metaTierResult = new BoundaryPlannerResult(List.of(bigintTuple(50)));
        AtomicReference<SampleRequest> dataTierCallCapture = new AtomicReference<>();
        MetaTierSampler metaTier = (request, requestedTabletCount) -> metaTierResult;
        Sampler dataTier = request -> {
            dataTierCallCapture.set(request);
            throw new StarRocksException("dataTier should not be called");
        };

        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(eq(database), eq(table), any()))
                    .thenReturn(fakeJob);

            DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isPresent());
            Assertions.assertSame(fakeJob, prepared.get().payload());
            Assertions.assertNull(dataTierCallCapture.get(), "data tier must not be invoked when meta tier succeeds");
        }
    }

    @Test
    public void testMetaTierUnavailableFallsBackToDataTier() throws Exception {
        MetaTierSampler metaTier = (request, requestedTabletCount) -> {
            throw new MetaTierUnavailableException("test: row-group statistics overlap");
        };
        Sampler dataTier = request -> new SampleSet(
                List.of(bigintTuple(10), bigintTuple(20), bigintTuple(30), bigintTuple(40)),
                new Estimates(FILE_TOTAL_BYTES, 4L));

        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()))
                    .thenReturn(fakeJob);

            DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isPresent());
        }
    }

    @Test
    public void testDataTierSampleBoundedByRemainingBudget() throws Exception {
        // "Make the soft deadline hard": the data tier must hand its sampler a
        // SampleRequest whose query_timeout equals the remaining pre-submit
        // budget, so an over-budget BE sample is cancelled rather than running
        // the deadline over by a full sample phase. A fixed clock makes the
        // remaining budget exactly PRE_SUBMIT_TIMEOUT.
        MetaTierSampler metaTier = (request, requestedTabletCount) -> {
            throw new MetaTierUnavailableException("test: force data-tier fallback");
        };
        AtomicReference<SampleRequest> dataTierCallCapture = new AtomicReference<>();
        Sampler dataTier = request -> {
            dataTierCallCapture.set(request);
            return new SampleSet(
                    List.of(bigintTuple(10), bigintTuple(20), bigintTuple(30), bigintTuple(40)),
                    new Estimates(FILE_TOTAL_BYTES, 4L));
        };
        Clock fixedClock = Clock.fixed(Instant.ofEpochSecond(1_000_000L), ZoneId.of("UTC"));

        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()))
                    .thenReturn(fakeJob);

            DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, fixedClock);
            pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            SampleRequest captured = dataTierCallCapture.get();
            Assertions.assertNotNull(captured, "data tier must be invoked on meta-tier fallback");
            Assertions.assertEquals((int) PRE_SUBMIT_TIMEOUT.toSeconds(), captured.getQueryTimeoutSeconds(),
                    "data-tier sample must be capped at the remaining pre-submit budget");
            // withQueryTimeoutSeconds is a pure copy — every other field carries through.
            Assertions.assertEquals(sampleRequest.getSampleByteLimit(), captured.getSampleByteLimit());
            Assertions.assertEquals(sampleRequest.getSeed(), captured.getSeed());
            Assertions.assertEquals(sampleRequest.getSortKey(), captured.getSortKey());
            // The original request the coordinator built is left uncapped (0).
            Assertions.assertEquals(0, sampleRequest.getQueryTimeoutSeconds());
        }
    }

    @Test
    public void testMetaTierNoSplitShortCircuitsBeforeFactory() throws Exception {
        MetaTierSampler metaTier = (request, requestedTabletCount) -> BoundaryPlannerResult.NO_SPLIT;
        Sampler dataTier = request -> {
            throw new AssertionError("data tier must not be invoked when meta tier returns NO_SPLIT");
        };

        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isEmpty(), "NO_SPLIT must short-circuit to Optional.empty");
            mocked.verifyNoInteractions();
        }
    }

    @Test
    public void testDataTierNoSplitShortCircuitsBeforeFactory() throws Exception {
        MetaTierSampler metaTier = (request, requestedTabletCount) -> {
            throw new MetaTierUnavailableException("test: composite sort key");
        };
        // Empty sample set produces NO_SPLIT in BoundaryPlanner.planRowQuantileBoundaries.
        Sampler dataTier = request -> SampleSet.EMPTY;

        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isEmpty());
            mocked.verifyNoInteractions();
        }
    }

    @Test
    public void testDataTierFailurePropagatesAsSampleFailed() {
        MetaTierSampler metaTier = (request, requestedTabletCount) -> {
            throw new MetaTierUnavailableException("test: stats missing");
        };
        Sampler dataTier = request -> {
            throw new StarRocksException("simulated executor RPC failure");
        };

        DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, Clock.systemUTC());
        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT));
        Assertions.assertTrue(thrown.getMessage().contains("simulated executor RPC failure"));
    }

    @Test
    public void testPreSubmitDeadlineElapsedBetweenTiersTriggersTimeout() {
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-01-01T00:00:00Z"));
        MetaTierSampler metaTier = (request, requestedTabletCount) -> {
            clock.advance(Duration.ofSeconds(31));
            throw new MetaTierUnavailableException("test: stale stats");
        };
        Sampler dataTier = request -> {
            throw new AssertionError("data tier must not run after the deadline elapsed");
        };

        DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, clock);
        Assertions.assertThrows(PreSplitPreSubmitTimeoutException.class,
                () -> pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, Duration.ofSeconds(30)));
    }

    @Test
    public void testSubmitDelegatesToReshardJobMgr() throws Exception {
        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        PreSplitPipeline.PreparedReshardJob prepared = new PreSplitPipeline.PreparedReshardJob(fakeJob);

        DefaultPreSplitPipeline pipeline = newPipeline(
                (r, k) -> BoundaryPlannerResult.NO_SPLIT, r -> SampleSet.EMPTY, Clock.systemUTC());
        pipeline.submit(prepared);

        verify(tabletReshardJobManager).addTabletReshardJob(fakeJob);
    }

    @Test
    public void testAwaitFinishedReturnsOnTerminalFinished() throws Exception {
        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        when(fakeJob.getJobId()).thenReturn(42L);
        when(fakeJob.getJobState()).thenReturn(TabletReshardJob.JobState.FINISHED);
        when(tabletReshardJobManager.getTabletReshardJob(42L)).thenReturn(fakeJob);

        DefaultPreSplitPipeline pipeline = newPipeline(
                (r, k) -> BoundaryPlannerResult.NO_SPLIT, r -> SampleSet.EMPTY, Clock.systemUTC());
        pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(fakeJob), POST_SUBMIT_TIMEOUT, () -> false);
    }

    @Test
    public void testAwaitFinishedThrowsOnTerminalAborted() {
        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        when(fakeJob.getJobId()).thenReturn(99L);
        when(fakeJob.getJobState()).thenReturn(TabletReshardJob.JobState.ABORTED);
        when(fakeJob.getErrorMessage()).thenReturn("simulated shard creation error");
        when(tabletReshardJobManager.getTabletReshardJob(99L)).thenReturn(fakeJob);

        DefaultPreSplitPipeline pipeline = newPipeline(
                (r, k) -> BoundaryPlannerResult.NO_SPLIT, r -> SampleSet.EMPTY, Clock.systemUTC());
        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(fakeJob), POST_SUBMIT_TIMEOUT, () -> false));
        Assertions.assertTrue(thrown.getMessage().contains("99"));
        Assertions.assertTrue(thrown.getMessage().contains("simulated shard creation error"));
    }

    @Test
    public void testAwaitFinishedThrowsPostSubmitTimeoutWhenDeadlineElapses() {
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-01-01T00:00:00Z"));
        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        when(fakeJob.getJobId()).thenReturn(7L);
        // Pin in a non-terminal state forever; the clock advances every poll iteration.
        when(fakeJob.getJobState()).thenAnswer(invocation -> {
            clock.advance(Duration.ofSeconds(60));
            return TabletReshardJob.JobState.RUNNING;
        });
        when(tabletReshardJobManager.getTabletReshardJob(7L)).thenReturn(fakeJob);

        DefaultPreSplitPipeline pipeline = newPipeline(
                (r, k) -> BoundaryPlannerResult.NO_SPLIT, r -> SampleSet.EMPTY, clock);
        Assertions.assertThrows(PreSplitPostSubmitTimeoutException.class,
                () -> pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(fakeJob),
                        Duration.ofSeconds(120), () -> false));
    }

    @Test
    public void testAwaitFinishedAbortsWhenShouldAbortReturnsTrue() {
        // Cancel-aware path: a supplier that returns true short-circuits the
        // helper before it polls the reshard manager. The helper throws
        // StarRocksException with an "await abandoned" message so the caller
        // routes through the fail-safe wrapper.
        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        when(fakeJob.getJobId()).thenReturn(55L);

        DefaultPreSplitPipeline pipeline = newPipeline(
                (r, k) -> BoundaryPlannerResult.NO_SPLIT, r -> SampleSet.EMPTY, Clock.systemUTC());
        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(fakeJob),
                        POST_SUBMIT_TIMEOUT, () -> true));
        Assertions.assertTrue(thrown.getMessage().contains("await abandoned"),
                "shouldAbort short-circuit must signal the cancellation reason");
        // shouldAbort fires before any reshard-manager poll.
        Mockito.verifyNoInteractions(tabletReshardJobManager);
    }

    @Test
    public void testAwaitFinishedThrowsWhenJobDisappears() {
        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        when(fakeJob.getJobId()).thenReturn(11L);
        when(tabletReshardJobManager.getTabletReshardJob(11L)).thenReturn(null);

        DefaultPreSplitPipeline pipeline = newPipeline(
                (r, k) -> BoundaryPlannerResult.NO_SPLIT, r -> SampleSet.EMPTY, Clock.systemUTC());
        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(fakeJob), POST_SUBMIT_TIMEOUT, () -> false));
        Assertions.assertTrue(thrown.getMessage().contains("disappeared"));
    }

    @Test
    public void testBuildTabletRangesProducesCountPlusOneRanges() {
        List<TabletRange> ranges = DefaultPreSplitPipeline.buildTabletRanges(
                List.of(bigintTuple(10), bigintTuple(20), bigintTuple(30)));
        Assertions.assertEquals(4, ranges.size());

        TabletRange first = ranges.get(0);
        Assertions.assertTrue(first.getRange().isMinimum(), "first range must open to -infinity");
        Assertions.assertFalse(first.getRange().isUpperBoundIncluded(), "upper bound must be exclusive");

        TabletRange middle = ranges.get(1);
        Assertions.assertTrue(middle.getRange().isLowerBoundIncluded(), "interior lower bound must be inclusive");
        Assertions.assertFalse(middle.getRange().isUpperBoundIncluded(), "interior upper bound must be exclusive");
        Assertions.assertEquals(bigintTuple(10), middle.getRange().getLowerBound());
        Assertions.assertEquals(bigintTuple(20), middle.getRange().getUpperBound());

        TabletRange last = ranges.get(3);
        Assertions.assertTrue(last.getRange().isMaximum(), "last range must close at +infinity");
        Assertions.assertTrue(last.getRange().isLowerBoundIncluded(), "lower bound must be inclusive");
    }

    @Test
    public void testBuildTabletRangesWithSingleBoundaryProducesTwoRanges() {
        List<TabletRange> ranges = DefaultPreSplitPipeline.buildTabletRanges(List.of(bigintTuple(100)));
        Assertions.assertEquals(2, ranges.size());
        Assertions.assertTrue(ranges.get(0).getRange().isMinimum());
        Assertions.assertTrue(ranges.get(1).getRange().isMaximum());
    }

    // ---------- preSubmit: unified per-index plan ----------

    @Test
    public void preSubmit_singleIndex_buildsSingleEntryMapJob() throws Exception {
        // A single-index target list (base only) builds one job from a single-entry
        // {oldTabletId -> ranges} map.
        BoundaryPlannerResult metaTierResult = new BoundaryPlannerResult(List.of(bigintTuple(50)));
        MetaTierSampler metaTier = (request, requestedTabletCount) -> metaTierResult;
        Sampler dataTier = request -> {
            throw new StarRocksException("dataTier should not be called");
        };

        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = ArgumentCaptor.forClass(Map.class);
            mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                            eq(database), eq(table), mapCaptor.capture()))
                    .thenReturn(fakeJob);

            DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isPresent());
            Assertions.assertSame(fakeJob, prepared.get().payload());
            Map<Long, List<TabletRange>> map = mapCaptor.getValue();
            Assertions.assertEquals(1, map.size());
            Assertions.assertTrue(map.containsKey(OLD_TABLET_ID));
        }
    }

    @Test
    public void preSubmit_baseAndRollup_splitsBoth() throws Exception {
        // Both the base and a rollup produce cuts -> the {oldTabletId -> ranges} map has two entries.
        stubVisibleIndexMetas(BASE_INDEX_META_ID, ROLLUP_INDEX_META_ID);
        BoundaryPlannerResult metaTierResult = new BoundaryPlannerResult(List.of(bigintTuple(50)));
        MetaTierSampler metaTier = (request, requestedTabletCount) -> metaTierResult;
        Sampler dataTier = request -> {
            throw new StarRocksException("dataTier should not be called");
        };

        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = ArgumentCaptor.forClass(Map.class);
            mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                            eq(database), eq(table), mapCaptor.capture()))
                    .thenReturn(fakeJob);

            DefaultPreSplitPipeline pipeline = newPipeline(baseAndRollupTargets(), metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isPresent());
            Assertions.assertSame(fakeJob, prepared.get().payload());
            Map<Long, List<TabletRange>> map = mapCaptor.getValue();
            Assertions.assertEquals(2, map.size());
            Assertions.assertTrue(map.containsKey(OLD_TABLET_ID));
            Assertions.assertTrue(map.containsKey(ROLLUP_TABLET_ID));
        }
    }

    @Test
    public void preSubmit_rollupNoCuts_baseOnly() throws Exception {
        // The rollup's own sort key yields NO_SPLIT while the base still produces cuts -- the
        // map ends up with only the base entry (a base-only submit is allowed).
        stubVisibleIndexMetas(BASE_INDEX_META_ID, ROLLUP_INDEX_META_ID);
        BoundaryPlannerResult metaTierResult = new BoundaryPlannerResult(List.of(bigintTuple(50)));
        MetaTierSampler metaTier = (request, requestedTabletCount) ->
                request.getSortKey().equals(BASE_SORT_KEY) ? metaTierResult : BoundaryPlannerResult.NO_SPLIT;
        Sampler dataTier = request -> {
            throw new StarRocksException("dataTier should not be called");
        };

        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = ArgumentCaptor.forClass(Map.class);
            mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                            eq(database), eq(table), mapCaptor.capture()))
                    .thenReturn(fakeJob);

            DefaultPreSplitPipeline pipeline = newPipeline(baseAndRollupTargets(), metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isPresent(), "base-only cuts must still submit (allowed base-only)");
            Assertions.assertSame(fakeJob, prepared.get().payload());
            Map<Long, List<TabletRange>> map = mapCaptor.getValue();
            Assertions.assertEquals(1, map.size());
            Assertions.assertTrue(map.containsKey(OLD_TABLET_ID));
        }
    }

    @Test
    public void preSubmit_baseMetaTierRollupDataTier_bothPlanned() throws Exception {
        // Per-index tier independence: the base's sort key reaches meta tier, the rollup's own
        // sort key is meta-tier-unavailable and falls back to data tier -- both still contribute
        // cuts to the combined map.
        stubVisibleIndexMetas(BASE_INDEX_META_ID, ROLLUP_INDEX_META_ID);
        BoundaryPlannerResult metaTierResult = new BoundaryPlannerResult(List.of(bigintTuple(50)));
        MetaTierSampler metaTier = (request, requestedTabletCount) -> {
            if (request.getSortKey().equals(BASE_SORT_KEY)) {
                return metaTierResult;
            }
            throw new MetaTierUnavailableException("test: rollup meta tier unavailable");
        };
        Sampler dataTier = request -> new SampleSet(
                List.of(bigintTuple(10), bigintTuple(20), bigintTuple(30), bigintTuple(40)),
                new Estimates(FILE_TOTAL_BYTES, 4L));

        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Map<Long, List<TabletRange>>> mapCaptor = ArgumentCaptor.forClass(Map.class);
            mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                            eq(database), eq(table), mapCaptor.capture()))
                    .thenReturn(fakeJob);

            DefaultPreSplitPipeline pipeline = newPipeline(baseAndRollupTargets(), metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isPresent());
            Map<Long, List<TabletRange>> map = mapCaptor.getValue();
            Assertions.assertEquals(2, map.size(),
                    "per-index tier independence: both base (meta) and rollup (data) must contribute cuts");
            Assertions.assertTrue(map.containsKey(OLD_TABLET_ID));
            Assertions.assertTrue(map.containsKey(ROLLUP_TABLET_ID));
        }
    }

    @Test
    public void preSubmit_visibleIndexAppearedAfterSnapshot_skips() throws Exception {
        // The eligibility snapshot only captured the base index, but a rollup has since become
        // visible on the live table -- the final id-set re-check must detect the mismatch and
        // skip entirely (never a base-only partial submit).
        stubVisibleIndexMetas(BASE_INDEX_META_ID, ROLLUP_INDEX_META_ID);
        BoundaryPlannerResult metaTierResult = new BoundaryPlannerResult(List.of(bigintTuple(50)));
        MetaTierSampler metaTier = (request, requestedTabletCount) -> metaTierResult;
        Sampler dataTier = request -> {
            throw new StarRocksException("dataTier should not be called");
        };

        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isEmpty(), "a visible-index-set mismatch must skip, never a base-only submit");
            mocked.verifyNoInteractions();
        }
    }

    @Test
    public void preSubmit_noIndexCuts_returnsEmpty() throws Exception {
        MetaTierSampler metaTier = (request, requestedTabletCount) -> BoundaryPlannerResult.NO_SPLIT;
        Sampler dataTier = request -> {
            throw new AssertionError("data tier must not be invoked when meta tier returns NO_SPLIT");
        };

        try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
            DefaultPreSplitPipeline pipeline = newPipeline(baseAndRollupTargets(), metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isEmpty());
            mocked.verifyNoInteractions();
        }
    }

    @Test
    public void preSubmit_recordsTierForFirstContributingIndexOnly() throws Exception {
        // Base yields 1 boundary, rollup yields 2 -- if tier/boundary recording ran per-index
        // instead of once for the first contributing index, the tier counter would bump by 2 and
        // the histogram would also record the rollup's 2-boundary sample.
        stubVisibleIndexMetas(BASE_INDEX_META_ID, ROLLUP_INDEX_META_ID);
        BoundaryPlannerResult baseResult = new BoundaryPlannerResult(List.of(bigintTuple(50)));
        BoundaryPlannerResult rollupResult = new BoundaryPlannerResult(List.of(bigintTuple(10), bigintTuple(20)));
        MetaTierSampler metaTier = (request, requestedTabletCount) ->
                request.getSortKey().equals(BASE_SORT_KEY) ? baseResult : rollupResult;
        Sampler dataTier = request -> {
            throw new StarRocksException("dataTier should not be called");
        };

        boolean savedHasInit = MetricRepo.hasInit;
        Histogram savedHistogram = MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED;
        LongCounterMetric savedSamplerInvocations = MetricRepo.COUNTER_TABLET_PRE_SPLIT_SAMPLER_INVOCATIONS;
        MetricRepo.hasInit = true;
        MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED =
                new MetricRegistry().histogram("presubmit_records_tier_for_first_contributing_index_test");
        // recordSamplerInvocation() (called unconditionally at the top of preSubmit) needs this
        // non-null once hasInit is forced true; MetricRepo.init() is never run in this bare test.
        MetricRepo.COUNTER_TABLET_PRE_SPLIT_SAMPLER_INVOCATIONS =
                new LongCounterMetric("presubmit_records_tier_for_first_contributing_index_test_invocations",
                        MetricUnit.REQUESTS, "test-local sampler invocations");
        try {
            String tierLabel = DefaultPreSplitPipeline.TIER_LABEL_META_TIER;
            long baselineTierCount = MetricRepo.COUNTER_TABLET_PRE_SPLIT_TIER_USED.getMetric(tierLabel).getValue();

            TabletReshardJob fakeJob = mock(TabletReshardJob.class);
            try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
                mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), any()))
                        .thenReturn(fakeJob);

                DefaultPreSplitPipeline pipeline =
                        newPipeline(baseAndRollupTargets(), metaTier, dataTier, Clock.systemUTC());
                pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);
            }

            Assertions.assertEquals(baselineTierCount + 1,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_TIER_USED.getMetric(tierLabel).getValue().longValue(),
                    "tier_used must be recorded exactly once (first contributing index), not once per index");
            Assertions.assertEquals(1, MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED.getCount(),
                    "boundaries_planned must be recorded exactly once (first contributing index)");
            Assertions.assertEquals(1, MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED.getSnapshot().getMax(),
                    "the recorded boundary count must be the base's (1), not the rollup's (2)");
        } finally {
            MetricRepo.hasInit = savedHasInit;
            MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED = savedHistogram;
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_SAMPLER_INVOCATIONS = savedSamplerInvocations;
        }
    }

    @Test
    public void preSubmit_baseNoCutsRollupCuts_recordsTierForRollup() throws Exception {
        // The base's own sort key yields NO_SPLIT while the rollup still produces cuts -- a
        // real (rollup-only) job is submitted, and the rollup must be the one recorded as the
        // first contributing index (not zero recordings, since it is not literally index 0).
        stubVisibleIndexMetas(BASE_INDEX_META_ID, ROLLUP_INDEX_META_ID);
        BoundaryPlannerResult rollupResult = new BoundaryPlannerResult(List.of(bigintTuple(10), bigintTuple(20)));
        MetaTierSampler metaTier = (request, requestedTabletCount) ->
                request.getSortKey().equals(BASE_SORT_KEY) ? BoundaryPlannerResult.NO_SPLIT : rollupResult;
        Sampler dataTier = request -> {
            throw new StarRocksException("dataTier should not be called");
        };

        boolean savedHasInit = MetricRepo.hasInit;
        Histogram savedHistogram = MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED;
        LongCounterMetric savedSamplerInvocations = MetricRepo.COUNTER_TABLET_PRE_SPLIT_SAMPLER_INVOCATIONS;
        MetricRepo.hasInit = true;
        MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED =
                new MetricRegistry().histogram("presubmit_base_no_cuts_rollup_cuts_test");
        MetricRepo.COUNTER_TABLET_PRE_SPLIT_SAMPLER_INVOCATIONS =
                new LongCounterMetric("presubmit_base_no_cuts_rollup_cuts_test_invocations",
                        MetricUnit.REQUESTS, "test-local sampler invocations");
        try {
            String tierLabel = DefaultPreSplitPipeline.TIER_LABEL_META_TIER;
            long baselineTierCount = MetricRepo.COUNTER_TABLET_PRE_SPLIT_TIER_USED.getMetric(tierLabel).getValue();

            TabletReshardJob fakeJob = mock(TabletReshardJob.class);
            try (MockedStatic<SplitTabletJobFactory> mocked = Mockito.mockStatic(SplitTabletJobFactory.class)) {
                mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(
                                eq(database), eq(table), any()))
                        .thenReturn(fakeJob);

                DefaultPreSplitPipeline pipeline =
                        newPipeline(baseAndRollupTargets(), metaTier, dataTier, Clock.systemUTC());
                Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                        pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

                Assertions.assertTrue(prepared.isPresent(),
                        "rollup-only cuts must still submit (allowed base-no-cut/rollup-cut)");
            }

            Assertions.assertEquals(baselineTierCount + 1,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_TIER_USED.getMetric(tierLabel).getValue().longValue(),
                    "tier_used must be recorded once for the rollup, the first (and only) contributing index");
            Assertions.assertEquals(1, MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED.getCount(),
                    "boundaries_planned must be recorded once for the rollup, not skipped");
            Assertions.assertEquals(2, MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED.getSnapshot().getMax(),
                    "the recorded boundary count must be the rollup's (2)");
        } finally {
            MetricRepo.hasInit = savedHasInit;
            MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED = savedHistogram;
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_SAMPLER_INVOCATIONS = savedSamplerInvocations;
        }
    }

    @Test
    public void forLoadKindInsertFromTableSinglePartitionForcesDataTier() {
        // Unpartitioned OlapTable; forLoadKind with INSERT_FROM_TABLE must install a
        // meta-tier stub that throws MetaTierUnavailableException — the OLAP source
        // has no Parquet/ORC footer so the meta tier is never usable for this load kind.
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isPartitioned()).thenReturn(false);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);

        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                database, table, singleBaseTarget(), FILE_TOTAL_BYTES, LoadKind.INSERT_FROM_TABLE, null);

        Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> pipeline.getMetaTierSamplerForTest().tryPlan(sampleRequest, 3),
                "INSERT_FROM_TABLE must force data tier via MetaTierUnavailableException");
    }

    @Test
    public void sampleSubqueryExecutorForInsertFromTableIsTableExecutor() {
        SampleSubqueryExecutor executor = DefaultPreSplitPipeline.sampleSubqueryExecutorFor(LoadKind.INSERT_FROM_TABLE);
        Assertions.assertInstanceOf(InsertFromTableSampleSubqueryExecutor.class, executor,
                "INSERT_FROM_TABLE load kind must produce an InsertFromTableSampleSubqueryExecutor");
    }

    // ---------- awaitFinished: wait for pre-split shards to spread across CNs before load (PR #76562) ----------

    private DefaultPreSplitPipeline newPipelineWithResource(Clock clock, ComputeResource loadComputeResource) {
        return new DefaultPreSplitPipeline(
                (r, k) -> BoundaryPlannerResult.NO_SPLIT, r -> SampleSet.EMPTY, tabletReshardJobManager,
                database, table, singleBaseTarget(), FILE_TOTAL_BYTES,
                POLL_INTERVAL, clock, loadComputeResource);
    }

    /** A FINISHED SplitTabletJob mock that reports the given freshly created tablet ids. */
    private SplitTabletJob finishedSplitJob(long jobId, List<Long> newTabletIds) {
        SplitTabletJob split = mock(SplitTabletJob.class);
        when(split.getJobId()).thenReturn(jobId);
        when(split.getJobState()).thenReturn(TabletReshardJob.JobState.FINISHED);
        when(split.getAllNewTabletIds()).thenReturn(newTabletIds);
        when(tabletReshardJobManager.getTabletReshardJob(jobId)).thenReturn(split);
        return split;
    }

    private static List<ComputeNode> aliveNodes(int count) {
        List<ComputeNode> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(mock(ComputeNode.class));
        }
        return nodes;
    }

    /** Every new tablet resolves to the same node id: the un-spread, single-CN state right after creation. */
    private static Map<Long, List<Long>> allOnOneNode(List<Long> tabletIds, long nodeId) {
        Map<Long, List<Long>> placement = new HashMap<>();
        for (Long tabletId : tabletIds) {
            placement.put(tabletId, List.of(nodeId));
        }
        return placement;
    }

    /** Each new tablet resolves to a distinct node id: fully spread across CNs. */
    private static Map<Long, List<Long>> spreadAcrossNodes(List<Long> tabletIds) {
        Map<Long, List<Long>> placement = new HashMap<>();
        long nodeId = 100L;
        for (Long tabletId : tabletIds) {
            placement.put(tabletId, List.of(nodeId++));
        }
        return placement;
    }

    @Test
    public void testAwaitShardSpreadReturnsWhenShardsAlreadySpread() throws Exception {
        List<Long> newTabletIds = List.of(1001L, 1002L, 1003L);
        SplitTabletJob split = finishedSplitJob(70L, newTabletIds);
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);
        when(warehouseManager.getAliveComputeNodes(resource)).thenReturn(aliveNodes(3));
        when(warehouseManager.getAllComputeNodeIdsAssignToTablets(eq(resource), any()))
                .thenReturn(spreadAcrossNodes(newTabletIds));

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 60L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(Clock.systemUTC(), resource);
            // FINISHED -> awaitShardSpread finds the 3 shards already on 3 distinct nodes and returns at once.
            Assertions.assertDoesNotThrow(() -> pipeline.awaitFinished(
                    new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false));
            verify(warehouseManager).getAllComputeNodeIdsAssignToTablets(eq(resource), any());
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadWaitsUntilShardsConverge() throws Exception {
        List<Long> newTabletIds = List.of(2001L, 2002L, 2003L);
        SplitTabletJob split = finishedSplitJob(71L, newTabletIds);
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);
        when(warehouseManager.getAliveComputeNodes(resource)).thenReturn(aliveNodes(3));
        AtomicInteger polls = new AtomicInteger();
        // First two polls: shards still on one node; third poll: spread across three.
        when(warehouseManager.getAllComputeNodeIdsAssignToTablets(eq(resource), any()))
                .thenAnswer(invocation -> polls.incrementAndGet() < 3
                        ? allOnOneNode(newTabletIds, 100L)
                        : spreadAcrossNodes(newTabletIds));

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 60L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(Clock.systemUTC(), resource);
            Assertions.assertDoesNotThrow(() -> pipeline.awaitFinished(
                    new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false));
            Assertions.assertTrue(polls.get() >= 3,
                    "the wait must keep polling placement until the shards spread (>= 3 polls), was " + polls.get());
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadProceedsOnTimeoutWithoutThrowing() throws Exception {
        List<Long> newTabletIds = List.of(3001L, 3002L, 3003L);
        SplitTabletJob split = finishedSplitJob(72L, newTabletIds);
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);
        when(warehouseManager.getAliveComputeNodes(resource)).thenReturn(aliveNodes(3));
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-01-01T00:00:00Z"));
        // Shards never spread; each poll pushes the clock past the 5s spread deadline.
        when(warehouseManager.getAllComputeNodeIdsAssignToTablets(eq(resource), any()))
                .thenAnswer(invocation -> {
                    clock.advance(Duration.ofSeconds(6));
                    return allOnOneNode(newTabletIds, 100L);
                });

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 5L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(clock, resource);
            // The bounded wait expires; the load proceeds anyway (still correct, just less parallel) — no throw.
            Assertions.assertDoesNotThrow(() -> pipeline.awaitFinished(
                    new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false));
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadDisabledByConfigSkipsPlacementLookup() throws Exception {
        SplitTabletJob split = finishedSplitJob(73L, List.of(4001L, 4002L, 4003L));
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 0L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(Clock.systemUTC(), resource);
            pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false);
            // 0 disables the wait: the load plans immediately without consulting shard placement.
            Mockito.verifyNoInteractions(warehouseManager);
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadSkippedWhenSingleNewTablet() throws Exception {
        SplitTabletJob split = finishedSplitJob(74L, List.of(5001L));
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 60L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(Clock.systemUTC(), resource);
            pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false);
            // A single new tablet cannot spread across nodes: nothing to wait for.
            Mockito.verifyNoInteractions(warehouseManager);
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadSkippedWhenComputeResourceUnknown() throws Exception {
        SplitTabletJob split = finishedSplitJob(75L, List.of(6001L, 6002L, 6003L));
        WarehouseManager warehouseManager = mock(WarehouseManager.class);

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 60L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            // Null load compute resource (the sink's target is unknown): the wait is skipped even when enabled.
            DefaultPreSplitPipeline pipeline = newPipelineWithResource(Clock.systemUTC(), null);
            pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false);
            Mockito.verifyNoInteractions(warehouseManager);
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadSkippedWhenSingleLiveNode() throws Exception {
        SplitTabletJob split = finishedSplitJob(76L, List.of(7001L, 7002L, 7003L));
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);
        when(warehouseManager.getAliveComputeNodes(resource)).thenReturn(aliveNodes(1)); // target = min(3, 1) = 1

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 60L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(Clock.systemUTC(), resource);
            pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false);
            // With a single live node there is nothing to spread across — no placement polling.
            verify(warehouseManager, never()).getAllComputeNodeIdsAssignToTablets(any(), any());
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadReturnsWhenLiveNodeLookupFails() throws Exception {
        SplitTabletJob split = finishedSplitJob(77L, List.of(7101L, 7102L, 7103L));
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);
        when(warehouseManager.getAliveComputeNodes(resource)).thenThrow(new RuntimeException("warehouse unavailable"));

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 60L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(Clock.systemUTC(), resource);
            // Resolving live nodes fails -> the wait is skipped (fail-safe), no placement polling, no throw.
            Assertions.assertDoesNotThrow(() -> pipeline.awaitFinished(
                    new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false));
            verify(warehouseManager, never()).getAllComputeNodeIdsAssignToTablets(any(), any());
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadTimesOutWhenPlacementLookupFails() throws Exception {
        List<Long> newTabletIds = List.of(7201L, 7202L, 7203L);
        SplitTabletJob split = finishedSplitJob(78L, newTabletIds);
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);
        when(warehouseManager.getAliveComputeNodes(resource)).thenReturn(aliveNodes(3));
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-01-01T00:00:00Z"));
        when(warehouseManager.getAllComputeNodeIdsAssignToTablets(eq(resource), any()))
                .thenAnswer(invocation -> {
                    clock.advance(Duration.ofSeconds(6));
                    throw new RuntimeException("placement lookup RPC failed");
                });

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 5L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(clock, resource);
            // The lookup fails every poll (treated as "not spread") -> the bounded wait expires -> proceeds, no throw.
            Assertions.assertDoesNotThrow(() -> pipeline.awaitFinished(
                    new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false));
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadTimesOutWhenPlacementNull() throws Exception {
        List<Long> newTabletIds = List.of(7301L, 7302L, 7303L);
        SplitTabletJob split = finishedSplitJob(79L, newTabletIds);
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);
        when(warehouseManager.getAliveComputeNodes(resource)).thenReturn(aliveNodes(3));
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-01-01T00:00:00Z"));
        when(warehouseManager.getAllComputeNodeIdsAssignToTablets(eq(resource), any()))
                .thenAnswer(invocation -> {
                    clock.advance(Duration.ofSeconds(6));
                    return null;
                });

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 5L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(clock, resource);
            // A null placement map is treated as "not spread"; the wait expires and proceeds, no throw.
            Assertions.assertDoesNotThrow(() -> pipeline.awaitFinished(
                    new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false));
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    @Test
    public void testAwaitShardSpreadTimesOutWhenTabletsUnassigned() throws Exception {
        List<Long> newTabletIds = List.of(7401L, 7402L, 7403L);
        SplitTabletJob split = finishedSplitJob(80L, newTabletIds);
        ComputeResource resource = mock(ComputeResource.class);
        WarehouseManager warehouseManager = mock(WarehouseManager.class);
        when(warehouseManager.getAliveComputeNodes(resource)).thenReturn(aliveNodes(3));
        AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-01-01T00:00:00Z"));
        // Every tablet is present in the map but has no node assigned yet (empty list) — not placed.
        Map<Long, List<Long>> unassigned = new HashMap<>();
        for (Long tabletId : newTabletIds) {
            unassigned.put(tabletId, List.of());
        }
        when(warehouseManager.getAllComputeNodeIdsAssignToTablets(eq(resource), any()))
                .thenAnswer(invocation -> {
                    clock.advance(Duration.ofSeconds(6));
                    return unassigned;
                });

        long savedSpread = Config.tablet_pre_split_await_shard_spread_seconds;
        Config.tablet_pre_split_await_shard_spread_seconds = 5L;
        try (MockedStatic<GlobalStateMgr> mockedGlobalState = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getWarehouseMgr()).thenReturn(warehouseManager);
            mockedGlobalState.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DefaultPreSplitPipeline pipeline = newPipelineWithResource(clock, resource);
            // 0 tablets placed on 0 distinct nodes every poll -> the wait expires and proceeds, no throw.
            Assertions.assertDoesNotThrow(() -> pipeline.awaitFinished(
                    new PreSplitPipeline.PreparedReshardJob(split), POST_SUBMIT_TIMEOUT, () -> false));
        } finally {
            Config.tablet_pre_split_await_shard_spread_seconds = savedSpread;
        }
    }

    /** A test-only {@link Clock} whose "now" advances only when callers say so. */
    private static final class AdvanceableClock extends Clock {
        private Instant now;

        AdvanceableClock(Instant start) {
            this.now = start;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.systemDefault();
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }

        void advance(Duration delta) {
            now = now.plus(delta);
        }
    }
}
