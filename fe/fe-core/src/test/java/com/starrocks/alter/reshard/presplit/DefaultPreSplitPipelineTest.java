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

import com.starrocks.alter.reshard.SplitTabletJobFactory;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.DUMMY_CONTEXT;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintTuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultPreSplitPipelineTest {

    private static final long OLD_TABLET_ID = 9000L;
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
        table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t");
        tabletReshardJobManager = mock(TabletReshardJobMgr.class);

        Column sortColumn = bigintColumn("ts");
        sampleRequest = new SampleRequest(DUMMY_CONTEXT, List.of(sortColumn), 16L * DebugUtil.MEGABYTE, 0L);
    }

    @AfterEach
    public void tearDown() {
        Config.tablet_reshard_target_size = savedConfigTargetSize;
        Config.tablet_reshard_max_split_count = savedConfigMaxSplitCount;
    }

    private DefaultPreSplitPipeline newPipeline(MetaTierSampler metaTierSampler, Sampler dataTierSampler, Clock clock) {
        return new DefaultPreSplitPipeline(
                metaTierSampler, dataTierSampler, tabletReshardJobManager,
                database, table, OLD_TABLET_ID, FILE_TOTAL_BYTES,
                POLL_INTERVAL, clock);
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
            mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(eq(database), eq(table), eq(OLD_TABLET_ID), any()))
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
            mocked.when(() -> SplitTabletJobFactory.forExternalBoundaries(any(), any(), eq(OLD_TABLET_ID), any()))
                    .thenReturn(fakeJob);

            DefaultPreSplitPipeline pipeline = newPipeline(metaTier, dataTier, Clock.systemUTC());
            Optional<PreSplitPipeline.PreparedReshardJob> prepared =
                    pipeline.preSubmit(sampleRequest, ACTIVE_COMPUTE_NODES, PRE_SUBMIT_TIMEOUT);

            Assertions.assertTrue(prepared.isPresent());
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
        pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(fakeJob), POST_SUBMIT_TIMEOUT);
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
                () -> pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(fakeJob), POST_SUBMIT_TIMEOUT));
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
                () -> pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(fakeJob), Duration.ofSeconds(120)));
    }

    @Test
    public void testAwaitFinishedThrowsWhenJobDisappears() {
        TabletReshardJob fakeJob = mock(TabletReshardJob.class);
        when(fakeJob.getJobId()).thenReturn(11L);
        when(tabletReshardJobManager.getTabletReshardJob(11L)).thenReturn(null);

        DefaultPreSplitPipeline pipeline = newPipeline(
                (r, k) -> BoundaryPlannerResult.NO_SPLIT, r -> SampleSet.EMPTY, Clock.systemUTC());
        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> pipeline.awaitFinished(new PreSplitPipeline.PreparedReshardJob(fakeJob), POST_SUBMIT_TIMEOUT));
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
