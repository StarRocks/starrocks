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

import com.google.common.base.Preconditions;
import com.starrocks.alter.reshard.SplitTabletJobFactory;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Production {@link PreSplitPipeline} composing the FE-side sampler tiers,
 * {@link BoundaryPlanner}, {@link SplitTabletJobFactory#forExternalBoundaries},
 * and {@link TabletReshardJobMgr}. Constructor-injected dependencies keep the
 * class testable without static mocking.
 *
 * <p>Tier routing: meta tier ({@link ParquetMetadataSampler#tryPlan}) is invoked
 * first. {@link MetaTierUnavailableException} switches the run to data tier
 * ({@link ReservoirSampler#sample} + {@link BoundaryPlanner}). Any other
 * sampler throw propagates as {@link StarRocksException} and the coordinator
 * maps it to {@link SkipReason#SAMPLE_FAILED}.
 *
 * <p>Pre-submit timeout is enforced as a soft deadline: the pipeline checks
 * the deadline at sampler-phase boundaries rather than preempting in-flight
 * RPCs. This keeps the abort surface narrow at the cost of allowing one
 * extra phase to complete past the deadline.
 *
 * <p>{@link #awaitFinished} polls {@link TabletReshardJobMgr} on a fixed
 * interval. No event surface exists today; polling is acceptable because the
 * post-submit timeout caps total wait time.
 */
public final class DefaultPreSplitPipeline implements PreSplitPipeline {

    private static final Logger LOG = LogManager.getLogger(DefaultPreSplitPipeline.class);

    static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(500);

    /**
     * Metric label for a meta tier success path: boundaries computed from Parquet/ORC row-group
     * statistics ({@code meta_tier}), no row data read.
     */
    static final String TIER_LABEL_META_TIER = "meta_tier";

    /**
     * Metric label for a data tier success path: boundaries computed from actual row samples
     * ({@code data_tier}) collected via a FILES sub-query. Covers both direct data-tier invocations
     * and meta-tier → data-tier fallbacks.
     */
    static final String TIER_LABEL_DATA_TIER = "data_tier";

    private final MetaTierSampler metaTierSampler;
    private final Sampler dataTierSampler;
    private final TabletReshardJobMgr tabletReshardJobManager;
    private final Database database;
    private final OlapTable table;
    private final long oldTabletId;
    private final long fileTotalBytes;
    private final Duration pollInterval;
    private final Clock clock;

    public DefaultPreSplitPipeline(
            MetaTierSampler metaTierSampler,
            Sampler dataTierSampler,
            TabletReshardJobMgr tabletReshardJobManager,
            Database database,
            OlapTable table,
            long oldTabletId,
            long fileTotalBytes,
            Duration pollInterval,
            Clock clock) {
        this.metaTierSampler = Objects.requireNonNull(metaTierSampler, "metaTierSampler");
        this.dataTierSampler = Objects.requireNonNull(dataTierSampler, "dataTierSampler");
        this.tabletReshardJobManager = Objects.requireNonNull(tabletReshardJobManager, "tabletReshardJobManager");
        this.database = Objects.requireNonNull(database, "database");
        this.table = Objects.requireNonNull(table, "table");
        Preconditions.checkArgument(oldTabletId > 0, "oldTabletId must be > 0, was %s", oldTabletId);
        Preconditions.checkArgument(fileTotalBytes >= 0, "fileTotalBytes must be >= 0, was %s", fileTotalBytes);
        this.oldTabletId = oldTabletId;
        this.fileTotalBytes = fileTotalBytes;
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Build a pipeline wired with the executors appropriate for {@code loadKind}.
     * Centralizes the construction so all hooks (D1 INSERT-from-FILES, D2
     * Broker Load, future callers) share the same plumbing.
     *
     * <p>Meta tier and data tier are both production for both load kinds:
     * {@link InsertFromFilesRowGroupStatisticsProvider} +
     * {@link InsertFromFilesSampleSubqueryExecutor} for
     * {@link LoadKind#INSERT_FROM_FILES};
     * {@link BrokerLoadRowGroupStatisticsProvider} +
     * {@link BrokerLoadSampleSubqueryExecutor} for {@link LoadKind#BROKER_LOAD}.
     */
    public static DefaultPreSplitPipeline forLoadKind(
            Database database, OlapTable table, long oldTabletId, long fileTotalBytes, LoadKind loadKind) {
        ParquetMetadataSampler metaTierSampler = new ParquetMetadataSampler(
                rowGroupStatisticsProviderFor(loadKind), Config.tablet_pre_split_meta_tier_overlap_threshold);
        Sampler dataTierSampler = new ReservoirSampler(sampleSubqueryExecutorFor(loadKind));
        TabletReshardJobMgr tabletReshardJobManager = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        return new DefaultPreSplitPipeline(
                metaTierSampler::tryPlan, dataTierSampler, tabletReshardJobManager,
                database, table, oldTabletId, fileTotalBytes,
                DEFAULT_POLL_INTERVAL, Clock.systemUTC());
    }

    private static RowGroupStatisticsProvider rowGroupStatisticsProviderFor(LoadKind loadKind) {
        return switch (loadKind) {
            case INSERT_FROM_FILES -> new InsertFromFilesRowGroupStatisticsProvider();
            case BROKER_LOAD -> new BrokerLoadRowGroupStatisticsProvider();
        };
    }

    private static SampleSubqueryExecutor sampleSubqueryExecutorFor(LoadKind loadKind) {
        return switch (loadKind) {
            case INSERT_FROM_FILES -> new InsertFromFilesSampleSubqueryExecutor();
            case BROKER_LOAD -> new BrokerLoadSampleSubqueryExecutor();
        };
    }

    @Override
    public Optional<PreparedReshardJob> preSubmit(SampleRequest request, int activeComputeNodeCount, Duration timeout)
            throws PreSplitPreSubmitTimeoutException, StarRocksException {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(timeout, "timeout");
        Instant deadline = clock.instant().plus(timeout);

        recordSamplerInvocation();

        int requestedTabletCount = TabletPreSplitCoordinator.selectTabletCount(
                new Estimates(fileTotalBytes, 0L), activeComputeNodeCount);

        TierOutcome outcome = planBoundariesWithFallback(request, requestedTabletCount, deadline);
        if (outcome.result.isNoSplit()) {
            return Optional.empty();
        }

        recordTierUsed(outcome.tier);
        recordBoundariesPlanned(outcome.result.getBoundaries().size());

        List<TabletRange> tabletRanges = buildTabletRanges(outcome.result.getBoundaries());
        TabletReshardJob job = SplitTabletJobFactory.forExternalBoundaries(database, table, oldTabletId, tabletRanges);
        return Optional.of(new PreparedReshardJob(job));
    }

    @Override
    public void submit(PreparedReshardJob preparedJob) throws StarRocksException {
        Objects.requireNonNull(preparedJob, "preparedJob");
        TabletReshardJob job = (TabletReshardJob) preparedJob.payload();
        tabletReshardJobManager.addTabletReshardJob(job);
    }

    @Override
    public void awaitFinished(PreparedReshardJob preparedJob, Duration timeout)
            throws PreSplitPostSubmitTimeoutException, StarRocksException {
        Objects.requireNonNull(preparedJob, "preparedJob");
        Objects.requireNonNull(timeout, "timeout");
        TabletReshardJob submitted = (TabletReshardJob) preparedJob.payload();
        long jobId = submitted.getJobId();
        Instant deadline = clock.instant().plus(timeout);

        while (true) {
            TabletReshardJob latest = tabletReshardJobManager.getTabletReshardJob(jobId);
            if (latest == null) {
                throw new StarRocksException(
                        "tablet reshard job " + jobId + " disappeared from TabletReshardJobMgr");
            }
            TabletReshardJob.JobState state = latest.getJobState();
            if (state == TabletReshardJob.JobState.FINISHED) {
                return;
            }
            if (state.isFinalState()) {
                throw new StarRocksException("tablet reshard job " + jobId + " aborted: "
                        + latest.getErrorMessage());
            }
            if (clock.instant().isAfter(deadline)) {
                throw new PreSplitPostSubmitTimeoutException(
                        "tablet reshard job " + jobId + " did not reach FINISHED within "
                                + timeout.toSeconds() + "s; lastObservedState=" + state);
            }
            sleepUntilNextPoll(jobId);
        }
    }

    /**
     * Try the meta tier first; on {@link MetaTierUnavailableException}, fall back
     * to the data tier against the same deadline. The deadline is checked at
     * phase boundaries — no in-flight sampler RPC is preempted.
     */
    private TierOutcome planBoundariesWithFallback(SampleRequest request, int requestedTabletCount, Instant deadline)
            throws PreSplitPreSubmitTimeoutException, StarRocksException {
        try {
            return runMetaTier(request, requestedTabletCount, deadline);
        } catch (MetaTierUnavailableException metaTierUnavailable) {
            LOG.info("Sample-Based Tablet Pre-Split: meta tier unavailable for table {} — falling back to data tier: {}",
                    table.getName(), metaTierUnavailable.getMessage());
            return runDataTier(request, requestedTabletCount, deadline);
        }
    }

    private TierOutcome runMetaTier(SampleRequest request, int requestedTabletCount, Instant deadline)
            throws PreSplitPreSubmitTimeoutException, StarRocksException {
        BoundaryPlannerResult result = metaTierSampler.tryPlan(request, requestedTabletCount);
        checkDeadline(deadline);
        return new TierOutcome(result, TIER_LABEL_META_TIER);
    }

    private TierOutcome runDataTier(SampleRequest request, int requestedTabletCount, Instant deadline)
            throws PreSplitPreSubmitTimeoutException, StarRocksException {
        checkDeadline(deadline);
        SampleSet sampleSet = dataTierSampler.sample(request);
        checkDeadline(deadline);
        BoundaryPlannerResult result =
                BoundaryPlanner.planRowQuantileBoundaries(sampleSet, requestedTabletCount, request.getSortKey());
        return new TierOutcome(result, TIER_LABEL_DATA_TIER);
    }

    /** Cuts {@code c1 < c2 < ... < c_{K-1}} → tablet ranges
     *  {@code (-∞, c1), [c1, c2), [c2, c3), ..., [c_{K-1}, +∞)}. */
    static List<TabletRange> buildTabletRanges(List<Tuple> boundaries) {
        Preconditions.checkArgument(!boundaries.isEmpty(), "boundaries must be non-empty");
        List<TabletRange> ranges = new ArrayList<>(boundaries.size() + 1);
        Tuple previousBoundary = null;
        for (Tuple boundary : boundaries) {
            ranges.add(new TabletRange(Range.of(
                    previousBoundary, boundary,
                    /*lowerIncluded=*/ previousBoundary != null,
                    /*upperIncluded=*/ false)));
            previousBoundary = boundary;
        }
        ranges.add(new TabletRange(Range.of(
                previousBoundary, /*upperBound=*/ (Tuple) null,
                /*lowerIncluded=*/ previousBoundary != null,
                /*upperIncluded=*/ false)));
        return ranges;
    }

    private void checkDeadline(Instant deadline) throws PreSplitPreSubmitTimeoutException {
        if (clock.instant().isAfter(deadline)) {
            throw new PreSplitPreSubmitTimeoutException(
                    "pre-submit phase exceeded its deadline; aborting before next sampler call");
        }
    }

    private void sleepUntilNextPoll(long jobId) throws StarRocksException {
        try {
            Thread.sleep(pollInterval.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new StarRocksException("interrupted while awaiting tablet reshard job " + jobId);
        }
    }

    private static void recordSamplerInvocation() {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_SAMPLER_INVOCATIONS.increase(1L);
        }
    }

    private static void recordTierUsed(String tierLabel) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_TIER_USED.getMetric(tierLabel).increase(1L);
        }
    }

    private static void recordBoundariesPlanned(int boundaryCount) {
        if (MetricRepo.hasInit) {
            MetricRepo.HISTO_TABLET_PRE_SPLIT_BOUNDARIES_PLANNED.update(boundaryCount);
        }
    }

    /** Internal carrier for the chosen tier's result plus its metric label. */
    private record TierOutcome(BoundaryPlannerResult result, String tier) {
    }
}
