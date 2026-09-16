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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * Production {@link PreSplitPipeline} composing the FE-side sampler tiers,
 * {@link BoundaryPlanner}, and {@link TabletReshardJobMgr}. The job itself comes
 * from {@link SplitTabletJobFactory#forExternalBoundaries}, which builds one job
 * spanning every split tablet (one or many visible indexes). Constructor-injected
 * dependencies keep the class testable without static mocking.
 *
 * <p>Tier routing: meta tier ({@link ParquetMetadataSampler#tryPlan}) is invoked
 * first. {@link MetaTierUnavailableException} switches the run to data tier
 * ({@link ReservoirSampler#sample} + {@link BoundaryPlanner}). Any other
 * sampler throw propagates as {@link StarRocksException} and the coordinator
 * maps it to {@link SkipReason#SAMPLE_FAILED}.
 *
 * <p>A pipeline built by {@link #forDerivedBoundaries} bypasses that routing
 * entirely: a {@link DerivedBoundarySource} computes the cuts from the sort
 * key's own domain, so neither tier is consulted and nothing is read.
 *
 * <p>Pre-submit timeout is enforced at sampler-phase boundaries via
 * {@link #checkDeadline}. The data tier additionally caps its BE-side sample
 * sub-query at the remaining budget ({@link SampleRequest#withQueryTimeoutSeconds}),
 * so an over-budget sample is cancelled by the BE instead of running the
 * deadline over by a full sample phase. The meta tier (file-footer reads, no
 * BE query) is still only boundary-checked, but its cost is bounded by the
 * file count rather than a sub-query that could hang.
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

    /**
     * Metric label for a derived tier success path: boundaries computed from what is known about the
     * key's own domain ({@code derived_tier}) — no file statistics and no row sample, so nothing is read
     * at all. Used when the sort key is a hidden row-id column whose value distribution follows from how
     * the id is produced rather than from the data.
     */
    static final String TIER_LABEL_DERIVED_TIER = "derived_tier";

    private final MetaTierSampler metaTierSampler;
    private final Sampler dataTierSampler;
    private final TabletReshardJobMgr tabletReshardJobManager;
    private final Database database;
    private final OlapTable table;
    private final List<IndexPreSplitTarget> indexTargets;
    private final long fileTotalBytes;
    private final Duration pollInterval;
    private final Clock clock;
    // The triggering load's compute resource, carried into the SplitTabletJobFactory job
    // (forExternalBoundaries) so pre-split shards are scheduled in the load's warehouse.
    // May be null (falls back to default).
    private final ComputeResource loadComputeResource;
    // Non-null only for a pipeline built by forDerivedBoundaries; null for every sampled pipeline.
    private final DerivedBoundarySource derivedBoundarySource;

    public DefaultPreSplitPipeline(
            MetaTierSampler metaTierSampler,
            Sampler dataTierSampler,
            TabletReshardJobMgr tabletReshardJobManager,
            Database database,
            OlapTable table,
            List<IndexPreSplitTarget> indexTargets,
            long fileTotalBytes,
            Duration pollInterval,
            Clock clock,
            ComputeResource loadComputeResource) {
        this(metaTierSampler, dataTierSampler, tabletReshardJobManager, database, table, indexTargets,
                fileTotalBytes, pollInterval, clock, loadComputeResource, /*derivedBoundarySource=*/ null);
    }

    DefaultPreSplitPipeline(
            MetaTierSampler metaTierSampler,
            Sampler dataTierSampler,
            TabletReshardJobMgr tabletReshardJobManager,
            Database database,
            OlapTable table,
            List<IndexPreSplitTarget> indexTargets,
            long fileTotalBytes,
            Duration pollInterval,
            Clock clock,
            ComputeResource loadComputeResource,
            DerivedBoundarySource derivedBoundarySource) {
        this.metaTierSampler = Objects.requireNonNull(metaTierSampler, "metaTierSampler");
        this.dataTierSampler = Objects.requireNonNull(dataTierSampler, "dataTierSampler");
        this.tabletReshardJobManager = Objects.requireNonNull(tabletReshardJobManager, "tabletReshardJobManager");
        this.database = Objects.requireNonNull(database, "database");
        this.table = Objects.requireNonNull(table, "table");
        Preconditions.checkArgument(indexTargets != null && !indexTargets.isEmpty(),
                "indexTargets must be non-empty");
        Preconditions.checkArgument(fileTotalBytes >= 0, "fileTotalBytes must be >= 0, was %s", fileTotalBytes);
        this.indexTargets = indexTargets;
        this.fileTotalBytes = fileTotalBytes;
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.loadComputeResource = loadComputeResource;
        this.derivedBoundarySource = derivedBoundarySource;
    }

    /**
     * Build a pipeline wired with the executors appropriate for {@code loadKind}.
     * Centralizes the construction so all hooks (INSERT-from-FILES,
     * Broker Load, future callers) share the same plumbing.
     *
     * <p>Meta tier and data tier are both production for both load kinds:
     * {@link InsertFromFilesRowGroupStatisticsProvider} +
     * {@link InsertFromFilesSampleSubqueryExecutor} for
     * {@link LoadKind#INSERT_FROM_FILES};
     * {@link BrokerLoadRowGroupStatisticsProvider} +
     * {@link BrokerLoadSampleSubqueryExecutor} for {@link LoadKind#BROKER_LOAD}.
     *
     * <p><b>Partitioned-table override.</b> When the target table is partitioned
     * ({@link com.starrocks.catalog.PartitionInfo#isPartitioned()}), the meta
     * tier is replaced with a no-op that always raises
     * {@link MetaTierUnavailableException} so the pipeline falls through to the
     * data tier. The meta tier reads per-column min/max from Parquet row-group
     * statistics, which is fundamentally lossy when the partition column has an
     * expression ({@code date_trunc}, {@code time_slice}): the sampler computes
     * boundaries from row-group stats of the raw column, but the load routes
     * each row through the expression first, so the boundaries do not align
     * with the partitions the rows actually land in. The multi-partition flow
     * relies on per-row partition-value tuples that only the data tier
     * projects, so meta tier cannot serve the partitioned path even
     * defensively. Unpartitioned tables retain the meta-tier-first routing.
     */
    public static DefaultPreSplitPipeline forLoadKind(
            Database database, OlapTable table, List<IndexPreSplitTarget> indexTargets, long fileTotalBytes,
            LoadKind loadKind, ComputeResource loadComputeResource) {
        MetaTierSampler metaTierSampler;
        if (loadKind == LoadKind.INSERT_FROM_TABLE || table.getPartitionInfo().isPartitioned()) {
            // INSERT_FROM_TABLE always uses the SQL data tier so internal OLAP and external
            // Iceberg sources share one sampling path without depending on file-footer access.
            // Partitioned tables also force data tier: meta tier per-column min/max is
            // lossy under expression-based partitioning.
            metaTierSampler = (request, requestedTabletCount) -> {
                throw new MetaTierUnavailableException(
                        loadKind == LoadKind.INSERT_FROM_TABLE
                                ? "INSERT-from-table uses the SQL data-tier sampler"
                                : "partitioned table forces data tier (meta tier per-column min/max "
                                        + "is lossy under expression-based partitioning)");
            };
        } else {
            ParquetMetadataSampler parquetMetadataSampler = new ParquetMetadataSampler(
                    rowGroupStatisticsProviderFor(loadKind),
                    Config.tablet_pre_split_meta_tier_overlap_threshold);
            metaTierSampler = parquetMetadataSampler::tryPlan;
        }
        Sampler dataTierSampler = new ReservoirSampler(sampleSubqueryExecutorFor(loadKind));
        TabletReshardJobMgr tabletReshardJobManager = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        return new DefaultPreSplitPipeline(
                metaTierSampler, dataTierSampler, tabletReshardJobManager,
                database, table, indexTargets, fileTotalBytes,
                DEFAULT_POLL_INTERVAL, Clock.systemUTC(), loadComputeResource);
    }

    /**
     * Build a pipeline whose boundaries come from {@code derivedBoundarySource} rather than from a
     * sampler. {@code estimatedBytes} sizes the tablet count exactly as a sampled load's input size
     * does; the source itself reads nothing, so there is no tier to fall back to.
     *
     * <p>Both samplers are installed as stubs that throw {@link IllegalStateException}. A derived
     * pipeline must never reach one, and a routing regression that lets it through has to fail loudly
     * instead of quietly sampling the data the derived tier exists to avoid reading.
     */
    static DefaultPreSplitPipeline forDerivedBoundaries(
            Database database, OlapTable table, List<IndexPreSplitTarget> indexTargets, long estimatedBytes,
            LoadKind loadKind, ComputeResource loadComputeResource, DerivedBoundarySource derivedBoundarySource) {
        Objects.requireNonNull(derivedBoundarySource, "derivedBoundarySource");
        MetaTierSampler metaTierSampler = (request, requestedTabletCount) -> {
            throw new IllegalStateException(loadKind.displayName()
                    + " uses the derived tier; the meta tier must not be reached");
        };
        Sampler dataTierSampler = request -> {
            throw new IllegalStateException(loadKind.displayName()
                    + " uses the derived tier; the data tier must not be reached");
        };
        TabletReshardJobMgr tabletReshardJobManager = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        return new DefaultPreSplitPipeline(
                metaTierSampler, dataTierSampler, tabletReshardJobManager,
                database, table, indexTargets, estimatedBytes,
                DEFAULT_POLL_INTERVAL, Clock.systemUTC(), loadComputeResource, derivedBoundarySource);
    }

    private static RowGroupStatisticsProvider rowGroupStatisticsProviderFor(LoadKind loadKind) {
        return switch (loadKind) {
            case INSERT_FROM_FILES -> new InsertFromFilesRowGroupStatisticsProvider();
            case BROKER_LOAD -> new BrokerLoadRowGroupStatisticsProvider();
            // INSERT_FROM_TABLE always forces data tier; the meta tier is bypassed in forLoadKind
            // before this method is ever reached for that load kind.
            case INSERT_FROM_TABLE -> throw new IllegalStateException(
                    "INSERT_FROM_TABLE never uses the meta tier; rowGroupStatisticsProviderFor must not be called");
            // MV_REFRESH is served by the derived tier, which is selected before any sampler is built.
            case MV_REFRESH -> throw new IllegalStateException(
                    "MV_REFRESH never samples; rowGroupStatisticsProviderFor must not be called");
        };
    }

    static SampleSubqueryExecutor sampleSubqueryExecutorFor(LoadKind loadKind) {
        return switch (loadKind) {
            case INSERT_FROM_FILES -> new InsertFromFilesSampleSubqueryExecutor();
            case BROKER_LOAD -> new BrokerLoadSampleSubqueryExecutor();
            case INSERT_FROM_TABLE -> new InsertFromTableSampleSubqueryExecutor();
            // MV_REFRESH is served by the derived tier, which is selected before any sampler is built.
            case MV_REFRESH -> throw new IllegalStateException(
                    "MV_REFRESH never samples; sampleSubqueryExecutorFor must not be called");
        };
    }

    /** Exposes the installed meta-tier sampler for unit tests that verify tier-routing logic. */
    MetaTierSampler getMetaTierSamplerForTest() {
        return metaTierSampler;
    }

    @Override
    public Optional<PreparedReshardJob> preSubmit(SampleRequest request, int activeComputeNodeCount, Duration timeout)
            throws PreSplitPreSubmitTimeoutException, StarRocksException {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(timeout, "timeout");
        if (derivedBoundarySource != null) {
            return preSubmitDerived(activeComputeNodeCount);
        }
        Instant deadline = clock.instant().plus(timeout);

        recordSamplerInvocation();

        int requestedTabletCount = TabletPreSplitCoordinator.selectPreSplitTabletCount(
                new Estimates(fileTotalBytes, 0L), activeComputeNodeCount);

        Map<Long, List<TabletRange>> oldTabletIdToRanges = new LinkedHashMap<>();
        for (IndexPreSplitTarget indexTarget : indexTargets) {
            SampleRequest indexRequest = new SampleRequest(request.getScanContext(), indexTarget.sortKey(),
                    request.getSampleByteLimit(), request.getSeed());
            TierOutcome outcome = planBoundariesWithFallback(
                    indexRequest, requestedTabletCount, activeComputeNodeCount, deadline);
            if (outcome.result().isNoSplit()) {
                continue;
            }
            PreSplitProfile.recordBoundariesPlanned(outcome.result().getBoundaries().size());
            if (oldTabletIdToRanges.isEmpty()) {
                // first index that produced cuts -> record load-level tier/boundary metrics once
                recordTierUsed(outcome.tier());
                recordBoundariesPlanned(outcome.result().getBoundaries().size());
            }
            oldTabletIdToRanges.put(indexTarget.oldTabletId(), buildTabletRanges(outcome.result().getBoundaries()));
        }
        return buildPreparedJob(oldTabletIdToRanges);
    }

    /**
     * Submit path for a {@link DerivedBoundarySource}: the source computes its cuts from the sort key's
     * own domain, so there is no footer read and no BE sample to bound, leaving the pre-submit deadline
     * nothing to guard. {@link #recordSamplerInvocation()} is deliberately not called either — the
     * derived tier is not a sampler, and counting it would make the sampler-invocation metric, and any
     * failure ratio built on it, meaningless.
     *
     * <p>Anything short of a full set of usable cuts skips the whole submit and records the reason as
     * an eligibility-skip: the source never ran a sampler, so the sampler-failure family would report
     * failures that have no matching invocation. A skip also describes the load as a whole (its
     * estimate, its key space) rather than one index, so the first one ends the submit instead of
     * carving the remaining indexes on a derivation the source already declined.
     */
    private Optional<PreparedReshardJob> preSubmitDerived(int activeComputeNodeCount) throws StarRocksException {
        PreSplitProfile.recordSourceTier(TIER_LABEL_DERIVED_TIER);
        PreSplitProfile.recordEstimatedInputBytes(fileTotalBytes);
        int requestedTabletCount = TabletPreSplitCoordinator.selectPreSplitTabletCount(
                new Estimates(fileTotalBytes, 0L), activeComputeNodeCount);

        Map<Long, List<TabletRange>> oldTabletIdToRanges = new LinkedHashMap<>();
        try (PreSplitProfile.Scope ignored = PreSplitProfile.startPhase(
                PreSplitProfile.Phase.PARTITION_AND_BOUNDARY_PLANNING)) {
            for (IndexPreSplitTarget indexTarget : indexTargets) {
                DerivedBoundarySource.Result derived = derivedBoundarySource.plan(indexTarget, requestedTabletCount);
                if (derived.skipReason() != null) {
                    LOG.info("Sample-Based Tablet Pre-Split: derived tier produced no boundaries for table {}: {}",
                            table.getName(), derived.skipReason());
                    PreSplitMetrics.recordEligibilitySkip(derived.skipReason());
                    return Optional.empty();
                }
                List<Tuple> boundaries = derived.boundaries().getBoundaries();
                validateDerivedBoundaries(boundaries, indexTarget.sortKey());
                PreSplitProfile.recordBoundariesPlanned(boundaries.size());
                List<TabletRange> ranges = buildTabletRanges(boundaries);
                if (oldTabletIdToRanges.isEmpty()) {
                    // first index that produced cuts -> record load-level tier/boundary metrics once
                    recordTierUsed(TIER_LABEL_DERIVED_TIER);
                    recordBoundariesPlanned(boundaries.size());
                }
                oldTabletIdToRanges.put(indexTarget.oldTabletId(), ranges);
            }
        } catch (StarRocksException | RuntimeException derivationFailed) {
            LOG.warn("Sample-Based Tablet Pre-Split: derived boundaries unusable for table {}",
                    table.getName(), derivationFailed);
            PreSplitMetrics.recordEligibilitySkip(SkipReason.DERIVATION_FAILED);
            return Optional.empty();
        }
        // Job construction is reported separately from derivation, because by here the cuts ARE derived:
        // the factory rejects on table state, table type or colocate stability, none of which say anything
        // about the boundary source. It still must not escape, though -- left to the shared sampled-path
        // handling a StarRocksException becomes SAMPLE_FAILED, naming a sampler that never ran, and a
        // RuntimeException reaches the hook's fail-safe with no skip recorded at all.
        try {
            Optional<PreparedReshardJob> preparedJob = buildPreparedJob(oldTabletIdToRanges);
            if (preparedJob.isEmpty()) {
                // Not "no useful cuts" -- cuts were derived. The visible-index set moved under us.
                PreSplitMetrics.recordEligibilitySkip(SkipReason.STALE_CATALOG_STATE);
            }
            return preparedJob;
        } catch (StarRocksException | RuntimeException cannotBuildJob) {
            LOG.warn("Sample-Based Tablet Pre-Split: derived cuts could not be turned into a job for table {}",
                    table.getName(), cannotBuildJob);
            PreSplitMetrics.recordEligibilitySkip(SkipReason.SUBMIT_FAILED);
            return Optional.empty();
        }
    }

    /**
     * Nothing upstream has checked the cuts a derived source computes: they are not read off sorted
     * data, and {@link BoundaryPlannerResult} only collapses adjacent duplicates. A malformed set would
     * therefore reach range construction and be caught no earlier than the BE's fallback to an unsplit
     * tablet. Reuses the sampler tiers' own schema check so both paths reject the same shapes, and adds
     * the ordering check the tiers get for free from sorting their samples.
     *
     * @throws StarRocksException when a cut does not match {@code sortKey} or the cuts are not
     *                            strictly increasing.
     */
    private static void validateDerivedBoundaries(List<Tuple> boundaries, List<Column> sortKey)
            throws StarRocksException {
        for (int cutIndex = 0; cutIndex < boundaries.size(); cutIndex++) {
            Tuple cut = boundaries.get(cutIndex);
            BoundaryPlanner.validateTupleAgainstSchema(cut, sortKey, "Derived cut " + cutIndex);
            // Both operands are schema-checked by now (the predecessor on the previous iteration), so
            // the comparator is comparing like with like.
            if (cutIndex > 0 && cut.compareTo(boundaries.get(cutIndex - 1)) <= 0) {
                throw new StarRocksException(String.format(
                        "Derived cut %d (%s) does not exceed its predecessor (%s)",
                        cutIndex, cut, boundaries.get(cutIndex - 1)));
            }
        }
    }

    /**
     * Tail shared by both submit paths: turn the planned per-index ranges into a job, or skip.
     */
    private Optional<PreparedReshardJob> buildPreparedJob(Map<Long, List<TabletRange>> oldTabletIdToRanges)
            throws StarRocksException {
        if (oldTabletIdToRanges.isEmpty()) {
            return Optional.empty();
        }
        // Final authoritative re-check: a rollup could have become visible (or dropped) between the target
        // snapshot and here. Re-resolve the visible index-id set under a brief READ lock; if it no longer
        // equals the planned set, skip pre-split (never submit a base-only partial). The factory's own READ
        // lock + table-state check + admission CAS cover the residual micro-window; a base-only split is
        // data-safe regardless (BE routes by true value), so this re-check is an extra safeguard on top
        // of that, not the sole defense.
        Set<Long> expectedIndexMetaIds = indexTargets.stream().map(IndexPreSplitTarget::indexMetaId)
                .collect(Collectors.toSet());
        if (!currentVisibleIndexMetaIds(database, table).equals(expectedIndexMetaIds)) {
            return Optional.empty();
        }
        TabletReshardJob job = SplitTabletJobFactory.forExternalBoundaries(database, table, oldTabletIdToRanges);
        // Carry the triggering load's warehouse so the job's shard creation + publish run there.
        if (loadComputeResource != null) {
            job.setWarehouseId(loadComputeResource.getWarehouseId());
        }
        return Optional.of(new PreparedReshardJob(job));
    }

    /**
     * Re-resolves {@code table}'s currently-visible index-meta-id set under a brief intensive READ
     * lock. Used by {@link #preSubmit} to detect a rollup that became visible (or was dropped)
     * between the eligibility-target snapshot and job assembly.
     */
    private static Set<Long> currentVisibleIndexMetaIds(Database database, OlapTable table) {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(database.getId(), table.getId(), LockType.READ);
        try {
            return table.getVisibleIndexMetas().stream()
                    .map(MaterializedIndexMeta::getIndexMetaId)
                    .collect(Collectors.toSet());
        } finally {
            locker.unLockTableWithIntensiveDbLock(database.getId(), table.getId(), LockType.READ);
        }
    }

    @Override
    public void submit(PreparedReshardJob preparedJob) throws StarRocksException {
        Objects.requireNonNull(preparedJob, "preparedJob");
        TabletReshardJob job = (TabletReshardJob) preparedJob.payload();
        tabletReshardJobManager.addTabletReshardJob(job);
    }

    @Override
    public void awaitFinished(PreparedReshardJob preparedJob, Duration timeout,
                              BooleanSupplier shouldAbort)
            throws PreSplitPostSubmitTimeoutException, StarRocksException {
        Objects.requireNonNull(preparedJob, "preparedJob");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(shouldAbort, "shouldAbort");
        TabletReshardJob submitted = (TabletReshardJob) preparedJob.payload();
        long jobId = submitted.getJobId();
        Instant deadline = clock.instant().plus(timeout);

        while (true) {
            if (shouldAbort.getAsBoolean()) {
                throw new StarRocksException("tablet reshard job " + jobId
                        + " await abandoned: caller signalled abort");
            }
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
    private TierOutcome planBoundariesWithFallback(SampleRequest request, int requestedTabletCount,
                                                   int activeComputeNodeCount, Instant deadline)
            throws PreSplitPreSubmitTimeoutException, StarRocksException {
        try {
            return runMetaTier(request, requestedTabletCount, deadline);
        } catch (MetaTierUnavailableException metaTierUnavailable) {
            LOG.info("Sample-Based Tablet Pre-Split: meta tier unavailable for table {} — falling back to data tier: {}",
                    table.getName(), metaTierUnavailable.getMessage());
            return runDataTier(request, requestedTabletCount, activeComputeNodeCount, deadline);
        }
    }

    private TierOutcome runMetaTier(SampleRequest request, int requestedTabletCount, Instant deadline)
            throws PreSplitPreSubmitTimeoutException, StarRocksException {
        // Record the tier when it starts so a later fallback still exposes the footer work that
        // contributed to the phase timers.
        PreSplitProfile.recordSourceTier(TIER_LABEL_META_TIER);
        // The production metadata sampler instruments footer-statistics fetch and boundary
        // planning separately. Keeping a scope around this combined interface would double-count
        // the fetch and incorrectly attribute planning work to SourceSamplingTime.
        BoundaryPlannerResult result = metaTierSampler.tryPlan(request, requestedTabletCount);
        // Unlike the data tier, a successful footer-only plan has no SampleSet through which to
        // expose the estimate that sized this attempt.
        PreSplitProfile.recordEstimatedInputBytes(fileTotalBytes);
        checkDeadline(deadline);
        return new TierOutcome(result, TIER_LABEL_META_TIER);
    }

    private TierOutcome runDataTier(SampleRequest request, int requestedTabletCount,
                                    int activeComputeNodeCount, Instant deadline)
            throws PreSplitPreSubmitTimeoutException, StarRocksException {
        checkDeadline(deadline);
        PreSplitProfile.recordSourceTier(TIER_LABEL_DATA_TIER);
        // Cap the sample at the remaining budget (see class doc); an over-budget
        // sample is cancelled by the BE → SAMPLE_FAILED → the load proceeds.
        SampleRequest budgetedRequest = request.withQueryTimeoutSeconds(remainingBudgetSeconds(deadline));
        SampleSet sampleSet;
        try (PreSplitProfile.Scope ignored = PreSplitProfile.startPhase(
                PreSplitProfile.Phase.SOURCE_SAMPLING)) {
            sampleSet = dataTierSampler.sample(budgetedRequest);
        }
        PreSplitProfile.recordSample(sampleSet);
        checkDeadline(deadline);
        BoundaryPlannerResult result;
        try (PreSplitProfile.Scope ignored = PreSplitProfile.startPhase(
                PreSplitProfile.Phase.PARTITION_AND_BOUNDARY_PLANNING)) {
            result = BoundaryPlanner.planRowQuantileBoundaries(
                    sampleSet, effectiveTabletCount(sampleSet, requestedTabletCount, activeComputeNodeCount),
                    request.getSortKey());
        }
        return new TierOutcome(result, TIER_LABEL_DATA_TIER);
    }

    /**
     * Re-sizes the split count against what the load's predicate actually selects.
     *
     * <p>{@code requestedTabletCount} was computed from {@code fileTotalBytes}, which measures the
     * whole source: for an external Iceberg table that is the entire snapshot, however selective the
     * INSERT's WHERE clause is. The data tier is the only tier that learns the filtered size, because
     * it is the only one that actually samples, so this is the first point where a better number
     * exists. Without it a selective INSERT into an unpartitioned target is carved into tablets sized
     * for the whole table -- and with a small {@code tablet_pre_split_target_size} that runs straight
     * into {@code tablet_reshard_max_split_count}, leaving sub-megabyte tablets. The multi-partition
     * flow already sizes from the sampler's estimate; this brings the single-partition flow in line.
     *
     * <p>A zero byte estimate means the sampler could not size the input at all (an Iceberg snapshot
     * whose summary carries no totals, for instance). Keep the caller's count in that case rather
     * than collapsing to the two-tablet floor on no evidence.
     */
    private static int effectiveTabletCount(SampleSet sampleSet, int requestedTabletCount,
                                            int activeComputeNodeCount) {
        Estimates sampledEstimates = sampleSet.getEstimates();
        if (sampledEstimates == null || sampledEstimates.totalBytes() <= 0L) {
            return requestedTabletCount;
        }
        return TabletPreSplitCoordinator.selectPreSplitTabletCount(sampledEstimates, activeComputeNodeCount);
    }

    /**
     * Whole-second budget remaining until {@code deadline} (rounded up). Floored
     * at 1 so we never hand the BE {@code query_timeout = 0}, which it reads as
     * "no timeout"; the preceding {@link #checkDeadline} guarantees the
     * remainder is positive.
     */
    private int remainingBudgetSeconds(Instant deadline) {
        long remainingMillis = Duration.between(clock.instant(), deadline).toMillis();
        return (int) Math.max(1L, (remainingMillis + 999L) / 1000L);
    }

    /** Cuts {@code c1 < c2 < ... < c_{K-1}} → tablet ranges
     *  {@code (-∞, c1), [c1, c2), [c2, c3), ..., [c_{K-1}, +∞)}.
     *  Requires a non-empty boundary list; callers that need to handle the empty case must guard
     *  before calling (e.g. return a single {@code Range.all()} tablet). */
    public static List<TabletRange> buildTabletRanges(List<Tuple> boundaries) {
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
