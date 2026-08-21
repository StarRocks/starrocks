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
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Shared core of the Sample-Based Tablet Pre-Split flow for every load-kind
 * entry hook (INSERT-from-FILES and INSERT-from-table via their INSERT hook,
 * Broker Load via {@link BrokerLoadPreSplitHook}). Once a hook has resolved its
 * source-specific inputs into a {@link Prepared} bundle, it hands control here.
 *
 * <p>This class owns the parts of the flow that are identical across load
 * kinds:
 * <ul>
 *   <li>{@link #dispatch} — partitioned-vs-unpartitioned routing, including the
 *       automatic-partition gate that conservatively skips manually
 *       list/range-partitioned targets (those cannot pre-create partitions from
 *       sampled values).</li>
 *   <li>{@link #runSinglePartitionFlow} — resolve the unique partition + base
 *       tablet, build a {@link DefaultPreSplitPipeline}, submit via
 *       {@link TabletPreSplitCoordinator#submitAsynchronously}, then sync-await
 *       fail-safely.</li>
 *   <li>{@link #runMultiPartitionFlow} — direct data-tier sample, group by
 *       predicted partition, submit ONE combined reshard via
 *       {@link TabletPreSplitCoordinator#submitForPartitionsCombined}, then
 *       sync-await fail-safely.</li>
 *   <li>{@link #runDataTierSampler} — direct data-tier sample step used by the
 *       multi-partition flow (which bypasses the pipeline's plan/submit
 *       stages).</li>
 * </ul>
 *
 * <p>The source-specific concerns each hook keeps for itself: statement-shape
 * detection, source-table / FILES schema resolution, authorization, and the
 * {@code ScanContext} + sort-key / partition-column extraction that feed the
 * {@link Prepared} bundle.
 */
final class PreSplitFlow {

    private static final Logger LOG = LogManager.getLogger(PreSplitFlow.class);

    // Target total number of weighted (min,max) endpoint pairs the meta-tier multi-partition sampler
    // emits, apportioned across row groups by row count so the boundary planner's row-quantiles
    // reflect row DENSITY (not row-group count). Kept in the same order of magnitude as the data-tier
    // reservoir target so per-partition boundary resolution is comparable.
    private static final int META_WEIGHTED_ENDPOINT_BUDGET = 40_000;
    // Per-row-group cap on emitted endpoint copies, so a single dominant row group cannot balloon the
    // FE-side sample and the pair count stays bounded even for a heavily skewed input.
    private static final long META_MAX_ENDPOINT_WEIGHT = 2048L;

    private PreSplitFlow() {
    }

    /**
     * Source-resolved inputs the flow needs. sortKeyColumns / partitionColumns are TARGET
     * columns (boundary planning + per-row partition projection); estimatedBytes sizes the
     * requested tablet count; computeResource sizes the active CN count; scanContext carries
     * the source-specific scan inputs; secondaryIndexSpecs names every OTHER visible index
     * (rollup) whose sort key the multi-partition data-tier sampler should project alongside
     * the base sort key -- empty for single-index targets.
     */
    record Prepared(ScanContext scanContext, List<Column> sortKeyColumns,
                    List<Column> partitionColumns, long estimatedBytes,
                    ComputeResource computeResource, List<SecondaryIndexSpec> secondaryIndexSpecs) {
    }

    static void dispatch(Database database, OlapTable target, Prepared prepared,
                         LoadKind loadKind, BooleanSupplier shouldAbort, ConnectContext context) {
        dispatch(database, target, prepared, loadKind, shouldAbort, context,
                PreSplitPartitionScope.unrestricted());
    }

    static void dispatch(Database database, OlapTable target, Prepared prepared,
                         LoadKind loadKind, BooleanSupplier shouldAbort, ConnectContext context,
                         PreSplitPartitionScope partitionScope) {
        if (target.getPartitionInfo().isPartitioned()) {
            // Manually list/range-partitioned targets do not support pre-creating partitions
            // from sampled values; skip conservatively, let the load proceed.
            if (!Boolean.TRUE.equals(target.supportedAutomaticPartition())) {
                PreSplitProfile.recordOutcome("SKIPPED: MANUAL_PARTITIONING");
                return;
            }
            runMultiPartitionFlow(database, target, prepared, loadKind, shouldAbort, context, partitionScope);
        } else {
            // An unpartitioned target owns exactly one partition and the single-partition flow has
            // nowhere to apply a scope. This hook runs pre-analysis, so an INSERT naming a partition
            // that does not exist on this table has not been rejected yet -- splitting the sole real
            // partition here would let a statement that goes on to fail analysis reshape the table.
            // Skip conservatively; this is the same shape the pre-scope code rejected outright.
            if (partitionScope.isSpecified()) {
                PreSplitProfile.recordOutcome("SKIPPED: PARTITION_SCOPE_ON_UNPARTITIONED_TABLE");
                return;
            }
            runSinglePartitionFlow(database, target, prepared, loadKind, shouldAbort);
        }
    }

    static void runSinglePartitionFlow(Database database, OlapTable table, Prepared prepared,
                                       LoadKind loadKind, BooleanSupplier shouldAbort) {
        PreSplitTargets.EligibleTarget target = PreSplitTargets.findEligibleTarget(database, table);
        if (target == null) {
            PreSplitProfile.recordOutcome("SKIPPED: PARTITION_NOT_ELIGIBLE");
            return;
        }
        runSinglePartitionFlow(target, prepared, loadKind, shouldAbort);
    }

    /**
     * Body of {@link #runSinglePartitionFlow(Database, OlapTable, Prepared, LoadKind, BooleanSupplier)}
     * for a target the caller resolved itself. A static overwrite resolves the TEMPORARY partition its
     * write lands in rather than the table's live one, and must not re-resolve.
     */
    static void runSinglePartitionFlow(PreSplitTargets.EligibleTarget target, Prepared prepared,
                                       LoadKind loadKind, BooleanSupplier shouldAbort) {
        PreSplitProfile.recordTargetPartitions(1L);
        int activeComputeNodeCount = TabletReshardUtils.computeNodeCount(prepared.computeResource());
        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                target.database(), target.olapTable(), target.indexTargets(), prepared.estimatedBytes(), loadKind,
                prepared.computeResource());
        submitAndAwaitSinglePartition(target, pipeline, prepared.scanContext(), loadKind,
                activeComputeNodeCount, shouldAbort);
    }

    static void runMultiPartitionFlow(Database database, OlapTable table, Prepared prepared,
                                      LoadKind loadKind, BooleanSupplier shouldAbort, ConnectContext context) {
        runMultiPartitionFlow(database, table, prepared, loadKind, shouldAbort, context,
                -1L, PreSplitPartitionScope.unrestricted());
    }

    static void runMultiPartitionFlow(Database database, OlapTable table, Prepared prepared,
                                      LoadKind loadKind, BooleanSupplier shouldAbort, ConnectContext context,
                                      PreSplitPartitionScope partitionScope) {
        runMultiPartitionFlow(database, table, prepared, loadKind, shouldAbort, context, -1L, partitionScope);
    }

    /**
     * Samples a dynamic overwrite before its write starts, pre-creates the predicted
     * transaction-scoped temporary partitions, and splits those temporary partitions.
     */
    static void runDynamicOverwriteFlow(Database database, OlapTable table, Prepared prepared,
                                        LoadKind loadKind, BooleanSupplier shouldAbort,
                                        ConnectContext context, long overwriteTransactionId) {
        if (!table.getPartitionInfo().isPartitioned()
                || !Boolean.TRUE.equals(table.supportedAutomaticPartition())
                || overwriteTransactionId <= 0) {
            PreSplitProfile.recordOutcome("SKIPPED: DYNAMIC_OVERWRITE_TARGET_NOT_ELIGIBLE");
            return;
        }
        runMultiPartitionFlow(database, table, prepared, loadKind, shouldAbort, context,
                overwriteTransactionId, PreSplitPartitionScope.unrestricted());
    }

    /** Splits the already-created temporary partitions of a static INSERT OVERWRITE job. */
    static void runStaticOverwriteFlow(Database database, OlapTable table, Prepared prepared,
                                       LoadKind loadKind, BooleanSupplier shouldAbort,
                                       ConnectContext context, PreSplitPartitionScope partitionScope) {
        if (!partitionScope.isSpecified() || !partitionScope.isTemporary()) {
            PreSplitProfile.recordOutcome("SKIPPED: STATIC_OVERWRITE_TARGET_NOT_ELIGIBLE");
            return;
        }
        if (!table.getPartitionInfo().isPartitioned()) {
            // An unpartitioned table's overwrite clones its sole partition into one temporary
            // partition, and that clone -- not the live partition runSinglePartitionFlow would resolve
            // for itself -- is what the load writes.
            PreSplitTargets.EligibleTarget target = resolveSoleTemporaryTarget(database, table, partitionScope);
            if (target != null) {
                runSinglePartitionFlow(target, prepared, loadKind, shouldAbort);
            }
        } else if (Boolean.TRUE.equals(table.supportedAutomaticPartition())) {
            // Manually list/range-partitioned targets are still skipped: the multi-partition flow
            // pre-creates partitions from sampled values, which it may not do outside a user-defined
            // partition set.
            runMultiPartitionFlow(database, table, prepared, loadKind, shouldAbort, context,
                    -1L, partitionScope);
        }
    }

    /**
     * Derived-tier route for a static overwrite that refreshes an incremental materialized view: the
     * boundaries come from the hidden row-id key's own domain, so nothing is sampled and the flow needs
     * no {@link Prepared} bundle.
     */
    static void runStaticOverwriteMaterializedViewFlow(Database database, OlapTable table,
                                                       PreSplitPartitionScope partitionScope, Estimates estimates,
                                                       ComputeResource computeResource,
                                                       BooleanSupplier shouldAbort) {
        if (!partitionScope.isSpecified() || !partitionScope.isTemporary()) {
            return;
        }
        PreSplitTargets.EligibleTarget target = resolveSoleTemporaryTarget(database, table, partitionScope);
        if (target == null) {
            return;
        }
        // Re-check the shape against the RESOLVED targets, not just against the table the caller looked at.
        // Derivability was established before this target was resolved under its own lock, so a rollup that
        // became visible in between would appear here as a second index target -- and the derived source
        // expresses its cuts in whatever each target's first sort-key column is. A numeric key on that
        // rollup would take row-id boundaries and pass type and ordering validation, so the split would be
        // submitted against a domain the arithmetic never described.
        if (!MaterializedViewRowIdBoundaries.hasSoleRowIdIndexTarget(target.indexTargets())) {
            PreSplitMetrics.recordEligibilitySkip(SkipReason.MATERIALIZED_VIEW_TARGET);
            return;
        }
        PreSplitProfile.recordTargetPartitions(1L);
        int activeComputeNodeCount = TabletReshardUtils.computeNodeCount(computeResource);
        DerivedBoundarySource boundarySource =
                MaterializedViewRowIdBoundaries.sourceFor(table, estimates, activeComputeNodeCount);
        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forDerivedBoundaries(
                target.database(), target.olapTable(), target.indexTargets(), estimates.totalBytes(),
                LoadKind.MV_REFRESH, computeResource, boundarySource);
        submitAndAwaitSinglePartition(target, pipeline, new MaterializedViewRowIdBoundaries.RowIdScanContext(),
                LoadKind.MV_REFRESH, activeComputeNodeCount, shouldAbort);
    }

    /**
     * Resolves the one temporary partition an overwrite writes into a single-partition target. Assumes
     * the caller has already established that {@code partitionScope} names temporary partitions.
     *
     * <p>A scope naming any other number of them yields {@code null}. For the derived materialized-view
     * route that limit is load-bearing: its cuts span the whole row-id space, but a partitioned target's
     * row ids form a contiguous band per partition whenever the scan order correlates with the partition
     * key, so full-span equal-width cuts would leave most tablets of most partitions empty and one of
     * each overloaded. For a sampled target the reason is simply that carving several partitions at once
     * is the multi-partition flow's job, not this one's.
     *
     * @return the resolved target, or {@code null} when the scope does not name exactly one partition or
     *         that partition is not eligible (which records its own {@link SkipReason}).
     */
    private static PreSplitTargets.EligibleTarget resolveSoleTemporaryTarget(
            Database database, OlapTable table, PreSplitPartitionScope partitionScope) {
        List<String> temporaryPartitionNames = partitionScope.catalogPartitionNames();
        if (temporaryPartitionNames.size() != 1) {
            return null;
        }
        return PreSplitTargets.findEligibleTemporaryTarget(database, table, temporaryPartitionNames.get(0));
    }

    /**
     * Submits one resolved single-partition target and, if the reshard job was admitted, waits for it
     * fail-safely. Shared by the sampled and derived routes, which differ only in the pipeline and the
     * scan context they hand over.
     */
    private static void submitAndAwaitSinglePartition(
            PreSplitTargets.EligibleTarget target, DefaultPreSplitPipeline pipeline, ScanContext scanContext,
            LoadKind loadKind, int activeComputeNodeCount, BooleanSupplier shouldAbort) {
        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitAsynchronously(
                target.database(), target.olapTable(), target.partitionId(), scanContext,
                loadKind, pipeline, activeComputeNodeCount);
        LOG.info("Sample-Based Tablet Pre-Split ({}) outcome for table {}: {}",
                loadKind, target.olapTable().getName(), outcome);
        PreSplitProfile.recordOutcome(outcome);
        if (outcome instanceof PreSplitOutcome.Submitted submitted) {
            if (submitted.preparedJob().payload() instanceof TabletReshardJob reshardJob) {
                PreSplitProfile.recordReshardJobId(reshardJob.getJobId());
            }
            TabletPreSplitCoordinator.awaitFinishedAllowingFallback(
                    loadKind, target.olapTable(), pipeline, submitted.preparedJob(), shouldAbort);
        }
    }

    private static void runMultiPartitionFlow(
            Database database, OlapTable table, Prepared prepared, LoadKind loadKind,
            BooleanSupplier shouldAbort, ConnectContext context, long overwriteTransactionId,
            PreSplitPartitionScope partitionScope) {
        int activeComputeNodeCount = TabletReshardUtils.computeNodeCount(prepared.computeResource());
        // Try the meta tier first (row-group footer statistics, no data scan), mirroring the
        // single-partition flow's meta-tier-first routing; fall back to the exact data tier for
        // any shape the footer path cannot serve.
        SampleSet samples = runMetaTierMultiPartitionSampler(table, prepared, loadKind);
        if (samples != null) {
            PreSplitProfile.recordSample(samples);
            LOG.info("Sample-Based Tablet Pre-Split ({}, multi-partition) served by META tier "
                    + "(row-group footer stats, no data scan) for table {}", loadKind, table.getName());
        } else {
            samples = runDataTierSampler(table, prepared, loadKind);
        }
        if (samples == null) {
            PreSplitProfile.recordOutcome("SKIPPED: SAMPLE_FAILED");
            return;
        }
        // The authoritative secondary index-id set the sampler projected. The grouper drops any
        // partition whose currently-resolved rollup set differs, and the coordinator re-checks the
        // same set immediately before planning each partition.
        Set<Long> sampledSecondaryIndexMetaIds = new HashSet<>(samples.getSecondaryIndexMetaIds());
        long sampledInputBytes = samples.getEstimates().totalBytes();
        List<PartitionSamples> groups;
        try (PreSplitProfile.Scope ignored = PreSplitProfile.startPhase(
                PreSplitProfile.Phase.PARTITION_AND_BOUNDARY_PLANNING)) {
            groups = overwriteTransactionId > 0
                    ? PartitionSampleGrouper.groupTemporary(
                            samples, table, context, database.getId(), sampledInputBytes,
                            sampledSecondaryIndexMetaIds, overwriteTransactionId)
                    : partitionScope.isSpecified()
                            ? PartitionSampleGrouper.groupSpecified(
                                    samples, table, context, database.getId(), sampledInputBytes,
                                    sampledSecondaryIndexMetaIds, partitionScope)
                            : PartitionSampleGrouper.group(
                                    samples, table, context, database.getId(), sampledInputBytes,
                                    sampledSecondaryIndexMetaIds);
        }
        PreSplitProfile.recordTargetPartitions(groups.size());
        if (groups.isEmpty()) {
            PreSplitProfile.recordOutcome("SKIPPED: GROUPER_EMPTY");
            return;
        }
        PreSplitOutcome outcome = overwriteTransactionId > 0
                ? TabletPreSplitCoordinator.submitForTemporaryPartitionsCombined(
                        database, table, groups, activeComputeNodeCount, context, prepared.computeResource(),
                        sampledSecondaryIndexMetaIds, overwriteTransactionId)
                : partitionScope.isTemporary()
                        ? TabletPreSplitCoordinator.submitForExistingTemporaryPartitionsCombined(
                                database, table, groups, activeComputeNodeCount, context, prepared.computeResource(),
                                sampledSecondaryIndexMetaIds)
                        : TabletPreSplitCoordinator.submitForPartitionsCombined(
                                database, table, groups, activeComputeNodeCount, context, prepared.computeResource(),
                                sampledSecondaryIndexMetaIds);
        LOG.info("Sample-Based Tablet Pre-Split ({}, multi-partition) outcome for table {}: {}",
                loadKind, table.getName(), outcome);
        PreSplitProfile.recordOutcome(outcome);
        if (outcome instanceof PreSplitOutcome.SubmittedCombined submittedCombined) {
            PreSplitProfile.recordReshardJobId(submittedCombined.combinedJob().getJobId());
            try {
                TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                        loadKind, table, submittedCombined.combinedJob(), shouldAbort);
            } finally {
                // The await is fail-safe: on timeout / abort it returns while the job may still be
                // pre-CLEANING, and the caller then goes on to plan and write with the very
                // transaction CLEANING was told to ignore. Revoking the exclusion here keeps the
                // watermark wait honest for exactly the window the transaction was idle -- worst
                // case CLEANING now waits for the load, which is the pre-existing behaviour for
                // every other in-flight transaction.
                submittedCombined.combinedJob().clearCleanupExcludedTransactionIds();
            }
        }
    }

    static SampleSet runDataTierSampler(OlapTable table, Prepared prepared, LoadKind loadKind) {
        PreSplitProfile.recordSourceTier(DefaultPreSplitPipeline.TIER_LABEL_DATA_TIER);
        try (PreSplitProfile.Scope ignored = PreSplitProfile.startPhase(
                PreSplitProfile.Phase.SOURCE_SAMPLING)) {
            SampleRequest request = new SampleRequest(
                    prepared.scanContext(), prepared.sortKeyColumns(), prepared.secondaryIndexSpecs(),
                    prepared.partitionColumns(), Config.tablet_pre_split_sample_byte_limit, /*seed*/ 0L)
                    .withQueryTimeoutSeconds((int) Config.tablet_pre_split_pre_submit_timeout_seconds);
            Sampler sampler = new ReservoirSampler(DefaultPreSplitPipeline.sampleSubqueryExecutorFor(loadKind));
            SampleSet sampleSet = sampler.sample(request);
            PreSplitProfile.recordSample(sampleSet);
            return sampleSet;
        } catch (StarRocksException sampleFailure) {
            LOG.info("Pre-split skipped for table {}: data-tier sampling failed — {}",
                    table.getName(), sampleFailure.getMessage());
            PreSplitMetrics.recordSamplerFailed(SkipReason.SAMPLE_FAILED);
            return null;
        } catch (RuntimeException sampleFailure) {
            LOG.warn("Pre-split skipped for table {}: data-tier sampling errored — {}",
                    table.getName(), sampleFailure.getMessage());
            PreSplitMetrics.recordSamplerFailed(SkipReason.SAMPLE_FAILED);
            return null;
        }
    }

    /**
     * Meta-tier producer for the multi-partition flow, tried before the data tier (mirroring the
     * single-partition flow's meta-tier-first routing). Reads only Parquet row-group min/max
     * footer statistics (no data scan) and emits one synthetic sample per row-group endpoint, so
     * the existing {@link PartitionSampleGrouper} (partition attribution +
     * auto-create) and {@link TabletPreSplitCoordinator} (boundary planning + submission) consume
     * the result unchanged. The partition-source value for each endpoint is lifted out of the
     * sort-key tuple — the partition source column is part of the sort key for time-partitioned
     * tables — so the grouper applies the partition expression and buckets endpoints exactly as it
     * would per-row samples; a row group straddling a boundary contributes its min endpoint to the
     * lower partition and its max endpoint to the upper one. Boundaries stay full sort-key arity
     * (the footer min/max tuples span the whole sort key), so the BE's {@code
     * validate_new_tablet_ranges} accepts them just as it does data-tier boundaries.
     *
     * <p>Returns {@code null} — the caller then falls back to the exact data tier — for any shape
     * the footer path cannot serve: a load kind without Parquet footers, a rollup target
     * (secondary-index sort keys the footer path does not carry), a partition source column absent
     * from the sort key, or too few usable footer statistics.
     */
    static SampleSet runMetaTierMultiPartitionSampler(OlapTable table, Prepared prepared, LoadKind loadKind) {
        if (loadKind != LoadKind.INSERT_FROM_FILES) {
            return null;
        }
        if (!prepared.secondaryIndexSpecs().isEmpty()) {
            return null;
        }
        List<Column> sortKeyColumns = prepared.sortKeyColumns();
        List<Column> partitionSourceColumns = prepared.partitionColumns();
        if (sortKeyColumns.isEmpty() || partitionSourceColumns.isEmpty()) {
            return null;
        }
        int[] partitionSourceIndexInSortKey = new int[partitionSourceColumns.size()];
        for (int i = 0; i < partitionSourceColumns.size(); i++) {
            int indexInSortKey = indexOfColumnByName(sortKeyColumns, partitionSourceColumns.get(i).getName());
            if (indexInSortKey < 0) {
                // Partition source column is not part of the sort key, so its per-row-group value is
                // absent from the min/max tuple: the footer path cannot attribute row groups to
                // partitions. Let the data tier (which projects partition source columns) handle it.
                return null;
            }
            partitionSourceIndexInSortKey[i] = indexInSortKey;
        }
        // Capability checks above do not touch the source. Once footer fetching starts, retain the
        // attempted tier even if unusable statistics force a data-tier fallback.
        PreSplitProfile.recordSourceTier(DefaultPreSplitPipeline.TIER_LABEL_META_TIER);
        try (PreSplitProfile.Scope ignored = PreSplitProfile.startPhase(
                PreSplitProfile.Phase.SOURCE_SAMPLING)) {
            SampleRequest request = new SampleRequest(
                    prepared.scanContext(), sortKeyColumns, prepared.secondaryIndexSpecs(),
                    partitionSourceColumns, Config.tablet_pre_split_sample_byte_limit, /*seed*/ 0L)
                    .withQueryTimeoutSeconds((int) Config.tablet_pre_split_pre_submit_timeout_seconds);
            List<RowGroupStatistics> rowGroups = new InsertFromFilesRowGroupStatisticsProvider().fetch(request);
            if (rowGroups == null || rowGroups.isEmpty()) {
                return null;
            }
            long totalRows = 0L;
            List<RowGroupStatistics> usableRowGroups = new ArrayList<>(rowGroups.size());
            for (RowGroupStatistics rowGroup : rowGroups) {
                if (rowGroup == null || rowGroup.getRowCount() <= 0L) {
                    // No rows -> nothing to place a boundary from; safe to skip (empty / all-null group).
                    continue;
                }
                if (rowGroup.isTruncated() || rowGroup.getMinTuple() == null || rowGroup.getMaxTuple() == null) {
                    // A row group that HAS rows but no usable min/max would leave those rows
                    // unrepresented in the boundaries (they could fall into an unsplit or mis-sized
                    // partition). Fall back to the data tier -- matching the single-partition meta tier,
                    // which treats any missing stats on a non-empty row group as meta-unavailable.
                    return null;
                }
                usableRowGroups.add(rowGroup);
                totalRows += rowGroup.getRowCount();
            }
            if (usableRowGroups.size() < 2) {
                return null;
            }
            // Fall back to the data tier when the row groups' sort-key [min,max] ranges overlap too
            // much: min/max endpoints then cannot place interior boundaries. This happens when the
            // source is not ordered by the sort key -- every row group spans nearly the whole range,
            // so the endpoints collapse to a couple of values and the boundary planner produces few,
            // uneven tablets. Mirrors the single-partition meta tier's overlap gate
            // (ParquetMetadataSampler + Config.tablet_pre_split_meta_tier_overlap_threshold).
            double overlapFraction = rowGroupOverlapFraction(usableRowGroups);
            if (overlapFraction > Config.tablet_pre_split_meta_tier_overlap_threshold) {
                LOG.info("Pre-split meta tier (multi-partition) row-group overlap {} > threshold {} for "
                                + "table {} (source likely unordered by sort key); falling back to data tier",
                        overlapFraction, Config.tablet_pre_split_meta_tier_overlap_threshold, table.getName());
                return null;
            }
            List<Tuple> sortKeyTuples = new ArrayList<>();
            List<Tuple> partitionSourceTuples = new ArrayList<>();
            for (RowGroupStatistics rowGroup : usableRowGroups) {
                // Weight each group's endpoints by its share of total rows so the boundary planner's
                // row-quantiles reflect row DENSITY, not row-group count (the data tier samples actual
                // rows, density-weighted by construction; unweighted endpoints let a 1M-row group and a
                // 100-row group each contribute the same two points, under-splitting the dense region).
                // >= 1 so every group still contributes both ends; capped so one dominant group cannot
                // balloon the FE-side sample, keeping the total pair count bounded regardless of skew.
                long weight = Math.min(META_MAX_ENDPOINT_WEIGHT, Math.max(1L, Math.round(
                        (double) rowGroup.getRowCount() / totalRows * META_WEIGHTED_ENDPOINT_BUDGET)));
                for (long copy = 0; copy < weight; copy++) {
                    addRowGroupEndpoint(rowGroup.getMinTuple(), partitionSourceIndexInSortKey,
                            sortKeyTuples, partitionSourceTuples);
                    addRowGroupEndpoint(rowGroup.getMaxTuple(), partitionSourceIndexInSortKey,
                            sortKeyTuples, partitionSourceTuples);
                }
            }
            if (sortKeyTuples.size() < 2) {
                // Too few usable endpoints to place any interior boundary; let the data tier try.
                return null;
            }
            return new SampleSet(sortKeyTuples, partitionSourceTuples,
                    new Estimates(prepared.estimatedBytes(), 0L));
        } catch (StarRocksException | RuntimeException metaTierFailure) {
            LOG.info("Pre-split meta tier (multi-partition) unavailable for table {}; falling back to "
                    + "data tier: {}", table.getName(), metaTierFailure.getMessage());
            return null;
        }
    }

    /**
     * Appends one synthetic sample built from a row-group endpoint tuple: the full sort-key tuple
     * plus the partition-source values lifted out of it by their sort-key positions. The two lists
     * grow in lock-step so {@code sortKeyTuples} and {@code partitionSourceTuples} stay parallel,
     * as {@link SampleSet} requires.
     */
    private static void addRowGroupEndpoint(Tuple endpointTuple, int[] partitionSourceIndexInSortKey,
                                            List<Tuple> sortKeyTuples, List<Tuple> partitionSourceTuples) {
        if (endpointTuple == null) {
            return;
        }
        List<Variant> sortKeyValues = endpointTuple.getValues();
        List<Variant> partitionSourceValues = new ArrayList<>(partitionSourceIndexInSortKey.length);
        for (int indexInSortKey : partitionSourceIndexInSortKey) {
            partitionSourceValues.add(sortKeyValues.get(indexInSortKey));
        }
        sortKeyTuples.add(endpointTuple);
        partitionSourceTuples.add(new Tuple(partitionSourceValues));
    }

    private static int indexOfColumnByName(List<Column> columns, String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Fraction of row groups (sorted by sort-key min) whose min falls below the running max of the
     * preceding groups — i.e. how much the row groups' sort-key ranges overlap. Near 0 means the
     * source is well ordered by the sort key (tight, non-overlapping min/max, so endpoint boundaries
     * are meaningful); near 1 means it is not (every group spans nearly the whole range, so min/max
     * endpoints cannot place interior boundaries and the caller falls back to the data tier). Same
     * definition the single-partition meta tier uses in {@link ParquetMetadataSampler}.
     * Package-private so {@code PreSplitFlowTest} can assert the fraction directly.
     */
    static double rowGroupOverlapFraction(List<RowGroupStatistics> rowGroups) {
        List<RowGroupStatistics> sorted = new ArrayList<>(rowGroups);
        sorted.sort(Comparator.comparing(RowGroupStatistics::getMinTuple));
        Tuple maxSeen = sorted.get(0).getMaxTuple();
        int overlapping = 0;
        for (int i = 1; i < sorted.size(); i++) {
            RowGroupStatistics current = sorted.get(i);
            if (current.getMinTuple().compareTo(maxSeen) < 0) {
                overlapping++;
            }
            if (current.getMaxTuple().compareTo(maxSeen) > 0) {
                maxSeen = current.getMaxTuple();
            }
        }
        return (double) overlapping / (sorted.size() - 1);
    }
}
