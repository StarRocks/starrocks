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
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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
 *       sampled values), and the INSERT-from-table rollup descope (a rollup's sort
 *       key cannot be remapped to source columns, so a multi-index target skips
 *       before sampling for that load kind only).</li>
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

    private PreSplitFlow() {
    }

    /**
     * Source-resolved inputs the flow needs. sortKeyColumns / partitionColumns are TARGET
     * columns (boundary planning + per-row partition projection); estimatedBytes sizes the
     * requested tablet count; computeResource sizes the active CN count; scanContext carries
     * the source-specific scan inputs; secondaryIndexSpecs names every OTHER visible index
     * (rollup) whose sort key the multi-partition data-tier sampler should project alongside
     * the base sort key -- empty for single-index targets and for INSERT-from-table (descoped).
     */
    record Prepared(ScanContext scanContext, List<Column> sortKeyColumns,
                    List<Column> partitionColumns, long estimatedBytes,
                    ComputeResource computeResource, List<SecondaryIndexSpec> secondaryIndexSpecs) {

        /**
         * Backward-compatible constructor for callers with no rollup to project;
         * defaults secondaryIndexSpecs to empty.
         */
        Prepared(ScanContext scanContext, List<Column> sortKeyColumns,
                List<Column> partitionColumns, long estimatedBytes, ComputeResource computeResource) {
            this(scanContext, sortKeyColumns, partitionColumns, estimatedBytes, computeResource, List.of());
        }
    }

    static void dispatch(Database database, OlapTable target, Prepared prepared,
                         LoadKind loadKind, BooleanSupplier shouldAbort, ConnectContext context) {
        if (loadKind == LoadKind.INSERT_FROM_TABLE && target.getVisibleIndexMetas().size() > 1) {
            // INSERT-from-table cannot remap a divergent rollup sort key to source columns (descoped).
            PreSplitMetrics.recordEligibilitySkip(SkipReason.HAS_MATERIALIZED_VIEW_OR_ROLLUP);
            return;
        }
        if (target.getPartitionInfo().isPartitioned()) {
            // Manually list/range-partitioned targets do not support pre-creating partitions
            // from sampled values; skip conservatively, let the load proceed.
            if (!Boolean.TRUE.equals(target.supportedAutomaticPartition())) {
                return;
            }
            runMultiPartitionFlow(database, target, prepared, loadKind, shouldAbort, context);
        } else {
            runSinglePartitionFlow(database, target, prepared, loadKind, shouldAbort);
        }
    }

    static void runSinglePartitionFlow(Database database, OlapTable table, Prepared prepared,
                                       LoadKind loadKind, BooleanSupplier shouldAbort) {
        PreSplitTargets.EligibleTarget target = PreSplitTargets.findEligibleTarget(database, table);
        if (target == null) {
            return;
        }
        if (loadKind == LoadKind.INSERT_FROM_TABLE && target.indexTargets().size() > 1) {
            // Authoritative re-check on the resolved index set: a rollup that became visible after the
            // dispatch-time descope gate must not be sampled with the base-only INSERT-from-table source
            // mapping. Skip pre-split (load proceeds) -- INSERT-from-table with a rollup is out of scope.
            PreSplitMetrics.recordEligibilitySkip(SkipReason.HAS_MATERIALIZED_VIEW_OR_ROLLUP);
            return;
        }
        int activeComputeNodeCount = TabletReshardUtils.computeNodeCount(prepared.computeResource());
        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                target.database(), target.olapTable(), target.indexTargets(), prepared.estimatedBytes(), loadKind,
                prepared.computeResource());
        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitAsynchronously(
                target.database(), target.olapTable(), target.partitionId(), prepared.scanContext(),
                loadKind, pipeline, activeComputeNodeCount);
        LOG.info("Sample-Based Tablet Pre-Split ({}) outcome for table {}: {}",
                loadKind, target.olapTable().getName(), outcome);
        if (outcome instanceof PreSplitOutcome.Submitted submitted) {
            TabletPreSplitCoordinator.awaitFinishedAllowingFallback(
                    loadKind, target.olapTable(), pipeline, submitted.preparedJob(), shouldAbort);
        }
    }

    static void runMultiPartitionFlow(Database database, OlapTable table, Prepared prepared,
                                      LoadKind loadKind, BooleanSupplier shouldAbort, ConnectContext context) {
        int activeComputeNodeCount = TabletReshardUtils.computeNodeCount(prepared.computeResource());
        // Try the meta tier first (row-group footer statistics, no data scan), mirroring the
        // single-partition flow's meta-tier-first routing; fall back to the exact data tier for
        // any shape the footer path cannot serve.
        SampleSet samples = runMetaTierMultiPartitionSampler(table, prepared, loadKind);
        if (samples != null) {
            LOG.info("Sample-Based Tablet Pre-Split ({}, multi-partition) served by META tier "
                    + "(row-group footer stats, no data scan) for table {}", loadKind, table.getName());
        } else {
            samples = runDataTierSampler(table, prepared, loadKind);
        }
        if (samples == null) {
            return;
        }
        // The authoritative secondary index-id set the sampler projected. The grouper drops any
        // partition whose currently-resolved rollup set differs, and the coordinator re-checks the
        // same set immediately before planning each partition.
        Set<Long> sampledSecondaryIndexMetaIds = new HashSet<>(samples.getSecondaryIndexMetaIds());
        List<PartitionSamples> groups = PartitionSampleGrouper.group(
                samples, table, context, database.getId(), prepared.estimatedBytes(),
                sampledSecondaryIndexMetaIds);
        if (groups.isEmpty()) {
            return;
        }
        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                database, table, groups, activeComputeNodeCount, context, prepared.computeResource(),
                sampledSecondaryIndexMetaIds);
        LOG.info("Sample-Based Tablet Pre-Split ({}, multi-partition) outcome for table {}: {}",
                loadKind, table.getName(), outcome);
        if (outcome instanceof PreSplitOutcome.SubmittedCombined submittedCombined) {
            TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                    loadKind, table, submittedCombined.combinedJob(), shouldAbort);
        }
    }

    static SampleSet runDataTierSampler(OlapTable table, Prepared prepared, LoadKind loadKind) {
        try {
            SampleRequest request = new SampleRequest(
                    prepared.scanContext(), prepared.sortKeyColumns(), prepared.secondaryIndexSpecs(),
                    prepared.partitionColumns(), Config.tablet_pre_split_sample_byte_limit, /*seed*/ 0L)
                    .withQueryTimeoutSeconds((int) Config.tablet_pre_split_pre_submit_timeout_seconds);
            Sampler sampler = new ReservoirSampler(DefaultPreSplitPipeline.sampleSubqueryExecutorFor(loadKind));
            return sampler.sample(request);
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
        try {
            SampleRequest request = new SampleRequest(
                    prepared.scanContext(), sortKeyColumns, prepared.secondaryIndexSpecs(),
                    partitionSourceColumns, Config.tablet_pre_split_sample_byte_limit, /*seed*/ 0L)
                    .withQueryTimeoutSeconds((int) Config.tablet_pre_split_pre_submit_timeout_seconds);
            List<RowGroupStatistics> rowGroups = new InsertFromFilesRowGroupStatisticsProvider().fetch(request);
            if (rowGroups == null || rowGroups.isEmpty()) {
                return null;
            }
            List<Tuple> sortKeyTuples = new ArrayList<>();
            List<Tuple> partitionSourceTuples = new ArrayList<>();
            for (RowGroupStatistics rowGroup : rowGroups) {
                if (rowGroup == null || rowGroup.getRowCount() == 0L || rowGroup.isTruncated()) {
                    continue;
                }
                addRowGroupEndpoint(rowGroup.getMinTuple(), partitionSourceIndexInSortKey,
                        sortKeyTuples, partitionSourceTuples);
                addRowGroupEndpoint(rowGroup.getMaxTuple(), partitionSourceIndexInSortKey,
                        sortKeyTuples, partitionSourceTuples);
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
}
