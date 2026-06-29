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
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
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

    private PreSplitFlow() {
    }

    /**
     * Source-resolved inputs the flow needs. sortKeyColumns / partitionColumns are TARGET
     * columns (boundary planning + per-row partition projection); estimatedBytes sizes the
     * requested tablet count; computeResource sizes the active CN count; scanContext carries
     * the source-specific scan inputs.
     */
    record Prepared(ScanContext scanContext, List<Column> sortKeyColumns,
                    List<Column> partitionColumns, long estimatedBytes,
                    ComputeResource computeResource) {
    }

    static void dispatch(Database database, OlapTable target, Prepared prepared,
                         LoadKind loadKind, BooleanSupplier shouldAbort, ConnectContext context) {
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
        int activeComputeNodeCount = TabletReshardUtils.computeNodeCount(prepared.computeResource());
        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                target.database(), target.olapTable(), target.oldTabletId(), prepared.estimatedBytes(), loadKind,
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
        SampleSet samples = runDataTierSampler(table, prepared, loadKind);
        if (samples == null) {
            return;
        }
        List<PartitionSamples> groups = PartitionSampleGrouper.group(
                samples, table, context, database.getId(), prepared.estimatedBytes());
        if (groups.isEmpty()) {
            return;
        }
        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                database, table, groups, activeComputeNodeCount, context);
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
                    prepared.scanContext(), prepared.sortKeyColumns(), prepared.partitionColumns(),
                    Config.tablet_pre_split_sample_byte_limit, /*seed*/ 0L)
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
}
