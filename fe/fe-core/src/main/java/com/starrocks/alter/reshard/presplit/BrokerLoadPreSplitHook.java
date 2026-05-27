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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.planner.LoadScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * BrokerLoadJob → coordinator bridge for Sample-Based Tablet Pre-Split on the
 * Broker Load path.
 *
 * <p>The hook is invoked from {@code BrokerLoadJob.createLoadingTask} after
 * the per-table {@link OlapTable} is resolved and the file statuses are
 * known. The triggering load's {@code LoadLoadingTask.prepare()} has ALREADY
 * built the sink plan against the pre-hook tablet layout by the time the
 * hook fires, so pre-created partitions and post-reshard tablet layouts are
 * invisible to the triggering Broker Load — they accelerate only SUBSEQUENT
 * loads on the same table. The triggering load goes through BE runtime
 * auto-create as today.
 *
 * <p>The hook is fire-and-forget: unlike the INSERT-from-FILES hook (which
 * runs before the planner and can synchronously await FINISHED), the Broker
 * Load txn is already open at this point. A synchronous wait for the reshard
 * daemon would deadlock its cleanup-phase prev-txn wait against this same
 * load txn, so we never call {@code awaitCombinedJobAllowingFallback} on the
 * Broker Load path.
 *
 * <p>Sampler-executor selection is delegated to
 * {@link DefaultPreSplitPipeline#forLoadKind}: meta tier uses
 * {@link BrokerLoadRowGroupStatisticsProvider}, data tier uses
 * {@link BrokerLoadSampleSubqueryExecutor}. The per-path Config flag
 * {@code enable_tablet_pre_split_for_broker_load} defaults to
 * {@code true} as of v4.1.0 (GA flip); set it to {@code false} to disable
 * cluster-wide. The session variable {@code enable_tablet_pre_split}
 * (also default {@code true}) provides a per-session opt-out checked
 * early in this hook so a session-opt-out load does not pay the
 * eligibility-target walk and scan-context build.
 */
public final class BrokerLoadPreSplitHook {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadPreSplitHook.class);

    private BrokerLoadPreSplitHook() {
    }

    /**
     * Entry point invoked from {@code BrokerLoadJob.createLoadingTask}.
     *
     * <p>The method is fully self-contained: any throw is swallowed and the
     * load proceeds without pre-split. Failing here must not abort an
     * otherwise-valid load.
     *
     * @param context      the load's {@link ConnectContext}. Mirrors the
     *                     {@link InsertFromFilesPreSplitHook} parameter
     *                     threading: passing the context explicitly avoids
     *                     a thread-local fallback that would create an
     *                     uninitialized {@link ConnectContext} (no auth /
     *                     no session vars / no current DB) and surface as
     *                     a confusing analyze-time NPE inside
     *                     {@link PartitionSampleGrouper}.
     * @param fileStatuses nested per-file-group file statuses from
     *                     {@code BrokerPendingTaskAttachment.getFileStatusByTable}.
     */
    public static void maybeRunPreSplit(
            ConnectContext context, Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource) {
        try {
            tryRunPreSplit(context, database, targetTable, brokerDesc, fileGroups, fileStatuses, computeResource);
        } catch (Throwable unexpected) {
            LOG.warn("Sample-Based Tablet Pre-Split hook failed for Broker Load; proceeding without pre-split",
                    unexpected);
        }
    }

    private static void tryRunPreSplit(
            ConnectContext context, Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource) {
        Objects.requireNonNull(context, "context");
        if (!Config.enable_tablet_pre_split_for_broker_load) {
            return;
        }
        // Honor the per-session opt-out before target resolution + scan-context
        // build. The caller (BrokerLoadJob.firePreSplitHooks) passes the load's
        // ConnectContext directly so we read the load's session variable rather
        // than relying on thread-local state. The helper bumps the
        // disabled_by_session bvar — the coordinator never sees this skip, but
        // operators still need the bvar.
        if (PreSplitMetrics.shortCircuitOnSessionOptOut(context.getSessionVariable())) {
            return;
        }
        // BrokerLoadJob.createLoadingTask guarantees database / targetTable / computeResource are
        // non-null by the time the hook fires; fileGroups / fileStatuses come from the attachment
        // and the pending-task contract makes them non-null too, but we tolerate null defensively.
        if (fileGroups == null || fileStatuses == null) {
            return;
        }
        // Table-level eligibility: structural checks shared with the multi-partition
        // coordinator's defensive re-check. Per-partition checks (single physical
        // partition, single base tablet, empty partition) remain with the legacy
        // single-partition path; the multi-partition path runs them per-bucket
        // after pre-create under its own short READ lock.
        SkipReason tableLevelSkip = PreSplitTargets.findEligibleTable(database, targetTable);
        if (tableLevelSkip != null) {
            PreSplitMetrics.recordEligibilitySkip(tableLevelSkip);
            return;
        }
        // Branch on partitioned vs unpartitioned, mirroring the INSERT-from-FILES hook
        // (see InsertFromFilesPreSplitHook#tryRunPreSplit). Partitioned tables go
        // through the multi-partition flow (data tier sampler → grouper → combined
        // submit, NO await); unpartitioned tables keep the legacy single-partition
        // path (behavior unchanged).
        if (targetTable.getPartitionInfo().isPartitioned()) {
            runMultiPartitionFlow(context, database, targetTable, brokerDesc, fileGroups, fileStatuses, computeResource);
        } else {
            runSinglePartitionFlow(database, targetTable, brokerDesc, fileGroups, fileStatuses, computeResource);
        }
    }

    /**
     * Legacy single-partition path: resolve the unique partition + base tablet,
     * then go through {@link DefaultPreSplitPipeline} + {@link TabletPreSplitCoordinator#submitAsynchronously}.
     * Unpartitioned-table behavior is unchanged; the Broker Load path
     * has always been fire-and-forget here (no await wrapper).
     */
    private static void runSinglePartitionFlow(
            Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource) {
        PreSplitTargets.EligibleTarget target = PreSplitTargets.findEligibleTarget(database, targetTable);
        if (target == null) {
            return;
        }
        submitToCoordinator(target, brokerDesc, fileGroups, fileStatuses, computeResource);
    }

    /**
     * Multi-partition path: sample the load's input via the data tier,
     * group sample rows by predicted partition value, pre-create missing
     * partitions, and submit ONE combined reshard via
     * {@link TabletPreSplitCoordinator#submitForPartitionsCombined}.
     *
     * <p><b>No await</b>: unlike the INSERT-from-FILES hook, the Broker Load
     * hook fires AFTER {@code LoadLoadingTask.prepare()} builds the sink plan,
     * and the load txn is already open. A synchronous wait would deadlock the
     * reshard daemon's cleanup-phase prev-txn wait against this same load txn.
     * Pre-created partitions and the post-reshard layout are visible only to
     * subsequent Broker Loads on the same partitions.
     */
    private static void runMultiPartitionFlow(
            ConnectContext context, Database database, OlapTable targetTable, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource) {
        int activeComputeNodeCount = Math.max(1,
                LoadScanNode.getAvailableComputeNodes(computeResource).size());
        long fileTotalBytes = sumFileBytes(fileStatuses);
        BrokerLoadScanContext scanContext = new BrokerLoadScanContext(
                brokerDesc, fileGroups, fileStatuses, computeResource);

        SampleSet samples = runDataTierSampler(scanContext, targetTable);
        if (samples == null) {
            return;
        }

        List<PartitionSamples> groups = PartitionSampleGrouper.group(
                samples, targetTable, context, database.getId(), fileTotalBytes);
        if (groups.isEmpty()) {
            // Grouper already recorded the skip reason bvar.
            return;
        }

        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                database, targetTable, groups, activeComputeNodeCount, context);
        LOG.info("Sample-Based Tablet Pre-Split (multi-partition) outcome for Broker Load on table {}: {}",
                targetTable.getName(), outcome);
        // NO awaitCombinedJobAllowingFallback — Broker Load is fire-and-forget.
        // The triggering load's sink plan was already built by task.prepare()
        // before this hook fires; the daemon drives the combined job to FINISHED
        // asynchronously for subsequent loads to observe.
    }

    /**
     * Run the data-tier sampler directly for the Broker Load multi-partition
     * flow. Mirrors {@code InsertFromFilesPreSplitHook#runDataTierSampler}:
     * the meta tier (Parquet row-group statistics) is fundamentally lossy
     * under expression-based partitioning, so the multi-partition flow always
     * uses the data tier. Sort-key columns drive boundary planning; partition-source
     * columns let the grouper project per-row partition values for bucketing.
     *
     * @return the sampled rows, or {@code null} when the sampler failed
     *         (caller no-ops; bvar recorded inline).
     */
    private static SampleSet runDataTierSampler(BrokerLoadScanContext scanContext, OlapTable targetTable) {
        try {
            List<Column> sortKey = MetaUtils.getRangeDistributionColumns(targetTable);
            List<Column> partitionSourceColumns =
                    targetTable.getPartitionInfo().getPartitionColumns(targetTable.getIdToColumn());
            SampleRequest request = new SampleRequest(
                    scanContext, sortKey, partitionSourceColumns,
                    Config.tablet_pre_split_sample_byte_limit, /*seed*/ 0L);
            Sampler sampler = new ReservoirSampler(new BrokerLoadSampleSubqueryExecutor());
            return sampler.sample(request);
        } catch (StarRocksException sampleFailure) {
            LOG.info("Pre-split skipped for Broker Load on table {}: data-tier sampling failed — {}",
                    targetTable.getName(), sampleFailure.getMessage());
            PreSplitMetrics.recordSamplerFailed(SkipReason.SAMPLE_FAILED);
            return null;
        } catch (RuntimeException sampleFailure) {
            LOG.warn("Pre-split skipped for Broker Load on table {}: data-tier sampling errored — {}",
                    targetTable.getName(), sampleFailure.getMessage());
            PreSplitMetrics.recordSamplerFailed(SkipReason.SAMPLE_FAILED);
            return null;
        }
    }

    private static void submitToCoordinator(
            PreSplitTargets.EligibleTarget target, BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatuses,
            ComputeResource computeResource) {
        BrokerLoadScanContext scanContext = new BrokerLoadScanContext(
                brokerDesc, fileGroups, fileStatuses, computeResource);
        int activeComputeNodeCount = Math.max(1,
                LoadScanNode.getAvailableComputeNodes(computeResource).size());
        long fileTotalBytes = sumFileBytes(fileStatuses);

        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                target.database(), target.olapTable(), target.oldTabletId(), fileTotalBytes,
                LoadKind.BROKER_LOAD);

        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitAsynchronously(
                target.database(), target.olapTable(), target.partitionId(), scanContext,
                LoadKind.BROKER_LOAD, pipeline, activeComputeNodeCount);
        LOG.info("Sample-Based Tablet Pre-Split outcome for Broker Load on table {}: {}",
                target.olapTable().getName(), outcome);
    }

    private static long sumFileBytes(List<List<TBrokerFileStatus>> fileStatuses) {
        long total = 0L;
        for (List<TBrokerFileStatus> fileGroupStatuses : fileStatuses) {
            if (fileGroupStatuses == null) {
                continue;
            }
            for (TBrokerFileStatus fileStatus : fileGroupStatuses) {
                if (fileStatus != null) {
                    total += fileStatus.size;
                }
            }
        }
        return total;
    }
}
