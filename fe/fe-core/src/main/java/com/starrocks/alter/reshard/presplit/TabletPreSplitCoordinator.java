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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.TimeoutException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * FE-side orchestrator for Sample-Based Tablet Pre-Split.
 *
 * <p>{@link #maybeAct(Database, OlapTable, long, ScanContext, LoadKind)} is the
 * eligibility gate — every check that fails produces a specific
 * {@link SkipReason} for downstream bvar labels.
 * {@link #runPreSplit(Database, OlapTable, long, ScanContext, LoadKind, PreSplitPipeline, int)}
 * is the full entry point used by the integrating load path (INSERT-from-FILES,
 * Broker Load): it runs the eligibility gate, then drives the
 * {@link PreSplitPipeline} through the pre-submit / submit / post-submit
 * phases described in the design doc.
 */
public final class TabletPreSplitCoordinator {

    private static final Logger LOG = LogManager.getLogger(TabletPreSplitCoordinator.class);

    private TabletPreSplitCoordinator() {
    }

    /**
     * @param database             table's database (carried through for the submission stage).
     * @param table                target table.
     * @param physicalPartitionId  load-target physical partition.
     * @param scanContext          integration-point scan context (used by the sampling stage).
     * @param loadKind             which integration path is calling — picks the right
     *                             per-path FE Config flag for the eligibility gate.
     */
    public static PreSplitOutcome maybeAct(
            Database database, OlapTable table, long physicalPartitionId, ScanContext scanContext, LoadKind loadKind) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(scanContext, "scanContext");
        Objects.requireNonNull(loadKind, "loadKind");

        SkipReason gateReason = checkConfigAndSession(loadKind);
        if (gateReason != null) {
            return skipEligibility(gateReason);
        }
        if (!table.isRangeDistribution()) {
            return skipEligibility(SkipReason.NOT_RANGE_DISTRIBUTION);
        }
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            return skipEligibility(SkipReason.TABLE_NOT_NORMAL);
        }
        if (table.getVisibleIndexMetas().size() != 1) {
            return skipEligibility(SkipReason.HAS_MATERIALIZED_VIEW_OR_ROLLUP);
        }
        if (!isFirstSortKeyColumnSupported(table)) {
            return skipEligibility(SkipReason.UNSUPPORTED_SORT_KEY);
        }

        PhysicalPartition partition = table.getPhysicalPartition(physicalPartitionId);
        if (partition == null) {
            return skipEligibility(SkipReason.METADATA_NOT_RESOLVED);
        }
        MaterializedIndex baseIndex = partition.getIndex(table.getBaseIndexMetaId());
        if (baseIndex == null) {
            return skipEligibility(SkipReason.METADATA_NOT_RESOLVED);
        }
        if (baseIndex.getTablets().size() != 1) {
            return skipEligibility(SkipReason.MULTIPLE_BASE_INDEX_TABLETS);
        }
        if (baseIndex.getRowCount() > 0) {
            return skipEligibility(SkipReason.PARTITION_NOT_EMPTY);
        }

        return new PreSplitOutcome.Eligible();
    }

    /** Build a {@code Skipped} outcome and record the eligibility-skipped counter for the given reason. */
    private static PreSplitOutcome.Skipped skipEligibility(SkipReason reason) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(reason.name().toLowerCase()).increase(1L);
        }
        return new PreSplitOutcome.Skipped(reason);
    }

    /**
     * Picks the per-path Config flag that gates the caller's load kind, then checks the
     * session opt-out. Returns {@code null} when both gates are open.
     */
    private static SkipReason checkConfigAndSession(LoadKind loadKind) {
        boolean configEnabled = switch (loadKind) {
            case INSERT_FROM_FILES -> Config.enable_tablet_pre_split_for_insert_from_files;
            case BROKER_LOAD -> Config.enable_tablet_pre_split_for_broker_load;
        };
        if (!configEnabled) {
            return SkipReason.DISABLED_BY_CONFIG;
        }
        if (!ConnectContext.getSessionVariableOrDefault().isEnableTabletPreSplit()) {
            return SkipReason.DISABLED_BY_SESSION;
        }
        return null;
    }

    private static boolean isFirstSortKeyColumnSupported(OlapTable table) {
        List<Column> keyColumns = table.getKeyColumnsInOrder();
        if (keyColumns.isEmpty()) {
            return false;
        }
        // Deeper per-column validation (decimal precision/scale, primitive-type match against
        // sampler tuples) is BoundaryPlanner's job at plan time; this gate only needs to keep
        // composite/complex types out of the external-boundaries split path.
        return keyColumns.get(0).getType().isScalarType();
    }

    /**
     * Run eligibility, sample, plan, and admit the reshard job to
     * {@link com.starrocks.alter.reshard.TabletReshardJobMgr}, then return
     * immediately — the reshard daemon completes the split asynchronously.
     *
     * <p>This is the fire-and-forget entry point used by the integrating load
     * path. The synchronous-await variant is {@link #runPreSplit}; that one
     * cannot be used from inside the planner because the daemon's
     * {@code TABLET_RESHARD} write-lock acquisition would deadlock with the
     * planner's read-lock.
     *
     * <p>Failures are mapped to {@link PreSplitOutcome.Skipped} with a specific
     * {@link SkipReason} so the load proceeds against the original single tablet:
     * {@link SkipReason#TIMEOUT_PRE_SUBMIT}, {@link SkipReason#SAMPLE_FAILED},
     * {@link SkipReason#NO_USEFUL_CUTS}, {@link SkipReason#SUBMIT_FAILED}.
     *
     * @param activeComputeNodeCount compute nodes available to the load after
     *                               warehouse/blocklist filtering (passed to the
     *                               pipeline's internal tablet-count selector).
     */
    public static PreSplitOutcome submitAsynchronously(
            Database database, OlapTable table, long physicalPartitionId, ScanContext scanContext,
            LoadKind loadKind, PreSplitPipeline pipeline, int activeComputeNodeCount) {
        Objects.requireNonNull(pipeline, "pipeline");
        Preconditions.checkArgument(activeComputeNodeCount >= 1,
                "activeComputeNodeCount must be >= 1, was %s", activeComputeNodeCount);

        PreSplitOutcome eligibility = maybeAct(database, table, physicalPartitionId, scanContext, loadKind);
        if (!(eligibility instanceof PreSplitOutcome.Eligible)) {
            return eligibility;
        }

        SampleRequest sampleRequest = new SampleRequest(
                scanContext, table.getKeyColumnsInOrder(),
                Config.tablet_pre_split_sample_byte_limit, /*seed*/ 0L);
        Duration preSubmitTimeout = Duration.ofSeconds(Config.tablet_pre_split_pre_submit_timeout_seconds);

        Optional<PreSplitPipeline.PreparedReshardJob> prepared;
        long preSubmitStartMillis = System.currentTimeMillis();
        try {
            prepared = pipeline.preSubmit(sampleRequest, activeComputeNodeCount, preSubmitTimeout);
        } catch (PreSplitPreSubmitTimeoutException timeout) {
            LOG.info("Pre-split skipped for table {}: pre-submit phase exceeded {}s — {}",
                    table.getName(), preSubmitTimeout.toSeconds(), timeout.getMessage());
            return skipPostEligibility(SkipReason.TIMEOUT_PRE_SUBMIT);
        } catch (StarRocksException sampleFailure) {
            LOG.warn("Pre-split skipped for table {}: sampling/planning failed — {}",
                    table.getName(), sampleFailure.getMessage());
            return skipPostEligibility(SkipReason.SAMPLE_FAILED);
        } finally {
            if (MetricRepo.hasInit) {
                MetricRepo.HISTO_TABLET_PRE_SPLIT_PRE_SUBMIT_WAIT_MS.update(
                        System.currentTimeMillis() - preSubmitStartMillis);
            }
        }
        if (prepared.isEmpty()) {
            LOG.info("Pre-split skipped for table {}: planner found no useful cuts", table.getName());
            return new PreSplitOutcome.Skipped(SkipReason.NO_USEFUL_CUTS);
        }

        PreSplitPipeline.PreparedReshardJob preparedJob = prepared.get();
        try {
            pipeline.submit(preparedJob);
        } catch (StarRocksException submitFailure) {
            // Surfaces the structured error so operators can diagnose admission failures
            // (table-state changed, journal write rejected, job-id collision, etc.).
            LOG.warn("Pre-split skipped for table {}: TabletReshardJobMgr rejected admission — {}",
                    table.getName(), submitFailure.getMessage());
            return skipPostEligibility(SkipReason.SUBMIT_FAILED);
        }
        return new PreSplitOutcome.Submitted(preparedJob);
    }

    /**
     * Build a {@code Skipped} outcome for a post-eligibility failure (the sampler
     * attempted but errored / timed out / could not admit) and record the
     * sampler-failed counter under the reason label. Distinct from
     * {@link #skipEligibility} which records eligibility-gate rejections, where
     * the sampler never ran.
     */
    private static PreSplitOutcome.Skipped skipPostEligibility(SkipReason reason) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_TABLET_PRE_SPLIT_SAMPLER_FAILED
                    .getMetric(reason.name().toLowerCase()).increase(1L);
        }
        return new PreSplitOutcome.Skipped(reason);
    }

    /**
     * Submit pre-split and synchronously wait for FINISHED. Convenience entry
     * point retained for callers that want the full lifecycle (e.g. tests, or
     * future synchronous integrations). The integrating load path uses
     * {@link #submitAsynchronously} instead — running pre-split synchronously
     * while the planner holds metadata locks deadlocks with the reshard
     * daemon's table-state-transition write lock.
     */
    public static PreSplitOutcome runPreSplit(
            Database database, OlapTable table, long physicalPartitionId, ScanContext scanContext,
            LoadKind loadKind, PreSplitPipeline pipeline, int activeComputeNodeCount)
            throws PreSplitPostSubmitTimeoutException {
        PreSplitOutcome outcome = submitAsynchronously(database, table, physicalPartitionId, scanContext,
                loadKind, pipeline, activeComputeNodeCount);
        // submitAsynchronously only emits Skipped or Submitted — Finished is reached after the await below.
        if (!(outcome instanceof PreSplitOutcome.Submitted submitted)) {
            return outcome;
        }
        Duration postSubmitTimeout = Duration.ofSeconds(Config.tablet_pre_split_post_submit_wait_seconds);
        long postSubmitStartMillis = System.currentTimeMillis();
        try {
            pipeline.awaitFinished(submitted.preparedJob(), postSubmitTimeout);
        } catch (TimeoutException timeout) {
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_TABLET_PRE_SPLIT_POST_SUBMIT_HARD_CAP.increase(1L);
                MetricRepo.COUNTER_TABLET_PRE_SPLIT_LOAD_ABORT.increase(1L);
            }
            LOG.warn("Pre-split post-submit timeout for table {} after {}s: {}",
                    table.getName(), postSubmitTimeout.toSeconds(), timeout.getMessage());
            throw PreSplitPostSubmitTimeoutException.from(timeout);
        } catch (StarRocksException waitFailure) {
            LOG.warn("Pre-split skipped for table {}: admitted job entered terminal-error state — {}",
                    table.getName(), waitFailure.getMessage());
            return new PreSplitOutcome.Skipped(SkipReason.JOB_FAILED_BEFORE_FINISH);
        } finally {
            if (MetricRepo.hasInit) {
                MetricRepo.HISTO_TABLET_PRE_SPLIT_POST_SUBMIT_WAIT_MS.update(
                        System.currentTimeMillis() - postSubmitStartMillis);
            }
        }
        return new PreSplitOutcome.Finished();
    }

    /**
     * Choose how many tablets to pre-split a load into.
     *
     * <p>Picks the larger of two lower bounds — the cluster's active compute-node
     * count (so every compute node gets at least one tablet) and the byte-volume
     * estimate ({@code ceil(estimatedTotalBytes / tablet_reshard_target_size)})
     * — then clamps to {@code [2, tablet_reshard_max_split_count]}. Two is the
     * minimum because a single-tablet result is equivalent to skipping pre-split.
     *
     * @param estimates              full-input estimates from the sampler. Only
     *                               {@link Estimates#totalBytes} is read.
     * @param activeComputeNodeCount compute nodes available to the load, after
     *                               warehouse/blocklist filtering. Must be {@code >= 1}.
     */
    static int selectTabletCount(Estimates estimates, int activeComputeNodeCount) {
        Objects.requireNonNull(estimates, "estimates");
        Preconditions.checkArgument(activeComputeNodeCount >= 1,
                "activeComputeNodeCount must be >= 1, was %s", activeComputeNodeCount);

        long targetSize = Config.tablet_reshard_target_size;
        int maxSplitCount = Config.tablet_reshard_max_split_count;
        Preconditions.checkState(targetSize > 0,
                "tablet_reshard_target_size must be > 0, was %s", targetSize);
        Preconditions.checkState(maxSplitCount >= 2,
                "tablet_reshard_max_split_count must be >= 2, was %s", maxSplitCount);

        // Integer ceil-divide written as (n - 1) / d + 1 with a zero-case guard so it
        // does not overflow when totalBytes is near Long.MAX_VALUE.
        long totalBytes = estimates.totalBytes();
        long byteTargetTabletCount = totalBytes == 0L ? 0L : ((totalBytes - 1) / targetSize) + 1;
        long proposed = Math.max(activeComputeNodeCount, byteTargetTabletCount);
        long clamped = Math.max(2L, Math.min(proposed, maxSplitCount));
        return (int) clamped;
    }
}
