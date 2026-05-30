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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.TimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 *
 * <p><b>Entry points</b>:
 * <ul>
 *   <li>{@link #submitAsynchronously} — single-partition path used by the
 *       load hooks when the target table has exactly one physical partition.
 *       Returns immediately after admitting the reshard job; the daemon
 *       drives it to FINISHED asynchronously.</li>
 *   <li>{@link #runPreSplit} — synchronous-await variant of
 *       {@link #submitAsynchronously} (tests and any future synchronous
 *       integration); blocks on {@link PreSplitPipeline#awaitFinished}.</li>
 *   <li>{@link #submitForPartitionsCombined} — multi-partition path;
 *       takes a {@link PartitionSampleGrouper#group grouped} list of
 *       {@link PartitionSamples}, pre-creates missing partitions, and
 *       submits ONE combined reshard via
 *       {@link com.starrocks.alter.reshard.SplitTabletJobFactory#forExternalBoundariesMultiTablet}.</li>
 * </ul>
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
        if (!areSortKeyColumnsSupported(table)) {
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
        PreSplitMetrics.recordEligibilitySkip(reason);
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

    private static boolean areSortKeyColumnsSupported(OlapTable table) {
        // Use the same sort-key accessor the substrate's BE-side
        // validate_new_tablet_ranges expects boundaries to match. For
        // DUPLICATE KEY + ORDER BY tables the substrate's sort-key arity
        // includes the ORDER BY trailing columns that the KEY clause omits;
        // boundaries produced from getKeyColumnsInOrder() (KEY columns only)
        // would mismatch the substrate's expected schema and trigger the
        // identical-fallback safety net.
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);
        if (sortKeyColumns.isEmpty()) {
            return false;
        }
        // Every sort-key column must be scalar — the sampler projects all of them
        // and BoundaryPlanner per-column-validates each at plan time. Rejecting
        // unsupported trailing columns here (UNSUPPORTED_SORT_KEY) is cheaper
        // than running the sampler only to fail at decode/validation with
        // SAMPLE_FAILED. Deeper per-column validation (decimal precision/scale,
        // primitive-type match against sampler tuples) is still BoundaryPlanner's
        // job at plan time.
        for (Column column : sortKeyColumns) {
            if (!column.getType().isScalarType()) {
                return false;
            }
        }
        return true;
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
                scanContext, MetaUtils.getRangeDistributionColumns(table),
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
        PreSplitMetrics.recordSamplerFailed(reason);
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
        try {
            awaitFinishedAndRecordMetrics(pipeline, submitted.preparedJob(), postSubmitTimeout);
        } catch (PreSplitPostSubmitTimeoutException timeout) {
            // Strict-abort semantics: caller opted into runPreSplit, so a timeout
            // aborts the calling load. The hard-cap counter was bumped inside
            // awaitFinishedAndRecordMetrics; bump the abort counter and rethrow.
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_TABLET_PRE_SPLIT_LOAD_ABORT.increase(1L);
            }
            LOG.warn("Pre-split post-submit timeout for table {} after {}s: {}",
                    table.getName(), postSubmitTimeout.toSeconds(), timeout.getMessage());
            throw timeout;
        } catch (StarRocksException waitFailure) {
            LOG.warn("Pre-split skipped for table {}: admitted job entered terminal-error state — {}",
                    table.getName(), waitFailure.getMessage());
            return new PreSplitOutcome.Skipped(SkipReason.JOB_FAILED_BEFORE_FINISH);
        }
        return new PreSplitOutcome.Finished();
    }

    /**
     * Block on {@link PreSplitPipeline#awaitFinished} for the admitted reshard job
     * and record the latency histogram + post-submit hard-cap counter consistently
     * across the two callers ({@link #runPreSplit} for abort-on-timeout semantics,
     * {@link com.starrocks.alter.reshard.presplit.InsertFromFilesPreSplitHook} for
     * fail-safe semantics). Translates the generic {@link TimeoutException} the
     * pipeline declares into the package-typed
     * {@link PreSplitPostSubmitTimeoutException}; callers decide whether to abort
     * the load or fall back. Always updates the histogram, including on timeout
     * or failure, so operators see the wait distribution even for failed waits.
     */
    static void awaitFinishedAndRecordMetrics(
            PreSplitPipeline pipeline, PreSplitPipeline.PreparedReshardJob preparedJob, Duration timeout)
            throws PreSplitPostSubmitTimeoutException, StarRocksException {
        long postSubmitStartMillis = System.currentTimeMillis();
        try {
            pipeline.awaitFinished(preparedJob, timeout);
        } catch (TimeoutException timeoutException) {
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_TABLET_PRE_SPLIT_POST_SUBMIT_HARD_CAP.increase(1L);
            }
            throw PreSplitPostSubmitTimeoutException.from(timeoutException);
        } finally {
            if (MetricRepo.hasInit) {
                MetricRepo.HISTO_TABLET_PRE_SPLIT_POST_SUBMIT_WAIT_MS.update(
                        System.currentTimeMillis() - postSubmitStartMillis);
            }
        }
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

    /**
     * Multi-partition entry point. Drives per-partition pre-create + boundary
     * planning, then submits ONE combined reshard job spanning every partition
     * whose pre-create + plan succeeded.
     *
     * <p>For each {@link PartitionSamples} entry:
     * <ul>
     *   <li>If {@code !existsInCatalog}, call
     *       {@link com.starrocks.server.LocalMetastore#addPartitions} to
     *       pre-create. If the partition raced into the catalog after the
     *       grouper's snapshot, the call is skipped and recorded as
     *       {@link PreSplitMetrics.PreCreateResult#ALREADY_EXISTS}.</li>
     *   <li>Re-resolve the post-create state under a brief intensive table
     *       READ lock and verify per-partition eligibility (single base
     *       tablet, empty). Mismatches surface as
     *       {@link SkipReason#PARTITION_NOT_ELIGIBLE_POST_CREATE} for that
     *       partition only; sibling partitions continue.</li>
     *   <li>Compute K_i via {@link #selectTabletCount} and plan boundaries
     *       via {@link BoundaryPlanner#planRowQuantileBoundaries}. K_i &lt; 2
     *       cannot occur (the selector clamps); planner returning no-split
     *       (effectiveK &lt; 2 from duplicate-collapse) yields
     *       {@link SkipReason#NO_USEFUL_CUTS} for that partition.</li>
     * </ul>
     *
     * <p>After the loop, when at least one partition contributed,
     * {@link SplitTabletJobFactory#forExternalBoundariesMultiTablet} builds
     * the combined job and
     * {@link com.starrocks.alter.reshard.TabletReshardJobMgr#addTabletReshardJob}
     * admits it. Any synchronous failure of either step downgrades the whole
     * hook to {@link SkipReason#SUBMIT_FAILED} — the in-flight per-partition
     * Submitted markers are NOT promoted (the load proceeds against the
     * pre-create-only layout).
     *
     * <p>The concurrent same-table race surfaces asynchronously inside
     * {@code SplitTabletJob.runPendingJob}'s {@code setTableState}; the
     * caller's {@code awaitFinishedAllowingFallback} handles
     * {@link SkipReason#JOB_FAILED_BEFORE_FINISH} from there.
     *
     * @param ctx ConnectContext used both for {@code LocalMetastore.addPartitions}
     *            (which reads the caller's compute resource for tablet
     *            allocation) and for any analyzer pass that the entry's
     *            AddPartitionClause defers.
     * @return {@link PreSplitOutcome.SubmittedCombined} on success;
     *         {@link PreSplitOutcome.Skipped} with a specific reason when no
     *         partition contributed or the combined submit failed.
     */
    public static PreSplitOutcome submitForPartitionsCombined(
            Database database, OlapTable table, List<PartitionSamples> partitionSamplesList,
            int activeComputeNodeCount, ConnectContext ctx) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(partitionSamplesList, "partitionSamplesList");
        Objects.requireNonNull(ctx, "ctx");
        Preconditions.checkArgument(activeComputeNodeCount >= 1,
                "activeComputeNodeCount must be >= 1, was %s", activeComputeNodeCount);

        // Phase 1: table-level eligibility re-check (defensive against concurrent ALTER
        // between the caller's earlier check and this entry).
        SkipReason tableLevelReason = PreSplitTargets.findEligibleTable(database, table);
        if (tableLevelReason != null) {
            return skipEligibility(tableLevelReason);
        }

        // Phase 2: per-partition pre-create + plan, accumulating into oldTabletIdToRanges.
        // perPartitionResults is parallel-by-input bookkeeping: Skipped(reason) for dropped
        // entries, Submitted(null) sentinel for entries that fed the combined job.
        List<Column> sortKey = MetaUtils.getRangeDistributionColumns(table);
        Map<Long, List<TabletRange>> oldTabletIdToRanges = new LinkedHashMap<>();
        List<PreSplitOutcome> perPartitionResults = new ArrayList<>(partitionSamplesList.size());

        for (PartitionSamples entry : partitionSamplesList) {
            PreSplitMetrics.recordPartitionCounted();
            PreSplitOutcome perPartition = planOnePartition(
                    database, table, entry, sortKey, activeComputeNodeCount, ctx, oldTabletIdToRanges);
            perPartitionResults.add(perPartition);
        }

        if (oldTabletIdToRanges.isEmpty()) {
            return new PreSplitOutcome.Skipped(SkipReason.NO_USEFUL_CUTS);
        }

        // Phase 3: single combined submit. Any synchronous failure here surfaces as
        // SUBMIT_FAILED — the per-partition Submitted-pending markers were never promoted
        // to a real reshard job so callers see "no pre-split happened".
        try {
            TabletReshardJob combinedJob = SplitTabletJobFactory.forExternalBoundariesMultiTablet(
                    database, table, oldTabletIdToRanges);
            GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().addTabletReshardJob(combinedJob);
            return new PreSplitOutcome.SubmittedCombined(combinedJob, perPartitionResults);
        } catch (StarRocksException submitFailure) {
            LOG.warn("Pre-split combined submit failed for table {}: {}",
                    table.getName(), submitFailure.getMessage());
            return skipPostEligibility(SkipReason.SUBMIT_FAILED);
        } catch (RuntimeException submitFailure) {
            // forExternalBoundariesMultiTablet validates the range list shape and may throw
            // IllegalArgumentException for bad caller-supplied ranges. Map to SUBMIT_FAILED
            // so the load still proceeds against the pre-create-only layout.
            LOG.warn("Pre-split combined submit rejected for table {}: {}",
                    table.getName(), submitFailure.getMessage());
            return skipPostEligibility(SkipReason.SUBMIT_FAILED);
        }
    }

    /**
     * Pre-create (when missing), re-resolve post-create state, and plan
     * boundaries for one {@link PartitionSamples} entry. On success, mutates
     * {@code oldTabletIdToRanges} and returns a {@code Submitted(null)}
     * sentinel; on any per-partition failure, returns a {@link PreSplitOutcome.Skipped}
     * carrying the specific reason. Throwables are caught and mapped — the
     * caller iterates the full list regardless of one entry's outcome.
     */
    private static PreSplitOutcome planOnePartition(
            Database database, OlapTable table, PartitionSamples entry, List<Column> sortKey,
            int activeComputeNodeCount, ConnectContext ctx,
            Map<Long, List<TabletRange>> oldTabletIdToRanges) {
        try {
            if (!entry.existsInCatalog()) {
                // Cheap pre-check: if the partition raced into the catalog since the grouper
                // snapshot, skip the addPartitions call and record ALREADY_EXISTS. addPartitions
                // would otherwise silently dedupe — we want the metric to attribute correctly.
                if (table.getPartition(entry.partitionName()) != null) {
                    PreSplitMetrics.recordPreCreate(PreSplitMetrics.PreCreateResult.ALREADY_EXISTS);
                } else {
                    try {
                        GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .addPartitions(ctx, database, table.getName(), entry.analyzedClause());
                        PreSplitMetrics.recordPreCreate(PreSplitMetrics.PreCreateResult.SUCCEEDED);
                    } catch (Throwable preCreateFailure) {
                        LOG.warn("Pre-split pre-create failed for partition {} on table {}: {}",
                                entry.partitionName(), table.getName(), preCreateFailure.getMessage());
                        PreSplitMetrics.recordPreCreate(PreSplitMetrics.PreCreateResult.FAILED);
                        // Bucket as a post-eligibility (sampler-failed) reason: the eligibility
                        // gate already passed and we attempted real work. Aligns with
                        // SUBMIT_FAILED's bucketing in submitForPartitionsCombined.
                        PreSplitMetrics.recordSamplerFailed(SkipReason.PRE_CREATE_FAILED);
                        return new PreSplitOutcome.Skipped(SkipReason.PRE_CREATE_FAILED);
                    }
                }
            }

            // Brief intensive READ lock for post-create re-resolve. Lock is acquired BEFORE the
            // try block so a throw from the lock call does not run the finally release.
            ResolvedPartition resolved = resolveUnderReadLock(database, table, entry.partitionName());
            if (resolved == null) {
                PreSplitMetrics.recordEligibilitySkip(SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE);
                return new PreSplitOutcome.Skipped(SkipReason.PARTITION_NOT_ELIGIBLE_POST_CREATE);
            }

            int requestedTabletCount = selectTabletCount(
                    new Estimates(entry.estimatedBytes(), 0L), activeComputeNodeCount);
            SampleSet sampleSet = buildSampleSet(entry);
            BoundaryPlannerResult planResult = BoundaryPlanner.planRowQuantileBoundaries(
                    sampleSet, requestedTabletCount, sortKey);
            if (planResult.isNoSplit()) {
                PreSplitMetrics.recordEligibilitySkip(SkipReason.NO_USEFUL_CUTS);
                return new PreSplitOutcome.Skipped(SkipReason.NO_USEFUL_CUTS);
            }

            List<TabletRange> tabletRanges = DefaultPreSplitPipeline.buildTabletRanges(
                    planResult.getBoundaries());
            oldTabletIdToRanges.put(resolved.oldTabletId, tabletRanges);
            // Submitted(null) is a "fed-into-combined-submit" sentinel; promoted at the outer
            // level when the combined job is admitted.
            return new PreSplitOutcome.Submitted(null);
        } catch (StarRocksException planFailure) {
            LOG.warn("Pre-split per-partition planning failed for partition {} on table {}: {}",
                    entry.partitionName(), table.getName(), planFailure.getMessage());
            PreSplitMetrics.recordSamplerFailed(SkipReason.SAMPLE_FAILED);
            return new PreSplitOutcome.Skipped(SkipReason.SAMPLE_FAILED);
        } catch (RuntimeException planFailure) {
            LOG.warn("Pre-split per-partition planning errored for partition {} on table {}: {}",
                    entry.partitionName(), table.getName(), planFailure.getMessage());
            PreSplitMetrics.recordSamplerFailed(SkipReason.SAMPLE_FAILED);
            return new PreSplitOutcome.Skipped(SkipReason.SAMPLE_FAILED);
        }
    }

    /**
     * Acquire an intensive table READ lock, re-resolve the partition's
     * default physical partition + base index, verify single-tablet + empty,
     * and return the discovered oldTabletId. Returns {@code null} when any
     * eligibility check fails (caller maps to PARTITION_NOT_ELIGIBLE_POST_CREATE).
     */
    private static ResolvedPartition resolveUnderReadLock(Database database, OlapTable table, String partitionName) {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(database.getId(), table.getId(), LockType.READ);
        try {
            Partition partition = table.getPartition(partitionName);
            if (partition == null) {
                return null;
            }
            PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
            if (physicalPartition == null) {
                return null;
            }
            MaterializedIndex baseIndex = physicalPartition.getIndex(table.getBaseIndexMetaId());
            if (baseIndex == null) {
                return null;
            }
            List<Tablet> tablets = baseIndex.getTablets();
            if (tablets.size() != 1 || baseIndex.getRowCount() > 0) {
                return null;
            }
            return new ResolvedPartition(physicalPartition.getId(), tablets.get(0).getId());
        } finally {
            locker.unLockTableWithIntensiveDbLock(database.getId(), table.getId(), LockType.READ);
        }
    }

    /**
     * Project the entry's {@link SampleRow} list into a {@link SampleSet} the
     * {@link BoundaryPlanner} consumes — only the sort-key tuples and a
     * zero-byte {@link Estimates} are needed here (estimatedBytes already
     * drove K_i selection).
     */
    private static SampleSet buildSampleSet(PartitionSamples entry) {
        List<SampleRow> rows = entry.samples();
        List<Tuple> sortKeyTuples = new ArrayList<>(rows.size());
        for (SampleRow row : rows) {
            sortKeyTuples.add(new Tuple(row.sortKeyTuple()));
        }
        return new SampleSet(sortKeyTuples, Estimates.ZERO);
    }

    /** One partition's post-create resolution: stable IDs the coordinator passes to the factory. */
    private record ResolvedPartition(long physicalPartitionId, long oldTabletId) { }
}
