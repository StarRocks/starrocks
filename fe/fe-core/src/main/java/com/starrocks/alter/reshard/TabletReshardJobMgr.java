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

package com.starrocks.alter.reshard;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletReshardJobsResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TabletReshardJobMgr extends LeaderDaemon implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(TabletReshardJobMgr.class);

    @SerializedName(value = "tabletReshardJobs")
    protected final Map<Long, TabletReshardJob> tabletReshardJobs = Maps.newConcurrentMap();

    // Original tablet id -> resharding tablet info
    protected final Map<Long, ReshardingTabletInfo> reshardingTabletInfos = Maps.newConcurrentMap();

    // Colocate checker: invoked from this manager's tick. Holds only a small per-leader,
    // in-memory placement-convergence negative cache (non-journaled). Owns no
    // thread of its own — shares this manager's scheduler cadence
    // ({@code tablet_reshard_job_scheduler_interval_ms}) and self-gates on shared-data-mode,
    // leader status, and empty unstable-groups before doing any real work.
    private final ColocateChecker colocateChecker = new ColocateChecker();

    // Per-table edge-triggered latch that stops the size-based auto-split trigger from re-issuing a
    // deterministic split that made no progress: a tablet dominated by a single distribution-key value
    // is un-splittable, so BE returns the identical-tablet fallback and the tablet stays over threshold.
    // Reuses the generic mechanism the colocate checker uses for its alignment-job storm; keyed on the
    // table's convergence signature (tablet ranges + dataVersion + a split-plan fingerprint of
    // target_size / max_split_count / computed count), so a successful split, a load, or a reshard-config
    // change re-arms it while the no-progress fallback (only visibleVersion moves; size/config unchanged)
    // does not. Touched only from
    // the single reshard scheduler tick (triggerTabletReshard), never from the concurrent
    // addReshardCandidate producers, so no synchronization is needed; cleared on leader demotion.
    private final TableAlignmentLatch sizeSplitLatch = new TableAlignmentLatch();

    // Coalescible reshard candidate for one table, marked by both the publish path and the periodic
    // TabletStatMgr scan: the largest tablet (split), the smallest adjacent fresh-pair sum (merge) and
    // the largest tablet living in an index that still has fewer tablets than the warehouse can drive
    // in parallel (early split; 0 when there is none). Long.MAX_VALUE is the "no merge" identity, so a
    // split-only publish mark and a split+merge periodic mark compose by (max, min, max) regardless of
    // arrival order. Self-contained (carries db/table id) so the drain needs no side key. Transient
    // (not persisted): leader failover falls back to the scan.
    private record ReshardCandidate(long dbId, long tableId, long maxTabletSize,
                                    long minAdjacentTabletPairSize, long maxUnderProvisionedTabletSize) {
    }

    // tableId (globally unique) -> coalesced reshard candidate awaiting a drain evaluation.
    private final Map<Long, ReshardCandidate> reshardCandidates = new ConcurrentHashMap<>();

    // Enqueue a table for a reshard evaluation, carrying the signals its caller already computed so the
    // drain triggers without re-walking the table. Both the write-locked publish path (split-only:
    // minAdjacentTabletPairSize = Long.MAX_VALUE) and the periodic scan (split+merge) mark here;
    // concurrent marks for the same table coalesce by (max, min, max) before the next drain. Callers
    // need only supply the signals (and gate on leader/eligibility for their own reasons); the
    // split/merge actionability decision lives here, so non-actionable signals are dropped and never
    // queued.
    public void addReshardCandidate(long dbId, long tableId, long maxTabletSize,
            long minAdjacentTabletPairSize, long maxUnderProvisionedTabletSize) {
        if (!isLeaderAdmissionOpen()) {
            return;
        }
        // Keep the queue empty in the common (no-reshard) case; the drain re-checks authoritatively.
        // All three disjuncts are required: dropping the merge one disables automatic merge.
        if (!TabletReshardUtils.needSplit(maxTabletSize)
                && !TabletReshardUtils.needEarlySplit(maxUnderProvisionedTabletSize)
                && !TabletReshardUtils.needMerge(minAdjacentTabletPairSize)) {
            return;
        }
        reshardCandidates.merge(tableId,
                new ReshardCandidate(dbId, tableId, maxTabletSize, minAdjacentTabletPairSize,
                        maxUnderProvisionedTabletSize),
                (existing, incoming) -> new ReshardCandidate(existing.dbId(), existing.tableId(),
                        Math.max(existing.maxTabletSize(), incoming.maxTabletSize()),
                        Math.min(existing.minAdjacentTabletPairSize(), incoming.minAdjacentTabletPairSize()),
                        Math.max(existing.maxUnderProvisionedTabletSize(),
                                incoming.maxUnderProvisionedTabletSize())));
    }

    public TabletReshardJobMgr() {
        super("tablet-reshard-job-mgr", Config.tablet_reshard_job_scheduler_interval_ms);
    }

    public TabletReshardJob getTabletReshardJob(long jobId) {
        return tabletReshardJobs.get(jobId);
    }

    public Map<Long, TabletReshardJob> getTabletReshardJobs() {
        return tabletReshardJobs;
    }

    public ReshardingTablet getReshardingTablet(long tabletId, long visibleVersion) {
        ReshardingTabletInfo reshardingTabletInfo = reshardingTabletInfos.get(tabletId);
        if (reshardingTabletInfo == null) {
            return null;
        }

        if (visibleVersion < reshardingTabletInfo.getVisibleVersion()) {
            return null;
        }

        return reshardingTabletInfo.getReshardingTablet();
    }

    public TabletReshardJob createTabletReshardJob(Database db, OlapTable table, SplitTabletClause splitTabletClause)
            throws StarRocksException {
        // User-facing DDL entry: no caller-side sample, so resolve the compute-node count here.
        return createTabletReshardJob(db, table, splitTabletClause,
                TabletReshardUtils.safeComputeNodeCountForTable(table.getId()));
    }

    /**
     * Automatic entry. {@code computeNodeCount} is the trigger's single sample, so the plan this job
     * executes and the no-progress fingerprint the trigger recorded describe the same layout.
     */
    public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
            SplitTabletClause splitTabletClause, int computeNodeCount) throws StarRocksException {
        TabletReshardJob job = new SplitTabletJobFactory(db, table, splitTabletClause, computeNodeCount)
                .createTabletReshardJob();
        addTabletReshardJob(job);
        return job;
    }

    public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause mergeTabletClause)
            throws StarRocksException {
        if (!Config.tablet_reshard_enable_tablet_merge) {
            throw new StarRocksException("Tablet merge is disabled. " +
                    "Set tablet_reshard_enable_tablet_merge=true to enable it.");
        }
        TabletReshardJob job = new MergeTabletJobFactory(db, table, mergeTabletClause).createTabletReshardJob();
        addTabletReshardJob(job);
    }

    // 64-bit fingerprint of the requested split plan: the raw reshard-config knobs plus the computed
    // child count of the tablet each rule would act on. Folding the raw configs guarantees any admin
    // config change re-arms the size-split latch even when calcSplitCount is capped at max_split_count;
    // the computed counts also capture a size-band crossing. The early inputs are folded for the same
    // reasons the normal ones are, plus two of their own: the ceiling moves with the warehouse size, so
    // without it a resize would change the requested child count with nothing else observable; and
    // max_parallel_tablets decides whether an index's early contribution is admitted at all, so an
    // operator raising it to release deferred work must re-arm the latch. murmur3 (matching
    // ColocateChecker) avoids the 32-bit Objects.hash collision that could hide a config change.
    @VisibleForTesting
    static long splitPlanSignature(long maxTabletSize, long maxUnderProvisionedTabletSize,
            int computeNodeCount) {
        return Hashing.murmur3_128().newHasher()
                .putLong(Config.tablet_reshard_target_size)
                .putInt(Config.tablet_reshard_max_split_count)
                .putInt(TabletReshardUtils.calcSplitCount(maxTabletSize, Config.tablet_reshard_target_size))
                .putLong(Config.tablet_reshard_min_split_size)
                .putBoolean(Config.tablet_reshard_enable_early_split)
                .putLong(Config.tablet_reshard_max_parallel_tablets)
                .putInt(TabletReshardUtils.earlySplitCeiling(computeNodeCount,
                        Config.tablet_reshard_max_split_count))
                .putInt(TabletReshardUtils.calcSplitCount(maxUnderProvisionedTabletSize,
                        Math.max(1L, TabletReshardUtils.earlySplitTargetSize())))
                .hash().asLong();
    }

    /**
     * Reshard-trigger decision. The sole caller is {@link #drainReshardCandidates()}, which feeds the
     * signals that both the publish path and the periodic TabletStatMgr scan supplied via
     * {@link #addReshardCandidate}. Self-gates on leader/admission, cloud-native range distribution,
     * and NORMAL table state; the authoritative NORMAL re-check happens in the job factory under its
     * own lock.
     */
    private void triggerTabletReshard(Database db, OlapTable table, long maxTabletSize,
                                      long minAdjacentTabletPairSize, long maxUnderProvisionedTabletSize) {
        if (!isLeaderAdmissionOpen()) {
            return;
        }
        if (!table.isCloudNativeTableOrMaterializedView() || !table.isRangeDistribution()) {
            return;
        }
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            return;
        }
        try {
            long tableId = table.getId();
            boolean normalSignal = TabletReshardUtils.needSplit(maxTabletSize);
            boolean earlySignal = TabletReshardUtils.needEarlySplit(maxUnderProvisionedTabletSize);
            // The smallest useful early contribution is a 2-way split, and early work is skipped while
            // another reshard job runs, so skip before paying for a plan that cannot produce anything.
            boolean earlyCapacityPossible = getTotalParallelTablets() == 0
                    && Config.tablet_reshard_max_parallel_tablets >= 2;
            if (normalSignal || (earlySignal && earlyCapacityPossible)) {
                // Resolve the node count exactly once and use it for BOTH the suppression fingerprint
                // and the job, so the fingerprint always describes the plan that ran.
                int computeNodeCount = TabletReshardUtils.safeComputeNodeCountForTable(tableId);
                long signature = ColocateChecker.tableConvergenceSignature(db, table,
                        splitPlanSignature(maxTabletSize, maxUnderProvisionedTabletSize, computeNodeCount));
                TableAlignmentLatch.AlignmentDecision decision = sizeSplitLatch.evaluate(tableId, signature);
                if (decision.fire()) {
                    try {
                        TabletReshardJob job = createTabletReshardJob(db, table, new SplitTabletClause(),
                                computeNodeCount);
                        sizeSplitLatch.recordFired(tableId, signature, job.getJobId(),
                                decision.nextAbortRetries());
                        LOG.info("Auto triggered split tablet job for table {}.{}, maxTabletSize {}, "
                                        + "maxUnderProvisionedTabletSize {}",
                                db.getFullName(), table.getName(), maxTabletSize,
                                maxUnderProvisionedTabletSize);
                        return;
                    } catch (StarRocksException e) {
                        // An empty plan is not a failure for an early-only trigger: fall through so an
                        // actionable merge signal on the same candidate is still evaluated.
                        if (normalSignal) {
                            throw e;
                        }
                        LOG.debug("Early split produced no work for table {}.{}: {}",
                                db.getFullName(), table.getName(), e.getMessage());
                    }
                } else if (sizeSplitLatch.claimSuppressionLog(tableId)) {
                    LOG.warn("Auto split for table {}.{} made no progress on an unchanged layout "
                                    + "(tablet not splittable); suppressing further split jobs until its data changes",
                            db.getFullName(), table.getName());
                }
                if (normalSignal) {
                    return;
                }
            }
            // Drop stale suppression ONLY when no split signal remains. Clearing it while an early
            // signal is live would erase the tombstone that stops a deterministic no-progress split from
            // re-firing on the next unchanged candidate.
            if (!normalSignal && !earlySignal) {
                sizeSplitLatch.forgetTable(tableId);
            }
            if (TabletReshardUtils.needMerge(minAdjacentTabletPairSize)) {
                createTabletReshardJob(db, table, new MergeTabletClause());
                LOG.info("Auto triggered merge tablet job for table {}.{}, minAdjacentTabletPairSize {}",
                        db.getFullName(), table.getName(), minAdjacentTabletPairSize);
            }
        } catch (Exception e) {
            LOG.warn("Failed to create tablet reshard job for table {}.{}.",
                    db.getFullName(), table.getName(), e);
        }
    }

    private boolean isLeaderAdmissionOpen() {
        return GlobalStateMgr.getCurrentState().isLeader()
                && GlobalStateMgr.getCurrentState().isLeaderWorkAdmissionOpen();
    }

    public void addTabletReshardJob(TabletReshardJob tabletReshardJob) throws StarRocksException {
        checkTabletReshardJob(tabletReshardJob);

        // Reserve the table before the job becomes visible to the scheduler. If the table is not
        // reservable (e.g. busy with another reshard job or other DDL), init() throws here and the
        // job is never queued, instead of being admitted and then forced to abort at execution time.
        tabletReshardJob.init();

        // jobId is generated by GlobalStateMgr.getNextId() (monotonic, never reused), so plain put
        // is safe — a collision would indicate an id-generator bug and is not handled defensively.
        tabletReshardJobs.put(tabletReshardJob.getJobId(), tabletReshardJob);

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateTabletReshardJob(tabletReshardJob);

        if (MetricRepo.hasInit) {
            if (tabletReshardJob.getJobType() == TabletReshardJob.JobType.SPLIT_TABLET) {
                MetricRepo.COUNTER_TABLET_RESHARD_SPLIT_JOB_TOTAL.increase(1L);
            } else if (tabletReshardJob.getJobType() == TabletReshardJob.JobType.MERGE_TABLET) {
                MetricRepo.COUNTER_TABLET_RESHARD_MERGE_JOB_TOTAL.increase(1L);
            }
        }

        LOG.info("Added tablet reshard job. {}", tabletReshardJob);
    }

    public long getTotalParallelTablets() {
        long totalParallelTablets = 0;
        for (TabletReshardJob job : tabletReshardJobs.values()) {
            if (job.isDone()) {
                continue;
            }
            totalParallelTablets += job.getParallelTablets();
        }
        return totalParallelTablets;
    }

    public void replayUpdateTabletReshardJob(TabletReshardJob tabletReshardJob) {
        tabletReshardJob.replay();
        tabletReshardJobs.put(tabletReshardJob.getJobId(), tabletReshardJob);
    }

    public void replayRemoveTabletReshardJob(long tabletReshardJobId) {
        if (tabletReshardJobs.remove(tabletReshardJobId) == null) {
            // Should not happen, just add a warning log
            LOG.warn("Failed to find tablet reshard job {} when replaying remove tablet reshard job",
                    tabletReshardJobId);
        }
    }

    public TTabletReshardJobsResponse getAllJobsInfo() {
        TTabletReshardJobsResponse response = new TTabletReshardJobsResponse();
        response.status = new TStatus();
        response.status.setStatus_code(TStatusCode.OK);
        for (TabletReshardJob job : tabletReshardJobs.values()) {
            try {
                response.addToItems(job.getInfo());
            } catch (Exception e) {
                if (response.status.getStatus_code() == TStatusCode.OK) {
                    // if encouter any unexpected exception, set error status for response
                    response.status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                    response.status.addToError_msgs(Strings.nullToEmpty(e.getMessage()));
                    LOG.warn("Encounter unexpected exception when getting tablet reshard jobs info. ", e);
                }
            }
        }
        return response;
    }

    protected void registerReshardingTablet(long tabletId, ReshardingTablet reshardingTablet, long visibleVersion) {
        reshardingTabletInfos.put(tabletId, new ReshardingTabletInfo(reshardingTablet, visibleVersion));
    }

    protected void unregisterReshardingTablet(long tabletId) {
        reshardingTabletInfos.remove(tabletId);
    }

    // tabletReshardJobs is persistent (@SerializedName, save/load via image) and
    // reshardingTabletInfos is also serialized through writer.writeJson(this), so neither
    // map should be cleared on demotion - the next leader resumes those jobs from the same
    // maps. Default onStopped() is sufficient.

    @Override
    protected void runAfterLeaseValid() {
        // The LeaderDaemon lease gate already skips this tick on a demoted node; this admission
        // re-check also covers the activation window (lease valid before leader-work admission opens):
        // neither runTabletReshardJobs() nor ColocateChecker.runOneCycle() self-gates on leader/admission,
        // and both create reshard jobs that journal via the non-throwing logJsonObject path, so a
        // non-admitted node must drop pending candidates and skip the tick.
        if (!isLeaderAdmissionOpen()) {
            reshardCandidates.clear();
            sizeSplitLatch.clear();
            return;
        }
        colocateChecker.runOneCycle();
        drainReshardCandidates();
        runTabletReshardJobs();
    }

    @VisibleForTesting
    void runAfterCatalogReadyForTest() {
        runAfterLeaseValid();
    }

    @VisibleForTesting
    int getReshardCandidateCount() {
        return reshardCandidates.size();
    }

    /** The queued candidate's early-split signal for {@code tableId}, or {@code -1} when none is queued. */
    @VisibleForTesting
    long peekMaxUnderProvisionedTabletSize(long tableId) {
        ReshardCandidate candidate = reshardCandidates.get(tableId);
        return candidate == null ? -1L : candidate.maxUnderProvisionedTabletSize();
    }

    @VisibleForTesting
    boolean hasSizeSplitLatch(long tableId) {
        return sizeSplitLatch.hasRecordedAttempt(tableId);
    }

    @VisibleForTesting
    void clearSizeSplitLatchForTest() {
        sizeSplitLatch.clear();
    }

    private void drainReshardCandidates() {
        if (reshardCandidates.isEmpty()) {
            return;
        }
        // Snapshot the keys; remove each atomically so a concurrent re-mark is re-evaluated next tick.
        for (Long tableId : new ArrayList<>(reshardCandidates.keySet())) {
            ReshardCandidate candidate = reshardCandidates.remove(tableId);
            if (candidate == null) {
                continue;
            }
            // db and table lookups are not atomic; the guard is conservative — a dropped db/table is
            // simply skipped this cycle, and its stale size-split latch entry is reclaimed so a
            // re-created table re-arms cleanly.
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(candidate.dbId());
            Table table = db == null ? null : GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(candidate.dbId(), candidate.tableId());
            if (!(table instanceof OlapTable)) {
                sizeSplitLatch.forgetTable(tableId);
                continue;
            }
            triggerTabletReshard(db, (OlapTable) table, candidate.maxTabletSize(),
                    candidate.minAdjacentTabletPairSize(), candidate.maxUnderProvisionedTabletSize());
        }
    }

    private void checkTabletReshardJob(TabletReshardJob tabletReshardJob) throws StarRocksException {
        if (tabletReshardJob.getJobState() != TabletReshardJob.JobState.PENDING) {
            throw new StarRocksException("Tablet reshard job state is not pending. " + tabletReshardJob);
        }

        long currentParallelTablets = getTotalParallelTablets();
        if (currentParallelTablets <= 0) { // No running jobs
            return;
        }

        long newParallelTablets = tabletReshardJob.getParallelTablets() + currentParallelTablets;
        if (newParallelTablets > Config.tablet_reshard_max_parallel_tablets) {
            throw new StarRocksException("Total parallel tablets exceed tablet_reshard_max_parallel_tablets: "
                    + Config.tablet_reshard_max_parallel_tablets);
        }
    }

    private void runTabletReshardJobs() {
        for (var iterator = tabletReshardJobs.entrySet().iterator(); iterator.hasNext(); /* */) {
            TabletReshardJob job = iterator.next().getValue();
            // Job is not done, run it
            if (!job.isDone()) {
                job.run();
                continue;
            }

            // Job is done, remove expired job once no automated cluster snapshot still covers the
            // pre-reshard state it retains (the parent/source tablets). Keeping the job keeps
            // isTableSafeToDeleteTablet() reporting the table's tablets as unsafe to reclaim.
            if (job.isExpired() && GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                    .isDeletionSafeToExecute(job.getFinishedTimeMs())) {
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveTabletReshardJob(job.getJobId(), wal -> {
                    iterator.remove();
                });
                LOG.info("Removed expired tablet reshard job. {}", job);
            }
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        for (TabletReshardJob job : tabletReshardJobs.values()) {
            if (job.isDone()) {
                continue;
            }

            job.registerReshardingTabletsOnRestart();
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.TABLET_RESHARD_JOB_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        TabletReshardJobMgr tabletReshardJobMgr = reader.readJson(TabletReshardJobMgr.class);
        tabletReshardJobs.putAll(tabletReshardJobMgr.tabletReshardJobs);
        reshardingTabletInfos.putAll(tabletReshardJobMgr.reshardingTabletInfos);
    }
}
