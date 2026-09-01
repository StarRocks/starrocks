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
import com.google.common.collect.Lists;
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
import com.starrocks.proto.ParentTabletPublishInfoPB;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    // Same mechanism for the merge trigger. Needed because the size signal is derived from adjacent
    // tablet PAIRS and knows nothing about colocate, while the planner refuses a pair that straddles
    // a ColocateRange: a range-colocate table settles at one tablet per range, so every adjacent pair
    // crosses and the signal stays permanently actionable against a permanently empty plan. Without a
    // latch that table re-walks every partition and index under the read lock on every stat pass,
    // forever.
    private final TableAlignmentLatch emptyMergePlanLatch = new TableAlignmentLatch();

    // Coalescible reshard candidate for one table, marked by both the publish path and the periodic
    // TabletStatMgr scan: the largest tablet (split), the smallest adjacent fresh-pair sum (merge) and
    // the largest tablet living in an index that still has fewer tablets than the warehouse can drive
    // in parallel (early split; 0 when there is none). Long.MAX_VALUE is the "no merge" identity, so a
    // split-only publish mark and a split+merge periodic mark compose the three signals by
    // (max, min, max) regardless of arrival order; the bound composes as last-nonzero, which is
    // deliberately order-dependent for the reason below. adaptiveBound rides along because the scan has already resolved it
    // -- resolving a warehouse probes StarMgr -- and it is a plan input the drain would otherwise have
    // to fetch again. It takes the newer mark's value rather than the larger, so a warehouse scaled
    // down between two marks is fingerprinted against the bound it now has: taking the max would keep
    // the stale wider one and re-create the permanent suppression the bound was folded in to prevent.
    // A producer that resolved nothing passes 0, which never displaces a resolved value.
    // Self-contained (carries db/table id) so the drain needs no side key. Transient (not persisted):
    // leader failover falls back to the scan.
    private record ReshardCandidate(long dbId, long tableId, long maxTabletSize,
                                    long minAdjacentTabletPairSize, long maxAdaptiveSplitTabletSize,
                                    int adaptiveBound) {
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
            long minAdjacentTabletPairSize) {
        addReshardCandidate(dbId, tableId, maxTabletSize, minAdjacentTabletPairSize, 0L, 0);
    }

    /**
     * As above, plus the largest tablet of an index that still holds fewer tablets than the warehouse
     * can drive in parallel, and the bound that judgement was made against. Only the periodic scan
     * knows either, because only it walks an index's tablet list and it has already resolved the
     * warehouse; every other producer passes 0 through the overload above.
     */
    public void addReshardCandidate(long dbId, long tableId, long maxTabletSize,
            long minAdjacentTabletPairSize, long maxAdaptiveSplitTabletSize, int adaptiveBound) {
        if (!isLeaderAdmissionOpen()) {
            return;
        }
        // Keep the queue empty in the common (no-reshard) case; the drain re-checks authoritatively.
        // All three disjuncts are required: dropping the merge one disables automatic merge.
        if (!TabletReshardUtils.needSplit(maxTabletSize)
                && maxAdaptiveSplitTabletSize <= 0
                && !TabletReshardUtils.needMerge(minAdjacentTabletPairSize)) {
            return;
        }
        reshardCandidates.merge(tableId,
                new ReshardCandidate(dbId, tableId, maxTabletSize, minAdjacentTabletPairSize,
                        maxAdaptiveSplitTabletSize, adaptiveBound),
                (existing, incoming) -> new ReshardCandidate(existing.dbId(), existing.tableId(),
                        Math.max(existing.maxTabletSize(), incoming.maxTabletSize()),
                        Math.min(existing.minAdjacentTabletPairSize(), incoming.minAdjacentTabletPairSize()),
                        Math.max(existing.maxAdaptiveSplitTabletSize(),
                                incoming.maxAdaptiveSplitTabletSize()),
                        incoming.adaptiveBound() != 0 ? incoming.adaptiveBound() : existing.adaptiveBound()));
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

    /**
     * Whether any split job could still owe a parent-view page. Publish consults this on every
     * transaction, so it has to be the cheapest question that can end the enquiry: finished jobs stay in
     * the map for tablet_reshard_history_job_keep_max_ms (3 days by default) and answer nothing, and a
     * cluster that never splits answers nothing at all.
     */
    public boolean hasLiveSplitJob() {
        for (TabletReshardJob job : tabletReshardJobs.values()) {
            if (job instanceof SplitTabletJob && !job.getJobState().isFinalState()) {
                return true;
            }
        }
        return false;
    }

    public List<ParentTabletPublishInfoPB> collectParentPublishInfos(Set<Long> publishedTabletIds) {
        List<ParentTabletPublishInfoPB> parentInfos = Lists.newArrayList();
        for (TabletReshardJob job : tabletReshardJobs.values()) {
            if (job instanceof SplitTabletJob splitJob) {
                parentInfos.addAll(splitJob.collectParentPublishInfos(publishedTabletIds));
            }
        }
        return parentInfos;
    }

    public TabletReshardJob createTabletReshardJob(Database db, OlapTable table, SplitTabletClause splitTabletClause)
            throws StarRocksException {
        TabletReshardJob job = new SplitTabletJobFactory(db, table, splitTabletClause).createTabletReshardJob();
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
    // the computed count also captures a size-band crossing. The adaptive term is folded raw, because
    // the target it would be counted against is per-index and cannot be rebuilt from one flat number
    // here -- so a growing tablet moves this fingerprint by every byte, and suppression only settles a
    // table whose data has stopped moving. That is the case it exists for. The bound is folded in because the plan
    // depends on it and the signal alone does not carry it: an eight-way attempt that latched after
    // producing an identical tablet would, on a warehouse scaled down to two nodes, become a two-way
    // attempt that might well succeed -- with the same tablet size, and so the same fingerprint,
    // suppressing it forever. murmur3 (matching ColocateChecker) avoids the 32-bit Objects.hash
    // collision that could hide a config change.
    @VisibleForTesting
    static long splitPlanSignature(long maxTabletSize, long maxAdaptiveSplitTabletSize, int adaptiveBound,
                                   int maxSplitCount) {
        return Hashing.murmur3_128().newHasher()
                .putLong(Config.tablet_reshard_target_size)
                .putInt(Config.tablet_reshard_max_split_count)
                .putInt(maxSplitCount)
                .putInt(TabletReshardUtils.calcSplitCount(maxTabletSize, Config.tablet_reshard_target_size,
                        maxSplitCount))
                .putLong(Config.tablet_reshard_min_split_size)
                .putLong(maxAdaptiveSplitTabletSize)
                .putInt(adaptiveBound)
                .hash().asLong();
    }

    // 64-bit fingerprint of everything a merge plan's emptiness depends on beyond the table layout,
    // which tableConvergenceSignature already folds (tablet ranges + dataVersion per partition). The
    // target size is folded because every merge threshold derives from it, and the caller's own signal
    // because it moves whenever the tablet mix or the parallelism floor does -- the floor itself is
    // deliberately not resolved here, since skipping that probe is part of what the latch buys.
    @VisibleForTesting
    static long mergePlanSignature(long minAdjacentTabletPairSize) {
        return Hashing.murmur3_128().newHasher()
                .putLong(Config.tablet_reshard_target_size)
                .putLong(minAdjacentTabletPairSize)
                .hash().asLong();
    }

    /**
     * Whether the quiet period after this table's previous reshard job has elapsed.
     *
     * <p>A split whose children must be UNSHARE-rewritten holds the partition's only compaction slot
     * for the whole rewrite, during which size-tiered compaction cannot run and the small files from
     * ongoing ingestion just pile up. Firing the next split the moment the previous one lands never
     * gives that backlog a chance to drain. Finished jobs stay in {@code tabletReshardJobs} for
     * tablet_reshard_history_job_keep_max_ms, so the previous finish time is read straight off them --
     * no extra state to keep in sync, and it survives a leader switch.
     *
     * <p>That retention is therefore a lower bound on the interval this can enforce: configure
     * tablet_reshard_orderby_split_interval_second beyond tablet_reshard_history_job_keep_max_ms (180s
     * against 3 days by default) and the finished job is evicted before the period ends, leaving no
     * timestamp and admitting the next split early. The failure is a shorter wait, never inconsistent
     * metadata. Closing it means persisting a per-table completion timestamp -- journaled, replayed and
     * garbage-collected on drop -- which is only worth it if the interval ever grows toward retention.
     */
    private boolean reshardQuietPeriodElapsed(long tableId) {
        int waitSeconds = Config.tablet_reshard_orderby_split_interval_second;
        if (waitSeconds <= 0) {
            return true;
        }
        long newestFinishMs = 0;
        for (TabletReshardJob job : tabletReshardJobs.values()) {
            if (job instanceof SplitTabletJob splitJob && splitJob.getTableId() == tableId && job.isDone()) {
                newestFinishMs = Math.max(newestFinishMs, job.getFinishedTimeMs());
            }
        }
        return newestFinishMs == 0
                || System.currentTimeMillis() - newestFinishMs >= waitSeconds * 1000L;
    }

    /**
     * Reshard-trigger decision. The sole caller is {@link #drainReshardCandidates()}, which feeds the
     * signals that both the publish path and the periodic TabletStatMgr scan supplied via
     * {@link #addReshardCandidate}. Self-gates on leader/admission, cloud-native range distribution,
     * and NORMAL table state; the authoritative NORMAL re-check happens in the job factory under its
     * own lock.
     */
    private void triggerTabletReshard(Database db, OlapTable table, long maxTabletSize,
                                      long minAdjacentTabletPairSize, long maxAdaptiveSplitTabletSize,
                                      int adaptiveBound) {
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
            boolean adaptiveSignal = maxAdaptiveSplitTabletSize > 0;
            if (normalSignal || adaptiveSignal) {
                if (TabletReshardUtils.splitRewritesEveryShard(table) && !reshardQuietPeriodElapsed(tableId)) {
                    // Leave the latch untouched: this is a "not yet", not "no progress possible".
                    return;
                }
                long signature = ColocateChecker.tableConvergenceSignature(db, table,
                        splitPlanSignature(maxTabletSize, maxAdaptiveSplitTabletSize, adaptiveBound,
                                TabletReshardUtils.effectiveMaxSplitCount(table)));
                TableAlignmentLatch.AlignmentDecision decision = sizeSplitLatch.evaluate(tableId, signature);
                if (decision.fire()) {
                    try {
                        TabletReshardJob job = createTabletReshardJob(db, table, new SplitTabletClause());
                        sizeSplitLatch.recordFired(tableId, signature, job.getJobId(),
                                decision.nextAbortRetries());
                        LOG.info("Auto triggered split tablet job for table {}.{}, maxTabletSize {}, "
                                        + "maxAdaptiveSplitTabletSize {}",
                                db.getFullName(), table.getName(), maxTabletSize,
                                maxAdaptiveSplitTabletSize);
                        return;
                    } catch (StarRocksException e) {
                        // An empty plan is not a failure for an early-only trigger: fall through so an
                        // actionable merge signal on the same candidate is still evaluated.
                        if (normalSignal) {
                            throw e;
                        }
                        if (e instanceof EmptyReshardPlanException) {
                            // Deterministic: the same layout and configuration will produce the same
                            // empty plan, so latch it. Without that, such a table re-plans on every
                            // scan forever, walking every partition and index under the table read
                            // lock each time. -1 is not a tracked job, which the latch's abort and
                            // settled probes both resolve to "no job" -- correct, nothing is running.
                            sizeSplitLatch.recordFired(tableId, signature, -1L, decision.nextAbortRetries());
                            LOG.info("Adaptive split produced no work for table {}.{}; suppressing until "
                                            + "its layout or configuration changes: {}",
                                    db.getFullName(), table.getName(), e.getMessage());
                        } else {
                            // Not now, rather than never: an exhausted parallel-tablet budget or a table
                            // another job owns clears on its own, and latching it here would suppress
                            // the retry until something unrelated moved the fingerprint.
                            LOG.info("Adaptive split for table {}.{} could not start; will retry: {}",
                                    db.getFullName(), table.getName(), e.getMessage());
                        }
                    }
                } else if (sizeSplitLatch.claimSuppressionLog(tableId)) {
                    LOG.warn("Auto split for table {}.{} made no progress on an unchanged layout; "
                                    + "suppressing further split jobs until its data changes",
                            db.getFullName(), table.getName());
                }
                if (normalSignal) {
                    return;
                }
            } else {
                // No split signal at all, so drop any stale suppression and let future growth re-arm.
                // Clearing it while an adaptive signal is live would erase the tombstone that stops a
                // deterministic no-progress split from re-firing on the next unchanged candidate.
                sizeSplitLatch.forgetTable(tableId);
            }
            // The feature gate is checked FIRST, before the signature below: that signature takes the
            // table READ lock and hashes every tablet of every visible index, while the gate itself
            // lives inside createTabletReshardJob. needMerge does not consult the flag, so a table
            // carrying only a merge signal is still queued while merge is disabled -- and the
            // resulting "merge is disabled" is a plain StarRocksException, so the catch below never
            // fires and the latch never arms. Evaluating the latch first would therefore pay that walk
            // on every stat pass, forever, for a job that cannot run. Before this feature the same
            // path threw immediately with no walk at all.
            if (Config.tablet_reshard_enable_tablet_merge
                    && TabletReshardUtils.needMerge(minAdjacentTabletPairSize)) {
                long mergeSignature = ColocateChecker.tableConvergenceSignature(db, table,
                        mergePlanSignature(minAdjacentTabletPairSize));
                TableAlignmentLatch.AlignmentDecision mergeDecision =
                        emptyMergePlanLatch.evaluate(tableId, mergeSignature);
                if (mergeDecision.fire()) {
                    try {
                        createTabletReshardJob(db, table, new MergeTabletClause());
                        LOG.info("Auto triggered merge tablet job for table {}.{}, minAdjacentTabletPairSize {}",
                                db.getFullName(), table.getName(), minAdjacentTabletPairSize);
                    } catch (EmptyReshardPlanException e) {
                        // Deterministic, so latch it: the same layout and configuration produce the
                        // same empty plan. Otherwise the table re-plans on every scan forever, walking
                        // every partition and index under the read lock each time. -1 is not a tracked
                        // job, which the latch's abort and settled probes both resolve to "no job" --
                        // correct, nothing is running. This mirrors the empty-plan half of the split
                        // path above and only that half: a merge job that does start is not recorded,
                        // so an aborting merge re-fires exactly as it did before this latch existed.
                        emptyMergePlanLatch.recordFired(tableId, mergeSignature, -1L,
                                mergeDecision.nextAbortRetries());
                        LOG.info("Merge produced no work for table {}.{}; suppressing until its layout "
                                        + "or configuration changes: {}",
                                db.getFullName(), table.getName(), e.getMessage());
                    }
                } else if (emptyMergePlanLatch.claimSuppressionLog(tableId)) {
                    LOG.info("Auto merge for table {}.{} produced no work on an unchanged layout; "
                                    + "suppressing further merge attempts until its data changes",
                            db.getFullName(), table.getName());
                }
            } else {
                // No merge signal, or merge is off: drop any suppression so it re-arms cleanly.
                emptyMergePlanLatch.forgetTable(tableId);
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
            emptyMergePlanLatch.clear();
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
        return candidate == null ? -1L : candidate.maxAdaptiveSplitTabletSize();
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
                emptyMergePlanLatch.forgetTable(tableId);
                continue;
            }
            triggerTabletReshard(db, (OlapTable) table, candidate.maxTabletSize(),
                    candidate.minAdjacentTabletPairSize(), candidate.maxAdaptiveSplitTabletSize(),
                    candidate.adaptiveBound());
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
