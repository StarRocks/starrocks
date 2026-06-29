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
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.FrontendDaemon;
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

public class TabletReshardJobMgr extends FrontendDaemon implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(TabletReshardJobMgr.class);

    @SerializedName(value = "tabletReshardJobs")
    protected final Map<Long, TabletReshardJob> tabletReshardJobs = Maps.newConcurrentMap();

    // Original tablet id -> resharding tablet info
    protected final Map<Long, ReshardingTabletInfo> reshardingTabletInfos = Maps.newConcurrentMap();

    // Colocate checker: stateless, invoked from this manager's tick. Owns no
    // thread of its own — shares this manager's scheduler cadence
    // ({@code tablet_reshard_job_scheduler_interval_ms}) and self-gates on shared-data-mode,
    // leader status, and empty unstable-groups before doing any real work.
    private final ColocateChecker colocateChecker = new ColocateChecker();

    // Coalescible reshard candidate for one table, marked by both the publish path and the periodic
    // TabletStatMgr scan: the largest tablet (split) and the smallest adjacent fresh-pair sum (merge).
    // Long.MAX_VALUE is the "no merge" identity, so a split-only publish mark and a split+merge periodic
    // mark compose by (max, min) regardless of arrival order. Self-contained (carries db/table id) so
    // the drain needs no side key. Transient (not persisted): leader failover falls back to the scan.
    private record ReshardCandidate(long dbId, long tableId,
                                    long maxTabletSize, long minAdjacentTabletPairSize) {
    }

    // tableId (globally unique) -> coalesced reshard candidate awaiting a drain evaluation.
    private final Map<Long, ReshardCandidate> reshardCandidates = new ConcurrentHashMap<>();

    // Enqueue a table for a reshard evaluation, carrying the signals its caller already computed so the
    // drain triggers without re-walking the table. Both the write-locked publish path (split-only:
    // minAdjacentTabletPairSize = Long.MAX_VALUE) and the periodic scan (split+merge) mark here;
    // concurrent marks for the same table coalesce by (max, min) before the next drain. Callers need
    // only supply the signals (and gate on leader/eligibility for their own reasons); the split/merge
    // actionability decision lives here, so non-actionable signals are dropped and never queued.
    public void addReshardCandidate(long dbId, long tableId, long maxTabletSize, long minAdjacentTabletPairSize) {
        if (!isLeaderAdmissionOpen()) {
            return;
        }
        // Keep the queue empty in the common (no-reshard) case; the drain re-checks authoritatively.
        if (!TabletReshardUtils.needSplit(maxTabletSize) && !TabletReshardUtils.needMerge(minAdjacentTabletPairSize)) {
            return;
        }
        reshardCandidates.merge(tableId,
                new ReshardCandidate(dbId, tableId, maxTabletSize, minAdjacentTabletPairSize),
                (existing, incoming) -> new ReshardCandidate(existing.dbId(), existing.tableId(),
                        Math.max(existing.maxTabletSize(), incoming.maxTabletSize()),
                        Math.min(existing.minAdjacentTabletPairSize(), incoming.minAdjacentTabletPairSize())));
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

    public void createTabletReshardJob(Database db, OlapTable table, SplitTabletClause splitTabletClause)
            throws StarRocksException {
        TabletReshardJob job = new SplitTabletJobFactory(db, table, splitTabletClause).createTabletReshardJob();
        addTabletReshardJob(job);
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

    /**
     * Reshard-trigger decision. The sole caller is {@link #drainReshardCandidates()}, which feeds the
     * signals that both the publish path and the periodic TabletStatMgr scan supplied via
     * {@link #addReshardCandidate}. Self-gates on leader/admission, cloud-native range distribution,
     * and NORMAL table state; the authoritative NORMAL re-check happens in the job factory under its
     * own lock.
     */
    private void triggerTabletReshard(Database db, OlapTable table,
                                      long maxTabletSize, long minAdjacentTabletPairSize) {
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
            if (TabletReshardUtils.needSplit(maxTabletSize)) {
                createTabletReshardJob(db, table, new SplitTabletClause());
                LOG.info("Auto triggered split tablet job for table {}.{}, maxTabletSize {}",
                        db.getFullName(), table.getName(), maxTabletSize);
                return;
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

    @Override
    protected void runAfterCatalogReady() {
        // Hard-gate the whole reshard tick: this is a FrontendDaemon (not stopped on demotion),
        // and neither runTabletReshardJobs() nor ColocateChecker.runOneCycle() self-gates on
        // leader/admission. Both create reshard jobs that journal via the non-throwing logJsonObject
        // path, so a demoted node must not run them.
        if (!isLeaderAdmissionOpen()) {
            reshardCandidates.clear();
            return;
        }
        colocateChecker.runOneCycle();
        drainReshardCandidates();
        runTabletReshardJobs();
    }

    @VisibleForTesting
    void runAfterCatalogReadyForTest() {
        runAfterCatalogReady();
    }

    @VisibleForTesting
    int getReshardCandidateCount() {
        return reshardCandidates.size();
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
            // db and table lookups are not atomic; the null guards are conservative — a dropped db/table
            // is simply skipped this cycle.
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(candidate.dbId());
            if (db == null) {
                continue;
            }
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(candidate.dbId(), candidate.tableId());
            if (!(table instanceof OlapTable)) {
                continue;
            }
            triggerTabletReshard(db, (OlapTable) table,
                    candidate.maxTabletSize(), candidate.minAdjacentTabletPairSize());
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

            // Job is done, remove expired job
            if (job.isExpired()) {
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
