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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RecycleMaterializedIndexInfo;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TTabletReshardJobsItem;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
 * TabletReshardJob is for tablet splitting and merging.
 */
public abstract class TabletReshardJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(TabletReshardJob.class);

    public enum JobState {
        PENDING, // Job is created
        PREPARING, // Creating new tablets
        RUNNING, // Do tablet splitting or merging
        CLEANING, // Clean old tablets
        FINISHED, // Job is finished
        ABORTING, // Job is aborting
        ABORTED; // Job is aborted

        public boolean isFinalState() {
            return this == JobState.FINISHED || this == JobState.ABORTED;
        }
    }

    public enum JobType {
        SPLIT_TABLET,
        MERGE_TABLET
    }

    @SerializedName(value = "jobId")
    protected final long jobId;

    @SerializedName(value = "jobType")
    protected final JobType jobType;

    @SerializedName(value = "jobState")
    protected volatile JobState jobState = JobState.PENDING;

    @SerializedName(value = "createdTimeMs")
    protected final long createdTimeMs = System.currentTimeMillis();
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs;
    @SerializedName(value = "stateStartedTimeMs")
    protected long stateStartedTimeMs = createdTimeMs;

    @SerializedName(value = "errorMessage")
    protected String errorMessage;

    // Reason of a publish failure the job is currently retrying, or null while every partition's
    // publish is healthy. The state lives per partition (see
    // ReshardingPhysicalPartition#publishFailureReason) rather than on the job, so a partition that
    // recovers stops reporting even while a sibling is still retrying, and it is never journaled --
    // a publish failure is always retried and never terminal. Surfaced through getInfo() so
    // information_schema.tablet_reshard_jobs explains a job stuck retrying a publish instead of
    // showing an empty ERROR_MESSAGE.
    protected abstract String anyPublishFailureReason();

    // ERROR_MESSAGE for information_schema.tablet_reshard_jobs: a terminal error always wins, and
    // otherwise the reason of a publish the job is still retrying.
    //
    // Only RUNNING retries a publish, so the reason is reported only in that state. That gate is what
    // keeps a reason from outliving the retry it describes: a partition can be dropped mid-job (DROP
    // PARTITION / TRUNCATE are permitted while the table is in TABLET_RESHARD), after which the
    // publish loop skips it and no publish result can clear its reason. runRunningJob() clears it on
    // that skip, but the gate also covers any future early-return added to the loop -- otherwise a
    // finished job would keep advertising a failure that already stopped being retried.
    protected String reportedErrorMessage() {
        if (errorMessage != null) {
            return errorMessage;
        }
        if (jobState != JobState.RUNNING) {
            return "";
        }
        String publishFailureReason = anyPublishFailureReason();
        return publishFailureReason == null ? "" : "publish version failed (retrying): " + publishFailureReason;
    }

    // The warehouse this job should run its compute work (shard creation + publish) in. Set by the
    // pre-split caller to the triggering load's warehouse; null for an online split / merge (and for a
    // job journaled before this field existed), which then fall back to the background warehouse.
    // Nullable so a missing field on replay deserializes to null (background), not 0 (a real warehouse).
    // Persisted so a leader-switch re-run targets the same warehouse.
    @SerializedName(value = "warehouseId")
    protected Long warehouseId;

    // Transactions that were opened only to reserve identifiers/metadata for the operation that is
    // synchronously waiting for this job, and that provably have not started writing yet. Excluding
    // them from the CLEANING watermark wait breaks the wait cycle where CLEANING waits for the
    // caller's transaction while the caller waits for this job.
    //
    // Deliberately NOT persisted, and cleared as soon as the caller stops waiting
    // (clearCleanupExcludedTransactionIds): the exclusion is only sound while the transaction is
    // known not to be writing. Once the caller proceeds -- or the leader changes and the caller is
    // gone -- CLEANING must go back to waiting for that transaction, otherwise it could unregister
    // the resharding tablets while the transaction is still writing to the old ones.
    // volatile + concurrent set: written by the submitting session thread, read by the reshard daemon.
    protected volatile Set<Long> cleanupExcludedTransactionIds;

    public TabletReshardJob(long jobId, JobType jobType) {
        this.jobId = jobId;
        this.jobType = jobType;
    }

    public long getJobId() {
        return jobId;
    }

    public JobType getJobType() {
        return jobType;
    }

    public Long getWarehouseId() {
        return warehouseId;
    }

    /**
     * Set the warehouse this job runs its compute work in. Called by the pre-split caller (before the
     * job is journaled) with the triggering load's warehouse, so shard creation and publish run there
     * rather than the background warehouse.
     */
    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    /**
     * Exclude a known-not-yet-writing transaction from this job's cleanup watermark wait, for as
     * long as its owner is synchronously waiting for this job. Callers must set this before the job
     * is admitted, and must call {@link #clearCleanupExcludedTransactionIds()} the moment they stop
     * waiting -- see the field comment for why the exclusion cannot outlive that window.
     */
    public void addCleanupExcludedTransactionId(long transactionId) {
        if (cleanupExcludedTransactionIds == null) {
            cleanupExcludedTransactionIds = ConcurrentHashMap.newKeySet();
        }
        cleanupExcludedTransactionIds.add(transactionId);
    }

    /**
     * Drop every cleanup-wait exclusion, so CLEANING waits for those transactions again. Called
     * once the waiting caller is about to proceed (its transaction may start writing at any moment)
     * or has given up on this job.
     */
    public void clearCleanupExcludedTransactionIds() {
        if (cleanupExcludedTransactionIds != null) {
            cleanupExcludedTransactionIds.clear();
        }
    }

    protected Set<Long> getCleanupExcludedTransactionIds() {
        return cleanupExcludedTransactionIds == null ? Set.of() : cleanupExcludedTransactionIds;
    }

    /**
     * Resolve the compute resource for this job's compute work: the explicitly-set warehouse when one
     * was provided (pre-split → the load's warehouse), otherwise the background warehouse (online
     * split / merge, or a job journaled before warehouseId existed).
     */
    protected ComputeResource resolveComputeResource(long tableId) {
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        return warehouseId == null
                ? warehouseMgr.getBackgroundComputeResource(tableId)
                : warehouseMgr.acquireComputeResource(warehouseId);
    }

    public JobState getJobState() {
        return jobState;
    }

    protected void setJobState(JobState jobState) {
        long currentTimeMs = System.currentTimeMillis();

        if (jobState.isFinalState()) {
            this.finishedTimeMs = currentTimeMs;
        }

        this.jobState = jobState;

        this.stateStartedTimeMs = currentTimeMs;

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateTabletReshardJob(this);

        LOG.info("Tablet reshard job set job state. {}", this);
    }

    public long getCreatedTimeMs() {
        return createdTimeMs;
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public boolean isExpired() {
        return isDone() &&
                (System.currentTimeMillis() - finishedTimeMs) > Config.tablet_reshard_history_job_keep_max_ms;
    }

    public boolean isDone() {
        return jobState.isFinalState();
    }

    public boolean isAborted() {
        return jobState == JobState.ABORTED;
    }

    protected boolean abort(String reason) {
        if (!canAbort()) {
            LOG.warn("Tablet reshard job cannot abort. {}", this);
            return false;
        }

        errorMessage = reason;
        setJobState(JobState.ABORTING);
        return true;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void run() {
        try {
            JobState prevState = null;
            do {
                prevState = jobState;
                switch (prevState) {
                    case PENDING:
                        runPendingJob();
                        break;
                    case PREPARING:
                        runPreparingJob();
                        break;
                    case RUNNING:
                        runRunningJob();
                        break;
                    case CLEANING:
                        runCleaningJob();
                        break;
                    case FINISHED:
                        runFinishedJob();
                        break;
                    case ABORTING:
                        runAbortingJob();
                        break;
                    case ABORTED:
                        runAbortedJob();
                        break;
                    default:
                        LOG.warn("Invalid state in tablet reshard job, try to abort. {}", this);
                        abort("Invalid state: " + jobState);
                        break;
                }
            } while (jobState != prevState);
        } catch (Exception e) {
            LOG.warn("Failed to run tablet reshard job, try to abort. {}. Exception: ",
                    this, e);
            abort(e.getMessage());
        }
    }

    public void replay() {
        try {
            switch (jobState) {
                case PENDING:
                    replayPendingJob();
                    break;
                case PREPARING:
                    replayPreparingJob();
                    break;
                case RUNNING:
                    replayRunningJob();
                    break;
                case CLEANING:
                    replayCleaningJob();
                    break;
                case FINISHED:
                    replayFinishedJob();
                    break;
                case ABORTING:
                    replayAbortingJob();
                    break;
                case ABORTED:
                    replayAbortedJob();
                    break;
                default:
                    LOG.warn("Invalid state in tablet reshard job. {}", this);
                    break;
            }
        } catch (Exception e) {
            LOG.warn("Caught exception when replay tablet reshard job. {}. ", this, e);
        }
    }

    public abstract long getParallelTablets();

    public abstract long getTableId();

    /*
     * Admission-time reservation. Reserve the table for this job before it is queued in
     * TabletReshardJobMgr. Must succeed before the job becomes visible to the scheduler, so that
     * an admitted job is guaranteed runnable and never forced to abort at execution time due to an
     * unexpected table state. Throws if the table is not reservable (not NORMAL / dropped).
     */
    public abstract void init() throws StarRocksException;

    protected abstract void runPendingJob();

    protected abstract void runPreparingJob();

    protected abstract void runRunningJob();

    protected abstract void runCleaningJob();

    protected abstract void runFinishedJob();

    protected abstract void runAbortingJob();

    protected abstract void runAbortedJob();

    protected abstract boolean canAbort();

    protected abstract void replayPendingJob();

    protected abstract void replayPreparingJob();

    protected abstract void replayRunningJob();

    protected abstract void replayCleaningJob();

    protected abstract void replayFinishedJob();

    protected abstract void replayAbortingJob();

    protected abstract void replayAbortedJob();

    protected abstract void registerReshardingTabletsOnRestart();

    public abstract TTabletReshardJobsItem getInfo();

    /**
     * Admission-time reservation body, shared by the split and merge jobs. The caller runs it under
     * the table WRITE lock: the table must be NORMAL, every index the job is about to reshard must
     * still be the live version of its index meta, and only then does the table flip to
     * {@code TABLET_RESHARD}.
     *
     * <p>All three steps belong in one lock scope. The job was built from a snapshot the factory took
     * under a lock it has since released, so the layout can have moved on; and the state flip is what
     * stops it from moving again, because the factories and this method both require NORMAL, so no
     * second reshard job can be admitted while this one holds the table. Validating before the flip
     * and under the same lock is therefore the only point at which "these indexes are live" can be
     * established and stay true for the rest of the job.
     */
    protected static void reserveTableForReshard(long dbId, OlapTable olapTable,
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) throws StarRocksException {
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new TabletReshardException(
                    "Unexpected table state " + olapTable.getState() + " in table " + olapTable.getName());
        }
        checkReshardingIndexesStillLatest(dbId, olapTable, reshardingPhysicalPartitions);
        olapTable.setState(OlapTable.OlapTableState.TABLET_RESHARD);
    }

    private static void checkReshardingIndexesStillLatest(long dbId, OlapTable olapTable,
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) throws StarRocksException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        String dbName = db == null ? "" : db.getFullName();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            PhysicalPartition physicalPartition = olapTable
                    .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
            if (physicalPartition == null) {
                // Dropped in the same gap (DROP PARTITION / TRUNCATE are permitted alongside a reshard).
                // Every later step of the job already skips a partition that is missing, so this is not
                // a reason to reject the job.
                continue;
            }
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                // The new index the job will install carries the source index's meta id, so the meta id
                // is still available even when the source index itself is already gone.
                TabletReshardUtils.checkIndexStillLatest(physicalPartition,
                        reshardingIndex.getMaterializedIndexId(),
                        reshardingIndex.getMaterializedIndex().getMetaId(),
                        dbName, olapTable.getName());
            }
        }
    }

    /**
     * Shared reshard-cleanup step for split and merge: for every superseded (old) materialized index,
     * schedule its removal in the {@code CatalogRecycleBin} at index granularity, so an in-flight query
     * planned against it can keep reading until the retention (partition_recycle_retention_period_secs)
     * expires (issue #75993).
     *
     * <p>Crucially, the old index is <b>left installed</b> on its live partition; only a retention
     * record is parked. A split reuses the parent's shard group for the child, so the group is never
     * orphaned; keeping the old index installed is what protects its shards, because
     * {@code StarMgrMetaSyncer.syncTableMetaInternal} reaps a group's shards per-shard by subtracting
     * the tablets of every index still on the partition. New writes resolve through the writable APIs
     * and use only the child layout. Queries resolve through the queryable APIs: an ORDER BY != PK split
     * deliberately pins them to the old parent until UNSHARE finishes, while other reshard operations
     * switch immediately. On erase, the recycle bin detaches the old index and drops its tablets, and
     * {@code StarMgrMetaSyncer} then reclaims the now-unreferenced shards per-shard --
     * never a partition-directory delete, which for a split would destroy the live child tablets that
     * share the parent's object-storage directory.
     *
     * <p>Runs on both the leader (runCleaningJob) and the replay path (replayFinishedJob); it is
     * deterministic (the index keeps its own id, no id allocation) and
     * {@code recycleMaterializedIndex} is idempotent, so a re-run is safe. The caller holds the table
     * WRITE lock and passes the locked table.
     */
    protected static void recycleOldMaterializedIndexes(long dbId, OlapTable olapTable,
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) {
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            long physicalPartitionId = reshardingPhysicalPartition.getPhysicalPartitionId();
            PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionId);
            if (physicalPartition == null) {
                continue;
            }
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                long oldIndexId = reshardingIndex.getMaterializedIndexId();
                // Idempotency guard: if the old index has already been erased (detached by a prior
                // retention cycle), there is nothing to schedule.
                if (physicalPartition.getIndex(oldIndexId) == null) {
                    continue;
                }
                GlobalStateMgr.getCurrentState().getRecycleBin().recycleMaterializedIndex(
                        new RecycleMaterializedIndexInfo(dbId, olapTable.getId(),
                                physicalPartition.getParentId(), physicalPartitionId, oldIndexId,
                                reshardingPhysicalPartition.getCommitVersion()));
            }
        }
    }

    /**
     * Shared reshard-completion step for split and merge: drop the creation-time placement pin on
     * every new shard, so the background balancer can spread them right away. StarOS only drops the
     * pin on its own once the superseded (old / source) shards are reclaimed, which happens a
     * recycle-bin retention plus a {@code StarMgrMetaSyncer} cycle later -- and for the whole of
     * that window the new shards cannot be moved off the source worker at all.
     *
     * <p>Best-effort by design: a failure only degrades to that old behavior, so it must never
     * interrupt the job. The caller is the leader-only cleaning path; replay paths do not call this.
     */
    protected void clearPlacementPreference(
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) {
        // Name the members of each preference group rather than just the new shards: StarOS needs to
        // know which preference is meant, since a new shard here becomes the pin target of the next
        // reshard on the same tablet. Every (superseded, new) combination of a resharding tablet is
        // exactly one preference, which reproduces what createShardsForSplit/ForMerge established --
        // a split pins each child to its one parent, a merge pins the one output to each source.
        List<List<Long>> preferenceMembers = new ArrayList<>();
        for (ReshardingPhysicalPartition partition : reshardingPhysicalPartitions.values()) {
            for (ReshardingMaterializedIndex index : partition.getReshardingIndexes().values()) {
                for (ReshardingTablet tablet : index.getReshardingTablets()) {
                    for (long oldTabletId : tablet.getOldTabletIds()) {
                        for (long newTabletId : tablet.getNewTabletIds()) {
                            preferenceMembers.add(List.of(oldTabletId, newTabletId));
                        }
                    }
                }
            }
        }
        if (preferenceMembers.isEmpty()) {
            return;
        }
        try {
            GlobalStateMgr.getCurrentState().getStarOSAgent().clearPlacementPreference(preferenceMembers);
        } catch (Exception e) {
            // Log the throwable, not just its message: this catch is broad enough to swallow a
            // programming error (an NPE would otherwise be recorded as a bare "null"), and the job
            // goes on to FINISHED either way, so the stack trace is the only trace left.
            LOG.warn("Failed to clear placement preference for reshard job {}", jobId, e);
        }
    }
}
