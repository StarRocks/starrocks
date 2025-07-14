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

package com.starrocks.alter.dynamictablet;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.LockedObject;
import com.starrocks.lake.Utils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/*
 * DynamicTabletJob is for dynamic tablet splitting and merging.
 * This is the base class of SplitTabletJob and MergeTabletJob
 */
public abstract class DynamicTabletJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(DynamicTabletJob.class);

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

    @SerializedName(value = "dbId")
    protected final long dbId;
    @SerializedName(value = "tableId")
    protected final long tableId;

    @SerializedName(value = "createdTimeMs")
    protected final long createdTimeMs = System.currentTimeMillis();
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs;

    @SerializedName(value = "errorMessage")
    protected String errorMessage;

    // Physical partition id -> PhysicalPartitionContext
    @SerializedName(value = "physicalPartitionContexts")
    protected final Map<Long, PhysicalPartitionContext> physicalPartitionContexts;

    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId;

    @SerializedName(value = "watershedGtid")
    protected long watershedGtid;

    public DynamicTabletJob(long jobId, JobType jobType, long dbId, long tableId,
            Map<Long, PhysicalPartitionContext> physicalPartitionContexts) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.dbId = dbId;
        this.tableId = tableId;
        this.physicalPartitionContexts = physicalPartitionContexts;
    }

    public long getJobId() {
        return jobId;
    }

    public JobType getJobType() {
        return jobType;
    }

    public JobState getJobState() {
        return jobState;
    }

    protected void setJobState(JobState jobState) {
        this.jobState = jobState;
        this.finishedTimeMs = System.currentTimeMillis();

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateDynamicTabletJob(this);
        LOG.info("Dynamic tablet job set job state. {}", this);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getCreatedTimeMs() {
        return createdTimeMs;
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public boolean isExpired() {
        return isDone() &&
                (System.currentTimeMillis() - finishedTimeMs) > Config.dynamic_tablet_history_job_keep_max_ms;
    }

    public boolean isDone() {
        return jobState.isFinalState();
    }

    protected boolean abort(String reason) {
        if (!canAbort()) {
            return false;
        }

        errorMessage = reason;
        setJobState(JobState.ABORTING);
        return true;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (PhysicalPartitionContext physicalPartitionContext : physicalPartitionContexts.values()) {
            parallelTablets += physicalPartitionContext.getParallelTablets();
        }
        return parallelTablets;
    }

    public long getTransactionId() {
        return watershedTxnId;
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
                        onJobDone();
                        break;
                    case ABORTING:
                        runAbortingJob();
                        break;
                    case ABORTED:
                        onJobDone();
                        break;
                    default:
                        LOG.warn("Invalid state in dynamic tablet job, try to abort. {}", this);
                        abort("Invalid state: " + jobState);
                        break;
                }
            } while (jobState != prevState);
        } catch (Exception e) {
            LOG.warn("Failed to run dynamic tablet job, try to abort. {}. Exception: ",
                    this, e);
            abort(e.getMessage());
        }
    }

    // Begin and commit the split transaction
    protected void runPendingJob() {
        setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.DYNAMIC_TABLET);

        updateNextVersion();

        registerDynamicTablets();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        watershedTxnId = globalStateMgr.getGlobalTransactionMgr().getTransactionIDGenerator()
                .getNextTransactionId();
        watershedGtid = globalStateMgr.getGtidGenerator().nextGtid();

        setJobState(JobState.PREPARING);
    }

    // Wait for previous versions published
    protected void runPreparingJob() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }

                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();
                long commitVersion = physicalPartitionContext.getCommitVersion();
                long visibleVersion = physicalPartition.getVisibleVersion();
                if (commitVersion != visibleVersion + 1) {
                    Preconditions.checkState(visibleVersion < commitVersion,
                            "partition=" + physicalPartition.getId() + " visibleVersion="
                                    + visibleVersion + " commitVersion=" + commitVersion);
                    return;
                }
            }
        }

        setJobState(JobState.RUNNING);
    }

    // Publish the split transaction, replace old materialized indexes with new ones
    protected void runRunningJob() {
        boolean allPartitionPublished = true;
        ThreadPoolExecutor publishThreadPool = GlobalStateMgr.getCurrentState().getPublishVersionDaemon()
                .getLakeTaskExecutor();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            boolean useAggregatePublish = olapTable.isFileBundling();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }

                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();
                int publishState = physicalPartitionContext.getPublishState();
                if (publishState > 0) { // Publish success
                    continue;
                }

                allPartitionPublished = false;

                if (publishState == 0) { // Publish in progress
                    continue;
                }

                // Publish async
                List<Tablet> tablets = new ArrayList<>();
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    tablets.addAll(index.getTablets());
                }
                Future<Boolean> future = publishThreadPool.submit(() -> publishVersion(tablets,
                        physicalPartitionContext.getCommitVersion(), useAggregatePublish));
                physicalPartitionContext.setPublishFuture(future);
            }
        }

        if (!allPartitionPublished) {
            return;
        }

        replacePhysicalPartitions();

        setJobState(JobState.CLEANING);
    }

    // Wait for all previous transactions finished, than delete old tablets
    protected void runCleaningJob() {
        try {
            if (!GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().isPreviousTransactionsFinished(
                    watershedTxnId, dbId, List.of(tableId))) {
                return;
            }
        } catch (Exception e) {
            throw new DynamicTabletJobException("Failed to wait previous transactions finished. " + this, e);
        }

        removePhysicalPartitions();

        unregisterDynamicTablets();

        setTableState(OlapTable.OlapTableState.DYNAMIC_TABLET, OlapTable.OlapTableState.NORMAL);

        setJobState(JobState.FINISHED);
    }

    // Clear new tablets
    protected void runAbortingJob() {
        unregisterDynamicTablets();

        setTableState(null, OlapTable.OlapTableState.NORMAL);

        setJobState(JobState.ABORTED);
    }

    // Can abort only when job state is PENDING
    protected boolean canAbort() {
        return jobState == JobState.PENDING;
    }

    protected void onJobDone() {
        // Clear to release memory
        physicalPartitionContexts.clear();

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateDynamicTabletJob(this);

        LOG.info("Dynamic tablet job is done. {}", this);
    }

    public void replay() {
        switch (jobState) {
            case PENDING:
                addTabletsToInvertedIndex();
                break;
            case PREPARING:
                setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.DYNAMIC_TABLET);
                updateNextVersion();
                registerDynamicTablets();
                break;
            case RUNNING:
                break;
            case CLEANING:
                replacePhysicalPartitions();
                break;
            case FINISHED:
                removePhysicalPartitions();
                unregisterDynamicTablets();
                setTableState(OlapTable.OlapTableState.DYNAMIC_TABLET, OlapTable.OlapTableState.NORMAL);
                break;
            case ABORTING:
                break;
            case ABORTED:
                unregisterDynamicTablets();
                setTableState(null, OlapTable.OlapTableState.NORMAL);
                break;
            default:
                break;
        }
    }

    private void setTableState(OlapTable.OlapTableState expectedState, OlapTable.OlapTableState newState) {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            if (expectedState != null && olapTable.getState() != expectedState) {
                throw new DynamicTabletJobException("Unexpected table state " + olapTable.getState() + ". " + this);
            }
            olapTable.setState(newState);
        }
    }

    private void updateNextVersion() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }
                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();

                long commitVersion = physicalPartitionContext.getCommitVersion();
                if (commitVersion <= 0) { // Not in replay
                    commitVersion = physicalPartition.getNextVersion();
                    physicalPartitionContext.setCommitVersion(commitVersion);
                }
                physicalPartition.setNextVersion(commitVersion + 1);
            }
        }
    }

    private boolean publishVersion(List<Tablet> tablets, long commitVersion, boolean useAggregatePublish) {
        try {
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = watershedTxnId;
            txnInfo.combinedTxnLog = false;
            txnInfo.commitTime = finishedTimeMs / 1000;
            txnInfo.txnType = jobType == JobType.SPLIT_TABLET ? TxnTypePB.TXN_SPLIT_TABLET : TxnTypePB.TXN_MERGE_TABLET;
            txnInfo.gtid = watershedGtid;

            // TODO: Add distribution columns
            Utils.publishVersion(tablets, txnInfo, commitVersion - 1, commitVersion, null,
                    WarehouseManager.DEFAULT_RESOURCE, null, useAggregatePublish);

            return true;
        } catch (Exception e) {
            LOG.warn("Failed to publish version for dynamic tablet job {}. ", this, e);
            return false;
        }
    }

    private void replacePhysicalPartitions() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }
                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();

                long commitVersion = physicalPartitionContext.getCommitVersion();
                Preconditions.checkState(commitVersion == physicalPartition.getVisibleVersion() + 1,
                        "commit version: " + commitVersion + ", visible version: "
                                + physicalPartition.getVisibleVersion());
                physicalPartition.setVisibleVersion(commitVersion, finishedTimeMs);

                long newPhysicalPartitionId = physicalPartitionContext.getNewPhysicalPartitionId();
                if (newPhysicalPartitionId <= 0) {
                    newPhysicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
                    physicalPartitionContext.setNewPhysicalPartitionId(newPhysicalPartitionId);
                }

                PhysicalPartition newPhysicalPartition = new PhysicalPartition(newPhysicalPartitionId,
                        physicalPartition);
                Map<Long, MaterializedIndexContext> indexContexts = physicalPartitionContext.getIndexContexts();
                for (MaterializedIndexContext indexContext : indexContexts.values()) {
                    MaterializedIndex newIndex = indexContext.getMaterializedIndex();
                    if (newIndex.getId() == physicalPartition.getBaseIndex().getId()) {
                        newPhysicalPartition.setBaseIndex(newIndex);
                    } else {
                        newPhysicalPartition.createRollupIndex(newIndex);
                    }
                }

                olapTable.replacePhysicalPartition(physicalPartition.getId(), newPhysicalPartition, true);
            }
        }
    }

    private void removePhysicalPartitions() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (long physicalPartitionId : physicalPartitionContexts.keySet()) {
                PhysicalPartition physicalPartition = olapTable.removePhysicalPartition(physicalPartitionId);
                if (physicalPartition == null) {
                    continue;
                }

                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
        }
    }

    private void addTabletsToInvertedIndex() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }
                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();

                TStorageMedium storageMedium = olapTable.getPartitionInfo()
                        .getDataProperty(physicalPartition.getParentId()).getStorageMedium();
                for (MaterializedIndexContext indexContext : physicalPartitionContext.getIndexContexts().values()) {
                    MaterializedIndex newIndex = indexContext.getMaterializedIndex();
                    TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartition.getId(), newIndex.getId(),
                            storageMedium, olapTable.isCloudNativeTableOrMaterializedView());

                    for (Tablet tablet : newIndex.getTablets()) {
                        invertedIndex.addTablet(tablet.getId(), tabletMeta);
                    }
                }
            }
        }
    }

    protected LockedObject<OlapTable> getLockedTable(LockType lockType) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) { // Table is dropped
            errorMessage = "Table not found";
            setJobState(JobState.ABORTING);
            throw new DynamicTabletJobException("Table not found. " + this);
        }

        OlapTable olapTable = (OlapTable) table;

        return new LockedObject<OlapTable>(dbId, List.of(tableId), lockType, olapTable);
    }

    protected void registerDynamicTablets() {
        DynamicTabletJobMgr dynamicTabletJobMgr = GlobalStateMgr.getCurrentState().getDynamicTabletJobMgr();
        for (PhysicalPartitionContext physicalPartitionContext : physicalPartitionContexts.values()) {
            long visibleVersion = physicalPartitionContext.getCommitVersion();
            for (MaterializedIndexContext indexContext : physicalPartitionContext.getIndexContexts().values()) {
                DynamicTablets dynamicTablets = indexContext.getDynamicTablets();
                for (SplittingTablet splittingTablet : dynamicTablets.getSplittingTablets().values()) {
                    dynamicTabletJobMgr.registerDynamicTablet(splittingTablet.getOldTabletId(),
                            splittingTablet, visibleVersion);
                }
                for (MergingTablet mergingTablet : dynamicTablets.getMergingTablets()) {
                    for (Long tabletId : mergingTablet.getOldTabletIds()) {
                        dynamicTabletJobMgr.registerDynamicTablet(tabletId, mergingTablet, visibleVersion);
                    }
                }
            }
        }
    }

    protected void unregisterDynamicTablets() {
        DynamicTabletJobMgr dynamicTabletJobMgr = GlobalStateMgr.getCurrentState().getDynamicTabletJobMgr();
        for (PhysicalPartitionContext physicalPartitionContext : physicalPartitionContexts.values()) {
            for (MaterializedIndexContext indexContext : physicalPartitionContext.getIndexContexts().values()) {
                DynamicTablets dynamicTablets = indexContext.getDynamicTablets();
                for (SplittingTablet splittingTablet : dynamicTablets.getSplittingTablets().values()) {
                    dynamicTabletJobMgr.unregisterDynamicTablet(splittingTablet.getOldTabletId());
                }
                for (MergingTablet mergingTablet : dynamicTablets.getMergingTablets()) {
                    for (Long tabletId : mergingTablet.getOldTabletIds()) {
                        dynamicTabletJobMgr.unregisterDynamicTablet(tabletId);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DynamicTabletJob: {");
        sb.append("job_id: ").append(jobId);
        sb.append(", job_type: ").append(jobType);
        sb.append(", job_state: ").append(jobState);
        sb.append(", db_id: ").append(dbId);
        sb.append(", table_id: ").append(tableId);
        sb.append(", created_time: ").append(TimeUtils.longToTimeString(createdTimeMs));
        if (finishedTimeMs > 0) {
            sb.append(", finished_time:").append(TimeUtils.longToTimeString(finishedTimeMs));
        }
        if (errorMessage != null) {
            sb.append(", error_message: ").append(errorMessage);
        }
        sb.append(", txn_id: ").append(watershedTxnId);
        sb.append("}");
        return sb.toString();
    }

    public static DynamicTabletJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DynamicTabletJob.class);
    }
}
