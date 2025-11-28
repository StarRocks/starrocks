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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.lake.Utils;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletReshardJobsItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/*
 * TabletReshardJob is for tablet splitting and merging.
 */
public class TabletReshardJob implements Writable {
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

    @SerializedName(value = "dbId")
    protected final long dbId;
    @SerializedName(value = "tableId")
    protected final long tableId;

    @SerializedName(value = "createdTimeMs")
    protected final long createdTimeMs = System.currentTimeMillis();
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs;
    @SerializedName(value = "stateStartedTimeMs")
    protected long stateStartedTimeMs = createdTimeMs;

    @SerializedName(value = "errorMessage")
    protected String errorMessage;

    // Original physical partition id -> new physical partition context
    @SerializedName(value = "physicalPartitionContexts")
    protected final Map<Long, PhysicalPartitionContext> physicalPartitionContexts;

    // The split or merge transaction id.
    // Transactions began before it may still use old metadata objects.
    // They should be deleted after these transactions are finished.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId;

    @SerializedName(value = "watershedGtid")
    protected long watershedGtid;

    public TabletReshardJob(long jobId, JobType jobType, long dbId, long tableId,
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
        long currentTimeMs = System.currentTimeMillis();

        if (jobState.isFinalState()) {
            this.finishedTimeMs = currentTimeMs;
        }

        this.jobState = jobState;

        this.stateStartedTimeMs = currentTimeMs;

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateTabletReshardJob(this);

        LOG.info("Tablet reshard job set job state. {}", this);
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
                (System.currentTimeMillis() - finishedTimeMs) > Config.tablet_reshard_history_job_keep_max_ms;
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

    // Begin and commit the split transaction
    protected void runPendingJob() {
        setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.TABLET_RESHARD);

        updateNextVersion();

        addTabletsToInvertedIndex();

        registerReshardingTablets();

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
            List<String> distributionColumns = olapTable.getDefaultDistributionInfo().getDistributionColumns().stream()
                    .map(ColumnId::getId).collect(Collectors.toList());
            boolean useAggregatePublish = olapTable.isFileBundling();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }

                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();
                PhysicalPartitionContext.PublishState publishState = physicalPartitionContext.getPublishState();
                if (publishState == PhysicalPartitionContext.PublishState.PUBLISH_SUCCESS) { // Publish success
                    continue;
                }

                allPartitionPublished = false;

                if (publishState == PhysicalPartitionContext.PublishState.PUBLISH_IN_PROGRESS) { // Publish in progress
                    continue;
                }

                // Publish async
                List<Tablet> tablets = new ArrayList<>();
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    tablets.addAll(index.getTablets());
                }
                long commitVersion = physicalPartitionContext.getCommitVersion();
                Future<Boolean> future = publishThreadPool.submit(() -> publishVersion(
                        tablets, commitVersion, distributionColumns, useAggregatePublish));
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
        } catch (Exception e) { // Db is dropped, ignore exception
            LOG.warn("Ignore exception when waiting previous transactions finished. " + this, e);
        }

        removePhysicalPartitions();

        unregisterReshardingTablets();

        setTableState(OlapTable.OlapTableState.TABLET_RESHARD, OlapTable.OlapTableState.NORMAL);

        // Clear to release memory
        physicalPartitionContexts.clear();

        setJobState(JobState.FINISHED);
    }

    // Clear resharding tablets
    protected void runAbortingJob() {
        try {
            unregisterReshardingTablets();

            setTableState(null, OlapTable.OlapTableState.NORMAL);
        } catch (Exception e) {
            LOG.warn("Ignore exception when aborting tablet reshard job. {}. ", this, e);
        }

        // Clear to release memory
        physicalPartitionContexts.clear();

        setJobState(JobState.ABORTED);
    }

    // Can abort only when job state is PENDING
    protected boolean canAbort() {
        return jobState == JobState.PENDING;
    }

    protected void onJobDone() {
        LOG.info("Tablet reshard job is done. {}", this);
    }

    public void replay() {
        try {
            switch (jobState) {
                case PENDING:
                    break;
                case PREPARING:
                    setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.TABLET_RESHARD);
                    updateNextVersion();
                    addTabletsToInvertedIndex();
                    registerReshardingTablets();
                    break;
                case RUNNING:
                    break;
                case CLEANING:
                    replacePhysicalPartitions();
                    break;
                case FINISHED:
                    removePhysicalPartitions();
                    unregisterReshardingTablets();
                    setTableState(OlapTable.OlapTableState.TABLET_RESHARD, OlapTable.OlapTableState.NORMAL);
                    break;
                case ABORTING:
                    break;
                case ABORTED:
                    unregisterReshardingTablets();
                    setTableState(null, OlapTable.OlapTableState.NORMAL);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            LOG.warn("Caught exception when replay tablet reshard job. {}. ", this, e);
        }
    }

    private void setTableState(OlapTable.OlapTableState expectedState, OlapTable.OlapTableState newState) {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            if (expectedState != null && olapTable.getState() != expectedState) {
                throw new TabletReshardJobException("Unexpected table state " + olapTable.getState() + ". " + this);
            }
            olapTable.setState(newState);
        }
    }

    private void updateNextVersion() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition oldPhysicalPartition = olapTable
                        .getPhysicalPartition(physicalPartitionEntry.getKey());
                if (oldPhysicalPartition == null) {
                    continue;
                }
                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();
                PhysicalPartition newPhysicalPartition = physicalPartitionContext.getPhysicalPartition();

                long commitVersion = physicalPartitionContext.getCommitVersion();
                if (commitVersion <= 0) {
                    commitVersion = oldPhysicalPartition.getNextVersion();
                    physicalPartitionContext.setCommitVersion(commitVersion);
                }

                long nextVersion = commitVersion + 1;
                oldPhysicalPartition.setNextVersion(nextVersion);
                newPhysicalPartition.setNextVersion(nextVersion);
            }
        }
    }

    private boolean publishVersion(List<Tablet> tablets, long commitVersion, List<String> distributionColumns,
            boolean useAggregatePublish) {
        try {
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = watershedTxnId;
            txnInfo.combinedTxnLog = false;
            txnInfo.commitTime = stateStartedTimeMs / 1000;
            txnInfo.txnType = TxnTypePB.TXN_TABLET_RESHARD;
            txnInfo.gtid = watershedGtid;

            Utils.publishVersion(tablets, txnInfo, commitVersion - 1, commitVersion, null, distributionColumns,
                    WarehouseManager.DEFAULT_RESOURCE, null, useAggregatePublish);

            return true;
        } catch (Exception e) {
            LOG.warn("Failed to publish version for tablet reshard job {}. ", this, e);
            return false;
        }
    }

    private void replacePhysicalPartitions() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition oldPhysicalPartition = olapTable
                        .getPhysicalPartition(physicalPartitionEntry.getKey());
                if (oldPhysicalPartition == null) {
                    continue;
                }
                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();
                PhysicalPartition newPhysicalPartition = physicalPartitionContext.getPhysicalPartition();

                long commitVersion = physicalPartitionContext.getCommitVersion();
                Preconditions.checkState(commitVersion == oldPhysicalPartition.getVisibleVersion() + 1,
                        "commit version: " + commitVersion + ", visible version: "
                                + oldPhysicalPartition.getVisibleVersion());

                oldPhysicalPartition.setVisibleVersion(commitVersion, stateStartedTimeMs);
                newPhysicalPartition.setVisibleVersion(commitVersion, stateStartedTimeMs);

                // Temporary code, ignore, will be replaced later
                var partition = olapTable.getPartition(oldPhysicalPartition.getParentId());
                if (partition != null) {
                    partition.removeSubPartition(oldPhysicalPartition.getId());
                    partition.addSubPartition(newPhysicalPartition);
                    olapTable.removePhysicalPartition(oldPhysicalPartition);
                    olapTable.addPhysicalPartition(newPhysicalPartition);
                }
            }
        }
    }

    private void removePhysicalPartitions() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (long physicalPartitionId : physicalPartitionContexts.keySet()) {
                // Temporary code, ignore, will be replaced later
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionId);
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
        for (PhysicalPartitionContext physicalPartitionContext : physicalPartitionContexts.values()) {
            PhysicalPartition physicalPartition = physicalPartitionContext.getPhysicalPartition();
            for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                // Use HDD in shared-data mode
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartition.getId(), index.getId(),
                        TStorageMedium.HDD, true);
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.addTablet(tablet.getId(), tabletMeta);
                }
            }
        }
    }

    private LockedObject<OlapTable> getLockedTable(LockType lockType) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) { // Table is dropped
            errorMessage = "Table not found";
            setJobState(JobState.ABORTING);
            throw new TabletReshardJobException("Table not found. " + this);
        }

        OlapTable olapTable = (OlapTable) table;

        return new LockedObject<OlapTable>(dbId, List.of(tableId), lockType, olapTable);
    }

    protected void registerReshardingTabletsOnRestart() {
        if (jobState == JobState.PENDING) {
            return;
        }

        registerReshardingTablets();
    }

    private void registerReshardingTablets() {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        for (PhysicalPartitionContext physicalPartitionContext : physicalPartitionContexts.values()) {
            long visibleVersion = physicalPartitionContext.getCommitVersion();

            for (ReshardingTablets reshardingTablets : physicalPartitionContext.getReshardingTabletses().values()) {
                for (SplittingTablet splittingTablet : reshardingTablets.getSplittingTablets()) {
                    tabletReshardJobMgr.registerReshardingTablet(splittingTablet.getOldTabletId(),
                            splittingTablet, visibleVersion);
                }
                for (MergingTablet mergingTablet : reshardingTablets.getMergingTablets()) {
                    for (Long tabletId : mergingTablet.getOldTabletIds()) {
                        tabletReshardJobMgr.registerReshardingTablet(tabletId, mergingTablet, visibleVersion);
                    }
                }
                for (IdenticalTablet identicalTablet : reshardingTablets.getIdenticalTablets()) {
                    tabletReshardJobMgr.registerReshardingTablet(identicalTablet.getOldTabletId(),
                            identicalTablet, visibleVersion);
                }
            }
        }
    }

    protected void unregisterReshardingTablets() {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        for (PhysicalPartitionContext physicalPartitionContext : physicalPartitionContexts.values()) {
            for (ReshardingTablets reshardingTablets : physicalPartitionContext.getReshardingTabletses().values()) {
                for (SplittingTablet splittingTablet : reshardingTablets.getSplittingTablets()) {
                    tabletReshardJobMgr.unregisterReshardingTablet(splittingTablet.getOldTabletId());
                }
                for (MergingTablet mergingTablet : reshardingTablets.getMergingTablets()) {
                    for (Long tabletId : mergingTablet.getOldTabletIds()) {
                        tabletReshardJobMgr.unregisterReshardingTablet(tabletId);
                    }
                }
                for (IdenticalTablet identicalTablet : reshardingTablets.getIdenticalTablets()) {
                    tabletReshardJobMgr.unregisterReshardingTablet(identicalTablet.getOldTabletId());
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TabletReshardJob: {");
        sb.append("job_id: ").append(jobId);
        sb.append(", job_type: ").append(jobType);
        sb.append(", job_state: ").append(jobState);
        sb.append(", db_id: ").append(dbId);
        sb.append(", table_id: ").append(tableId);
        sb.append(", created_time: ").append(TimeUtils.longToTimeString(createdTimeMs));
        if (finishedTimeMs > 0) {
            sb.append(", finished_time:").append(TimeUtils.longToTimeString(finishedTimeMs));
        } else {
            sb.append(", state_started_time: ").append(TimeUtils.longToTimeString(stateStartedTimeMs));
        }
        if (errorMessage != null) {
            sb.append(", error_message: ").append(errorMessage);
        }
        sb.append(", txn_id: ").append(watershedTxnId);
        sb.append(", parallel_partitions: ").append(physicalPartitionContexts.size());
        sb.append("}");
        return sb.toString();
    }

    public TTabletReshardJobsItem getInfo() {
        TTabletReshardJobsItem item = new TTabletReshardJobsItem();
        item.setJob_id(jobId);
        item.setDb_id(dbId);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            item.setDb_name("");
            LOG.warn("Failed to get database name for tablet reshard job. {}", this);
        } else {
            item.setDb_name(db.getFullName());
        }

        item.setTable_id(tableId);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            item.setTable_name("");
            LOG.warn("Failed to get table name for tablet reshard job. {}", this);
        } else {
            item.setTable_name(table.getName());
        }
        item.setJob_type(jobType.name());
        item.setJob_state(jobState.name());
        item.setTransaction_id(watershedTxnId);
        item.setParallel_partitions(physicalPartitionContexts.size());
        item.setParallel_tablets(getParallelTablets());
        item.setCreated_time(createdTimeMs / 1000);
        item.setFinished_time(finishedTimeMs / 1000);
        if (errorMessage != null) {
            item.setError_message(errorMessage);
        } else {
            item.setError_message("");
        }
        return item;
    }

}
