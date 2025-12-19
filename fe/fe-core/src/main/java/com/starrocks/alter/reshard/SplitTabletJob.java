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
import com.starrocks.alter.reshard.ReshardingPhysicalPartition.PublishResult;
import com.starrocks.alter.reshard.ReshardingPhysicalPartition.PublishState;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.AnalysisException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/*
 * SplitTabletJob is for tablet splitting.
 */
public class SplitTabletJob extends TabletReshardJob {
    private static final Logger LOG = LogManager.getLogger(SplitTabletJob.class);

    @SerializedName(value = "dbId")
    protected final long dbId;

    @SerializedName(value = "tableId")
    protected final long tableId;

    @SerializedName(value = "reshardingPhysicalPartitions")
    protected final Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions;

    @SerializedName(value = "transactionId")
    protected long transactionId;

    @SerializedName(value = "gtid")
    protected long gtid;

    @SerializedName(value = "endTransactionId")
    protected long endTransactionId;

    public SplitTabletJob(long jobId, long dbId, long tableId,
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) {
        super(jobId, JobType.SPLIT_TABLET);
        this.dbId = dbId;
        this.tableId = tableId;
        this.reshardingPhysicalPartitions = reshardingPhysicalPartitions;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<Long, ReshardingPhysicalPartition> getReshardingPhysicalPartitions() {
        return reshardingPhysicalPartitions;
    }

    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public long getParallelTablets() {
        long parallelTablets = 0;
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            parallelTablets += reshardingPhysicalPartition.getParallelTablets();
        }
        return parallelTablets;
    }

    /*
     * 1. Set table state to TABLET_RESHARD
     * 2. Begin transaction (allocate transaction id)
     * 3. Commit transaction (update next version)
     * 4. Add new tablets to inverted index
     * 5. Register resharding tablets
     * 6. Set job state to PREPARING
     * Job cannot be cancelled after this step
     */
    @Override
    protected void runPendingJob() {
        // 1. Set table state to TABLET_RESHARD
        setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.TABLET_RESHARD);

        // 2. Begin transaction (allocate transaction id)
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        transactionId = globalStateMgr.getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        gtid = globalStateMgr.getGtidGenerator().nextGtid();

        // 3. Commit transaction (update next version)
        updateNextVersions();

        // 4. Add new tablets to inverted index
        addTabletsToInvertedIndex();

        // 5. Register resharding tablets
        registerReshardingTablets();

        // 6. Set job state to PREPARING
        setJobState(JobState.PREPARING);
    }

    /*
     * 1. Wait for previous versions published
     * 2. Set job state to RUNNING
     */
    @Override
    protected void runPreparingJob() {
        // 1. Wait for previous versions published
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }

                // Wait for previous versions published
                long commitVersion = reshardingPhysicalPartition.getCommitVersion();
                long visibleVersion = physicalPartition.getVisibleVersion();
                if (commitVersion != visibleVersion + 1) {
                    Preconditions.checkState(visibleVersion < commitVersion,
                            "partition=" + physicalPartition.getId() + " visibleVersion="
                                    + visibleVersion + " commitVersion=" + commitVersion);
                    return;
                }
            }
        }

        // 2. Set job state to RUNNING
        setJobState(JobState.RUNNING);
    }

    /*
     * 1. Publish the split transaction, update new tablet ranges
     * 2. Add the new versions of materialized index to catalog
     * 3. Get end transaction id
     * 4. Set job state to CLEANING
     */
    @Override
    protected void runRunningJob() {
        // 1. Publish the split transaction, update new tablet ranges
        boolean allPartitionFinished = true;
        ThreadPoolExecutor publishThreadPool = GlobalStateMgr.getCurrentState().getPublishVersionDaemon()
                .getLakeTaskExecutor();

        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            boolean useAggregatePublish = olapTable.isFileBundling();
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }

                long commitVersion = reshardingPhysicalPartition.getCommitVersion();
                long visibleVersion = physicalPartition.getVisibleVersion();
                Preconditions.checkState(commitVersion == visibleVersion + 1,
                        "partition=" + physicalPartition.getId() + " visibleVersion="
                                + visibleVersion + " commitVersion=" + commitVersion);

                PublishResult publishResult = reshardingPhysicalPartition.getPublishResult();
                if (publishResult.publishState() == PublishState.NOT_STARTED
                        || publishResult.publishState() == PublishState.FAILED) {
                    // Publish not started or publish failed
                    allPartitionFinished = false;
                    // Start publish asynchronously
                    List<Tablet> tablets = new ArrayList<>();
                    for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.ALL)) {
                        tablets.addAll(index.getTablets());
                    }
                    Future<Map<Long, TabletRange>> future = publishThreadPool.submit(() -> publishVersion(
                            tablets, commitVersion, useAggregatePublish));
                    reshardingPhysicalPartition.setPublishFuture(future);
                } else if (publishResult.publishState() == PublishState.IN_PROGRESS) {
                    // Publish is in progress
                    allPartitionFinished = false;
                } else if (publishResult.publishState() == PublishState.SUCCESS) {
                    // Publish success, update new tablet ranges
                    // Note this will be executed repeatedly when retry job, it should be idempotent
                    Map<Long, TabletRange> tabletRanges = publishResult.tabletRanges();
                    for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                            .getReshardingIndexes().values()) {
                        MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
                        for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                            SplittingTablet splittingTablet = reshardingTablet.getSplittingTablet();
                            if (splittingTablet != null) {
                                for (Long tabletId : splittingTablet.getNewTabletIds()) {
                                    TabletRange tabletRange = tabletRanges.get(tabletId);
                                    Preconditions.checkNotNull(tabletRange,
                                            "Range of tablet " + tabletId + " not found");
                                    newIndex.getTablet(tabletId).setRange(tabletRange.getRange());
                                }
                            }
                        }
                    }
                } else {
                    LOG.error("Unknown publish state {} in {}", publishResult.publishState(), this);
                    throw new TabletReshardException("Unknown publish state " + publishResult.publishState());
                }
            }
        }

        if (!allPartitionFinished) {
            return;
        }

        // 2. Add the new versions of materialized index to catalog
        addNewMaterializedIndexes();

        // 3. Get end transaction id
        endTransactionId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator()
                .peekNextTransactionId();

        // 4. Set job state to CLEANING
        setJobState(JobState.CLEANING);
    }

    /*
     * 1. Wait for previous transactions finished
     * 2. Remove old versions of materialized index
     * 3. Unregister resharding tablets
     * 4. Set tablet state to NORMAL
     * 5. Set job state to FINISHED
     */
    @Override
    protected void runCleaningJob() {
        // 1. Wait for previous transactions finished
        try {
            if (!GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().isPreviousTransactionsFinished(
                    endTransactionId, dbId, List.of(tableId))) {
                return;
            }
        } catch (AnalysisException e) { // Db is dropped, ignore exception
            LOG.warn("Ignore exception when waiting previous transactions finished. " + this, e);
        }

        // 2. Remove old versions of materialized index
        removeOldMaterializedIndexes();

        // 3. Unregister resharding tablets
        unregisterReshardingTablets();

        // 4. Set tablet state to NORMAL
        setTableState(OlapTable.OlapTableState.TABLET_RESHARD, OlapTable.OlapTableState.NORMAL);

        // 6. Set job state to FINISHED
        setJobState(JobState.FINISHED);
    }

    @Override
    protected void runFinishedJob() {
        LOG.info("Split tablet job is finished. {}", this);
    }

    /*
     * 1. Unregister resharding tablets
     * 2. Set table state to NORMAL
     * 3. Set job state to ABORTED
     */
    @Override
    protected void runAbortingJob() {
        try {
            // 1. Unregister resharding tablets
            unregisterReshardingTablets();

            // 2. Set table state to NORMAL
            setTableState(null, OlapTable.OlapTableState.NORMAL);
        } catch (Exception e) {
            LOG.warn("Ignore exception when aborting tablet reshard job. {}. ", this, e);
        }

        // 3. Set job state to ABORTED
        setJobState(JobState.ABORTED);
    }

    @Override
    protected void runAbortedJob() {
        LOG.info("Split tablet job is aborted. {}", this);
    }

    // Can abort only when job state is PENDING
    @Override
    protected boolean canAbort() {
        return jobState == JobState.PENDING;
    }

    // Correspond to job added
    @Override
    protected void replayPendingJob() {
        LOG.info("Split tablet job replayed pending job. {}", this);
    }

    // Correspond to runPendingJob()
    @Override
    protected void replayPreparingJob() {
        setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.TABLET_RESHARD);
        updateNextVersions();
        addTabletsToInvertedIndex();
        registerReshardingTablets();
        LOG.info("Split tablet job replayed preparing job. {}", this);
    }

    // Correspond to runPreparingJob()
    @Override
    protected void replayRunningJob() {
        LOG.info("Split tablet job replayed running job. {}", this);
    }

    // Correspond to runRunningJob()
    @Override
    protected void replayCleaningJob() {
        addNewMaterializedIndexes();
        LOG.info("Split tablet job replayed cleaning job. {}", this);
    }

    // Correspond to runCleaningJob()
    @Override
    protected void replayFinishedJob() {
        removeOldMaterializedIndexes();
        unregisterReshardingTablets();
        setTableState(OlapTable.OlapTableState.TABLET_RESHARD, OlapTable.OlapTableState.NORMAL);
        LOG.info("Split tablet job replayed finished job. {}", this);
    }

    // Correspond to job abort
    @Override
    protected void replayAbortingJob() {
        LOG.info("Split tablet job replayed aborting job. {}", this);
    }

    // Correspond to runAbortingJob()
    @Override
    protected void replayAbortedJob() {
        unregisterReshardingTablets();
        setTableState(null, OlapTable.OlapTableState.NORMAL);
        LOG.info("Split tablet job replayed aborted job. {}", this);
    }

    @Override
    protected void registerReshardingTabletsOnRestart() {
        if (jobState == JobState.PENDING || jobState.isFinalState()) {
            return;
        }

        registerReshardingTablets();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SplitTabletJob: {");
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
        if (transactionId > 0) {
            sb.append(", txn_id: ").append(transactionId);
        }
        if (endTransactionId > 0) {
            sb.append(", end_txn_id: ").append(endTransactionId);
        }
        sb.append(", parallel_partitions: ").append(reshardingPhysicalPartitions.size());
        sb.append("}");
        return sb.toString();
    }

    @Override
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
        item.setTransaction_id(transactionId);
        item.setParallel_partitions(reshardingPhysicalPartitions.size());
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

    private void setTableState(OlapTable.OlapTableState expectedState, OlapTable.OlapTableState newState) {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            if (olapTable.getState() == newState) {
                return;
            }
            if (expectedState != null && olapTable.getState() != expectedState) {
                throw new TabletReshardException(
                        "Unexpected table state " + olapTable.getState() + " in table " + olapTable.getName());
            }
            olapTable.setState(newState);
        }
    }

    private void updateNextVersions() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }

                long commitVersion = reshardingPhysicalPartition.getCommitVersion();
                if (commitVersion <= 0) {
                    commitVersion = physicalPartition.getNextVersion();
                    reshardingPhysicalPartition.setCommitVersion(commitVersion);
                }

                physicalPartition.setNextVersion(commitVersion + 1);
            }
        }
    }

    private Map<Long, TabletRange> publishVersion(List<Tablet> tablets, long commitVersion,
            boolean useAggregatePublish) {
        try {
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = transactionId;
            txnInfo.combinedTxnLog = false;
            txnInfo.commitTime = stateStartedTimeMs / 1000;
            txnInfo.txnType = TxnTypePB.TXN_TABLET_RESHARD;
            txnInfo.gtid = gtid;

            Map<Long, TabletRange> tabletRange = new HashMap<>();
            Utils.publishVersion(tablets, txnInfo, commitVersion - 1, commitVersion, null, tabletRange,
                    WarehouseManager.DEFAULT_RESOURCE, null, useAggregatePublish);

            return tabletRange;
        } catch (Exception e) {
            LOG.warn("Failed to publish version for tablet reshard job {}. ", this, e);
            throw new TabletReshardException("Failed to publish version: " + e.getMessage(), e);
        }
    }

    private void addNewMaterializedIndexes() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }

                long commitVersion = reshardingPhysicalPartition.getCommitVersion();
                Preconditions.checkState(commitVersion == physicalPartition.getVisibleVersion() + 1,
                        "commit version: " + commitVersion + ", visible version: "
                                + physicalPartition.getVisibleVersion());

                physicalPartition.setVisibleVersion(commitVersion, stateStartedTimeMs);

                // Temporary code, ignore, will be replaced later
                for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                        .getReshardingIndexes().values()) {
                    MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
                    if (newIndex.getId() == olapTable.getBaseIndexMetaId()) {
                        physicalPartition.setBaseIndex(newIndex);
                    } else {
                        physicalPartition.createRollupIndex(newIndex);
                    }
                }
            }
        }
    }

    private void removeOldMaterializedIndexes() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
                PhysicalPartition physicalPartition = olapTable
                        .getPhysicalPartition(reshardingPhysicalPartition.getPhysicalPartitionId());
                if (physicalPartition == null) {
                    continue;
                }

                for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                        .getReshardingIndexes().values()) {
                    MaterializedIndex oldIndex = physicalPartition.getIndex(reshardingIndex.getMaterializedIndexId());
                    if (oldIndex == null) {
                        continue;
                    }

                    /*
                     * To do later
                     * for (Tablet tablet : oldIndex.getTablets()) {
                     * invertedIndex.deleteTablet(tablet.getId());
                     * }
                     */
                }
            }
        }
    }

    private void addTabletsToInvertedIndex() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                MaterializedIndex index = reshardingIndex.getMaterializedIndex();
                // Use HDD in shared-data mode
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId,
                        reshardingPhysicalPartition.getPhysicalPartitionId(),
                        index.getId(), TStorageMedium.HDD, true);
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
            throw new TabletReshardException("Table not found. " + this);
        }

        OlapTable olapTable = (OlapTable) table;

        return new LockedObject<>(dbId, List.of(tableId), lockType, olapTable);
    }

    private void registerReshardingTablets() {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            long visibleVersion = reshardingPhysicalPartition.getCommitVersion();
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    for (long tabletId : reshardingTablet.getOldTabletIds()) {
                        tabletReshardJobMgr.registerReshardingTablet(tabletId, reshardingTablet,
                                visibleVersion);
                    }
                }
            }
        }
    }

    protected void unregisterReshardingTablets() {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    for (long tabletId : reshardingTablet.getOldTabletIds()) {
                        tabletReshardJobMgr.unregisterReshardingTablet(tabletId);
                    }
                }
            }
        }
    }
}
