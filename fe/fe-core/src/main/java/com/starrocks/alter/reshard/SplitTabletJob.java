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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

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
     * 2. Find split point and set tablet ranges
     */
    @Override
    protected void runPendingJob() {
        // 1. Set table state to TABLET_RESHARD
        setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.TABLET_RESHARD);

        // 2. Find split point
        boolean allPartitionFinished = true;
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            Optional<Map<Long, List<TabletRange>>> result = reshardingPhysicalPartition.getFutureResult();
            if (result != null && result.isPresent()) {
                // Find split point success, set tablet range
                Map<Long, List<TabletRange>> splitRanges = result.get();
                for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition.getReshardingIndexes()
                        .values()) {
                    MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
                    for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                        SplittingTablet splittingTablet = reshardingTablet.getSplittingTablet();
                        if (splittingTablet != null) {
                            List<TabletRange> tabletRanges = splitRanges.get(splittingTablet.getOldTabletId());
                            List<Long> tabletIds = splittingTablet.getNewTabletIds();
                            Preconditions.checkState(tabletRanges != null && tabletRanges.size() == tabletIds.size());
                            for (int i = 0; i < tabletRanges.size(); ++i) {
                                newIndex.getTablet(tabletIds.get(i)).setRange(tabletRanges.get(i).getRange());
                            }
                        }
                    }
                }
                continue;
            }

            allPartitionFinished = false;

            if (result != null && result.isEmpty()) {
                continue; // Find split point in progress
            }

            // Find split point not started, start find split point asynchronously
            List<SplittingTablet> splittingTablets = new ArrayList<>();
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition.getReshardingIndexes()
                    .values()) {
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    SplittingTablet splittingTablet = reshardingTablet.getSplittingTablet();
                    if (splittingTablet != null) {
                        splittingTablets.add(splittingTablet);
                    }
                }
            }
            Future<Map<Long, List<TabletRange>>> future = TabletReshardUtils.findSplitPoint(splittingTablets);
            reshardingPhysicalPartition.setFuture(future);
        }

        if (!allPartitionFinished) {
            return;
        }

        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            reshardingPhysicalPartition.setFuture(null);
        }

        setJobState(JobState.PREPARING);
    }

    /*
     * 1. Begin transaction (allocate transaction id)
     * 2. Commit transaction (update next version)
     * 3. Add new tablets to inverted index
     * 4. Register resharding tablets
     * Job cannot be cancelled after this step
     */
    @Override
    protected void runPreparingJob() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        transactionId = globalStateMgr.getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
        gtid = globalStateMgr.getGtidGenerator().nextGtid();

        updateNextVersions();

        addTabletsToInvertedIndex();

        registerReshardingTablets();

        setJobState(JobState.RUNNING);
    }

    /*
     * 1. Wait for previous versions published
     * 2. Publish the split transaction
     * 3. Add the new versions of materialized index
     */
    @Override
    protected void runRunningJob() {
        boolean allPartitionFinished = true;
        ThreadPoolExecutor publishThreadPool = GlobalStateMgr.getCurrentState().getPublishVersionDaemon()
                .getLakeTaskExecutor();

        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            List<String> distributionColumns = olapTable.getDefaultDistributionInfo().getDistributionColumns()
                    .stream().map(ColumnId::getId).collect(Collectors.toList());
            boolean useAggregatePublish = olapTable.isFileBundling();

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

                // Publish the split transaction
                Optional<Boolean> result = reshardingPhysicalPartition.getFutureResult();
                if (result != null && result.isPresent() && result.get()) {
                    continue; // Publish success
                }

                allPartitionFinished = false;

                if (result != null && result.isEmpty()) {
                    continue; // Publish in progress
                }

                // Publish not started or publish failed, start publish asynchronously
                List<Tablet> tablets = new ArrayList<>();
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.ALL)) {
                    tablets.addAll(index.getTablets());
                }
                Future<Boolean> future = publishThreadPool.submit(() -> publishVersion(
                        tablets, commitVersion, distributionColumns, useAggregatePublish));
                reshardingPhysicalPartition.setFuture(future);
            }
        }

        if (!allPartitionFinished) {
            return;
        }

        endTransactionId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator()
                .peekNextTransactionId();

        // Add the new versions of materialized index
        addNewMaterializedIndexes();

        setJobState(JobState.CLEANING);
    }

    /*
     * 1. Wait for previous transactions finished
     * 2. Remove old versions of materialized index
     * 3. Unregister resharding tablets
     * 4. Set tablet state to NORMAL
     */
    @Override
    protected void runCleaningJob() {
        try {
            if (!GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().isPreviousTransactionsFinished(
                    endTransactionId, dbId, List.of(tableId))) {
                return;
            }
        } catch (AnalysisException e) { // Db is dropped, ignore exception
            LOG.warn("Ignore exception when waiting previous transactions finished. " + this, e);
        }

        removeOldMaterializedIndexes();

        unregisterReshardingTablets();

        setTableState(OlapTable.OlapTableState.TABLET_RESHARD, OlapTable.OlapTableState.NORMAL);

        // Clear to release memory
        reshardingPhysicalPartitions.clear();

        setJobState(JobState.FINISHED);
    }

    @Override
    protected void runFinishedJob() {
        LOG.info("Split tablet job is finished. {}", this);
    }

    /*
     * 1. Unregister resharding tablets
     * 2. Set table state to NORMAL
     */
    @Override
    protected void runAbortingJob() {
        try {
            unregisterReshardingTablets();

            setTableState(null, OlapTable.OlapTableState.NORMAL);
        } catch (Exception e) {
            LOG.warn("Ignore exception when aborting tablet reshard job. {}. ", this, e);
        }

        // Clear to release memory
        reshardingPhysicalPartitions.clear();

        setJobState(JobState.ABORTED);
    }

    @Override
    protected void runAbortedJob() {
        LOG.info("Split tablet job is aborted. {}", this);
    }

    // Can abort only when job state is PENDING or PREPARING
    @Override
    protected boolean canAbort() {
        return jobState == JobState.PENDING || jobState == JobState.PREPARING;
    }

    @Override
    protected void replayPendingJob() {
        LOG.info("Split tablet job replayed pending job. {}", this);
    }

    @Override
    protected void replayPreparingJob() {
        setTableState(OlapTable.OlapTableState.NORMAL, OlapTable.OlapTableState.TABLET_RESHARD);
        LOG.info("Split tablet job replayed preparing job. {}", this);
    }

    @Override
    protected void replayRunningJob() {
        updateNextVersions();
        addTabletsToInvertedIndex();
        registerReshardingTablets();
        LOG.info("Split tablet job replayed running job. {}", this);
    }

    @Override
    protected void replayCleaningJob() {
        addNewMaterializedIndexes();
        LOG.info("Split tablet job replayed cleaning job. {}", this);
    }

    @Override
    protected void replayFinishedJob() {
        removeOldMaterializedIndexes();
        unregisterReshardingTablets();
        setTableState(OlapTable.OlapTableState.TABLET_RESHARD, OlapTable.OlapTableState.NORMAL);
        LOG.info("Split tablet job replayed finished job. {}", this);
    }

    @Override
    protected void replayAbortingJob() {
        LOG.info("Split tablet job replayed aborting job. {}", this);
    }

    @Override
    protected void replayAbortedJob() {
        unregisterReshardingTablets();
        setTableState(null, OlapTable.OlapTableState.NORMAL);
        LOG.info("Split tablet job replayed aborted job. {}", this);
    }

    @Override
    protected void registerReshardingTabletsOnRestart() {
        if (jobState == JobState.PENDING || jobState == JobState.PREPARING) {
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

    private boolean publishVersion(List<Tablet> tablets, long commitVersion, List<String> distributionColumns,
            boolean useAggregatePublish) {
        try {
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = transactionId;
            txnInfo.combinedTxnLog = false;
            txnInfo.commitTime = stateStartedTimeMs / 1000;
            txnInfo.txnType = TxnTypePB.TXN_TABLET_RESHARD;
            txnInfo.gtid = gtid;

            Utils.publishVersion(tablets, txnInfo, commitVersion - 1, commitVersion, null, distributionColumns,
                    WarehouseManager.DEFAULT_RESOURCE, null, useAggregatePublish);

            return true;
        } catch (Exception e) {
            LOG.warn("Failed to publish version for tablet reshard job {}. ", this, e);
            return false;
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
                    if (newIndex.getId() == olapTable.getBaseIndexId()) {
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
