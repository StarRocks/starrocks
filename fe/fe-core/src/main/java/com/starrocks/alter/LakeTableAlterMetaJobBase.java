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

package com.starrocks.alter;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Status;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.Utils;
import com.starrocks.mv.MVRepairHandler.PartitionRepairInfo;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.task.TabletTaskExecutor;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTaskType;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.validation.constraints.NotNull;

public abstract class LakeTableAlterMetaJobBase extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(LakeTableAlterMetaJobBase.class);

    @SerializedName(value = "watershedTxnId")
    private long watershedTxnId = -1;
    @SerializedName(value = "watershedGtid")
    private long watershedGtid = -1;
    // PhysicalPartitionId -> indexId -> MaterializedIndex
    @SerializedName(value = "partitionIndexMap")
    private Table<Long, Long, MaterializedIndex> physicalPartitionIndexMap = HashBasedTable.create();
    @SerializedName(value = "commitVersionMap")
    private Map<Long, Long> commitVersionMap = new HashMap<>();
    private AgentBatchTask batchTask = null;
    private boolean isFileBundling = false;

    public LakeTableAlterMetaJobBase(JobType jobType) {
        super(jobType);
    }

    public LakeTableAlterMetaJobBase(long jobId, JobType jobType, long dbId, long tableId,
                                     String tableName, long timeoutMs) {
        super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {
        // send task to be
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<Partition> partitions = Lists.newArrayList();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbId);

        if (db == null) {
            throw new AlterCancelException("database does not exist, dbId:" + dbId);
        }

        LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        if (table == null) {
            throw new AlterCancelException("table does not exist, tableName:" + tableName);
        }

        int numReplicas = 0;
        int numIndexes = 0;
        List<PhysicalPartition> physicalPartitions = Lists.newArrayList();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            partitions.addAll(table.getPartitions());
            for (Partition partition : partitions) {
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    numReplicas += physicalPartition.storageReplicaCount();
                    numIndexes += physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size();
                    physicalPartitions.add(physicalPartition);
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }

        int numAliveNodes = 0;
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        final List<Long> computeNodeIds = GlobalStateMgr.
                getCurrentState().getWarehouseMgr().getAllComputeNodeIds(computeResource);
        for (long nodeId : computeNodeIds) {
            ComputeNode node = systemInfoService.getBackendOrComputeNode(nodeId);
            if (node != null && node.isAlive()) {
                ++numAliveNodes;
            }
        }

        if (numAliveNodes == 0) {
            throw new AlterCancelException("no alive compute nodes");
        }

        if (this.watershedTxnId == -1) {
            this.watershedTxnId = globalStateMgr.getGlobalTransactionMgr().getTransactionIDGenerator()
                    .getNextTransactionId();
            this.watershedGtid = globalStateMgr.getGtidGenerator().nextGtid();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        }

        try {
            if (Config.enable_alter_table_schema_parallel) {
                LOG.info("start to alter meta {} partitions concurrently for table {}.{} with {} replicas in warehouse {}",
                        partitions.size(), db.getFullName(), table.getName(), numReplicas, warehouseId);
                updatePartitionTabletMetaConcurrently(db, table, physicalPartitions, numReplicas, numAliveNodes, numIndexes);
            } else {
                LOG.info("start to alter meta {} partitions sequentially for table {}.{} with {} replicas in warehouse {}",
                        partitions.size(), db.getFullName(), table.getName(), numReplicas, warehouseId);
                for (Partition partition : partitions) {
                    updatePartitionTabletMeta(db, table, partition);
                }
            }
        } catch (DdlException e) {
            throw new AlterCancelException(e.getMessage());
        }

        this.jobState = JobState.RUNNING;
    }

    protected abstract TabletMetadataUpdateAgentTask createTask(PhysicalPartition partition,
                                                                MaterializedIndex index, long nodeId, Set<Long> tablets);

    protected abstract void updateCatalog(Database db, LakeTable table);

    protected abstract void restoreState(LakeTableAlterMetaJobBase job);

    protected abstract boolean enableFileBundling();

    protected abstract boolean disableFileBundling();

    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        // do nothing
    }

    @Override
    protected void runRunningJob() throws AlterCancelException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database has been dropped
            throw new AlterCancelException("database does not exist, dbId:" + dbId);
        }

        LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            // table has been dropped
            throw new AlterCancelException("table does not exist, tableId:" + tableId);
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            commitVersionMap.clear();
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);
                long commitVersion = partition.getNextVersion();
                commitVersionMap.put(partitionId, commitVersion);
                LOG.debug("commit version of partition {} is {}. jobId={}", partitionId,
                        commitVersion, jobId);
            }

            this.jobState = JobState.FINISHED_REWRITING;
            this.finishedTimeMs = System.currentTimeMillis();

            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);

            // NOTE: !!! below this point, this update meta job must success unless the database or table been dropped. !!!
            updateNextVersion(table);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }
    }

    @Override
    protected void runFinishedRewritingJob() throws AlterCancelException {
        // run publish version
        Preconditions.checkState(jobState == JobState.FINISHED_REWRITING);
        // If the table or database has been dropped, `readyToPublishVersion()` will throw AlterCancelException and
        // this schema change job will be cancelled.
        if (!readyToPublishVersion()) {
            return;
        }

        if (!publishVersion()) {
            return;
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database has been dropped
            LOG.warn("database does not exist, dbId:" + dbId);
            throw new AlterCancelException("database does not exist, dbId:" + dbId);
        }

        LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            // table has been dropped
            LOG.warn("table does not exist, tableId:" + tableId);
            throw new AlterCancelException("table does not exist, tableId:" + tableId);
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            updateCatalog(db, table);
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
            // set visible version
            updateVisibleVersion(table);
            table.setState(OlapTable.OlapTableState.NORMAL);

        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }

        handleMVRepair(db, table);
        LOG.info("update meta job finished: {}", jobId);
    }

    boolean readyToPublishVersion() throws AlterCancelException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database has been dropped
            throw new AlterCancelException("database does not exist, dbId:" + dbId);
        }
        LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            // table has been dropped
            throw new AlterCancelException("table does not exist, tableId:" + tableId);
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            isFileBundling = table.isFileBundling();
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                PhysicalPartition partition = table.getPhysicalPartition(partitionId);
                Preconditions.checkState(partition != null, partitionId);
                long commitVersion = commitVersionMap.get(partitionId);
                if (commitVersion != partition.getVisibleVersion() + 1) {
                    Preconditions.checkState(partition.getVisibleVersion() < commitVersion,
                            "partition=" + partitionId + " visibleVersion=" + partition.getVisibleVersion() +
                                    " commitVersion=" + commitVersion);
                    return false;
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }
        return true;
    }

    protected boolean lakePublishVersion() {
        try {
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = watershedTxnId;
            txnInfo.combinedTxnLog = false;
            txnInfo.commitTime = finishedTimeMs / 1000;
            txnInfo.txnType = TxnTypePB.TXN_NORMAL;
            txnInfo.gtid = watershedGtid;
            // there are two scenario we should use aggregate_publish
            // 1. this task is change `file_bundling` to true
            // 2. the table is enable `file_bundling` and this task is not change `file_bundling`
            //    to false.
            boolean useAggregatePublish = enableFileBundling() || (isFileBundling && !disableFileBundling());
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                long commitVersion = commitVersionMap.get(partitionId);
                Map<Long, MaterializedIndex> dirtyIndexMap = physicalPartitionIndexMap.row(partitionId);
                List<Tablet> tablets = new ArrayList<>();
                for (MaterializedIndex index : dirtyIndexMap.values()) {
                    if (!useAggregatePublish) {
                        Utils.publishVersion(index.getTablets(), txnInfo, commitVersion - 1, commitVersion,
                                computeResource, false);
                    } else {
                        tablets.addAll(index.getTablets());
                    }
                }
                if (useAggregatePublish) {
                    Utils.aggregatePublishVersion(tablets, Lists.newArrayList(txnInfo), commitVersion - 1, commitVersion, 
                                null, null, computeResource, null);
                }
            }
            return true;
        } catch (Exception e) {
            LOG.error("Fail to publish version for schema change job {}: {}", jobId, e.getMessage());
            return false;
        }
    }

    public void addDirtyPartitionIndex(long partitionId, long indexId, MaterializedIndex index) {
        physicalPartitionIndexMap.put(partitionId, indexId, index);
    }

    public void updatePartitionTabletMeta(Database db, LakeTable table, Partition partition) throws DdlException {
        Collection<PhysicalPartition> physicalPartitions;
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            physicalPartitions = partition.getSubPartitions();
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }

        for (PhysicalPartition physicalPartition : physicalPartitions) {
            updatePhysicalPartitionTabletMeta(db, table, physicalPartition);
        }
    }

    public void updatePhysicalPartitionTabletMeta(Database db, OlapTable table,
                                                  PhysicalPartition partition) throws DdlException {
        List<MaterializedIndex> indexList;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            indexList = new ArrayList<>(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }
        for (MaterializedIndex index : indexList) {
            updateIndexTabletMeta(db, table, partition, index);
        }
    }

    public void updateIndexTabletMeta(Database db, OlapTable table, PhysicalPartition partition,
                                      MaterializedIndex index) throws DdlException {
        addDirtyPartitionIndex(partition.getId(), index.getId(), index);
        // be id -> <tablet id,schemaHash>
        Map<Long, Set<Long>> beIdToTabletSet = Maps.newHashMap();
        List<Tablet> tablets;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            tablets = new ArrayList<>(index.getTablets());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }

        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        for (Tablet tablet : tablets) {
            Long backendId = warehouseManager.getAliveComputeNodeId(computeResource, tablet.getId());
            if (backendId == null) {
                throw new AlterCancelException("no alive node");
            }
            Set<Long> set = beIdToTabletSet.computeIfAbsent(backendId, k -> Sets.newHashSet());
            set.add(tablet.getId());
        }

        int totalTaskNum = beIdToTabletSet.keySet().size();
        MarkedCountDownLatch<Long, Set<Long>> countDownLatch = new MarkedCountDownLatch<>(totalTaskNum);
        batchTask = new AgentBatchTask();
        for (Map.Entry<Long, Set<Long>> kv : beIdToTabletSet.entrySet()) {
            countDownLatch.addMark(kv.getKey(), kv.getValue());
            TabletMetadataUpdateAgentTask task = createTask(partition, index, kv.getKey(), kv.getValue());
            Preconditions.checkState(task != null, "task is null");
            task.setLatch(countDownLatch);
            task.setTxnId(watershedTxnId);
            batchTask.addTask(task);
        }
        // send all tasks and wait them finished
        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
        LOG.info("Sent update tablet metadata task. tableName={} partitionId={} indexId={} taskNum={}",
                tableName, partition.getId(), index.getId(), batchTask.getTaskNum());

        // estimate timeout
        long timeout = Config.tablet_create_timeout_second * 1000L * totalTaskNum;
        timeout = Math.min(timeout, Config.max_create_table_timeout_second * 1000L);
        boolean ok = false;
        try {
            ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
        }

        if (!ok || !countDownLatch.getStatus().ok()) {
            String errMsg = "Failed to update tablet meta.";
            // clear tasks
            AgentTaskQueue.removeBatchTask(batchTask, TTaskType.UPDATE_TABLET_META_INFO);

            if (!countDownLatch.getStatus().ok()) {
                errMsg += " Error: " + countDownLatch.getStatus().getErrorMsg();
            } else {
                List<Map.Entry<Long, Set<Long>>> unfinishedMarks = countDownLatch.getLeftMarks();
                // only show at most 3 results
                List<Map.Entry<Long, Set<Long>>> subList =
                        unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                if (!subList.isEmpty()) {
                    errMsg += " Unfinished mark: " + Joiner.on(", ").join(subList);
                }
            }
            errMsg += ". This operation maybe partial successfully, You should retry until success.";
            LOG.warn(errMsg);
            throw new DdlException(errMsg);
        }
    }

    void updateNextVersion(@NotNull LakeTable table) {
        for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition physicalPartition = table.getPhysicalPartition(partitionId);
            long commitVersion = commitVersionMap.get(physicalPartition.getId());
            Preconditions.checkState(physicalPartition.getNextVersion() == commitVersion,
                    "partitionNextVersion=" + physicalPartition.getNextVersion() + " commitVersion=" + commitVersion);
            physicalPartition.setNextVersion(commitVersion + 1);
            LOG.info("LakeTableAlterMetaJob id: {} update next version of partition: {}, commitVersion: {}",
                    jobId, physicalPartition.getId(), commitVersion);
        }
    }

    void updateVisibleVersion(@NotNull LakeTable table) {
        for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Preconditions.checkState(partition.getVisibleVersion() == commitVersion - 1,
                    "partitionVisitionVersion=" + partition.getVisibleVersion() + " commitVersion=" + commitVersion);
            partition.updateVisibleVersion(commitVersion, finishedTimeMs);
            if (enableFileBundling() || disableFileBundling()) {
                partition.setMetadataSwitchVersion(commitVersion);
            }
            LOG.info("partitionVisibleVersion=" + partition.getVisibleVersion() + " commitVersion=" + commitVersion);
            LOG.info("LakeTableAlterMetaJob id: {} update visible version of partition: {}, visible Version: {}",
                    jobId, partition.getId(), commitVersion);
        }
    }

    protected AgentBatchTask getBatchTask() {
        return batchTask;
    }

    protected long getWatershedTxnId() {
        return watershedTxnId;
    }

    @Override
    protected boolean cancelImpl(String errMsg) {
        if (jobState == JobState.CANCELLED || jobState == JobState.FINISHED) {
            return false;
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db != null) {
            LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (table != null) {
                Locker locker = new Locker();
                locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
                try {
                    // Cancel a job of state `FINISHED_REWRITING` only when the database or table has been dropped.
                    if (jobState == JobState.FINISHED_REWRITING) {
                        return false;
                    }
                    table.setState(OlapTable.OlapTableState.NORMAL);
                } finally {
                    locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
                }
            }
        }
        if (span != null) {
            span.setStatus(StatusCode.ERROR, errMsg);
            span.end();
        }
        this.jobState = JobState.CANCELLED;
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        return true;
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // LakeTableAlterMetaJob is not supported by show for now
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        LakeTableAlterMetaJobBase other = (LakeTableAlterMetaJobBase) replayedJob;

        LOG.info("Replaying lake table update meta job. state={} jobId={}", replayedJob.jobState, replayedJob.jobId);

        if (this != other) {
            Preconditions.checkState(this.type.equals(other.type));
            Preconditions.checkState(this.jobId == other.jobId);
            Preconditions.checkState(this.dbId == other.dbId);
            Preconditions.checkState(this.tableId == other.tableId);

            this.jobState = other.jobState;
            this.createTimeMs = other.createTimeMs;
            this.finishedTimeMs = other.finishedTimeMs;
            this.errMsg = other.errMsg;
            this.timeoutMs = other.timeoutMs;

            this.physicalPartitionIndexMap = other.physicalPartitionIndexMap;
            this.watershedTxnId = other.watershedTxnId;
            this.watershedGtid = other.watershedGtid;
            this.commitVersionMap = other.commitVersionMap;

            restoreState(other);
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            // database has been dropped
            LOG.warn("database does not exist, dbId:" + dbId);
            return;
        }

        LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            if (jobState == JobState.FINISHED_REWRITING) {
                updateNextVersion(table);
            } else if (jobState == JobState.FINISHED) {
                updateVisibleVersion(table);
                updateCatalog(db, table);
                table.setState(OlapTable.OlapTableState.NORMAL);
            } else if (jobState == JobState.CANCELLED) {
                table.setState(OlapTable.OlapTableState.NORMAL);
            } else if (jobState == JobState.PENDING || jobState == JobState.WAITING_TXN) {
                table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
            } else {
                throw new RuntimeException("unknown job state '{}'" + jobState.name());
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }
    }

    // for test
    public Table<Long, Long, MaterializedIndex> getPartitionIndexMap() {
        return physicalPartitionIndexMap;
    }

    // for test
    public Map<Long, Long> getCommitVersionMap() {
        return commitVersionMap;
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }

    private void handleMVRepair(Database db, LakeTable table) {
        if (table.getRelatedMaterializedViews().isEmpty()) {
            return;
        }

        List<PartitionRepairInfo> partitionRepairInfos = Lists.newArrayListWithCapacity(commitVersionMap.size());

        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            for (Map.Entry<Long, Long> partitionVersion : commitVersionMap.entrySet()) {
                long partitionId = partitionVersion.getKey();
                Partition partition = table.getPartition(partitionId);
                if (partition == null || table.isTempPartition(partitionId)) {
                    continue;
                }
                // TODO(fixme): last version/version time is not kept in transaction state, use version - 1 for last commit
                //  version.
                // TODO: we may add last version time to check mv's version map with base table's version time.
                PartitionRepairInfo partitionRepairInfo = new PartitionRepairInfo(partition.getId(),  partition.getName(),
                        partitionVersion.getValue() - 1, partitionVersion.getValue(), finishedTimeMs);
                partitionRepairInfos.add(partitionRepairInfo);
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }

        if (partitionRepairInfos.isEmpty()) {
            return;
        }

        GlobalStateMgr.getCurrentState().getLocalMetastore().handleMVRepair(db, table, partitionRepairInfos);
    }

    private void updatePartitionTabletMetaConcurrently(Database db, LakeTable table,
                                                       List<PhysicalPartition> partitions, int numReplicas,
                                                       int numBackends, int numIndexes) throws DdlException {
        if (partitions.isEmpty()) {
            return;
        }

        long start = System.currentTimeMillis();
        int timeout = Math.max(1, numReplicas / numBackends) * Config.tablet_create_timeout_second;
        int maxTimeout = numIndexes * Config.max_create_table_timeout_second;
        timeout = Math.min(timeout, maxTimeout);
        MarkedCountDownLatch<Long, Set<Long>> countDownLatch = new MarkedCountDownLatch<>(numReplicas, true);
        Map<Long, List<Long>> taskSignatures = new HashMap<>();

        try {
            int numFinishedReplicas;
            int numSendedReplicas = 0;
            long startTime = System.currentTimeMillis();
            long maxWaitTimeMs = timeout * 1000L;

            for (PhysicalPartition partition : partitions) {
                if (!countDownLatch.getStatus().ok()) {
                    break;
                }
                Locker locker = new Locker();
                List<MaterializedIndex> indexList;
                locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
                try {
                    indexList = new ArrayList<>(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE));
                } finally {
                    locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
                }
                for (MaterializedIndex index : indexList) {
                    List<TabletMetadataUpdateAgentTask> tasks = buildUpdateTabletMetaTasks(db, table, partition, index);
                    AgentBatchTask agentBatchTask = sendUpdateTabletMetaTasks(tasks, countDownLatch);
                    LOG.info("Sent update tablet metadata task. tableName={} partitionId={} indexId={} taskNum={}",
                            tableName, partition.getId(), index.getId(), agentBatchTask.getTaskNum());
                    for (TabletMetadataUpdateAgentTask task : tasks) {
                        List<Long> signatures = taskSignatures.computeIfAbsent(task.getBackendId(), k -> new ArrayList<>());
                        signatures.add(task.getSignature());
                        numSendedReplicas += task.getTablets().size();
                    }
                    numFinishedReplicas = numReplicas - (int) countDownLatch.getCount();
                    while (numSendedReplicas - numFinishedReplicas >
                            Config.tablet_meta_update_max_pending_replicas_per_be * numBackends) {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime > startTime + maxWaitTimeMs) {
                            throw new TimeoutException("Wait in updatePartitionTabletMetaConcurrently exceeded timeout");
                        }
                        ThreadUtil.sleepAtLeastIgnoreInterrupts(100);
                        numFinishedReplicas = numReplicas - (int) countDownLatch.getCount();
                    }
                }
            }

            LOG.info("update partition tablet meta concurrently for {}, waiting for all tasks finish with timeout {}s",
                    table.getName(), timeout);
            TabletTaskExecutor.waitForFinished(countDownLatch, timeout);
            LOG.info("update partition tablet meta concurrently for {}, all tasks finished, took {}ms",
                    table.getName(), System.currentTimeMillis() - start);

        } catch (Exception e) {
            LOG.warn("Failed to execute updatePartitionTabletMetaConcurrently", e);
            countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.getMessage()));
            throw new DdlException(e.getMessage());
        } finally {
            if (!countDownLatch.getStatus().ok()) {
                for (Map.Entry<Long, List<Long>> entry : taskSignatures.entrySet()) {
                    for (Long signature : entry.getValue()) {
                        AgentTaskQueue.removeTask(entry.getKey(), TTaskType.UPDATE_TABLET_META_INFO, signature);
                    }
                }
            }
        }
    }

    private List<TabletMetadataUpdateAgentTask> buildUpdateTabletMetaTasks(Database db, OlapTable table,
                                                                           PhysicalPartition partition,
                                                                           MaterializedIndex index) throws DdlException {
        addDirtyPartitionIndex(partition.getId(), index.getId(), index);

        // be id -> <tablet id,schemaHash>
        Map<Long, Set<Long>> beIdToTabletSet = Maps.newHashMap();
        List<Tablet> tablets;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            tablets = new ArrayList<>(index.getTablets());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }

        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        for (Tablet tablet : tablets) {
            Long backendId = warehouseManager.getAliveComputeNodeId(computeResource, tablet.getId());
            if (backendId == null) {
                throw new AlterCancelException("no alive node");
            }
            Set<Long> set = beIdToTabletSet.computeIfAbsent(backendId, k -> Sets.newHashSet());
            set.add(tablet.getId());
        }

        List<TabletMetadataUpdateAgentTask> tasks = new ArrayList<>();
        for (Map.Entry<Long, Set<Long>> kv : beIdToTabletSet.entrySet()) {
            TabletMetadataUpdateAgentTask task = createTask(partition, index, kv.getKey(), kv.getValue());
            Preconditions.checkState(task != null, "task is null");
            task.setTxnId(watershedTxnId);
            tasks.add(task);
        }

        return tasks;
    }
    private AgentBatchTask sendUpdateTabletMetaTasks(List<TabletMetadataUpdateAgentTask> tasks,
                                                     MarkedCountDownLatch<Long, Set<Long>> countDownLatch) {
        AgentBatchTask agentBatchTask = new AgentBatchTask();
        for (TabletMetadataUpdateAgentTask task : tasks) {
            task.setLatch(countDownLatch);
            countDownLatch.addMark(task.getBackendId(), task.getTablets());
            agentBatchTask.addTask(task);
        }
        AgentTaskQueue.addBatchTask(agentBatchTask);
        AgentTaskExecutor.submit(agentBatchTask);
        return agentBatchTask;
    }
}
