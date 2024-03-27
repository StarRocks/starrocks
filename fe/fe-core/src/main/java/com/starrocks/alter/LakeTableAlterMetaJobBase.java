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
import com.starrocks.common.FeConstants;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.thrift.TTaskType;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public abstract class LakeTableAlterMetaJobBase extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(LakeTableAlterMetaJobBase.class);
    @SerializedName(value = "watershedTxnId")
    private long watershedTxnId = -1;
    // PhysicalPartitionId -> indexId -> MaterializedIndex
    @SerializedName(value = "partitionIndexMap")
    private Table<Long, Long, MaterializedIndex> physicalPartitionIndexMap = HashBasedTable.create();
    @SerializedName(value = "commitVersionMap")
    // Mapping from physical partition id to commit version
    private Map<Long, Long> commitVersionMap = new HashMap<>();

    public LakeTableAlterMetaJobBase(long jobId, JobType jobType, long dbId, long tableId,
                                                  String tableName, long timeoutMs) {
        super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {
        // send task to be
        List<Partition> partitions = Lists.newArrayList();
        OlapTable olapTable;
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);

        if (db == null) {
            throw new AlterCancelException("database does not exist, dbId:" + dbId);
        }
        db.readLock();

        try {
            olapTable = (OlapTable) db.getTable(tableName);
            if (olapTable == null) {
                throw new AlterCancelException("table does not exist, tableName:" + tableName);
            }
            partitions.addAll(olapTable.getPartitions());
        } finally {
            db.readUnlock();
        }

        if (this.watershedTxnId == -1) {
            this.watershedTxnId =
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        }

        try {
            for (Partition partition : partitions) {
                updatePartitionTabletMeta(db, olapTable.getName(), partition.getName());
            }
        } catch (DdlException e) {
            throw new AlterCancelException(e.getMessage());
        }

        this.jobState = JobState.RUNNING;
    }

    protected abstract TabletMetadataUpdateAgentTask createTask(long backendId, Set<Long> tablets);

    protected abstract void updateCatalog(Database db, LakeTable table);

    protected abstract void restoreState(LakeTableAlterMetaJobBase job);

    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        // do nothing
    }

    @Override
    protected void runRunningJob() throws AlterCancelException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database has been dropped
            throw new AlterCancelException("database does not exist, dbId:" + dbId);
        }
        db.writeLock();

        LakeTable table = (LakeTable) db.getTable(tableId);
        if (table == null) {
            // table has been dropped
            throw new AlterCancelException("table does not exist, tableId:" + tableId);
        }
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
            db.writeUnlock();
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
            LOG.info("publish version failed, will retry later. jobId={}", jobId);
            return;
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database has been dropped
            LOG.warn("database does not exist, dbId:" + dbId);
            throw new AlterCancelException("database does not exist, dbId:" + dbId);
        }
        db.writeLock();

        try {
            LakeTable table = (LakeTable) db.getTable(tableId);
            if (table == null) {
                // table has been dropped
                LOG.warn("table does not exist, tableId:" + tableId);
                throw new AlterCancelException("table does not exist, tableId:" + tableId);
            } else {
                updateCatalog(db, table);
            }
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);

            // set visible version
            updateVisibleVersion(table);
            table.setState(OlapTable.OlapTableState.NORMAL);

        } finally {
            db.writeUnlock();
        }

        LOG.info("update meta job finished: {}", jobId);
    }

    boolean readyToPublishVersion() throws AlterCancelException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database has been dropped
            throw new AlterCancelException("database does not exist, dbId:" + dbId);
        }
        db.readLock();

        LakeTable table = (LakeTable) db.getTable(tableId);
        if (table == null) {
            // table has been dropped
            throw new AlterCancelException("table does not exist, tableId:" + tableId);
        }
        try {
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
            db.readUnlock();
        }
        return true;
    }

    boolean publishVersion() {
        try {
            for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
                long commitVersion = commitVersionMap.get(partitionId);
                Map<Long, MaterializedIndex> dirtyIndexMap = physicalPartitionIndexMap.row(partitionId);
                for (MaterializedIndex index : dirtyIndexMap.values()) {
                    Utils.publishVersion(index.getTablets(), watershedTxnId, commitVersion - 1, commitVersion,
                            finishedTimeMs / 1000);
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

    public void updatePartitionTabletMeta(Database db,
                                          String tableName,
                                          String partitionName) throws DdlException {
        // be id -> <tablet id,schemaHash>
        Map<Long, Set<Long>> beIdToTabletSet = Maps.newHashMap();
        db.readLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableName);
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }

            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(
                        MaterializedIndex.IndexExtState.VISIBLE)) {
                    addDirtyPartitionIndex(physicalPartition.getId(), index.getId(), index);
                    for (Tablet tablet : index.getTablets()) {
                        Long backendId = Utils.chooseBackend((LakeTablet) tablet);
                        Set<Long> set = beIdToTabletSet.computeIfAbsent(backendId, k -> Sets.newHashSet());
                        set.add(tablet.getId());
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        int totalTaskNum = beIdToTabletSet.keySet().size();
        MarkedCountDownLatch<Long, Set<Long>> countDownLatch = new MarkedCountDownLatch<>(totalTaskNum);
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Map.Entry<Long, Set<Long>> kv : beIdToTabletSet.entrySet()) {
            countDownLatch.addMark(kv.getKey(), kv.getValue());
            TabletMetadataUpdateAgentTask task = createTask(kv.getKey(), kv.getValue());
            Preconditions.checkState(task != null, "task is null");
            task.setLatch(countDownLatch);
            task.setTxnId(watershedTxnId);
            batchTask.addTask(task);
        }
        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            LOG.info("send update tablet meta task for table {}, partitions {}, number: {}",
                    tableName, partitionName, batchTask.getTaskNum());

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
                String errMsg = "Failed to update partition[" + partitionName + "]. tablet meta.";
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
    }

    void updateNextVersion(@NotNull LakeTable table) {
        for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Preconditions.checkState(partition.getNextVersion() == commitVersion,
                    "partitionNextVersion=" + partition.getNextVersion() + " commitVersion=" + commitVersion);
            partition.setNextVersion(commitVersion + 1);
            LOG.info("LakeTableAlterMetaJob id: {} update next version of partition: {}, commitVersion: {}",
                    jobId, partition.getId(), commitVersion);
        }
    }

    void updateVisibleVersion(@NotNull LakeTable table) {
        for (long partitionId : physicalPartitionIndexMap.rowKeySet()) {
            PhysicalPartition partition = table.getPhysicalPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Preconditions.checkState(partition.getVisibleVersion() == commitVersion - 1,
                    "partitionVisitionVersion=" + partition.getVisibleVersion() + " commitVersion=" + commitVersion);
            partition.updateVisibleVersion(commitVersion);
            LOG.info("partitionVisibleVersion=" + partition.getVisibleVersion() + " commitVersion=" + commitVersion);
            LOG.info("LakeTableAlterMetaJob id: {} update visible version of partition: {}, visible Version: {}",
                    jobId, partition.getId(), commitVersion);
        }
    }

    @Override
    protected boolean cancelImpl(String errMsg) {
        if (jobState == JobState.CANCELLED || jobState == JobState.FINISHED) {
            return false;
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database has been dropped
            return false;
        }
        db.writeLock();
        LakeTable table;
        try {
            table = (LakeTable) db.getTable(tableId);

            // Cancel a job of state `FINISHED_REWRITING` only when the database or table has been dropped.
            if (jobState == JobState.FINISHED_REWRITING && table != null) {
                return false;
            }

            this.jobState = JobState.CANCELLED;
            this.errMsg = errMsg;
            this.finishedTimeMs = System.currentTimeMillis();

            if (table != null) {
                table.setState(OlapTable.OlapTableState.NORMAL);
            }

            if (span != null) {
                span.setStatus(StatusCode.ERROR, errMsg);
                span.end();
            }
        } finally {
            db.writeUnlock();
        }

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
            this.commitVersionMap = other.commitVersionMap;

            restoreState(other);
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            // database has been dropped
            LOG.warn("database does not exist, dbId:" + dbId);
            return;
        }
        db.writeLock();
        LakeTable table = (LakeTable) db.getTable(tableId);
        if (table == null) {
            return;
        }

        try {
            if (jobState == JobState.FINISHED_REWRITING) {
                updateNextVersion(table);
            } else if (jobState == JobState.FINISHED) {
                updateVisibleVersion(table);
                table.setState(OlapTable.OlapTableState.NORMAL);
            } else if (jobState == JobState.CANCELLED) {
                table.setState(OlapTable.OlapTableState.NORMAL);
            } else if (jobState == JobState.PENDING || jobState == JobState.WAITING_TXN) {
                // do nothing
            } else {
                throw new RuntimeException("unknown job state '{}'" + jobState.name());
            }
        } finally {
            db.writeUnlock();
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
}
