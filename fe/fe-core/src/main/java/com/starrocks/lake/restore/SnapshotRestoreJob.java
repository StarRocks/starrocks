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

package com.starrocks.lake.restore;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.Status;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.epack.persist.CreateTableInfoEPack;
import com.starrocks.lake.LakeTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.task.TabletRestoreAgentTask;
import com.starrocks.task.TabletTaskExecutor;
import com.starrocks.thrift.TRestoreTabletInfo;
import com.starrocks.thrift.TRestoreTabletRequest;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Job descriptor for table-level snapshot restore. FE builds one job per restore request and
 * dispatches physical-partition restore tasks to BE. The job keeps track of each partition restore
 * so that BE callbacks can update the state and the job can survive FE restarts.
 */
public class SnapshotRestoreJob extends AbstractJob {
    private static final Logger LOG = LogManager.getLogger(SnapshotRestoreJob.class);

    public enum State {
        PENDING,
        RUNNING,
        FINISHED,
        FAILED,
        CANCELLED
    }

    @SerializedName(value = "state")
    private State state;
    @SerializedName(value = "errorMessage")
    private String errorMessage;

    @SerializedName(value = "databaseId")
    private final long databaseId;
    @SerializedName(value = "tableId")
    private final long tableId;

    @SerializedName(value = "pendingTable")
    private final OlapTable pendingTable;
    @SerializedName(value = "tasks")
    private final Map<Long, RestoreTask> partitionTasks;

    public SnapshotRestoreJob(Long jobId,
                              String label,
                              long databaseId,
                              long tableId,
                              List<RestoreTask> tasks,
                              OlapTable pendingTable) {
        super(JobType.TABLE_SNAPSHOT_RESTORE);
        this.jobId = jobId;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.label = label;
        this.dbId = databaseId;
        this.dbName = "";
        this.timeoutMs = Config.tablet_restore_task_timeout_second * 1000L;
        this.partitionTasks = Maps.newLinkedHashMap();
        for (RestoreTask task : tasks) {
            long physicalPartitionId = task.getTargetPhysicalPartitionId();
            partitionTasks.put(physicalPartitionId, task);
        }
        this.createTime = System.currentTimeMillis();
        this.finishedTime = 0L;
        this.state = State.PENDING;
        this.errorMessage = "";

        this.pendingTable = pendingTable;
    }

    @Override
    public synchronized void run() {
        if (state == State.FINISHED || state == State.FAILED) {
            return;
        }
        if (state == State.CANCELLED) {
            cleanupRunningRegistrations();
            return;
        }
        if (state == State.PENDING) {
            state = State.RUNNING;
            LOG.info("Start tablet restore job {} for table {} in database {} ({} physical partitions)",
                    jobId, tableId, databaseId, partitionTasks.size());
        }

        if (hasFailedEntry()) {
            failJob("Tablet restore job failed due to failed subtasks");
            return;
        }

        if (allTasksCompleted()) {
            onJobFinished();
            return;
        }

        while (state == State.RUNNING) {
            boolean dispatched = false;
            for (Map.Entry<Long, RestoreTask> entry : partitionTasks.entrySet()) {
                RestoreTask task = entry.getValue();
                if (task.getTaskState() == RestoreTask.PartitionTaskState.PENDING) {
                    try {
                        dispatchTabletRestore(entry.getKey(), task);
                        dispatched = true;
                    } catch (Exception e) {
                        task.setTaskState(RestoreTask.PartitionTaskState.FAILED);
                        RestoreTask.TabletRestoreEntry entry2 = task.getRepresentativeEntry();
                        long targetTabletId = entry2 == null ? -1L : entry2.getTargetTabletId();
                        failJob(String.format("Failed to dispatch tablet restore task for tablet %d: %s",
                                targetTabletId, e.getMessage()));
                        return;
                    }
                }
            }
            if (!dispatched) {
                break;
            }
        }
    }

    @Override
    public synchronized Status cancel() {
        if (state == State.FINISHED || state == State.FAILED || state == State.CANCELLED) {
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "Job with label " + label + " can not be cancelled. state: " + state);
        }
        state = State.CANCELLED;
        status = new Status(Status.ErrCode.COMMON_ERROR, "Job cancelled");
        errorMessage = "Job cancelled";
        finishedTime = System.currentTimeMillis();
        cleanupRunningRegistrations();
        LOG.warn("Tablet restore job {} cancelled", jobId);
        return Status.OK;
    }

    public synchronized void onPartitionRestoreFinished(long physicalPartitionId, boolean success, String errMsg) {
        RestoreTask task = partitionTasks.get(physicalPartitionId);
        if (task == null) {
            LOG.warn("Tablet restore job {} received unknown physical partition {}", jobId, physicalPartitionId);
            return;
        }
        if (success) {
            task.setTaskState(RestoreTask.PartitionTaskState.SUCCESS);
            if (state == State.RUNNING && allTasksCompleted()) {
                onJobFinished();
            }
        } else {
            task.setTaskState(RestoreTask.PartitionTaskState.FAILED);
            RestoreTask.TabletRestoreEntry entry = task.getRepresentativeEntry();
            long targetTabletId = entry == null ? -1L : entry.getTargetTabletId();
            String message = errMsg == null ? "" : errMsg;
            failJob(String.format("Tablet restore failed for tablet %d: %s",
                    targetTabletId, message));
        }
    }

    private void dispatchTabletRestore(long physicalPartitionId, RestoreTask task) {

        RestoreTask.TabletRestoreEntry tabletRestoreEntry = task.getRepresentativeEntry();
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Long backendId = warehouseManager.getAliveComputeNodeId(WarehouseManager.DEFAULT_RESOURCE,
                tabletRestoreEntry.getSourceTabletId());

        if (backendId == null) {
            throw new IllegalStateException(String.format(
                    "No alive compute node available for tablet %d. All backends may be down.",
                    tabletRestoreEntry.getSourceTabletId()));
        }

        TabletRestoreAgentTask agentTask = new TabletRestoreAgentTask(
                null,
                backendId,
                databaseId,
                tableId,
                task.getTargetPhysicalPartitionId(),
                buildRestoreRequest(task));
        boolean sent = TabletTaskExecutor.sendTabletRestoreTask(agentTask);
        if (!sent) {
            throw new IllegalStateException("Failed to submit tablet restore task to agent");
        }
        task.setTaskState(RestoreTask.PartitionTaskState.RUNNING);

        LOG.info("Tablet restore job {} dispatched task signature {} to backend {}", jobId, physicalPartitionId, backendId);
    }

    private TRestoreTabletRequest buildRestoreRequest(
            RestoreTask task) {
        TRestoreTabletRequest request = new TRestoreTabletRequest();
        RestoreTask.TabletRestoreEntry representativeEntry = task.getRepresentativeEntry();
        if (representativeEntry == null) {
            throw new IllegalStateException("No tablet mapping found for physical partition restore task");
        }
        if (task.getSourceVisibleVersion() > 0) {
            request.setSource_visible_version(task.getSourceVisibleVersion());
        }

        List<TRestoreTabletInfo> tabletInfos = new ArrayList<>();
        for (RestoreTask.TabletRestoreEntry entry : task.getTabletEntries()) {
            TRestoreTabletInfo tabletInfo = new TRestoreTabletInfo();
            tabletInfo.setSource_tablet_id(entry.getSourceTabletId());
            tabletInfo.setTarget_tablet_id(entry.getTargetTabletId());
            if (entry.getTargetSchemaId() > 0) {
                tabletInfo.setTarget_schema_id(entry.getTargetSchemaId());
            }
            tabletInfos.add(tabletInfo);
        }
        if (!tabletInfos.isEmpty()) {
            request.setTablet_infos(tabletInfos);
        }
        return request;
    }

    private boolean hasFailedEntry() {
        for (RestoreTask task : partitionTasks.values()) {
            if (task.getTaskState() == RestoreTask.PartitionTaskState.FAILED) {
                return true;
            }
        }
        return false;
    }

    private boolean allTasksCompleted() {
        for (RestoreTask task : partitionTasks.values()) {
            if (task.getTaskState() != RestoreTask.PartitionTaskState.SUCCESS) {
                return false;
            }
        }
        return true;
    }

    private int getRunningTaskCount() {
        int count = 0;
        for (RestoreTask task : partitionTasks.values()) {
            if (task.getTaskState() == RestoreTask.PartitionTaskState.RUNNING) {
                count++;
            }
        }
        return count;
    }

    private void cleanupRunningRegistrations() {
        for (RestoreTask task : partitionTasks.values()) {
            if (task.getTaskState() == RestoreTask.PartitionTaskState.RUNNING) {
                if (state == State.FAILED || state == State.CANCELLED) {
                    task.setTaskState(RestoreTask.PartitionTaskState.FAILED);
                }
            }
        }
    }

    private void onJobFinished() {
        try {
            registerRestoredTable();
        } catch (StarRocksException e) {
            failJob("Failed to register restored table: " + e.getMessage());
            return;
        }
        state = State.FINISHED;
        status = Status.OK;
        finishedTime = System.currentTimeMillis();
        LOG.info("Tablet restore job {} finished; restored {} physical partitions", jobId, partitionTasks.size());
    }

    private void failJob(String message) {
        if (state == State.FAILED || state == State.CANCELLED) {
            return;
        }
        state = State.FAILED;
        status = new Status(Status.ErrCode.COMMON_ERROR, message);
        errorMessage = message;
        finishedTime = System.currentTimeMillis();
        cleanupRunningRegistrations();
        LOG.warn("Tablet restore job {} failed: {}", jobId, message);
    }

    public List<String> getInfo() {
        List<String> info = Lists.newArrayList();
        // JobId
        info.add(String.valueOf(jobId));
        // Label
        info.add(label);
        // Timestamp (snapshot timestamp - not applicable for snapshot restore, use empty string)
        info.add("");
        // DbName
        String dbName = "";
        if (globalStateMgr != null) {
            Database db = globalStateMgr.getLocalMetastore().getDb(databaseId);
            if (db != null) {
                dbName = db.getFullName();
            }
        }
        info.add(dbName);
        // State
        info.add(state.name());
        // AllowLoad (not applicable for snapshot restore)
        info.add("N/A");
        // ReplicationNum (not applicable for snapshot restore)
        info.add("N/A");
        // RestoreObjs
        info.add(getRestoreObjs());
        // CreateTime
        info.add(TimeUtils.longToTimeString(createTime));
        // MetaPreparedTime (not applicable, use N/A)
        info.add("N/A");
        // SnapshotFinishedTime (not applicable, use N/A)
        info.add("N/A");
        // DownloadFinishedTime (not applicable, use N/A)
        info.add("N/A");
        // FinishedTime
        info.add(TimeUtils.longToTimeString(finishedTime));
        // UnfinishedTasks
        info.add(getUnfinishedTasks());
        // Progress
        info.add(getProgress());
        // TaskErrMsg
        info.add(getTaskErrMsg());
        // Status
        info.add(status != null ? status.toString() : "OK");
        // Timeout
        info.add(String.valueOf(timeoutMs / 1000));
        return info;
    }

    private String getRestoreObjs() {
        if (pendingTable == null) {
            return "table: " + tableId;
        }
        return "table: " + pendingTable.getName() + " (id: " + tableId + ")";
    }

    private String getUnfinishedTasks() {
        List<String> unfinished = Lists.newArrayList();
        for (Map.Entry<Long, RestoreTask> entry : partitionTasks.entrySet()) {
            RestoreTask task = entry.getValue();
            if (task.getTaskState() != RestoreTask.PartitionTaskState.SUCCESS) {
                unfinished.add(String.valueOf(entry.getKey()));
            }
        }
        return Joiner.on(", ").join(unfinished);
    }

    private String getProgress() {
        int total = partitionTasks.size();
        int finished = 0;
        for (RestoreTask task : partitionTasks.values()) {
            if (task.getTaskState() == RestoreTask.PartitionTaskState.SUCCESS) {
                finished++;
            }
        }
        return String.format("[%d/%d]", finished, total);
    }

    private String getTaskErrMsg() {
        if (errorMessage != null && !errorMessage.isEmpty()) {
            return errorMessage;
        }
        return "";
    }

    private OlapTable loadPendingTable() throws StarRocksException {
        if (pendingTable == null) {
            throw new StarRocksException("Pending table metadata is missing");
        }
        return pendingTable;
    }

    protected void registerRestoredTable() throws StarRocksException {
        if (globalStateMgr == null) {
            throw new StarRocksException("GlobalStateMgr is not initialized");
        }
        Database targetDatabase = globalStateMgr.getLocalMetastore().getDb(databaseId);
        if (targetDatabase == null) {
            throw new StarRocksException(String.format("Target database %d not found", databaseId));
        }

        Locker locker = new Locker();
        locker.lockDatabase(targetDatabase.getId(), LockType.WRITE);
        boolean registered = false;
        OlapTable tableForRestore = null;
        try {
            if (!targetDatabase.isExist()) {
                throw new StarRocksException(String.format("Database '%s' has been dropped",
                        targetDatabase.getFullName()));
            }
            if (targetDatabase.getTable(tableId) != null) {
                return;
            }

            tableForRestore = loadPendingTable();
            tableForRestore.maySetDatabaseId(targetDatabase.getId());

            registered = targetDatabase.registerTableUnlocked(tableForRestore);
            if (!registered) {
                throw new StarRocksException(String.format("Failed to register table '%s.%s'",
                        targetDatabase.getFullName(), tableForRestore.getName()));
            }

            tableForRestore.onCreate(targetDatabase);
            registerTabletsInInvertedIndex(targetDatabase, tableForRestore);
        } catch (StarRocksException e) {
            if (registered && tableForRestore != null) {
                targetDatabase.unRegisterTableUnlocked(tableForRestore);
            }
            throw e;
        } catch (Throwable t) {
            if (registered && tableForRestore != null) {
                targetDatabase.unRegisterTableUnlocked(tableForRestore);
            }
            throw new StarRocksException("Failed to register restored table: " + t.getMessage(), t);
        } finally {
            locker.unLockDatabase(targetDatabase.getId(), LockType.WRITE);
        }

        String storageVolumeId = tableForRestore.getTableProperty() != null
                ? tableForRestore.getTableProperty().getStorageVolume() : null;
        CreateTableInfoEPack createInfo = new CreateTableInfoEPack(targetDatabase.getFullName(),
                tableForRestore, storageVolumeId, null, null);
        globalStateMgr.getEditLog().logCreateTable(createInfo);
    }

    private void registerTabletsInInvertedIndex(Database targetDatabase, OlapTable tableForRestore) {
        TabletInvertedIndex invertedIndex = globalStateMgr != null
                ? globalStateMgr.getTabletInvertedIndex()
                : GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        boolean isLakeTable = tableForRestore instanceof LakeTable;
        long dbId = targetDatabase.getId();

        for (Partition partition : tableForRestore.getPartitions()) {
            DataProperty dataProperty = tableForRestore.getPartitionInfo().getDataProperty(partition.getId());
            TStorageMedium storageMedium = dataProperty != null ? dataProperty.getStorageMedium() : TStorageMedium.HDD;
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long physicalPartitionId = physicalPartition.getId();
                for (MaterializedIndex index : physicalPartition.getLatestMaterializedIndices(IndexExtState.ALL)) {
                    TabletMeta tabletMeta = new TabletMeta(dbId, tableForRestore.getId(),
                            physicalPartitionId, index.getId(), storageMedium, isLakeTable);
                    for (Tablet tablet : index.getTablets()) {
                        invertedIndex.addTablet(tablet.getId(), tabletMeta);
                    }
                }
            }
        }
    }

    @Override
    public synchronized void replayRun() {
        if (state == State.PENDING) {
            state = State.RUNNING;
        }
    }

    @Override
    public synchronized void replayCancel() {
        state = State.CANCELLED;
        status = new Status(Status.ErrCode.COMMON_ERROR, "Job cancelled");
        finishedTime = System.currentTimeMillis();
    }

    @Override
    public boolean isDone() {
        return state == State.FINISHED || state == State.FAILED || state == State.CANCELLED;
    }

    @Override
    public boolean isPending() {
        return state == State.PENDING;
    }

    @Override
    public boolean isCancelled() {
        return state == State.CANCELLED;
    }

    public long getTableId() {
        return tableId;
    }

    public long getFinishedTimeMs() {
        return finishedTime;
    }

    public State getState() {
        return state;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
