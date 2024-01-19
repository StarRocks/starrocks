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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/backup/RestoreJob.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.backup;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Table.Cell;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.backup.BackupJobInfo.BackupIndexInfo;
import com.starrocks.backup.BackupJobInfo.BackupPartitionInfo;
import com.starrocks.backup.BackupJobInfo.BackupPhysicalPartitionInfo;
import com.starrocks.backup.BackupJobInfo.BackupTableInfo;
import com.starrocks.backup.BackupJobInfo.BackupTabletInfo;
import com.starrocks.backup.RestoreFileMapping.IdChain;
import com.starrocks.backup.Status.ErrCode;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.task.DirMoveTask;
import com.starrocks.task.DownloadTask;
import com.starrocks.task.ReleaseSnapshotTask;
import com.starrocks.task.SnapshotTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RestoreJob extends AbstractJob {
    private static final Logger LOG = LogManager.getLogger(RestoreJob.class);

    public enum RestoreJobState {
        PENDING, // Job is newly created. Check and prepare meta in globalStateMgr. Create replica if necessary.
        // Waiting for replica creation finished synchronously, then sending snapshot tasks.
        // then transfer to SNAPSHOTING
        SNAPSHOTING, // Waiting for snapshot finished. Than transfer to DOWNLOAD.
        DOWNLOAD, // Send download tasks.
        DOWNLOADING, // Waiting for download finished.
        COMMIT, // After download finished, all data is ready for taking effect.
        // Send movement tasks to BE, than transfer to COMMITTING
        COMMITTING, // wait all tasks finished. Transfer to FINISHED
        FINISHED,
        CANCELLED
    }

    @SerializedName(value = "backupTimestamp")
    private String backupTimestamp;

    @SerializedName(value = "jobInfo")
    protected BackupJobInfo jobInfo;

    // TODO temporarily ignore this property, this will cause fe exit in such case:
    // 1. Restored table is added into master memory in prepareMeta pause
    // 2. Loading to this table write a commit txn log
    // 3. Non-master node replaying commit txn log will encounter NullPointerException
    //    cause newly restored table has not been written to log
    @SerializedName(value = "allowLoad")
    private boolean allowLoad;

    @SerializedName(value = "state")
    private RestoreJobState state;

    @SerializedName(value = "backupMeta")
    protected BackupMeta backupMeta;

    @SerializedName(value = "fileMapping")
    protected RestoreFileMapping fileMapping = new RestoreFileMapping();

    @SerializedName(value = "metaPreparedTime")
    private long metaPreparedTime = -1;
    @SerializedName(value = "snapshotFinishedTime")
    private long snapshotFinishedTime = -1;
    @SerializedName(value = "downloadFinishedTime")
    private long downloadFinishedTime = -1;

    @SerializedName(value = "restoreReplicationNum")
    protected int restoreReplicationNum;

    // this 2 members is to save all newly restored objs
    // tbl name -> part
    @SerializedName(value = "restoredPartitions")
    protected List<Pair<String, Partition>> restoredPartitions = Lists.newArrayList();
    @SerializedName(value = "restoredTbls")
    private List<Table> restoredTbls = Lists.newArrayList();

    // save all restored partitions' version info which are already exist in globalStateMgr
    // table id -> partition id -> version
    @SerializedName(value = "restoredVersionInfo")
    private com.google.common.collect.Table<Long, Long, Long> restoredVersionInfo = HashBasedTable.create();
    // tablet id->(be id -> snapshot info)
    @SerializedName(value = "snapshotInfos")
    protected com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos = HashBasedTable.create();

    protected Map<Long, Long> unfinishedSignatureToId = Maps.newConcurrentMap();

    private MvRestoreContext mvRestoreContext;

    private AgentBatchTask batchTask;

    public RestoreJob() {
        super(JobType.RESTORE);
    }

    public RestoreJob(String label, String backupTs, long dbId, String dbName, BackupJobInfo jobInfo,
                      boolean allowLoad, int restoreReplicationNum, long timeoutMs,
                      GlobalStateMgr globalStateMgr, long repoId, BackupMeta backupMeta,
                      MvRestoreContext mvRestoreContext) {
        super(JobType.RESTORE, label, dbId, dbName, timeoutMs, globalStateMgr, repoId);
        this.backupTimestamp = backupTs;
        this.jobInfo = jobInfo;
        this.allowLoad = allowLoad;
        this.restoreReplicationNum = restoreReplicationNum;
        this.state = RestoreJobState.PENDING;
        this.backupMeta = backupMeta;
        this.mvRestoreContext = mvRestoreContext;
    }

    public RestoreJobState getState() {
        return state;
    }

    public RestoreFileMapping getFileMapping() {
        return fileMapping;
    }

    public BackupJobInfo getJobInfo() {
        return jobInfo;
    }

    public BackupMeta getBackupMeta() {
        return backupMeta;
    }

    public synchronized boolean finishTabletSnapshotTask(SnapshotTask task, TFinishTaskRequest request) {
        if (checkTaskStatus(task, task.getJobId(), request)) {
            return false;
        }

        Preconditions.checkState(request.isSetSnapshot_path());

        // snapshot path does not contain last 'tablet_id' and 'schema_hash' dir
        // eg:
        // /path/to/your/be/data/snapshot/20180410102311.0/
        // Full path will look like:
        // /path/to/your/be/data/snapshot/20180410102311.0/10006/352781111/
        SnapshotInfo info = new SnapshotInfo(task.getDbId(), task.getTableId(), task.getPartitionId(),
                task.getIndexId(), task.getTabletId(), task.getBackendId(),
                task.getSchemaHash(), request.getSnapshot_path(), Lists.newArrayList());

        snapshotInfos.put(task.getTabletId(), task.getBackendId(), info);
        taskProgress.remove(task.getSignature());
        Long removedTabletId = unfinishedSignatureToId.remove(task.getSignature());
        if (removedTabletId != null) {
            taskErrMsg.remove(task.getSignature());
            Preconditions.checkState(task.getTabletId() == removedTabletId, removedTabletId);
            LOG.debug("get finished snapshot info: {}, unfinished tasks num: {}, remove result: {}. {}",
                    info, unfinishedSignatureToId.size(), this, removedTabletId);
            return true;
        }
        return false;
    }

    public synchronized boolean finishTabletDownloadTask(DownloadTask task, TFinishTaskRequest request) {
        if (checkTaskStatus(task, task.getJobId(), request)) {
            return false;
        }

        Preconditions.checkState(request.isSetDownloaded_tablet_ids());

        for (Long tabletId : request.getDownloaded_tablet_ids()) {
            SnapshotInfo info = snapshotInfos.get(tabletId, task.getBackendId());
            if (info == null) {
                LOG.error("failed to find snapshot infos of tablet {} in be {}, {}",
                        tabletId, task.getBackendId(), this);
                return false;
            }
        }

        taskProgress.remove(task.getSignature());
        Long beId = unfinishedSignatureToId.remove(task.getSignature());
        if (beId == null || beId != task.getBackendId()) {
            LOG.error("invalid download task: {}. {}", task, this);
            return false;
        }

        taskErrMsg.remove(task.getSignature());
        return true;
    }

    public synchronized boolean finishDirMoveTask(DirMoveTask task, TFinishTaskRequest request) {
        if (checkTaskStatus(task, task.getJobId(), request)) {
            return false;
        }

        taskProgress.remove(task.getSignature());
        Long tabletId = unfinishedSignatureToId.remove(task.getSignature());
        if (tabletId == null || tabletId != task.getTabletId()) {
            LOG.error("invalid dir move task: {}. {}", task, this);
            return false;
        }

        taskErrMsg.remove(task.getSignature());
        return true;
    }

    private boolean checkTaskStatus(AgentTask task, long jobId, TFinishTaskRequest request) {
        Preconditions.checkState(jobId == this.jobId);
        Preconditions.checkState(dbId == task.getDbId());

        if (request.getTask_status().getStatus_code() != TStatusCode.OK) {
            taskErrMsg.put(task.getSignature(), Joiner.on(",").join(request.getTask_status().getError_msgs()));
            return true;
        }
        return false;
    }

    @Override
    public synchronized void replayRun() {
        LOG.info("replay run restore job: {}", this);
        switch (state) {
            case DOWNLOAD:
                replayCheckAndPrepareMeta();
                break;
            case FINISHED:
                replayWaitingAllTabletsCommitted();
                break;
            default:
                break;
        }
    }

    @Override
    public synchronized void replayCancel() {
        cancelInternal(true /* is replay */);
    }

    @Override
    public boolean isPending() {
        return state == RestoreJobState.PENDING;
    }

    @Override
    public boolean isCancelled() {
        return state == RestoreJobState.CANCELLED;
    }

    public void setRepo(Repository repo) {
        this.repo = repo;
    }

    @Override
    public void run() {
        if (state == RestoreJobState.FINISHED || state == RestoreJobState.CANCELLED) {
            return;
        }

        if (System.currentTimeMillis() - createTime > timeoutMs) {
            status = new Status(ErrCode.TIMEOUT, "restore job with label: " + label + "  timeout.");
            cancelInternal(false);
            return;
        }

        // get repo if not set
        if (repo == null) {
            repo = globalStateMgr.getBackupHandler().getRepoMgr().getRepo(repoId);
            if (repo == null) {
                status = new Status(ErrCode.COMMON_ERROR, "failed to get repository: " + repoId);
                cancelInternal(false);
                return;
            }
        }

        LOG.info("run restore job: {}", this);

        checkIfNeedCancel();

        if (status.ok()) {
            switch (state) {
                case PENDING:
                    checkAndPrepareMeta();
                    break;
                case SNAPSHOTING:
                    waitingAllSnapshotsFinished();
                    break;
                case DOWNLOAD:
                    downloadSnapshots();
                    break;
                case DOWNLOADING:
                    waitingAllDownloadFinished();
                    break;
                case COMMIT:
                    commit();
                    break;
                case COMMITTING:
                    waitingAllTabletsCommitted();
                    break;
                default:
                    break;
            }
        }

        if (!status.ok()) {
            cancelInternal(false);
        }
    }

    /**
     * return true if some restored objs have been dropped.
     */
    private void checkIfNeedCancel() {
        if (state == RestoreJobState.PENDING) {
            return;
        }

        Database db = globalStateMgr.getDb(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " has been dropped");
            return;
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            for (IdChain idChain : fileMapping.getMapping().keySet()) {
                OlapTable tbl = (OlapTable) db.getTable(idChain.getTblId());
                if (tbl == null) {
                    status = new Status(ErrCode.NOT_FOUND, "table " + idChain.getTblId() + " has been dropped");
                    return;
                }
                PhysicalPartition part = tbl.getPhysicalPartition(idChain.getPartId());
                if (part == null) {
                    status = new Status(ErrCode.NOT_FOUND, "partition " + idChain.getPartId() + " has been dropped");
                    return;
                }

                MaterializedIndex index = part.getIndex(idChain.getIdxId());
                if (index == null) {
                    status = new Status(ErrCode.NOT_FOUND, "index " + idChain.getIdxId() + " has been dropped");
                    return;
                }
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    /**
     * Restore rules as follow:
     * A. Table already exist
     * A1. Partition already exist, generate file mapping
     * A2. Partition does not exist, add restored partition to the table.
     * Reset all index/tablet/replica id, and create replica on BE outside the db lock.
     * B. Table does not exist
     * B1. Add table to the db, reset all table/index/tablet/replica id,
     * and create replica on BE outside the db lock.
     * <p>
     * All newly created table/partition/index/tablet/replica should be saved for rolling back.
     * <p>
     * Step:
     * 1. download and deserialize backup meta from repository.
     * 2. set all existing restored table's state to RESTORE.
     * 3. check if the expected restore objs are valid.
     * 4. create replicas if necessary.
     * 5. add restored objs to globalStateMgr.
     * 6. make snapshot for all replicas for incremental download later.
     */
    private void checkAndPrepareMeta() {
        MetricRepo.COUNTER_UNFINISHED_RESTORE_JOB.increase(1L);
        Database db = globalStateMgr.getDb(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
            return;
        }

        // generate job id
        jobId = globalStateMgr.getNextId();

        if (backupMeta == null) {
            status = new Status(ErrCode.COMMON_ERROR, "Failed to read backup meta from file");
            return;
        }

        // Set all restored tbls' state to RESTORE
        // Table's origin state must be NORMAL and does not have unfinished load job.
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        try {
            for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
                Table tbl = db.getTable(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
                if (tbl == null) {
                    continue;
                }

                if (!tbl.isNativeTableOrMaterializedView()) {
                    status = new Status(ErrCode.COMMON_ERROR, "Only support restore OLAP table: " + tbl.getName());
                    return;
                }

                OlapTable olapTbl = (OlapTable) tbl;
                if (olapTbl.getState() != OlapTableState.NORMAL) {
                    status = new Status(ErrCode.COMMON_ERROR,
                            "Table " + tbl.getName() + "'s state is not NORMAL: " + olapTbl.getState().name());
                    return;
                }

                if (olapTbl.existTempPartitions()) {
                    status = new Status(ErrCode.COMMON_ERROR, "Do not support restoring table with temp partitions");
                    return;
                }

                olapTbl.setState(OlapTableState.RESTORE);
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        // Check and prepare meta objects.
        batchTask = new AgentBatchTask();
        locker.lockDatabase(db, LockType.READ);
        try {
            for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
                Table remoteTbl = backupMeta.getTable(tblInfo.name);
                Preconditions.checkNotNull(remoteTbl);
                Table localTbl = db.getTable(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
                if (localTbl != null) {
                    if (localTbl instanceof OlapTable && localTbl.hasAutoIncrementColumn()) {
                        // it must be !isReplay == true
                        ((OlapTable) localTbl).sendDropAutoIncrementMapTask();
                    }
                    if (localTbl.isMaterializedView()) {
                        MaterializedView mv = (MaterializedView) localTbl;
                        if (mv.isActive()) {
                            // Skip to restore existed mv if mv is existed and active in the current local db,
                            // mv should be refreshed by local table changes rather than backup/restore because we don't
                            // track mv's version map with data's restore.
                            // eg: we restore mv's data (old version) but not restore associated version map which may
                            // cause wrong result if it can be used to rewrite.
                            LOG.warn("Skip to restore existed and active mv: {}", mv.getName());
                            continue;
                        }
                    }

                    tblInfo.checkAndRecoverAutoIncrementId(localTbl);
                    // table already exist, check schema
                    if (!localTbl.isNativeTableOrMaterializedView()) {
                        status = new Status(ErrCode.COMMON_ERROR,
                                "Only support retore olap table: " + localTbl.getName());
                        return;
                    }
                    OlapTable localOlapTbl = (OlapTable) localTbl;
                    OlapTable remoteOlapTbl = (OlapTable) remoteTbl;

                    List<String> intersectPartNames = Lists.newArrayList();
                    Status st = localOlapTbl.getIntersectPartNamesWith(remoteOlapTbl, intersectPartNames);
                    if (!st.ok()) {
                        status = st;
                        return;
                    }
                    LOG.debug("get intersect part names: {}, job: {}", intersectPartNames, this);
                    if (localOlapTbl.getSignature(BackupHandler.SIGNATURE_VERSION, intersectPartNames, true)
                            != remoteOlapTbl.getSignature(BackupHandler.SIGNATURE_VERSION, intersectPartNames, true) ||
                            localOlapTbl.getBaseSchema().size() != remoteOlapTbl.getBaseSchema().size()) {
                        List<Pair<Integer, String>> localCheckSumList = localOlapTbl.getSignatureSequence(
                                BackupHandler.SIGNATURE_VERSION, intersectPartNames);
                        List<Pair<Integer, String>> remoteCheckSumList = remoteOlapTbl.getSignatureSequence(
                                BackupHandler.SIGNATURE_VERSION, intersectPartNames);

                        String errMsg = "";
                        if (localCheckSumList.size() == remoteCheckSumList.size()) {
                            for (int i = 0; i < localCheckSumList.size(); ++i) {
                                int localCheckSum = ((Integer) localCheckSumList.get(i).first).intValue();
                                int remoteCheckSum = ((Integer) remoteCheckSumList.get(i).first).intValue();
                                if (localCheckSum != remoteCheckSum) {
                                    errMsg = ((String) localCheckSumList.get(i).second);
                                    break;
                                }
                            }
                        }

                        status = new Status(ErrCode.COMMON_ERROR,
                                "Table " + jobInfo.getAliasByOriginNameIfSet(tblInfo.name)
                                        + " already exist but with different schema, errMsg: "
                                        + errMsg);
                        return;
                    }

                    if (localOlapTbl.getKeysType() != remoteOlapTbl.getKeysType()) {
                        status = new Status(ErrCode.COMMON_ERROR,
                                "Table " + jobInfo.getAliasByOriginNameIfSet(tblInfo.name)
                                        + " already exist but with different key type");
                        return;
                    }

                    for (int i = 0; i < localOlapTbl.getBaseSchema().size(); ++i) {
                        Column localColumn = localOlapTbl.getBaseSchema().get(i);
                        Column remoteColumn = remoteOlapTbl.getBaseSchema().get(i);

                        if (!localColumn.equals(remoteColumn)) {
                            status = new Status(ErrCode.COMMON_ERROR,
                                    "Table " + jobInfo.getAliasByOriginNameIfSet(tblInfo.name)
                                            + " already exist but with different schema, column: " + localColumn.getName()
                                            + " in existed table is different from the column in backup snapshot, column: "
                                            + remoteColumn.getName());
                            return;
                        }
                    }

                    // Table with same name and has same schema. Check partition
                    for (BackupPartitionInfo backupPartInfo : tblInfo.partitions.values()) {
                        Partition localPartition = localOlapTbl.getPartition(backupPartInfo.name);
                        if (localPartition != null) {
                            if (!backupPartInfo.subPartitions.isEmpty()) {
                                status = new Status(ErrCode.COMMON_ERROR, "Partition " + backupPartInfo.name + " in table "
                                        + localTbl.getName() + " is automatic bucket partition, can not be restored");
                                return;
                            }
                            // Partition already exist.
                            PartitionInfo localPartInfo = localOlapTbl.getPartitionInfo();
                            if (localPartInfo.isRangePartition()) {
                                // If this is a range partition, check range
                                RangePartitionInfo localRangePartInfo = (RangePartitionInfo) localPartInfo;
                                RangePartitionInfo remoteRangePartInfo
                                        = (RangePartitionInfo) remoteOlapTbl.getPartitionInfo();
                                Range<PartitionKey> localRange = localRangePartInfo.getRange(localPartition.getId());
                                Range<PartitionKey> remoteRange = remoteRangePartInfo.getRange(backupPartInfo.id);
                                if (localRange.equals(remoteRange)) {
                                    // Same partition, same range
                                    if (genFileMappingWhenBackupReplicasEqual(localPartInfo, localPartition, localTbl,
                                            backupPartInfo, tblInfo)) {
                                        return;
                                    }
                                } else {
                                    // Same partition name, different range
                                    status = new Status(ErrCode.COMMON_ERROR, "Partition " + backupPartInfo.name
                                            + " in table " + localTbl.getName()
                                            + " has different range with partition in repository");
                                    return;
                                }
                            } else {
                                // If this is a single partitioned table.
                                if (genFileMappingWhenBackupReplicasEqual(localPartInfo, localPartition, localTbl,
                                        backupPartInfo, tblInfo)) {
                                    return;
                                }
                            }
                        } else {
                            // partitions does not exist
                            PartitionInfo localPartitionInfo = localOlapTbl.getPartitionInfo();
                            if (localPartitionInfo.isRangePartition()) {
                                // Check if the partition range can be added to the table
                                RangePartitionInfo localRangePartitionInfo = (RangePartitionInfo) localPartitionInfo;
                                RangePartitionInfo remoteRangePartitionInfo
                                        = (RangePartitionInfo) remoteOlapTbl.getPartitionInfo();
                                Range<PartitionKey> remoteRange = remoteRangePartitionInfo.getRange(backupPartInfo.id);
                                if (localRangePartitionInfo.getAnyIntersectRange(remoteRange, false) != null) {
                                    status = new Status(ErrCode.COMMON_ERROR, "Partition " + backupPartInfo.name
                                            + " in table " + localTbl.getName()
                                            + " has conflict range with existing ranges");
                                    return;
                                } else {
                                    // this partition can be added to this table, set ids
                                    Partition restorePart = resetPartitionForRestore(localOlapTbl, remoteOlapTbl,
                                            backupPartInfo.name,
                                            restoreReplicationNum);
                                    if (restorePart == null) {
                                        status = new Status(ErrCode.COMMON_ERROR, "Restored partition "
                                                + backupPartInfo.name + "of " + localTbl.getName() + "is null");
                                        return;
                                    }
                                    restoredPartitions.add(Pair.create(localOlapTbl.getName(), restorePart));
                                }
                            } else {
                                // It is impossible that a single partitioned table exist without any existing partition
                                status = new Status(ErrCode.COMMON_ERROR,
                                        "No partition exist in single partitioned table " + localOlapTbl.getName());
                                return;
                            }
                        }
                    }
                } else {
                    // Table does not exist
                    OlapTable remoteOlapTbl = (OlapTable) remoteTbl;

                    // Retain only expected restore partitions in this table;
                    Set<String> allPartNames = remoteOlapTbl.getPartitionNames();
                    for (String partName : allPartNames) {
                        if (!tblInfo.containsPart(partName)) {
                            remoteOlapTbl.dropPartitionAndReserveTablet(partName);
                        }
                    }

                    // reset all ids in this table
                    Status st = resetTableForRestore(remoteOlapTbl, db);
                    // TODO: delete the shards for lake table if it fails.
                    if (!st.ok()) {
                        status = st;
                        return;
                    }

                    tblInfo.checkAndRecoverAutoIncrementId((Table) remoteOlapTbl);

                    // DO NOT set remote table's new name here, cause we will still need the origin name later
                    // remoteOlapTbl.setName(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
                    remoteOlapTbl.setState(OlapTableState.RESTORE);
                    LOG.debug("put remote table {} to restoredTbls", remoteOlapTbl.getName());
                    restoredTbls.add(remoteOlapTbl);
                }
            } // end of all restore tables

            LOG.debug("finished to prepare restored partitions and tables. {}", this);
            // for now, nothing is modified in globalStateMgr

            // generate create replica tasks for all restored partitions
            for (Pair<String, Partition> entry : restoredPartitions) {
                OlapTable localTbl = (OlapTable) db.getTable(entry.first);
                Preconditions.checkNotNull(localTbl, localTbl.getName());
                Partition restorePart = entry.second;
                OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
                BackupPartitionInfo backupPartitionInfo
                        = jobInfo.getTableInfo(entry.first).getPartInfo(restorePart.getName());

                createReplicas(localTbl, restorePart);

                genFileMapping(localTbl, restorePart, remoteTbl.getId(), backupPartitionInfo, true);
            }

            // generate create replica task for all restored tables
            for (Table restoreTbl : restoredTbls) {
                for (Partition restorePart : restoreTbl.getPartitions()) {
                    createReplicas((OlapTable) restoreTbl, restorePart);
                    BackupTableInfo backupTableInfo = jobInfo.getTableInfo(restoreTbl.getName());
                    genFileMapping((OlapTable) restoreTbl, restorePart, backupTableInfo.id,
                            backupTableInfo.getPartInfo(restorePart.getName()), true);
                }
                // set restored table's new name after all 'genFileMapping'
                ((OlapTable) restoreTbl).setName(jobInfo.getAliasByOriginNameIfSet(restoreTbl.getName()));
            }

            LOG.debug("finished to generate create replica tasks. {}", this);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        // Send create replica task to BE outside the db lock
        sendCreateReplicaTasks();
        if (!status.ok()) {
            return;
        }

        // add all restored partition and tbls to globalStateMgr
        addRestorePartitionsAndTables(db);
        if (!status.ok()) {
            return;
        }

        LOG.info("finished to prepare meta. begin to make snapshot. {}", this);

        // begin to make snapshots for all replicas
        // snapshot is for incremental download
        prepareAndSendSnapshotTasks(db);
        if (!status.ok()) {
            return;
        }

        metaPreparedTime = System.currentTimeMillis();
        state = RestoreJobState.SNAPSHOTING;

        // No log here, PENDING state restore job will redo this method
        LOG.info("finished to prepare meta and send snapshot tasks, num: {}. {}",
                batchTask.getTaskNum(), this);
    }

    protected Status resetTableForRestore(OlapTable remoteOlapTbl, Database db) {
        return remoteOlapTbl.resetIdsForRestore(globalStateMgr, db, restoreReplicationNum, mvRestoreContext);
    }

    protected void sendCreateReplicaTasks() {
        if (batchTask.getTaskNum() > 0) {
            MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(batchTask.getTaskNum());
            for (AgentTask task : batchTask.getAllTasks()) {
                latch.addMark(task.getBackendId(), task.getTabletId());
                ((CreateReplicaTask) task).setLatch(latch);
                AgentTaskQueue.addTask(task);
            }
            AgentTaskExecutor.submit(batchTask);

            // estimate timeout, at most 10 min
            long timeout = Config.tablet_create_timeout_second * 1000L * batchTask.getTaskNum();
            timeout = Math.min(10L * 60L * 1000L, timeout);
            boolean ok = false;
            try {
                LOG.info("begin to send create replica tasks to BE for restore. total {} tasks. timeout: {}",
                        batchTask.getTaskNum(), timeout);
                ok = latch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
            }

            if (ok) {
                LOG.debug("finished to create all restored replcias. {}", this);
                return;
            }

            List<Entry<Long, Long>> unfinishedMarks = latch.getLeftMarks();
            // only show at most 10 results
            List<Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 10));
            String idStr = Joiner.on(", ").join(subList);
            status = new Status(ErrCode.COMMON_ERROR,
                    "Failed to create replicas for restore. unfinished marks: " + idStr);
        }
    }

    protected void addRestorePartitionsAndTables(Database db) {
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        try {
            // add restored partitions.
            // table should be in State RESTORE, so no other partitions can be
            // added to or removed from this table during the restore process.
            addRestoredPartitions(db, false);

            // add restored tables
            for (Table tbl : restoredTbls) {
                if (!db.registerTableUnlocked(tbl)) {
                    status = new Status(ErrCode.COMMON_ERROR, "Table " + tbl.getName()
                            + " already exist in db: " + db.getOriginName());
                    return;
                }
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    protected void prepareAndSendSnapshotTasks(Database db) {
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        Multimap<Long, Long> bePathsMap = HashMultimap.create();
        batchTask = new AgentBatchTask();
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            for (IdChain idChain : fileMapping.getMapping().keySet()) {
                OlapTable tbl = (OlapTable) db.getTable(idChain.getTblId());
                PhysicalPartition part = tbl.getPhysicalPartition(idChain.getPartId());
                MaterializedIndex index = part.getIndex(idChain.getIdxId());
                LocalTablet tablet = (LocalTablet) index.getTablet(idChain.getTabletId());
                Replica replica = tablet.getReplicaById(idChain.getReplicaId());
                long signature = globalStateMgr.getNextId();
                SnapshotTask task = new SnapshotTask(null, replica.getBackendId(), signature,
                        jobId, db.getId(),
                        tbl.getId(), part.getId(), index.getId(), tablet.getId(),
                        part.getVisibleVersion(),
                        tbl.getSchemaHashByIndexId(index.getId()), timeoutMs,
                        true /* is restore task*/);
                batchTask.addTask(task);
                unfinishedSignatureToId.put(signature, tablet.getId());
                bePathsMap.put(replica.getBackendId(), replica.getPathHash());
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        // check disk capacity
        com.starrocks.common.Status st =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkExceedDiskCapacityLimit(bePathsMap, true);
        if (!st.ok()) {
            status = new Status(ErrCode.COMMON_ERROR, st.getErrorMsg());
            return;
        }

        // send tasks
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);
    }

    private boolean genFileMappingWhenBackupReplicasEqual(PartitionInfo localPartInfo, Partition localPartition,
                                                          Table localTbl,
                                                          BackupPartitionInfo backupPartInfo, BackupTableInfo tblInfo) {
        if (localPartInfo.getReplicationNum(localPartition.getId()) != restoreReplicationNum) {
            status = new Status(ErrCode.COMMON_ERROR, "Partition " + backupPartInfo.name
                    + " in table " + localTbl.getName()
                    + " has different replication num '"
                    + localPartInfo.getReplicationNum(localPartition.getId())
                    + "' with partition in repository, which is " + restoreReplicationNum);
            return true;
        }

        // No need to check range, just generate file mapping
        OlapTable localOlapTbl = (OlapTable) localTbl;
        genFileMapping(localOlapTbl, localPartition, tblInfo.id, backupPartInfo,
                true /* overwrite when commit */);
        restoredVersionInfo.put(localOlapTbl.getId(), localPartition.getId(),
                backupPartInfo.version);
        return false;
    }

    protected void createReplicas(OlapTable localTbl, Partition restorePart) {
        Set<String> bfColumns = localTbl.getCopiedBfColumns();
        double bfFpp = localTbl.getBfFpp();
        for (PhysicalPartition physicalPartition : restorePart.getSubPartitions()) {
            for (MaterializedIndex restoredIdx : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                MaterializedIndexMeta indexMeta = localTbl.getIndexMetaByIndexId(restoredIdx.getId());
                TabletMeta tabletMeta = new TabletMeta(dbId, localTbl.getId(), physicalPartition.getId(),
                        restoredIdx.getId(), indexMeta.getSchemaHash(), TStorageMedium.HDD);
                for (Tablet restoreTablet : restoredIdx.getTablets()) {
                    GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                    for (Replica restoreReplica : ((LocalTablet) restoreTablet).getImmutableReplicas()) {
                        GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                                .addReplica(restoreTablet.getId(), restoreReplica);
                        LOG.info("tablet {} physical partition {} index {} replica {}",
                                restoreTablet.getId(), physicalPartition.getId(), restoredIdx.getId(),
                                restoreReplica.getId());
                        CreateReplicaTask task = new CreateReplicaTask(restoreReplica.getBackendId(), dbId,
                                localTbl.getId(), physicalPartition.getId(), restoredIdx.getId(),
                                restoreTablet.getId(), indexMeta.getShortKeyColumnCount(),
                                indexMeta.getSchemaHash(), indexMeta.getSchemaVersion(), restoreReplica.getVersion(),
                                indexMeta.getKeysType(), indexMeta.getStorageType(),
                                TStorageMedium.HDD /* all restored replicas will be saved to HDD */,
                                indexMeta.getSchema(), bfColumns, bfFpp, null,
                                localTbl.getCopiedIndexes(),
                                localTbl.isInMemory(),
                                localTbl.enablePersistentIndex(),
                                localTbl.primaryIndexCacheExpireSec(),
                                localTbl.getPartitionInfo().getTabletType(restorePart.getId()),
                                localTbl.getCompressionType(), indexMeta.getSortKeyIdxes(),
                                indexMeta.getSortKeyUniqueIds());
                        task.setInRestoreMode(true);
                        batchTask.addTask(task);
                    }
                }
            }
        }
    }

    // reset remote partition.
    // reset all id in remote partition, but DO NOT modify any exist globalStateMgr objects.
    public Partition resetPartitionForRestore(OlapTable localTbl, OlapTable remoteTbl, String partName,
                                              int restoreReplicationNum) {
        Preconditions.checkState(localTbl.getPartition(partName) == null);
        Partition remotePart = remoteTbl.getPartition(partName);
        Preconditions.checkNotNull(remotePart);
        PartitionInfo localPartitionInfo = localTbl.getPartitionInfo();
        Preconditions.checkState(localPartitionInfo.isRangePartition());

        // generate new partition id
        long newPartId = globalStateMgr.getNextId();
        remotePart.setIdForRestore(newPartId);

        // indexes
        Map<String, Long> localIdxNameToId = localTbl.getIndexNameToId();
        for (String localIdxName : localIdxNameToId.keySet()) {
            // set ids of indexes in remote partition to the local index ids
            long remoteIdxId = remoteTbl.getIndexIdByName(localIdxName);
            MaterializedIndex remoteIdx = remotePart.getIndex(remoteIdxId);
            long localIdxId = localIdxNameToId.get(localIdxName);
            remoteIdx.setIdForRestore(localIdxId);
            if (localIdxId != localTbl.getBaseIndexId()) {
                // not base table, reset
                remotePart.deleteRollupIndex(remoteIdxId);
                remotePart.createRollupIndex(remoteIdx);
            }
        }

        for (PhysicalPartition physicalPartition : remotePart.getSubPartitions()) {
            // generate new physical partition id
            if (physicalPartition.getId() != newPartId) {
                remotePart.removeSubPartition(physicalPartition.getId());

                long newPhysicalPartId = globalStateMgr.getNextId();
                physicalPartition.setIdForRestore(newPhysicalPartId);
                physicalPartition.setParentId(newPartId);
                remotePart.addSubPartition(physicalPartition);
            }
            // save version info for creating replicas
            long visibleVersion = physicalPartition.getVisibleVersion();

            // tablets
            for (MaterializedIndex remoteIdx : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                int schemaHash = remoteTbl.getSchemaHashByIndexId(remoteIdx.getId());
                int remotetabletSize = remoteIdx.getTablets().size();
                remoteIdx.clearTabletsForRestore();
                // generate new table
                status = remoteTbl.createTabletsForRestore(remotetabletSize, remoteIdx, globalStateMgr, restoreReplicationNum,
                        visibleVersion, schemaHash, physicalPartition.getId(), physicalPartition.getShardGroupId());
                if (!status.ok()) {
                    return null;
                }
            }
        }
        return remotePart;
    }

    protected void genFileMapping(OlapTable localTbl, Partition localPartition, Long remoteTblId,
                                  BackupPartitionInfo backupPartInfo, boolean overwrite) {
        if (localPartition.getSubPartitions().size() > 1) {
            genFileMappingWithSubPartition(localTbl, localPartition, remoteTblId, backupPartInfo, overwrite);
        } else {
            genFileMappingWithPartition(localTbl, localPartition, remoteTblId, backupPartInfo, overwrite);
        }
    }

    protected void genFileMappingWithPartition(OlapTable localTbl, Partition localPartition, Long remoteTblId,
                                               BackupPartitionInfo backupPartInfo, boolean overwrite) {
        for (MaterializedIndex localIdx : localPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            BackupIndexInfo backupIdxInfo = backupPartInfo.getIdx(localTbl.getIndexNameById(localIdx.getId()));
            Preconditions.checkState(backupIdxInfo.tablets.size() == localIdx.getTablets().size());
            for (int i = 0; i < localIdx.getTablets().size(); i++) {
                LocalTablet localTablet = (LocalTablet) localIdx.getTablets().get(i);
                BackupTabletInfo backupTabletInfo = backupIdxInfo.tablets.get(i);
                for (Replica localReplica : localTablet.getImmutableReplicas()) {
                    IdChain src = new IdChain(remoteTblId, backupPartInfo.id, backupIdxInfo.id, backupTabletInfo.id,
                            -1L /* no replica id */);
                    IdChain dest = new IdChain(localTbl.getId(), localPartition.getId(),
                            localIdx.getId(), localTablet.getId(), localReplica.getId());
                    fileMapping.putMapping(dest, src, overwrite);
                    LOG.debug("tablet mapping: {} to {} file mapping: {} to {}",
                            backupTabletInfo.id, localTablet.getId(), src, dest);
                }
            }
        }
    }

    // files in repo to files in local
    protected void genFileMappingWithSubPartition(OlapTable localTbl, Partition localPartition, Long remoteTblId,
                                                  BackupPartitionInfo backupPartInfo, boolean overwrite) {
        for (PhysicalPartition physicalPartition : localPartition.getSubPartitions()) {
            BackupPhysicalPartitionInfo physicalPartitionInfo = backupPartInfo.subPartitions.get(
                    physicalPartition.getBeforeRestoreId());
            for (MaterializedIndex localIdx : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo backupIdxInfo = physicalPartitionInfo.getIdx(localTbl.getIndexNameById(localIdx.getId()));
                Preconditions.checkState(backupIdxInfo.tablets.size() == localIdx.getTablets().size());
                for (int i = 0; i < localIdx.getTablets().size(); i++) {
                    LocalTablet localTablet = (LocalTablet) localIdx.getTablets().get(i);
                    BackupTabletInfo backupTabletInfo = backupIdxInfo.tablets.get(i);
                    for (Replica localReplica : localTablet.getImmutableReplicas()) {
                        IdChain src = new IdChain(remoteTblId, physicalPartitionInfo.id, backupIdxInfo.id, backupTabletInfo.id,
                                -1L /* no replica id */);
                        IdChain dest = new IdChain(localTbl.getId(), physicalPartition.getId(),
                                localIdx.getId(), localTablet.getId(), localReplica.getId());
                        fileMapping.putMapping(dest, src, overwrite);
                        LOG.debug("tablet mapping: {} to {} file mapping: {} to {}",
                                backupTabletInfo.id, localTablet.getId(), src, dest);
                    }
                }
            }
        }
    }

    private void replayCheckAndPrepareMeta() {
        Database db = globalStateMgr.getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        try {
            // replay set all existing tables's state to RESTORE
            for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
                Table tbl = db.getTable(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
                if (tbl == null) {
                    continue;
                }
                OlapTable olapTbl = (OlapTable) tbl;
                olapTbl.setState(OlapTableState.RESTORE);
            }

            // restored partitions
            addRestoredPartitions(db, true);

            // restored tables
            for (Table restoreTbl : restoredTbls) {
                db.registerTableUnlocked(restoreTbl);
                // modify tablet inverted index
                for (Partition restorePart : restoreTbl.getPartitions()) {
                    modifyInvertedIndex((OlapTable) restoreTbl, restorePart);
                }
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        LOG.info("replay check and prepare meta. {}", this);
    }

    protected void addRestoredPartitions(Database db, boolean modify) {
        for (Pair<String, Partition> entry : restoredPartitions) {
            OlapTable localTbl = (OlapTable) db.getTable(entry.first);
            Partition restorePart = entry.second;
            OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
            RangePartitionInfo localPartitionInfo = (RangePartitionInfo) localTbl.getPartitionInfo();
            RangePartitionInfo remotePartitionInfo = (RangePartitionInfo) remoteTbl.getPartitionInfo();
            BackupPartitionInfo backupPartitionInfo =
                    jobInfo.getTableInfo(entry.first).getPartInfo(restorePart.getName());
            long remotePartId = backupPartitionInfo.id;
            Range<PartitionKey> remoteRange = remotePartitionInfo.getRange(remotePartId);
            DataProperty remoteDataProperty = remotePartitionInfo.getDataProperty(remotePartId);
            localPartitionInfo.addPartition(restorePart.getId(), false, remoteRange,
                    remoteDataProperty, (short) restoreReplicationNum,
                    remotePartitionInfo.getIsInMemory(remotePartId));
            localTbl.addPartition(restorePart);
            if (modify) {
                // modify tablet inverted index
                modifyInvertedIndex(localTbl, restorePart);
            }
        }
    }

    protected void modifyInvertedIndex(OlapTable restoreTbl, Partition restorePart) {
        for (MaterializedIndex restoreIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
            int schemaHash = restoreTbl.getSchemaHashByIndexId(restoreIdx.getId());
            TabletMeta tabletMeta = new TabletMeta(dbId, restoreTbl.getId(), restorePart.getId(),
                    restoreIdx.getId(), schemaHash, TStorageMedium.HDD);
            for (Tablet restoreTablet : restoreIdx.getTablets()) {
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                for (Replica restoreReplica : ((LocalTablet) restoreTablet).getImmutableReplicas()) {
                    GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                            .addReplica(restoreTablet.getId(), restoreReplica);
                }
            }
        }
    }

    private void waitingAllSnapshotsFinished() {
        if (unfinishedSignatureToId.isEmpty()) {
            snapshotFinishedTime = System.currentTimeMillis();
            state = RestoreJobState.DOWNLOAD;

            globalStateMgr.getEditLog().logRestoreJob(this);
            LOG.info("finished making snapshots. {}", this);
            return;
        }

        LOG.info("waiting {} replicas to make snapshot: [{}]. {}",
                unfinishedSignatureToId.size(), unfinishedSignatureToId, this);
        return;
    }

    private void downloadSnapshots() {
        // Categorize snapshot infos by db id.
        ArrayListMultimap<Long, SnapshotInfo> dbToSnapshotInfos = ArrayListMultimap.create();
        for (SnapshotInfo info : snapshotInfos.values()) {
            dbToSnapshotInfos.put(info.getDbId(), info);
        }

        // Send download tasks
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        batchTask = new AgentBatchTask();
        for (long dbId : dbToSnapshotInfos.keySet()) {
            List<SnapshotInfo> infos = dbToSnapshotInfos.get(dbId);

            Database db = globalStateMgr.getDb(dbId);
            if (db == null) {
                status = new Status(ErrCode.NOT_FOUND, "db " + dbId + " does not exist");
                return;
            }

            // We classify the snapshot info by backend
            ArrayListMultimap<Long, SnapshotInfo> beToSnapshots = ArrayListMultimap.create();
            for (SnapshotInfo info : infos) {
                beToSnapshots.put(info.getBeId(), info);
            }

            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                for (Long beId : beToSnapshots.keySet()) {
                    List<SnapshotInfo> beSnapshotInfos = beToSnapshots.get(beId);
                    List<FsBroker> brokerAddrs = Lists.newArrayList();
                    THdfsProperties hdfsProperties = new THdfsProperties();
                    if (repo.getStorage().hasBroker()) {
                        Status st = repo.getBrokerAddress(beId, globalStateMgr, brokerAddrs);
                        if (!st.ok()) {
                            status = st;
                            return;
                        }
                        Preconditions.checkState(brokerAddrs.size() == 1);
                    } else {
                        BrokerDesc brokerDesc = new BrokerDesc(repo.getStorage().getProperties());
                        try {
                            HdfsUtil.getTProperties(repo.getLocation(), brokerDesc, hdfsProperties);
                        } catch (UserException e) {
                            status = new Status(ErrCode.COMMON_ERROR, "Get properties from " + repo.getLocation() + " error.");
                            return;
                        }
                    }
                    // allot tasks
                    prepareDownloadTasks(beSnapshotInfos, db, beId, brokerAddrs, hdfsProperties);
                    if (!status.ok()) {
                        return;
                    }
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }

        // send task
        sendDownloadTasks();

        state = RestoreJobState.DOWNLOADING;

        // No edit log here
        LOG.info("finished to send download tasks to BE. num: {}. {}", batchTask.getTaskNum(), this);
        return;
    }

    protected void waitingAllDownloadFinished() {
        if (unfinishedSignatureToId.isEmpty()) {
            downloadFinishedTime = System.currentTimeMillis();
            state = RestoreJobState.COMMIT;

            // backupMeta is useless now
            backupMeta = null;

            globalStateMgr.getEditLog().logRestoreJob(this);
            LOG.info("finished to download. {}", this);
        }

        LOG.info("waiting {} tasks to finish downloading from repo. {}", unfinishedSignatureToId.size(), this);
    }

    protected void prepareDownloadTasks(List<SnapshotInfo> beSnapshotInfos, Database db, long beId, List<FsBroker> brokerAddrs,
                                        THdfsProperties hdfsProperties) {
        int totalNum = beSnapshotInfos.size();
        int batchNum = totalNum;
        if (Config.max_download_task_per_be > 0) {
            batchNum = Math.min(totalNum, Config.max_download_task_per_be);
        }
        // each task contains several upload subtasks
        int taskNumPerBatch = Math.max(totalNum / batchNum, 1);
        LOG.debug("backend {} has {} batch, total {} tasks, {}",
                beId, batchNum, totalNum, this);

        int index = 0;
        for (int batch = 0; batch < batchNum; batch++) {
            Map<String, String> srcToDest = Maps.newHashMap();
            int currentBatchTaskNum = (batch == batchNum - 1) ? totalNum - index : taskNumPerBatch;
            for (int j = 0; j < currentBatchTaskNum; j++) {
                SnapshotInfo info = beSnapshotInfos.get(index++);
                Table tbl = db.getTable(info.getTblId());
                if (tbl == null) {
                    status = new Status(ErrCode.NOT_FOUND, "restored table "
                            + info.getTabletId() + " does not exist");
                    return;
                }
                OlapTable olapTbl = (OlapTable) tbl;

                PhysicalPartition part = olapTbl.getPhysicalPartition(info.getPartitionId());
                if (part == null) {
                    status = new Status(ErrCode.NOT_FOUND, "partition "
                            + info.getPartitionId() + " does not exist in restored table: "
                            + tbl.getName());
                    return;
                }

                MaterializedIndex idx = part.getIndex(info.getIndexId());
                if (idx == null) {
                    status = new Status(ErrCode.NOT_FOUND,
                            "index " + info.getIndexId() + " does not exist in partion " + part.getId()
                                    + "of restored table " + tbl.getName());
                    return;
                }

                LocalTablet tablet = (LocalTablet) idx.getTablet(info.getTabletId());
                if (tablet == null) {
                    status = new Status(ErrCode.NOT_FOUND,
                            "tablet " + info.getTabletId() + " does not exist in restored table "
                                    + tbl.getName());
                    return;
                }

                Replica replica = tablet.getReplicaByBackendId(info.getBeId());
                if (replica == null) {
                    status = new Status(ErrCode.NOT_FOUND,
                            "replica in be " + info.getBeId() + " of tablet "
                                    + tablet.getId() + " does not exist in restored table "
                                    + tbl.getName());
                    return;
                }

                IdChain catalogIds = new IdChain(tbl.getId(), part.getId(), idx.getId(),
                        info.getTabletId(), replica.getId());
                IdChain repoIds = fileMapping.get(catalogIds);
                if (repoIds == null) {
                    status = new Status(ErrCode.NOT_FOUND,
                            "failed to get id mapping of globalStateMgr ids: " + catalogIds.toString());
                    LOG.info("current file mapping: {}", fileMapping);
                    return;
                }

                String repoTabletPath = jobInfo.getFilePath(repoIds);

                // eg:
                // bos://location/__starrocks_repository_my_repo/_ss_my_ss/_ss_content/__db_10000/
                // __tbl_10001/__part_10002/_idx_10001/__10003
                String src = repo.getRepoPath(label, repoTabletPath);
                SnapshotInfo snapshotInfo = snapshotInfos.get(info.getTabletId(), info.getBeId());
                Preconditions.checkNotNull(snapshotInfo, info.getTabletId() + "-" + info.getBeId());
                // download to previously existing snapshot dir
                String dest = snapshotInfo.getTabletPath();
                srcToDest.put(src, dest);
                LOG.debug("catalog id: {}, repo id: {}, repoTabletPath: {}, src: {}, dest: {}",
                        catalogIds, repoIds, repoTabletPath, src, dest);
            }
            long signature = globalStateMgr.getNextId();
            DownloadTask task;
            if (repo.getStorage().hasBroker()) {
                task = new DownloadTask(null, beId, signature, jobId, dbId,
                        srcToDest, brokerAddrs.get(0), repo.getStorage().getProperties());
            } else {
                task = new DownloadTask(null, beId, signature, jobId, dbId,
                        srcToDest, null, repo.getStorage().getProperties(), hdfsProperties);
            }
            batchTask.addTask(task);
            unfinishedSignatureToId.put(signature, beId);
        }
    }

    protected void sendDownloadTasks() {
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);
    }

    private void commit() {
        // Send task to move the download dir
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();

        prepareAndSendDirMoveTasks();

        state = RestoreJobState.COMMITTING;
    }

    protected void prepareAndSendDirMoveTasks() {
        batchTask = new AgentBatchTask();
        // tablet id->(be id -> download info)
        for (Cell<Long, Long, SnapshotInfo> cell : snapshotInfos.cellSet()) {
            SnapshotInfo info = cell.getValue();
            long signature = globalStateMgr.getNextId();
            DirMoveTask task = new DirMoveTask(null, cell.getColumnKey(), signature, jobId, dbId,
                    info.getTblId(), info.getPartitionId(), info.getTabletId(),
                    cell.getRowKey(), info.getTabletPath(), info.getSchemaHash(),
                    true /* need reload tablet header */);
            batchTask.addTask(task);
            unfinishedSignatureToId.put(signature, info.getTabletId());
        }

        // send task
        for (AgentTask task : batchTask.getAllTasks()) {
            AgentTaskQueue.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);

        // No log here
        LOG.info("finished to send move dir tasks. num: {}. {}", batchTask.getTaskNum(), this);
    }

    protected void waitingAllTabletsCommitted() {
        if (unfinishedSignatureToId.isEmpty()) {
            LOG.info("finished to commit all tablet. {}", this);
            Status st = allTabletCommitted(false /* not replay */);
            if (!st.ok()) {
                status = st;
            }
            MetricRepo.COUNTER_UNFINISHED_RESTORE_JOB.increase(-1L);
            return;
        }
        LOG.info("waiting {} tablets to commit. {}", unfinishedSignatureToId.size(), this);
    }

    private Status allTabletCommitted(boolean isReplay) {
        Database db = globalStateMgr.getDb(dbId);
        if (db == null) {
            return new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
        }

        // set all restored partition version
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        try {
            // set all tables' state to NORMAL
            setTableStateToNormal(db);

            for (long tblId : restoredVersionInfo.rowKeySet()) {
                Table tbl = db.getTable(tblId);
                if (tbl == null) {
                    continue;
                }
                OlapTable olapTbl = (OlapTable) tbl;
                Map<Long, Long> map = restoredVersionInfo.rowMap().get(tblId);
                for (Map.Entry<Long, Long> entry : map.entrySet()) {
                    long partId = entry.getKey();
                    PhysicalPartition part = olapTbl.getPhysicalPartition(partId);
                    if (part == null) {
                        continue;
                    }

                    // update partition visible version
                    part.updateVersionForRestore(entry.getValue());

                    // we also need to update the replica version of these overwritten restored partitions
                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        updateTablets(idx, part);
                    }

                    LOG.debug("restore set partition {} version in table {}, version: {}",
                            partId, tblId, entry.getValue());
                }
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        if (!isReplay) {
            restoredPartitions.clear();
            restoredTbls.clear();

            // release snapshot before clearing snapshotInfos
            releaseSnapshots();

            snapshotInfos.clear();

            finishedTime = System.currentTimeMillis();
            state = RestoreJobState.FINISHED;

            globalStateMgr.getEditLog().logRestoreJob(this);

            locker.lockDatabase(db, LockType.READ);
            try {
                for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
                    Table tbl = db.getTable(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
                    if (tbl == null) {
                        LOG.warn("skip post actions after restore success, table name does not existed: %s",
                                tblInfo.name);
                        continue;
                    }
                    if (!tbl.isNativeTableOrMaterializedView()) {
                        continue;
                    }
                    LOG.info("do post actions for table : {}", tbl.getName());

                    // only for olap table
                    if (tbl.isOlapTable()) {
                        try {
                            // register table in DynamicPartitionScheduler after restore job finished
                            DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), (OlapTable) tbl);
                        } catch (Exception e) {
                            // no throw exceptions
                            LOG.warn(String.format("register table %s for dynamic partition scheduler failed: ",
                                    tbl.getName()), e);
                        }
                    }

                    // rebuild olap table after restore job finished,
                    // - for base table, update existed materialized view's base table info
                    // - for materialized view, update existed materialized view's base table infos
                    if (tbl instanceof OlapTable) {
                        OlapTable olapTable = (OlapTable) tbl;
                        try {
                            olapTable.doAfterRestore(mvRestoreContext);
                        } catch (Exception e) {
                            // no throw exceptions
                            LOG.warn(String.format("rebuild olap table %s failed: ", olapTable.getName()), e);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Do post actions after restore success failed: ", e);
                throw e;
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }

        LOG.info("job is finished. is replay: {}. {}", isReplay, this);
        return Status.OK;
    }

    protected void updateTablets(MaterializedIndex idx, PhysicalPartition part) {
        for (Tablet tablet : idx.getTablets()) {
            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                // force update all info for all replica
                replica.updateForRestore(part.getVisibleVersion(),
                        replica.getDataSize(), replica.getRowCount());
                replica.setLastReportVersion(part.getVisibleVersion());
            }
        }
    }

    protected void releaseSnapshots() {
        if (snapshotInfos.isEmpty()) {
            return;
        }
        // we do not care about the release snapshot tasks' success or failure,
        // the GC thread on BE will sweep the snapshot, finally.
        batchTask = new AgentBatchTask();
        for (SnapshotInfo info : snapshotInfos.values()) {
            ReleaseSnapshotTask releaseTask = new ReleaseSnapshotTask(null, info.getBeId(), info.getDbId(),
                    info.getTabletId(), info.getPath());
            batchTask.addTask(releaseTask);
        }
        AgentTaskExecutor.submit(batchTask);
        LOG.info("send {} release snapshot tasks, job: {}", snapshotInfos.size(), this);
    }

    private void replayWaitingAllTabletsCommitted() {
        allTabletCommitted(true /* is replay */);
    }

    public List<String> getInfo() {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(jobId));
        info.add(label);
        info.add(backupTimestamp);
        info.add(dbName);
        info.add(state.name());
        info.add(String.valueOf(allowLoad));
        info.add(String.valueOf(restoreReplicationNum));
        info.add(getRestoreObjs());
        info.add(TimeUtils.longToTimeString(createTime));
        info.add(TimeUtils.longToTimeString(metaPreparedTime));
        info.add(TimeUtils.longToTimeString(snapshotFinishedTime));
        info.add(TimeUtils.longToTimeString(downloadFinishedTime));
        info.add(TimeUtils.longToTimeString(finishedTime));
        info.add(Joiner.on(", ").join(unfinishedSignatureToId.entrySet()));
        info.add(Joiner.on(", ").join(taskProgress.entrySet().stream().map(
                e -> "[" + e.getKey() + ": " + e.getValue().first + "/" + e.getValue().second + "]").collect(
                Collectors.toList())));
        info.add(Joiner.on(", ").join(taskErrMsg.entrySet().stream().map(n -> "[" + n.getKey() + ": " + n.getValue()
                + "]").collect(Collectors.toList())));
        info.add(status.toString());
        info.add(String.valueOf(timeoutMs / 1000));
        return info;
    }

    private String getRestoreObjs() {
        Preconditions.checkState(jobInfo != null);
        return jobInfo.getInfo();
    }

    @Override
    public boolean isDone() {
        if (state == RestoreJobState.FINISHED || state == RestoreJobState.CANCELLED) {
            return true;
        }
        return false;
    }

    // cancel by user
    @Override
    public synchronized Status cancel() {
        if (isDone()) {
            return new Status(ErrCode.COMMON_ERROR,
                    "Job with label " + label + " can not be cancelled. state: " + state);
        }

        status = new Status(ErrCode.COMMON_ERROR, "user cancelled, current state: " + state.name());
        cancelInternal(false);
        MetricRepo.COUNTER_UNFINISHED_RESTORE_JOB.increase(-1L);
        return Status.OK;
    }

    public void cancelInternal(boolean isReplay) {
        // We need to clean the residual due to current state
        if (!isReplay) {
            switch (state) {
                case SNAPSHOTING:
                    // remove all snapshot tasks in AgentTaskQueue
                    for (Long taskId : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.MAKE_SNAPSHOT, taskId);
                    }
                    break;
                case DOWNLOADING:
                    // remove all down tasks in AgentTaskQueue
                    for (Long taskId : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.DOWNLOAD, taskId);
                    }
                    break;
                case COMMITTING:
                    // remove all dir move tasks in AgentTaskQueue
                    for (Long taskId : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.MOVE, taskId);
                    }
                    break;
                default:
                    break;
            }
        }

        // clean restored objs
        Database db = globalStateMgr.getDb(dbId);
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.WRITE);
            try {
                // rollback table's state to NORMAL
                setTableStateToNormal(db);

                // remove restored tbls
                for (Table restoreTbl : restoredTbls) {
                    LOG.info("remove restored table when cancelled: {}", restoreTbl.getName());
                    for (Partition part : restoreTbl.getPartitions()) {
                        for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Tablet tablet : idx.getTablets()) {
                                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tablet.getId());
                            }
                        }
                    }
                    db.dropTable(restoreTbl.getName());
                }

                // remove restored partitions
                for (Pair<String, Partition> entry : restoredPartitions) {
                    OlapTable restoreTbl = (OlapTable) db.getTable(entry.first);
                    if (restoreTbl == null) {
                        continue;
                    }
                    LOG.info("remove restored partition in table {} when cancelled: {}",
                            restoreTbl.getName(), entry.second.getName());

                    restoreTbl.dropPartition(dbId, entry.second.getName(), true /* is restore */);
                }
            } finally {
                locker.unLockDatabase(db, LockType.WRITE);
            }
        }

        if (!isReplay) {
            // backupMeta is useless
            backupMeta = null;

            releaseSnapshots();

            snapshotInfos.clear();
            RestoreJobState curState = state;
            finishedTime = System.currentTimeMillis();
            state = RestoreJobState.CANCELLED;
            // log
            globalStateMgr.getEditLog().logRestoreJob(this);

            LOG.info("finished to cancel restore job. current state: {}. is replay: {}. {}",
                    curState.name(), isReplay, this);
            return;
        }

        LOG.info("finished to cancel restore job. is replay: {}. {}", isReplay, this);
    }

    private void setTableStateToNormal(Database db) {
        for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
            Table tbl = db.getTable(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
            if (tbl == null) {
                continue;
            }

            if (!tbl.isNativeTableOrMaterializedView()) {
                continue;
            }

            OlapTable olapTbl = (OlapTable) tbl;
            if (olapTbl.getState() == OlapTableState.RESTORE
                    || olapTbl.getState() == OlapTableState.RESTORE_WITH_LOAD) {
                olapTbl.setState(OlapTableState.NORMAL);
            }
        }
    }

    public static RestoreJob read(DataInput in) throws IOException {
        RestoreJob job = new RestoreJob();
        job.readFields(in);
        return job;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, backupTimestamp);
        jobInfo.write(out);
        out.writeBoolean(allowLoad);

        Text.writeString(out, state.name());

        if (backupMeta != null) {
            out.writeBoolean(true);
            backupMeta.write(out);
        } else {
            out.writeBoolean(false);
        }

        fileMapping.write(out);

        out.writeLong(metaPreparedTime);
        out.writeLong(snapshotFinishedTime);
        out.writeLong(downloadFinishedTime);

        out.writeInt(restoreReplicationNum);

        out.writeInt(restoredPartitions.size());
        for (Pair<String, Partition> entry : restoredPartitions) {
            Text.writeString(out, entry.first);
            entry.second.write(out);
        }

        out.writeInt(restoredTbls.size());
        for (Table tbl : restoredTbls) {
            tbl.write(out);
        }

        out.writeInt(restoredVersionInfo.rowKeySet().size());
        for (long tblId : restoredVersionInfo.rowKeySet()) {
            out.writeLong(tblId);
            out.writeInt(restoredVersionInfo.row(tblId).size());
            for (Map.Entry<Long, Long> entry : restoredVersionInfo.row(tblId).entrySet()) {
                out.writeLong(entry.getKey());
                out.writeLong(entry.getValue());
                out.writeLong(0); // write a version_hash for compatibility
            }
        }

        out.writeInt(snapshotInfos.rowKeySet().size());
        for (long tabletId : snapshotInfos.rowKeySet()) {
            out.writeLong(tabletId);
            Map<Long, SnapshotInfo> map = snapshotInfos.row(tabletId);
            out.writeInt(map.size());
            for (Map.Entry<Long, SnapshotInfo> entry : map.entrySet()) {
                out.writeLong(entry.getKey());
                entry.getValue().write(out);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        backupTimestamp = Text.readString(in);
        jobInfo = BackupJobInfo.read(in);
        allowLoad = in.readBoolean();

        state = RestoreJobState.valueOf(Text.readString(in));

        if (in.readBoolean()) {
            backupMeta = BackupMeta.read(in);
        }

        fileMapping = RestoreFileMapping.read(in);

        metaPreparedTime = in.readLong();
        snapshotFinishedTime = in.readLong();
        downloadFinishedTime = in.readLong();

        restoreReplicationNum = in.readInt();

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tblName = Text.readString(in);
            Partition part = Partition.read(in);
            restoredPartitions.add(Pair.create(tblName, part));
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            restoredTbls.add(Table.read(in));
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tblId = in.readLong();
            int innerSize = in.readInt();
            for (int j = 0; j < innerSize; j++) {
                long partId = in.readLong();
                long version = in.readLong();
                in.readLong(); // read a version_hash for compatibility
                restoredVersionInfo.put(tblId, partId, version);
            }
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tabletId = in.readLong();
            int innerSize = in.readInt();
            for (int j = 0; j < innerSize; j++) {
                long beId = in.readLong();
                SnapshotInfo info = SnapshotInfo.read(in);
                snapshotInfos.put(tabletId, beId, info);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", backup ts: ").append(backupTimestamp);
        sb.append(", state: ").append(state.name());
        return sb.toString();
    }
}

