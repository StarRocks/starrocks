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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/backup/BackupHandler.java

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.AbstractJob.JobType;
import com.starrocks.backup.BackupJob.BackupJobState;
import com.starrocks.backup.BackupJobInfo.BackupTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AbstractBackupStmt;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.BackupStmt.BackupType;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.sql.ast.DropRepositoryStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.task.DirMoveTask;
import com.starrocks.task.DownloadTask;
import com.starrocks.task.SnapshotTask;
import com.starrocks.task.UploadTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class BackupHandler extends LeaderDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(BackupHandler.class);

    public static final int SIGNATURE_VERSION = 1;
    public static final Path BACKUP_ROOT_DIR = Paths.get(Config.tmp_dir, "backup").normalize();
    public static final Path RESTORE_ROOT_DIR = Paths.get(Config.tmp_dir, "restore").normalize();

    public static final Path TEST_BACKUP_ROOT_DIR = Paths.get(Config.tmp_dir, "test_backup").normalize();

    private RepositoryMgr repoMgr = new RepositoryMgr();

    // db id -> last running or finished backup/restore jobs
    // We only save the last backup/restore job of a database.
    // Newly submitted job will replace the current job, only if current job is finished or cancelled.
    // If the last job is finished, user can get the job info from repository. If the last job is cancelled,
    // user can get the error message before submitting the next one.
    // Use ConcurrentMap to get rid of locks.
    protected Map<Long, AbstractJob> dbIdToBackupOrRestoreJob = Maps.newConcurrentMap();

    // this lock is used for handling one backup or restore request at a time.
    private ReentrantLock seqlock = new ReentrantLock();

    private boolean isInit = false;

    private GlobalStateMgr globalStateMgr;

    public BackupHandler() {
        // for persist
    }

    public BackupHandler(GlobalStateMgr globalStateMgr) {
        super("backupHandler", 3000L);
        this.globalStateMgr = globalStateMgr;
    }

    public void setGlobalStateMgr(GlobalStateMgr globalStateMgr) {
        this.globalStateMgr = globalStateMgr;
    }

    @Override
    public synchronized void start() {
        Preconditions.checkNotNull(globalStateMgr);
        super.start();
        repoMgr.start();
    }

    public RepositoryMgr getRepoMgr() {
        return repoMgr;
    }

    private boolean init() {
        // Check and create backup dir if necessarily
        File backupDir = new File(BACKUP_ROOT_DIR.toString());
        if (!backupDir.exists()) {
            if (!backupDir.mkdirs()) {
                LOG.warn("failed to create backup dir: " + BACKUP_ROOT_DIR);
                return false;
            }
        } else {
            if (!backupDir.isDirectory()) {
                LOG.warn("backup dir is not a directory: " + BACKUP_ROOT_DIR);
                return false;
            }
        }

        // Check and create restore dir if necessarily
        File restoreDir = new File(RESTORE_ROOT_DIR.toString());
        if (!restoreDir.exists()) {
            if (!restoreDir.mkdirs()) {
                LOG.warn("failed to create restore dir: " + RESTORE_ROOT_DIR);
                return false;
            }
        } else {
            if (!restoreDir.isDirectory()) {
                LOG.warn("restore dir is not a directory: " + RESTORE_ROOT_DIR);
                return false;
            }
        }
        isInit = true;
        return true;
    }

    public AbstractJob getJob(long dbId) {
        return dbIdToBackupOrRestoreJob.get(dbId);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!isInit) {
            if (!init()) {
                return;
            }
        }

        for (AbstractJob job : dbIdToBackupOrRestoreJob.values()) {
            job.setGlobalStateMgr(globalStateMgr);
            job.run();
        }
    }

    // handle create repository stmt
    public void createRepository(CreateRepositoryStmt stmt) throws DdlException {
        if (stmt.hasBroker()) {
            if (!globalStateMgr.getBrokerMgr().containsBroker(stmt.getBrokerName())) {
                ErrorReport
                        .reportDdlException(ErrorCode.ERR_COMMON_ERROR, "broker does not exist: " + stmt.getBrokerName());
            }
        }

        BlobStorage storage = new BlobStorage(stmt.getBrokerName(), stmt.getProperties(), stmt.hasBroker());
        long repoId = globalStateMgr.getNextId();
        Repository repo = new Repository(repoId, stmt.getName(), stmt.isReadOnly(), stmt.getLocation(), storage);

        Status st = repoMgr.addAndInitRepoIfNotExist(repo, false);
        if (!st.ok()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Failed to create repository: " + st.getErrMsg());
        }
    }

    // handle drop repository stmt
    public void dropRepository(DropRepositoryStmt stmt) throws DdlException {
        tryLock();
        try {
            Repository repo = repoMgr.getRepo(stmt.getRepoName());
            if (repo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository does not exist");
            }

            for (AbstractJob job : dbIdToBackupOrRestoreJob.values()) {
                if (!job.isDone() && job.getRepoId() == repo.getId()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Backup or restore job is running on this repository."
                                    + " Can not drop it");
                }
            }

            Status st = repoMgr.removeRepo(repo.getName(), false /* not replay */);
            if (!st.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Failed to drop repository: " + st.getErrMsg());
            }
        } finally {
            seqlock.unlock();
        }
    }

    // the entry method of submitting a backup or restore job
    public void process(AbstractBackupStmt stmt) throws DdlException {
        // check if repo exist
        String repoName = stmt.getRepoName();
        Repository repository = repoMgr.getRepo(repoName);
        if (repository == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository " + repoName + " does not exist");
        }

        // check if db exist
        String dbName = stmt.getDbName();
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // Try to get sequence lock.
        // We expect at most one operation on a repo at same time.
        // But this operation may take a few seconds with lock held.
        // So we use tryLock() to give up this operation if we can not get lock.
        tryLock();
        try {
            // Check if there is backup or restore job running on this database
            AbstractJob currentJob = dbIdToBackupOrRestoreJob.get(db.getId());
            if (currentJob != null && !currentJob.isDone()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Can only run one backup or restore job of a database at same time");
            }

            if (stmt instanceof BackupStmt) {
                backup(repository, db, (BackupStmt) stmt);
            } else if (stmt instanceof RestoreStmt) {
                restore(repository, db, (RestoreStmt) stmt);
            }
        } finally {
            seqlock.unlock();
        }
    }

    private void tryLock() throws DdlException {
        try {
            if (!seqlock.tryLock(10, TimeUnit.SECONDS)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Another backup or restore job"
                        + " is being submitted. Please wait and try again");
            }
        } catch (InterruptedException e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Got interrupted exception when "
                    + "try locking. Try again");
        }
    }

    private void backup(Repository repository, Database db, BackupStmt stmt) throws DdlException {
        if (repository.isReadOnly()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository " + repository.getName()
                    + " is read only");
        }

        // Check if backup objects are valid
        // This is just a pre-check to avoid most of the invalid backup requests.
        // Also calculate the signature for incremental backup check.
        List<TableRef> tblRefs = stmt.getTableRefs();
        BackupMeta curBackupMeta = null;
        db.readLock();
        try {
            List<Table> backupTbls = Lists.newArrayList();
            for (TableRef tblRef : tblRefs) {
                String tblName = tblRef.getName().getTbl();
                Table tbl = db.getTable(tblName);
                if (tbl == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName);
                    return;
                }
                if (!tbl.isOlapTable()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_NOT_OLAP_TABLE, tblName);
                }

                OlapTable olapTbl = (OlapTable) tbl;
                if (olapTbl.existTempPartitions()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support backing up table with temp partitions");
                }

                PartitionNames partitionNames = tblRef.getPartitionNames();
                if (partitionNames != null) {
                    if (partitionNames.isTemp()) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                "Do not support backing up temp partitions");
                    }

                    for (String partName : partitionNames.getPartitionNames()) {
                        Partition partition = olapTbl.getPartition(partName);
                        if (partition == null) {
                            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                    "Unknown partition " + partName + " in table" + tblName);
                        }
                    }
                }

                // copy a table with selected partitions for calculating the signature
                List<String> reservedPartitions = partitionNames == null ? null : partitionNames.getPartitionNames();
                OlapTable copiedTbl = olapTbl.selectiveCopy(reservedPartitions, true, IndexExtState.VISIBLE);
                if (copiedTbl == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Failed to copy table " + tblName + " with selected partitions");
                }
                backupTbls.add(copiedTbl);
            }
            curBackupMeta = new BackupMeta(backupTbls);
        } finally {
            db.readUnlock();
        }

        // Check if label already be used
        List<String> existSnapshotNames = Lists.newArrayList();
        Status st = repository.listSnapshots(existSnapshotNames);
        if (!st.ok()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, st.getErrMsg());
        }
        if (existSnapshotNames.contains(stmt.getLabel())) {
            if (stmt.getType() == BackupType.FULL) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Snapshot with name '"
                        + stmt.getLabel() + "' already exist in repository");
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Currently does not support "
                        + "incremental backup");

                // TODO:
                // This is a incremental backup, the existing snapshot in repository will be treated
                // as base snapshot.
                // But first we need to check if the existing snapshot has same meta.
                List<BackupMeta> backupMetas = Lists.newArrayList();
                st = repository.getSnapshotMetaFile(stmt.getLabel(), backupMetas, -1, -1);
                if (!st.ok()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Failed to get existing meta info for repository: "
                                    + st.getErrMsg());
                }
                Preconditions.checkState(backupMetas.size() == 1);

                if (!curBackupMeta.compatibleWith(backupMetas.get(0))) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Can not make incremental backup. Meta does not compatible");
                }
            }
        }

        // Create a backup job
        BackupJob backupJob = new BackupJob(stmt.getLabel(), db.getId(), db.getOriginName(), tblRefs,
                stmt.getTimeoutMs(), globalStateMgr, repository.getId());
        // write log
        globalStateMgr.getEditLog().logBackupJob(backupJob);

        // must put to dbIdToBackupOrRestoreJob after edit log, otherwise the state of job may be changed.
        dbIdToBackupOrRestoreJob.put(db.getId(), backupJob);

        LOG.info("finished to submit backup job: {}", backupJob);
    }

    private void restore(Repository repository, Database db, RestoreStmt stmt) throws DdlException {
        // Check if snapshot exist in repository
        List<BackupJobInfo> infos = Lists.newArrayList();
        Status status = repository.getSnapshotInfoFile(stmt.getLabel(), stmt.getBackupTimestamp(), infos);
        if (!status.ok()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Failed to get info of snapshot '" + stmt.getLabel() + "' because: "
                            + status.getErrMsg() + ". Maybe specified wrong backup timestamp");
        }

        // Check if all restore objects are exist in this snapshot.
        // Also remove all unrelated objs
        Preconditions.checkState(infos.size() == 1);
        BackupJobInfo jobInfo = infos.get(0);
        // If TableRefs is empty, it means that we do not specify any table in Restore stmt.
        // So, we should restore all table in current database.
        if (stmt.getTableRefs().size() != 0) {
            checkAndFilterRestoreObjsExistInSnapshot(jobInfo, stmt.getTableRefs());
        }

        BackupMeta backupMeta = downloadAndDeserializeMetaInfo(jobInfo, repository, stmt);

        // Create a restore job
        RestoreJob restoreJob = null;
        if (backupMeta != null) {
            for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
                Table remoteTbl = backupMeta.getTable(tblInfo.name);
                if (remoteTbl.isLakeTable()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_NOT_OLAP_TABLE, remoteTbl.getName());
                }
            }
        }
        restoreJob = new RestoreJob(stmt.getLabel(), stmt.getBackupTimestamp(),
                db.getId(), db.getOriginName(), jobInfo, stmt.allowLoad(), stmt.getReplicationNum(),
                stmt.getTimeoutMs(), globalStateMgr, repository.getId(), backupMeta);
        globalStateMgr.getEditLog().logRestoreJob(restoreJob);

        // must put to dbIdToBackupOrRestoreJob after edit log, otherwise the state of job may be changed.
        dbIdToBackupOrRestoreJob.put(db.getId(), restoreJob);

        LOG.info("finished to submit restore job: {}", restoreJob);
    }

    private BackupMeta downloadAndDeserializeMetaInfo(BackupJobInfo jobInfo, Repository repo, RestoreStmt stmt) {
        // the meta version is used when reading backup meta from file.
        // we do not persist this field, because this is just a temporary solution.
        // the true meta version should be getting from backup job info, which is saved when doing backup job.
        // But the earlier version of StarRocks do not save the meta version in backup job info, so we allow user to
        // set this 'metaVersion' in restore stmt.
        // NOTICE: because we do not persist it, this info may be lost if Frontend restart,
        // and if you don't want to lose it, backup your data again by using latest StarRocks version.
        List<BackupMeta> backupMetas = Lists.newArrayList();
        Status st = repo.getSnapshotMetaFile(jobInfo.name, backupMetas,
                stmt.getMetaVersion() == -1 ? jobInfo.metaVersion : stmt.getMetaVersion(),
                stmt.getStarRocksMetaVersion() == -1 ? jobInfo.starrocksMetaVersion : stmt.getStarRocksMetaVersion());
        if (!st.ok()) {
            return null;
        }
        Preconditions.checkState(backupMetas.size() == 1);
        return backupMetas.get(0);
    }

    private void checkAndFilterRestoreObjsExistInSnapshot(BackupJobInfo jobInfo, List<TableRef> tblRefs)
            throws DdlException {
        Set<String> allTbls = Sets.newHashSet();
        for (TableRef tblRef : tblRefs) {
            String tblName = tblRef.getName().getTbl();
            if (!jobInfo.containsTbl(tblName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Table " + tblName + " does not exist in snapshot " + jobInfo.name);
            }
            BackupTableInfo tblInfo = jobInfo.getTableInfo(tblName);
            PartitionNames partitionNames = tblRef.getPartitionNames();
            if (partitionNames != null) {
                if (partitionNames.isTemp()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support restoring temporary partitions");
                }
                // check the selected partitions
                for (String partName : partitionNames.getPartitionNames()) {
                    if (!tblInfo.containsPart(partName)) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                "Partition " + partName + " of table " + tblName
                                        + " does not exist in snapshot " + jobInfo.name);
                    }
                }
            }

            // set alias
            if (tblRef.hasExplicitAlias()) {
                jobInfo.setAlias(tblName, tblRef.getExplicitAlias());
            }

            // only retain restore partitions
            tblInfo.retainPartitions(partitionNames == null ? null : partitionNames.getPartitionNames());
            allTbls.add(tblName);
        }

        // only retain restore tables
        jobInfo.retainTables(allTbls);
    }

    public AbstractJob getAbstractJobByDbName(String dbName) throws DdlException {
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        return dbIdToBackupOrRestoreJob.get(db.getId());
    }

    public void cancel(CancelBackupStmt stmt) throws DdlException {
        AbstractJob job = getAbstractJobByDbName(stmt.getDbName());
        if (job == null || (job instanceof BackupJob && stmt.isRestore())
                || (job instanceof RestoreJob && !stmt.isRestore())) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "No "
                    + (stmt.isRestore() ? "restore" : "backup" + " job")
                    + " is currently running");
        }

        Status status = job.cancel();
        if (!status.ok()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Failed to cancel job: " + status.getErrMsg());
        }

        LOG.info("finished to cancel {} job: {}", (stmt.isRestore() ? "restore" : "backup"), job);
    }

    public boolean handleFinishedSnapshotTask(SnapshotTask task, TFinishTaskRequest request) {
        AbstractJob job = dbIdToBackupOrRestoreJob.get(task.getDbId());
        if (job == null) {
            LOG.warn("failed to find backup or restore job for task: {}", task);
            // return true to remove this task from AgentTaskQueue
            return true;
        }
        if (job instanceof BackupJob) {
            if (task.isRestoreTask()) {
                LOG.warn("expect finding restore job, but get backup job {} for task: {}", job, task);
                // return true to remove this task from AgentTaskQueue
                return true;
            }

            return ((BackupJob) job).finishTabletSnapshotTask(task, request);
        } else {
            if (!task.isRestoreTask()) {
                LOG.warn("expect finding backup job, but get restore job {} for task: {}", job, task);
                // return true to remove this task from AgentTaskQueue
                return true;
            }
            return ((RestoreJob) job).finishTabletSnapshotTask(task, request);
        }
    }

    public boolean handleFinishedSnapshotUploadTask(UploadTask task, TFinishTaskRequest request) {
        AbstractJob job = dbIdToBackupOrRestoreJob.get(task.getDbId());
        if (job == null || (job instanceof RestoreJob)) {
            LOG.info("invalid upload task: {}, no backup job is found. db id: {}", task, task.getDbId());
            return false;
        }
        BackupJob restoreJob = (BackupJob) job;
        if (restoreJob.getJobId() != task.getJobId() || restoreJob.getState() != BackupJobState.UPLOADING) {
            LOG.info("invalid upload task: {}, job id: {}, job state: {}",
                    task, restoreJob.getJobId(), restoreJob.getState().name());
            return false;
        }
        return restoreJob.finishSnapshotUploadTask(task, request);
    }

    public boolean handleDownloadSnapshotTask(DownloadTask task, TFinishTaskRequest request) {
        AbstractJob job = dbIdToBackupOrRestoreJob.get(task.getDbId());
        if (job == null || !(job instanceof RestoreJob)) {
            LOG.warn("failed to find restore job for task: {}", task);
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        return ((RestoreJob) job).finishTabletDownloadTask(task, request);
    }

    public boolean handleDirMoveTask(DirMoveTask task, TFinishTaskRequest request) {
        AbstractJob job = dbIdToBackupOrRestoreJob.get(task.getDbId());
        if (job == null || !(job instanceof RestoreJob)) {
            LOG.warn("failed to find restore job for task: {}", task);
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        return ((RestoreJob) job).finishDirMoveTask(task, request);
    }

    public void replayAddJob(AbstractJob job) {
        if (job.isCancelled()) {
            AbstractJob existingJob = dbIdToBackupOrRestoreJob.get(job.getDbId());
            if (existingJob == null || existingJob.isDone()) {
                LOG.error("invalid existing job: {}. current replay job is: {}",
                        existingJob, job);
                return;
            }
            existingJob.setGlobalStateMgr(globalStateMgr);
            existingJob.replayCancel();
        } else if (!job.isPending()) {
            AbstractJob existingJob = dbIdToBackupOrRestoreJob.get(job.getDbId());
            if (existingJob == null || existingJob.isDone()) {
                LOG.error("invalid existing job: {}. current replay job is: {}",
                        existingJob, job);
                return;
            }
            // We use replayed job, not the existing job, to do the replayRun().
            // Because if we use the existing job to run again,
            // for example: In restore job, PENDING will transfer to SNAPSHOTING, not DOWNLOAD.
            job.replayRun();
        }
        if (isJobExpired(job, System.currentTimeMillis())) {
            LOG.warn("skip expired job {}", job);
            return;
        }
        dbIdToBackupOrRestoreJob.put(job.getDbId(), job);
    }

    public boolean report(TTaskType type, long jobId, long taskId, int finishedNum, int totalNum) {
        for (AbstractJob job : dbIdToBackupOrRestoreJob.values()) {
            if (job.getType() == JobType.BACKUP) {
                if (!job.isDone() && job.getJobId() == jobId && type == TTaskType.UPLOAD) {
                    job.taskProgress.put(taskId, Pair.create(finishedNum, totalNum));
                    return true;
                }
            } else if (job.getType() == JobType.RESTORE) {
                if (!job.isDone() && job.getJobId() == jobId && type == TTaskType.DOWNLOAD) {
                    job.taskProgress.put(taskId, Pair.create(finishedNum, totalNum));
                    return true;
                }
            }
        }
        return false;
    }

    public static BackupHandler read(DataInput in) throws IOException {
        BackupHandler backupHandler = new BackupHandler();
        backupHandler.readFields(in);
        return backupHandler;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        repoMgr.write(out);

        out.writeInt(dbIdToBackupOrRestoreJob.size());
        for (AbstractJob job : dbIdToBackupOrRestoreJob.values()) {
            job.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        repoMgr = RepositoryMgr.read(in);

        long currentTimeMs = System.currentTimeMillis();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            AbstractJob job = AbstractJob.read(in);
            if (isJobExpired(job, currentTimeMs)) {
                LOG.warn("skip expired job {}", job);
                continue;
            }
            dbIdToBackupOrRestoreJob.put(job.getDbId(), job);
        }
        LOG.info("finished replay {} backup/store jobs from image", dbIdToBackupOrRestoreJob.size());
    }

    public long saveBackupHandler(DataOutputStream dos, long checksum) throws IOException {
        write(dos);
        return checksum;
    }

    public long loadBackupHandler(DataInputStream dis, long checksum, GlobalStateMgr globalStateMgr) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_42) {
            readFields(dis);
        }
        setGlobalStateMgr(globalStateMgr);
        LOG.info("finished replay backupHandler from image");
        return checksum;
    }

    /**
     * will remove finished/cancelled job periodically
     */
    private boolean isJobExpired(AbstractJob job, long currentTimeMs) {
        return (job.isDone() || job.isCancelled())
                && (currentTimeMs - job.getFinishedTime()) / 1000 > Config.history_job_keep_max_second;
    }

    public void removeOldJobs() throws DdlException {
        tryLock();
        try {
            long currentTimeMs = System.currentTimeMillis();
            Iterator<Map.Entry<Long, AbstractJob>> iterator = dbIdToBackupOrRestoreJob.entrySet().iterator();
            while (iterator.hasNext()) {
                AbstractJob job = iterator.next().getValue();
                if (isJobExpired(job, currentTimeMs)) {
                    LOG.warn("discard expired job {}", job);
                    iterator.remove();
                }
            }
        } finally {
            seqlock.unlock();
        }
    }
}


