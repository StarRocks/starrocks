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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.backup.AbstractJob.JobType;
import com.starrocks.backup.BackupJob.BackupJobState;
import com.starrocks.backup.BackupJobInfo.BackupTableInfo;
import com.starrocks.backup.SnapshotRetentionCache.SnapshotRetention;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionNames;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.TableRefPersist;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AbstractBackupStmt;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.BackupStmt.BackupType;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.CatalogRef;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.sql.ast.DropRepositoryStmt;
import com.starrocks.sql.ast.DropSnapshotStmt;
import com.starrocks.sql.ast.FunctionRef;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.task.DirMoveTask;
import com.starrocks.task.DownloadTask;
import com.starrocks.task.SnapshotTask;
import com.starrocks.task.UploadTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.starrocks.scheduler.MVActiveChecker.MV_BACKUP_INACTIVE_REASON;

public class BackupHandler extends LeaderDaemon implements Writable, MemoryTrackable {

    private static final Logger LOG = LogManager.getLogger(BackupHandler.class);

    private static final long FAKE_DB_ID = -1;

    public static final int SIGNATURE_VERSION = 1;
    public static final Path BACKUP_ROOT_DIR = Paths.get(Config.tmp_dir, "backup").normalize();
    public static final Path RESTORE_ROOT_DIR = Paths.get(Config.tmp_dir, "restore").normalize();

    public static final Path TEST_BACKUP_ROOT_DIR = Paths.get(Config.tmp_dir, "test_backup").normalize();

    @SerializedName("rm")
    private RepositoryMgr repoMgr = new RepositoryMgr();

    // Retention read out of the repositories, remembered per snapshot. Runtime only: every FE fills
    // its own, and losing it costs a re-read.
    private final SnapshotRetentionCache retentionCache = new SnapshotRetentionCache();

    // repo id + label -> how many times in a row automatic cleanup failed to delete that snapshot.
    // Runtime only, so a restart or a leader switch resumes the attempts.
    private final Map<String, Integer> cleanFailures = Maps.newConcurrentMap();

    // db id -> last running or finished backup/restore jobs
    // We only save the last backup/restore job of a database.
    // Newly submitted job will replace the current job, only if current job is finished or cancelled.
    // If the last job is finished, user can get the job info from repository. If the last job is cancelled,
    // user can get the error message before submitting the next one.
    // Use ConcurrentMap to get rid of locks.
    // Backup/Restore job for external catalog using -1 to identify the job in dbIdToBackupOrRestoreJob
    // which means that only one external catalog backup/restore job can be run in entire cluster
    protected Map<Long, AbstractJob> dbIdToBackupOrRestoreJob = Maps.newConcurrentMap();

    protected MvRestoreContext mvRestoreContext = new MvRestoreContext();

    // this lock is used for handling one backup or restore request at a time.
    private ReentrantLock seqlock = new ReentrantLock();

    private boolean isInit = false;

    private GlobalStateMgr globalStateMgr;

    public BackupHandler() {
        // for persist - LeaderDaemon requires a name; the worker thread is never started for
        // the deserialization-only instance (start() requires globalStateMgr to be non-null).
        super("backup-handler", 3000L);
    }

    public BackupHandler(GlobalStateMgr globalStateMgr) {
        super("backup-handler", 3000L);
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

    public SnapshotRetentionCache getRetentionCache() {
        return retentionCache;
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
    protected void runAfterLeaseValid() {
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

    @Override
    protected void onStopped() {
        // dbIdToBackupOrRestoreJob is persistent state (saved/loaded via image and replayed
        // through the editlog) - the next leader resumes those jobs from the same map. Only
        // drop the per-leader-session init flag so the new leader re-validates backup/restore
        // tmp directories before resuming jobs.
        isInit = false;
        // repoMgr was started by start(); stop its ping loop so it does not keep talking to
        // remote storage after demotion. Fire-and-forget: its worker self-cleans in onStopped() and
        // deregisters, and the re-activation gate covers it. The same instance is reused on
        // re-election; its persisted repo maps remain intact across the stop/start cycle.
        try {
            repoMgr.stopBestEffort();
        } catch (Throwable t) {
            LOG.warn("stop repoMgr failed", t);
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
        String location = stmt.getLocation();
        while (location.endsWith("/")) {
            location = location.substring(0, location.length() - 1);
        }
        Repository repo = new Repository(repoId, stmt.getName(), stmt.isReadOnly(), location, storage);

        Status st = repoMgr.addAndInitRepoIfNotExist(repo);
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

            Status st = repoMgr.removeRepo(repo.getName());
            if (!st.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Failed to drop repository: " + st.getErrMsg());
            }
        } finally {
            seqlock.unlock();
        }
    }

    // the entry method of submitting a backup or restore job
    public void process(ConnectContext context, AbstractBackupStmt stmt) throws DdlException {
        // check if repo exist
        String repoName = stmt.getRepoName();
        Repository repository = repoMgr.getRepo(repoName);
        Database db = null;
        BackupJobInfo jobInfo = null;
        if (repository == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository " + repoName + " does not exist");
        }

        if (stmt instanceof RestoreStmt) {
            // Check if snapshot exist in repository, if existed, get jobInfo for restore process
            List<BackupJobInfo> infos = Lists.newArrayList();
            Status status = repository.getSnapshotInfoFile(stmt.getLabel(), ((RestoreStmt) stmt).getBackupTimestamp(), infos);
            if (!status.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Failed to get info of snapshot '" + stmt.getLabel() + "' because: "
                                + status.getErrMsg() + ". Maybe specified wrong backup timestamp");
            }
            Preconditions.checkState(infos.size() == 1);
            jobInfo = infos.get(0);

            if (jobInfo.dbName == null || jobInfo.dbName.isEmpty()) {
                // if jobInfo.dbName == null, means that this snapshot only contains external catalog info
                if ((stmt.getOriginDbName() != null && !stmt.getOriginDbName().isEmpty()) ||
                        stmt.getDbName() != null && !stmt.getDbName().isEmpty()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Can not specify database for external catalog snapshot");
                }

                if (!stmt.containsExternalCatalog()) {
                    // set `ALL` flag for external catalog restore if no `CATALOG(s)` set in restore stmt
                    stmt.setAllExternalCatalog();
                }
            } else if (stmt.containsExternalCatalog()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "This is not a snapshot for external catalog restore, snapshot: " + stmt.getLabel());
            }
        }

        if (!stmt.containsExternalCatalog()) {
            // check if db exist
            String dbName = stmt.getDbName();
            if (dbName == null) {
                // if target dbName if null, use dbName in snapshot
                dbName = jobInfo.dbName;
            }

            db = globalStateMgr.getLocalMetastore().getDb(dbName);
            if (db == null) {
                if (stmt instanceof RestoreStmt) {
                    try {
                        globalStateMgr.getLocalMetastore().createDb(dbName, null);
                        db = globalStateMgr.getLocalMetastore().getDb(dbName);
                        if (db == null) {
                            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
                        }
                    } catch (Exception e) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                "Can not create database: " + dbName + " in restore process");
                    }
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
                }
            }
        }

        // Try to get sequence lock.
        // We expect at most one operation on a repo at same time.
        // But this operation may take a few seconds with lock held.
        // So we use tryLock() to give up this operation if we can not get lock.
        tryLock();
        try {
            // Check if there is backup or restore job running on this database
            AbstractJob currentJob = dbIdToBackupOrRestoreJob.get(stmt.containsExternalCatalog() ? FAKE_DB_ID : db.getId());
            if (currentJob != null && currentJob.getDbId() == FAKE_DB_ID && !currentJob.isDone()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Can only run one backup or restore job of external catalog");
            } else if (currentJob != null && !currentJob.isDone()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Can only run one backup or restore job of a database at same time");
            }

            if (stmt instanceof BackupStmt) {
                backup(context, repository, db, (BackupStmt) stmt);
            } else if (stmt instanceof RestoreStmt) {
                restore(repository, db, (RestoreStmt) stmt, jobInfo);
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

    private void backup(ConnectContext context, Repository repository, Database db, BackupStmt stmt) throws DdlException {
        if (repository.isReadOnly()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository " + repository.getName()
                    + " is read only");
        }

        // Check if backup objects are valid
        // This is just a pre-check to avoid most of the invalid backup requests.
        // Also calculate the signature for incremental backup check.
        List<TableRef> tblRefs = stmt.getTableRefs();
        BackupMeta curBackupMeta = null;
        Locker locker = new Locker();
        if (!stmt.containsExternalCatalog()) {
            locker.lockDatabase(db.getId(), LockType.READ);
        }
        try {
            List<Table> backupTbls = Lists.newArrayList();
            for (TableRef tblRef : tblRefs) {
                String tblName = tblRef.getTableName();
                Table tbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tblName);
                if (tbl == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName);
                    return;
                }
                if (!tbl.isSupportBackupRestore()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Table: " + tblName + " can not support backup restore, type: " +
                                    tbl.getType());
                }

                if (tbl.isOlapView()) {
                    backupTbls.add(tbl);
                    continue;
                }

                OlapTable olapTbl = (OlapTable) tbl;
                if (olapTbl.existTempPartitions()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support backing up table with temp partitions");
                }

                PartitionRef partitionRef = tblRef.getPartitionRef();
                if (partitionRef != null) {
                    if (partitionRef.isTemp()) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                "Do not support backing up temp partitions");
                    }

                    for (String partName : partitionRef.getPartitionNames()) {
                        Partition partition = olapTbl.getPartition(partName);
                        if (partition == null) {
                            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                    "Unknown partition " + partName + " in table" + tblName);
                        }
                    }
                }

                // copy a table with selected partitions for calculating the signature
                List<String> reservedPartitions = partitionRef == null ? null : partitionRef.getPartitionNames();
                OlapTable copiedTbl = olapTbl.selectiveCopy(reservedPartitions, true, IndexExtState.VISIBLE);
                if (copiedTbl == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Failed to copy table " + tblName + " with selected partitions");
                }
                if (copiedTbl.isMaterializedView()) {
                    MaterializedView copiedMv = (MaterializedView) copiedTbl;
                    copiedMv.setInactiveAndReason(String.format("Set the materialized view %s inactive because %s",
                            copiedMv.getName(), MV_BACKUP_INACTIVE_REASON));
                }
                backupTbls.add(copiedTbl);
            }
            curBackupMeta = new BackupMeta(backupTbls);
        } finally {
            if (!stmt.containsExternalCatalog()) {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
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
        long dbId = stmt.containsExternalCatalog() ? FAKE_DB_ID : db.getId();
        String dbName = stmt.containsExternalCatalog() ? "" : db.getOriginName();

        List<TableRefPersist> tableRefPersists = Lists.newArrayList();
        for (TableRef tableRef : tblRefs) {
            PartitionRef partitionRef = tableRef.getPartitionRef();
            PartitionNames partitionNames = null;
            if (partitionRef != null) {
                partitionNames = new PartitionNames(partitionRef.isTemp(), partitionRef.getPartitionNames());
            }

            tableRefPersists.add(new TableRefPersist(
                    new TableName(
                            tableRef.getCatalogName() == null ? context.getCurrentCatalog() : tableRef.getCatalogName(),
                            tableRef.getDbName() == null ? db.getFullName() : tableRef.getDbName(),
                            tableRef.getTableName()),
                    tableRef.getAlias(),
                    partitionNames));
        }

        BackupJob backupJob = new BackupJob(stmt.getLabel(), dbId, dbName, tableRefPersists,
                stmt.getTimeoutMs(), globalStateMgr, repository.getId());
        backupJob.setTtl(stmt.getTtl());
        List<Function> allFunctions = Lists.newArrayList();

        if (!stmt.containsExternalCatalog() && (!stmt.withOnClause() || stmt.allFunction())) {
            for (Map.Entry<String, List<Function>> entry : db.getNameToFunction().entrySet()) {
                List<Function> fns = entry.getValue();
                allFunctions.addAll(fns);
            }
        } else if (!stmt.containsExternalCatalog()) {
            for (FunctionRef fnRef : stmt.getFnRefs()) {
                String functionName = fnRef.getFunctionName();

                List<Function> functionList = db.getFunctionsByName(functionName);
                if (functionList.isEmpty()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Invalid backup function(s), function name: " + functionName);
                }

                allFunctions.addAll(functionList);
            }
        }
        backupJob.setBackupFunctions(allFunctions);
        List<Catalog> backupCatalogs = Lists.newArrayList();
        for (CatalogRef catalogRef : stmt.getExternalCatalogRefs()) {
            backupCatalogs.add(getCatalog(catalogRef.getCatalogName()));
        }
        backupJob.setBackupCatalogs(backupCatalogs);

        addBackupJob(backupJob);

        LOG.info("finished to submit backup job: {}", backupJob);
    }

    protected void addBackupJob(BackupJob backupJob) {
        // write log
        globalStateMgr.getEditLog().logBackupJob(backupJob, wal -> {
            // must put to dbIdToBackupOrRestoreJob after edit log, otherwise the state of job may be changed.
            dbIdToBackupOrRestoreJob.put(backupJob.dbId, backupJob);
        });
    }

    private Catalog getCatalog(String catalogName) throws DdlException {
        Preconditions.checkNotNull(globalStateMgr, "globalStateMgr is null");
        Catalog catalog = globalStateMgr.getCatalogMgr().getCatalogByName(catalogName);
        if (catalog == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "external catalog " + catalogName + " does not existed.");
        }
        return catalog;
    }

    private void restore(Repository repository, Database db, RestoreStmt stmt, BackupJobInfo jobInfo) throws DdlException {
        BackupMeta backupMeta = downloadAndDeserializeMetaInfo(jobInfo, repository, stmt);

        // check the original dbName existed in snapshot or not
        if (!stmt.getOriginDbName().isEmpty() && !stmt.getOriginDbName().equals(jobInfo.dbName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "target database: " + stmt.getOriginDbName() + " is not existed in snapshot");
        }

        // If restore statement contains `ON` clause, filter the specified backup objects which are needed through infomation
        // provide in stmt and BackupMeta.
        if (stmt.withOnClause()) {
            checkAndFilterRestoreObjsExistInSnapshot(jobInfo, stmt.getTableRefs(), stmt, backupMeta);
        }

        // For UDFs restore, restore all functions in BackupMeta if statement does not contains `ON` clause or contains `ON`
        // and `ALL` clause for functions. Otherwise, restore the functions specified after `ON` clause in restore statement.
        if (stmt.withOnClause() && backupMeta != null) {
            checkAndFilterRestoreFunctionsInBackupMeta(stmt, backupMeta);
        }

        if (stmt.containsExternalCatalog()) {
            checkAndFilterRestoreCatalogsInBackupMeta(stmt, backupMeta);
        }

        if (backupMeta != null) {
            for (BackupTableInfo tblInfo : jobInfo.tables.values()) {
                Table remoteTbl = backupMeta.getTable(tblInfo.name);
                if (!remoteTbl.isSupportBackupRestore()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Table: " + remoteTbl.getName() +
                                    " can not support backup restore, type: " + remoteTbl.getType());
                }
                mvRestoreContext.addIntoMvBaseTableBackupInfoIfNeeded(db.getOriginName(), remoteTbl, jobInfo, tblInfo);
            }
        }

        long dbId = stmt.containsExternalCatalog() ? FAKE_DB_ID : db.getId();
        String dbName = stmt.containsExternalCatalog() ? "" : db.getOriginName();

        int replicationNum = RunMode.defaultReplicationNum();
        String replicationNumProp = stmt.getProperties().get("replication_num");
        if (!Strings.isNullOrEmpty(replicationNumProp)) {
            replicationNum = Integer.parseInt(replicationNumProp);
        }

        // Create a restore job
        RestoreJob  restoreJob = new RestoreJob(stmt.getLabel(), stmt.getBackupTimestamp(),
                dbId, dbName, jobInfo, stmt.allowLoad(), replicationNum,
                stmt.getTimeoutMs(), globalStateMgr, repository.getId(), backupMeta, mvRestoreContext);
        addRestoreJob(restoreJob);

        LOG.info("finished to submit restore job: {}", restoreJob);
    }

    protected void addRestoreJob(RestoreJob restoreJob) {
        // write log
        globalStateMgr.getEditLog().logRestoreJob(restoreJob, wal -> {
            // must put to dbIdToBackupOrRestoreJob after edit log, otherwise the state of job may be changed.
            dbIdToBackupOrRestoreJob.put(restoreJob.dbId, restoreJob);
        });
    }

    protected BackupMeta downloadAndDeserializeMetaInfo(BackupJobInfo jobInfo, Repository repo, RestoreStmt stmt) {
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

    protected void checkAndFilterRestoreCatalogsInBackupMeta(RestoreStmt stmt, BackupMeta backupMeta) throws DdlException {
        List<Catalog> catalogsInBackupMeta = backupMeta.getCatalogs();
        List<Catalog> restoredCatalogs = Lists.newArrayList();
        for (CatalogRef catalogRef : stmt.getExternalCatalogRefs()) {
            Optional<Catalog> hitCatalog = catalogsInBackupMeta.stream().filter(x -> catalogRef.getCatalogName()
                    .equalsIgnoreCase(x.getName())).findFirst();
            if (!hitCatalog.isPresent()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Can not find restore catalog: " + catalogRef.getCatalogName());
            }

            if (catalogRef.getAlias() != null && !catalogRef.getAlias().isEmpty()) {
                if (catalogRef.getAlias().equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support set alias as default catalog for external catalog restore");
                }
                hitCatalog.get().setName(catalogRef.getAlias());
            }

            restoredCatalogs.add(hitCatalog.get());
        }

        if (!restoredCatalogs.isEmpty()) {
            backupMeta.setCatalogs(restoredCatalogs);
        }
    }

    protected void checkAndFilterRestoreFunctionsInBackupMeta(RestoreStmt stmt, BackupMeta backupMeta) throws DdlException {
        List<Function> functionsInBackupMeta = backupMeta.getFunctions();
        List<Function> restoredFunctions = Lists.newArrayList();
        Set<String> hitFnNames = Sets.newHashSet();
        for (FunctionRef fnRef : stmt.getFnRefs()) {

            String functionName = fnRef.getFunctionName();
            List<Function> hitFunc = functionsInBackupMeta.stream()
                    .filter(x -> functionName.equalsIgnoreCase(x.functionName()))
                    .collect(Collectors.toList());
            if (hitFunc.isEmpty()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Can not find restore function: " + fnRef.getFnName().toString());
            }

            if (fnRef.getAlias() != null && !fnRef.getAlias().isEmpty()) {
                hitFunc.stream().forEach(fn -> fn.setFunctionName(
                        new FunctionName(fn.getFunctionName().getDb(), fnRef.getAlias())));
            }
            hitFnNames.add(hitFunc.get(0).getFunctionName().getFunction());
            restoredFunctions.addAll(hitFunc);
        }

        if (stmt.allFunction()) {
            for (Function fn : functionsInBackupMeta) {
                if (hitFnNames.contains(fn.getFunctionName().getFunction())) {
                    continue;
                }
                restoredFunctions.add(fn);
            }
        }
        backupMeta.setFunctions(restoredFunctions);
    }

    private void checkAndFilterRestoreObjsExistInSnapshot(BackupJobInfo jobInfo, List<TableRef> tblRefs, RestoreStmt stmt,
                                                          BackupMeta backupMeta) throws DdlException {
        Set<String> allTbls = Sets.newHashSet();
        Set<String> originTblName = Sets.newHashSet();
        for (TableRef tblRef : tblRefs) {
            String tblName = tblRef.getTableName();
            originTblName.add(tblName);
            if (!jobInfo.containsTbl(tblName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Table " + tblName + " does not exist in snapshot " + jobInfo.name);
            }
            BackupTableInfo tblInfo = jobInfo.getTableInfo(tblName);
            PartitionRef partitionRef = tblRef.getPartitionRef();
            if (partitionRef != null) {
                if (partitionRef.isTemp()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support restoring temporary partitions");
                }
                // check the selected partitions
                for (String partName : partitionRef.getPartitionNames()) {
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
            tblInfo.retainPartitions(partitionRef == null ? null : partitionRef.getPartitionNames());
            allTbls.add(tblName);
        }

        if (backupMeta != null) {
            if (stmt.allTable()) {
                allTbls.addAll(backupMeta.getTables().values().stream()
                        .filter(x -> x.isOlapTable() && !originTblName.contains(x.getName()))
                        .map(x -> x.getName()).collect(Collectors.toSet()));
            }

            if (stmt.allMV()) {
                allTbls.addAll(backupMeta.getTables().values().stream()
                        .filter(x -> x.isOlapMaterializedView() && !originTblName.contains(x.getName()))
                        .map(x -> x.getName()).collect(Collectors.toSet()));
            }

            if (stmt.allView()) {
                allTbls.addAll(backupMeta.getTables().values().stream()
                        .filter(x -> x.isOlapView() && !originTblName.contains(x.getName()))
                        .map(x -> x.getName()).collect(Collectors.toSet()));
            }
        }

        // only retain restore tables
        jobInfo.retainTables(allTbls);
    }

    public AbstractJob getAbstractJob(boolean isExternalCatalog, String dbName) throws DdlException {
        return isExternalCatalog ? dbIdToBackupOrRestoreJob.get(FAKE_DB_ID) : getAbstractJobByDbName(dbName);
    }

    public AbstractJob getAbstractJobByDbName(String dbName) throws DdlException {
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        return dbIdToBackupOrRestoreJob.get(db.getId());
    }

    public void cancel(CancelBackupStmt stmt) throws DdlException {
        AbstractJob job = null;
        job = getAbstractJob(stmt.isExternalCatalog(), stmt.getDbName());
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
        dbIdToBackupOrRestoreJob.put(job.getDbId(), job);
        mvRestoreContext.addIntoMvBaseTableBackupInfo(job);
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

    public void saveBackupHandlerV2(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(
                SRMetaBlockID.BACKUP_MGR, 2 + dbIdToBackupOrRestoreJob.size());
        writer.writeJson(this);
        writer.writeInt(dbIdToBackupOrRestoreJob.size());
        for (AbstractJob job : dbIdToBackupOrRestoreJob.values()) {
            writer.writeJson(job);
        }
        writer.close();
    }

    public void loadBackupHandlerV2(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        BackupHandler data = reader.readJson(BackupHandler.class);
        this.repoMgr = data.repoMgr;

        long currentTimeMs = System.currentTimeMillis();
        reader.readCollection(AbstractJob.class, job -> {
            if (isJobExpired(job, currentTimeMs)) {
                LOG.warn("skip expired job {}", job);
                return;
            }
            dbIdToBackupOrRestoreJob.put(job.getDbId(), job);
            mvRestoreContext.addIntoMvBaseTableBackupInfo(job);
        });
    }

    // handle drop snapshot stmt
    public void dropSnapshot(DropSnapshotStmt stmt) throws DdlException {
        tryLock();
        try {
            Repository repo = repoMgr.getRepo(stmt.getRepoName());
            if (repo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository does not exist: " + stmt.getRepoName());
            }
            if (repo.isReadOnly()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository " + repo.getName() + " is read only");
            }

            String label = stmt.getSnapshotName();
            List<String> labels = Lists.newArrayList();
            Status st = repo.listSnapshots(labels);
            if (!st.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Failed to list snapshots in repository " + repo.getName() + ": " + st.getErrMsg());
            }
            // Listing the directory, rather than reading the job info file, is what decides whether
            // the snapshot is there: an interrupted backup leaves a directory with no job info file,
            // and that leftover is exactly what an operator needs to be able to drop.
            if (!labels.contains(label)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Snapshot does not exist in repository " + repo.getName() + ": " + label);
            }
            if (isBeingBackedUp(repo.getId(), label)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Can not drop snapshot " + label + ": a backup is writing it");
            }
            if (isBeingRestored(repo.getId(), label)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Can not drop snapshot " + label + ": a restore is reading it");
            }

            SnapshotRetention retention = retentionCache.get(repo, label);
            if (!stmt.isForceDrop()) {
                checkOwnedByThisCluster(repo, label, retention);
            }

            Status deleteStatus = deleteSnapshot(repo, label, retention, DeleteTrigger.MANUAL);
            if (!deleteStatus.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Failed to delete snapshot " + label + " in repository " + repo.getName() + ": "
                                + deleteStatus.getErrMsg());
            }
        } finally {
            seqlock.unlock();
        }
    }

    /**
     * Refuses a snapshot that this cluster did not write. Deleting one is a decision about data
     * someone else owns, which is what FORCE is for: it says the operator has established that the
     * data is theirs to remove, in a way this cluster cannot check for them.
     *
     * <p>A snapshot with no readable retention -- written before this feature existed, or with a job
     * info file that cannot be read -- is refused for the same reason: it cannot be shown to belong
     * here.
     */
    private void checkOwnedByThisCluster(Repository repo, String label, SnapshotRetention retention)
            throws DdlException {
        int clusterId = globalStateMgr.getNodeMgr().getClusterId();
        if (retention != null && retention.isOwnedBy(clusterId)) {
            return;
        }

        String whose = retention == null || retention.getClusterId() == null
                ? "does not record which cluster created it"
                : "was created by cluster " + retention.getClusterId();
        ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                "Snapshot " + label + " in repository " + repo.getName() + " " + whose
                        + ", and this cluster is " + clusterId
                        + ". Use DROP SNAPSHOT ... FORCE to drop it anyway");
    }

    /** What asked for a snapshot's files to be deleted. */
    public enum DeleteTrigger {
        // Automatic ttl cleanup: the only caller that comes back on its own, and therefore the only
        // one whose failures count toward backup_clean_retry_limit.
        AUTO,
        // DROP SNAPSHOT, whose failure is reported to the operator in their own session.
        MANUAL
    }

    /**
     * Deletes the snapshots whose ttl has elapsed. Run by the cleaner daemon on the leader.
     *
     * <p>What may be deleted is decided entirely from the repository: a snapshot goes only when its
     * job info file names this cluster and an expiration that has passed. Everything else -- a
     * snapshot from another cluster, one written before this feature existed, one still being backed
     * up, one whose job info file cannot be read -- falls outside that and is left alone.
     */
    public void cleanExpiredSnapshots() {
        int clusterId = globalStateMgr.getNodeMgr().getClusterId();
        for (Repository repo : repoMgr.getAllRepositories()) {
            if (repo.isReadOnly()) {
                continue;
            }

            List<String> labels = Lists.newArrayList();
            Status st = repo.listSnapshots(labels);
            if (!st.ok()) {
                LOG.warn("failed to list snapshots in repo {} for ttl cleanup: {}", repo.getName(), st.getErrMsg());
                continue;
            }

            for (String label : labels) {
                SnapshotRetention retention = retentionCache.get(repo, label);
                if (retention == null || !retention.isOwnedBy(clusterId)
                        || !retention.isExpired(System.currentTimeMillis())) {
                    continue;
                }
                if (isCleanRetryExhausted(repo.getId(), label, retention)) {
                    continue;
                }
                if (isBeingBackedUp(repo.getId(), label) || isBeingRestored(repo.getId(), label)) {
                    continue;
                }
                if (!cleanExpiredSnapshot(repo, label, retention)) {
                    // A backup or restore is being submitted; come back next round.
                    return;
                }
            }
        }
    }

    /**
     * Deletes one expired snapshot under the seqlock, and reports whether the round can go on. The
     * lock is taken and released per snapshot, so a large backlog never keeps backup and restore
     * requests waiting for long.
     */
    private boolean cleanExpiredSnapshot(Repository repo, String label, SnapshotRetention retention) {
        if (!tryLockQuietly()) {
            return false;
        }
        try {
            // The snapshot was judged expired before the lock was taken, and the label can have been
            // dropped and backed up again in between -- the reuse check passes once the directory is
            // gone -- so what sits under it now may be fresh data. Re-reading the backup timestamp
            // tells the two apart, and covers both variants: a backup still running has written no
            // job info file at all, and a finished one wrote a different timestamp.
            if (!retention.getBackupTimestamp().equals(repo.getSnapshotTimestamp(label))) {
                LOG.info("skip deleting snapshot {} in repo {}: it no longer holds the backup that expired, "
                        + "or could not be re-read", label, repo.getName());
                cleanFailures.remove(cleanFailureKey(repo.getId(), label, retention));
                return true;
            }
            if (isBeingBackedUp(repo.getId(), label)) {
                LOG.info("skip deleting snapshot {} in repo {}: a backup is writing it", label, repo.getName());
                return true;
            }
            if (isBeingRestored(repo.getId(), label)) {
                LOG.info("skip deleting snapshot {} in repo {}: a restore is reading it", label, repo.getName());
                return true;
            }

            Status deleteStatus = deleteSnapshot(repo, label, retention, DeleteTrigger.AUTO);
            if (!deleteStatus.ok()) {
                int failures = cleanFailures.merge(cleanFailureKey(repo.getId(), label, retention), 1, Integer::sum);
                LOG.warn("failed to delete expired snapshot {} in repo {} ({} consecutive failures): {}",
                        label, repo.getName(), failures, deleteStatus.getErrMsg());
                if (failures >= Config.backup_clean_retry_limit) {
                    LOG.error("giving up automatic cleanup of snapshot {} in repo {} after {} consecutive "
                                    + "failures. It is attempted again after a restart or a leader switch, or "
                                    + "once backup_clean_retry_limit is raised; DROP SNAPSHOT deletes it either way",
                            label, repo.getName(), failures);
                }
            }
            return true;
        } finally {
            seqlock.unlock();
        }
    }

    /**
     * Deletes one snapshot's files and counts the outcome. The delete is a recursive delete of the
     * snapshot directory, so a failure leaves the snapshot exactly as it was and a repeat is safe.
     */
    private Status deleteSnapshot(Repository repo, String label, SnapshotRetention retention, DeleteTrigger trigger) {
        Status st = repo.deleteSnapshot(label);
        if (!st.ok()) {
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_BACKUP_SNAPSHOT_CLEAN_FAILED.increase(1L);
            }
            return st;
        }

        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_BACKUP_SNAPSHOT_CLEAN_SUCCESS.increase(1L);
        }
        retentionCache.invalidate(repo.getId(), label);
        if (retention != null) {
            cleanFailures.remove(cleanFailureKey(repo.getId(), label, retention));
        }
        LOG.info("deleted snapshot {} in repo {}, trigger: {}, backup timestamp: {}, cluster id: {}",
                label, repo.getName(), trigger,
                retention == null ? null : retention.getBackupTimestamp(),
                retention == null ? null : retention.getClusterId());
        return Status.OK;
    }

    private boolean isCleanRetryExhausted(long repoId, String label, SnapshotRetention retention) {
        return cleanFailures.getOrDefault(cleanFailureKey(repoId, label, retention), 0)
                >= Config.backup_clean_retry_limit;
    }

    // Keyed by the backup timestamp as well, so a new backup under the same label starts from zero.
    private static String cleanFailureKey(long repoId, String label, SnapshotRetention retention) {
        return repoId + ":" + label + ":" + retention.getBackupTimestamp();
    }

    /** The background counterpart of {@link #tryLock()}: gives up rather than failing a statement. */
    private boolean tryLockQuietly() {
        try {
            return seqlock.tryLock(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private boolean isBeingBackedUp(long repoId, String label) {
        return dbIdToBackupOrRestoreJob.values().stream()
                .anyMatch(job -> job instanceof BackupJob && isUnfinishedJobFor(job, repoId, label));
    }

    private boolean isBeingRestored(long repoId, String label) {
        return dbIdToBackupOrRestoreJob.values().stream()
                .anyMatch(job -> job instanceof RestoreJob && isUnfinishedJobFor(job, repoId, label));
    }

    private static boolean isUnfinishedJobFor(AbstractJob job, long repoId, String label) {
        return !job.isDone() && !job.isCancelled() && job.getRepoId() == repoId && label.equals(job.getLabel());
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
                    // discard mv backup table info if needed.
                    mvRestoreContext.discardExpiredBackupTableInfo(job);

                    LOG.warn("discard expired job {}", job);
                    iterator.remove();
                }
            }
        } finally {
            seqlock.unlock();
        }
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("BackupOrRestoreJob", (long) dbIdToBackupOrRestoreJob.size());
    }

    @Override
    public long estimateSize() {
        return Estimator.estimate(repoMgr)
                + Estimator.estimate(dbIdToBackupOrRestoreJob, 5, 20);
    }

    public Map<Long, Long> getRunningBackupRestoreCount() {
        long count = dbIdToBackupOrRestoreJob.values().stream().filter(job -> !job.isDone()).count();
        Map<Long, Long> result = new HashMap<>();
        result.put(WarehouseManager.DEFAULT_WAREHOUSE_ID, count);
        return result;
    }
}
