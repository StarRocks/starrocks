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
package com.starrocks.meta;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.staros.proto.FilePathInfo;
import com.starrocks.alter.AlterJobExecutor;
import com.starrocks.alter.AlterMVJobExecutor;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SetVarHint;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.UserVariableHint;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.View;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.StorageInfo;
import com.starrocks.load.pipe.PipeManager;
import com.starrocks.mv.analyzer.MVPartitionExprResolver;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.server.AbstractTableFactory;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.TableFactoryProvider;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AdminSetPartitionVersionStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.task.TabletTaskExecutor;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StarRocksMeta implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(StarRocksMeta.class);

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();

        long id = 0L;
        if (!globalStateMgr.tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (localMetastore.getDb(dbName) != null) {
                throw new AlreadyExistsException("Database Already Exists");
            } else {
                id = globalStateMgr.getNextId();
                Database db = new Database(id, dbName);
                String volume = StorageVolumeMgr.DEFAULT;
                if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)) {
                    volume = properties.remove(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME);
                }
                if (!GlobalStateMgr.getCurrentState().getStorageVolumeMgr().bindDbToStorageVolume(volume, id)) {
                    throw new DdlException(String.format("Storage volume %s not exists", volume));
                }
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
                String storageVolumeId = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeIdOfDb(id);

                localMetastore.createDb(db, storageVolumeId);
            }
        } finally {
            globalStateMgr.unlock();
        }
        LOG.info("createDb dbName = " + dbName + ", id = " + id);
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();

        // 1. check if database exists
        Database db;
        if (!globalStateMgr.tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            db = localMetastore.getDb(dbName);
            if (db == null) {
                throw new MetaNotFoundException("Database not found");
            }
            if (!isForceDrop && !db.getTemporaryTables().isEmpty()) {
                throw new DdlException("The database [" + dbName + "] " +
                        "cannot be dropped because there are still some temporary tables in it. " +
                        "If you want to forcibly drop, please use \"DROP DATABASE <database> FORCE.\"");
            }
        } finally {
            globalStateMgr.unlock();
        }

        // 2. drop tables in db
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            if (!db.isExist()) {
                throw new MetaNotFoundException("Database '" + dbName + "' not found");
            }
            if (!isForceDrop && GlobalStateMgr.getCurrentState()
                    .getGlobalTransactionMgr().existCommittedTxns(db.getId(), null, null)) {
                throw new DdlException(
                        "There are still some transactions in the COMMITTED state waiting to be completed. " +
                                "The database [" + dbName +
                                "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                " please use \"DROP DATABASE <database> FORCE\".");
            }

            // 3. remove db from globalStateMgr
            // save table names for recycling
            Set<String> tableNames = new HashSet<>(db.getTableNamesViewWithLock());
            localMetastore.dropDb(db, isForceDrop);
            recycleBin.recycleDatabase(db, tableNames, isForceDrop);
            db.setExist(false);

            // 4. drop mv task
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            TGetTasksParams tasksParams = new TGetTasksParams();
            tasksParams.setDb(dbName);
            List<Long> dropTaskIdList = taskManager.filterTasks(tasksParams)
                    .stream().map(Task::getId).collect(Collectors.toList());
            taskManager.dropTasks(dropTaskIdList, false);

            // 5. Drop Pipes
            PipeManager pipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
            pipeManager.dropPipesOfDb(dbName, db.getId());

            LOG.info("finish drop database[{}], id: {}, is force : {}", dbName, db.getId(), isForceDrop);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void recoverDatabase(RecoverDbStmt recoverStmt) throws DdlException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();

        // check is new db with same name already exist
        if (getDb(recoverStmt.getDbName()) != null) {
            throw new DdlException("Database[" + recoverStmt.getDbName() + "] already exist.");
        }

        Database db = recycleBin.recoverDatabase(recoverStmt.getDbName());

        // add db to globalStateMgr
        if (!globalStateMgr.tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (localMetastore.getDb(db.getFullName()) != null) {
                throw new DdlException("Database[" + db.getOriginName() + "] already exist.");
                // it's ok that we do not put db back to CatalogRecycleBin
                // cause this db cannot recover anymore
            }

            List<MaterializedView> materializedViews = db.getMaterializedViews();
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            for (MaterializedView materializedView : materializedViews) {
                MaterializedView.RefreshType refreshType = materializedView.getRefreshScheme().getType();
                if (refreshType != MaterializedView.RefreshType.SYNC) {
                    Task task = TaskBuilder.buildMvTask(materializedView, db.getFullName());
                    TaskBuilder.updateTaskInfo(task, materializedView);
                    taskManager.createTask(task, false);
                }
            }

            localMetastore.recoverDatabase(db);
        } finally {
            globalStateMgr.unlock();
        }

        LOG.info("finish recover database, name: {}, id: {}", recoverStmt.getDbName(), db.getId());
    }

    public void alterDatabaseQuota(AlterDatabaseQuotaStmt stmt) throws DdlException {
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();

        String dbName = stmt.getDbName();
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        DatabaseInfo dbInfo = new DatabaseInfo(db.getFullName(), "", stmt.getQuota(), stmt.getQuotaType());
        localMetastore.alterDatabaseQuota(dbInfo);
    }

    public void renameDatabase(AlterDatabaseRenameStatement stmt) throws DdlException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        LocalMetastore localMetastore = globalStateMgr.getLocalMetastore();

        String fullDbName = stmt.getDbName();
        String newFullDbName = stmt.getNewDbName();

        if (fullDbName.equals(newFullDbName)) {
            throw new DdlException("Same database name");
        }


        if (!globalStateMgr.tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            // check if db exists
            Database db = localMetastore.getDb(fullDbName);
            if (db == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, fullDbName);
            }

            // check if name is already used
            if (localMetastore.getDb(newFullDbName) != null) {
                throw new DdlException("Database name[" + newFullDbName + "] is already used");
            }

            localMetastore.renameDatabase(fullDbName, newFullDbName);
        } finally {
            globalStateMgr.unlock();
        }

        LOG.info("rename database[{}] to [{}], id: {}", fullDbName, newFullDbName, 0);
    }

    @Override
    public List<String> listDbNames() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames();
    }

    @Override
    public Database getDb(String name) {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(name);
    }

    @Override
    public Database getDb(long dbId) {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
    }

    public Optional<Database> mayGetDb(String name) {
        return Optional.ofNullable(getDb(name));
    }

    public Optional<Database> mayGetDb(long dbId) {
        return Optional.ofNullable(getDb(dbId));
    }

    /**
     * Following is the step to create an olap table:
     * 1. create columns
     * 2. create partition info
     * 3. create distribution info
     * 4. set table id and base index id
     * 5. set bloom filter columns
     * 6. set and build TableProperty includes:
     * 6.1. dynamicProperty
     * 6.2. replicationNum
     * 6.3. inMemory
     * 7. set index meta
     * 8. check colocation properties
     * 9. create tablet in BE
     * 10. add this table to FE's meta
     * 11. add this table to ColocateGroup if necessary
     *
     * @return whether the table is created
     */
    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        // check if db exists
        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDbName());
        }

        boolean isTemporaryTable = (stmt instanceof CreateTemporaryTableStmt);
        // perform the existence check which is cheap before any further heavy operations.
        // NOTE: don't even check the quota if already exists.
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            String tableName = stmt.getTableName();
            if (!isTemporaryTable && getTable(db.getFullName(), tableName) != null) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
                LOG.info("create table[{}] which already exists", tableName);
                return false;
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        // only internal table should check quota and cluster capacity
        if (!stmt.isExternal()) {
            // check cluster capacity
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkClusterCapacity();
            // check db quota
            db.checkQuota();
        }

        AbstractTableFactory tableFactory = TableFactoryProvider.getFactory(stmt.getEngineName());
        if (tableFactory == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, stmt.getEngineName());
        }

        Table table = tableFactory.createTable(GlobalStateMgr.getCurrentState().getLocalMetastore(), db, stmt);
        String storageVolumeId = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getStorageVolumeIdOfTable(table.getId());

        try {
            onCreate(db, table, storageVolumeId, stmt.isSetIfNotExists());
        } catch (DdlException e) {
            if (table.isCloudNativeTable()) {
                GlobalStateMgr.getCurrentState().getStorageVolumeMgr().unbindTableToStorageVolume(table.getId());
            }
            throw e;
        }
        return true;
    }

    public void onCreate(Database db, Table table, String storageVolumeId, boolean isSetIfNotExists)
            throws DdlException {
        // check database exists again, because database can be dropped when creating table
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. " +
                    "Try again or increasing value of `catalog_try_lock_timeout_ms` configuration.");
        }

        try {
            /*
             * When creating table or mv, we need to create the tablets and prepare some of the
             * metadata first before putting this new table or mv in the database. So after the
             * first step, we need to acquire the global lock and double check whether the db still
             * exists because it maybe dropped by other concurrent client. And if we don't use the lock
             * protection and handle the concurrency properly, the replay of table/mv creation may fail
             * on restart or on follower.
             *
             * After acquire the db lock, we also need to acquire the db lock and write edit log. Since the
             * db lock maybe under high contention and IO is busy, current thread can hold the global lock
             * for quite a long time and make the other operation waiting for the global lock fail.
             *
             * So here after the confirmation of existence of modifying database, we release the global lock
             * When dropping database, we will set the `exist` field of db object to false. And in the following
             * creation process, we will double-check the `exist` field.
             */
            if (getDb(db.getId()) == null) {
                throw new DdlException("Database has been dropped when creating table/mv/view");
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        if (db.isSystemDatabase()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, table.getName(),
                    "cannot create table in system database");
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            if (!db.isExist()) {
                throw new DdlException("Database has been dropped when creating table/mv/view");
            }

            if (!db.registerTableUnlocked(table)) {
                if (!isSetIfNotExists) {
                    table.delete(db.getId(), false);
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, table.getName(),
                            "table already exists");
                } else {
                    LOG.info("Create table[{}] which already exists", table.getName());
                    return;
                }
            }

            // NOTE: The table has been added to the database, and the following procedure cannot throw exception.
            LOG.info("Successfully create table: {}-{}, in database: {}-{}",
                    table.getName(), table.getId(), db.getFullName(), db.getId());

            CreateTableInfo createTableInfo = new CreateTableInfo(db.getFullName(), table, storageVolumeId);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableInfo);
            table.onCreate(db);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        createTable(stmt.getCreateTableStmt());
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check database
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        if (db.isSystemDatabase()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "cannot drop table in system database: " + db.getOriginName());
        }
        db.dropTable(tableName, stmt.isSetIfExists(), stmt.isForceDrop());
    }

    @Override
    public void dropTemporaryTable(String dbName, long tableId, String tableName,
                                   boolean isSetIfExsists, boolean isForce) throws DdlException {
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        db.dropTemporaryTable(tableId, tableName, isSetIfExsists, isForce);
    }

    public void recoverTable(RecoverTableStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Table table = getTable(db.getFullName(), tableName);
            if (table != null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }

            if (!GlobalStateMgr.getCurrentState().getRecycleBin().recoverTable(db, tableName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            Table recoverTable = getTable(db.getFullName(), tableName);
            if (recoverTable instanceof OlapTable) {
                DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), (OlapTable) recoverTable);
            }

        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    /*
     * Truncate specified table or partitions.
     * The main idea is:
     *
     * 1. using the same schema to create new table(partitions)
     * 2. use the new created table(partitions) to replace the old ones.
     *
     * if no partition specified, it will truncate all partitions of this table, including all temp partitions,
     * otherwise, it will only truncate those specified partitions.
     *
     */
    @Override
    public void truncateTable(TruncateTableStmt truncateTableStmt, ConnectContext context) throws DdlException {
        TableRef tblRef = truncateTableStmt.getTblRef();
        TableName dbTbl = tblRef.getName();
        // check, and save some info which need to be checked again later
        Map<String, Partition> origPartitions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        OlapTable copiedTbl;
        Database db = getDb(dbTbl.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbTbl.getDb());
        }

        boolean truncateEntireTable = tblRef.getPartitionNames() == null;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table table = MetaUtils.getSessionAwareTable(context, db, dbTbl);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, dbTbl.getTbl());
            }

            if (!table.isOlapOrCloudNativeTable()) {
                throw new DdlException("Only support truncate OLAP table or LAKE table");
            }

            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                throw InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName());
            }

            if (!truncateEntireTable) {
                for (String partName : tblRef.getPartitionNames().getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition " + partName + " does not exist");
                    }

                    origPartitions.put(partName, partition);
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().recordDropPartition(partition.getId());
                }
            } else {
                for (Partition partition : olapTable.getPartitions()) {
                    origPartitions.put(partition.getName(), partition);
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().recordDropPartition(partition.getId());
                }
            }

            copiedTbl = getShadowCopyTable(olapTable);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        // 2. use the copied table to create partitions
        List<Partition> newPartitions = Lists.newArrayListWithCapacity(origPartitions.size());
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        try {
            for (Map.Entry<String, Partition> entry : origPartitions.entrySet()) {
                long oldPartitionId = entry.getValue().getId();
                long newPartitionId = GlobalStateMgr.getCurrentState().getNextId();
                String newPartitionName = entry.getKey();

                PartitionInfo partitionInfo = copiedTbl.getPartitionInfo();
                partitionInfo.setTabletType(newPartitionId, partitionInfo.getTabletType(oldPartitionId));
                partitionInfo.setIsInMemory(newPartitionId, partitionInfo.getIsInMemory(oldPartitionId));
                partitionInfo.setReplicationNum(newPartitionId, partitionInfo.getReplicationNum(oldPartitionId));
                partitionInfo.setDataProperty(newPartitionId, partitionInfo.getDataProperty(oldPartitionId));

                if (copiedTbl.isCloudNativeTable()) {
                    partitionInfo.setDataCacheInfo(newPartitionId,
                            partitionInfo.getDataCacheInfo(oldPartitionId));
                }

                copiedTbl.setDefaultDistributionInfo(entry.getValue().getDistributionInfo());

                Partition newPartition =
                        createPartition(db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet,
                                ConnectContext.get().getCurrentWarehouseId());
                newPartitions.add(newPartition);
            }
            buildPartitions(db, copiedTbl, newPartitions.stream().map(Partition::getSubPartitions)
                    .flatMap(p -> p.stream()).collect(Collectors.toList()), ConnectContext.get().getCurrentWarehouseId());
        } catch (DdlException e) {
            tabletIdSet.forEach(tabletId -> GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId));
            throw e;
        }
        Preconditions.checkState(origPartitions.size() == newPartitions.size());

        // all partitions are created successfully, try to replace the old partitions.
        // before replacing, we need to check again.
        // Things may be changed outside the database lock.
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTable(db.getId(), copiedTbl.getId());
            if (olapTable == null) {
                throw new DdlException("Table[" + copiedTbl.getName() + "] is dropped");
            }

            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                throw InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName());
            }

            // check partitions
            for (Map.Entry<String, Partition> entry : origPartitions.entrySet()) {
                Partition partition = olapTable.getPartition(entry.getValue().getId());
                if (partition == null || !partition.getName().equalsIgnoreCase(entry.getKey())) {
                    throw new DdlException("Partition [" + entry.getKey() + "] is changed during truncating table, " +
                            "please retry");
                }
            }

            // check if meta changed
            // rollup index may be added or dropped, and schema may be changed during creating partition operation.
            boolean metaChanged = false;
            if (olapTable.getIndexNameToId().size() != copiedTbl.getIndexNameToId().size()) {
                metaChanged = true;
            } else {
                // compare schemaHash
                Map<Long, Integer> copiedIndexIdToSchemaHash = copiedTbl.getIndexIdToSchemaHash();
                for (Map.Entry<Long, Integer> entry : olapTable.getIndexIdToSchemaHash().entrySet()) {
                    long indexId = entry.getKey();
                    if (!copiedIndexIdToSchemaHash.containsKey(indexId)) {
                        metaChanged = true;
                        break;
                    }
                    if (!copiedIndexIdToSchemaHash.get(indexId).equals(entry.getValue())) {
                        metaChanged = true;
                        break;
                    }
                }
            }

            if (olapTable.getDefaultDistributionInfo().getType() != copiedTbl.getDefaultDistributionInfo().getType()) {
                metaChanged = true;
            }

            if (metaChanged) {
                throw new DdlException("Table[" + copiedTbl.getName() + "]'s meta has been changed. try again.");
            }

            // replace
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .truncateTableInternal(olapTable, newPartitions, truncateEntireTable, false);

            try {
                GlobalStateMgr.getCurrentState().getColocateTableIndex()
                        .updateLakeTableColocationInfo(olapTable, true /* isJoin */, null /* expectGroupId */);
            } catch (DdlException e) {
                LOG.info("table {} update colocation info failed when truncate table, {}", olapTable.getId(), e.getMessage());
            }

            // write edit log
            TruncateTableInfo info = new TruncateTableInfo(db.getId(), olapTable.getId(), newPartitions,
                    truncateEntireTable);
            GlobalStateMgr.getCurrentState().getLocalMetastore().truncateTable(info);

            // refresh mv
            Set<MvId> relatedMvs = olapTable.getRelatedMaterializedViews();
            for (MvId mvId : relatedMvs) {
                MaterializedView materializedView = (MaterializedView) getTable(db.getId(), mvId.getId());
                if (materializedView == null) {
                    LOG.warn("Table related materialized view {} can not be found", mvId.getId());
                    continue;
                }
                if (materializedView.isLoadTriggeredRefresh()) {
                    refreshMaterializedView(db.getFullName(), getTable(db.getId(), mvId.getId()).getName(), false, null,
                            Constants.TaskRunPriority.NORMAL.value(), true, false);
                }
            }
        } catch (DdlException e) {
            tabletIdSet.forEach(tabletId -> GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId));
            throw e;
        } catch (MetaNotFoundException e) {
            LOG.warn("Table related materialized view can not be found", e);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        LOG.info("finished to truncate table {}, partitions: {}",
                tblRef.getName().toSql(), tblRef.getPartitionNames());
    }

    /*
     * used for handling AlterTableStmt (for client is the ALTER TABLE command).
     * including SchemaChangeHandler and RollupHandler
     */
    @Override
    public void alterTable(ConnectContext context, AlterTableStmt stmt) throws UserException {
        AlterJobExecutor alterJobExecutor = new AlterJobExecutor();
        alterJobExecutor.process(stmt, context);
    }

    @Override
    public void alterTableComment(Database db, Table table, AlterTableCommentClause clause) {
        ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(db.getId(), table.getId());
        log.setComment(clause.getNewComment());
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(log);
        table.setComment(clause.getNewComment());
    }

    @Override
    public void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "] is under " + olapTable.getState());
        }

        String oldTableName = olapTable.getName();
        String newTableName = tableRenameClause.getNewTableName();
        if (oldTableName.equals(newTableName)) {
            throw new DdlException("Same table name");
        }

        // check if name is already used
        if (getTable(db.getFullName(), newTableName) != null) {
            throw new DdlException("Table name[" + newTableName + "] is already used");
        }

        olapTable.checkAndSetName(newTableName, false);

        db.dropTable(oldTableName);
        db.registerTableUnlocked(olapTable);
        inactiveRelatedMaterializedView(db, olapTable,
                MaterializedViewExceptions.inactiveReasonForBaseTableRenamed(oldTableName));

        TableInfo tableInfo = TableInfo.createForTableRename(db.getId(), olapTable.getId(), newTableName);
        GlobalStateMgr.getCurrentState().getLocalMetastore().renameTable(tableInfo);
        LOG.info("rename table[{}] to {}, tableId: {}", oldTableName, newTableName, olapTable.getId());
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbName, tblName);
    }

    public Table getTable(Long dbId, Long tableId) {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
    }

    public Optional<Table> mayGetTable(long dbId, long tableId) {
        return mayGetDb(dbId).flatMap(db -> Optional.ofNullable(db.getTable(tableId)));
    }

    public Optional<Table> mayGetTable(String dbName, String tableName) {
        return mayGetDb(dbName).flatMap(db -> Optional.ofNullable(db.getTable(tableName)));
    }

    @Override
    public void createView(CreateViewStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTable();

        // check if db exists
        Database db = this.getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // check if table exists in db
        boolean existed = false;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (getTable(db.getFullName(), tableName) != null) {
                existed = true;
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create view[{}] which already exists", tableName);
                    return;
                } else if (stmt.isReplace()) {
                    LOG.info("view {} already exists, need to replace it", tableName);
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        if (existed) {
            // already existed, need to alter the view
            AlterViewStmt alterViewStmt = AlterViewStmt.fromReplaceStmt(stmt);
            new AlterJobExecutor().process(alterViewStmt, ConnectContext.get());
            LOG.info("replace view {} successfully", tableName);
        } else {
            List<Column> columns = stmt.getColumns();
            long tableId = GlobalStateMgr.getCurrentState().getNextId();
            View view = new View(tableId, tableName, columns);
            view.setComment(stmt.getComment());
            view.setInlineViewDefWithSqlMode(stmt.getInlineViewDef(),
                    ConnectContext.get().getSessionVariable().getSqlMode());
            // init here in case the stmt string from view.toSql() has some syntax error.
            try {
                view.init();
            } catch (UserException e) {
                throw new DdlException("failed to init view stmt", e);
            }

            onCreate(db, view, "", stmt.isSetIfNotExists());
            LOG.info("successfully create view[" + tableName + "-" + view.getId() + "]");
        }
    }

    /**
     * used for handling AlterViewStmt (the ALTER VIEW command).
     */
    @Override
    public void alterView(AlterViewStmt stmt) {
        new AlterJobExecutor().process(stmt, ConnectContext.get());
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {
        MaterializedViewHandler materializedViewHandler =
                GlobalStateMgr.getCurrentState().getAlterJobMgr().getMaterializedViewHandler();
        String tableName = stmt.getBaseIndexName();
        // check db
        String dbName = stmt.getDBName();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // check cluster capacity
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkClusterCapacity();
        // check db quota
        db.checkQuota();

        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
            throw new DdlException("create materialized failed. database:" + db.getFullName() + " not exist");
        }
        try {
            Table table = getTable(db.getFullName(), tableName);
            if (table == null) {
                throw new DdlException("create materialized failed. table:" + tableName + " not exist");
            }
            if (table.isCloudNativeTable()) {
                throw new DdlException("Creating synchronous materialized view(rollup) is not supported in " +
                        "shared data clusters.\nPlease use asynchronous materialized view instead.\n" +
                        "Refer to https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements" +
                        "/data-definition/CREATE%20MATERIALIZED%20VIEW#asynchronous-materialized-view for details.");
            }
            if (!table.isOlapTable()) {
                throw new DdlException("Do not support create synchronous materialized view(rollup) on " +
                        table.getType().name() + " table[" + tableName + "]");
            }
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS) {
                throw new DdlException(
                        "Do not support create materialized view on primary key table[" + tableName + "]");
            }
            if (GlobalStateMgr.getCurrentState().getInsertOverwriteJobMgr().hasRunningOverwriteJob(olapTable.getId())) {
                throw new DdlException("Table[" + olapTable.getName() + "] is doing insert overwrite job, " +
                        "please start to create materialized view after insert overwrite");
            }
            olapTable.checkStableAndNormal();

            materializedViewHandler.processCreateMaterializedView(stmt, db, olapTable);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    // TODO(murphy) refactor it into MVManager
    @Override
    public void createMaterializedView(CreateMaterializedViewStatement stmt)
            throws DdlException {
        // check mv exists,name must be different from view/mv/table which exists in metadata
        String mvName = stmt.getTableName().getTbl();
        String dbName = stmt.getTableName().getDb();
        LOG.debug("Begin create materialized view: {}", mvName);
        // check if db exists
        Database db = this.getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check if table exists in db
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (getTable(db.getFullName(), mvName) != null) {
                if (stmt.isIfNotExists()) {
                    LOG.info("Create materialized view [{}] which already exists", mvName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, mvName);
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        // create columns
        List<Column> baseSchema = stmt.getMvColumnItems();
        validateColumns(baseSchema);

        Map<String, String> properties = stmt.getProperties();
        if (properties == null) {
            properties = Maps.newHashMap();
        }

        // create partition info
        PartitionInfo partitionInfo = buildPartitionInfo(stmt);
        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo baseDistribution = distributionDesc.toDistributionInfo(baseSchema);
        // create refresh scheme
        MaterializedView.MvRefreshScheme mvRefreshScheme;
        RefreshSchemeClause refreshSchemeDesc = stmt.getRefreshSchemeDesc();
        if (refreshSchemeDesc.getType() == MaterializedView.RefreshType.ASYNC) {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            MaterializedView.AsyncRefreshContext asyncRefreshContext = mvRefreshScheme.getAsyncRefreshContext();
            asyncRefreshContext.setDefineStartTime(asyncRefreshSchemeDesc.isDefineStartTime());
            int randomizeStart = 0;
            if (properties.containsKey(PropertyAnalyzer.PROPERTY_MV_RANDOMIZE_START)) {
                try {
                    randomizeStart = Integer.parseInt(properties.get((PropertyAnalyzer.PROPERTY_MV_RANDOMIZE_START)));
                } catch (NumberFormatException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                            PropertyAnalyzer.PROPERTY_MV_RANDOMIZE_START + " only accept integer as parameter");
                }
                // remove this transient variable
                properties.remove(PropertyAnalyzer.PROPERTY_MV_RANDOMIZE_START);
            }

            long random = getRandomStart(asyncRefreshSchemeDesc.getIntervalLiteral(), randomizeStart);
            if (asyncRefreshSchemeDesc.isDefineStartTime() || randomizeStart == -1) {
                long definedStartTime = Utils.getLongFromDateTime(asyncRefreshSchemeDesc.getStartTime());
                // Add random set only if mv_random_start > 0 when user has already set the start time
                if (randomizeStart > 0) {
                    definedStartTime += random;
                }
                asyncRefreshContext.setStartTime(definedStartTime);
            } else if (asyncRefreshSchemeDesc.getIntervalLiteral() != null) {
                long currentTimeSecond = Utils.getLongFromDateTime(LocalDateTime.now());
                long randomizedStart = currentTimeSecond + random;
                asyncRefreshContext.setStartTime(randomizedStart);
            }
            if (asyncRefreshSchemeDesc.getIntervalLiteral() != null) {
                long intervalStep = ((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getValue();
                String refreshTimeUnit = asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription();
                asyncRefreshContext.setStep(intervalStep);
                asyncRefreshContext.setTimeUnit(refreshTimeUnit);

                // Check the interval time should not be less than the min allowed config time.
                if (Config.materialized_view_min_refresh_interval > 0) {
                    TimeUnit intervalTimeUnit = TimeUtils.convertUnitIdentifierToTimeUnit(refreshTimeUnit);
                    long periodSeconds = TimeUtils.convertTimeUnitValueToSecond(intervalStep, intervalTimeUnit);
                    if (periodSeconds < Config.materialized_view_min_refresh_interval) {
                        throw new DdlException(String.format("Refresh schedule interval %s is too small which may cost " +
                                        "a lot of memory/cpu resources to refresh the asynchronous materialized view, " +
                                        "please config an interval larger than " +
                                        "Config.materialized_view_min_refresh_interval(%ss).",
                                periodSeconds,
                                Config.materialized_view_min_refresh_interval));
                    }
                }
            }

            // task which type is EVENT_TRIGGERED can not use external table as base table now.
            if (asyncRefreshContext.getTimeUnit() == null) {
                // asyncRefreshContext's timeUnit is null means this task's type is EVENT_TRIGGERED
                Map<TableName, Table> tableNameTableMap = AnalyzerUtils.collectAllTable(stmt.getQueryStatement());
                if (tableNameTableMap.values().stream().anyMatch(table -> !table.isNativeTableOrMaterializedView())) {
                    throw new DdlException(
                            "Materialized view which type is ASYNC need to specify refresh interval for " +
                                    "external table");
                }
            }
        } else if (refreshSchemeDesc.getType() == MaterializedView.RefreshType.SYNC) {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            mvRefreshScheme.setType(MaterializedView.RefreshType.SYNC);
        } else if (refreshSchemeDesc.getType().equals(MaterializedView.RefreshType.MANUAL)) {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            mvRefreshScheme.setType(MaterializedView.RefreshType.MANUAL);
        } else {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            mvRefreshScheme.setType(MaterializedView.RefreshType.INCREMENTAL);
        }
        mvRefreshScheme.setMoment(refreshSchemeDesc.getMoment());
        // create mv
        long mvId = GlobalStateMgr.getCurrentState().getNextId();
        MaterializedView materializedView;
        if (RunMode.isSharedNothingMode()) {
            if (refreshSchemeDesc.getType().equals(MaterializedView.RefreshType.INCREMENTAL)) {
                materializedView = GlobalStateMgr.getCurrentState().getMaterializedViewMgr()
                        .createSinkTable(stmt, partitionInfo, mvId, db.getId());
                materializedView.setMaintenancePlan(stmt.getMaintenancePlan());
            } else {
                materializedView =
                        new MaterializedView(mvId, db.getId(), mvName, baseSchema, stmt.getKeysType(), partitionInfo,
                                baseDistribution, mvRefreshScheme);
            }
        } else {
            Preconditions.checkState(RunMode.isSharedDataMode());
            if (refreshSchemeDesc.getType().equals(MaterializedView.RefreshType.INCREMENTAL)) {
                throw new DdlException("Incremental materialized view in shared_data mode is not supported");
            }

            materializedView =
                    new LakeMaterializedView(mvId, db.getId(), mvName, baseSchema, stmt.getKeysType(), partitionInfo,
                            baseDistribution, mvRefreshScheme);
        }

        //bitmap indexes
        List<Index> mvIndexes = stmt.getMvIndexes();
        materializedView.setIndexes(mvIndexes);

        // sort keys
        if (CollectionUtils.isNotEmpty(stmt.getSortKeys())) {
            materializedView.setTableProperty(new TableProperty());
            materializedView.getTableProperty().setMvSortKeys(stmt.getSortKeys());
        }
        // set comment
        materializedView.setComment(stmt.getComment());
        // set baseTableIds
        materializedView.setBaseTableInfos(stmt.getBaseTableInfos());
        // set viewDefineSql
        materializedView.setViewDefineSql(stmt.getInlineViewDef());
        materializedView.setSimpleDefineSql(stmt.getSimpleViewDef());
        materializedView.setOriginalViewDefineSql(stmt.getOriginalViewDefineSql());
        // set partitionRefTableExprs
        if (stmt.getPartitionRefTableExpr() != null) {
            //avoid to get a list of null inside
            materializedView.setPartitionRefTableExprs(Lists.newArrayList(stmt.getPartitionRefTableExpr()));
        }
        // set base index id
        long baseIndexId = GlobalStateMgr.getCurrentState().getNextId();
        materializedView.setBaseIndexId(baseIndexId);
        // set query output indexes
        materializedView.setQueryOutputIndices(stmt.getQueryOutputIndices());
        // set base index meta
        int schemaVersion = 0;
        int schemaHash = Util.schemaHash(schemaVersion, baseSchema, null, 0d);
        short shortKeyColumnCount = GlobalStateMgr.calcShortKeyColumnCount(baseSchema, null);
        TStorageType baseIndexStorageType = TStorageType.COLUMN;
        materializedView.setIndexMeta(baseIndexId, mvName, baseSchema, schemaVersion, schemaHash,
                shortKeyColumnCount, baseIndexStorageType, stmt.getKeysType());

        // validate hint
        Map<String, String> optHints = Maps.newHashMap();
        if (stmt.isExistQueryScopeHint()) {
            SessionVariable sessionVariable = VariableMgr.newSessionVariable();
            for (HintNode hintNode : stmt.getAllQueryScopeHints()) {
                if (hintNode instanceof SetVarHint) {
                    for (Map.Entry<String, String> entry : hintNode.getValue().entrySet()) {
                        VariableMgr.setSystemVariable(sessionVariable,
                                new SystemVariable(entry.getKey(), new StringLiteral(entry.getValue())), true);
                        optHints.put(entry.getKey(), entry.getValue());
                    }
                } else if (hintNode instanceof UserVariableHint) {
                    throw new DdlException("unsupported user variable hint in Materialized view for now.");
                }
            }
        }

        boolean isNonPartitioned = partitionInfo.isUnPartitioned();
        DataProperty dataProperty = PropertyAnalyzer.analyzeMVDataProperty(materializedView, properties);
        PropertyAnalyzer.analyzeMVProperties(db, materializedView, properties, isNonPartitioned);
        try {
            Set<Long> tabletIdSet = new HashSet<>();
            // process single partition info
            if (isNonPartitioned) {
                long partitionId = GlobalStateMgr.getCurrentState().getNextId();
                Preconditions.checkNotNull(dataProperty);
                partitionInfo.setDataProperty(partitionId, dataProperty);
                partitionInfo.setReplicationNum(partitionId, materializedView.getDefaultReplicationNum());
                partitionInfo.setIsInMemory(partitionId, false);
                partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
                StorageInfo storageInfo = materializedView.getTableProperty().getStorageInfo();
                partitionInfo.setDataCacheInfo(partitionId,
                        storageInfo == null ? null : storageInfo.getDataCacheInfo());
                Long version = Partition.PARTITION_INIT_VERSION;
                Partition partition = createPartition(db, materializedView, partitionId, mvName, version, tabletIdSet,
                        materializedView.getWarehouseId());
                buildPartitions(db, materializedView, new ArrayList<>(partition.getSubPartitions()),
                        materializedView.getWarehouseId());
                materializedView.addPartition(partition);
            } else {
                Expr partitionExpr = stmt.getPartitionExpDesc().getExpr();
                Map<Expr, SlotRef> partitionExprMaps = MVPartitionExprResolver.getMVPartitionExprsChecked(partitionExpr,
                        stmt.getQueryStatement(), stmt.getBaseTableInfos());
                LOG.info("Generate mv {} partition exprs: {}", mvName, partitionExprMaps);
                materializedView.setPartitionExprMaps(partitionExprMaps);
            }

            GlobalStateMgr.getCurrentState().getMaterializedViewMgr().prepareMaintenanceWork(stmt, materializedView);

            String storageVolumeId = "";
            if (materializedView.isCloudNativeMaterializedView()) {
                storageVolumeId = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                        .getStorageVolumeIdOfTable(materializedView.getId());
            }
            onCreate(db, materializedView, storageVolumeId, stmt.isIfNotExists());
        } catch (DdlException e) {
            if (materializedView.isCloudNativeMaterializedView()) {
                GlobalStateMgr.getCurrentState().getStorageVolumeMgr().unbindTableToStorageVolume(materializedView.getId());
            }
            throw e;
        }
        LOG.info("Successfully create materialized view [{}:{}]", mvName, materializedView.getMvId());

        // NOTE: The materialized view has been added to the database, and the following procedure cannot throw exception.
        createTaskForMaterializedView(dbName, materializedView, optHints);
        DynamicPartitionUtil.registerOrRemovePartitionTTLTable(db.getId(), materializedView);
    }

    private long getRandomStart(IntervalLiteral interval, long randomizeStart) throws DdlException {
        if (interval == null || randomizeStart == -1) {
            return 0;
        }
        // randomize the start time if not specified manually, to avoid refresh conflicts
        // default random interval is min(300s, INTERVAL/2)
        // user could specify it through mv_randomize_start
        long period = ((IntLiteral) interval.getValue()).getLongValue();
        TimeUnit timeUnit =
                TimeUtils.convertUnitIdentifierToTimeUnit(interval.getUnitIdentifier().getDescription());
        long intervalSeconds = TimeUtils.convertTimeUnitValueToSecond(period, timeUnit);
        long randomInterval = randomizeStart == 0 ? Math.min(300, intervalSeconds / 2) : randomizeStart;
        return randomInterval > 0 ? ThreadLocalRandom.current().nextLong(randomInterval) : randomInterval;
    }

    private void createTaskForMaterializedView(String dbName, MaterializedView materializedView,
                                               Map<String, String> optHints) throws DdlException {
        MaterializedView.RefreshType refreshType = materializedView.getRefreshScheme().getType();
        MaterializedView.RefreshMoment refreshMoment = materializedView.getRefreshScheme().getMoment();

        if (refreshType.equals(MaterializedView.RefreshType.INCREMENTAL)) {
            GlobalStateMgr.getCurrentState().getMaterializedViewMgr().startMaintainMV(materializedView);
            return;
        }

        if (refreshType != MaterializedView.RefreshType.SYNC) {

            Task task = TaskBuilder.buildMvTask(materializedView, dbName);
            TaskBuilder.updateTaskInfo(task, materializedView);

            if (optHints != null) {
                Map<String, String> taskProperties = task.getProperties();
                taskProperties.putAll(optHints);
            }

            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            taskManager.createTask(task, false);
            if (refreshMoment.equals(MaterializedView.RefreshMoment.IMMEDIATE)) {
                taskManager.executeTask(task.getName());
            }
        }
    }

    /**
     * Leave some clean up work to {@link MaterializedView#onDrop}
     */
    @Override
    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDbName());
        }
        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            table = getTable(db.getFullName(), stmt.getMvName());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        if (table instanceof MaterializedView) {
            try {
                Authorizer.checkMaterializedViewAction(ConnectContext.get().getCurrentUserIdentity(),
                        ConnectContext.get().getCurrentRoleIds(), stmt.getDbMvName(), PrivilegeType.DROP);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        stmt.getDbMvName().getCatalog(),
                        ConnectContext.get().getCurrentUserIdentity(),
                        ConnectContext.get().getCurrentRoleIds(), PrivilegeType.DROP.name(), ObjectType.MATERIALIZED_VIEW.name(),
                        stmt.getDbMvName().getTbl());
            }

            db.dropTable(table.getName(), stmt.isSetIfExists(), true);
        } else {
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processDropMaterializedView(stmt);
        }
    }

    @Override
    public String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement)
            throws DdlException, MetaNotFoundException {
        String dbName = refreshMaterializedViewStatement.getMvName().getDb();
        String mvName = refreshMaterializedViewStatement.getMvName().getTbl();
        boolean force = refreshMaterializedViewStatement.isForceRefresh();
        PartitionRangeDesc range = refreshMaterializedViewStatement.getPartitionRangeDesc();
        return refreshMaterializedView(dbName, mvName, force, range, Constants.TaskRunPriority.HIGH.value(),
                Config.enable_mv_refresh_sync_refresh_mergeable, true, refreshMaterializedViewStatement.isSync());
    }

    @Override
    public void cancelRefreshMaterializedView(
            CancelRefreshMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        String dbName = stmt.getMvName().getDb();
        String mvName = stmt.getMvName().getTbl();
        MaterializedView materializedView = getMaterializedViewToRefresh(dbName, mvName);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task refreshTask = taskManager.getTask(TaskBuilder.getMvTaskName(materializedView.getId()));
        boolean isForce = stmt.isForce();
        if (refreshTask != null) {
            taskManager.killTask(refreshTask.getName(), isForce);
        }
    }

    private String executeRefreshMvTask(String dbName, MaterializedView materializedView,
                                        ExecuteOption executeOption)
            throws DdlException {
        MaterializedView.RefreshType refreshType = materializedView.getRefreshScheme().getType();
        LOG.info("Start to execute refresh materialized view task, mv: {}, refreshType: {}, executionOption:{}",
                materializedView.getName(), refreshType, executeOption);

        if (refreshType.equals(MaterializedView.RefreshType.INCREMENTAL)) {
            GlobalStateMgr.getCurrentState().getMaterializedViewMgr().onTxnPublish(materializedView);
        } else if (refreshType != MaterializedView.RefreshType.SYNC) {
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            final String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
            if (!taskManager.containTask(mvTaskName)) {
                Task task = TaskBuilder.buildMvTask(materializedView, dbName);
                TaskBuilder.updateTaskInfo(task, materializedView);
                taskManager.createTask(task, false);
            }
            return taskManager.executeTask(mvTaskName, executeOption).getQueryId();
        }
        return null;
    }

    private MaterializedView getMaterializedViewToRefresh(String dbName, String mvName)
            throws DdlException, MetaNotFoundException {
        Database db = this.getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        final Table table = getTable(db.getFullName(), mvName);
        MaterializedView materializedView = null;
        if (table instanceof MaterializedView) {
            materializedView = (MaterializedView) table;
        }
        if (materializedView == null) {
            throw new MetaNotFoundException(mvName + " is not a materialized view");
        }
        return materializedView;
    }

    public String refreshMaterializedView(String dbName, String mvName, boolean force, PartitionRangeDesc range,
                                          int priority, boolean mergeRedundant, boolean isManual)
            throws DdlException, MetaNotFoundException {
        return refreshMaterializedView(dbName, mvName, force, range, priority, mergeRedundant, isManual, false);
    }

    public String refreshMaterializedView(String dbName, String mvName, boolean force, PartitionRangeDesc range,
                                          int priority, boolean mergeRedundant, boolean isManual, boolean isSync)
            throws DdlException, MetaNotFoundException {
        MaterializedView materializedView = getMaterializedViewToRefresh(dbName, mvName);

        HashMap<String, String> taskRunProperties = new HashMap<>();
        taskRunProperties.put(TaskRun.PARTITION_START, range == null ? null : range.getPartitionStart());
        taskRunProperties.put(TaskRun.PARTITION_END, range == null ? null : range.getPartitionEnd());
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(force));

        ExecuteOption executeOption = new ExecuteOption(priority, mergeRedundant, taskRunProperties);
        executeOption.setManual(isManual);
        executeOption.setSync(isSync);
        return executeRefreshMvTask(dbName, materializedView, executeOption);
    }

    @Override
    public void alterMaterializedView(AlterMaterializedViewStmt stmt) {
        new AlterMVJobExecutor().process(stmt, ConnectContext.get());
    }

    @Override
    public void addPartitions(ConnectContext ctx, Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException {
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table table = getTable(db.getFullName(), tableName);
            CatalogUtils.checkTableExist(db, tableName);
            CatalogUtils.checkNativeTable(db, table);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        addPartitions(ctx, db, tableName,
                addPartitionClause.getResolvedPartitionDescList(),
                addPartitionClause.isTempPartition(),
                addPartitionClause.getDistributionDesc());
    }

    private void addPartitions(ConnectContext ctx, Database db, String tableName, List<PartitionDesc> partitionDescs,
                               boolean isTempPartition, DistributionDesc distributionDesc) throws DdlException {
        DistributionInfo distributionInfo;
        OlapTable olapTable;
        OlapTable copiedTable;

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        Set<String> checkExistPartitionName = Sets.newConcurrentHashSet();
        try {
            olapTable = checkTable(db, tableName);

            // get partition info
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();

            // check partition type
            checkPartitionType(partitionInfo);

            // check partition num
            checkPartitionNum(olapTable);

            // get distributionInfo
            distributionInfo = getDistributionInfo(olapTable, distributionDesc).copy();
            olapTable.inferDistribution(distributionInfo);

            // check colocation
            checkColocation(db, olapTable, distributionInfo, partitionDescs);
            copiedTable = getShadowCopyTable(olapTable);
            copiedTable.setDefaultDistributionInfo(distributionInfo);
            checkExistPartitionName = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, partitionDescs);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        Preconditions.checkNotNull(distributionInfo);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkNotNull(copiedTable);

        // create partition outside db lock
        checkDataProperty(partitionDescs);

        Set<Long> tabletIdSetForAll = Sets.newHashSet();
        HashMap<String, Set<Long>> partitionNameToTabletSet = Maps.newHashMap();
        try {
            // create partition list
            List<Pair<Partition, PartitionDesc>> newPartitions =
                    createPartitionMap(db, copiedTable, partitionDescs, partitionNameToTabletSet, tabletIdSetForAll,
                            checkExistPartitionName, ctx.getCurrentWarehouseId());

            // build partitions
            List<Partition> partitionList = newPartitions.stream().map(x -> x.first).collect(Collectors.toList());
            buildPartitions(db, copiedTable, partitionList.stream().map(Partition::getSubPartitions)
                    .flatMap(p -> p.stream()).collect(Collectors.toList()), ctx.getCurrentWarehouseId());

            // check again
            if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
                throw new DdlException("db " + db.getFullName()
                        + "(" + db.getId() + ") has been dropped");
            }
            Set<String> existPartitionNameSet = Sets.newHashSet();
            try {
                olapTable = checkTable(db, tableName);
                existPartitionNameSet = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable,
                        partitionDescs);
                if (existPartitionNameSet.size() > 0) {
                    for (String partitionName : existPartitionNameSet) {
                        LOG.info("add partition[{}] which already exists", partitionName);
                    }
                }

                // check if meta changed
                checkIfMetaChange(olapTable, copiedTable, tableName);

                // get partition info
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();

                // check partition type
                checkPartitionType(partitionInfo);

                // update partition info
                updatePartitionInfo(partitionInfo, newPartitions, existPartitionNameSet, isTempPartition, olapTable);

                try {
                    GlobalStateMgr.getCurrentState().getColocateTableIndex()
                            .updateLakeTableColocationInfo(olapTable, true /* isJoin */, null /* expectGroupId */);
                } catch (DdlException e) {
                    LOG.info("table {} update colocation info failed when add partition, {}", olapTable.getId(), e.getMessage());
                }

                // add partition log
                GlobalStateMgr.getCurrentState().getLocalMetastore().
                        addPartitionLog(db, olapTable, partitionDescs, isTempPartition, partitionInfo, partitionList,
                                existPartitionNameSet);
            } finally {
                cleanExistPartitionNameSet(existPartitionNameSet, partitionNameToTabletSet);
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }
        } catch (DdlException e) {
            cleanTabletIdSetForAll(tabletIdSetForAll);
            throw e;
        }
    }

    private void cleanTabletIdSetForAll(Set<Long> tabletIdSetForAll) {
        // Cleanup of shards for LakeTable is taken care by ShardDeleter
        for (Long tabletId : tabletIdSetForAll) {
            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId);
        }
    }

    private void cleanExistPartitionNameSet(Set<String> existPartitionNameSet,
                                            HashMap<String, Set<Long>> partitionNameToTabletSet) {
        for (String partitionName : existPartitionNameSet) {
            Set<Long> existPartitionTabletSet = partitionNameToTabletSet.get(partitionName);
            if (existPartitionTabletSet == null) {
                // should not happen
                continue;
            }
            for (Long tabletId : existPartitionTabletSet) {
                // createPartitionWithIndices create duplicate tablet that if not exists scenario
                // so here need to clean up those created tablets which partition already exists from invert index
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId);
            }
        }
    }

    private void checkPartitionNum(OlapTable olapTable) throws DdlException {
        if (olapTable.getNumberOfPartitions() > Config.max_partition_number_per_table) {
            throw new DdlException("Table " + olapTable.getName() + " created partitions exceeded the maximum limit: " +
                    Config.max_partition_number_per_table + ". You can modify this restriction on by setting" +
                    " max_partition_number_per_table larger.");
        }
    }

    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        CatalogUtils.checkTableExist(db, table.getName());
        Locker locker = new Locker();
        OlapTable olapTable = (OlapTable) table;
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();

        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName());
        }
        if (!partitionInfo.isRangePartition() && partitionInfo.getType() != PartitionType.LIST) {
            throw new DdlException("Alter table [" + olapTable.getName() + "] failed. Not a partitioned table");
        }
        boolean isTempPartition = clause.isTempPartition();

        List<String> existPartitions = Lists.newArrayList();
        List<String> notExistPartitions = Lists.newArrayList();
        for (String partitionName : clause.getResolvedPartitionNames()) {
            if (olapTable.checkPartitionNameExist(partitionName, isTempPartition)) {
                existPartitions.add(partitionName);
            } else {
                notExistPartitions.add(partitionName);
            }
        }
        if (CollectionUtils.isNotEmpty(notExistPartitions)) {
            if (clause.isSetIfExists()) {
                LOG.info("drop partition[{}] which does not exist", notExistPartitions);
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DROP_PARTITION_NON_EXISTENT, notExistPartitions);
            }
        }
        if (CollectionUtils.isEmpty(existPartitions)) {
            return;
        }
        for (String partitionName : existPartitions) {
            // drop
            if (isTempPartition) {
                olapTable.dropTempPartition(partitionName, true);
            } else {
                Partition partition = olapTable.getPartition(partitionName);
                if (!clause.isForceDrop()) {
                    if (partition != null) {
                        if (GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                                .existCommittedTxns(db.getId(), olapTable.getId(), partition.getId())) {
                            throw new DdlException(
                                    "There are still some transactions in the COMMITTED state waiting to be completed." +
                                            " The partition [" + partitionName +
                                            "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                            " please use \"DROP PARTITION <partition> FORCE\".");
                        }
                    }
                }
                Range<PartitionKey> partitionRange = null;
                if (partition != null) {
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().recordDropPartition(partition.getId());
                    if (partitionInfo instanceof RangePartitionInfo) {
                        partitionRange = ((RangePartitionInfo) partitionInfo).getRange(partition.getId());
                    }
                }

                olapTable.dropPartition(db.getId(), partitionName, clause.isForceDrop());
                if (olapTable instanceof MaterializedView) {
                    MaterializedView mv = (MaterializedView) olapTable;
                    SyncPartitionUtils.dropBaseVersionMeta(mv, partitionName, partitionRange);
                }
            }
        }
        if (!isTempPartition) {
            try {
                for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
                    MaterializedView materializedView = (MaterializedView) getTable(db.getId(), mvId.getId());
                    if (materializedView != null && materializedView.isLoadTriggeredRefresh()) {
                        refreshMaterializedView(
                                db.getFullName(), materializedView.getName(), false, null,
                                Constants.TaskRunPriority.NORMAL.value(), true, false);
                    }
                }
            } catch (MetaNotFoundException e) {
                throw new DdlException("fail to refresh materialized views when dropping partition", e);
            }
        }
        long dbId = db.getId();
        long tableId = olapTable.getId();


        if (clause.getPartitionName() != null) {
            String partitionName = clause.getPartitionName();
            DropPartitionInfo info = new DropPartitionInfo(dbId, tableId, partitionName, isTempPartition, clause.isForceDrop());
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(info);
            LOG.info("succeed in dropping partition[{}], is temp : {}, is force : {}", partitionName, isTempPartition,
                    clause.isForceDrop());
        } else {
            DropPartitionsInfo info =
                    new DropPartitionsInfo(dbId, tableId, isTempPartition, clause.isForceDrop(), existPartitions);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartitions(info);
            LOG.info("succeed in dropping partitions[{}], is temp : {}, is force : {}", existPartitions, isTempPartition,
                    clause.isForceDrop());
        }
    }

    public void recoverPartition(RecoverPartitionStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Table table = getTable(db.getFullName(), tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (!table.isOlapOrCloudNativeTable()) {
                throw new DdlException("table[" + tableName + "] is not OLAP table or LAKE table");
            }
            OlapTable olapTable = (OlapTable) table;

            String partitionName = recoverStmt.getPartitionName();
            if (olapTable.getPartition(partitionName) != null) {
                throw new DdlException("partition[" + partitionName + "] already exist in table[" + tableName + "]");
            }

            GlobalStateMgr.getCurrentState().getRecycleBin().recoverPartition(db.getId(), olapTable, partitionName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    private OlapTable checkTable(Database db, String tableName) throws DdlException {
        CatalogUtils.checkTableExist(db, tableName);
        Table table = getTable(db.getFullName(), tableName);
        CatalogUtils.checkNativeTable(db, table);
        OlapTable olapTable = (OlapTable) table;
        CatalogUtils.checkTableState(olapTable, tableName);
        return olapTable;
    }

    private OlapTable checkTable(Database db, Long tableId) throws DdlException {
        Table table = getTable(db.getId(), tableId);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableId);
        }
        CatalogUtils.checkNativeTable(db, table);
        OlapTable olapTable = (OlapTable) table;
        CatalogUtils.checkTableState(olapTable, table.getName());
        return olapTable;
    }

    private void checkPartitionType(PartitionInfo partitionInfo) throws DdlException {
        PartitionType partitionType = partitionInfo.getType();
        if (!partitionInfo.isRangePartition() && partitionType != PartitionType.LIST) {
            throw new DdlException("Only support adding partition to range/list partitioned table");
        }
    }

    private DistributionInfo getDistributionInfo(OlapTable olapTable, DistributionDesc distributionDesc)
            throws DdlException {
        DistributionInfo distributionInfo;
        List<Column> baseSchema = olapTable.getBaseSchema();
        DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
        if (distributionDesc != null) {
            distributionInfo = distributionDesc.toDistributionInfo(baseSchema);
            // for now. we only support modify distribution's bucket num
            if (distributionInfo.getType() != defaultDistributionInfo.getType()) {
                throw new DdlException("Cannot assign different distribution type. default is: "
                        + defaultDistributionInfo.getType());
            }

            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                List<Column> newDistriCols = MetaUtils.getColumnsByColumnIds(olapTable,
                        hashDistributionInfo.getDistributionColumns());
                List<Column> defaultDistriCols = MetaUtils.getColumnsByColumnIds(olapTable,
                        defaultDistributionInfo.getDistributionColumns());
                if (!newDistriCols.equals(defaultDistriCols)) {
                    throw new DdlException("Cannot assign hash distribution with different distribution cols. "
                            + "default is: " + defaultDistriCols);
                }
                if (hashDistributionInfo.getBucketNum() < 0) {
                    throw new DdlException("Cannot assign hash distribution buckets less than 0");
                }
            }
            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.RANDOM) {
                RandomDistributionInfo randomDistributionInfo = (RandomDistributionInfo) distributionInfo;
                if (randomDistributionInfo.getBucketNum() < 0) {
                    throw new DdlException("Cannot assign random distribution buckets less than 0");
                }
            }
        } else {
            distributionInfo = defaultDistributionInfo;
        }
        return distributionInfo;
    }

    private void checkColocation(Database db, OlapTable olapTable, DistributionInfo distributionInfo,
                                 List<PartitionDesc> partitionDescs)
            throws DdlException {
        if (GlobalStateMgr.getCurrentState().getColocateTableIndex().isColocateTable(olapTable.getId())) {
            String fullGroupName = db.getId() + "_" + olapTable.getColocateGroup();
            ColocateGroupSchema groupSchema = GlobalStateMgr.getCurrentState()
                    .getColocateTableIndex().getGroupSchema(fullGroupName);
            Preconditions.checkNotNull(groupSchema);
            groupSchema.checkDistribution(olapTable.getIdToColumn(), distributionInfo);
            for (PartitionDesc partitionDesc : partitionDescs) {
                groupSchema.checkReplicationNum(partitionDesc.getReplicationNum());
            }
        }
    }

    private void checkDataProperty(List<PartitionDesc> partitionDescs) {
        for (PartitionDesc partitionDesc : partitionDescs) {
            DataProperty dataProperty = partitionDesc.getPartitionDataProperty();
            Preconditions.checkNotNull(dataProperty);
        }
    }

    private List<Pair<Partition, PartitionDesc>> createPartitionMap(Database db, OlapTable copiedTable,
                                                                    List<PartitionDesc> partitionDescs,
                                                                    HashMap<String, Set<Long>> partitionNameToTabletSet,
                                                                    Set<Long> tabletIdSetForAll,
                                                                    Set<String> existPartitionNameSet,
                                                                    long warehouseId)
            throws DdlException {
        List<Pair<Partition, PartitionDesc>> partitionList = Lists.newArrayList();
        for (PartitionDesc partitionDesc : partitionDescs) {
            long partitionId = GlobalStateMgr.getCurrentState().getNextId();
            DataProperty dataProperty = partitionDesc.getPartitionDataProperty();
            String partitionName = partitionDesc.getPartitionName();
            if (existPartitionNameSet.contains(partitionName)) {
                continue;
            }
            Long version = partitionDesc.getVersionInfo();
            Set<Long> tabletIdSet = Sets.newHashSet();

            copiedTable.getPartitionInfo().setDataProperty(partitionId, dataProperty);
            copiedTable.getPartitionInfo().setTabletType(partitionId, partitionDesc.getTabletType());
            copiedTable.getPartitionInfo().setReplicationNum(partitionId, partitionDesc.getReplicationNum());
            copiedTable.getPartitionInfo().setIsInMemory(partitionId, partitionDesc.isInMemory());
            copiedTable.getPartitionInfo().setDataCacheInfo(partitionId, partitionDesc.getDataCacheInfo());

            Partition partition =
                    createPartition(db, copiedTable, partitionId, partitionName, version, tabletIdSet, warehouseId);

            partitionList.add(Pair.create(partition, partitionDesc));
            tabletIdSetForAll.addAll(tabletIdSet);
            partitionNameToTabletSet.put(partitionName, tabletIdSet);
        }
        return partitionList;
    }

    private void checkIfMetaChange(OlapTable olapTable, OlapTable copiedTable, String tableName) throws DdlException {
        // rollup index may be added or dropped during add partition operation.
        // schema may be changed during add partition operation.
        boolean metaChanged = false;
        if (olapTable.getIndexNameToId().size() != copiedTable.getIndexNameToId().size()) {
            metaChanged = true;
        } else {
            // compare schemaHash
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                long indexId = entry.getKey();
                if (!copiedTable.getIndexIdToMeta().containsKey(indexId)) {
                    metaChanged = true;
                    break;
                }
                if (copiedTable.getIndexIdToMeta().get(indexId).getSchemaHash() !=
                        entry.getValue().getSchemaHash()) {
                    metaChanged = true;
                    break;
                }
            }
        }

        if (olapTable.getDefaultDistributionInfo().getType() !=
                copiedTable.getDefaultDistributionInfo().getType()) {
            metaChanged = true;
        }

        if (metaChanged) {
            throw new DdlException("Table[" + tableName + "]'s meta has been changed. try again.");
        }
    }

    private void updatePartitionInfo(PartitionInfo partitionInfo, List<Pair<Partition, PartitionDesc>> partitionList,
                                     Set<String> existPartitionNameSet, boolean isTempPartition,
                                     OlapTable olapTable)
            throws DdlException {
        if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            rangePartitionInfo.handleNewRangePartitionDescs(olapTable.getIdToColumn(),
                    partitionList, existPartitionNameSet, isTempPartition);
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            listPartitionInfo.handleNewListPartitionDescs(olapTable.getIdToColumn(),
                    partitionList, existPartitionNameSet, isTempPartition);
        } else {
            throw new DdlException("Only support adding partition to range/list partitioned table");
        }

        if (isTempPartition) {
            for (Pair<Partition, PartitionDesc> entry : partitionList) {
                Partition partition = entry.first;
                if (!existPartitionNameSet.contains(partition.getName())) {
                    olapTable.addTempPartition(partition);
                }
            }
        } else {
            for (Pair<Partition, PartitionDesc> entry : partitionList) {
                Partition partition = entry.first;
                if (!existPartitionNameSet.contains(partition.getName())) {
                    olapTable.addPartition(partition);
                }
            }
        }
    }

    @Override
    public void renamePartition(Database db, Table table, PartitionRenameClause renameClause) throws DdlException {
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "] is under " + olapTable.getState());
        }

        if (!olapTable.getPartitionInfo().isRangePartition()) {
            throw new DdlException("Table[" + olapTable.getName() + "] is single partitioned. "
                    + "no need to rename partition name.");
        }

        String partitionName = renameClause.getPartitionName();
        String newPartitionName = renameClause.getNewPartitionName();
        if (partitionName.equalsIgnoreCase(newPartitionName)) {
            throw new DdlException("Same partition name");
        }

        Partition partition = olapTable.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException("Partition[" + partitionName + "] does not exists");
        }

        // check if name is already used
        if (olapTable.checkPartitionNameExist(newPartitionName)) {
            throw new DdlException("Partition name[" + newPartitionName + "] is already used");
        }

        olapTable.renamePartition(partitionName, newPartitionName);

        // log
        TableInfo tableInfo = TableInfo.createForPartitionRename(db.getId(), olapTable.getId(), partition.getId(),
                newPartitionName);
        GlobalStateMgr.getCurrentState().getLocalMetastore().renamePartition(tableInfo);
        LOG.info("rename partition[{}] to {}", partitionName, newPartitionName);
    }

    /*
     * The entry of replacing partitions with temp partitions.
     */
    public void replaceTempPartition(Database db, String tableName, ReplacePartitionClause clause) throws DdlException {
        List<String> partitionNames = clause.getPartitionNames();
        // duplicate temp partition will cause Incomplete transaction
        List<String> tempPartitionNames =
                clause.getTempPartitionNames().stream().distinct().collect(Collectors.toList());

        boolean isStrictRange = clause.isStrictRange();
        boolean useTempPartitionName = clause.useTempPartitionName();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Table table = getTable(db.getFullName(), tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (!table.isOlapOrCloudNativeTable()) {
                throw new DdlException("Table[" + tableName + "] is not OLAP table or LAKE table");
            }

            OlapTable olapTable = (OlapTable) table;
            // check partition exist
            for (String partName : partitionNames) {
                if (!olapTable.checkPartitionNameExist(partName, false)) {
                    throw new DdlException("Partition[" + partName + "] does not exist");
                }
            }
            for (String partName : tempPartitionNames) {
                if (!olapTable.checkPartitionNameExist(partName, true)) {
                    throw new DdlException("Temp partition[" + partName + "] does not exist");
                }
            }

            partitionNames.stream().forEach(e ->
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().recordDropPartition(olapTable.getPartition(e).getId()));
            olapTable.replaceTempPartitions(partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);

            // write log
            ReplacePartitionOperationLog info = new ReplacePartitionOperationLog(db.getId(), olapTable.getId(),
                    partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);
            GlobalStateMgr.getCurrentState().getLocalMetastore().replaceTempPartition(info);
            LOG.info("finished to replace partitions {} with temp partitions {} from table: {}",
                    clause.getPartitionNames(), clause.getTempPartitionNames(), tableName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void setPartitionVersion(AdminSetPartitionVersionStmt stmt) {
        Database database = getDb(stmt.getTableName().getDb());
        if (database == null) {
            throw ErrorReportException.report(ErrorCode.ERR_BAD_DB_ERROR, stmt.getTableName().getDb());
        }
        Locker locker = new Locker();
        locker.lockDatabase(database.getId(), LockType.WRITE);
        try {
            Table table = getTable(database.getFullName(), stmt.getTableName().getTbl());
            if (table == null) {
                throw ErrorReportException.report(ErrorCode.ERR_BAD_TABLE_ERROR, stmt.getTableName().getTbl());
            }
            if (!table.isOlapTableOrMaterializedView()) {
                throw ErrorReportException.report(ErrorCode.ERR_NOT_OLAP_TABLE, stmt.getTableName().getTbl());
            }

            PhysicalPartition physicalPartition;
            OlapTable olapTable = (OlapTable) table;
            if (stmt.getPartitionId() != -1) {
                physicalPartition = olapTable.getPhysicalPartition(stmt.getPartitionId());
                if (physicalPartition == null) {
                    throw ErrorReportException.report(ErrorCode.ERR_NO_SUCH_PARTITION, stmt.getPartitionName());
                }
            } else {
                Partition partition = olapTable.getPartition(stmt.getPartitionName());
                if (partition == null) {
                    throw ErrorReportException.report(ErrorCode.ERR_NO_SUCH_PARTITION, stmt.getPartitionName());
                }

                physicalPartition = partition.getDefaultPhysicalPartition();
                if (partition.getSubPartitions().size() >= 2) {
                    throw ErrorReportException.report(ErrorCode.ERR_MULTI_SUB_PARTITION, stmt.getPartitionName());
                }
            }

            long visibleVersionTime = System.currentTimeMillis();
            physicalPartition.setVisibleVersion(stmt.getVersion(), visibleVersionTime);
            physicalPartition.setNextVersion(stmt.getVersion() + 1);

            PartitionVersionRecoveryInfo.PartitionVersion partitionVersion =
                    new PartitionVersionRecoveryInfo.PartitionVersion(database.getId(), table.getId(),
                    physicalPartition.getId(), stmt.getVersion());
            for (MaterializedIndex index : physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    if (!(tablet instanceof LocalTablet)) {
                        continue;
                    }

                    LocalTablet localTablet = (LocalTablet) tablet;
                    for (Replica replica : localTablet.getAllReplicas()) {
                        if (replica.getVersion() > stmt.getVersion() && localTablet.getAllReplicas().size() > 1) {
                            replica.setBad(true);
                            LOG.warn("set tablet: {} on backend: {} to bad, " +
                                            "because its version: {} is higher than partition visible version: {}",
                                    tablet.getId(), replica.getBackendId(), replica.getVersion(), stmt.getVersion());
                        }
                    }
                }
            }
            GlobalStateMgr.getCurrentState().getLocalMetastore().setPartitionVersion(
                    new PartitionVersionRecoveryInfo(Lists.newArrayList(partitionVersion), visibleVersionTime));
            LOG.info("Successfully set partition: {} version to {}, table: {}, db: {}",
                    stmt.getPartitionName(), stmt.getVersion(), table.getName(), database.getFullName());
        } finally {
            locker.unLockDatabase(database.getId(), LockType.WRITE);
        }
    }

    public Partition getPartition(String dbName, String tblName, String partitionName) {
        return null;
    }

    public Partition getPartition(Database db, OlapTable olapTable, Long partitionId) {
        return olapTable.getPartition(partitionId);
    }

    public List<Partition> getAllPartitions(Database db, OlapTable olapTable) {
        return new ArrayList<>(olapTable.getAllPartitions());
    }

    public void addSubPartitions(Database db, OlapTable table, Partition partition,
                                 int numSubPartition, long warehouseId) throws DdlException {
        try {
            table.setAutomaticBucketing(true);
            addSubPartitions(db, table, partition, numSubPartition, null, warehouseId);
        } finally {
            table.setAutomaticBucketing(false);
        }
    }

    private void addSubPartitions(Database db, OlapTable table, Partition partition,
                                  int numSubPartition, String[] subPartitionNames, long warehouseId) throws DdlException {
        OlapTable olapTable;
        OlapTable copiedTable;

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            olapTable = checkTable(db, table.getId());

            if (partition.getDistributionInfo().getType() != DistributionInfo.DistributionInfoType.RANDOM) {
                throw new DdlException("Only support adding physical partition to random distributed table");
            }

            copiedTable = getShadowCopyTable(olapTable);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        Preconditions.checkNotNull(olapTable);
        Preconditions.checkNotNull(copiedTable);

        List<PhysicalPartition> subPartitions = new ArrayList<>();
        // create physical partition
        for (int i = 0; i < numSubPartition; i++) {
            String name = subPartitionNames != null && subPartitionNames.length > i ? subPartitionNames[i] : null;
            PhysicalPartition subPartition = createPhysicalPartition(name, db, copiedTable, partition, warehouseId);
            subPartitions.add(subPartition);
        }

        // build partitions
        buildPartitions(db, copiedTable, subPartitions, warehouseId);

        // check again
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
            throw new DdlException("db " + db.getFullName()
                    + "(" + db.getId() + ") has been dropped");
        }
        try {
            // check if meta changed
            checkIfMetaChange(olapTable, copiedTable, table.getName());

            for (PhysicalPartition subPartition : subPartitions) {
                // add sub partition
                GlobalStateMgr.getCurrentState().getTabletMetastore().addPhysicalPartition(partition, subPartition);
                olapTable.addPhysicalPartition(subPartition);
            }

            olapTable.setShardGroupChanged(true);

            // add partition log
            GlobalStateMgr.getCurrentState().getLocalMetastore().addSubPartitionLog(db, olapTable, partition, subPartitions);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    private PhysicalPartition createPhysicalPartition(String name, Database db, OlapTable olapTable,
                                                      Partition partition, long warehouseId) throws DdlException {
        long partitionId = partition.getId();
        DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo().copy();
        olapTable.inferDistribution(distributionInfo);
        // create sub partition
        Map<Long, MaterializedIndex> indexMap = new HashMap<>();
        for (long indexId : olapTable.getIndexIdToMeta().keySet()) {
            MaterializedIndex rollup = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
            indexMap.put(indexId, rollup);
        }

        Long id = GlobalStateMgr.getCurrentState().getNextId();
        // physical partitions in the same logical partition use the same shard_group_id,
        // so that the shards of this logical partition are more evenly distributed.
        long shardGroupId = partition.getDefaultPhysicalPartition().getShardGroupId();

        if (name == null) {
            name = partition.generatePhysicalPartitionName(id);
        }
        PhysicalPartition physicalParition = new PhysicalPartition(
                id, name, partition.getId(), shardGroupId, indexMap.get(olapTable.getBaseIndexId()));

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        short replicationNum = partitionInfo.getReplicationNum(partitionId);
        TStorageMedium storageMedium = partitionInfo.getDataProperty(partitionId).getStorageMedium();
        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndex index = entry.getValue();
            MaterializedIndexMeta indexMeta = olapTable.getIndexIdToMeta().get(indexId);
            Set<Long> tabletIdSet = new HashSet<>();

            // create tablets
            TabletMeta tabletMeta =
                    new TabletMeta(db.getId(), olapTable.getId(), id, indexId, indexMeta.getSchemaHash(),
                            storageMedium, olapTable.isCloudNativeTableOrMaterializedView());

            if (olapTable.isCloudNativeTableOrMaterializedView()) {
                GlobalStateMgr.getCurrentState().getTabletManager().createLakeTablets(
                        olapTable, id, shardGroupId, index, distributionInfo,
                        tabletMeta, tabletIdSet, warehouseId);
            } else {
                GlobalStateMgr.getCurrentState().getTabletManager().createOlapTablets(
                        olapTable, index, Replica.ReplicaState.NORMAL, distributionInfo,
                        physicalParition.getVisibleVersion(), replicationNum, tabletMeta, tabletIdSet);
            }
            if (index.getId() != olapTable.getBaseIndexId()) {
                // add rollup index to partition
                physicalParition.createRollupIndex(index);
            }
        }

        return physicalParition;
    }

    public void buildPartitions(Database db, OlapTable table, List<PhysicalPartition> partitions, long warehouseId)
            throws DdlException {
        if (partitions.isEmpty()) {
            return;
        }
        int numAliveNodes = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber();

        if (RunMode.isSharedDataMode()) {
            numAliveNodes = 0;
            List<Long> computeNodeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseId);
            for (long nodeId : computeNodeIds) {
                if (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId).isAlive()) {
                    ++numAliveNodes;
                }
            }
        }
        if (numAliveNodes == 0) {
            if (RunMode.isSharedDataMode()) {
                throw new DdlException("no alive compute nodes");
            } else {
                throw new DdlException("no alive backends");
            }
        }

        int numReplicas = 0;
        for (PhysicalPartition partition : partitions) {
            numReplicas += partition.storageReplicaCount();
        }

        try {
            GlobalStateMgr.getCurrentState().getConsistencyChecker().addCreatingTableId(table.getId());
            if (numReplicas > Config.create_table_max_serial_replicas) {
                LOG.info("start to build {} partitions concurrently for table {}.{} with {} replicas",
                        partitions.size(), db.getFullName(), table.getName(), numReplicas);
                TabletTaskExecutor.buildPartitionsConcurrently(
                        db.getId(), table, partitions, numReplicas, numAliveNodes, warehouseId);
            } else {
                LOG.info("start to build {} partitions sequentially for table {}.{} with {} replicas",
                        partitions.size(), db.getFullName(), table.getName(), numReplicas);
                TabletTaskExecutor.buildPartitionsSequentially(
                        db.getId(), table, partitions, numReplicas, numAliveNodes, warehouseId);
            }
        } finally {
            GlobalStateMgr.getCurrentState().getConsistencyChecker().deleteCreatingTableId(table.getId());
        }
    }

    // create new partitions from source partitions.
    // new partitions have the same indexes as source partitions.
    public List<Partition> createTempPartitionsFromPartitions(Database db, Table table,
                                                              String namePostfix, List<Long> sourcePartitionIds,
                                                              List<Long> tmpPartitionIds, DistributionDesc distributionDesc,
                                                              long warehouseId) {
        Preconditions.checkState(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        Map<Long, String> origPartitions = Maps.newHashMap();
        OlapTable copiedTbl = getCopiedTable(db, olapTable, sourcePartitionIds, origPartitions, distributionDesc != null);
        copiedTbl.setDefaultDistributionInfo(olapTable.getDefaultDistributionInfo());

        // 2. use the copied table to create partitions
        List<Partition> newPartitions = null;
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        try {
            newPartitions = getNewPartitionsFromPartitions(db, olapTable, sourcePartitionIds, origPartitions,
                    copiedTbl, namePostfix, tabletIdSet, tmpPartitionIds, distributionDesc, warehouseId);
            buildPartitions(db, copiedTbl, newPartitions.stream().map(Partition::getSubPartitions)
                    .flatMap(p -> p.stream()).collect(Collectors.toList()), warehouseId);
        } catch (Exception e) {
            // create partition failed, remove all newly created tablets
            for (Long tabletId : tabletIdSet) {
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId);
            }
            LOG.warn("create partitions from partitions failed.", e);
            throw new RuntimeException("create partitions failed: " + e.getMessage(), e);
        }
        return newPartitions;
    }

    @VisibleForTesting
    public List<Partition> getNewPartitionsFromPartitions(Database db, OlapTable olapTable,
                                                          List<Long> sourcePartitionIds,
                                                          Map<Long, String> origPartitions, OlapTable copiedTbl,
                                                          String namePostfix, Set<Long> tabletIdSet,
                                                          List<Long> tmpPartitionIds, DistributionDesc distributionDesc,
                                                          long warehouseId)
            throws DdlException {
        List<Partition> newPartitions = Lists.newArrayListWithCapacity(sourcePartitionIds.size());
        for (int i = 0; i < sourcePartitionIds.size(); ++i) {
            long newPartitionId = tmpPartitionIds.get(i);
            long sourcePartitionId = sourcePartitionIds.get(i);
            String newPartitionName = origPartitions.get(sourcePartitionId) + namePostfix;
            if (olapTable.checkPartitionNameExist(newPartitionName, true)) {
                // to prevent creating the same partitions when failover
                // this will happen when OverwriteJob crashed after created temp partitions,
                // but before changing to PREPARED state
                LOG.warn("partition:{} already exists in table:{}", newPartitionName, olapTable.getName());
                continue;
            }
            PartitionInfo partitionInfo = copiedTbl.getPartitionInfo();
            partitionInfo.setTabletType(newPartitionId, partitionInfo.getTabletType(sourcePartitionId));
            partitionInfo.setIsInMemory(newPartitionId, partitionInfo.getIsInMemory(sourcePartitionId));
            partitionInfo.setReplicationNum(newPartitionId, partitionInfo.getReplicationNum(sourcePartitionId));
            partitionInfo.setDataProperty(newPartitionId, partitionInfo.getDataProperty(sourcePartitionId));
            if (copiedTbl.isCloudNativeTableOrMaterializedView()) {
                partitionInfo.setDataCacheInfo(newPartitionId, partitionInfo.getDataCacheInfo(sourcePartitionId));
            }

            Partition newPartition = null;
            if (distributionDesc != null) {
                DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(olapTable.getColumns());
                if (distributionInfo.getBucketNum() == 0) {
                    Partition sourcePartition = olapTable.getPartition(sourcePartitionId);
                    olapTable.optimizeDistribution(distributionInfo, sourcePartition);
                }
                newPartition = createPartition(
                        db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet, distributionInfo, warehouseId);
            } else {
                newPartition = createPartition(db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet, warehouseId);
            }

            newPartitions.add(newPartition);
        }
        return newPartitions;
    }

    public Partition createPartition(Database db, OlapTable table, long partitionId, String partitionName,
                              Long version, Set<Long> tabletIdSet, long warehouseId) throws DdlException {
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo().copy();
        table.inferDistribution(distributionInfo);

        return createPartition(db, table, partitionId, partitionName, version, tabletIdSet, distributionInfo, warehouseId);
    }

    public Partition createPartition(Database db, OlapTable table, long partitionId, String partitionName,
                              Long version, Set<Long> tabletIdSet, DistributionInfo distributionInfo,
                              long warehouseId) throws DdlException {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        Map<Long, MaterializedIndex> indexMap = new HashMap<>();
        for (long indexId : table.getIndexIdToMeta().keySet()) {
            MaterializedIndex rollup = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
            indexMap.put(indexId, rollup);
        }

        // create shard group
        long shardGroupId = 0;
        if (table.isCloudNativeTableOrMaterializedView()) {
            shardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().
                    createShardGroup(db.getId(), table.getId(), partitionId);
        }

        Partition logicalPartition = new Partition(
                partitionId,
                partitionName,
                distributionInfo);

        PhysicalPartition physicalPartition = new PhysicalPartition(
                partitionId,
                partitionName,
                partitionId,
                shardGroupId,
                indexMap.get(table.getBaseIndexId()));

        logicalPartition.addSubPartition(physicalPartition);

        //LogicalPartition partition = new LogicalPartition(partitionId, partitionName, indexMap.get(table.getBaseIndexId()), distributionInfo, shardGroupId);
        // version
        if (version != null) {
            physicalPartition.updateVisibleVersion(version);
        }

        short replicationNum = partitionInfo.getReplicationNum(partitionId);
        TStorageMedium storageMedium = partitionInfo.getDataProperty(partitionId).getStorageMedium();
        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndex index = entry.getValue();
            MaterializedIndexMeta indexMeta = table.getIndexIdToMeta().get(indexId);

            // create tablets
            TabletMeta tabletMeta =
                    new TabletMeta(db.getId(), table.getId(), partitionId, indexId, indexMeta.getSchemaHash(),
                            storageMedium, table.isCloudNativeTableOrMaterializedView());

            if (table.isCloudNativeTableOrMaterializedView()) {
                GlobalStateMgr.getCurrentState().getTabletManager().
                        createLakeTablets(table, partitionId, shardGroupId, index, distributionInfo,
                                tabletMeta, tabletIdSet, warehouseId);
            } else {
                GlobalStateMgr.getCurrentState().getTabletManager().
                        createOlapTablets(table, index, Replica.ReplicaState.NORMAL, distributionInfo,
                                physicalPartition.getVisibleVersion(), replicationNum, tabletMeta, tabletIdSet);
            }
            if (index.getId() != table.getBaseIndexId()) {
                // add rollup index to partition
                physicalPartition.createRollupIndex(index);
            }
        }
        return logicalPartition;
    }

    public Database getDbIncludeRecycleBin(long dbId) {
        Database db = getDb(dbId);
        if (db == null) {
            db = GlobalStateMgr.getCurrentState().getRecycleBin().getDatabase(dbId);
        }
        return db;
    }

    public Table getTableIncludeRecycleBin(Database db, long tableId) {
        Table table = getTable(db.getId(), tableId);
        if (table == null) {
            table = GlobalStateMgr.getCurrentState().getRecycleBin().getTable(db.getId(), tableId);
        }
        return table;
    }

    public List<Table> getTablesIncludeRecycleBin(Database db) {
        List<Table> tables = db.getTables();
        tables.addAll(GlobalStateMgr.getCurrentState().getRecycleBin().getTables(db.getId()));
        return tables;
    }

    public Partition getPartitionIncludeRecycleBin(OlapTable table, long partitionId) {
        Partition partition = table.getPartition(partitionId);
        if (partition == null) {
            partition = GlobalStateMgr.getCurrentState().getRecycleBin().getPartition(partitionId);
        }
        return partition;
    }

    public PhysicalPartition getPhysicalPartitionIncludeRecycleBin(OlapTable table, long physicalPartitionId) {
        PhysicalPartition partition = table.getPhysicalPartition(physicalPartitionId);
        if (partition == null) {
            partition = GlobalStateMgr.getCurrentState().getRecycleBin().getPhysicalPartition(physicalPartitionId);
        }
        return partition;
    }

    public Collection<Partition> getPartitionsIncludeRecycleBin(OlapTable table) {
        Collection<Partition> partitions = new ArrayList<>(table.getPartitions());
        partitions.addAll(GlobalStateMgr.getCurrentState().getRecycleBin().getPartitions(table.getId()));
        return partitions;
    }

    public Collection<Partition> getAllPartitionsIncludeRecycleBin(OlapTable table) {
        Collection<Partition> partitions = table.getAllPartitions();
        partitions.addAll(GlobalStateMgr.getCurrentState().getRecycleBin().getPartitions(table.getId()));
        return partitions;
    }

    // NOTE: result can be null, cause partition erase is not in db lock
    public DataProperty getDataPropertyIncludeRecycleBin(PartitionInfo info, long partitionId) {
        DataProperty dataProperty = info.getDataProperty(partitionId);
        if (dataProperty == null) {
            dataProperty = GlobalStateMgr.getCurrentState().getRecycleBin().getPartitionDataProperty(partitionId);
        }
        return dataProperty;
    }

    // NOTE: result can be -1, cause partition erase is not in db lock
    public short getReplicationNumIncludeRecycleBin(PartitionInfo info, long partitionId) {
        short replicaNum = info.getReplicationNum(partitionId);
        if (replicaNum == (short) -1) {
            replicaNum = GlobalStateMgr.getCurrentState().getRecycleBin().getPartitionReplicationNum(partitionId);
        }
        return replicaNum;
    }

    public List<Long> getDbIdsIncludeRecycleBin() {
        List<Long> dbIds = getDbIds();
        dbIds.addAll(GlobalStateMgr.getCurrentState().getRecycleBin().getAllDbIds());
        return dbIds;
    }

    @Override
    public Pair<Table, MaterializedIndexMeta> getMaterializedViewIndex(String dbName, String indexName) {
        Database database = getDb(dbName);
        if (database == null) {
            return null;
        }
        return database.getMaterializedViewIndex(indexName);
    }

    @VisibleForTesting
    public OlapTable getCopiedTable(Database db, OlapTable olapTable, List<Long> sourcePartitionIds,
                                    Map<Long, String> origPartitions, boolean isOptimize) {
        OlapTable copiedTbl;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                if (!isOptimize || olapTable.getState() != OlapTable.OlapTableState.SCHEMA_CHANGE) {
                    throw new RuntimeException("Table' state is not NORMAL: " + olapTable.getState()
                            + ", tableId:" + olapTable.getId() + ", tabletName:" + olapTable.getName());
                }
            }
            for (Long id : sourcePartitionIds) {
                origPartitions.put(id, olapTable.getPartition(id).getName());
            }
            copiedTbl = getShadowCopyTable(olapTable);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        return copiedTbl;
    }

    @VisibleForTesting
    public OlapTable getCopiedTable(Database db, OlapTable olapTable, List<Long> sourcePartitionIds,
                                    Map<Long, String> origPartitions) {
        return getCopiedTable(db, olapTable, sourcePartitionIds, origPartitions, false);
    }

    private OlapTable getShadowCopyTable(OlapTable olapTable) {
        OlapTable copiedTable;
        if (olapTable instanceof LakeMaterializedView) {
            copiedTable = new LakeMaterializedView();
        } else if (olapTable instanceof MaterializedView) {
            copiedTable = new MaterializedView();
        } else if (olapTable instanceof LakeTable) {
            copiedTable = new LakeTable();
        } else {
            copiedTable = new OlapTable();
        }

        olapTable.copyOnlyForQuery(copiedTable);
        return copiedTable;
    }

    /*
     * generate and check columns' order and key's existence
     */
    public void validateColumns(List<Column> columns) throws DdlException {
        if (columns.isEmpty()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        boolean encounterValue = false;
        boolean hasKey = false;
        for (Column column : columns) {
            if (column.isKey()) {
                if (encounterValue) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_OLAP_KEY_MUST_BEFORE_VALUE);
                }
                hasKey = true;
            } else {
                encounterValue = true;
            }
        }

        if (!hasKey) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_KEYS);
        }
    }

    public void setLakeStorageInfo(Database db, OlapTable table, String storageVolumeId, Map<String, String> properties)
            throws DdlException {
        DataCacheInfo dataCacheInfo = null;
        try {
            dataCacheInfo = PropertyAnalyzer.analyzeDataCacheInfo(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // get service shard storage info from StarMgr
        FilePathInfo pathInfo = !storageVolumeId.isEmpty() ?
                GlobalStateMgr.getCurrentState().getStarOSAgent().allocateFilePath(storageVolumeId, db.getId(), table.getId()) :
                GlobalStateMgr.getCurrentState().getStarOSAgent().allocateFilePath(db.getId(), table.getId());
        table.setStorageInfo(pathInfo, dataCacheInfo);
    }

    public static PartitionInfo buildPartitionInfo(CreateMaterializedViewStatement stmt) throws DdlException {
        ExpressionPartitionDesc expressionPartitionDesc = stmt.getPartitionExpDesc();
        if (expressionPartitionDesc != null) {
            Expr expr = expressionPartitionDesc.getExpr();
            if (expr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) expr;
                if (slotRef.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                    return new ListPartitionInfo(PartitionType.LIST,
                            Collections.singletonList(stmt.getPartitionColumn()));
                }
            }
            if ((expr instanceof FunctionCallExpr)) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE)) {
                    Column partitionColumn = new Column(stmt.getPartitionColumn());
                    partitionColumn.setType(com.starrocks.catalog.Type.DATE);
                    return expressionPartitionDesc.toPartitionInfo(
                            Collections.singletonList(partitionColumn),
                            Maps.newHashMap(), false);
                }
            }
            return expressionPartitionDesc.toPartitionInfo(
                    Collections.singletonList(stmt.getPartitionColumn()),
                    Maps.newHashMap(), false);
        } else {
            return new SinglePartitionInfo();
        }
    }

    public static void inactiveRelatedMaterializedView(Database db, Table olapTable, String reason) {
        for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getId(), mvId.getId());
            if (mv != null) {
                LOG.warn("Inactive MV {}/{} because {}", mv.getName(), mv.getId(), reason);
                mv.setInactiveAndReason(reason);

                // recursive inactive
                inactiveRelatedMaterializedView(db, mv,
                        MaterializedViewExceptions.inactiveReasonForBaseTableActive(mv.getName()));
            } else {
                LOG.info("Ignore materialized view {} does not exists", mvId);
            }
        }
    }

    public void onErasePartition(Partition partition) {
        // remove tablet in inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            for (MaterializedIndex index : physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    long tabletId = tablet.getId();
                    invertedIndex.deleteTablet(tabletId);
                }
            }
        }
    }
}
