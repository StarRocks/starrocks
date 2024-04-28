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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/Alter.java

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

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.View;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.RenameMaterializedViewLog;
import com.starrocks.persist.SwapTableOperationLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
<<<<<<< HEAD
=======
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import org.apache.commons.collections4.CollectionUtils;
>>>>>>> 5b00b70c4a ([Enhancement] Support to batch drop partitions (#43539))
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class AlterJobMgr {
    private static final Logger LOG = LogManager.getLogger(AlterJobMgr.class);
    public static final String MANUAL_INACTIVE_MV_REASON = "user use alter materialized view set status to inactive";

    private final SchemaChangeHandler schemaChangeHandler;
    private final MaterializedViewHandler materializedViewHandler;
    private final SystemHandler clusterHandler;

    public AlterJobMgr(SchemaChangeHandler schemaChangeHandler,
                       MaterializedViewHandler materializedViewHandler,
                       SystemHandler systemHandler) {
        this.schemaChangeHandler = schemaChangeHandler;
        this.materializedViewHandler = materializedViewHandler;
        this.clusterHandler = systemHandler;
    }

    public void start() {
        schemaChangeHandler.start();
        materializedViewHandler.start();
        clusterHandler.start();
    }

    public void stop() {
        schemaChangeHandler.setStop();
        materializedViewHandler.setStop();
        clusterHandler.setStop();
    }

    public void processDropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        // check db
        String dbName = stmt.getDbName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        Locker locker = new Locker();
        if (!locker.lockAndCheckExist(db, LockType.WRITE)) {
            throw new DdlException("drop materialized failed. database:" + db.getFullName() + " not exist");
        }
        try {
            Table table = null;
            boolean hasfindTable = false;
            for (Table t : db.getTables()) {
                if (t instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) t;
                    for (MaterializedIndexMeta mvMeta : olapTable.getVisibleIndexMetas()) {
                        String indexName = olapTable.getIndexNameById(mvMeta.getIndexId());
                        if (indexName == null) {
                            LOG.warn("OlapTable {} miss index {}", olapTable.getName(), mvMeta.getIndexId());
                            continue;
                        }
                        if (indexName.equals(stmt.getMvName())) {
                            table = olapTable;
                            hasfindTable = true;
                            break;
                        }
                    }
                    if (hasfindTable) {
                        break;
                    }
                }
            }
            if (table == null) {
                throw new MetaNotFoundException("Materialized view " + stmt.getMvName() + " is not found");
            }
            // check table type
            if (table.getType() != TableType.OLAP) {
                throw new DdlException(
                        "Do not support non-OLAP table [" + table.getName() + "] when drop materialized view");
            }
            // check table state
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName());
            }
            // drop materialized view
            materializedViewHandler.processDropMaterializedView(stmt, db, olapTable);

        } catch (MetaNotFoundException e) {
            if (stmt.isSetIfExists()) {
                LOG.info(e.getMessage());
            } else {
                throw e;
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    public void alterMaterializedViewStatus(MaterializedView materializedView, String status, boolean isReplay) {
        LOG.info("process change materialized view {} status to {}, isReplay: {}",
                materializedView.getName(), status, isReplay);
        if (AlterMaterializedViewStatusClause.ACTIVE.equalsIgnoreCase(status)) {
            ConnectContext context = new ConnectContext();
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

            String createMvSql = materializedView.getMaterializedViewDdlStmt(false, isReplay);
            QueryStatement mvQueryStatement = null;
            try {
                mvQueryStatement = recreateMVQuery(materializedView, context, createMvSql);
            } catch (SemanticException e) {
                throw new SemanticException("Can not active materialized view [%s]" +
                        " because analyze materialized view define sql: \n\n%s" +
                        "\n\nCause an error: %s", materializedView.getName(), createMvSql, e.getDetailMsg());
            }

            // Skip checks to maintain eventual consistency when replay
            List<BaseTableInfo> baseTableInfos =
                    Lists.newArrayList(MaterializedViewAnalyzer.getBaseTableInfos(mvQueryStatement, !isReplay));
            materializedView.setBaseTableInfos(baseTableInfos);
            materializedView.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMap();
            materializedView.onReload();
            materializedView.setActive();
        } else if (AlterMaterializedViewStatusClause.INACTIVE.equalsIgnoreCase(status)) {
            materializedView.setInactiveAndReason(MANUAL_INACTIVE_MV_REASON);
        }
    }

    /*
     * Recreate the MV query and validate the correctness of syntax and schema
     */
    public static QueryStatement recreateMVQuery(MaterializedView materializedView,
                                                 ConnectContext context,
                                                 String createMvSql) {
        // If we could parse the MV sql successfully, and the schema of mv does not change,
        // we could reuse the existing MV
        Optional<Database> mayDb = GlobalStateMgr.getCurrentState().mayGetDb(materializedView.getDbId());

        // check database existing
        String dbName = mayDb.orElseThrow(() ->
                new SemanticException("database " + materializedView.getDbId() + " not exists")).getFullName();
        context.setDatabase(dbName);

        // Try to parse and analyze the creation sql
        List<StatementBase> statementBaseList = SqlParser.parse(createMvSql, context.getSessionVariable());
        CreateMaterializedViewStatement createStmt = (CreateMaterializedViewStatement) statementBaseList.get(0);
        Analyzer.analyze(createStmt, context);

        // validate the schema
        List<Column> newColumns = createStmt.getMvColumnItems().stream()
                .sorted(Comparator.comparing(Column::getName))
                .collect(Collectors.toList());
        List<Column> existedColumns = materializedView.getColumns().stream()
                .sorted(Comparator.comparing(Column::getName))
                .collect(Collectors.toList());
        if (newColumns.size() != existedColumns.size()) {
            throw new SemanticException(String.format("number of columns changed: %d != %d",
                    existedColumns.size(), newColumns.size()));
        }

        for (int i = 0; i < existedColumns.size(); i++) {
            Column existed = existedColumns.get(i);
            Column created = newColumns.get(i);
            if (!isSchemaCompatible(existed, created)) {
                LOG.warn("Active materialized view {} failed, column schema changed: {} != {}",
                        materializedView.getName(), existed.toString(), created.toString());
                String message = MaterializedViewExceptions.inactiveReasonForColumnNotCompatible(
                        existed.toString(), created.toString());
                materializedView.setInactiveAndReason(message);
                throw new SemanticException(message);
            }
        }

        return createStmt.getQueryStatement();
    }

    /**
     * Check if the schema of existed and created column is compatible, if not, return false
     * @param existed mv's existed column
     * @param created new mv's created column
     */
    private static boolean isSchemaCompatible(Column existed, Column created) {
        if (Config.enable_active_materialized_view_schema_strict_check) {
            return existed.isSchemaCompatible(created);
        } else {
            return isSchemaCompatibleInLoose(existed, created);
        }
    }

    /**
     * Check if the schema of existed and created column is compatible in loose mode
     * @param t1 mv's existed column
     * @param t2 new mv's created column
     */
    private static boolean isSchemaCompatibleInLoose(Column t1, Column t2) {
        // check whether the column name are the same
        if (!t1.getName().equalsIgnoreCase(t2.getName())) {
            return false;
        }
        // check whether the column primitive type are the same
        if (!t1.getType().getPrimitiveType().equals(t2.getType().getPrimitiveType())) {
            return false;
        }
        return true;
    }

    public void replayAlterMaterializedViewBaseTableInfos(AlterMaterializedViewBaseTableInfosLog log) {
        long dbId = log.getDbId();
        long mvId = log.getMvId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        MaterializedView mv = (MaterializedView) db.getTable(mvId);
        if (mv == null) {
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(mv.getId()), LockType.WRITE);
        try {
            mv.replayAlterMaterializedViewBaseTableInfos(log);
        } catch (Throwable e) {
            LOG.warn("replay alter materialized-view status failed: {}", mv.getName(), e);
            mv.setInactiveAndReason("replay alter status failed: " + e.getMessage());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(mv.getId()), LockType.WRITE);
        }
    }

    public void replayAlterMaterializedViewStatus(AlterMaterializedViewStatusLog log) {
        long dbId = log.getDbId();
        long tableId = log.getTableId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        MaterializedView mv = (MaterializedView) db.getTable(tableId);
        if (mv == null) {
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(mv.getId()), LockType.WRITE);
        try {
            alterMaterializedViewStatus(mv, log.getStatus(), true);
        } catch (Throwable e) {
            LOG.warn("replay alter materialized-view status failed: {}", mv.getName(), e);
            mv.setInactiveAndReason("replay alter status failed: " + e.getMessage());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(mv.getId()), LockType.WRITE);
        }
    }

    public void replayRenameMaterializedView(RenameMaterializedViewLog log) {
        long dbId = log.getDbId();
        long materializedViewId = log.getId();
        String newMaterializedViewName = log.getNewMaterializedViewName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        MaterializedView oldMaterializedView = (MaterializedView) db.getTable(materializedViewId);
        if (oldMaterializedView != null) {
            try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db,
                    Lists.newArrayList(oldMaterializedView.getId()), LockType.WRITE)) {
                db.dropTable(oldMaterializedView.getName());
                oldMaterializedView.setName(newMaterializedViewName);
                db.registerTableUnlocked(oldMaterializedView);
                updateTaskDefinition(oldMaterializedView);
                LOG.info("Replay rename materialized view [{}] to {}, id: {}", oldMaterializedView.getName(),
                        newMaterializedViewName, oldMaterializedView.getId());
            } catch (Throwable e) {
                oldMaterializedView.setInactiveAndReason("replay rename failed: " + e.getMessage());
                LOG.warn("replay rename materialized-view failed: {}", oldMaterializedView.getName(), e);
            }
        }
    }

    private void updateTaskDefinition(MaterializedView materializedView) {
        Task currentTask = GlobalStateMgr.getCurrentState().getTaskManager().getTask(
                TaskBuilder.getMvTaskName(materializedView.getId()));
        if (currentTask != null) {
            currentTask.setDefinition(materializedView.getTaskDefinition());
            currentTask.setPostRun(TaskBuilder.getAnalyzeMVStmt(materializedView.getName()));
        }
    }

    public void replayChangeMaterializedViewRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log) {
        long dbId = log.getDbId();
        long id = log.getId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            return;
        }

        MaterializedView oldMaterializedView = (MaterializedView) db.getTable(id);
        if (oldMaterializedView == null) {
            LOG.warn("Ignore change materialized view refresh scheme log because table:" + id + "is null");
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(oldMaterializedView.getId()), LockType.WRITE);
        try {
            final MaterializedView.MvRefreshScheme newMvRefreshScheme = new MaterializedView.MvRefreshScheme();
            final MaterializedView.MvRefreshScheme oldRefreshScheme = oldMaterializedView.getRefreshScheme();
            newMvRefreshScheme.setAsyncRefreshContext(oldRefreshScheme.getAsyncRefreshContext());
            newMvRefreshScheme.setLastRefreshTime(oldRefreshScheme.getLastRefreshTime());
            final MaterializedView.RefreshType refreshType = log.getRefreshType();
            final MaterializedView.AsyncRefreshContext asyncRefreshContext = log.getAsyncRefreshContext();
            newMvRefreshScheme.setType(refreshType);
            newMvRefreshScheme.setAsyncRefreshContext(asyncRefreshContext);

            long maxChangedTableRefreshTime =
                    MvUtils.getMaxTablePartitionInfoRefreshTime(
                            log.getAsyncRefreshContext().getBaseTableVisibleVersionMap().values());
            newMvRefreshScheme.setLastRefreshTime(maxChangedTableRefreshTime);

            oldMaterializedView.setRefreshScheme(newMvRefreshScheme);
            LOG.info(
                    "Replay materialized view [{}]'s refresh type to {}, start time to {}, " +
                            "interval step to {}, timeunit to {}, id: {}, maxChangedTableRefreshTime:{}",
                    oldMaterializedView.getName(), refreshType.name(), asyncRefreshContext.getStartTime(),
                    asyncRefreshContext.getStep(),
                    asyncRefreshContext.getTimeUnit(), oldMaterializedView.getId(), maxChangedTableRefreshTime);
        } catch (Throwable e) {
            oldMaterializedView.setInactiveAndReason("replay failed: " + e.getMessage());
            LOG.warn("replay change materialized-view refresh scheme failed: {}",
                    oldMaterializedView.getName(), e);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(oldMaterializedView.getId()), LockType.WRITE);
        }
    }

    public void replayAlterMaterializedViewProperties(short opCode, ModifyTablePropertyOperationLog log) {
        long dbId = log.getDbId();
        long tableId = log.getTableId();
        Map<String, String> properties = log.getProperties();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        MaterializedView mv = (MaterializedView) db.getTable(tableId);
        if (mv == null) {
            LOG.warn("Ignore change materialized view properties og because table:" + tableId + "is null");
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(mv.getId()), LockType.WRITE);
        try {
            TableProperty tableProperty = mv.getTableProperty();
            if (tableProperty == null) {
                tableProperty = new TableProperty(properties);
                mv.setTableProperty(tableProperty.buildProperty(opCode));
            } else {
                tableProperty.modifyTableProperties(properties);
                tableProperty.buildProperty(opCode);
            }
        } catch (Throwable e) {
            mv.setInactiveAndReason("replay failed: " + e.getMessage());
            LOG.warn("replay alter materialized-view properties failed: {}", mv.getName(), e);
        } finally {
<<<<<<< HEAD
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(mv.getId()), LockType.WRITE);
=======
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    public void processAlterTable(AlterTableStmt stmt) throws UserException {
        TableName dbTableName = stmt.getTbl();
        String dbName = dbTableName.getDb();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check conflict alter ops first
        List<AlterClause> alterClauses = stmt.getOps();
        AlterOperations currentAlterOps = new AlterOperations();
        currentAlterOps.checkConflict(alterClauses);

        // check cluster capacity and db quota, only need to check once.
        if (currentAlterOps.needCheckCapacity()) {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkClusterCapacity();
            db.checkQuota();
        }

        // some operations will take long time to process, need to be done outside the databse lock
        boolean needProcessOutsideDatabaseLock = false;
        String tableName = dbTableName.getTbl();

        boolean isSynchronous = true;
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        OlapTable olapTable;
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (!(table.isOlapOrCloudNativeTable() || table.isMaterializedView())) {
                throw new DdlException("Do not support alter non-native table/materialized-view[" + tableName + "]");
            }
            olapTable = (OlapTable) table;

            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName());
            }

            if (currentAlterOps.hasSchemaChangeOp()) {
                // if modify storage type to v2, do schema change to convert all related tablets to segment v2 format
                schemaChangeHandler.process(alterClauses, db, olapTable);
                isSynchronous = false;
            } else if (currentAlterOps.contains(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC) &&
                    olapTable.isCloudNativeTable()) {
                schemaChangeHandler.processLakeTableAlterMeta(alterClauses, db, olapTable);
                isSynchronous = false;
            } else if (currentAlterOps.hasRollupOp()) {
                materializedViewHandler.process(alterClauses, db, olapTable);
                isSynchronous = false;
            } else if (currentAlterOps.hasPartitionOp()) {
                Preconditions.checkState(alterClauses.size() == 1);
                AlterClause alterClause = alterClauses.get(0);
                if (alterClause instanceof DropPartitionClause) {
                    DropPartitionClause dropPartitionClause = (DropPartitionClause) alterClause;
                    if (!dropPartitionClause.isTempPartition()) {
                        DynamicPartitionUtil.checkAlterAllowed((OlapTable) db.getTable(tableName));
                    }
                    if (dropPartitionClause.getPartitionName() != null && dropPartitionClause.getPartitionName()
                            .startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                        throw new DdlException("Deletion of shadow partitions is not allowed");
                    }
                    List<String> partitionNames = dropPartitionClause.getPartitionNames();
                    if (CollectionUtils.isNotEmpty(partitionNames)) {
                        boolean hasShadowPartition = partitionNames.stream()
                                .anyMatch(partitionName -> partitionName.startsWith(
                                        ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX));
                        if (hasShadowPartition) {
                            throw new DdlException("Deletion of shadow partitions is not allowed");
                        }
                    }
                    GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(db, olapTable, dropPartitionClause);
                } else if (alterClause instanceof ReplacePartitionClause) {
                    ReplacePartitionClause replacePartitionClause = (ReplacePartitionClause) alterClause;
                    List<String> partitionNames = replacePartitionClause.getPartitionNames();
                    for (String partitionName : partitionNames) {
                        if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                            throw new DdlException("Replace shadow partitions is not allowed");
                        }
                    }
                    GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .replaceTempPartition(db, tableName, replacePartitionClause);
                } else if (alterClause instanceof ModifyPartitionClause) {
                    ModifyPartitionClause clause = ((ModifyPartitionClause) alterClause);
                    // expand the partition names if it is 'Modify Partition(*)'
                    if (clause.isNeedExpand()) {
                        List<String> partitionNames = clause.getPartitionNames();
                        partitionNames.clear();
                        for (Partition partition : olapTable.getPartitions()) {
                            partitionNames.add(partition.getName());
                        }
                    }
                    Map<String, String> properties = clause.getProperties();
                    if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                        needProcessOutsideDatabaseLock = true;
                    } else {
                        List<String> partitionNames = clause.getPartitionNames();
                        modifyPartitionsProperty(db, olapTable, partitionNames, properties);
                    }
                } else if (alterClause instanceof AddPartitionClause) {
                    needProcessOutsideDatabaseLock = true;
                } else {
                    throw new DdlException("Invalid alter operation: " + alterClause.getOpType());
                }
            } else if (currentAlterOps.hasTruncatePartitionOp()) {
                needProcessOutsideDatabaseLock = true;
            } else if (currentAlterOps.hasRenameOp()) {
                processRename(db, olapTable, alterClauses);
            } else if (currentAlterOps.hasSwapOp()) {
                new AlterJobExecutor().process(stmt, ConnectContext.get());
            } else if (currentAlterOps.hasAlterCommentOp()) {
                processAlterComment(db, olapTable, alterClauses);
            } else if (currentAlterOps.contains(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC)) {
                needProcessOutsideDatabaseLock = true;
            } else if (currentAlterOps.contains(AlterOpType.COMPACT)) {
                needProcessOutsideDatabaseLock = true;
            } else {
                throw new DdlException("Invalid alter operations: " + currentAlterOps);
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        // the following ops should be done outside db lock. because it contains synchronized create operation
        if (needProcessOutsideDatabaseLock) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            if (alterClause instanceof AddPartitionClause) {
                if (!((AddPartitionClause) alterClause).isTempPartition()) {
                    DynamicPartitionUtil.checkAlterAllowed((OlapTable) db.getTable(tableName));
                }
                GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .addPartitions(db, tableName, (AddPartitionClause) alterClause);
            } else if (alterClause instanceof TruncatePartitionClause) {
                // This logic is used to adapt mysql syntax.
                // ALTER TABLE test TRUNCATE PARTITION p1;
                TruncatePartitionClause clause = (TruncatePartitionClause) alterClause;
                TableRef tableRef = new TableRef(stmt.getTbl(), null, clause.getPartitionNames());
                TruncateTableStmt tStmt = new TruncateTableStmt(tableRef);
                ConnectContext ctx = new ConnectContext();
                ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
                GlobalStateMgr.getCurrentState().getLocalMetastore().truncateTable(tStmt, ctx);
            } else if (alterClause instanceof ModifyPartitionClause) {
                ModifyPartitionClause clause = ((ModifyPartitionClause) alterClause);
                Map<String, String> properties = clause.getProperties();
                List<String> partitionNames = clause.getPartitionNames();
                // currently, only in memory property could reach here
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY));
                olapTable = (OlapTable) db.getTable(tableName);
                if (olapTable.isCloudNativeTable()) {
                    throw new DdlException("Lake table not support alter in_memory");
                }

                schemaChangeHandler.updatePartitionsInMemoryMeta(
                        db, tableName, partitionNames, properties);

                locker.lockDatabase(db, LockType.WRITE);
                try {
                    modifyPartitionsProperty(db, olapTable, partitionNames, properties);
                } finally {
                    locker.unLockDatabase(db, LockType.WRITE);
                }
            } else if (alterClause instanceof ModifyTablePropertiesClause) {
                Map<String, String> properties = alterClause.getProperties();
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC));

                olapTable = (OlapTable) db.getTable(tableName);
                if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                    schemaChangeHandler.updateTableMeta(db, tableName,
                            properties, TTabletMetaType.INMEMORY);
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX)) {
                    schemaChangeHandler.updateTableMeta(db, tableName, properties,
                            TTabletMetaType.ENABLE_PERSISTENT_INDEX);
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM)) {
                    schemaChangeHandler.updateTableMeta(db, tableName, properties,
                            TTabletMetaType.WRITE_QUORUM);
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE)) {
                    schemaChangeHandler.updateTableMeta(db, tableName, properties,
                            TTabletMetaType.REPLICATED_STORAGE);
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE)) {
                    schemaChangeHandler.updateTableMeta(db, tableName, properties,
                            TTabletMetaType.BUCKET_SIZE);
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC)) {
                    schemaChangeHandler.updateTableMeta(db, tableName, properties,
                            TTabletMetaType.PRIMARY_INDEX_CACHE_EXPIRE_SEC);
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)) {
                    boolean isSuccess = schemaChangeHandler.updateBinlogConfigMeta(db, olapTable.getId(),
                            properties, TTabletMetaType.BINLOG_CONFIG);
                    if (!isSuccess) {
                        throw new DdlException("modify binlog config of FEMeta failed or table has been droped");
                    }
                } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)
                        || properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
                    schemaChangeHandler.updateTableConstraint(db, olapTable.getName(), properties);
                } else {
                    throw new DdlException("Invalid alter operation: " + alterClause.getOpType());
                }
            } else if (alterClause instanceof CompactionClause) {
                String s = (((CompactionClause) alterClause).isBaseCompaction() ? "base" : "cumulative")
                        + " compact " + tableName + " partitions: " + ((CompactionClause) alterClause).getPartitionNames();
                compactionHandler.process(alterClauses, db, olapTable);
            }
        }

        if (isSynchronous) {
            olapTable.lastSchemaUpdateTime.set(System.nanoTime());
>>>>>>> 5b00b70c4a ([Enhancement] Support to batch drop partitions (#43539))
        }
    }

    public void replaySwapTable(SwapTableOperationLog log) {
        try {
            swapTableInternal(log);
        } catch (DdlException e) {
            LOG.warn("should not happen", e);
        }
        long dbId = log.getDbId();
        long origTblId = log.getOrigTblId();
        long newTblId = log.getNewTblId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        OlapTable origTable = (OlapTable) db.getTable(origTblId);
        OlapTable newTbl = (OlapTable) db.getTable(newTblId);
        LOG.debug("finish replay swap table {}-{} with table {}-{}", origTblId, origTable.getName(), newTblId,
                newTbl.getName());
    }

    /**
     * The swap table operation works as follow:
     * For example, SWAP TABLE A WITH TABLE B.
     * must pre check A can be renamed to B and B can be renamed to A
     */
    public void swapTableInternal(SwapTableOperationLog log) throws DdlException {
        long dbId = log.getDbId();
        long origTblId = log.getOrigTblId();
        long newTblId = log.getNewTblId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        OlapTable origTable = (OlapTable) db.getTable(origTblId);
        OlapTable newTbl = (OlapTable) db.getTable(newTblId);

        String origTblName = origTable.getName();
        String newTblName = newTbl.getName();

        // drop origin table and new table
        db.dropTable(origTblName);
        db.dropTable(newTblName);

        // rename new table name to origin table name and add it to database
        newTbl.checkAndSetName(origTblName, false);
        db.registerTableUnlocked(newTbl);

        // rename origin table name to new table name and add it to database
        origTable.checkAndSetName(newTblName, false);
        db.registerTableUnlocked(origTable);

        // swap dependencies of base table
        if (origTable.isMaterializedView()) {
            MaterializedView oldMv = (MaterializedView) origTable;
            MaterializedView newMv = (MaterializedView) newTbl;
            updateTaskDefinition(oldMv);
            updateTaskDefinition(newMv);
        }
    }

    public void alterView(AlterViewInfo alterViewInfo) {
        long dbId = alterViewInfo.getDbId();
        long tableId = alterViewInfo.getTableId();
        String inlineViewDef = alterViewInfo.getInlineViewDef();
        List<Column> newFullSchema = alterViewInfo.getNewFullSchema();
        String comment = alterViewInfo.getComment();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        View view = (View) db.getTable(tableId);

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(view.getId()), LockType.WRITE);
        try {
            String viewName = view.getName();
            view.setInlineViewDefWithSqlMode(inlineViewDef, alterViewInfo.getSqlMode());
            try {
                view.init();
            } catch (UserException e) {
                throw new AlterJobException("failed to init view stmt", e);
            }
            view.setNewFullSchema(newFullSchema);
            view.setComment(comment);
            LocalMetastore.inactiveRelatedMaterializedView(db, view,
                    MaterializedViewExceptions.inactiveReasonForBaseViewChanged(viewName));
            db.dropTable(viewName);
            db.registerTableUnlocked(view);

            LOG.info("replay modify view[{}] definition to {}", viewName, inlineViewDef);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(view.getId()), LockType.WRITE);
        }
    }

    public void replayModifyPartition(ModifyPartitionInfo info) {
        Database db = GlobalStateMgr.getCurrentState().getDb(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        try {
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.getDataProperty() != null) {
                partitionInfo.setDataProperty(info.getPartitionId(), info.getDataProperty());
            }
            if (info.getReplicationNum() != (short) -1) {
                short replicationNum = info.getReplicationNum();
                partitionInfo.setReplicationNum(info.getPartitionId(), replicationNum);
                // update default replication num if this table is unpartitioned table
                if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                    olapTable.setReplicationNum(replicationNum);
                }
            }
            partitionInfo.setIsInMemory(info.getPartitionId(), info.isInMemory());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        }
    }

    public SchemaChangeHandler getSchemaChangeHandler() {
        return this.schemaChangeHandler;
    }

    public MaterializedViewHandler getMaterializedViewHandler() {
        return this.materializedViewHandler;
    }

    public SystemHandler getClusterHandler() {
        return this.clusterHandler;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        Map<Long, AlterJobV2> schemaChangeAlterJobs = schemaChangeHandler.getAlterJobsV2();
        Map<Long, AlterJobV2> materializedViewAlterJobs = materializedViewHandler.getAlterJobsV2();

        int cnt = 1 + schemaChangeAlterJobs.size() + 1 + materializedViewAlterJobs.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.ALTER_MGR, cnt);

        writer.writeInt(schemaChangeAlterJobs.size());
        for (AlterJobV2 alterJobV2 : schemaChangeAlterJobs.values()) {
            writer.writeJson(alterJobV2);
        }

        writer.writeInt(materializedViewAlterJobs.size());
        for (AlterJobV2 alterJobV2 : materializedViewAlterJobs.values()) {
            writer.writeJson(alterJobV2);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(AlterJobV2.class, alterJobV2 -> {
            schemaChangeHandler.addAlterJobV2(alterJobV2);

            // ATTN : we just want to add tablet into TabletInvertedIndex when only PendingJob is checkpoint
            // to prevent TabletInvertedIndex data loss,
            // So just use AlterJob.replay() instead of AlterHandler.replay().
            if (alterJobV2.getJobState() == AlterJobV2.JobState.PENDING) {
                alterJobV2.replay(alterJobV2);
                LOG.info("replay pending alter job when load alter job {} ", alterJobV2.getJobId());
            }
        });

        reader.readCollection(AlterJobV2.class, alterJobV2 -> {
            materializedViewHandler.addAlterJobV2(alterJobV2);

            // ATTN : we just want to add tablet into TabletInvertedIndex when only PendingJob is checkpoint
            // to prevent TabletInvertedIndex data loss,
            // So just use AlterJob.replay() instead of AlterHandler.replay().
            if (alterJobV2.getJobState() == AlterJobV2.JobState.PENDING) {
                alterJobV2.replay(alterJobV2);
                LOG.info("replay pending alter job when load alter job {} ", alterJobV2.getJobId());
            }
        });
    }
}
