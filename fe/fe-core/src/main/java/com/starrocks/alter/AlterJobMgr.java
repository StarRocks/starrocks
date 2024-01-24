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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.BatchModifyPartitionsInfo;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
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
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.ModifyPartitionClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncatePartitionClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.threeten.extra.PeriodDuration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class AlterJobMgr {
    private static final Logger LOG = LogManager.getLogger(AlterJobMgr.class);
    public static final String MANUAL_INACTIVE_MV_REASON = "user use alter materialized view set status to inactive";

    private final SchemaChangeHandler schemaChangeHandler;
    private final MaterializedViewHandler materializedViewHandler;
    private final SystemHandler clusterHandler;
    private CompactionHandler compactionHandler;

    public AlterJobMgr() {
        schemaChangeHandler = new SchemaChangeHandler();
        materializedViewHandler = new MaterializedViewHandler();
        clusterHandler = new SystemHandler();
        compactionHandler = new CompactionHandler();
    }

    public void start() {
        schemaChangeHandler.start();
        materializedViewHandler.start();
        clusterHandler.start();
        compactionHandler = new CompactionHandler();
    }

    public void processCreateSynchronousMaterializedView(CreateMaterializedViewStmt stmt)
            throws DdlException, AnalysisException {
        String tableName = stmt.getBaseIndexName();
        // check db
        String dbName = stmt.getDBName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // check cluster capacity
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkClusterCapacity();
        // check db quota
        db.checkQuota();

        Locker locker = new Locker();
        if (!locker.lockAndCheckExist(db, LockType.WRITE)) {
            throw new DdlException("create materialized failed. database:" + db.getFullName() + " not exist");
        }
        try {
            Table table = db.getTable(tableName);
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
            locker.unLockDatabase(db, LockType.WRITE);
        }
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

            String createMvSql = materializedView.getMaterializedViewDdlStmt(false);
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
        if (!Objects.equals(newColumns, existedColumns)) {
            String msg = String.format("mv schema changed: [%s] does not match [%s]",
                    existedColumns, newColumns);
            materializedView.setInactiveAndReason(msg);
            throw new SemanticException(msg);
        }

        return createStmt.getQueryStatement();
    }

    public void replayAlterMaterializedViewBaseTableInfos(AlterMaterializedViewBaseTableInfosLog log) {
        long dbId = log.getDbId();
        long mvId = log.getMvId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        MaterializedView mv = null;
        try {
            mv = (MaterializedView) db.getTable(mvId);
            mv.replayAlterMaterializedViewBaseTableInfos(log);
        } catch (Throwable e) {
            if (mv != null) {
                LOG.warn("replay alter materialized-view status failed: {}", mv.getName(), e);
                mv.setInactiveAndReason("replay alter status failed: " + e.getMessage());
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    public void replayAlterMaterializedViewStatus(AlterMaterializedViewStatusLog log) {
        long dbId = log.getDbId();
        long tableId = log.getTableId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        MaterializedView mv = null;
        try {
            mv = (MaterializedView) db.getTable(tableId);
            alterMaterializedViewStatus(mv, log.getStatus(), true);
        } catch (Throwable e) {
            if (mv != null) {
                LOG.warn("replay alter materialized-view status failed: {}", mv.getName(), e);
                mv.setInactiveAndReason("replay alter status failed: " + e.getMessage());
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    public void replayRenameMaterializedView(RenameMaterializedViewLog log) {
        long dbId = log.getDbId();
        long materializedViewId = log.getId();
        String newMaterializedViewName = log.getNewMaterializedViewName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        MaterializedView oldMaterializedView = null;
        try {
            oldMaterializedView = (MaterializedView) db.getTable(materializedViewId);
            db.dropTable(oldMaterializedView.getName());
            oldMaterializedView.setName(newMaterializedViewName);
            db.registerTableUnlocked(oldMaterializedView);
            updateTaskDefinition(oldMaterializedView);
            LOG.info("Replay rename materialized view [{}] to {}, id: {}", oldMaterializedView.getName(),
                    newMaterializedViewName, oldMaterializedView.getId());
        } catch (Throwable e) {
            if (oldMaterializedView != null) {
                oldMaterializedView.setInactiveAndReason("replay rename failed: " + e.getMessage());
                LOG.warn("replay rename materialized-view failed: {}", oldMaterializedView.getName(), e);
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    private void updateTaskDefinition(MaterializedView materializedView) {
        Task currentTask = GlobalStateMgr.getCurrentState().getTaskManager().getTask(
                TaskBuilder.getMvTaskName(materializedView.getId()));
        if (currentTask != null) {
            currentTask.setDefinition("insert overwrite " + materializedView.getName() + " " +
                    materializedView.getViewDefineSql());
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
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        MaterializedView oldMaterializedView = null;
        try {
            final MaterializedView.MvRefreshScheme newMvRefreshScheme = new MaterializedView.MvRefreshScheme();

            oldMaterializedView = (MaterializedView) db.getTable(id);
            if (oldMaterializedView == null) {
                LOG.warn("Ignore change materialized view refresh scheme log because table:" + id + "is null");
                return;
            }
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
            if (oldMaterializedView != null) {
                oldMaterializedView.setInactiveAndReason("replay failed: " + e.getMessage());
                LOG.warn("replay change materialized-view refresh scheme failed: {}",
                        oldMaterializedView.getName(), e);
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    public void replayAlterMaterializedViewProperties(short opCode, ModifyTablePropertyOperationLog log) {
        long dbId = log.getDbId();
        long tableId = log.getTableId();
        Map<String, String> properties = log.getProperties();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        MaterializedView mv = null;
        try {
            mv = (MaterializedView) db.getTable(tableId);
            TableProperty tableProperty = mv.getTableProperty();
            if (tableProperty == null) {
                tableProperty = new TableProperty(properties);
                mv.setTableProperty(tableProperty.buildProperty(opCode));
            } else {
                tableProperty.modifyTableProperties(properties);
                tableProperty.buildProperty(opCode);
            }
        } catch (Throwable e) {
            if (mv != null) {
                mv.setInactiveAndReason("replay failed: " + e.getMessage());
                LOG.warn("replay alter materialized-view properties failed: {}", mv.getName(), e);
            }
        } finally {
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
                    if (dropPartitionClause.getPartitionName()
                            .startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                        throw new DdlException("Deletion of shadow partitions is not allowed");
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

        // the following ops should done outside db lock. because it contain synchronized create operation
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
                GlobalStateMgr.getCurrentState().getLocalMetastore().truncateTable(tStmt);
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
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        try {
            View view = (View) db.getTable(tableId);
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
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    public ShowResultSet processAlterCluster(AlterSystemStmt stmt) throws UserException {
        return clusterHandler.process(Collections.singletonList(stmt.getAlterClause()), null, null);
    }

    private void processAlterComment(Database db, OlapTable table, List<AlterClause> alterClauses) throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof AlterTableCommentClause) {
                GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .alterTableComment(db, table, (AlterTableCommentClause) alterClause);
                break;
            } else {
                throw new DdlException("Unsupported alter table clause " + alterClause);
            }
        }
    }

    private void processRename(Database db, OlapTable table, List<AlterClause> alterClauses) throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof TableRenameClause) {
                GlobalStateMgr.getCurrentState().getLocalMetastore().renameTable(db, table, (TableRenameClause) alterClause);
                break;
            } else if (alterClause instanceof RollupRenameClause) {
                GlobalStateMgr.getCurrentState().getLocalMetastore().renameRollup(db, table, (RollupRenameClause) alterClause);
                break;
            } else if (alterClause instanceof PartitionRenameClause) {
                PartitionRenameClause partitionRenameClause = (PartitionRenameClause) alterClause;
                if (partitionRenameClause.getPartitionName()
                        .startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                    throw new DdlException("Rename of shadow partitions is not allowed");
                }
                GlobalStateMgr.getCurrentState().getLocalMetastore().renamePartition(db, table, partitionRenameClause);
                break;
            } else if (alterClause instanceof ColumnRenameClause) {
                GlobalStateMgr.getCurrentState().getLocalMetastore().renameColumn(db, table, (ColumnRenameClause) alterClause);
                break;
            } else {
                Preconditions.checkState(false);
            }
        }
    }

    /**
     * Batch update partitions' properties
     * caller should hold the db lock
     */
    public void modifyPartitionsProperty(Database db,
                                         OlapTable olapTable,
                                         List<String> partitionNames,
                                         Map<String, String> properties)
            throws DdlException, AnalysisException {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isWriteLockHeldByCurrentThread(db));
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        List<ModifyPartitionInfo> modifyPartitionInfos = Lists.newArrayList();
        if (olapTable.getState() != OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName());
        }

        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }
        }

        boolean hasInMemory = false;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
            hasInMemory = true;
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        // get value from properties here
        // 1. data property
        PeriodDuration periodDuration = null;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
            String storageCoolDownTTL = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
            if (!partitionInfo.isRangePartition()) {
                throw new DdlException("Only support range partition table to modify storage_cooldown_ttl");
            }
            if (Strings.isNotBlank(storageCoolDownTTL)) {
                periodDuration = TimeUtils.parseHumanReadablePeriodOrDuration(storageCoolDownTTL);
            }
        }
        DataProperty newDataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties, null, false);
        // 2. replication num
        short newReplicationNum =
                PropertyAnalyzer.analyzeReplicationNum(properties, (short) -1);
        // 3. in memory
        boolean newInMemory = PropertyAnalyzer.analyzeBooleanProp(properties,
                PropertyAnalyzer.PROPERTIES_INMEMORY, false);
        // 4. tablet type
        TTabletType tTabletType =
                PropertyAnalyzer.analyzeTabletType(properties);

        // modify meta here
        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            // 1. date property

            if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                continue;
            }

            if (newDataProperty != null) {
                // for storage_cooldown_ttl
                if (periodDuration != null) {
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    Range<PartitionKey> range = rangePartitionInfo.getRange(partition.getId());
                    PartitionKey partitionKey = range.upperEndpoint();
                    if (partitionKey.isMaxValue()) {
                        newDataProperty = new DataProperty(TStorageMedium.SSD, DataProperty.MAX_COOLDOWN_TIME_MS);
                    } else {
                        String stringUpperValue = partitionKey.getKeys().get(0).getStringValue();
                        DateTimeFormatter dateTimeFormatter = DateUtils.probeFormat(stringUpperValue);
                        LocalDateTime upperTime = DateUtils.parseStringWithDefaultHSM(stringUpperValue, dateTimeFormatter);
                        LocalDateTime updatedUpperTime = upperTime.plus(periodDuration);
                        DateLiteral dateLiteral = new DateLiteral(updatedUpperTime, Type.DATETIME);
                        long coolDownTimeStamp = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
                        newDataProperty = new DataProperty(TStorageMedium.SSD, coolDownTimeStamp);
                    }
                }
                partitionInfo.setDataProperty(partition.getId(), newDataProperty);
            }
            // 2. replication num
            if (newReplicationNum != (short) -1) {
                if (colocateTableIndex.isColocateTable(olapTable.getId())) {
                    throw new DdlException(
                            "table " + olapTable.getName() + " is colocate table, cannot change replicationNum");
                }
                partitionInfo.setReplicationNum(partition.getId(), newReplicationNum);
                // update default replication num if this table is unpartitioned table
                if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                    olapTable.setReplicationNum(newReplicationNum);
                }
            }
            // 3. in memory
            boolean oldInMemory = partitionInfo.getIsInMemory(partition.getId());
            if (hasInMemory && (newInMemory != oldInMemory)) {
                partitionInfo.setIsInMemory(partition.getId(), newInMemory);
            }
            // 4. tablet type
            if (tTabletType != partitionInfo.getTabletType(partition.getId())) {
                partitionInfo.setTabletType(partition.getId(), tTabletType);
            }
            ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), olapTable.getId(), partition.getId(),
                    newDataProperty, newReplicationNum, hasInMemory ? newInMemory : oldInMemory);
            modifyPartitionInfos.add(info);
        }

        // log here
        BatchModifyPartitionsInfo info = new BatchModifyPartitionsInfo(modifyPartitionInfos);
        GlobalStateMgr.getCurrentState().getEditLog().logBatchModifyPartition(info);
    }

    public void replayModifyPartition(ModifyPartitionInfo info) {
        Database db = GlobalStateMgr.getCurrentState().getDb(info.getDbId());
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
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
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    public AlterHandler getSchemaChangeHandler() {
        return this.schemaChangeHandler;
    }

    public AlterHandler getMaterializedViewHandler() {
        return this.materializedViewHandler;
    }

    public AlterHandler getClusterHandler() {
        return this.clusterHandler;
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        Map<Long, AlterJobV2> schemaChangeAlterJobs = schemaChangeHandler.getAlterJobsV2();
        Map<Long, AlterJobV2> materializedViewAlterJobs = materializedViewHandler.getAlterJobsV2();

        int cnt = 1 + schemaChangeAlterJobs.size() + 1 + materializedViewAlterJobs.size();
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.ALTER_MGR, cnt);

        writer.writeJson(schemaChangeAlterJobs.size());
        for (AlterJobV2 alterJobV2 : schemaChangeAlterJobs.values()) {
            writer.writeJson(alterJobV2);
        }

        writer.writeJson(materializedViewAlterJobs.size());
        for (AlterJobV2 alterJobV2 : materializedViewAlterJobs.values()) {
            writer.writeJson(alterJobV2);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int schemaChangeJobSize = reader.readJson(int.class);
        for (int i = 0; i != schemaChangeJobSize; ++i) {
            AlterJobV2 alterJobV2 = reader.readJson(AlterJobV2.class);
            schemaChangeHandler.addAlterJobV2(alterJobV2);

            // ATTN : we just want to add tablet into TabletInvertedIndex when only PendingJob is checkpoint
            // to prevent TabletInvertedIndex data loss,
            // So just use AlterJob.replay() instead of AlterHandler.replay().
            if (alterJobV2.getJobState() == AlterJobV2.JobState.PENDING) {
                alterJobV2.replay(alterJobV2);
                LOG.info("replay pending alter job when load alter job {} ", alterJobV2.getJobId());
            }
        }

        int materializedViewJobSize = reader.readJson(int.class);
        for (int i = 0; i != materializedViewJobSize; ++i) {
            AlterJobV2 alterJobV2 = reader.readJson(AlterJobV2.class);
            materializedViewHandler.addAlterJobV2(alterJobV2);

            // ATTN : we just want to add tablet into TabletInvertedIndex when only PendingJob is checkpoint
            // to prevent TabletInvertedIndex data loss,
            // So just use AlterJob.replay() instead of AlterHandler.replay().
            if (alterJobV2.getJobState() == AlterJobV2.JobState.PENDING) {
                alterJobV2.replay(alterJobV2);
                LOG.info("replay pending alter job when load alter job {} ", alterJobV2.getJobId());
            }
        }
    }
}
