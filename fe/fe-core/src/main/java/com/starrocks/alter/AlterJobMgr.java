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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.catalog.View;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
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
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.mv.MaterializedViewMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewClause;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.ModifyPartitionClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SwapTableClause;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncatePartitionClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.starrocks.catalog.TableProperty.INVALID;

public class AlterJobMgr {
    private static final Logger LOG = LogManager.getLogger(AlterJobMgr.class);

    private final SchemaChangeHandler schemaChangeHandler;
    private final MaterializedViewHandler materializedViewHandler;
    private final SystemHandler clusterHandler;

    public AlterJobMgr() {
        schemaChangeHandler = new SchemaChangeHandler();
        materializedViewHandler = new MaterializedViewHandler();
        clusterHandler = new SystemHandler();
    }

    public void start() {
        schemaChangeHandler.start();
        materializedViewHandler.start();
        clusterHandler.start();
    }

    public void processCreateMaterializedView(CreateMaterializedViewStmt stmt)
            throws DdlException, AnalysisException {
        String tableName = stmt.getBaseIndexName();
        // check db
        String dbName = stmt.getDBName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // check cluster capacity
        GlobalStateMgr.getCurrentSystemInfo().checkClusterCapacity();
        // check db quota
        db.checkQuota();

        if (!db.writeLockAndCheckExist()) {
            throw new DdlException("create materialized failed. database:" + db.getFullName() + " not exist");
        }
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException("create materialized failed. table:" + tableName + " not exist");
            }
            if (!table.isOlapTable()) {
                throw new DdlException("Do not support create rollup on " + table.getType().name() +
                        " table[" + tableName + "], please use new syntax to create materialized view");
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
            db.writeUnlock();
        }
    }

    public void processDropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        // check db
        String dbName = stmt.getDbName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        if (!db.writeLockAndCheckExist()) {
            throw new DdlException("drop materialized failed. database:" + db.getFullName() + " not exist");
        }
        try {
            Table table = null;
            boolean hasfindTable = false;
            for (Table t : db.getTables()) {
                if (t instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) t;
                    for (MaterializedIndex mvIdx : olapTable.getVisibleIndex()) {
                        String indexName = olapTable.getIndexNameById(mvIdx.getId());
                        if (indexName == null) {
                            LOG.warn("OlapTable {} miss index {}", olapTable.getName(), mvIdx.getId());
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
                throw new DdlException("Table[" + table.getName() + "]'s state is not NORMAL. "
                        + "Do not allow doing DROP ops");
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
            db.writeUnlock();
        }
    }

    public void processAlterMaterializedView(AlterMaterializedViewStmt stmt)
            throws DdlException, MetaNotFoundException {
        // check db
        final TableName mvName = stmt.getMvName();
        final String oldMvName = mvName.getTbl();
        final String newMvName = stmt.getNewMvName();
        final RefreshSchemeDesc refreshSchemeDesc = stmt.getRefreshSchemeDesc();
        final String status = stmt.getStatus();
        ModifyTablePropertiesClause modifyTablePropertiesClause = stmt.getModifyTablePropertiesClause();
        String dbName = mvName.getDb();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        if (!db.writeLockAndCheckExist()) {
            throw new DdlException("alter materialized failed. database:" + db.getFullName() + " not exist");
        }
        MaterializedView materializedView = null;
        try {
            final Table table = db.getTable(oldMvName);
            if (table instanceof MaterializedView) {
                materializedView = (MaterializedView) table;
            }

            if (materializedView == null) {
                throw new MetaNotFoundException("Materialized view " + mvName + " is not found");
            }
            // check materialized view state
            if (materializedView.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Materialized view [" + materializedView.getName() + "]'s state is not NORMAL. "
                        + "Do not allow to do ALTER ops");
            }

            MaterializedViewMgr.getInstance().stopMaintainMV(materializedView);

            // rename materialized view
            if (newMvName != null) {
                processRenameMaterializedView(oldMvName, newMvName, db, materializedView);
            } else if (refreshSchemeDesc != null) {
                // change refresh scheme
                processChangeRefreshScheme(refreshSchemeDesc, materializedView, dbName);
            } else if (modifyTablePropertiesClause != null) {
                try {
                    processModifyTableProperties(modifyTablePropertiesClause, db, materializedView);
                } catch (AnalysisException ae) {
                    throw new DdlException(ae.getMessage());
                }
            } else if (status != null) {
                if (AlterMaterializedViewStmt.ACTIVE.equalsIgnoreCase(status)) {
                    if (materializedView.isActive()) {
                        return;
                    }
                    processChangeMaterializedViewStatus(materializedView, status, false);
                    GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .refreshMaterializedView(dbName, materializedView.getName(), true, null,
                                    Constants.TaskRunPriority.NORMAL.value(), true, false);
                } else if (AlterMaterializedViewStmt.INACTIVE.equalsIgnoreCase(status)) {
                    if (!materializedView.isActive()) {
                        return;
                    }
                    LOG.warn("Setting the materialized view {}({}) to inactive because " +
                                    "user alter materialized view status to inactive",
                            materializedView.getName(), materializedView.getId());
                    processChangeMaterializedViewStatus(materializedView, status, false);
                } else {
                    throw new DdlException("Unsupported modification materialized view status:" + status);
                }
                AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(materializedView.getDbId(),
                        materializedView.getId(), status);
                GlobalStateMgr.getCurrentState().getEditLog().logAlterMvStatus(log);

            } else {
                throw new DdlException("Unsupported modification for materialized view");
            }

            MaterializedViewMgr.getInstance().rebuildMaintainMV(materializedView);
        } finally {
            db.writeUnlock();
        }
    }

    private void processChangeMaterializedViewStatus(MaterializedView materializedView, String status, boolean isReplay) {

        if (AlterMaterializedViewStmt.ACTIVE.equalsIgnoreCase(status)) {
            String viewDefineSql = materializedView.getViewDefineSql();
            ConnectContext context = new ConnectContext();
            context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

            List<StatementBase> statementBaseList = SqlParser.parse(viewDefineSql, context.getSessionVariable());
            QueryStatement queryStatement = (QueryStatement) statementBaseList.get(0);
            try {
                Analyzer.analyze(queryStatement, context);
            } catch (SemanticException e) {
                throw new SemanticException("Can not active materialized view [" + materializedView.getName() +
                        "] because analyze materialized view define sql: \n\n" + viewDefineSql +
                        "\n\nCause an error: " + e.getMessage());
            }

            Map<TableName, Table> tableNameTableMap = AnalyzerUtils.collectAllConnectorTableAndView(queryStatement);
            List<BaseTableInfo> baseTableInfos = MaterializedViewAnalyzer.getBaseTableInfos(tableNameTableMap, !isReplay);
            materializedView.setBaseTableInfos(baseTableInfos);
            materializedView.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMap();
            GlobalStateMgr.getCurrentState().updateBaseTableRelatedMv(materializedView.getDbId(),
                    materializedView, baseTableInfos);
            materializedView.setActive(true);
        } else if (AlterMaterializedViewStmt.INACTIVE.equalsIgnoreCase(status)) {
            materializedView.setActive(false);
        }
    }

    private void processModifyTableProperties(ModifyTablePropertiesClause modifyTablePropertiesClause,
                                              Database db,
                                              MaterializedView materializedView) throws AnalysisException {
        Map<String, String> properties = modifyTablePropertiesClause.getProperties();
        Map<String, String> propClone = Maps.newHashMap();
        propClone.putAll(properties);
        int partitionTTL = INVALID;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)) {
            partitionTTL = PropertyAnalyzer.analyzePartitionTimeToLive(properties);
        }
        int partitionRefreshNumber = INVALID;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)) {
            partitionRefreshNumber = PropertyAnalyzer.analyzePartitionRefreshNumber(properties);
        }
        int autoRefreshPartitionsLimit = INVALID;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)) {
            autoRefreshPartitionsLimit = PropertyAnalyzer.analyzeAutoRefreshPartitionsLimit(properties, materializedView);
        }
        List<TableName> excludedTriggerTables = Lists.newArrayList();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            excludedTriggerTables = PropertyAnalyzer.analyzeExcludedTriggerTables(properties, materializedView);
        }
        int maxMVRewriteStaleness = INVALID;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
            maxMVRewriteStaleness = PropertyAnalyzer.analyzeMVRewriteStaleness(properties);
        }
        List<UniqueConstraint> uniqueConstraints = Lists.newArrayList();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
            uniqueConstraints = PropertyAnalyzer.analyzeUniqueConstraint(properties, db, materializedView);
            properties.remove(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT);
        }
        List<ForeignKeyConstraint> foreignKeyConstraints = Lists.newArrayList();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
            foreignKeyConstraints = PropertyAnalyzer.analyzeForeignKeyConstraint(properties, db, materializedView);
            properties.remove(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT);
        }

        if (!properties.isEmpty()) {
            // analyze properties
            List<SetListItem> setListItems = Lists.newArrayList();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (!entry.getKey().startsWith(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX)) {
                    throw new AnalysisException("Modify failed because unknown properties: " + properties +
                            ", please add `session.` prefix if you want add session variables for mv(" +
                            "eg, \"session.query_timeout\"=\"30000000\").");
                }
                String varKey = entry.getKey().substring(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX.length());
                SystemVariable variable = new SystemVariable(varKey, new StringLiteral(entry.getValue()));
                setListItems.add(variable);
            }
            SetStmtAnalyzer.analyze(new SetStmt(setListItems), null);
        }

        boolean isChanged = false;
        Map<String, String> curProp = materializedView.getTableProperty().getProperties();
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER) &&
                materializedView.getTableProperty().getPartitionTTLNumber() != partitionTTL) {
            curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER, String.valueOf(partitionTTL));
            materializedView.getTableProperty().setPartitionTTLNumber(partitionTTL);
            isChanged = true;
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER) &&
                materializedView.getTableProperty().getPartitionRefreshNumber() != partitionRefreshNumber) {
            curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, String.valueOf(partitionRefreshNumber));
            materializedView.getTableProperty().setPartitionRefreshNumber(partitionRefreshNumber);
            isChanged = true;
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT) &&
                materializedView.getTableProperty().getAutoRefreshPartitionsLimit() != autoRefreshPartitionsLimit) {
            curProp.put(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT, String.valueOf(autoRefreshPartitionsLimit));
            materializedView.getTableProperty().setAutoRefreshPartitionsLimit(autoRefreshPartitionsLimit);
            isChanged = true;
        }
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            curProp.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES,
                    propClone.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES));
            materializedView.getTableProperty().setExcludedTriggerTables(excludedTriggerTables);
            isChanged = true;
        }
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
            materializedView.setUniqueConstraints(uniqueConstraints);
            isChanged = true;
        }
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
            materializedView.setForeignKeyConstraints(foreignKeyConstraints);
            isChanged = true;
        }
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
            curProp.put(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND,
                    propClone.get(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND));
            materializedView.setMaxMVRewriteStaleness(maxMVRewriteStaleness);
            isChanged = true;
        }
        DynamicPartitionUtil.registerOrRemovePartitionTTLTable(materializedView.getDbId(), materializedView);
        if (!properties.isEmpty()) {
            // set properties if there are no exceptions
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                materializedView.getTableProperty().modifyTableProperties(entry.getKey(), entry.getValue());
            }
            isChanged = true;
        }

        if (isChanged) {
            ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(materializedView.getDbId(),
                    materializedView.getId(), propClone);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMaterializedViewProperties(log);
        }
        LOG.info("alter materialized view properties {}, id: {}", propClone, materializedView.getId());
    }

    private void processChangeRefreshScheme(RefreshSchemeDesc refreshSchemeDesc, MaterializedView materializedView,
                                            String dbName) throws DdlException {
        MaterializedView.RefreshType newRefreshType = refreshSchemeDesc.getType();
        MaterializedView.RefreshType oldRefreshType = materializedView.getRefreshScheme().getType();

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task currentTask = taskManager.getTask(TaskBuilder.getMvTaskName(materializedView.getId()));
        Task task;
        if (currentTask == null) {
            task = TaskBuilder.buildMvTask(materializedView, dbName);
            TaskBuilder.updateTaskInfo(task, refreshSchemeDesc, materializedView);
            taskManager.createTask(task, false);
        } else {
            Task changedTask = TaskBuilder.rebuildMvTask(materializedView, dbName, currentTask.getProperties());
            TaskBuilder.updateTaskInfo(changedTask, refreshSchemeDesc, materializedView);
            taskManager.alterTask(currentTask, changedTask, false);
            task = currentTask;
        }

        // for event triggered type, run task
        if (task.getType() == Constants.TaskType.EVENT_TRIGGERED) {
            taskManager.executeTask(task.getName());
        }

        final MaterializedView.MvRefreshScheme refreshScheme = materializedView.getRefreshScheme();
        refreshScheme.setType(newRefreshType);
        if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            IntervalLiteral intervalLiteral = asyncRefreshSchemeDesc.getIntervalLiteral();
            if (intervalLiteral != null) {
                final IntLiteral step = (IntLiteral) intervalLiteral.getValue();
                final MaterializedView.AsyncRefreshContext asyncRefreshContext = refreshScheme.getAsyncRefreshContext();
                if (asyncRefreshSchemeDesc.isDefineStartTime()) {
                    asyncRefreshContext.setStartTime(Utils.getLongFromDateTime(asyncRefreshSchemeDesc.getStartTime()));
                }
                asyncRefreshContext.setStep(step.getLongValue());
                asyncRefreshContext.setTimeUnit(intervalLiteral.getUnitIdentifier().getDescription());
            } else {
                if (materializedView.getBaseTableInfos().stream().anyMatch(tableInfo ->
                        !tableInfo.getTable().isNativeTableOrMaterializedView()
                )) {
                    throw new DdlException("Materialized view which type is ASYNC need to specify refresh interval for " +
                            "external table");
                }
                refreshScheme.setAsyncRefreshContext(new MaterializedView.AsyncRefreshContext());
            }
        }

        final ChangeMaterializedViewRefreshSchemeLog log = new ChangeMaterializedViewRefreshSchemeLog(materializedView);
        GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(log);
        LOG.info("change materialized view refresh type {} to {}, id: {}", oldRefreshType,
                newRefreshType, materializedView.getId());
    }

    private void processRenameMaterializedView(String oldMvName, String newMvName, Database db,
                                               MaterializedView materializedView) throws DdlException {
        if (db.getTable(newMvName) != null) {
            throw new DdlException("Materialized view [" + newMvName + "] is already used");
        }
        materializedView.setName(newMvName);
        db.dropTable(oldMvName);
        db.createTable(materializedView);
        final RenameMaterializedViewLog renameMaterializedViewLog =
                new RenameMaterializedViewLog(materializedView.getId(), db.getId(), newMvName);
        updateTaskDefinition(materializedView);
        GlobalStateMgr.getCurrentState().getEditLog().logMvRename(renameMaterializedViewLog);
        LOG.info("rename materialized view[{}] to {}, id: {}", oldMvName, newMvName, materializedView.getId());
    }

    public void replayRenameMaterializedView(RenameMaterializedViewLog log) {
        long dbId = log.getDbId();
        long materializedViewId = log.getId();
        String newMaterializedViewName = log.getNewMaterializedViewName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        MaterializedView oldMaterializedView = (MaterializedView) db.getTable(materializedViewId);
        db.dropTable(oldMaterializedView.getName());
        oldMaterializedView.setName(newMaterializedViewName);
        db.createTable(oldMaterializedView);
        updateTaskDefinition(oldMaterializedView);
        LOG.info("Replay rename materialized view [{}] to {}, id: {}", oldMaterializedView.getName(),
                newMaterializedViewName, oldMaterializedView.getId());
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
        db.writeLock();
        try {
            MaterializedView oldMaterializedView;
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

            long maxChangedTableRefreshTime = log.getAsyncRefreshContext().getBaseTableVisibleVersionMap().values().stream()
                    .map(x -> x.values().stream().map(
                            MaterializedView.BasePartitionInfo::getLastRefreshTime).max(Long::compareTo))
                    .map(x -> x.orElse(null)).filter(Objects::nonNull)
                    .max(Long::compareTo)
                    .orElse(System.currentTimeMillis());
            newMvRefreshScheme.setLastRefreshTime(maxChangedTableRefreshTime);
            oldMaterializedView.setRefreshScheme(newMvRefreshScheme);
            LOG.info(
                    "Replay materialized view [{}]'s refresh type to {}, start time to {}, " +
                            "interval step to {}, timeunit to {}, id: {}",
                    oldMaterializedView.getName(), refreshType.name(), asyncRefreshContext.getStartTime(),
                    asyncRefreshContext.getStep(),
                    asyncRefreshContext.getTimeUnit(), oldMaterializedView.getId());
        } finally {
            db.writeUnlock();
        }
    }

    public void replayAlterMaterializedViewProperties(short opCode, ModifyTablePropertyOperationLog log) {
        long dbId = log.getDbId();
        long tableId = log.getTableId();
        Map<String, String> properties = log.getProperties();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        db.writeLock();
        try {
            MaterializedView mv = (MaterializedView) db.getTable(tableId);
            TableProperty tableProperty = mv.getTableProperty();
            if (tableProperty == null) {
                tableProperty = new TableProperty(properties);
                mv.setTableProperty(tableProperty.buildProperty(opCode));
            } else {
                tableProperty.modifyTableProperties(properties);
                tableProperty.buildProperty(opCode);
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void replayAlterMaterializedViewStatus(AlterMaterializedViewStatusLog log) {
        long dbId = log.getDbId();
        long tableId = log.getTableId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        db.writeLock();
        try {
            MaterializedView mv = (MaterializedView) db.getTable(tableId);
            processChangeMaterializedViewStatus(mv, log.getStatus(), true);
        } finally {
            db.writeUnlock();
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
            GlobalStateMgr.getCurrentSystemInfo().checkClusterCapacity();
            db.checkQuota();
        }

        // some operations will take long time to process, need to be done outside the databse lock
        boolean needProcessOutsideDatabaseLock = false;
        String tableName = dbTableName.getTbl();

        boolean isSynchronous = true;
        db.writeLock();
        OlapTable olapTable;
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (!table.isOlapOrCloudNativeTable()) {
                throw new DdlException("Do not support alter non-native table[" + tableName + "]");
            }
            olapTable = (OlapTable) table;

            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("The state of \"" + table.getName() + "\" is " + olapTable.getState().name()
                                       + ". Alter operation is only permitted if NORMAL");
            }

            if (currentAlterOps.hasSchemaChangeOp()) {
                // if modify storage type to v2, do schema change to convert all related tablets to segment v2 format
                schemaChangeHandler.process(alterClauses, db, olapTable);
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
                    GlobalStateMgr.getCurrentState().dropPartition(db, olapTable, dropPartitionClause);
                } else if (alterClause instanceof ReplacePartitionClause) {
                    ReplacePartitionClause replacePartitionClause = (ReplacePartitionClause) alterClause;
                    List<String> partitionNames = replacePartitionClause.getPartitionNames();
                    for (String partitionName : partitionNames) {
                        if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                            throw new DdlException("Replace shadow partitions is not allowed");
                        }
                    }
                    GlobalStateMgr.getCurrentState()
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
                processSwap(db, olapTable, alterClauses);
            } else if (currentAlterOps.contains(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC)) {
                needProcessOutsideDatabaseLock = true;
            } else {
                throw new DdlException("Invalid alter operations: " + currentAlterOps);
            }
        } finally {
            db.writeUnlock();
        }

        // the following ops should done outside db lock. because it contain synchronized create operation
        if (needProcessOutsideDatabaseLock) {
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            if (alterClause instanceof AddPartitionClause) {
                if (!((AddPartitionClause) alterClause).isTempPartition()) {
                    DynamicPartitionUtil.checkAlterAllowed((OlapTable) db.getTable(tableName));
                }
                GlobalStateMgr.getCurrentState().addPartitions(db, tableName, (AddPartitionClause) alterClause);
            } else if (alterClause instanceof TruncatePartitionClause) {
                // This logic is use to adapt mysql syntax.
                // ALTER TABLE test TRUNCATE PARTITION p1;
                TruncatePartitionClause clause = (TruncatePartitionClause) alterClause;
                TableRef tableRef = new TableRef(stmt.getTbl(), null, clause.getPartitionNames());
                TruncateTableStmt tStmt = new TruncateTableStmt(tableRef);
                GlobalStateMgr.getCurrentState().truncateTable(tStmt);
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

                db.writeLock();
                try {
                    modifyPartitionsProperty(db, olapTable, partitionNames, properties);
                } finally {
                    db.writeUnlock();
                }
            } else if (alterClause instanceof ModifyTablePropertiesClause) {
                Map<String, String> properties = alterClause.getProperties();
                Preconditions.checkState(properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT) ||
                        properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT));

                olapTable = (OlapTable) db.getTable(tableName);
                if (olapTable.isCloudNativeTable()) {
                    throw new DdlException("Lake table not support alter in_memory or enable_persistent_index or write_quorum");
                }

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
            }
        }

        if (isSynchronous) {
            olapTable.lastSchemaUpdateTime.set(System.currentTimeMillis());
        }
    }

    // entry of processing swap table
    private void processSwap(Database db, OlapTable origTable, List<AlterClause> alterClauses) throws UserException {
        if (!(alterClauses.get(0) instanceof SwapTableClause)) {
            throw new DdlException("swap operation only support table");
        }

        // must hold db write lock
        Preconditions.checkState(db.isWriteLockHeldByCurrentThread());

        SwapTableClause clause = (SwapTableClause) alterClauses.get(0);
        String origTblName = origTable.getName();
        String newTblName = clause.getTblName();
        Table newTbl = db.getTable(newTblName);
        if (newTbl == null || !newTbl.isOlapOrCloudNativeTable()) {
            throw new DdlException("Table " + newTblName + " does not exist or is not OLAP/LAKE table");
        }
        OlapTable olapNewTbl = (OlapTable) newTbl;

        // First, we need to check whether the table to be operated on can be renamed
        olapNewTbl.checkAndSetName(origTblName, true);
        origTable.checkAndSetName(newTblName, true);

        swapTableInternal(db, origTable, olapNewTbl);

        // write edit log
        SwapTableOperationLog log = new SwapTableOperationLog(db.getId(), origTable.getId(), olapNewTbl.getId());
        GlobalStateMgr.getCurrentState().getEditLog().logSwapTable(log);

        LOG.info("finish swap table {}-{} with table {}-{}", origTable.getId(), origTblName, newTbl.getId(),
                newTblName);
    }

    public void replaySwapTable(SwapTableOperationLog log) {
        long dbId = log.getDbId();
        long origTblId = log.getOrigTblId();
        long newTblId = log.getNewTblId();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        OlapTable origTable = (OlapTable) db.getTable(origTblId);
        OlapTable newTbl = (OlapTable) db.getTable(newTblId);

        try {
            swapTableInternal(db, origTable, newTbl);
        } catch (DdlException e) {
            LOG.warn("should not happen", e);
        }
        LOG.debug("finish replay swap table {}-{} with table {}-{}", origTblId, origTable.getName(), newTblId,
                newTbl.getName());
    }

    /**
     * The swap table operation works as follow:
     * For example, SWAP TABLE A WITH TABLE B.
     * must pre check A can be renamed to B and B can be renamed to A
     */
    private void swapTableInternal(Database db, OlapTable origTable, OlapTable newTbl)
            throws DdlException {
        String origTblName = origTable.getName();
        String newTblName = newTbl.getName();

        // drop origin table and new table
        db.dropTable(origTblName);
        db.dropTable(newTblName);

        // rename new table name to origin table name and add it to database
        newTbl.checkAndSetName(origTblName, false);
        db.createTable(newTbl);

        // rename origin table name to new table name and add it to database
        origTable.checkAndSetName(newTblName, false);
        db.createTable(origTable);
    }

    public void processAlterView(AlterViewStmt stmt, ConnectContext ctx) throws UserException {
        TableName dbTableName = stmt.getTableName();
        String dbName = dbTableName.getDb();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = dbTableName.getTbl();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (table.getType() != TableType.VIEW) {
                throw new DdlException("The specified table [" + tableName + "] is not a view");
            }


            AlterViewClause alterViewClause = (AlterViewClause) stmt.getAlterClause();
            String inlineViewDef = alterViewClause.getInlineViewDef();
            List<Column> newFullSchema = alterViewClause.getColumns();
            long sqlMode = ctx.getSessionVariable().getSqlMode();

            View view = (View) table;
            String viewName = view.getName();

            view.setInlineViewDefWithSqlMode(inlineViewDef, ctx.getSessionVariable().getSqlMode());
            try {
                view.init();
            } catch (UserException e) {
                throw new DdlException("failed to init view stmt", e);
            }
            view.setNewFullSchema(newFullSchema);

            db.dropTable(viewName);
            db.createTable(view);

            AlterViewInfo alterViewInfo = new AlterViewInfo(db.getId(), view.getId(), inlineViewDef, newFullSchema, sqlMode);
            GlobalStateMgr.getCurrentState().getEditLog().logModifyViewDef(alterViewInfo);
            LOG.info("modify view[{}] definition to {}", viewName, inlineViewDef);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayModifyViewDef(AlterViewInfo alterViewInfo) throws DdlException {
        long dbId = alterViewInfo.getDbId();
        long tableId = alterViewInfo.getTableId();
        String inlineViewDef = alterViewInfo.getInlineViewDef();
        List<Column> newFullSchema = alterViewInfo.getNewFullSchema();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        db.writeLock();
        try {
            View view = (View) db.getTable(tableId);
            String viewName = view.getName();
            view.setInlineViewDefWithSqlMode(inlineViewDef, alterViewInfo.getSqlMode());
            try {
                view.init();
            } catch (UserException e) {
                throw new DdlException("failed to init view stmt", e);
            }
            view.setNewFullSchema(newFullSchema);

            db.dropTable(viewName);
            db.createTable(view);

            LOG.info("replay modify view[{}] definition to {}", viewName, inlineViewDef);
        } finally {
            db.writeUnlock();
        }
    }

    public ShowResultSet processAlterCluster(AlterSystemStmt stmt) throws UserException {
        return clusterHandler.process(Collections.singletonList(stmt.getAlterClause()), null, null);
    }

    private void processRename(Database db, OlapTable table, List<AlterClause> alterClauses) throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof TableRenameClause) {
                GlobalStateMgr.getCurrentState().renameTable(db, table, (TableRenameClause) alterClause);
                break;
            } else if (alterClause instanceof RollupRenameClause) {
                GlobalStateMgr.getCurrentState().renameRollup(db, table, (RollupRenameClause) alterClause);
                break;
            } else if (alterClause instanceof PartitionRenameClause) {
                PartitionRenameClause partitionRenameClause = (PartitionRenameClause) alterClause;
                if (partitionRenameClause.getPartitionName()
                        .startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                    throw new DdlException("Rename of shadow partitions is not allowed");
                }
                GlobalStateMgr.getCurrentState().renamePartition(db, table, partitionRenameClause);
                break;
            } else if (alterClause instanceof ColumnRenameClause) {
                GlobalStateMgr.getCurrentState().renameColumn(db, table, (ColumnRenameClause) alterClause);
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
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentColocateIndex();
        List<ModifyPartitionInfo> modifyPartitionInfos = Lists.newArrayList();
        if (olapTable.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s state is not NORMAL");
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

        // get value from properties here
        // 1. data property
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
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        for (String partitionName : partitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            // 1. date property
            if (newDataProperty != null) {
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
        db.writeLock();
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
            db.writeUnlock();
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
