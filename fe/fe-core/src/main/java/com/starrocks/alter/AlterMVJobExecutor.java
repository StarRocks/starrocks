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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.RenameMaterializedViewLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.VariableMgr;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.lang3.StringUtils;
import org.threeten.extra.PeriodDuration;

import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.TableProperty.INVALID;

public class AlterMVJobExecutor extends AlterJobExecutor {
    @Override
    public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
        String newMvName = clause.getNewTableName();
        String oldMvName = table.getName();

        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), newMvName) != null) {
            throw new SemanticException("Materialized view [" + newMvName + "] is already used");
        }
        table.setName(newMvName);
        db.dropTable(oldMvName);
        db.registerTableUnlocked(table);
        final RenameMaterializedViewLog renameMaterializedViewLog =
                new RenameMaterializedViewLog(table.getId(), db.getId(), newMvName);
        updateTaskDefinition((MaterializedView) table);
        GlobalStateMgr.getCurrentState().getEditLog().logMvRename(renameMaterializedViewLog);
        LOG.info("rename materialized view[{}] to {}, id: {}", oldMvName, newMvName, table.getId());
        return null;
    }

    @Override
    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause modifyTablePropertiesClause,
                                                 ConnectContext context) {
        MaterializedView materializedView = (MaterializedView) table;

        Map<String, String> properties = modifyTablePropertiesClause.getProperties();
        Map<String, String> propClone = Maps.newHashMap();
        propClone.putAll(properties);
        int partitionTTL = INVALID;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)) {
            partitionTTL = PropertyAnalyzer.analyzePartitionTTLNumber(properties);
        }
        Pair<String, PeriodDuration> ttlDuration = null;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
            ttlDuration = PropertyAnalyzer.analyzePartitionTTL(properties);
        }
        int partitionRefreshNumber = INVALID;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)) {
            partitionRefreshNumber = PropertyAnalyzer.analyzePartitionRefreshNumber(properties);
        }
        String resourceGroup = null;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP)) {
            resourceGroup = PropertyAnalyzer.analyzeResourceGroup(properties);
            properties.remove(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP);
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
        TableProperty.QueryRewriteConsistencyMode oldExternalQueryRewriteConsistencyMode =
                materializedView.getTableProperty().getForceExternalTableQueryRewrite();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)) {
            String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
            oldExternalQueryRewriteConsistencyMode = TableProperty.analyzeExternalTableQueryRewrite(propertyValue);
            properties.remove(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
        }
        TableProperty.QueryRewriteConsistencyMode oldQueryRewriteConsistencyMode =
                materializedView.getTableProperty().getQueryRewriteConsistencyMode();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY)) {
            String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY);
            oldQueryRewriteConsistencyMode = TableProperty.analyzeQueryRewriteMode(propertyValue);
            properties.remove(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY);
        }
        TableProperty.MVQueryRewriteSwitch queryRewriteSwitch =
                materializedView.getTableProperty().getMvQueryRewriteSwitch();
        if (properties.containsKey(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE)) {
            String value = properties.get(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE);
            queryRewriteSwitch = TableProperty.analyzeQueryRewriteSwitch(value);
            properties.remove(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE);
        }
        TableProperty.MVTransparentRewriteMode mvTransparentRewriteMode =
                materializedView.getTableProperty().getMvTransparentRewriteMode();
        if (properties.containsKey(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE)) {
            String value = properties.get(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE);
            mvTransparentRewriteMode = TableProperty.analyzeMVTransparentRewrite(value);
            properties.remove(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE);
        }

        // warehouse
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            String warehouseName = properties.remove(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
            materializedView.setWarehouseId(warehouse.getId());
        }

        // labels.location
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION)) {
            if (!materializedView.isCloudNativeMaterializedView()) {
                PropertyAnalyzer.analyzeLocation(materializedView, properties);
            }
        }

        if (!properties.isEmpty()) {
            if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
                throw new SemanticException("Modify failed because unsupported properties: " +
                        "colocate group is not supported for materialized view");
            }
            // analyze properties
            List<SetListItem> setListItems = Lists.newArrayList();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (!entry.getKey().startsWith(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX)) {
                    throw new SemanticException("Modify failed because unknown properties: " + properties +
                            ", please add `session.` prefix if you want add session variables for mv(" +
                            "eg, \"session.query_timeout\"=\"30000000\").");
                }
                String varKey = entry.getKey().substring(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX.length());
                SystemVariable variable = new SystemVariable(varKey, new StringLiteral(entry.getValue()));
                try {
                    VariableMgr.checkSystemVariableExist(variable);
                } catch (DdlException e) {
                    throw new SemanticException(e.getMessage());
                }
                setListItems.add(variable);
            }
            SetStmtAnalyzer.analyze(new SetStmt(setListItems), null);
        }

        // TODO(murphy) refactor the code
        boolean isChanged = false;
        Map<String, String> curProp = materializedView.getTableProperty().getProperties();
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL) && ttlDuration != null &&
                !materializedView.getTableProperty().getPartitionTTL().equals(ttlDuration.second)) {
            curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, ttlDuration.first);
            materializedView.getTableProperty().setPartitionTTL(ttlDuration.second);
            isChanged = true;
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER) &&
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
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP) &&
                !StringUtils.equals(materializedView.getTableProperty().getResourceGroup(), resourceGroup)) {
            if (resourceGroup != null && !resourceGroup.isEmpty() &&
                    GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(resourceGroup) == null) {
                throw new SemanticException(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP
                        + " " + resourceGroup + " does not exist.");
            }
            curProp.put(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP, resourceGroup);
            materializedView.getTableProperty().setResourceGroup(resourceGroup);
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
            // get the updated foreign key constraint from table property.
            // for external table, create time is added into FOREIGN_KEY_CONSTRAINT
            Map<String, String> mvProperties = materializedView.getTableProperty().getProperties();
            String foreignKeys = mvProperties.get(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT);
            propClone.put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, foreignKeys);
            isChanged = true;
        }
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
            curProp.put(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND,
                    propClone.get(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND));
            materializedView.setMaxMVRewriteStaleness(maxMVRewriteStaleness);
            isChanged = true;
        }
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)) {
            materializedView.getTableProperty().getProperties().
                    put(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE,
                            String.valueOf(oldExternalQueryRewriteConsistencyMode));
            materializedView.getTableProperty().setForceExternalTableQueryRewrite(oldExternalQueryRewriteConsistencyMode);
            isChanged = true;
        }
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY)) {
            materializedView.getTableProperty().getProperties().
                    put(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY,
                            String.valueOf(oldQueryRewriteConsistencyMode));
            materializedView.getTableProperty().setQueryRewriteConsistencyMode(oldQueryRewriteConsistencyMode);
            isChanged = true;
        }
        // enable_query_rewrite
        if (propClone.containsKey(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE)) {
            materializedView.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, String.valueOf(queryRewriteSwitch));
            materializedView.getTableProperty().setMvQueryRewriteSwitch(queryRewriteSwitch);
            if (!materializedView.isEnableRewrite()) {
                // invalidate caches for mv rewrite when disable mv rewrite.
                CachingMvPlanContextBuilder.getInstance().invalidateFromCache(materializedView, false);
            } else {
                CachingMvPlanContextBuilder.getInstance().putAstIfAbsent(materializedView);
            }
            isChanged = true;
        }
        // transparent_mv_rewrite_mode
        if (propClone.containsKey(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE)) {
            materializedView.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE, String.valueOf(mvTransparentRewriteMode));
            materializedView.getTableProperty().setMvTransparentRewriteMode(mvTransparentRewriteMode);
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
        return null;
    }

    @Override
    public Void visitRefreshSchemeClause(RefreshSchemeClause refreshSchemeDesc, ConnectContext context) {
        try {
            MaterializedView materializedView = (MaterializedView) table;
            String dbName = db.getFullName();

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
                Task changedTask = TaskBuilder.rebuildMvTask(materializedView, dbName, currentTask.getProperties(),
                        currentTask);
                TaskBuilder.updateTaskInfo(changedTask, refreshSchemeDesc, materializedView);
                taskManager.alterTask(currentTask, changedTask, false);
                task = currentTask;
            }

            // for event triggered type, run task
            if (task.getType() == Constants.TaskType.EVENT_TRIGGERED) {
                taskManager.executeTask(task.getName(), ExecuteOption.makeMergeRedundantOption());
            }

            final MaterializedView.MvRefreshScheme refreshScheme = materializedView.getRefreshScheme();
            Locker locker = new Locker();
            if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
                throw new DmlException("update meta failed. database:" + db.getFullName() + " not exist");
            }
            try {
                // check
                Table mv = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), materializedView.getId());
                if (mv == null) {
                    throw new DmlException(
                            "update meta failed. materialized view:" + materializedView.getName() + " not exist");
                }
                refreshScheme.setType(newRefreshType);
                if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
                    AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
                    IntervalLiteral intervalLiteral = asyncRefreshSchemeDesc.getIntervalLiteral();
                    if (intervalLiteral != null) {
                        final IntLiteral step = (IntLiteral) intervalLiteral.getValue();
                        final MaterializedView.AsyncRefreshContext asyncRefreshContext = refreshScheme.getAsyncRefreshContext();
                        asyncRefreshContext.setStartTime(
                                Utils.getLongFromDateTime(asyncRefreshSchemeDesc.getStartTime()));
                        asyncRefreshContext.setDefineStartTime(asyncRefreshSchemeDesc.isDefineStartTime());
                        asyncRefreshContext.setStep(step.getLongValue());
                        asyncRefreshContext.setTimeUnit(intervalLiteral.getUnitIdentifier().getDescription());
                    } else {
                        if (materializedView.getBaseTableInfos().stream().anyMatch(tableInfo ->
                                !MvUtils.getTableChecked(tableInfo).isNativeTableOrMaterializedView()
                        )) {
                            throw new DdlException("Materialized view which type is ASYNC need to specify refresh interval for " +
                                    "external table");
                        }
                        refreshScheme.setAsyncRefreshContext(new MaterializedView.AsyncRefreshContext());
                    }
                }

                final ChangeMaterializedViewRefreshSchemeLog log = new ChangeMaterializedViewRefreshSchemeLog(materializedView);
                GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(log);
            } finally {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }
            LOG.info("change materialized view refresh type {} to {}, id: {}", oldRefreshType,
                    newRefreshType, materializedView.getId());
            return null;
        } catch (DdlException e) {
            throw new AlterJobException(e.getMessage(), e);
        }
    }

    @Override
    public Void visitAlterMaterializedViewStatusClause(AlterMaterializedViewStatusClause clause, ConnectContext context) {
        String status = clause.getStatus();
        MaterializedView materializedView = (MaterializedView) table;
        String dbName = db.getFullName();

        try {
            if (AlterMaterializedViewStatusClause.ACTIVE.equalsIgnoreCase(status)) {
                materializedView.fixRelationship();
                if (materializedView.isActive()) {
                    return null;
                }

                GlobalStateMgr.getCurrentState().getAlterJobMgr().
                        alterMaterializedViewStatus(materializedView, status, false);
                // for manual refresh type, do not refresh
                if (materializedView.getRefreshScheme().getType() != MaterializedView.RefreshType.MANUAL) {
                    GlobalStateMgr.getCurrentState().getStarRocksMeta()
                            .refreshMaterializedView(dbName, materializedView.getName(), true, null,
                                    Constants.TaskRunPriority.NORMAL.value(), true, false);
                }
            } else if (AlterMaterializedViewStatusClause.INACTIVE.equalsIgnoreCase(status)) {
                if (!materializedView.isActive()) {
                    return null;
                }
                LOG.warn("Setting the materialized view {}({}) to inactive because " +
                                "user use alter materialized view set status to inactive",
                        materializedView.getName(), materializedView.getId());
                GlobalStateMgr.getCurrentState().getAlterJobMgr().
                        alterMaterializedViewStatus(materializedView, status, false);
            } else {
                throw new AlterJobException("Unsupported modification materialized view status:" + status);
            }
            AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(materializedView.getDbId(),
                    materializedView.getId(), status);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMvStatus(log);
            return null;
        } catch (DdlException | MetaNotFoundException e) {
            throw new AlterJobException(e.getMessage(), e);
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
}
