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
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
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
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
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
import com.starrocks.sql.optimizer.MvPlanContextBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.threeten.extra.PeriodDuration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.alter.AlterJobMgr.MANUAL_INACTIVE_MV_REASON;
import static com.starrocks.common.util.PropertyAnalyzer.analyzeLocation;
import static com.starrocks.common.util.PropertyAnalyzer.getExcludeString;

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
        final MaterializedView mv = (MaterializedView) table;
        final Map<String, String> properties = modifyTablePropertiesClause.getProperties();
        final Map<String, String> propClone = Maps.newHashMap(properties);
        final Map<String, String> curProp = mv.getTableProperty().getProperties();
        // NOTE: multi properties can be changed once so only trigger to actions if all properties have been parsed successfully.
        final List<Runnable> actions = Lists.newArrayList();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)) {
            int partitionTTL = PropertyAnalyzer.analyzePartitionTTLNumber(properties);
            if (mv.getTableProperty().getPartitionTTLNumber() != partitionTTL) {
                if (!mv.getPartitionInfo().isRangePartition()) {
                    throw new SemanticException("partition_ttl_number is only supported for range partitioned materialized view");
                }
                actions.add(() -> {
                    curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER, String.valueOf(partitionTTL));
                    mv.getTableProperty().setPartitionTTLNumber(partitionTTL);
                });
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
            Pair<String, PeriodDuration> ttlDuration = PropertyAnalyzer.analyzePartitionTTL(properties, true);
            if (ttlDuration != null &&
                    !mv.getTableProperty().getPartitionTTL().equals(ttlDuration.second)) {
                if (!mv.getPartitionInfo().isRangePartition()) {
                    throw new SemanticException("partition_ttl is only supported for range partitioned materialized view");
                }
                actions.add(() -> {
                    curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, ttlDuration.first);
                    mv.getTableProperty().setPartitionTTL(ttlDuration.second);
                });
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION)) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());
            TableName mvTableName = new TableName(db.getFullName(), mv.getName());
            Map<Expr, Expr> mvPartitionByExprToAdjustMap =
                    MaterializedViewAnalyzer.getMVPartitionByExprToAdjustMap(mvTableName, mv);
            String ttlRetentionCondition = PropertyAnalyzer.analyzePartitionRetentionCondition(db,
                    mv, properties, true, mvPartitionByExprToAdjustMap);
            if (ttlRetentionCondition != null &&
                    !ttlRetentionCondition.equalsIgnoreCase(mv.getTableProperty().getPartitionRetentionCondition())) {
                actions.add(() -> {
                    curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION, ttlRetentionCondition);
                    mv.getTableProperty().setPartitionRetentionCondition(ttlRetentionCondition);
                    // re-analyze mv retention condition
                    mv.analyzeMVRetentionCondition(context);
                });
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT)) {
            String timeDriftConstraintSpec = properties.get(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT);
            PropertyAnalyzer.analyzeTimeDriftConstraint(timeDriftConstraintSpec, mv, properties);
            if (timeDriftConstraintSpec != null && !timeDriftConstraintSpec.equalsIgnoreCase(
                    mv.getTableProperty().getTimeDriftConstraintSpec())) {
                actions.add(() -> {
                    curProp.put(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, timeDriftConstraintSpec);
                    mv.getTableProperty().setTimeDriftConstraintSpec(timeDriftConstraintSpec);
                });
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)) {
            int partitionRefreshNumber = PropertyAnalyzer.analyzePartitionRefreshNumber(properties);
            actions.add(() -> {
                curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, String.valueOf(partitionRefreshNumber));
                mv.getTableProperty().setPartitionRefreshNumber(partitionRefreshNumber);
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY)) {
            String partitionRefreshStrategy = PropertyAnalyzer.analyzePartitionRefreshStrategy(properties);
            if (!mv.getTableProperty().getPartitionRefreshStrategy().equals(partitionRefreshStrategy)) {
                actions.add(() -> {
                    curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY, String.valueOf(partitionRefreshStrategy));
                    mv.getTableProperty().setPartitionRefreshStrategy(partitionRefreshStrategy);
                });
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP)) {
            String resourceGroup = PropertyAnalyzer.analyzeResourceGroup(properties);
            properties.remove(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP);
            if (!StringUtils.equals(mv.getTableProperty().getResourceGroup(), resourceGroup)) {
                if (resourceGroup != null && !resourceGroup.isEmpty() &&
                        GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(resourceGroup) == null) {
                    throw new SemanticException(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP
                            + " " + resourceGroup + " does not exist.");
                }
                actions.add(() -> {
                    curProp.put(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP, resourceGroup);
                    mv.getTableProperty().setResourceGroup(resourceGroup);
                });
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)) {
            int autoRefreshPartitionsLimit = PropertyAnalyzer.analyzeAutoRefreshPartitionsLimit(properties, mv);
            if (mv.getTableProperty().getAutoRefreshPartitionsLimit() != autoRefreshPartitionsLimit) {
                actions.add(() -> {
                    curProp.put(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT,
                            String.valueOf(autoRefreshPartitionsLimit));
                    mv.getTableProperty().setAutoRefreshPartitionsLimit(autoRefreshPartitionsLimit);
                });
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            List<TableName> excludedTriggerTables = PropertyAnalyzer.analyzeExcludedTables(properties,
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, mv);
            actions.add(() -> {
                curProp.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES,
                        propClone.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES));
                mv.getTableProperty().setExcludedTriggerTables(excludedTriggerTables);
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES)) {
            List<TableName> excludedRefreshBaseTables = PropertyAnalyzer.analyzeExcludedTables(properties,
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES, mv);
            actions.add(() -> {
                curProp.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES,
                        propClone.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES));
                mv.getTableProperty().setExcludedRefreshTables(excludedRefreshBaseTables);
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
            int maxMVRewriteStaleness = PropertyAnalyzer.analyzeMVRewriteStaleness(properties);
            actions.add(() -> {
                curProp.put(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND,
                        propClone.get(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND));
                mv.setMaxMVRewriteStaleness(maxMVRewriteStaleness);
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
            List<UniqueConstraint> uniqueConstraints = PropertyAnalyzer.analyzeUniqueConstraint(properties, db, mv);
            properties.remove(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT);
            actions.add(() -> {
                mv.setUniqueConstraints(uniqueConstraints);
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
            final List<ForeignKeyConstraint> foreignKeyConstraints =
                    PropertyAnalyzer.analyzeForeignKeyConstraint(properties, db, mv);
            actions.add(() -> {
                mv.setForeignKeyConstraints(foreignKeyConstraints);
                // get the updated foreign key constraint from table property.
                // for external table, create time is added into FOREIGN_KEY_CONSTRAINT
                Map<String, String> mvProperties = mv.getTableProperty().getProperties();
                String foreignKeys = mvProperties.get(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT);
                propClone.put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, foreignKeys);
            });
            properties.remove(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)) {
            String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
            properties.remove(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
            actions.add(() -> {
                TableProperty.QueryRewriteConsistencyMode oldExternalQueryRewriteConsistencyMode =
                        TableProperty.analyzeExternalTableQueryRewrite(propertyValue);
                mv.getTableProperty().getProperties().
                        put(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE,
                                String.valueOf(oldExternalQueryRewriteConsistencyMode));
                mv.getTableProperty().setForceExternalTableQueryRewrite(oldExternalQueryRewriteConsistencyMode);
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY)) {
            String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY);
            TableProperty.QueryRewriteConsistencyMode oldQueryRewriteConsistencyMode =
                    TableProperty.analyzeQueryRewriteMode(propertyValue);
            properties.remove(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY);
            actions.add(() -> {
                mv.getTableProperty().getProperties().
                        put(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY,
                                String.valueOf(oldQueryRewriteConsistencyMode));
                mv.getTableProperty().setQueryRewriteConsistencyMode(oldQueryRewriteConsistencyMode);
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE)) {
            String value = properties.get(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE);
            TableProperty.MVQueryRewriteSwitch queryRewriteSwitch = TableProperty.analyzeQueryRewriteSwitch(value);
            properties.remove(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE);
            actions.add(() -> {
                mv.getTableProperty().getProperties()
                        .put(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, String.valueOf(queryRewriteSwitch));
                mv.getTableProperty().setMvQueryRewriteSwitch(queryRewriteSwitch);
                if (!mv.isEnableRewrite()) {
                    // invalidate caches for mv rewrite when disable mv rewrite.
                    CachingMvPlanContextBuilder.getInstance().updateMvPlanContextCache(mv, false);
                } else {
                    CachingMvPlanContextBuilder.getInstance().putAstIfAbsent(mv);
                }
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE)) {
            String value = properties.get(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE);
            TableProperty.MVTransparentRewriteMode mvTransparentRewriteMode = TableProperty.analyzeMVTransparentRewrite(value);
            properties.remove(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE);
            actions.add(() -> {
                mv.getTableProperty().getProperties()
                        .put(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE, String.valueOf(mvTransparentRewriteMode));
                mv.getTableProperty().setMvTransparentRewriteMode(mvTransparentRewriteMode);
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            // warehouse
            String warehouseName = properties.remove(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
            actions.add(() -> {
                mv.setWarehouseId(warehouse.getId());
            });
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION)) {
            // labels.location
            if (!mv.isCloudNativeMaterializedView()) {
                analyzeLocation(mv, properties);
            }
        }
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)) {
            List<Column> baseSchema = mv.getColumns();

            // analyze bloom filter columns
            Set<String> bfColumns = null;
            try {
                bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema,
                        mv.getKeysType() == KeysType.PRIMARY_KEYS);
            } catch (AnalysisException e) {
                throw new SemanticException("Failed to analyze bloom filter columns: " + e.getMessage());
            }
            if (bfColumns != null && bfColumns.isEmpty()) {
                bfColumns = null;
            }

            // analyze bloom filter fpp
            double bfFpp = 0;
            try {
                bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
            } catch (AnalysisException e) {
                throw new SemanticException("Failed to analyze bloom filter fpp: " + e.getMessage());
            }
            if (bfColumns != null && bfFpp == 0) {
                bfFpp = FeConstants.DEFAULT_BLOOM_FILTER_FPP;
            } else if (bfColumns == null) {
                bfFpp = 0;
            }

            Set<ColumnId> bfColumnIds;
            if (bfColumns != null && !bfColumns.isEmpty()) {
                bfColumnIds = Sets.newTreeSet(ColumnId.CASE_INSENSITIVE_ORDER);
                for (String colName : bfColumns) {
                    bfColumnIds.add(mv.getColumn(colName).getColumnId());
                }
            } else {
                bfColumnIds = null;
            }
            Set<ColumnId> oldBfColumnIds = mv.getBfColumnIds();
            if (bfColumnIds != null && oldBfColumnIds != null &&
                    bfColumnIds.equals(oldBfColumnIds) && mv.getBfFpp() == bfFpp) {
                // do nothing
            } else {
                double finalBfFpp = bfFpp;
                actions.add(() -> {
                    mv.setBloomFilterInfo(bfColumnIds, finalBfFpp);
                });
            }
            properties.remove(PropertyAnalyzer.PROPERTIES_BF_COLUMNS);
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
                            "eg, \"session.insert_timeout\"=\"30000000\").");
                }
                String varKey = entry.getKey().substring(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX.length());
                SystemVariable variable = new SystemVariable(varKey, new StringLiteral(entry.getValue()));
                try {
                    GlobalStateMgr.getCurrentState().getVariableMgr().checkSystemVariableExist(variable);
                } catch (DdlException e) {
                    throw new SemanticException(e.getMessage());
                }
                setListItems.add(variable);
            }
            SetStmtAnalyzer.analyze(new SetStmt(setListItems), null);
        }

        boolean isChanged = !actions.isEmpty();
        if (!properties.isEmpty()) {
            // set properties if there are no exceptions
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                mv.getTableProperty().modifyTableProperties(entry.getKey(), entry.getValue());
            }
            isChanged = true;
        }

        // if all properties are analyzed successfully, run actions
        actions.forEach(Runnable::run);

        // register or remove partition ttl table again after partition ttl has changed
        DynamicPartitionUtil.registerOrRemovePartitionTTLTable(mv.getDbId(), mv);
        if (isChanged) {
            ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(mv.getDbId(),
                    mv.getId(), propClone);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMaterializedViewProperties(log);
        }
        LOG.info("alter materialized view properties {}, id: {}", propClone, mv.getId());
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
                        alterMaterializedViewStatus(materializedView, status, "", false);
                // for manual refresh type, do not refresh
                if (materializedView.getRefreshScheme().getType() != MaterializedView.RefreshType.MANUAL) {
                    GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .refreshMaterializedView(dbName, materializedView.getName(), false, null,
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
                        alterMaterializedViewStatus(materializedView, status, MANUAL_INACTIVE_MV_REASON, false);
            } else {
                throw new AlterJobException("Unsupported modification materialized view status:" + status);
            }
            AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(materializedView.getDbId(),
                    materializedView.getId(), status, MANUAL_INACTIVE_MV_REASON);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMvStatus(log);
            return null;
        } catch (DdlException | MetaNotFoundException e) {
            throw new AlterJobException(e.getMessage(), e);
        }
    }

    private void updateTaskDefinition(MaterializedView mv) {
        Task currentTask = GlobalStateMgr.getCurrentState().getTaskManager().getTask(
                TaskBuilder.getMvTaskName(mv.getId()));
        if (currentTask != null) {
            currentTask.setDefinition(mv.getTaskDefinition());
            currentTask.setPostRun(TaskBuilder.getAnalyzeMVStmt(mv.getName()));
        }
    }

    /**
     * Inactive the materialized view and its related materialized views.
     *
     * NOTE:
     * 1. This method will clear all visible version map of the MV since for all schema changes, the MV should be
     * refreshed.
     * 2. User's inactive-mv command should not call this which will reserve the visible version map.
     */
    private static void doInactiveMaterializedView(MaterializedView mv, String reason) {
        // Only check this in leader and not replay to avoid duplicate inactive
        if (mv == null || !GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        LOG.warn("Inactive MV {}/{} because {}", mv.getName(), mv.getId(), reason);
        // inactive mv by reason
        if (mv.isActive()) {
            // log edit log
            String status = AlterMaterializedViewStatusClause.INACTIVE;
            GlobalStateMgr.getCurrentState().getAlterJobMgr().
                    alterMaterializedViewStatus(mv, status, reason, false);
            AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(mv.getDbId(),
                    mv.getId(), status, MANUAL_INACTIVE_MV_REASON);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMvStatus(log);
        } else {
            mv.setInactiveAndReason(reason);
        }
        // clear version map to make sure the MV will be refreshed
        mv.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMap();
        // recursive inactive
        inactiveRelatedMaterializedView(mv,
                MaterializedViewExceptions.inactiveReasonForBaseTableActive(mv.getName()), false);
    }

    /**
     * Inactive related materialized views because of base table/view is changed or dropped in the leader background.
     */
    public static void inactiveRelatedMaterializedView(Table olapTable, String reason, boolean isReplay) {
        if (!Config.enable_mv_automatic_inactive_by_base_table_changes) {
            LOG.warn("Skip to inactive related materialized views because of automatic inactive is disabled, " +
                    "table:{}, reason:{}", olapTable.getName(), reason);
            return;
        }
        // Only check this in leader and not replay to avoid duplicate inactive
        if (!GlobalStateMgr.getCurrentState().isLeader() || isReplay) {
            LOG.warn("Skip to inactive related materialized views because of base table/view {} is " +
                            "changed or dropped in the leader backgroud, isLeader: {}, isReplay, reason:{}",
                    olapTable.getName(), GlobalStateMgr.getCurrentState().isLeader(), isReplay, reason);
            return;
        }
        for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
            if (db == null) {
                LOG.warn("Table {} inactive MaterializedView, viewId {} ,db {} not found",
                        olapTable.getName(), mvId.getId(), mvId.getDbId());
                continue;
            }
            MaterializedView mv = (MaterializedView) db.getTable(mvId.getId());
            if (mv == null) {
                LOG.info("Ignore materialized view {} does not exists", mvId);
                continue;
            }
            doInactiveMaterializedView(mv, reason);
        }
    }

    /**
     * Inactive related mvs after modified columns have been done. Only inactive mvs after
     * modified columns have done because the modified process may be failed and in this situation
     * should not inactive mvs then.
     */
    public static void inactiveRelatedMaterializedViews(Database db,
                                                        OlapTable olapTable,
                                                        Set<String> modifiedColumns) {
        if (modifiedColumns == null || modifiedColumns.isEmpty()) {
            return;
        }
        if (!Config.enable_mv_automatic_inactive_by_base_table_changes) {
            LOG.warn("Skip to inactive related materialized views because of automatic inactive is disabled, " +
                    "table:{}, modifiedColumns:{}", olapTable.getName(), modifiedColumns);
            return;
        }
        // Only check this in leader and not replay to avoid duplicate inactive
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        // inactive related asynchronous mvs
        for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getId(), mvId.getId());
            if (mv == null) {
                LOG.warn("Ignore materialized view {} does not exists", mvId);
                continue;

            }
            // TODO: support more types for base table's schema change.
            String reason = MaterializedViewExceptions.inactiveReasonForColumnChanged(modifiedColumns);
            try {
                List<MvPlanContext> mvPlanContexts = MvPlanContextBuilder.getPlanContext(mv, true);
                for (MvPlanContext mvPlanContext : mvPlanContexts) {
                    if (mvPlanContext != null) {
                        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
                        Set<ColumnRefOperator> usedColRefs = MvUtils.collectScanColumn(mvPlan, scan -> {
                            if (scan == null) {
                                return false;
                            }
                            Table table = scan.getTable();
                            return table.getId() == olapTable.getId();
                        });
                        Set<String> usedColNames = usedColRefs.stream()
                                .map(x -> x.getName())
                                .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
                        if (modifiedColumns.stream().anyMatch(usedColNames::contains)) {
                            doInactiveMaterializedView(mv, reason);
                        }
                    }
                }
            } catch (SemanticException e) {
                LOG.warn("Get related materialized view {} failed:", mv.getName(), e);
                LOG.warn("Setting the materialized view {}({}) to invalid because " +
                                "the columns  of the table {} was modified.", mv.getName(), mv.getId(),
                        olapTable.getName());
                doInactiveMaterializedView(mv, reason);
            } catch (Exception e) {
                LOG.warn("Get related materialized view {} failed:", mv.getName(), e);
                // basic check: may lose some situations
                if (mv.getColumns().stream().anyMatch(x -> modifiedColumns.contains(x.getName()))) {
                    doInactiveMaterializedView(mv, reason);
                }
            }
        }
    }

    public static void analyzeMVProperties(Database db,
                                           MaterializedView mv,
                                           Map<String, String> properties,
                                           boolean isNonPartitioned,
                                           Map<Expr, Expr> exprAdjustedMap) throws DdlException {
        try {
            analyzeMVPropertiesImpl(db, mv, properties, isNonPartitioned, exprAdjustedMap);
        } catch (AnalysisException e) {
            if (mv.isCloudNativeMaterializedView()) {
                GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                        .unbindTableToStorageVolume(mv.getId());
            }
            ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER, e.getMessage());
        }
    }

    private static void analyzeMVPropertiesImpl(Database db,
                                                MaterializedView mv,
                                                Map<String, String> properties,
                                                boolean isNonPartitioned,
                                                Map<Expr, Expr> exprAdjustedMap) throws AnalysisException, DdlException {
        // replicated storage
        mv.setEnableReplicatedStorage(
                PropertyAnalyzer.analyzeBooleanProp(
                        properties, PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE,
                        Config.enable_replicated_storage_as_default_engine));

        // replication_num
        short replicationNum = RunMode.defaultReplicationNum();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
            replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, replicationNum);
            mv.setReplicationNum(replicationNum);
        }
        // bloom_filter_columns
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)) {
            List<Column> baseSchema = mv.getColumns();
            Set<String> bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema,
                    mv.getKeysType() == KeysType.PRIMARY_KEYS);
            if (bfColumns != null && bfColumns.isEmpty()) {
                bfColumns = null;
            }
            double bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
            if (bfColumns != null && bfFpp == 0) {
                bfFpp = FeConstants.DEFAULT_BLOOM_FILTER_FPP;
            } else if (bfColumns == null) {
                bfFpp = 0;
            }
            Set<ColumnId> bfColumnIds = null;
            if (bfColumns != null && !bfColumns.isEmpty()) {
                bfColumnIds = Sets.newTreeSet(ColumnId.CASE_INSENSITIVE_ORDER);
                for (String colName : bfColumns) {
                    bfColumnIds.add(mv.getColumn(colName).getColumnId());
                }
            }
            mv.setBloomFilterInfo(bfColumnIds, bfFpp);
        }
        // mv_rewrite_staleness second.
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
            int maxMVRewriteStaleness = PropertyAnalyzer.analyzeMVRewriteStaleness(properties);
            mv.setMaxMVRewriteStaleness(maxMVRewriteStaleness);
            mv.getTableProperty().getProperties().put(
                    PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND,
                    Integer.toString(maxMVRewriteStaleness));
        }
        // partition ttl
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
            if (isNonPartitioned) {
                throw new AnalysisException(PropertyAnalyzer.PROPERTIES_PARTITION_TTL
                        + " is only supported by partitioned materialized-view");
            }

            Pair<String, PeriodDuration> ttlDuration = PropertyAnalyzer.analyzePartitionTTL(properties, true);
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, ttlDuration.first);
            mv.getTableProperty().setPartitionTTL(ttlDuration.second);
        }
        // partition retention condition
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION)) {
            if (isNonPartitioned) {
                throw new AnalysisException(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION
                        + " is only supported by partitioned materialized-view");
            }
            String ttlRetentionCondition = PropertyAnalyzer.analyzePartitionRetentionCondition(db, mv,
                    properties, true, exprAdjustedMap);
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION, ttlRetentionCondition);
            mv.getTableProperty().setPartitionRetentionCondition(ttlRetentionCondition);
        }

        // partition ttl number
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)) {
            int number = PropertyAnalyzer.analyzePartitionTTLNumber(properties);
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER, String.valueOf(number));
            mv.getTableProperty().setPartitionTTLNumber(number);
            if (!mv.getPartitionInfo().isRangePartition()) {
                throw new AnalysisException(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER
                        + " does not support non-range-partitioned materialized view.");
            }
        }
        // partition auto refresh partitions limit
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)) {
            int limit = PropertyAnalyzer.analyzeAutoRefreshPartitionsLimit(properties, mv);
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT, String.valueOf(limit));
            mv.getTableProperty().setAutoRefreshPartitionsLimit(limit);
        }
        // partition refresh number
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)) {
            int number = PropertyAnalyzer.analyzePartitionRefreshNumber(properties);
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, String.valueOf(number));
            mv.getTableProperty().setPartitionRefreshNumber(number);
            if (isNonPartitioned) {
                throw new AnalysisException(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER
                        + " does not support non-partitioned materialized view.");
            }
        }
        // partition refresh strategy
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY)) {
            String strategy = PropertyAnalyzer.analyzePartitionRefreshStrategy(properties);
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY, strategy);
            mv.getTableProperty().setPartitionRefreshStrategy(strategy);
            if (isNonPartitioned) {
                throw new AnalysisException(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY
                        + " does not support non-partitioned materialized view.");
            }
        }
        // exclude trigger tables
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            List<TableName> tables = PropertyAnalyzer.analyzeExcludedTables(properties,
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES,
                    mv);
            String tableSb = getExcludeString(tables);
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, tableSb);
            mv.getTableProperty().setExcludedTriggerTables(tables);
        }
        // exclude refresh base tables
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES)) {
            List<TableName> tables = PropertyAnalyzer.analyzeExcludedTables(properties,
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES,
                    mv);
            String tableSb = getExcludeString(tables);
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES, tableSb);
            mv.getTableProperty().setExcludedRefreshTables(tables);
        }
        // resource_group
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP)) {
            String resourceGroup = PropertyAnalyzer.analyzeResourceGroup(properties);
            if (GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(resourceGroup) == null) {
                throw new AnalysisException(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP
                        + " " + resourceGroup + " does not exist.");
            }
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP, resourceGroup);
            mv.getTableProperty().setResourceGroup(resourceGroup);
        }
        // force external query rewrite
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)) {
            String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
            TableProperty.QueryRewriteConsistencyMode value =
                    TableProperty.analyzeExternalTableQueryRewrite(propertyValue);
            properties.remove(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
            mv.getTableProperty().getProperties().
                    put(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE, String.valueOf(value));
            mv.getTableProperty().setForceExternalTableQueryRewrite(value);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY)) {
            String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY);
            TableProperty.QueryRewriteConsistencyMode value = TableProperty.analyzeQueryRewriteMode(propertyValue);
            properties.remove(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY);
            mv.getTableProperty().getProperties().
                    put(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY, String.valueOf(value));
            mv.getTableProperty().setQueryRewriteConsistencyMode(value);
        }
        // unique keys
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
            List<UniqueConstraint> uniqueConstraints = PropertyAnalyzer.analyzeUniqueConstraint(properties, db,
                    mv);
            mv.setUniqueConstraints(uniqueConstraints);
        }
        // foreign keys
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
            List<ForeignKeyConstraint> foreignKeyConstraints = PropertyAnalyzer.analyzeForeignKeyConstraint(
                    properties, db, mv);
            mv.setForeignKeyConstraints(foreignKeyConstraints);
        }

        // time drift constraint
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT)) {
            String timeDriftConstraintSpec = properties.get(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT);
            PropertyAnalyzer.analyzeTimeDriftConstraint(timeDriftConstraintSpec, mv, properties);
            mv.getTableProperty().getProperties()
                    .put(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, timeDriftConstraintSpec);
            mv.getTableProperty().setTimeDriftConstraintSpec(timeDriftConstraintSpec);
        }

        // labels.location
        if (!mv.isCloudNativeMaterializedView()) {
            analyzeLocation(mv, properties);
        }

        // colocate_with
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
            String colocateGroup = PropertyAnalyzer.analyzeColocate(properties);
            if (org.apache.commons.lang3.StringUtils.isNotEmpty(colocateGroup) &&
                    !mv.getDefaultDistributionInfo().supportColocate()) {
                throw new AnalysisException(": random distribution does not support 'colocate_with'");
            }
            GlobalStateMgr.getCurrentState().getColocateTableIndex().addTableToGroup(
                    db, mv, colocateGroup, mv.isCloudNativeMaterializedView());
        }

        // enable_query_rewrite
        if (properties.containsKey(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE)) {
            String str = properties.get(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE);
            TableProperty.MVQueryRewriteSwitch value = TableProperty.analyzeQueryRewriteSwitch(str);
            mv.getTableProperty().setMvQueryRewriteSwitch(value);
            mv.getTableProperty().getProperties().put(
                    PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, str);
            properties.remove(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE);
        }

        // enable_query_rewrite
        if (properties.containsKey(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE)) {
            String str = properties.get(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE);
            TableProperty.MVTransparentRewriteMode value = TableProperty.analyzeMVTransparentRewrite(str);
            mv.getTableProperty().setMvTransparentRewriteMode(value);
            mv.getTableProperty().getProperties().put(
                    PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE, str);
            properties.remove(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE);
        }

        // compression
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COMPRESSION)) {
            String str = properties.get(PropertyAnalyzer.PROPERTIES_COMPRESSION);
            mv.getTableProperty().getProperties().put(
                    PropertyAnalyzer.PROPERTIES_COMPRESSION, str);
            properties.remove(PropertyAnalyzer.PROPERTIES_COMPRESSION);
        }

        // ORDER BY() -> sortKeys
        if (CollectionUtils.isNotEmpty(mv.getTableProperty().getMvSortKeys())) {
            mv.getTableProperty().putMvSortKeys();
        }

        // lake storage info
        if (mv.isCloudNativeMaterializedView()) {
            String volume = "";
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)) {
                volume = properties.remove(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME);
            }
            StorageVolumeMgr svm = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
            svm.bindTableToStorageVolume(volume, db.getId(), mv.getId());
            String storageVolumeId = svm.getStorageVolumeIdOfTable(mv.getId());
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .setLakeStorageInfo(db, mv, storageVolumeId, properties);
        }

        // warehouse
        if (mv.isCloudNativeMaterializedView()) {
            // use warehouse for current session（if u exec "set warehouse aaa" before you create mv1, then use aaa）
            long warehouseId = ConnectContext.get().getCurrentWarehouseId();
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
                String warehouseName = properties.remove(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
                Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .getWarehouse(warehouseName);
                warehouseId = warehouse.getId();
            }

            mv.setWarehouseId(warehouseId);
            LOG.debug("set warehouse {} in materializedView", warehouseId);
        }

        // datacache.partition_duration
        if (mv.isCloudNativeMaterializedView()) {
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION)) {
                PeriodDuration duration = PropertyAnalyzer.analyzeDataCachePartitionDuration(properties);
                mv.setDataCachePartitionDuration(duration);
            }
        }

        // NOTE: for recognizing unknown properties, this should be put as the last if condition
        // session properties
        if (!properties.isEmpty()) {
            // analyze properties
            List<SetListItem> setListItems = Lists.newArrayList();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                SystemVariable variable = getMVSystemVariable(properties, entry);
                GlobalStateMgr.getCurrentState().getVariableMgr().checkSystemVariableExist(variable);
                setListItems.add(variable);
            }
            SetStmtAnalyzer.analyze(new SetStmt(setListItems), null);

            // set properties if there are no exceptions
            mv.getTableProperty().getProperties().putAll(properties);
        }
    }

    @NotNull
    private static SystemVariable getMVSystemVariable(Map<String, String> properties, Map.Entry<String, String> entry)
            throws AnalysisException {
        if (!entry.getKey().startsWith(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX)) {
            throw new AnalysisException("Analyze materialized properties failed " +
                    "because unknown properties: " + properties +
                    ", please add `session.` prefix if you want add session variables for mv(" +
                    "eg, \"session.insert_timeout\"=\"30000000\").");
        }
        String varKey = entry.getKey().substring(
                PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX.length());
        SystemVariable variable = new SystemVariable(varKey, new StringLiteral(entry.getValue()));
        return variable;
    }
}
