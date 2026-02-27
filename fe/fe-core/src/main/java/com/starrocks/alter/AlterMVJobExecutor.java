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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
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
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.analyzer.mv.IVMAnalyzer;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddMVColumnClause;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.DropMVColumnClause;
import com.starrocks.sql.ast.GroupByClause;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.formatter.AST2SQLVisitor;
import com.starrocks.sql.formatter.FormatOptions;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MvPlanContextBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.type.Type;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.lang3.StringUtils;
import org.threeten.extra.PeriodDuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.alter.AlterJobMgr.MANUAL_INACTIVE_MV_REASON;

public class AlterMVJobExecutor extends AlterJobExecutor {
    @Override
    public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
        String newMvName = clause.getNewTableName();
        String oldMvName = table.getName();

        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), newMvName) != null) {
            throw new SemanticException("Materialized view [" + newMvName + "] is already used");
        }
        final RenameMaterializedViewLog renameMaterializedViewLog =
                new RenameMaterializedViewLog(table.getId(), db.getId(), newMvName);
        GlobalStateMgr.getCurrentState().getEditLog().logMvRename(renameMaterializedViewLog, wal -> {
            table.setName(newMvName);
            db.dropTable(oldMvName);
            db.registerTableUnlocked(table);
            updateTaskDefinition((MaterializedView) table);
        });
        LOG.info("rename materialized view[{}] to {}, id: {}", oldMvName, newMvName, table.getId());
        return null;
    }

    private void alterPartitionTTLNumber(Map<String, String> properties,
                                         MaterializedView materializedView,
                                         TableProperty tableProperty,
                                         List<Runnable> appliers) {
        int partitionTTL = PropertyAnalyzer.analyzePartitionTTLNumber(properties);
        if (materializedView.getTableProperty().getPartitionTTLNumber() != partitionTTL) {
            if (!materializedView.getPartitionInfo().isRangePartition()) {
                throw new SemanticException(
                        "partition_ttl_number is only supported for range partitioned materialized view");
            }
            appliers.add(() -> {
                tableProperty.modifyTableProperties(
                        PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER, String.valueOf(partitionTTL));
                tableProperty.setPartitionTTLNumber(partitionTTL);
            });
        }
    }

    private void alterPartitionTTL(Map<String, String> properties,
                                   MaterializedView materializedView,
                                   TableProperty tableProperty,
                                   List<Runnable> appliers) {
        Pair<String, PeriodDuration> ttlDuration = PropertyAnalyzer.analyzePartitionTTL(properties, true);
        if (!materializedView.getTableProperty().getPartitionTTL().equals(ttlDuration.second)) {
            if (!materializedView.getPartitionInfo().isRangePartition()) {
                throw new SemanticException(
                        "partition_ttl is only supported for range partitioned materialized view");
            }
            appliers.add(() -> {
                tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, ttlDuration.first);
                tableProperty.setPartitionTTL(ttlDuration.second);
            });
        }
    }

    private void alterPartitionRetentionCondition(Map<String, String> properties,
                                                  MaterializedView materializedView,
                                                  TableProperty tableProperty,
                                                  List<Runnable> appliers,
                                                  ConnectContext context) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(materializedView.getDbId());
        TableName mvTableName = new TableName(db.getFullName(), materializedView.getName());
        Map<Expr, Expr> mvPartitionByExprToAdjustMap =
                MaterializedViewAnalyzer.getMVPartitionByExprToAdjustMap(mvTableName, materializedView);
        String ttlRetentionCondition = PropertyAnalyzer.analyzePartitionRetentionCondition(
                db, materializedView, properties, true, mvPartitionByExprToAdjustMap);
        if (ttlRetentionCondition != null
                && !ttlRetentionCondition.equalsIgnoreCase(tableProperty.getPartitionRetentionCondition())) {
            Pair<Optional<Expr>, Optional<ScalarOperator>> condition
                    = materializedView.analyzeMVRetentionCondition(context, ttlRetentionCondition);
            if (condition != null) {
                appliers.add(() -> {
                    tableProperty.modifyTableProperties(
                            PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION, ttlRetentionCondition);
                    tableProperty.setPartitionRetentionCondition(ttlRetentionCondition);
                    materializedView.setMVRetentionCondition(condition.first, condition.second);
                });
            }
        }
    }

    private void alterTimeDriftConstraint(Map<String, String> properties,
                                          MaterializedView materializedView,
                                          TableProperty tableProperty,
                                          List<Runnable> appliers) {
        String spec = properties.get(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT);
        PropertyAnalyzer.analyzeTimeDriftConstraint(spec, materializedView, properties);
        if (!spec.equalsIgnoreCase(tableProperty.getTimeDriftConstraintSpec())) {
            appliers.add(() -> {
                tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, spec);
                tableProperty.setTimeDriftConstraintSpec(spec);
            });
        }
    }

    private void alterPartitionRefreshNumber(Map<String, String> properties,
                                             TableProperty tableProperty,
                                             List<Runnable> appliers) {
        int partitionRefreshNumber = PropertyAnalyzer.analyzePartitionRefreshNumber(properties);
        if (tableProperty.getPartitionRefreshNumber() != partitionRefreshNumber) {
            appliers.add(() -> {
                tableProperty.modifyTableProperties(
                        PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, String.valueOf(partitionRefreshNumber));
                tableProperty.setPartitionRefreshNumber(partitionRefreshNumber);
            });
        }
    }

    private void alterPartitionRefreshStrategy(Map<String, String> properties,
                                             TableProperty tableProperty,
                                             List<Runnable> appliers) {
        String partitionRefreshStrategy = PropertyAnalyzer.analyzePartitionRefreshStrategy(properties);
        if (!tableProperty.getPartitionRefreshStrategy().equals(partitionRefreshStrategy)) {
            appliers.add(() -> {
                tableProperty.modifyTableProperties(
                        PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY, partitionRefreshStrategy);
                tableProperty.setPartitionRefreshStrategy(partitionRefreshStrategy);
            });
        }
    }

    private void alterMVRefreshMode(Map<String, String> properties,
                                    MaterializedView materializedView,
                                    TableProperty tableProperty,
                                    List<Runnable> appliers,
                                    ConnectContext context) {
        String mvRefreshMode = PropertyAnalyzer.analyzeRefreshMode(properties);
        // cannot alter original pct based mv to incremental or auto, only support original ivm/pct based mv
        MaterializedView.RefreshMode currentRefreshMode =
                MaterializedView.RefreshMode.valueOf(mvRefreshMode.toUpperCase(Locale.ROOT));
        if (currentRefreshMode.isIncrementalOrAuto()) {
            ParseNode mvDefinedQueryParseNode = materializedView.getDefineQueryParseNode();
            if ((mvDefinedQueryParseNode instanceof QueryStatement queryStatement)) {
                IVMAnalyzer ivmAnalyzer = new IVMAnalyzer(context, null, queryStatement);

                Optional<IVMAnalyzer.IVMAnalyzeResult> result;
                try {
                    result = ivmAnalyzer.rewrite(
                            MaterializedView.RefreshMode.valueOf(mvRefreshMode.toUpperCase(Locale.ROOT)));
                } catch (SemanticException e) {
                    throw new SemanticException("Cannot alter materialized view refresh mode to %s: %s",
                            mvRefreshMode, e.getMessage());
                }
                if (result.isEmpty()) {
                    throw new SemanticException("Cannot alter materialized view refresh mode to %s," +
                            " because the materialized view is not eligible for %s refresh mode",
                            mvRefreshMode, mvRefreshMode);
                }
                // if materialized's original refresh mode is not auto or ivm, throw exception
                if (!materializedView.getCurrentRefreshMode().isIncrementalOrAuto()) {
                    throw new SemanticException("Cannot alter materialized view refresh mode to %s," +
                            " only support alter original incremental/auto based materialized view",
                            mvRefreshMode);
                }
                currentRefreshMode = result.get().currentRefreshMode();
            } else {
                throw new SemanticException("Cannot alter materialized view refresh mode to %s", mvRefreshMode);
            }
        }

        MaterializedView.RefreshMode finalCurrentRefreshMode = currentRefreshMode;
        if (!tableProperty.getMvRefreshMode().equals(mvRefreshMode)) {
            appliers.add(() -> {
                tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE, mvRefreshMode);
                tableProperty.setMvRefreshMode(mvRefreshMode);
                materializedView.setCurrentRefreshMode(finalCurrentRefreshMode);
            });
        }
    }

    private void alterResourceGroup(Map<String, String> properties,
                                    TableProperty tableProperty,
                                    List<Runnable> appliers) {
        String resourceGroup = PropertyAnalyzer.analyzeResourceGroup(properties);
        properties.remove(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP);
        if (!StringUtils.equals(tableProperty.getResourceGroup(), resourceGroup)) {
            if (resourceGroup != null && !resourceGroup.isEmpty() &&
                    GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(resourceGroup) ==
                            null) {
                throw new SemanticException(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP
                        + " " + resourceGroup + " does not exist.");
            }
            appliers.add(() -> {
                tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP, resourceGroup);
                tableProperty.setResourceGroup(resourceGroup);
            });
        }
    }

    private void alterAutoRefreshPartitionsLimit(Map<String, String> properties,
                                                 MaterializedView materializedView,
                                                 TableProperty tableProperty,
                                                 List<Runnable> appliers) {
        int autoRefreshPartitionsLimit = PropertyAnalyzer.analyzeAutoRefreshPartitionsLimit(properties, materializedView);
        if (tableProperty.getAutoRefreshPartitionsLimit() != autoRefreshPartitionsLimit) {
            appliers.add(() -> {
                tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT,
                        String.valueOf(autoRefreshPartitionsLimit));
                tableProperty.setAutoRefreshPartitionsLimit(autoRefreshPartitionsLimit);
            });
        }
    }

    private void alterExcludedTriggerTables(Map<String, String> properties,
                                            MaterializedView materializedView,
                                            TableProperty tableProperty,
                                            List<Runnable> appliers) {
        String excludedTriggerTablesStr = properties.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES);
        List<TableName> excludedTriggerTables = PropertyAnalyzer.analyzeExcludedTables(properties,
                PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, materializedView);
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, excludedTriggerTablesStr);
            tableProperty.setExcludedTriggerTables(excludedTriggerTables);
        });
    }

    private void alterExcludedRefreshTables(Map<String, String> properties,
                                            MaterializedView materializedView,
                                            TableProperty tableProperty,
                                            List<Runnable> appliers) {
        String excludedRefreshTablesStr = properties.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES);
        List<TableName> excludedRefreshBaseTables = PropertyAnalyzer.analyzeExcludedTables(properties,
                PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES, materializedView);
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES, excludedRefreshTablesStr);
            tableProperty.setExcludedRefreshTables(excludedRefreshBaseTables);
        });
    }

    private void alterMvRewriteStalenessSecond(Map<String, String> properties,
                                               MaterializedView materializedView,
                                               TableProperty tableProperty,
                                               List<Runnable> appliers) {
        String rawValue = properties.get(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND);
        int maxMVRewriteStaleness = PropertyAnalyzer.analyzeMVRewriteStaleness(properties);
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND, rawValue);
            materializedView.setMaxMVRewriteStaleness(maxMVRewriteStaleness);
        });
    }

    private void alterUniqueConstraint(Map<String, String> properties,
                                       MaterializedView materializedView,
                                       List<Runnable> appliers) {
        List<UniqueConstraint> uniqueConstraints = PropertyAnalyzer.analyzeUniqueConstraint(properties, db, materializedView);
        appliers.add(() -> {
            materializedView.setUniqueConstraints(uniqueConstraints);
        });
    }

    private void alterForeignKeyConstraint(Map<String, String> properties,
                                           Map<String, String> propClone,
                                           MaterializedView materializedView,
                                           List<Runnable> appliers) {
        List<ForeignKeyConstraint> foreignKeyConstraints =
                PropertyAnalyzer.analyzeForeignKeyConstraint(properties, db, materializedView);
        // update properties using analyzed foreign key constraints
        // for external table, create time is added into FOREIGN_KEY_CONSTRAINT
        String newProperty = foreignKeyConstraints
                .stream().map(ForeignKeyConstraint::toString).collect(Collectors.joining(";"));
        propClone.put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, newProperty);
        appliers.add(() -> {
            materializedView.setForeignKeyConstraints(foreignKeyConstraints);
        });
    }

    private void alterForceExternalTableQueryRewrite(Map<String, String> properties,
                                                     TableProperty tableProperty,
                                                     List<Runnable> appliers) {
        String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
        TableProperty.QueryRewriteConsistencyMode mode = TableProperty.analyzeExternalTableQueryRewrite(propertyValue);
        properties.remove(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE, String.valueOf(mode));
            tableProperty.setForceExternalTableQueryRewrite(mode);
        });
    }

    private void alterQueryRewriteConsistency(Map<String, String> properties,
                                              TableProperty tableProperty,
                                              List<Runnable> appliers) {
        String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY);
        TableProperty.QueryRewriteConsistencyMode mode = TableProperty.analyzeQueryRewriteMode(propertyValue);
        properties.remove(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY);
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY, String.valueOf(mode));
            tableProperty.setQueryRewriteConsistencyMode(mode);
        });
    }

    private void alterEnableQueryRewrite(Map<String, String> properties,
                                         MaterializedView materializedView,
                                         TableProperty tableProperty,
                                         List<Runnable> appliers) {
        String value = properties.get(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE);
        TableProperty.MVQueryRewriteSwitch queryRewriteSwitch = TableProperty.analyzeQueryRewriteSwitch(value);
        properties.remove(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE);
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, String.valueOf(queryRewriteSwitch));
            tableProperty.setMvQueryRewriteSwitch(queryRewriteSwitch);
            if (!materializedView.isEnableRewrite()) {
                // invalidate caches for mv rewrite when disable mv rewrite.
                CachingMvPlanContextBuilder.getInstance().evictMaterializedViewCache(materializedView);
            } else {
                CachingMvPlanContextBuilder.getInstance().cacheMaterializedView(materializedView);
            }
        });
    }

    private void alterTransparentMvRewriteMode(Map<String, String> properties,
                                               TableProperty tableProperty,
                                               List<Runnable> appliers) {
        String value = properties.get(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE);
        TableProperty.MVTransparentRewriteMode mode = TableProperty.analyzeMVTransparentRewrite(value);
        properties.remove(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE);
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE, String.valueOf(mode));
            tableProperty.setMvTransparentRewriteMode(mode);
        });
    }

    private void alterWarehouse(Map<String, String> properties,
                                MaterializedView materializedView,
                                List<Runnable> appliers) {
        String warehouseName = properties.remove(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
        appliers.add(() -> {
            // Note: warehouse is stored as warehouseId and output in getMaterializedViewDdlStmt(),
            // so we don't need to add it to TableProperty.properties to avoid duplication.
            materializedView.setWarehouseId(warehouse.getId());
            // Also update the associated task's properties to remove stale warehouse
            updateTaskWarehouseProperty(materializedView);
        });
    }

    /**
     * Update the associated task's properties to remove stale warehouse property.
     * The warehouse will be fetched from MV dynamically during task execution.
     */
    private void updateTaskWarehouseProperty(MaterializedView materializedView) {
        try {
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            String taskName = TaskBuilder.getMvTaskName(materializedView.getId());
            Task task = taskManager.getTask(taskName);
            if (task != null) {
                // Remove the warehouse property from task so it will be fetched from MV at runtime.
                // Use removeTaskProperty to ensure thread-safe modification under task lock.
                taskManager.removeTaskProperty(task, PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            }
        } catch (Exception e) {
            LOG.warn("Failed to update task warehouse property for MV {}: {}",
                    materializedView.getName(), e.getMessage());
        }
    }

    private void alterLabelsLocation(Map<String, String> properties,
                                     MaterializedView materializedView,
                                     List<Runnable> appliers) {
        if (materializedView.isCloudNativeMaterializedView()) {
            throw new SemanticException(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION
                    + " is not supported for cloud native materialized view.");
        }
        String location = PropertyAnalyzer.analyzeLocation(properties, true);
        appliers.add(() -> {
            materializedView.getTableProperty()
                    .modifyTableProperties(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, location);
            materializedView.setLocation(location);
        });
        properties.remove(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION);
    }

    private void alterBFColumns(Map<String, String> properties,
                                MaterializedView materializedView,
                                List<Runnable> appliers) {
        List<Column> baseSchema = materializedView.getColumns();

        // analyze bloom filter columns
        Set<String> bfColumns;
        try {
            bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema,
                    materializedView.getKeysType() == KeysType.PRIMARY_KEYS);
        } catch (AnalysisException e) {
            throw new SemanticException("Failed to analyze bloom filter columns: " + e.getMessage());
        }
        if (bfColumns != null && bfColumns.isEmpty()) {
            bfColumns = null;
        }

        // analyze bloom filter fpp
        double bfFpp;
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

        Set<ColumnId> bfColumnIds = null;
        if (bfColumns != null && !bfColumns.isEmpty()) {
            bfColumnIds = Sets.newTreeSet(ColumnId.CASE_INSENSITIVE_ORDER);
            for (String colName : bfColumns) {
                bfColumnIds.add(materializedView.getColumn(colName).getColumnId());
            }
        }
        Set<ColumnId> oldBfColumnIds = materializedView.getBfColumnIds();
        if (bfColumnIds != null && bfColumnIds.equals(oldBfColumnIds) && materializedView.getBfFpp() == bfFpp) {
            // do nothing
        } else {
            final Set<ColumnId> finalBfColumnIds = bfColumnIds;
            final double finalBfFpp = bfFpp;
            appliers.add(() -> {
                materializedView.setBloomFilterInfo(finalBfColumnIds, finalBfFpp);
            });
        }
        properties.remove(PropertyAnalyzer.PROPERTIES_BF_COLUMNS);
    }

    private void alterColocateWith(Map<String, String> properties,
                                   MaterializedView materializedView) {
        // TODO: when support shared-nothing mode, must check PROPERTIES_LABELS_LOCATION
        if (RunMode.isSharedNothingMode()) {
            throw new SemanticException("Modify failed because unsupported properties: " +
                    "colocate_with is not supported for materialized view in shared-nothing cluster.");
        }
        try {
            String colocateGroup = PropertyAnalyzer.analyzeColocate(properties);
            GlobalStateMgr.getCurrentState().getColocateTableIndex()
                    .modifyTableColocate(db, materializedView, colocateGroup, false, null);
        } catch (DdlException e) {
            throw new AlterJobException(e.getMessage(), e);
        }
    }

    private void alterSessionVariables(Map<String, String> properties,
                                       TableProperty tableProperty,
                                       List<Runnable> appliers) {
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
        appliers.add(() -> {
            // set properties if there are no exceptions
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tableProperty.modifyTableProperties(entry.getKey(), entry.getValue());
            }
        });
    }

    @Override
    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause modifyTablePropertiesClause,
                                                 ConnectContext context) {
        MaterializedView materializedView = (MaterializedView) table;
        Map<String, String> properties = modifyTablePropertiesClause.getProperties();
        Map<String, String> propClone = Maps.newHashMap(properties);
        List<Runnable> appliers = new ArrayList<>();
        TableProperty tableProperty = materializedView.getTableProperty();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)) {
            alterPartitionTTLNumber(properties, materializedView, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
            alterPartitionTTL(properties, materializedView, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION)) {
            alterPartitionRetentionCondition(properties, materializedView, tableProperty, appliers, context);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT)) {
            alterTimeDriftConstraint(properties, materializedView, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)) {
            alterPartitionRefreshNumber(properties, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY)) {
            alterPartitionRefreshStrategy(properties, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE)) {
            alterMVRefreshMode(properties, materializedView, tableProperty, appliers, context);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP)) {
            alterResourceGroup(properties, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)) {
            alterAutoRefreshPartitionsLimit(properties, materializedView, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            alterExcludedTriggerTables(properties, materializedView, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES)) {
            alterExcludedRefreshTables(properties, materializedView, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
            alterMvRewriteStalenessSecond(properties, materializedView, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
            alterUniqueConstraint(properties, materializedView, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
            alterForeignKeyConstraint(properties, propClone, materializedView, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)) {
            alterForceExternalTableQueryRewrite(properties, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY)) {
            alterQueryRewriteConsistency(properties, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE)) {
            alterEnableQueryRewrite(properties, materializedView, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE)) {
            alterTransparentMvRewriteMode(properties, tableProperty, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            alterWarehouse(properties, materializedView, appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION)) {
            alterLabelsLocation(properties, materializedView, appliers);
        }
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)) {
            alterBFColumns(properties, materializedView,  appliers);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
            alterColocateWith(properties, materializedView);
        }
        if (!properties.isEmpty()) {
            alterSessionVariables(properties, tableProperty, appliers);
        }

        if (!appliers.isEmpty()) {
            ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(
                    materializedView.getDbId(), materializedView.getId(), propClone);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMaterializedViewProperties(log, wal -> {
                for (Runnable applier : appliers) {
                    applier.run();
                }
            });
            DynamicPartitionUtil.registerOrRemovePartitionTTLTable(materializedView.getDbId(), materializedView);
        }
        LOG.info("alter materialized view properties {}, id: {}", propClone, materializedView.getId());
        return null;
    }

    @Override
    public Void visitAddMVColumnClause(AddMVColumnClause clause, ConnectContext context) {
        MaterializedView mv = (MaterializedView) table;
        String columnName = clause.getColumnName();
        
        // Check if column already exists
        if (mv.getColumn(columnName) != null) {
            throw new SemanticException("Column '%s' already exists in materialized view", columnName);
        }
        List<Table> baseTables = mv.getBaseTables();
        if (baseTables.size() != 1) {
            throw new SemanticException("Add column to materialized view only supports single base table");
        }
        Table baseTable = baseTables.get(0);

        // Validate aggregate expression - it should contain aggregate functions
        ParseNode astParseNode = mv.initDefineQueryParseNode();
        // check mv's parse node is a simple QueryStatement
        if (astParseNode == null || !(astParseNode instanceof QueryStatement)) {
            throw new SemanticException("Materialized view definition is invalid");
        }
        QueryStatement queryStatement = (QueryStatement) astParseNode;
        if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
            throw new SemanticException("Materialized view definition is invalid, only support select statement");
        }
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();

        // Determine the column type from the aggregate expression
        Expr addColumnExpr = clause.getAggregateExpression();
        boolean isAggregateFunction = false;
        if (addColumnExpr instanceof FunctionCallExpr) {
            FunctionCallExpr funcExpr = (FunctionCallExpr) addColumnExpr;
            String funcName = funcExpr.getFunctionName();
            isAggregateFunction = ExprUtils.isAggregateFunction(funcName);
        }
        GroupByClause groupByClause = selectRelation.getGroupByClause();
        if (isAggregateFunction &&
                ((groupByClause != null && groupByClause.isEmpty()) ||
                 (selectRelation.getGroupBy() != null && selectRelation.getGroupBy().isEmpty()))) {
            throw new SemanticException("Add new aggregate function column failed: Materialized view must contain group by " +
                    "columns.");
        }

        List<Expr> newOutputCols = Lists.newArrayList(selectRelation.getOutputExpression());
        // always add it into output column
        newOutputCols.add(addColumnExpr);
        // if the column is not a expr, it can be:
        // - group by column, if the query contains aggregate
        // - output column, if the query contains no aggregate
        if (isAggregateFunction) {
            if (selectRelation.getAggregate() != null) {
                selectRelation.getAggregate().add((FunctionCallExpr) addColumnExpr);
            }
        } else {
            if (selectRelation.getAggregate() != null && !selectRelation.getAggregate().isEmpty()) {
                groupByClause.getGroupingExprs().add(addColumnExpr);
                groupByClause.getOriGroupingExprs().add(addColumnExpr);
                List<Expr> groupByExprs = selectRelation.getGroupBy();
                groupByExprs.add(addColumnExpr);
            }
        }
        selectRelation.setOutputExpr(newOutputCols);
        SelectList selectList = selectRelation.getSelectList();
        List<SelectListItem> selectListItems = selectList.getItems();
        selectListItems.add(new SelectListItem(addColumnExpr, columnName));

        // analyze this ast
        ConnectContext connectContext = context == null ? ConnectContext.buildInner() : context;
        Analyzer.analyze(queryStatement, connectContext);

        List<SlotRef> slots = Lists.newArrayList();
        addColumnExpr.collect(SlotRef.class, slots);
        if (slots.size() != 1) {
            throw new SemanticException("Materialized view definition is invalid, " +
                    "the added column expression must be a simple column reference");
        }
        // find the base column from base table
        SlotRef slotRef = slots.get(0);
        String baseColumnName = slotRef.getColumnName();
        if (baseColumnName == null) {
            throw new SemanticException("Materialized view definition is invalid, " +
                    "the added column expression must be a simple column reference");
        }
        Column baseColumn = baseTable.getColumn(baseColumnName);
        if (baseColumn == null) {
            throw new SemanticException("Materialized view definition is invalid, " +
                    "the added column expression must be a simple column reference");
        }
        MaterializedView.AsyncRefreshContext mvAsyncRefreshContext = mv.getRefreshScheme().getAsyncRefreshContext();
        long baseColumnCreatedTime = baseColumn.getCreatedTime();
        Set<String> toRefreshPartitionNames = Sets.newHashSet();
        if (baseColumnCreatedTime == -1) {
            // If the new column's definition requires re-calculating historical data to maintain consistency,
            // FSE will throw an exception.
            if (!mv.isSupportFastSchemaEvolutionInDanger()) {
                String reason = String.format("Cannot add column " +
                        "'%s' to materialized view '%s' because the base column '%s' created time is unknown and needs " +
                        "to refresh the whole base table.", columnName, mv.getName(), baseColumnName);
                throw new SemanticException(MaterializedViewExceptions.unSupportedReasonForMVFSE(reason));
            }
        } else {
            checkMVVisibleVersionAffectedBySchemaChange(mv, baseTable,
                    mvAsyncRefreshContext, columnName, baseColumnCreatedTime, toRefreshPartitionNames);
        }

        // correct new column type
        Type newColumnType = AnalyzerUtils.transformTableColumnType(addColumnExpr.getType(), false);
        AddColumnsClause addColumnsClause = clause.toAddColumnsClause(newColumnType);

        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        try {
            schemaChangeHandler.process(Lists.newArrayList(addColumnsClause), db, mv);
        } catch (StarRocksException e) {
            throw new AlterJobException("Failed to add column to materialized view: " + e.getMessage(), e);
        }

        try {
            // TODO(fixme): clear affected partition entries from the version map when FSE is allowed in danger mode
            // will cause a full refresh for the affected partitions.

            // Clear affected partition entries from the version map when FSE is allowed in danger mode
            // so that MV refresh will recompute data for those partitions
            // if (!toRefreshPartitionNames.isEmpty()) {
            //     LOG.info("Clearing affected partition versions for materialized view {} after adding column: {}",
            //             mv.getName(), toRefreshPartitionNames);
            //     Map<String, MaterializedView.BasePartitionInfo> basePartitionInfoMap =
            //             mvAsyncRefreshContext.getBaseTableVisibleVersionMap().get(baseTable.getId());
            //     if (basePartitionInfoMap != null) {
            //         for (String partitionName : toRefreshPartitionNames) {
            //             basePartitionInfoMap.remove(partitionName);
            //         }
            //     }
            // }

            // renew materialized view's defined query
            String newDefinedSql = AST2SQLVisitor.withOptions(FormatOptions.allEnable()
                    .setEnableDigest(false)).visit(astParseNode);
            mv.setViewDefineSql(newDefinedSql);
            mv.setSimpleDefineSql(newDefinedSql);
            mv.setOriginalViewDefineSql(newDefinedSql);
            mv.resetDefinedQueryParseNode();
            // new query output index
            List<Integer> newQueryOutputIndices = Lists.newArrayList(mv.getQueryOutputIndices());
            if (!newQueryOutputIndices.isEmpty()) {
                newQueryOutputIndices.add(newQueryOutputIndices.size());
                mv.setQueryOutputIndices(newQueryOutputIndices);
            }
            CachingMvPlanContextBuilder.getInstance().cacheMaterializedView(mv);

            // to be compatible with old MV schema.
            mv.initUniqueId();

            // write edit log to persist schema change
            AlterMaterializedViewBaseTableInfosLog log = new AlterMaterializedViewBaseTableInfosLog(null, mv,
                            AlterMaterializedViewBaseTableInfosLog.AlterType.ADD_COLUMN);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMvBaseTableInfos(log);

            LOG.info("Logged schema change for materialized view '{}': added column '{}' with expression '{}'",
                    mv.getName(), columnName, addColumnExpr.toString());
            return null;
        } catch (DmlException e) {
            throw new AlterJobException("Failed to add column to materialized view: " + e.getMessage(), e);
        }
    }

    private void checkMVVisibleVersionAffectedBySchemaChange(MaterializedView mv, Table baseTable,
                                                             MaterializedView.AsyncRefreshContext mvAsyncRefreshContext,
                                                             String columnName,
                                                             long baseColumnCreatedTime,
                                                             Set<String> toRefreshPartitionNames) {
        if (baseTable.isNativeTable()) {
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> olapTableVisiblePartitionMap =
                    mvAsyncRefreshContext.getBaseTableVisibleVersionMap();
            if (!olapTableVisiblePartitionMap.containsKey(baseTable.getId())) {
                return;
            }
            OlapTable olapBaseTable = (OlapTable) baseTable;
            Map<String, MaterializedView.BasePartitionInfo> basePartitionInfoMap =
                    olapTableVisiblePartitionMap.get(baseTable.getId());
            if (basePartitionInfoMap == null || basePartitionInfoMap.isEmpty()) {
                return;
            }
            for (Map.Entry<String, MaterializedView.BasePartitionInfo> entry : basePartitionInfoMap.entrySet()) {
                Partition partition = olapBaseTable.getPartition(entry.getKey());
                // Skip if partition is null (may have been dropped while MV's version map still contains it)
                if (partition == null) {
                    LOG.warn("Partition {} not found in base table {} for materialized view {}, " +
                                    "it may have been dropped",
                            entry.getKey(), baseTable.getName(), mv.getName());
                    continue;
                }
                // if the partition's visible version time is greater than base column's created time which means
                // the base table has changed after the column is added, so we need to remove this partition info
                // to trigger a full refresh for this partition.
                PhysicalPartition latestPhysicalPartition = partition.getLatestPhysicalPartition();
                if (latestPhysicalPartition != null &&
                        latestPhysicalPartition.getVisibleVersionTime() > baseColumnCreatedTime) {
                    toRefreshPartitionNames.add(entry.getKey());
                }
            }
            // if toRefreshPartitionNames's not empty, throw exception when all partitions are affected
            if (!toRefreshPartitionNames.isEmpty() && !mv.isSupportFastSchemaEvolutionInDanger()) {
                LOG.warn("After adding column to materialized view {}, to remove partition infos {} " +
                                "to trigger full refresh, base column created time: {}, " +
                                "partition visible version time: {}, to-refresh partitions: {}",
                        mv.getName(), toRefreshPartitionNames, baseColumnCreatedTime,
                        olapTableVisiblePartitionMap.get(baseTable.getId()), toRefreshPartitionNames);
                String reason = String.format("Cannot add column " +
                                "'%s' to materialized view '%s' because partitions in the base table '%s' are affected " +
                                "by this schema change and need to be refreshed: %s.",
                        columnName, mv.getName(), baseTable.getName(), toRefreshPartitionNames);
                throw new SemanticException(MaterializedViewExceptions.unSupportedReasonForMVFSE(reason));
            }
        } else {
            if (!mv.isSupportFastSchemaEvolutionInDanger()) {
                String reason = String.format("Cannot add column " +
                        "'%s' to materialized view '%s' because the base table '%s' is not an olap table and " +
                        "we cannot determine which partitions are affected by this schema change.",
                        columnName, mv.getName(), baseTable.getName());
                throw new SemanticException(MaterializedViewExceptions.unSupportedReasonForMVFSE(reason));
            }
        }
    }

    @Override
    public Void visitDropMVColumnClause(DropMVColumnClause clause, ConnectContext context) {
        MaterializedView mv = (MaterializedView) table;
        String columnName = clause.getColumnName();

        if (mv.getColumn(columnName) == null) {
            throw new SemanticException("Column '%s' does not exist in materialized view", columnName);
        }

        ParseNode astParseNode = mv.initDefineQueryParseNode();
        if (astParseNode == null || !(astParseNode instanceof QueryStatement)) {
            throw new SemanticException("Materialized view definition is invalid");
        }
        QueryStatement queryStatement = (QueryStatement) astParseNode;
        if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
            throw new SemanticException("Materialized view definition is invalid, only support select statement");
        }
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        List<Column> visibleSchema = mv.getFullVisibleSchema();
        int columnIndex = -1;
        for (int i = 0; i < visibleSchema.size(); i++) {
            if (visibleSchema.get(i).getName().equalsIgnoreCase(columnName)) {
                columnIndex = i;
                break;
            }
        }
        if (columnIndex < 0) {
            throw new SemanticException("Column '%s' does not exist in materialized view", columnName);
        }

        List<Expr> outputExpr = selectRelation.getOutputExpression();
        SelectList selectList = selectRelation.getSelectList();
        List<SelectListItem> selectListItems = selectList.getItems();
        if (outputExpr == null || columnIndex >= outputExpr.size() || columnIndex >= selectListItems.size()) {
            throw new SemanticException("Materialized view definition is invalid");
        }
        List<Expr> newOutputExpr = Lists.newArrayList(outputExpr);
        Expr removedExpr = newOutputExpr.remove(columnIndex);
        if (selectRelation.getGroupByClause() != null) {
            GroupByClause groupByClause = selectRelation.getGroupByClause();
            if (groupByClause.getGroupingExprs().stream()
                    .anyMatch(expr -> expr == removedExpr || expr.equals(removedExpr))) {
                throw new SemanticException("Cannot drop column '%s' because it is used in GROUP BY clause", columnName);
            }
            if (groupByClause.getOriGroupingExprs().stream()
                    .anyMatch(expr -> expr == removedExpr || expr.equals(removedExpr))) {
                throw new SemanticException("Cannot drop column '%s' because it is used in GROUP BY clause", columnName);
            }
            if (selectRelation.getGroupBy().stream()
                    .anyMatch(expr -> expr == removedExpr || expr.equals(removedExpr))) {
                throw new SemanticException("Cannot drop column '%s' because it is used in GROUP BY clause", columnName);
            }
        }

        selectRelation.setOutputExpr(newOutputExpr);
        selectListItems.remove(columnIndex);
        // if the removed expr is an aggregate function, remove it from aggregate list
        if (removedExpr instanceof FunctionCallExpr && selectRelation.getAggregate() != null) {
            selectRelation.getAggregate().removeIf(expr -> expr == removedExpr || expr.equals(removedExpr));
        }

        // Validate and adjust query output indices before schema change
        List<Integer> newQueryOutputIndices = Lists.newArrayList(mv.getQueryOutputIndices());
        if (!newQueryOutputIndices.isEmpty()) {
            if (columnIndex < 0 || columnIndex >= newQueryOutputIndices.size()) {
                throw new SemanticException(String.format("Materialized view with ordered output indices %s is invalid: " +
                        "cannot drop column '%s'", columnIndex, columnName));
            }
            // Remove the dropped column index and adjust remaining indices
            newQueryOutputIndices.remove(columnIndex);
            // Decrement all indices greater than the removed column index
            for (int i = 0; i < newQueryOutputIndices.size(); i++) {
                if (newQueryOutputIndices.get(i) > columnIndex) {
                    newQueryOutputIndices.set(i, newQueryOutputIndices.get(i) - 1);
                }
            }
        }

        ConnectContext connectContext = context == null ? ConnectContext.buildInner() : context;
        Analyzer.analyze(queryStatement, connectContext);

        // fast schema evolution
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        try {
            schemaChangeHandler.process(Lists.newArrayList(clause.toDropColumnClause()), db, mv);
            mv.lastSchemaUpdateTime.set(System.nanoTime());
        } catch (StarRocksException e) {
            throw new AlterJobException("Failed to drop column from materialized view: " + e.getMessage(), e);
        }

        try {
            String newDefinedSql = AST2SQLVisitor.withOptions(FormatOptions.allEnable()
                    .setEnableDigest(false)).visit(astParseNode);
            mv.setViewDefineSql(newDefinedSql);
            mv.setSimpleDefineSql(newDefinedSql);
            mv.setQueryOutputIndices(newQueryOutputIndices);
            mv.setOriginalViewDefineSql(newDefinedSql);
            mv.resetDefinedQueryParseNode();
            CachingMvPlanContextBuilder.getInstance().cacheMaterializedView(mv);
            // to be compatible with old MV schema.
            mv.initUniqueId();

            // write edit log to persist schema change
            AlterMaterializedViewBaseTableInfosLog log = new AlterMaterializedViewBaseTableInfosLog(null, mv,
                    AlterMaterializedViewBaseTableInfosLog.AlterType.DROP_COLUMN);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMvBaseTableInfos(log);

            // inactive related mvs
            inactiveRelatedMaterializedViewsRecursive(mv, Sets.newHashSet(columnName));

            LOG.info("Logged schema change for materialized view '{}': dropped column '{}'",
                    mv.getName(), columnName);
        } catch (DmlException e) {
            throw new AlterJobException("Failed to drop column from materialized view: " + e.getMessage(), e);
        }
        return null;
    }

    @Override
    public Void visitRefreshSchemeClause(RefreshSchemeClause refreshSchemeDesc, ConnectContext context) {
        try {
            MaterializedView materializedView = (MaterializedView) table;
            String dbName = db.getFullName();

            MaterializedViewRefreshType newRefreshType = MaterializedViewRefreshType.getType(refreshSchemeDesc);
            MaterializedViewRefreshType oldRefreshType = materializedView.getRefreshScheme().getType();

            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            Task currentTask = taskManager.getTask(TaskBuilder.getMvTaskName(materializedView.getId()));
            Task task;
            if (currentTask == null) {
                task = TaskBuilder.buildMvTask(materializedView, dbName);
                TaskBuilder.updateTaskInfo(task, refreshSchemeDesc, materializedView);
                taskManager.createTask(task);
            } else {
                Task changedTask = TaskBuilder.rebuildMvTask(materializedView, dbName, currentTask.getProperties(),
                        currentTask);
                TaskBuilder.updateTaskInfo(changedTask, refreshSchemeDesc, materializedView);
                taskManager.alterTask(currentTask, changedTask);
                task = currentTask;
            }

            // for event triggered type, run task
            if (task.getType() == Constants.TaskType.EVENT_TRIGGERED) {
                taskManager.executeTask(task.getName(), ExecuteOption.makeMergeRedundantOption());
            }

            final MaterializedView.MvRefreshScheme refreshScheme = materializedView.getRefreshScheme();
            Locker locker = new Locker();
            if (!locker.lockTableAndCheckDbExist(db, materializedView.getId(), LockType.WRITE)) {
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
                locker.unLockTableWithIntensiveDbLock(db.getId(), materializedView.getId(), LockType.WRITE);
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
                // check if the materialized view can be activated without rebuilding relationships.
                if (materializedView.isActive()) {
                    return null;
                }

                GlobalStateMgr.getCurrentState().getAlterJobMgr().
                        alterMaterializedViewStatus(materializedView, status, "", false);
                // for manual refresh type, do not refresh
                if (materializedView.getRefreshScheme().getType() != MaterializedViewRefreshType.MANUAL) {
                    GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .refreshMaterializedView(dbName, materializedView.getName(), false, null,
                                    Constants.TaskRunPriority.NORMAL.value(), true, false);
                }
                AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(materializedView.getDbId(),
                        materializedView.getId(), status, "");
                GlobalStateMgr.getCurrentState().getEditLog().logAlterMvStatus(log);
            } else if (AlterMaterializedViewStatusClause.INACTIVE.equalsIgnoreCase(status)) {
                if (!materializedView.isActive()) {
                    return null;
                }
                LOG.warn("Setting the materialized view {}({}) to inactive because " +
                                "user use alter materialized view set status to inactive",
                        materializedView.getName(), materializedView.getId());
                Set<MvId> visited = Sets.newHashSet();
                // not clear version map for user manual inactive by default since mv's refreshed data has not been
                // broken from the current base tables.
                // this method will write edit log in it.
                doInactiveMaterializedViewRecursive(materializedView, MANUAL_INACTIVE_MV_REASON, false, visited);
            } else {
                throw new AlterJobException("Unsupported modification materialized view status:" + status);
            }
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
        }
    }

    /**
     * Inactive the materialized view and its related materialized views.
     */
    private static void doInactiveMaterializedViewRecursive(MaterializedView mv, String reason,
                                                            boolean isClearVersionMap,
                                                            Set<MvId> visited) {
        // Only check this in leader and not replay to avoid duplicate inactive
        if (mv == null || !GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        if (visited.contains(mv.getMvId())) {
            return;
        }
        // inactive this mv first
        doInactiveMaterializedViewOnly(mv, reason, isClearVersionMap);
        // add it into visited
        visited.add(mv.getMvId());

        // reset inactive reason
        reason = MaterializedViewExceptions.inactiveReasonForBaseTableInActive(mv.getName());
        // recursive inactive
        for (MvId mvId : mv.getRelatedMaterializedViews()) {
            if (visited.contains(mvId)) {
                continue;
            }
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
            if (db == null) {
                LOG.warn("Table {} inactive MaterializedView, viewId {} ,db {} not found",
                        mv.getName(), mvId.getId(), mvId.getDbId());
                continue;
            }
            MaterializedView relatedMV = (MaterializedView) db.getTable(mvId.getId());
            if (relatedMV == null) {
                LOG.info("Ignore materialized view {} does not exists", mvId);
                continue;
            }
            // do inactive this mvs
            doInactiveMaterializedViewRecursive(relatedMV, reason, isClearVersionMap, visited);
        }
    }

    /**
     * 1. This method will clear all visible version map of the MV since for all schema changes, the MV should be
     * refreshed.
     * 2. User's inactive-mv command should not call this which will reserve the visible version map.
     * @param mv target mv to inactive
     * @param reason inactive reason
     * @param isClearVersionMap whether to clear version map, if true the following mv refresh will refresh the whole data.
     */
    private static void doInactiveMaterializedViewOnly(MaterializedView mv, String reason,
                                                       boolean isClearVersionMap) {
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
                    mv.getId(), status, reason);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterMvStatus(log);
        } else {
            mv.setInactiveAndReason(reason);
        }
        // clear version map to make sure the MV will be refreshed
        if (isClearVersionMap) {
            mv.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMap();
        }
    }

    /**
     * Inactive related materialized views because of base table/view is changed or dropped in the leader background.
     * </p>
     * NOTE: This method will clear the related mvs' version map by default since the base table
     *  has broken from mv existed refreshed data.
     */
    public static void inactiveRelatedMaterializedViewsRecursive(Table olapTable, String reason, boolean isReplay) {
        if (olapTable == null) {
            return;
        }
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
        Set<MvId> inactiveMVIds = Sets.newHashSet();
        for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
            MaterializedView mv = GlobalStateMgr.getCurrentState().getLocalMetastore().getMaterializedView(mvId);
            if (mv == null) {
                LOG.info("Ignore materialized view {} does not exists", mvId);
                continue;
            }
            doInactiveMaterializedViewRecursive(mv, reason, true, inactiveMVIds);
        }
    }

    /**
     * Inactive related mvs after modified columns have been done. Only inactive mvs after
     * modified columns have done because the modified process may be failed and in this situation
     * should not inactive mvs then.
     * </p>
     * NOTE: This method will clear the related mvs' version map by default since the base table
     *  has broken from mv existed refreshed data.
     */
    public static void inactiveRelatedMaterializedViewsRecursive(OlapTable olapTable, Set<String> modifiedColumns) {
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
        Set<MvId> visited = Sets.newHashSet();
        // inactive related asynchronous mvs
        for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
            if (visited.contains(mvId)) {
                continue;
            }
            MaterializedView mv = GlobalStateMgr.getCurrentState().getLocalMetastore().getMaterializedView(mvId);
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
                            doInactiveMaterializedViewRecursive(mv, reason, true, visited);
                        }
                    }
                }
            } catch (SemanticException e) {
                LOG.warn("Get related materialized view {} failed:", mv.getName(), e);
                LOG.warn("Setting the materialized view {}({}) to invalid because " +
                                "the columns  of the table {} was modified.", mv.getName(), mv.getId(),
                        olapTable.getName());
                doInactiveMaterializedViewRecursive(mv, reason, true, visited);
            } catch (Exception e) {
                LOG.warn("Get related materialized view {} failed:", mv.getName(), e);
                // basic check: may lose some situations
                if (mv.getColumns().stream().anyMatch(x -> modifiedColumns.contains(x.getName()))) {
                    doInactiveMaterializedViewRecursive(mv, reason, true, visited);
                }
            }
        }
    }

    /**
     * Check related synchronous materialized views before modified columns, throw exceptions
     * if modified columns affect the related rollup/synchronous mvs.
     */
    public static void checkModifiedColumWithMaterializedViews(OlapTable olapTable,
                                                               Set<String> modifiedColumns) throws DdlException {
        if (modifiedColumns == null || modifiedColumns.isEmpty()) {
            return;
        }

        // If there is synchronized materialized view referring the column, throw exception.
        if (olapTable.getIndexNameToMetaId().size() > 1) {
            Map<Long, MaterializedIndexMeta> metaMap = olapTable.getIndexMetaIdToMeta();
            for (Map.Entry<Long, MaterializedIndexMeta> entry : metaMap.entrySet()) {
                Long indexMetaId = entry.getKey();
                if (indexMetaId == olapTable.getBaseIndexMetaId()) {
                    continue;
                }
                MaterializedIndexMeta meta = entry.getValue();
                List<Column> schema = meta.getSchema();
                String indexName = olapTable.getIndexNameByMetaId(indexMetaId);
                // ignore agg_keys type because it's like duplicated without agg functions
                boolean hasAggregateFunction = olapTable.getKeysType() != KeysType.AGG_KEYS &&
                        schema.stream().anyMatch(x -> x.isAggregated());
                if (hasAggregateFunction) {
                    for (Column rollupCol : schema) {
                        String colName = rollupCol.getName();
                        if (modifiedColumns.contains(colName)) {
                            throw new DdlException(String.format("Can not drop/modify the column %s, " +
                                    "because the column is used in the related rollup %s, " +
                                    "please drop the rollup index first.", colName, indexName));
                        }
                        if (rollupCol.getRefColumns() != null) {
                            for (SlotRef refColumn : rollupCol.getRefColumns()) {
                                String refColName = refColumn.getColumnName();
                                if (modifiedColumns.contains(refColName)) {
                                    String defineExprSql = rollupCol.getDefineExpr() == null ? "" :
                                            ExprToSql.toSql(rollupCol.getDefineExpr());
                                    throw new DdlException(String.format("Can not drop/modify the column %s, " +
                                                    "because the column is used in the related rollup %s " +
                                                    "with the define expr:%s, please drop the rollup index first.",
                                            refColName, indexName, defineExprSql));
                                }
                            }
                        }
                    }
                }
                if (meta.getWhereClause() != null) {
                    Expr whereExpr = meta.getWhereClause();
                    List<SlotRef> whereSlots = new ArrayList<>();
                    whereExpr.collect(SlotRef.class, whereSlots);
                    for (SlotRef refColumn : whereSlots) {
                        String colName = refColumn.getColumnName();
                        if (modifiedColumns.contains(colName)) {
                            String whereExprSql = ExprToSql.toSql(whereExpr);
                            throw new DdlException(String.format("Can not drop/modify the column %s, " +
                                            "because the column is used in the related rollup %s " +
                                            "with the where expr:%s, please drop the rollup index first.",
                                    colName, indexName, whereExprSql));
                        }
                    }
                }
            }
        }
    }

    /**
     * Inactive the materialized view because of its task is failed for consecutive times and write the edit log.
     */
    public static void inactiveForConsecutiveFailures(MaterializedView mv) {
        if (mv == null) {
            return;
        }
        final String inactiveReason = MaterializedViewExceptions.inactiveReasonForConsecutiveFailures(mv.getName());
        // inactive related mv
        mv.setInactiveAndReason(inactiveReason);
        // write edit log
        AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(mv.getDbId(),
                mv.getId(), AlterMaterializedViewStatusClause.INACTIVE, inactiveReason);
        GlobalStateMgr.getCurrentState().getEditLog().logAlterMvStatus(log);
    }
}
