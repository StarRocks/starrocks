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
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
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
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.analyzer.mv.IVMAnalyzer;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MvPlanContextBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
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
            ttlDuration = PropertyAnalyzer.analyzePartitionTTL(properties, true);
        }
        String ttlRetentionCondition = null;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION)) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(materializedView.getDbId());
            TableName mvTableName = new TableName(db.getFullName(), materializedView.getName());
            Map<Expr, Expr> mvPartitionByExprToAdjustMap =
                    MaterializedViewAnalyzer.getMVPartitionByExprToAdjustMap(mvTableName, materializedView);
            ttlRetentionCondition = PropertyAnalyzer.analyzePartitionRetentionCondition(db,
                    materializedView, properties, true, mvPartitionByExprToAdjustMap);
        }
        String timeDriftConstraintSpec = null;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT)) {
            String spec = properties.get(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT);
            PropertyAnalyzer.analyzeTimeDriftConstraint(spec, materializedView, properties);
            timeDriftConstraintSpec = spec;
        }
        int partitionRefreshNumber = INVALID;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)) {
            partitionRefreshNumber = PropertyAnalyzer.analyzePartitionRefreshNumber(properties);
        }
        String partitionRefreshStrategy = null;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY)) {
            partitionRefreshStrategy = PropertyAnalyzer.analyzePartitionRefreshStrategy(properties);
        }
        String mvRefreshMode = null;
        MaterializedView.RefreshMode currentRefreshMode = MaterializedView.RefreshMode.PCT;
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE)) {
            mvRefreshMode = PropertyAnalyzer.analyzeRefreshMode(properties);

            // cannot alter original pct based mv to incremental or auto, only support original ivm/pct based mv
            currentRefreshMode = MaterializedView.RefreshMode.valueOf(mvRefreshMode.toUpperCase(Locale.ROOT));
            if (currentRefreshMode.isIncrementalOrAuto()) {
                ParseNode mvDefinedQueryParseNode = materializedView.getDefineQueryParseNode();
                if (mvDefinedQueryParseNode != null && (mvDefinedQueryParseNode instanceof QueryStatement)) {
                    QueryStatement queryStatement = (QueryStatement) mvDefinedQueryParseNode;
                    IVMAnalyzer ivmAnalyzer = new IVMAnalyzer(context, queryStatement);

                    Optional<IVMAnalyzer.IVMAnalyzeResult> result = Optional.empty();
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
            excludedTriggerTables = PropertyAnalyzer.analyzeExcludedTables(properties,
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, materializedView);
        }
        List<TableName> excludedRefreshBaseTables = Lists.newArrayList();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES)) {
            excludedRefreshBaseTables = PropertyAnalyzer.analyzeExcludedTables(properties,
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES, materializedView);
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

        boolean isChanged = false;
        // bloom_filter_columns
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)) {
            List<Column> baseSchema = materializedView.getColumns();

            // analyze bloom filter columns
            Set<String> bfColumns = null;
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

            Set<ColumnId> bfColumnIds = null;
            if (bfColumns != null && !bfColumns.isEmpty()) {
                bfColumnIds = Sets.newTreeSet(ColumnId.CASE_INSENSITIVE_ORDER);
                for (String colName : bfColumns) {
                    bfColumnIds.add(materializedView.getColumn(colName).getColumnId());
                }
            }
            Set<ColumnId> oldBfColumnIds = materializedView.getBfColumnIds();
            if (bfColumnIds != null && oldBfColumnIds != null &&
                    bfColumnIds.equals(oldBfColumnIds) && materializedView.getBfFpp() == bfFpp) {
                // do nothing
            } else {
                isChanged = true;
                materializedView.setBloomFilterInfo(bfColumnIds, bfFpp);
            }
            properties.remove(PropertyAnalyzer.PROPERTIES_BF_COLUMNS);
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
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

        if (!properties.isEmpty()) {
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

        // TODO(murphy) refactor the code
        Map<String, String> curProp = materializedView.getTableProperty().getProperties();
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL) && ttlDuration != null &&
                !materializedView.getTableProperty().getPartitionTTL().equals(ttlDuration.second)) {
            if (!materializedView.getPartitionInfo().isRangePartition()) {
                throw new SemanticException("partition_ttl is only supported for range partitioned materialized view");
            }
            curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, ttlDuration.first);
            materializedView.getTableProperty().setPartitionTTL(ttlDuration.second);
            isChanged = true;
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER) &&
                materializedView.getTableProperty().getPartitionTTLNumber() != partitionTTL) {
            if (!materializedView.getPartitionInfo().isRangePartition()) {
                throw new SemanticException("partition_ttl_number is only supported for range partitioned materialized view");
            }
            curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER, String.valueOf(partitionTTL));
            materializedView.getTableProperty().setPartitionTTLNumber(partitionTTL);
            isChanged = true;
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION) &&
                ttlRetentionCondition != null &&
                !ttlRetentionCondition.equalsIgnoreCase(materializedView.getTableProperty().getPartitionRetentionCondition())) {
            curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION, ttlRetentionCondition);
            materializedView.getTableProperty().setPartitionRetentionCondition(ttlRetentionCondition);
            // re-analyze mv retention condition
            materializedView.analyzeMVRetentionCondition(context);
            isChanged = true;
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT) &&
                timeDriftConstraintSpec != null && !timeDriftConstraintSpec.equalsIgnoreCase(
                materializedView.getTableProperty().getTimeDriftConstraintSpec())) {
            curProp.put(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, timeDriftConstraintSpec);
            materializedView.getTableProperty().setTimeDriftConstraintSpec(timeDriftConstraintSpec);
            isChanged = true;
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER) &&
                materializedView.getTableProperty().getPartitionRefreshNumber() != partitionRefreshNumber) {
            curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, String.valueOf(partitionRefreshNumber));
            materializedView.getTableProperty().setPartitionRefreshNumber(partitionRefreshNumber);
            isChanged = true;
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY) &&
                !materializedView.getTableProperty().getPartitionRefreshStrategy().equals(partitionRefreshStrategy)) {
            curProp.put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY, String.valueOf(partitionRefreshStrategy));
            materializedView.getTableProperty().setPartitionRefreshStrategy(partitionRefreshStrategy);
            isChanged = true;
        } else if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE) &&
                !materializedView.getTableProperty().getMvRefreshMode().equals(mvRefreshMode)) {
            curProp.put(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE, String.valueOf(mvRefreshMode));
            materializedView.getTableProperty().setMvRefreshMode(mvRefreshMode);
            isChanged = true;
            materializedView.setCurrentRefreshMode(currentRefreshMode);
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
        if (propClone.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES)) {
            curProp.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES,
                    propClone.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES));
            materializedView.getTableProperty().setExcludedRefreshTables(excludedRefreshBaseTables);
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
                CachingMvPlanContextBuilder.getInstance().evictMaterializedViewCache(materializedView);
            } else {
                CachingMvPlanContextBuilder.getInstance().cacheMaterializedView(materializedView);
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

            MaterializedViewRefreshType newRefreshType = MaterializedViewRefreshType.getType(refreshSchemeDesc);
            MaterializedViewRefreshType oldRefreshType = materializedView.getRefreshScheme().getType();

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

    private void updateTaskDefinition(MaterializedView materializedView) {
        Task currentTask = GlobalStateMgr.getCurrentState().getTaskManager().getTask(
                TaskBuilder.getMvTaskName(materializedView.getId()));
        if (currentTask != null) {
            currentTask.setDefinition(materializedView.getTaskDefinition());
            currentTask.setPostRun(TaskBuilder.getAnalyzeMVStmt(materializedView.getName()));
        }
    }

    /**
     * Inactive the materialized view and its related materialized views.
     * <p>
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
        if (olapTable.getIndexNameToId().size() > 1) {
            Map<Long, MaterializedIndexMeta> metaMap = olapTable.getIndexIdToMeta();
            for (Map.Entry<Long, MaterializedIndexMeta> entry : metaMap.entrySet()) {
                Long id = entry.getKey();
                if (id == olapTable.getBaseIndexId()) {
                    continue;
                }
                MaterializedIndexMeta meta = entry.getValue();
                List<Column> schema = meta.getSchema();
                String indexName = olapTable.getIndexNameById(id);
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
                                            rollupCol.getDefineExpr().toSql();
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
                            String whereExprSql = whereExpr.toSql();
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
