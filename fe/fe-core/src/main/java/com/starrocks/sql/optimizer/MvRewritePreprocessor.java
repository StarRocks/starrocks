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

package com.starrocks.sql.optimizer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getMvPartialPartitionPredicates;

public class MvRewritePreprocessor {
    private static final Logger LOG = LogManager.getLogger(MvRewritePreprocessor.class);
    private final ConnectContext connectContext;
    private final ColumnRefFactory queryColumnRefFactory;
    private final OptimizerContext context;
    private final OptExpression logicalTree;
    private final ColumnRefSet requiredColumns;

    public MvRewritePreprocessor(ConnectContext connectContext,
                                 ColumnRefFactory queryColumnRefFactory,
                                 OptimizerContext context,
                                 OptExpression logicalTree,
                                 ColumnRefSet requiredColumns) {
        this.connectContext = connectContext;
        this.queryColumnRefFactory = queryColumnRefFactory;
        this.context = context;
        this.logicalTree = logicalTree;
        this.requiredColumns = requiredColumns;
    }

    public void prepare(OptExpression optExpression) {
        try (Timer ignored = Tracers.watchScope("preprocessMvs")) {
            // MV Rewrite will be used when cbo is enabled.
            if (context.getOptimizerConfig().isRuleBased()) {
                return;
            }

            Set<Table> queryTables = MvUtils.getAllTables(optExpression).stream().collect(Collectors.toSet());
            logMVParams(connectContext, queryTables);

            try {
                Set<MaterializedView> relatedMVs =
                        getRelatedMVs(connectContext, queryTables, context.getOptimizerConfig().isRuleBased());
                Set<Pair<MaterializedView, MvPlanContext>> validMVs = filterValidMVs(connectContext, relatedMVs);
                prepareRelatedMVs(queryTables, validMVs);
                if (!validMVs.isEmpty() && connectContext.getSessionVariable().isEnableViewBasedMvRewrite()) {
                    // if related mvs is empty, no need to process plans with view
                    processPlanWithView(connectContext, logicalTree, queryColumnRefFactory, requiredColumns);
                }
            } catch (Exception e) {
                List<String> tableNames = queryTables.stream().map(Table::getName).collect(Collectors.toList());
                LOG.warn("Prepare query tables {} for mv failed", tableNames, e);
            }
        }
    }

    private void logMVParams(ConnectContext connectContext, Set<Table> queryTables) {
        if (!Tracers.isSetTraceModule(Tracers.Module.MV)) {
            return;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        logMVPrepare(connectContext, "Query input tables: {}", queryTables);

        // enable or not
        logMVPrepare(connectContext, "---------------------------------");
        logMVPrepare(connectContext, "Materialized View Enable/Disable Params: ");
        logMVPrepare(connectContext, "  enable_experimental_mv: {}", Config.enable_experimental_mv);
        logMVPrepare(connectContext, "  enable_materialized_view_rewrite: {}",
                sessionVariable.isEnableMaterializedViewRewrite());
        logMVPrepare(connectContext, "  enable_materialized_view_union_rewrite: {}",
                sessionVariable.isEnableMaterializedViewUnionRewrite());
        logMVPrepare(connectContext, "  enable_materialized_view_view_delta_rewrite: {}",
                sessionVariable.isEnableMaterializedViewViewDeltaRewrite());
        logMVPrepare(connectContext, "  enable_materialized_view_single_table_view_delta_rewrite: {}",
                sessionVariable.isEnableMaterializedViewSingleTableViewDeltaRewrite());
        logMVPrepare(connectContext, "  enable_materialized_view_plan_cache: {}",
                sessionVariable.isEnableMaterializedViewPlanCache());
        logMVPrepare(connectContext, "  mv_auto_analyze_async: {}",
                Config.mv_auto_analyze_async);
        logMVPrepare(connectContext, "  enable_mv_automatic_active_check: {}",
                Config.enable_mv_automatic_active_check);
        logMVPrepare(connectContext, "  enable_sync_materialized_view_rewrite: {}",
                sessionVariable.isEnableSyncMaterializedViewRewrite());
        logMVPrepare(connectContext, "  enable_view_based_mv_rewrite: {}",
                sessionVariable.isEnableViewBasedMvRewrite());

        // limit
        logMVPrepare(connectContext, "---------------------------------");
        logMVPrepare(connectContext, "Materialized View Limit Params: ");
        logMVPrepare(connectContext, "  optimizer_materialized_view_timelimit: {}",
                sessionVariable.getOptimizerMaterializedViewTimeLimitMillis());
        logMVPrepare(connectContext, "  materialized_view_join_same_table_permutation_limit: {}",
                sessionVariable.getMaterializedViewJoinSameTablePermutationLimit());
        logMVPrepare(connectContext, "  skip_whole_phase_lock_mv_limit: {}",
                Config.skip_whole_phase_lock_mv_limit);

        // config
        logMVPrepare(connectContext, "---------------------------------");
        logMVPrepare(connectContext, "Materialized View Config Params: ");
        logMVPrepare(connectContext, "  analyze_mv: {}", sessionVariable.getAnalyzeForMV());
        logMVPrepare(connectContext, "  query_excluding_mv_names: {}", sessionVariable.getQueryExcludingMVNames());
        logMVPrepare(connectContext, "  query_including_mv_names: {}", sessionVariable.getQueryIncludingMVNames());
        logMVPrepare(connectContext, "  materialized_view_rewrite_mode: {}",
                sessionVariable.getMaterializedViewRewriteMode());
        logMVPrepare(connectContext, "---------------------------------");
    }

    private void processPlanWithView(ConnectContext connectContext,
                                     OptExpression logicOperatorTree,
                                     ColumnRefFactory columnRefFactory,
                                     ColumnRefSet requiredColumns) {
        List<LogicalViewScanOperator> viewScans = Lists.newArrayList();
        // process equivalent operator，construct logical plan with view
        OptExpression logicalPlanWithView = extractLogicalPlanWithView(logicOperatorTree, viewScans, columnRefFactory);
        if (viewScans.isEmpty()) {
            // means there is no plan with view
            return;
        }
        // optimize logical plan with view
        OptExpression optimizedPlan = optimizeViewPlan(
                logicalPlanWithView, connectContext, requiredColumns, columnRefFactory);
        context.setLogicalTreeWithView(optimizedPlan);
        context.setViewScans(viewScans);
    }

    private OptExpression optimizeViewPlan(
            OptExpression logicalTree,
            ConnectContext connectContext,
            ColumnRefSet requiredColumns,
            ColumnRefFactory columnRefFactory) {
        OptimizerConfig optimizerConfig = new OptimizerConfig(OptimizerConfig.OptimizerAlgorithm.RULE_BASED);
        optimizerConfig.disableRuleSet(RuleSetType.SINGLE_TABLE_MV_REWRITE);
        optimizerConfig.disableRuleSet(RuleSetType.MULTI_TABLE_MV_REWRITE);
        Optimizer optimizer = new Optimizer(optimizerConfig);
        OptExpression optimizedViewPlan = optimizer.optimize(connectContext, logicalTree,
                new PhysicalPropertySet(), requiredColumns, columnRefFactory);
        return optimizedViewPlan;
    }

    private OptExpression extractLogicalPlanWithView(
            OptExpression logicalTree,
            List<LogicalViewScanOperator> viewScans,
            ColumnRefFactory columnRefFactory) {
        List<OptExpression> inputs = Lists.newArrayList();
        if (logicalTree.getOp().getEquivalentOp() != null) {
            LogicalViewScanOperator viewScanOperator = logicalTree.getOp().getEquivalentOp().cast();
            // collect LogicalViewScanOperator to original logical tree,
            // which will be used in mv union rewrite
            // should use cloned plan because the following optimizeViewPlan will change the plan
            OptExpression clonePlan = MvUtils.cloneExpression(logicalTree);
            OptExpression optimizedViewPlan = optimizeViewPlan(
                    clonePlan, connectContext, viewScanOperator.getOutputColumnSet(), columnRefFactory);
            viewScanOperator.setOriginalPlan(optimizedViewPlan);
            viewScans.add(viewScanOperator);
            LogicalViewScanOperator.Builder builder = new LogicalViewScanOperator.Builder();
            builder.withOperator(viewScanOperator);
            builder.setProjection(null);
            LogicalViewScanOperator clone = builder.build();
            OptExpression viewScanExpr = OptExpression.create(clone);
            // should add a projection to make predicate pushdown rules work right
            Projection projection = viewScanOperator.getProjection();
            LogicalProjectOperator projectOperator = new LogicalProjectOperator(projection.getColumnRefMap());
            OptExpression projectionExpr = OptExpression.create(projectOperator, viewScanExpr);
            return projectionExpr;
        } else {
            for (OptExpression input : logicalTree.getInputs()) {
                OptExpression newInput = extractLogicalPlanWithView(input, viewScans, columnRefFactory);
                inputs.add(newInput);
            }
            Operator.Builder builder = OperatorBuilderFactory.build(logicalTree.getOp());
            builder.withOperator(logicalTree.getOp());
            Operator newOp = builder.build();
            return OptExpression.create(newOp, inputs);
        }
    }

    private MaterializedView copyOnlyMaterializedView(MaterializedView mv) {
        // TODO: add read lock?
        // Query will not lock dbs in the optimizer stage, so use a shallow copy of mv to avoid
        // metadata race for different operations.
        // Ensure to re-optimize if the mv's version has changed after the optimization.
        MaterializedView copiedMV = new MaterializedView();
        mv.copyOnlyForQuery(copiedMV);
        return copiedMV;
    }

    private static Set<MaterializedView> getRelatedMVs(ConnectContext connectContext,
                                                       Set<Table> queryTables,
                                                       boolean isRuleBased) {
        if (Config.enable_experimental_mv
                && connectContext.getSessionVariable().isEnableMaterializedViewRewrite()
                && !isRuleBased) {
            Set<MaterializedView> relatedMVs = getRelatedAsyncMVs(connectContext, queryTables);
            if (connectContext.getSessionVariable().isEnableSyncMaterializedViewRewrite()) {
                relatedMVs.addAll(getRelatedSyncMVs(connectContext, queryTables));
            }
            return relatedMVs;
        } else {
            return Sets.newHashSet();
        }
    }

    private static Set<Pair<MaterializedView, MvPlanContext>> filterValidMVs(ConnectContext connectContext,
                                                                             Set<MaterializedView> relatedMVs) {
        // filter mvs by including/excluding settings
        String queryExcludingMVNames = connectContext.getSessionVariable().getQueryExcludingMVNames();
        String queryIncludingMVNames = connectContext.getSessionVariable().getQueryIncludingMVNames();
        if (!Strings.isNullOrEmpty(queryExcludingMVNames) || !Strings.isNullOrEmpty(queryIncludingMVNames)) {
            logMVPrepare(connectContext, "queryExcludingMVNames:{}, queryIncludingMVNames:{}",
                    Strings.nullToEmpty(queryExcludingMVNames), Strings.nullToEmpty(queryIncludingMVNames));

            final Set<String> queryExcludingMVNamesSet = Strings.isNullOrEmpty(queryExcludingMVNames) ? Sets.newHashSet()
                    : Arrays.stream(queryExcludingMVNames.split(",")).map(String::trim).collect(Collectors.toSet());

            final Set<String> queryIncludingMVNamesSet = Strings.isNullOrEmpty(queryIncludingMVNames) ? Sets.newHashSet()
                    : Arrays.stream(queryIncludingMVNames.split(",")).map(String::trim).collect(Collectors.toSet());
            relatedMVs = relatedMVs.stream()
                    .filter(mv -> queryIncludingMVNamesSet.isEmpty() || queryIncludingMVNamesSet.contains(mv.getName()))
                    .filter(mv -> queryExcludingMVNamesSet.isEmpty() || !queryExcludingMVNamesSet.contains(mv.getName()))
                    .collect(Collectors.toSet());
        }

        // filter mvs which are active and have valid plans
        Set<Pair<MaterializedView, MvPlanContext>> filteredMVs = Sets.newHashSet();
        for (MaterializedView mv : relatedMVs) {
            try {
                if (!mv.isActive()) {
                    logMVPrepare(connectContext, mv, "MV is not active: {}", mv.getName());
                    continue;
                }

                List<MvPlanContext> mvPlanContexts = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv,
                        connectContext.getSessionVariable().isEnableMaterializedViewPlanCache());
                if (mvPlanContexts == null) {
                    logMVPrepare(connectContext, mv, "MV plan is not valid: {}, cannot generate plan for rewrite",
                            mv.getName());
                    continue;
                }
                for (MvPlanContext mvPlanContext : mvPlanContexts) {
                    if (!mvPlanContext.isValidMvPlan()) {
                        if (mvPlanContext.getLogicalPlan() != null) {
                            logMVPrepare(connectContext, mv, "MV plan is not valid: {}, plan:\n {}",
                                    mv.getName(), mvPlanContext.getLogicalPlan().debugString());
                        } else {
                            logMVPrepare(connectContext, mv, "MV plan is not valid: {}",
                                    mv.getName());
                        }
                        continue;
                    }
                    filteredMVs.add(Pair.create(mv, mvPlanContext));
                }
            } catch (Exception e) {
                LOG.warn("filter check failed mv:{}", mv.getName(), e);
            }
        }
        if (filteredMVs.isEmpty()) {
            logMVPrepare(connectContext, "There are no valid related mvs for the query plan");
        }
        return filteredMVs;
    }

    private static Set<MaterializedView> getRelatedAsyncMVs(ConnectContext connectContext,
                                                            Set<Table> queryTables) {
        // get all related materialized views, include nested mvs
        return MvUtils.getRelatedMvs(connectContext,
                connectContext.getSessionVariable().getNestedMvRewriteMaxLevel(), queryTables);
    }

    private static Set<MaterializedView> getRelatedSyncMVs(ConnectContext connectContext,
                                                           Set<Table> queryTables) {
        Set<MaterializedView> relatedMvs = Sets.newHashSet();
        // get all related materialized views, include nested mvs
        for (Table table : queryTables) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            relatedMvs.addAll(getTableRelatedSyncMVs(connectContext, olapTable));
        }
        return relatedMvs;
    }

    private static Set<MaterializedView> getTableRelatedSyncMVs(ConnectContext connectContext,
                                                                OlapTable olapTable) {
        Set<MaterializedView> relatedMvs = Sets.newHashSet();
        for (MaterializedIndexMeta indexMeta : olapTable.getVisibleIndexMetas()) {
            long indexId = indexMeta.getIndexId();
            if (indexMeta.getIndexId() == olapTable.getBaseIndexId()) {
                continue;
            }
            // Old sync mv may not contain the index define sql.
            if (Strings.isNullOrEmpty(indexMeta.getViewDefineSql())) {
                continue;
            }

            // To avoid adding optimization times, only put the mv with complex expressions into materialized views.
            if (!MVUtils.containComplexExpresses(indexMeta)) {
                continue;
            }

            try {
                long dbId = indexMeta.getDbId();
                String viewDefineSql = indexMeta.getViewDefineSql();
                String mvName = olapTable.getIndexNameById(indexId);
                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);

                // distribution info
                DistributionInfo baseTableDistributionInfo = olapTable.getDefaultDistributionInfo();
                DistributionInfo mvDistributionInfo = baseTableDistributionInfo.copy();
                Set<String> mvColumnNames =
                        indexMeta.getSchema().stream().map(Column::getName).collect(Collectors.toSet());
                if (baseTableDistributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) baseTableDistributionInfo;
                    Set<String> distributedColumns =
                            hashDistributionInfo.getDistributionColumns().stream().map(Column::getName)
                                    .collect(Collectors.toSet());
                    // NOTE: SyncMV's column may not be equal to base table's exactly.
                    List<Column> newDistributionColumns = Lists.newArrayList();
                    for (Column mvColumn : indexMeta.getSchema()) {
                        if (distributedColumns.contains(mvColumn.getName())) {
                            newDistributionColumns.add(mvColumn);
                        }
                    }
                    // Set random distribution info if sync mv' columns may not contain distribution keys,
                    if (newDistributionColumns.size() != distributedColumns.size()) {
                        mvDistributionInfo = new RandomDistributionInfo();
                    } else {
                        ((HashDistributionInfo) mvDistributionInfo).setDistributionColumns(newDistributionColumns);
                    }
                }
                // partition info
                PartitionInfo basePartitionInfo = olapTable.getPartitionInfo();
                PartitionInfo mvPartitionInfo = basePartitionInfo;
                // Set single partition if sync mv' columns do not contain partition by columns.
                if (basePartitionInfo.isPartitioned()) {
                    if (basePartitionInfo.getPartitionColumns().stream()
                            .anyMatch(x -> !mvColumnNames.contains(x.getName())) ||
                            !(basePartitionInfo instanceof ExpressionRangePartitionInfo)) {
                        mvPartitionInfo = new SinglePartitionInfo();
                    }
                }
                // refresh schema
                MaterializedView.MvRefreshScheme mvRefreshScheme =
                        new MaterializedView.MvRefreshScheme(MaterializedView.RefreshType.SYNC);
                MaterializedView mv = new MaterializedView(db, mvName, indexMeta, olapTable,
                        mvPartitionInfo, mvDistributionInfo, mvRefreshScheme);
                mv.setViewDefineSql(viewDefineSql);
                mv.setBaseIndexId(indexId);
                relatedMvs.add(mv);
            } catch (Exception e) {
                logMVPrepare(connectContext, "Fail to get the related sync materialized views from table:{}, " +
                        "exception:{}", olapTable.getName(), e);
            }
        }
        return relatedMvs;
    }

    public void prepareRelatedMVs(Set<Table> queryTables,
                                  Set<Pair<MaterializedView, MvPlanContext>> relatedMvs) {
        if (relatedMvs.isEmpty()) {
            return;
        }

        Set<ColumnRefOperator> originQueryColumns = Sets.newHashSet(queryColumnRefFactory.getColumnRefs());
        for (Pair<MaterializedView, MvPlanContext> pair : relatedMvs) {
            MaterializedView mv = pair.first;
            MvPlanContext mvPlanContext = pair.second;
            try {
                preprocessMv(mv, mvPlanContext, queryTables, originQueryColumns);
            } catch (Exception e) {
                List<String> tableNames = queryTables.stream().map(Table::getName).collect(Collectors.toList());
                logMVPrepare(connectContext, "Preprocess MV {} failed: {}", mv.getName(), e.getMessage());
                LOG.warn("Preprocess mv {} failed for query tables:{}", mv.getName(), tableNames, e);
            }
        }
        // all base table related mvs
        List<String> relatedMvNames =
                relatedMvs.stream().map(pair -> pair.first.getName()).collect(Collectors.toList());
        // all mvs that match SPJG pattern and can ben used to try mv rewrite
        List<String> candidateMvNames = context.getCandidateMvs().stream()
                .map(materializationContext -> materializationContext.getMv().getName())
                .collect(Collectors.toList());
        logMVPrepare(connectContext, "RelatedMVs: {}, CandidateMVs: {}", relatedMvNames,
                candidateMvNames);
    }

    private void preprocessMv(MaterializedView mv, MvPlanContext mvPlanContext,
                              Set<Table> queryTables,
                              Set<ColumnRefOperator> originQueryColumns) throws AnalysisException {
        Preconditions.checkState(mvPlanContext != null);
        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        Preconditions.checkState(mvPlan != null);

        Set<String> partitionNamesToRefresh = Sets.newHashSet();
        if (!mv.getPartitionNamesToRefreshForMv(partitionNamesToRefresh, true)) {
            logMVPrepare(connectContext, mv, "MV {} cannot be used for rewrite, " +
                    "stale partitions {}", mv.getName(), partitionNamesToRefresh);
            return;
        }
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            if (!partitionNamesToRefresh.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (BaseTableInfo base : mv.getBaseTableInfos()) {
                    Optional<Table> baseTable = base.mayGetTable();
                    if (!baseTable.isPresent() || baseTable.get().isView()) {
                        continue;
                    }
                    String versionInfo = Joiner.on(",").join(mv.getBaseTableLatestPartitionInfo(baseTable.get()));
                    sb.append(String.format("base table %s version: %s; ", base, versionInfo));
                }
                logMVPrepare(connectContext, mv, "MV {} is outdated, stale partitions {}, detailed version info: {}",
                        mv.getName(), partitionNamesToRefresh, sb.toString());
                return;
            }
        } else if (!mv.getPartitionNames().isEmpty() && partitionNamesToRefresh.containsAll(mv.getPartitionNames())) {
            // if the mv is partitioned, and all partitions need refresh,
            // then it can not be a candidate

            StringBuilder sb = new StringBuilder();
            try {
                for (BaseTableInfo base : mv.getBaseTableInfos()) {
                    if (!base.mayGetTable().isPresent()) {
                        continue;
                    }
                    String versionInfo = Joiner.on(",").join(mv.getBaseTableLatestPartitionInfo(base.getTable()));
                    sb.append(String.format("base table %s version: %s; ", base, versionInfo));
                }
            } catch (Exception e) {
                // ignore exception for `getPartitions` is only supported for hive/jdbc.
            }
            logMVPrepare(connectContext, mv, "MV {} is outdated and all its partitions need to be " +
                    "refreshed: {}, refreshed mv partitions: {}, base table detailed info: {}", mv.getName(),
                    partitionNamesToRefresh, mv.getPartitionNames(), sb.toString());
            return;
        }

        ScalarOperator mvPartialPartitionPredicates = null;
        if (mv.getPartitionInfo() instanceof ExpressionRangePartitionInfo && !partitionNamesToRefresh.isEmpty()) {
            // when mv is partitioned and there are some refreshed partitions,
            // when should calculate the latest partition range predicates for partition-by base table
            mvPartialPartitionPredicates = getMvPartialPartitionPredicates(mv, mvPlan, partitionNamesToRefresh);
            if (mvPartialPartitionPredicates == null) {
                logMVPrepare(connectContext, mv, "Partitioned MV {} is outdated which contains some partitions " +
                        "to be refreshed: {}, and cannot compensate it to predicate", mv.getName(), partitionNamesToRefresh);
                return;
            }
        }
        logMVPrepare(connectContext, mv, "MV' partitions to refresh: {}", partitionNamesToRefresh);
        logMVPrepare(connectContext, mv, "MV compensate partition predicate: {}", mvPartialPartitionPredicates);

        // Add mv info into dump info
        if (connectContext.getDumpInfo() != null) {
            String dbName = connectContext.getGlobalStateMgr().getDb(mv.getDbId()).getFullName();
            connectContext.getDumpInfo().addTable(dbName, mv);
        }

        List<Table> baseTables = MvUtils.getAllTables(mvPlan);
        List<Table> intersectingTables = baseTables.stream().filter(queryTables::contains).collect(Collectors.toList());
        Pair<Table, Column> partitionTableAndColumns = mv.getDirectTableAndPartitionColumn();

        // Only record `refTableUpdatedPartitionNames` when `mvPartialPartitionPredicates` is not null and it needs
        // to be compensated by using it.
        Set<String> refTableUpdatedPartitionNames = null;
        if (mvPartialPartitionPredicates != null) {
            Table refBaseTable = partitionTableAndColumns.first;
            refTableUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfTable(refBaseTable, true);
            logMVPrepare(connectContext, mv, "Ref table {} partitions to refresh: {}", refBaseTable.getName(),
                    refTableUpdatedPartitionNames);
        }


        // If query tables are set which means use related mv for non lock optimization,
        // copy mv's metadata into a ready-only object.
        MaterializedView copiedMV = (context.getQueryTables() != null) ? copyOnlyMaterializedView(mv) : mv;
        MaterializationContext materializationContext =
                new MaterializationContext(context, copiedMV, mvPlan, queryColumnRefFactory,
                        mvPlanContext.getRefFactory(), partitionNamesToRefresh,
                        baseTables, originQueryColumns, intersectingTables,
                        mvPartialPartitionPredicates, refTableUpdatedPartitionNames);
        List<ColumnRefOperator> mvOutputColumns = mvPlanContext.getOutputColumns();
        // generate scan mv plan here to reuse it in rule applications
        LogicalOlapScanOperator scanMvOp = createScanMvOperator(materializationContext, partitionNamesToRefresh);
        materializationContext.setScanMvOperator(scanMvOp);
        // should keep the sequence of schema
        List<ColumnRefOperator> scanMvOutputColumns = Lists.newArrayList();
        for (Column column : getMvOutputColumns(copiedMV)) {
            scanMvOutputColumns.add(scanMvOp.getColumnReference(column));
        }
        Preconditions.checkState(mvOutputColumns.size() == scanMvOutputColumns.size());

        // construct output column mapping from mv sql to mv scan operator
        // eg: for mv1 sql define: select a, (b + 1) as c2, (a * b) as c3 from table;
        // select sql plan output columns:    a, b + 1, a * b
        //                                    |    |      |
        //                                    v    v      V
        // mv scan operator output columns:  a,   c2,    c3
        Map<ColumnRefOperator, ColumnRefOperator> outputMapping = Maps.newHashMap();
        for (int i = 0; i < mvOutputColumns.size(); i++) {
            outputMapping.put(mvOutputColumns.get(i), scanMvOutputColumns.get(i));
        }
        materializationContext.setOutputMapping(outputMapping);
        context.addCandidateMvs(materializationContext);
        logMVPrepare(connectContext, copiedMV, "Prepare MV {} success", copiedMV.getName());
    }

    public List<Column> getMvOutputColumns(MaterializedView mv) {
        if (mv.getQueryOutputIndices() == null || mv.getQueryOutputIndices().isEmpty()) {
            return mv.getBaseSchema();
        } else {
            List<Column> schema = mv.getBaseSchema();
            List<Column> outputColumns = Lists.newArrayList();
            for (Integer index : mv.getQueryOutputIndices()) {
                outputColumns.add(schema.get(index));
            }
            return outputColumns;
        }
    }

    /**
     * Make a LogicalOlapScanOperator by using MV's schema which includes:
     * - partition infos.
     * - distribution infos.
     * - original MV's predicates which can be deduced from MV opt expression and be used
     * for partition/distribution pruning.
     */
    private LogicalOlapScanOperator createScanMvOperator(MaterializationContext mvContext,
                                                         Set<String> excludedPartitions) {
        final MaterializedView mv = mvContext.getMv();

        final ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumnMetaMapBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<Column, ColumnRefOperator> columnMetaToColRefMapBuilder = ImmutableMap.builder();

        final ColumnRefFactory columnRefFactory = mvContext.getQueryRefFactory();
        int relationId = columnRefFactory.getNextRelationId();

        // first add base schema to avoid replaced in full schema.
        Set<String> columnNames = Sets.newHashSet();
        for (Column column : mv.getBaseSchema()) {
            ColumnRefOperator columnRef = columnRefFactory.create(column.getName(),
                    column.getType(),
                    column.isAllowNull());
            columnRefFactory.updateColumnToRelationIds(columnRef.getId(), relationId);
            columnRefFactory.updateColumnRefToColumns(columnRef, column, mv);
            colRefToColumnMetaMapBuilder.put(columnRef, column);
            columnMetaToColRefMapBuilder.put(column, columnRef);
            columnNames.add(column.getName());
        }
        for (Column column : mv.getFullSchema()) {
            if (columnNames.contains(column.getName())) {
                continue;
            }
            ColumnRefOperator columnRef = columnRefFactory.create(column.getName(),
                    column.getType(),
                    column.isAllowNull());
            columnRefFactory.updateColumnToRelationIds(columnRef.getId(), relationId);
            columnRefFactory.updateColumnRefToColumns(columnRef, column, mv);
            colRefToColumnMetaMapBuilder.put(columnRef, column);
            columnMetaToColRefMapBuilder.put(column, columnRef);
        }
        final Map<Column, ColumnRefOperator> columnMetaToColRefMap = columnMetaToColRefMapBuilder.build();

        // construct partition
        List<Long> selectPartitionIds = Lists.newArrayList();
        List<Long> selectTabletIds = Lists.newArrayList();
        List<String> selectedPartitionNames = Lists.newArrayList();
        for (Partition p : mv.getPartitions()) {
            if (!excludedPartitions.contains(p.getName()) && p.hasData()) {
                selectPartitionIds.add(p.getId());
                selectedPartitionNames.add(p.getName());
                for (PhysicalPartition physicalPartition : p.getSubPartitions()) {
                    MaterializedIndex materializedIndex = physicalPartition.getIndex(mv.getBaseIndexId());
                    selectTabletIds.addAll(materializedIndex.getTabletIdsInOrder());
                }
            }
        }
        final PartitionNames partitionNames = new PartitionNames(false, selectedPartitionNames);

        return LogicalOlapScanOperator.builder()
                .setTable(mv)
                .setColRefToColumnMetaMap(colRefToColumnMetaMapBuilder.build())
                .setColumnMetaToColRefMap(columnMetaToColRefMap)
                .setDistributionSpec(getTableDistributionSpec(mvContext, columnMetaToColRefMap))
                .setSelectedIndexId(mv.getBaseIndexId())
                .setSelectedPartitionId(selectPartitionIds)
                .setPartitionNames(partitionNames)
                .setSelectedTabletId(selectTabletIds)
                .setHintsTabletIds(Collections.emptyList())
                .setHintsReplicaIds(Collections.emptyList())
                .setHasTableHints(false)
                .setUsePkIndex(false)
                .build();
    }

    private DistributionSpec getTableDistributionSpec(
            MaterializationContext mvContext, Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        final MaterializedView mv = mvContext.getMv();

        DistributionSpec distributionSpec = null;
        // construct distribution
        DistributionInfo distributionInfo = mv.getDefaultDistributionInfo();
        if (distributionInfo.getType() == DistributionInfoType.HASH) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            List<Column> distributedColumns = hashDistributionInfo.getDistributionColumns();
            List<Integer> hashDistributeColumns = new ArrayList<>();
            for (Column distributedColumn : distributedColumns) {
                hashDistributeColumns.add(columnMetaToColRefMap.get(distributedColumn).getId());
            }
            final HashDistributionDesc hashDistributionDesc =
                    new HashDistributionDesc(hashDistributeColumns, HashDistributionDesc.SourceType.LOCAL);
            distributionSpec = DistributionSpec.createHashDistributionSpec(hashDistributionDesc);
        } else if (distributionInfo.getType() == DistributionInfoType.RANDOM) {
            distributionSpec = DistributionSpec.createAnyDistributionSpec();
        }

        return distributionSpec;
    }
}