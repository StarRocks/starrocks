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

import com.google.common.annotations.VisibleForTesting;
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
import com.starrocks.catalog.MvUpdateInfo;
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
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvRewriteStrategy;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.MvRefreshArbiter.getPartitionNamesToRefreshForMv;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator.getMvPartialPartitionPredicates;

public class MvRewritePreprocessor {
    private static final Logger LOG = LogManager.getLogger(MvRewritePreprocessor.class);
    private final ConnectContext connectContext;
    private final ColumnRefFactory queryColumnRefFactory;
    private final OptimizerContext context;
    private final ColumnRefSet requiredColumns;

    public MvRewritePreprocessor(ConnectContext connectContext,
                                 ColumnRefFactory queryColumnRefFactory,
                                 OptimizerContext context,
                                 ColumnRefSet requiredColumns) {
        this.connectContext = connectContext;
        this.queryColumnRefFactory = queryColumnRefFactory;
        this.context = context;
        this.requiredColumns = requiredColumns;
    }

    @VisibleForTesting
    public class MvWithPlanContext {
        private final MaterializedView mv;
        private final MvPlanContext mvPlanContext;
        public MvWithPlanContext(MaterializedView mv, MvPlanContext mvPlanContext) {
            this.mv = mv;
            this.mvPlanContext = mvPlanContext;
        }

        public MaterializedView getMv() {
            return mv;
        }

        public MvPlanContext getMvPlanContext() {
            return mvPlanContext;
        }
    }

    /**
     * To avoid MvRewriteProcessor cost too much optimizer time, reduce all related mvs to a limited size.
     * <h3>Why to Choose The Best Related MVs Strategy</h3>
     *
     * <p>Why still to choose limited related mvs from all active mvs?</p>
     *
     * <p>1. optimizer time cost. Even there is MVPlanCache to reduce mv optimizer plan time, but it still may cost
     * too much time for mv preprocessor, because mv optimizer plan costs too much time because of cold start when
     * MVPlanCache has no cache, or MVPlanCache has exceeded limited capacity which is 1000 by default.</p>
     *
     * <p>2. check mv's freshness for each mv will cost too much time if there are too many mvs.</p>
     *
     * <h3>How to Choose The Best Related MVs Strategy</h3>
     * <p>
     *     Choose the best related mvs from all active mvs as following order:
     *     1. find the max intersected table num between mv and query which means it's better for rewrite.
     *     2. find the latest fresh mv which means its freshness is better.
     * </p>
     *
     * <h3>More Information</h3>
     * <p>
     *  NOTE: there are still some limitations about this ordering algorithm:
     *  1. consider repeated tables in one mv later which one table can be used repeatedly in one mv.
     *  2. consider random factor so can cache and use more mvs.
     * </p>
     */
    public static class MVCorrelation implements Comparable<MVCorrelation> {
        private final MaterializedView mv;
        private final long mvQueryIntersectedTablesNum;
        private final int mvQueryScanOpNumDiff;
        private final long mvRefreshTimestamp;

        public MVCorrelation(MaterializedView mv,
                             long mvQueryIntersectedTablesNum,
                             int mvQueryScanOpNumDiff,
                             long mvRefreshTimestamp) {
            this.mv = mv;
            this.mvQueryIntersectedTablesNum = mvQueryIntersectedTablesNum;
            this.mvQueryScanOpNumDiff = mvQueryScanOpNumDiff;
            this.mvRefreshTimestamp = mvRefreshTimestamp;
        }

        public MaterializedView getMv() {
            return this.mv;
        }

        public static long getMvQueryIntersectedTableNum(List<BaseTableInfo> baseTableInfos,
                                                         Set<String> queryTableNames) {
            return baseTableInfos.stream()
                    .filter(baseTableInfo -> {
                        String baseTableName = baseTableInfo.getTableName();
                        // assert not null
                        if (Strings.isNullOrEmpty(baseTableName)) {
                            return false;
                        }
                        return queryTableNames.contains(baseTableName);
                    }).count();
        }

        public static int getMvQueryScanOpDiff(List<MvPlanContext> planContexts,
                                               int mvBaseTableSize,
                                               int queryScanOpNum) {
            int diff = Math.abs(queryScanOpNum - mvBaseTableSize);
            if (planContexts == null || planContexts.isEmpty()) {
                return diff;
            }
            return planContexts.stream()
                    .map(mvPlanContext -> mvPlanContext.getMvScanOpNum())
                    .map(num -> Math.abs(queryScanOpNum - num))
                    .min(Comparator.comparing(Integer::intValue))
                    .orElse(diff);
        }

        @Override
        public int compareTo(@NotNull MVCorrelation other) {
            // 1. compare intersected table nums, larger is better.
            int result = Long.compare(this.mvQueryIntersectedTablesNum, other.mvQueryIntersectedTablesNum);
            if (result != 0) {
                return result;
            }
            // 2. compare base table num diff,  less is better
            result = Integer.compare(other.mvQueryScanOpNumDiff, this.mvQueryScanOpNumDiff);
            if (result != 0) {
                return result;
            }
            // 3. compare refresh timestamp, larger is better.
            return Long.compare(this.mvRefreshTimestamp, other.mvRefreshTimestamp);
        }

        @Override
        public String toString() {
            return String.format("Correlation: mv=%s, mvQueryInteractedTablesNum=%s, " +
                    "mvQueryScanOpNumDiff=%s, mvRefreshTimestamp=%s", mv.getName(),
                    mvQueryIntersectedTablesNum, mvQueryScanOpNumDiff, mvRefreshTimestamp);
        }
    }

    public void prepare(OptExpression queryOptExpression, MvRewriteStrategy strategy) {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        // MV Rewrite will be used when cbo is enabled.
        if (context.getOptimizerConfig().isRuleBased() || sessionVariable.isDisableMaterializedViewRewrite() ||
                !sessionVariable.isEnableMaterializedViewRewrite()) {
            return;
        }

        try (Timer ignored = Tracers.watchScope("preprocessMvs")) {
            Set<Table> queryTables = MvUtils.getAllTables(queryOptExpression).stream().collect(Collectors.toSet());
            logMVParams(connectContext, queryTables);

            QueryMaterializationContext queryMaterializationContext = new QueryMaterializationContext();
            try {
                // 1. get related mvs for all input tables
                Set<MaterializedView> relatedMVs = getRelatedMVs(queryTables, context.getOptimizerConfig().isRuleBased());

                // 2. choose best related mvs by user's config or related mv limit
                Set<MaterializedView> selectedRelatedMVs;
                try (Timer t1 = Tracers.watchScope("chooseCandidates")) {
                    selectedRelatedMVs = chooseBestRelatedMVs(queryTables, relatedMVs, queryOptExpression);
                }

                // 3. convert to mv with planContext, skip if mv has no valid plan(not SPJG)
                Set<MvWithPlanContext> mvWithPlanContexts;
                try (Timer t2 = Tracers.watchScope("generateMvPlan")) {
                    mvWithPlanContexts = getMvWithPlanContext(selectedRelatedMVs);
                }

                // 4. process related mvs to candidates
                try (Timer t3 = Tracers.watchScope("validateMv")) {
                    prepareRelatedMVs(queryTables, mvWithPlanContexts);
                }

                // 5. process relate mvs with views
                try (Timer t4 = Tracers.watchScope("mvWithView")) {
                    processPlanWithView(queryMaterializationContext, connectContext, queryOptExpression,
                            queryColumnRefFactory, requiredColumns);
                }

                // add queryMaterializationContext into context
                if (context.getCandidateMvs() != null && !context.getCandidateMvs().isEmpty()) {
                    context.setQueryMaterializationContext(queryMaterializationContext);
                }

                // initialize mv rewrite strategy finally
                MvRewriteStrategy.prepareRewriteStrategy(context, connectContext, queryOptExpression, strategy);
            } catch (Exception e) {
                List<String> tableNames = queryTables.stream().map(Table::getName).collect(Collectors.toList());
                logMVPrepare(connectContext, "Prepare query tables {} for mv failed:{}", tableNames, e.getMessage());
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
        logMVPrepare(connectContext, "  enable_view_based_mv_rewrite: {}",
                sessionVariable.isEnableViewBasedMvRewrite());
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
        logMVPrepare(connectContext, "  cbo_materialized_view_rewrite_rule_output_limit: {}",
                sessionVariable.getCboMaterializedViewRewriteRuleOutputLimit());
        logMVPrepare(connectContext, "  cbo_materialized_view_rewrite_candidate_limit: {}",
                sessionVariable.getCboMaterializedViewRewriteCandidateLimit());
        logMVPrepare(connectContext, "  cbo_materialized_view_rewrite_related_mvs_limit: {}",
                sessionVariable.getCboMaterializedViewRewriteRelatedMVsLimit());
        logMVPrepare(connectContext, "  materialized_view_rewrite_mode: {}",
                sessionVariable.getMaterializedViewRewriteMode());
        logMVPrepare(connectContext, "---------------------------------");
    }

    private void processPlanWithView(QueryMaterializationContext queryMaterializationContext,
                                     ConnectContext connectContext,
                                     OptExpression logicOperatorTree,
                                     ColumnRefFactory columnRefFactory,
                                     ColumnRefSet requiredColumns) {
        if (!connectContext.getSessionVariable().isEnableViewBasedMvRewrite()) {
            return;
        }
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
        queryMaterializationContext.setLogicalTreeWithView(optimizedPlan);
        queryMaterializationContext.setViewScans(viewScans);
    }

    private OptExpression optimizeViewPlan(OptExpression logicalTree,
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

    private OptExpression extractLogicalPlanWithView(OptExpression logicalTree,
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

    private static MaterializedView copyOnlyMaterializedView(MaterializedView mv) {
        // TODO: add read lock?
        // Query will not lock dbs in the optimizer stage, so use a shallow copy of mv to avoid
        // metadata race for different operations.
        // Ensure to re-optimize if the mv's version has changed after the optimization.
        MaterializedView copiedMV = new MaterializedView();
        mv.copyOnlyForQuery(copiedMV);
        return copiedMV;
    }

    @VisibleForTesting
    public Set<MaterializedView> getRelatedMVs(Set<Table> queryTables,
                                               boolean isRuleBased) {
        if (Config.enable_experimental_mv
                && connectContext.getSessionVariable().isEnableMaterializedViewRewrite()
                && !isRuleBased) {
            // related asynchronous materialized views
            Set<MaterializedView> relatedMVs = getRelatedAsyncMVs(queryTables);

            // related synchronous materialized views
            if (connectContext.getSessionVariable().isEnableSyncMaterializedViewRewrite()) {
                relatedMVs.addAll(getRelatedSyncMVs(queryTables));
            }
            return relatedMVs;
        } else {
            return Sets.newHashSet();
        }
    }

    private static Set<String> splitQueryMVNamesConfig(String str) {
        if (Strings.isNullOrEmpty(str)) {
            return Sets.newHashSet();
        }
        return Arrays.stream(str.split(",")).map(String::trim).collect(Collectors.toSet());
    }

    private Set<MaterializedView> getRelatedMVsByConfig(Set<MaterializedView> relatedMVs) {
        // filter mvs by including/excluding settings
        String queryExcludingMVNames = connectContext.getSessionVariable().getQueryExcludingMVNames();
        String queryIncludingMVNames = connectContext.getSessionVariable().getQueryIncludingMVNames();
        if (Strings.isNullOrEmpty(queryExcludingMVNames) && Strings.isNullOrEmpty(queryIncludingMVNames)) {
            return relatedMVs;
        }
        logMVPrepare(connectContext, "queryExcludingMVNames:{}, queryIncludingMVNames:{}",
                Strings.nullToEmpty(queryExcludingMVNames), Strings.nullToEmpty(queryIncludingMVNames));

        final Set<String> queryExcludingMVNamesSet = splitQueryMVNamesConfig(queryExcludingMVNames);
        final Set<String> queryIncludingMVNamesSet = splitQueryMVNamesConfig(queryIncludingMVNames);

        return relatedMVs.stream()
                .filter(mv -> queryIncludingMVNamesSet.isEmpty() || queryIncludingMVNamesSet.contains(mv.getName()))
                .filter(mv -> queryExcludingMVNamesSet.isEmpty() || !queryExcludingMVNamesSet.contains(mv.getName()))
                .collect(Collectors.toSet());
    }

    private List<MvWithPlanContext> getMVWithContext(MaterializedView mv) {
        if (!mv.isActive()) {
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "inactive");
            return null;
        }

        List<MvPlanContext> mvPlanContexts = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv,
                connectContext.getSessionVariable().isEnableMaterializedViewPlanCache());
        if (CollectionUtils.isEmpty(mvPlanContexts)) {
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "invalid query plan");
            return null;
        }
        List<MvWithPlanContext> mvWithPlanContexts = Lists.newArrayList();
        for (int i = 0; i < mvPlanContexts.size(); i++) {
            MvPlanContext mvPlanContext = mvPlanContexts.get(i);
            if (!mvPlanContext.isValidMvPlan()) {
                OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "invalid query plan {}/{}: {}",
                        i, mvPlanContexts.size(), mvPlanContext.getInvalidReason());
                continue;
            }
            mvWithPlanContexts.add(new MvWithPlanContext(mv, mvPlanContext));
        }
        return mvWithPlanContexts;
    }

    private static boolean canMVRewriteIfMVHasExtraTables(ConnectContext connectContext,
                                                          MaterializedView mv,
                                                          Set<Table> queryTables) {
        // 1. when mv has foreign key constraints, it's ok whether query has extra tables or mv has extra tables.
        if (mv.hasForeignKeyConstraints()) {
            return true;
        }
        Set<Table> baseTables = mv.getBaseTableInfos().stream().map(x -> MvUtils.getTableChecked(x))
                .filter(x -> !x.isView() && !x.isMaterializedView())
                .collect(Collectors.toSet());
        Set<Table> extraTables =  baseTables.stream().filter(t -> !queryTables.contains(t)).collect(Collectors.toSet());
        if (extraTables.isEmpty()) {
            return true;
        }
        // 2. otherwise extra base tables should contain foreign constraints
        if (extraTables.stream().anyMatch(baseTable -> !(baseTable.hasForeignKeyConstraints() ||
                baseTable.hasUniqueConstraints()))) {
            Set<String> extraTableNames = extraTables.stream().map(Table::getName).collect(Collectors.toSet());
            logMVPrepare(connectContext, mv, "Exclude mv {} because it contains extra base tables: {}",
                    mv.getName(), Joiner.on(",").join(extraTableNames));
            return false;
        }
        return true;
    }

    /**
     * Check if the MV is eligible for query rewrite
     *
     * @param force build the MV plan even if it's not in the plan cache
     */
    public static Pair<Boolean, String> isMVValidToRewriteQuery(ConnectContext connectContext,
                                                                MaterializedView mv,
                                                                boolean force,
                                                                Set<Table> queryTables) {
        if (!mv.isActive())  {
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "is not active");
            return Pair.create(false, "MV is not active");
        }
        if (!mv.isEnableRewrite()) {
            String message = PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE + "=" +
                    mv.getTableProperty().getMvQueryRewriteSwitch();
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), message);
            return Pair.create(false, message);
        }
        // if mv is a subset of query tables, it can be used for rewrite.
        if (CollectionUtils.isNotEmpty(queryTables) &&
                !canMVRewriteIfMVHasExtraTables(connectContext, mv, queryTables)) {
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "MV contains extra tables besides FK-PK");
            return Pair.create(false, "MV contains extra tables besides FK-PK");
        }
        // if mv is in plan cache(avoid building plan), check whether it's valid
        if (connectContext == null || connectContext.getSessionVariable().isEnableMaterializedViewPlanCache()) {
            List<MvPlanContext> planContexts = force ?
                            CachingMvPlanContextBuilder.getInstance().getPlanContext(mv, false) :
                            CachingMvPlanContextBuilder.getInstance().getPlanContextFromCacheIfPresent(mv);
            if (CollectionUtils.isNotEmpty(planContexts) &&
                    planContexts.stream().noneMatch(MvPlanContext::isValidMvPlan)) {
                logMVPrepare(connectContext, "MV {} has no valid plan from {} plan contexts",
                        mv.getName(), planContexts.size());
                String message = planContexts.stream()
                        .map(MvPlanContext::getInvalidReason)
                        .collect(Collectors.joining(";"));
                OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), message);
                return Pair.create(false, "no valid plan: " + message);
            }
        }
        return Pair.create(true, null);
    }

    private Set<MaterializedView> chooseBestRelatedMVsByCorrelations(Set<Table> queryTables,
                                                                     Set<MaterializedView> validMVs,
                                                                     OptExpression queryOptExpression,
                                                                     int maxRelatedMVsLimit) {
        int queryScanOpNum = MvUtils.getOlapScanNode(queryOptExpression).size();
        Set<String> queryTableNames = queryTables.stream().map(t -> t.getName()).collect(Collectors.toSet());
        Queue<MVCorrelation> bestRelatedMVs = new PriorityQueue<>(maxRelatedMVsLimit);
        for (MaterializedView mv : validMVs) {
            List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
            long mvQueryInteractedTableNum = MVCorrelation.getMvQueryIntersectedTableNum(baseTableInfos, queryTableNames);
            List<MvPlanContext> planContexts =
                    CachingMvPlanContextBuilder.getInstance().getPlanContextFromCacheIfPresent(mv);
            int mvQueryScanOpDiff = MVCorrelation.getMvQueryScanOpDiff(planContexts, baseTableInfos.size(), queryScanOpNum);
            MVCorrelation mvCorrelation = new MVCorrelation(mv, mvQueryInteractedTableNum,
                    mvQueryScanOpDiff, mv.getLastRefreshTime());
            if (bestRelatedMVs.size() < maxRelatedMVsLimit) {
                bestRelatedMVs.add(mvCorrelation);
            } else if (bestRelatedMVs.peek().compareTo(mvCorrelation) < 0) {
                // if the peek is less than new mv(larger is better), poll it and add new one
                bestRelatedMVs.poll();
                bestRelatedMVs.add(mvCorrelation);
            }
        }
        logMVPrepare(connectContext, "Choose the best {} related mvs from all {} mvs because related " +
                        "mv exceeds max config limit {}",
                bestRelatedMVs.size(), validMVs.size(), maxRelatedMVsLimit);
        return bestRelatedMVs.stream().map(cor -> cor.getMv()).collect(Collectors.toSet());
    }

    @VisibleForTesting
    public Set<MaterializedView> chooseBestRelatedMVs(Set<Table> queryTables,
                                                      Set<MaterializedView> relatedMVs,
                                                      OptExpression queryOptExpression) {
        // 1. filter mvs which is set by config: including/excluding mvs
        Set<MaterializedView> validMVs = getRelatedMVsByConfig(relatedMVs);
        logMVPrepare(connectContext, "Choose {}/{} mvs after user config", validMVs.size(), relatedMVs.size());

        // 2. choose all valid mvs and filter mvs that cannot be rewritten for the query
        validMVs = validMVs.stream()
                .filter(mv -> isMVValidToRewriteQuery(connectContext, mv, false, queryTables).first)
                .collect(Collectors.toSet());
        logMVPrepare(connectContext, "Choose {}/{} valid mvs after checking valid",
                validMVs.size(), relatedMVs.size());

        // 3. choose max config related mvs for mv rewrite to avoid too much optimize time
        int maxRelatedMVsLimit = connectContext.getSessionVariable().getCboMaterializedViewRewriteRelatedMVsLimit();
        if (validMVs.size() <= maxRelatedMVsLimit) {
            return validMVs;
        }
        return chooseBestRelatedMVsByCorrelations(queryTables, validMVs, queryOptExpression, maxRelatedMVsLimit);
    }

    @VisibleForTesting
    public Set<MvWithPlanContext> getMvWithPlanContext(Set<MaterializedView> validMVs) {
        // filter mvs which are active and have valid plans
        Set<MvWithPlanContext> mvWithPlanContexts = Sets.newHashSet();
        for (MaterializedView mv : validMVs) {
            try {
                List<MvWithPlanContext> mvWithPlanContext = getMVWithContext(mv);
                if (mvWithPlanContext != null) {
                    mvWithPlanContexts.addAll(mvWithPlanContext);
                }
            } catch (Exception e) {
                logMVPrepare(connectContext, "Get mv plan context failed:{}", e.getMessage());
                LOG.warn("filter check failed mv:{}", mv.getName(), e);
            }
        }
        if (mvWithPlanContexts.isEmpty()) {
            logMVPrepare(connectContext, "There are no valid related mvs for the query plan");
        }
        return mvWithPlanContexts;
    }

    private Set<MaterializedView> getRelatedAsyncMVs(Set<Table> queryTables) {
        int maxLevel = connectContext.getSessionVariable().getNestedMvRewriteMaxLevel();
        // get all related materialized views, include nested mvs
        return MvUtils.getRelatedMvs(connectContext, maxLevel, queryTables);
    }

    private Set<MaterializedView> getRelatedSyncMVs(Set<Table> queryTables) {
        Set<MaterializedView> relatedMvs = Sets.newHashSet();
        // get all related materialized views, include nested mvs
        for (Table table : queryTables) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            relatedMvs.addAll(getTableRelatedSyncMVs(olapTable));
        }
        return relatedMvs;
    }

    private Set<MaterializedView> getTableRelatedSyncMVs(OlapTable olapTable) {
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
                                  Set<MvWithPlanContext> mvWithPlanContexts) {
        if (mvWithPlanContexts.isEmpty()) {
            return;
        }

        for (MvWithPlanContext mvWithPlanContext : mvWithPlanContexts) {
            MaterializedView mv = mvWithPlanContext.getMv();
            MvPlanContext mvPlanContext = mvWithPlanContext.getMvPlanContext();
            try {
                // mv's partitions to refresh
                MvUpdateInfo mvUpdateInfo = getPartitionNamesToRefreshForMv(mv, true);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "stale partitions {}", mvUpdateInfo);
                    continue;
                }
                Set<String> partitionNamesToRefresh = mvUpdateInfo.getMvToRefreshPartitionNames();
                if (!checkMvPartitionNamesToRefresh(mv, partitionNamesToRefresh, mvPlanContext)) {
                    continue;
                }
                logMVPrepare(mv, "MV' partitions to refresh: {}", partitionNamesToRefresh);

                // mv's partial partition predicates
                ScalarOperator mvPartialPartitionPredicates =
                        mvPartialPartitionPredicate(mv, mvPlanContext, partitionNamesToRefresh);
                if (mvPartialPartitionPredicates == null) {
                    OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "partition compensate fail");
                    continue;
                }
                logMVPrepare(mv, "MV compensate partition predicate: {}", mvPartialPartitionPredicates);
                MaterializationContext materializationContext = buildMaterializationContext(context, mv, mvPlanContext,
                        mvPartialPartitionPredicates, mvUpdateInfo, queryTables);
                if (materializationContext == null) {
                    continue;
                }
                context.addCandidateMvs(materializationContext);
                logMVPrepare(connectContext, mv, "Prepare MV {} success", mv.getName());
            } catch (Exception e) {
                List<String> tableNames = queryTables.stream().map(Table::getName).collect(Collectors.toList());
                logMVPrepare(connectContext, "Preprocess MV {} failed: {}", mv.getName(), DebugUtil.getStackTrace(e));
                LOG.warn("Preprocess mv {} failed for query tables:{}", mv.getName(), tableNames, e);
            }
        }
        // all base table related mvs
        List<String> relatedMvNames = mvWithPlanContexts.stream()
                .map(mvWithPlanContext -> mvWithPlanContext.getMv().getName())
                .collect(Collectors.toList());
        // all mvs that match SPJG pattern and can ben used to try mv rewrite
        List<String> candidateMvNames = context.getCandidateMvs().stream()
                .map(materializationContext -> materializationContext.getMv().getName())
                .collect(Collectors.toList());
        logMVPrepare(connectContext, "RelatedMVs: {}, CandidateMVs: {}", relatedMvNames,
                candidateMvNames);
    }

    /**
     * Get mv's partition names to refresh for partitioned MV.
     * @param mv:  input materialized view
     * @param mvPlanContext: the associated materialized view context
     * @return partition names to refresh if the mv is valid for rewrite, otherwise null
     */
    public static boolean checkMvPartitionNamesToRefresh(MaterializedView mv,
                                                         Set<String> partitionNamesToRefresh,
                                                         MvPlanContext mvPlanContext) {
        Preconditions.checkState(mvPlanContext != null);
        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        Preconditions.checkState(mvPlan != null);
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            if (!partitionNamesToRefresh.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (BaseTableInfo base : mv.getBaseTableInfos()) {
                    Optional<Table> baseTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(base);
                    if (!baseTable.isPresent() || baseTable.get().isView()) {
                        continue;
                    }
                    String versionInfo = Joiner.on(",").join(mv.getBaseTableLatestPartitionInfo(baseTable.get()));
                    sb.append(String.format("base table %s version: %s; ", base, versionInfo));
                }
                logMVPrepare(mv, "MV {} is outdated, stale partitions {}, detailed version info: {}",
                        mv.getName(), partitionNamesToRefresh, sb.toString());
                return false;
            }
        } else if (!mv.getPartitionNames().isEmpty() && partitionNamesToRefresh.containsAll(mv.getPartitionNames())) {
            // if the mv is partitioned, and all partitions need refresh,
            // then it can not be a candidate
            StringBuilder sb = new StringBuilder();
            try {
                for (BaseTableInfo base : mv.getBaseTableInfos()) {
                    Optional<Table> tableOptional = MvUtils.getTableWithIdentifier(base);
                    if (!tableOptional.isPresent()) {
                        continue;
                    }
                    String versionInfo = Joiner.on(",").join(mv.getBaseTableLatestPartitionInfo(tableOptional.get()));
                    sb.append(String.format("base table %s version: %s; ", base, versionInfo));
                }
            } catch (Exception e) {
                // ignore exception for `getPartitions` is only supported for hive/jdbc.
            }
            logMVPrepare(mv, "MV {} is outdated and all its partitions need to be " +
                            "refreshed: {}, refreshed mv partitions: {}, base table detailed info: {}", mv.getName(),
                    partitionNamesToRefresh, mv.getPartitionNames(), sb.toString());
            return false;
        }
        return true;
    }

    /**
     * Get mv's compensated partial partition predicates for partitioned MV.
     */
    private static ScalarOperator mvPartialPartitionPredicate(MaterializedView mv,
                                                              MvPlanContext mvPlanContext,
                                                              Set<String> partitionNamesToRefresh) {
        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        ScalarOperator mvPartialPartitionPredicates = ConstantOperator.TRUE;
        // TODO: support list partition
        if (mv.getPartitionInfo() instanceof ExpressionRangePartitionInfo && !partitionNamesToRefresh.isEmpty()) {
            // when mv is partitioned and there are some refreshed partitions,
            // when should calculate the latest partition range predicates for partition-by base table
            try {
                mvPartialPartitionPredicates = getMvPartialPartitionPredicates(mv, mvPlan, partitionNamesToRefresh);
                if (mvPartialPartitionPredicates == null) {
                    logMVPrepare(mv, "Partitioned MV {} is outdated which contains some partitions " +
                            "to be refreshed: {}, and cannot compensate it to predicate", mv.getName(), partitionNamesToRefresh);
                    return null;
                }
            } catch (AnalysisException e) {
                logMVPrepare(mv, "Get partitioned MV {} with to-refresh partitions {} predicate failed: " +
                        "{}", mv.getName(), partitionNamesToRefresh, DebugUtil.getStackTrace(e));
                return null;
            }
        }
        return mvPartialPartitionPredicates;
    }

    /**
     * Build materialization context for the given materialized view.
     * @param mv: the input materialized view
     * @param mvPlanContext: the associated materialized view context
     * @param queryTables: mv's define query tables
     * @return: the materialization context if the mv is valid for rewrite, otherwise null
     * @throws AnalysisException
     */
    public static MaterializationContext buildMaterializationContext(OptimizerContext context,
                                                                     MaterializedView mv,
                                                                     MvPlanContext mvPlanContext,
                                                                     ScalarOperator mvPartialPartitionPredicates,
                                                                     MvUpdateInfo mvUpdateInfo,
                                                                     Set<Table> queryTables) {
        Preconditions.checkState(mvPlanContext != null);
        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        Preconditions.checkState(mvPlan != null);

        // Add mv info into dump info
        if (context.getDumpInfo() != null) {
            String dbName = GlobalStateMgr.getCurrentState().getDb(mv.getDbId()).getFullName();
            context.getDumpInfo().addTable(dbName, mv);
        }

        List<Table> baseTables = MvUtils.getAllTables(mvPlan);
        List<Table> intersectingTables = baseTables.stream().filter(queryTables::contains).collect(Collectors.toList());

        // If query tables are set which means use related mv for non lock optimization,
        // copy mv's metadata into a ready-only object.
        MaterializedView copiedMV = (context.getQueryTables() != null) ? copyOnlyMaterializedView(mv) : mv;
        List<ColumnRefOperator> mvOutputColumns = mvPlanContext.getOutputColumns();
        MaterializationContext materializationContext =
                new MaterializationContext(context, copiedMV, mvPlan, context.getColumnRefFactory(),
                        mvPlanContext.getRefFactory(), baseTables, intersectingTables,
                        mvPartialPartitionPredicates, mvUpdateInfo, mvOutputColumns);
        // generate scan mv plan here to reuse it in rule applications
        LogicalOlapScanOperator scanMvOp = createScanMvOperator(mv,
                materializationContext.getQueryRefFactory(), mvUpdateInfo.getMvToRefreshPartitionNames());
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

        return materializationContext;
    }

    /**
     * Get mv's ordered columns by defined output columns order.
     * @param mv: mv to check
     * @return: mv's defined output columns in the defined order
     */
    public static List<Column> getMvOutputColumns(MaterializedView mv) {
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
    public static LogicalOlapScanOperator createScanMvOperator(MaterializedView mv,
                                                               ColumnRefFactory columnRefFactory,
                                                               Set<String> excludedPartitions) {
        final ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumnMetaMapBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<Column, ColumnRefOperator> columnMetaToColRefMapBuilder = ImmutableMap.builder();

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
                .setDistributionSpec(getTableDistributionSpec(mv, columnMetaToColRefMap))
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

    private static DistributionSpec getTableDistributionSpec(MaterializedView mv,
                                                             Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
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