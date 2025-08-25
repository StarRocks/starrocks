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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.mv.MVPlanValidationResult;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.mv.MVCorrelation;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.optimizer.rule.mv.MaterializedViewWrapper;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public class MvRewritePreprocessor {
    private static final Logger LOG = LogManager.getLogger(MvRewritePreprocessor.class);

    private static final Executor MV_PREPARE_EXECUTOR = Executors.newFixedThreadPool(
            ThreadPoolManager.cpuIntensiveThreadPoolSize(),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("mv-prepare-%d").build());

    private final ConnectContext connectContext;
    private final ColumnRefFactory queryColumnRefFactory;
    private final OptimizerContext context;
    private final ColumnRefSet requiredColumns;
    private final QueryMaterializationContext queryMaterializationContext;

    public MvRewritePreprocessor(ConnectContext connectContext,
                                 ColumnRefFactory queryColumnRefFactory,
                                 OptimizerContext context,
                                 ColumnRefSet requiredColumns) {
        this.connectContext = connectContext;
        this.queryColumnRefFactory = queryColumnRefFactory;
        this.context = context;
        this.requiredColumns = requiredColumns;
        this.queryMaterializationContext = context.getQueryMaterializationContext() == null ?
                new QueryMaterializationContext() : context.getQueryMaterializationContext();
    }

    public void prepare(OptExpression queryOptExpression) {
        try (Timer ignored = Tracers.watchScope("MVPreprocess")) {
            Set<Table> queryTables = MvUtils.getAllTables(queryOptExpression).stream().collect(Collectors.toSet());
            logMVParams(connectContext, queryTables);

            // use a new context rather than reuse the existed context to avoid cache conflict.
            try {
                // 1. get related mvs for all input tables
                Set<MaterializedViewWrapper> relatedMVs =
                        getRelatedMVs(queryTables, context.getOptimizerOptions().isRuleBased());
                if (relatedMVs.isEmpty()) {
                    return;
                }

                // filter mvs which is set by config: including/excluding mvs
                Set<MaterializedViewWrapper> relatedMVWrappers = getRelatedMVsByConfig(relatedMVs);
                logMVPrepare(connectContext, "Choose {}/{} mvs after user config", relatedMVWrappers.size(), relatedMVs.size());
                // add into queryMaterializationContext for later use
                if (relatedMVWrappers.isEmpty()) {
                    return;
                }
                this.queryMaterializationContext.addRelatedMVs(
                        relatedMVWrappers.stream().map(MaterializedViewWrapper::getMV).collect(Collectors.toSet()));

                // 2. choose best related mvs by user's config or related mv limit
                List<MaterializedViewWrapper> candidateMVs;
                try (Timer t1 = Tracers.watchScope("MVChooseCandidates")) {
                    candidateMVs = chooseBestRelatedMVs(queryTables, relatedMVWrappers, queryOptExpression);
                }
                if (candidateMVs.isEmpty()) {
                    return;
                }

                // 3. convert to mv with planContext, skip if mv has no valid plan(not SPJG)
                List<MaterializedViewWrapper> mvWithPlanContexts;
                try (Timer t2 = Tracers.watchScope("MVGenerateMvPlan")) {
                    mvWithPlanContexts = getMvWithPlanContext(candidateMVs);
                }
                if (mvWithPlanContexts.isEmpty()) {
                    return;
                }

                // 4. process related mvs to candidates
                try (Timer t3 = Tracers.watchScope("MVPrepareRelatedMVs")) {
                    prepareRelatedMVs(queryTables, mvWithPlanContexts);
                }

                // To avoid disturbing queries without mv, only initialize materialized view context
                // when there are candidate mvs.
                if (CollectionUtils.isNotEmpty(context.getCandidateMvs())) {
                    // it's safe used in the optimize context here since the query mv context is not shared across the
                    // connect-context.
                    connectContext.setQueryMVContext(queryMaterializationContext);
                }

                // 5. process relate mvs with views
                try (Timer t4 = Tracers.watchScope("MVProcessWithView")) {
                    processPlanWithView(queryMaterializationContext, connectContext, queryOptExpression,
                            queryColumnRefFactory, requiredColumns);
                }

                if (queryMaterializationContext.getValidCandidateMVs().size() > 1) {
                    queryMaterializationContext.setEnableQueryContextCache(true);
                }
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

        // Get all MV related session variables
        List<Pair<String, Object>> mvSessionVariables = Lists.newArrayList(
                Pair.create("enable_materialized_view_rewrite", sessionVariable.isEnableMaterializedViewRewrite()),
                Pair.create("enable_view_based_mv_rewrite", sessionVariable.isEnableViewBasedMvRewrite()),
                Pair.create("enable_materialized_view_union_rewrite", sessionVariable.isEnableMaterializedViewUnionRewrite()),
                Pair.create("enable_materialized_view_view_delta_rewrite",
                        sessionVariable.isEnableMaterializedViewViewDeltaRewrite()),
                Pair.create("enable_materialized_view_plan_cache", sessionVariable.isEnableMaterializedViewPlanCache()),
                Pair.create("enable_sync_materialized_view_rewrite", sessionVariable.isEnableSyncMaterializedViewRewrite()),
                Pair.create("enable_materialized_view_text_match_rewrite",
                        sessionVariable.isEnableMaterializedViewTextMatchRewrite()),
                Pair.create("enable_materialized_view_multi_stages_rewrite",
                        sessionVariable.isEnableMaterializedViewMultiStagesRewrite()),
                Pair.create("optimizer_materialized_view_timelimit",
                        sessionVariable.getOptimizerMaterializedViewTimeLimitMillis()),
                Pair.create("materialized_view_join_same_table_permutation_limit",
                        sessionVariable.getMaterializedViewJoinSameTablePermutationLimit()),
                Pair.create("analyze_mv", sessionVariable.getAnalyzeForMV()),
                Pair.create("query_excluding_mv_names", sessionVariable.getQueryExcludingMVNames()),
                Pair.create("query_including_mv_names", sessionVariable.getQueryIncludingMVNames()),
                Pair.create("cbo_materialized_view_rewrite_rule_output_limit",
                        sessionVariable.getCboMaterializedViewRewriteRuleOutputLimit()),
                Pair.create("cbo_materialized_view_rewrite_candidate_limit",
                        sessionVariable.getCboMaterializedViewRewriteCandidateLimit()),
                Pair.create("cbo_materialized_view_rewrite_related_mvs_limit",
                        sessionVariable.getCboMaterializedViewRewriteRelatedMVsLimit()),
                Pair.create("materialized_view_rewrite_mode", sessionVariable.getMaterializedViewRewriteMode())
        );

        // Get all MV related config variables
        List<Pair<String, Object>> mvConfigVariables = Lists.newArrayList(
                Pair.create("enable_experimental_mv", Config.enable_experimental_mv),
                Pair.create("mv_auto_analyze_async", Config.mv_auto_analyze_async),
                Pair.create("enable_mv_automatic_active_check", Config.enable_mv_automatic_active_check),
                Pair.create("skip_whole_phase_lock_mv_limit", Config.skip_whole_phase_lock_mv_limit),
                Pair.create("enable_materialized_view_concurrent_prepare", Config.enable_materialized_view_concurrent_prepare)
        );

        // Log session variables
        logMVPrepare(connectContext, "---------------------------------");
        logMVPrepare(connectContext, "Materialized View Session Variables:");
        for (Pair<String, Object> variable : mvSessionVariables) {
            logMVPrepare(connectContext, "  {}: {}", variable.first, variable.second);
        }

        // Log config variables
        logMVPrepare(connectContext, "---------------------------------");
        logMVPrepare(connectContext, "Materialized View Config Variables:");
        for (Pair<String, Object> variable : mvConfigVariables) {
            logMVPrepare(connectContext, "  {}: {}", variable.first, variable.second);
        }
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
        if (queryMaterializationContext == null || queryMaterializationContext.getValidCandidateMVs().isEmpty()) {
            return;
        }
        final List<LogicalViewScanOperator> logicalViewScanOperators = Lists.newArrayList();
        // process equivalent operator，construct logical plan with view
        final OptExpression logicalPlanWithViewInline = extractLogicalPlanWithView(logicOperatorTree,
                logicalViewScanOperators);
        if (CollectionUtils.isEmpty(logicalViewScanOperators)) {
            // means there is no plan with view
            return;
        }
        // optimize logical plan with view
        OptExpression optViewScanExpressions = MvUtils.optimizeViewPlan(
                logicalPlanWithViewInline, connectContext, requiredColumns, columnRefFactory);
        queryMaterializationContext.setQueryOptPlanWithView(optViewScanExpressions);
        queryMaterializationContext.setQueryViewScanOps(logicalViewScanOperators);
    }


    private OptExpression extractLogicalPlanWithView(OptExpression logicalTree,
                                                     List<LogicalViewScanOperator> viewScans) {
        List<OptExpression> inputs = Lists.newArrayList();
        if (logicalTree.getOp().getEquivalentOp() != null) {
            LogicalViewScanOperator viewScanOperator = logicalTree.getOp().getEquivalentOp().cast();
            // If the view scan operator is not rewritten, needs to use the original plan to replace the view scan operator.
            // We don't need to evaluate the original plan directly, if view-based-rewrite successes, the original plan will
            // not be used, so use Lazy Evaluator here.
            // 1. Collect LogicalViewScanOperator to an original logical tree which will be used in mv union rewrite.
            // 2. Clone the original plan because the following optimizeViewPlan will change the plan
            OptExpression clonePlan = MvUtils.cloneExpression(logicalTree);
            viewScanOperator.setOriginalPlanEvaluator(new LogicalViewScanOperator.OptPlanEvaluator(clonePlan,
                    connectContext, viewScanOperator.getOutputColumnSet(), queryColumnRefFactory));
            viewScans.add(viewScanOperator);

            // replace the original plan with a new view scan operator
            LogicalViewScanOperator.Builder builder = new LogicalViewScanOperator.Builder();
            builder.withOperator(viewScanOperator);
            builder.setProjection(null);
            LogicalViewScanOperator clonedViewScanOp = builder.build();
            OptExpression viewScanExpr = OptExpression.create(clonedViewScanOp);

            // add a projection to make predicate push-down rules work.
            Projection projection = viewScanOperator.getProjection();
            LogicalProjectOperator projectOperator = new LogicalProjectOperator(projection.getColumnRefMap());
            OptExpression projectionExpr = OptExpression.create(projectOperator, viewScanExpr);
            return projectionExpr;
        } else {
            for (OptExpression input : logicalTree.getInputs()) {
                OptExpression newInput = extractLogicalPlanWithView(input, viewScans);
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
    public Set<MaterializedViewWrapper> getRelatedMVs(Set<Table> queryTables,
                                                      boolean isRuleBased) {
        if (Config.enable_experimental_mv
                && connectContext.getSessionVariable().isEnableMaterializedViewRewrite()
                && !isRuleBased) {
            // related asynchronous materialized views
            Set<MaterializedViewWrapper> relatedMVs = getRelatedAsyncMVs(queryTables);

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

    private Set<MaterializedViewWrapper> getRelatedMVsByConfig(Set<MaterializedViewWrapper> relatedMVs) {
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

        return relatedMVs
                .stream()
                .filter(wrapper -> queryIncludingMVNamesSet.isEmpty()
                        || queryIncludingMVNamesSet.contains(wrapper.getMV().getName()))
                .filter(wrapper -> queryExcludingMVNamesSet.isEmpty()
                        || !queryExcludingMVNamesSet.contains(wrapper.getMV().getName()))
                .collect(Collectors.toSet());
    }

    private List<MaterializedViewWrapper> getMVWithContext(MaterializedViewWrapper wrapper,
                                                           long timeoutMs) {
        final MaterializedView mv = wrapper.getMV();
        if (!mv.isActive()) {
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "inactive");
            return null;
        }
        // NOTE: To avoid building plan for every mv cost too much time, we should only get plan
        // when the mv is in the plan cache.
        List<MvPlanContext> mvPlanContexts;
        if (mv.getRefreshScheme().isSync() || connectContext.getSessionVariable().isEnableMaterializedViewForceRewrite()) {
            mvPlanContexts = CachingMvPlanContextBuilder.getInstance()
                    .getPlanContext(connectContext.getSessionVariable(), mv);
        } else {
            mvPlanContexts = CachingMvPlanContextBuilder.getInstance()
                    .getPlanContextIfPresent(mv, timeoutMs);
        }
        if (CollectionUtils.isEmpty(mvPlanContexts)) {
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "invalid query plan");
            return null;
        }
        List<MaterializedViewWrapper> mvWithPlanContexts = Lists.newArrayList();
        for (int i = 0; i < mvPlanContexts.size(); i++) {
            MvPlanContext mvPlanContext = mvPlanContexts.get(i);
            if (!mvPlanContext.isValidMvPlan()) {
                OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "invalid query plan {}/{}: {}",
                        i, mvPlanContexts.size(), mvPlanContext.getInvalidReason());
                continue;
            }
            mvWithPlanContexts.add(MaterializedViewWrapper.create(mv, wrapper.getLevel(), mvPlanContext));
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
     * NOTE: This method can be time-costing if mv's defined plan is not in the plan cache and is complex,
     * so it's better to use it with force=false.
     * @param force build the MV plan even if it's not in the plan cache
     */
    public static MVPlanValidationResult isMVValidToRewriteQuery(ConnectContext connectContext,
                                                                 MaterializedView mv,
                                                                 Set<Table> queryTables,
                                                                 boolean force,
                                                                 boolean isNoPlanAsInvalid,
                                                                 long timeoutMs) {
        if (!mv.isActive())  {
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "is not active");
            return MVPlanValidationResult.invalid("MV is not active");
        }
        if (!mv.isEnableRewrite()) {
            String message = PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE + "=" +
                    mv.getTableProperty().getMvQueryRewriteSwitch();
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), message);
            return MVPlanValidationResult.invalid(message);
        }
        // if mv is a subset of query tables, it can be used for rewrite.
        if (CollectionUtils.isNotEmpty(queryTables) &&
                !canMVRewriteIfMVHasExtraTables(connectContext, mv, queryTables)) {
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "MV contains extra tables besides FK-PK");
            return MVPlanValidationResult.invalid("MV contains extra tables besides FK-PK");
        }
        // if mv is in plan cache(avoid building plan), check whether it's valid
        final List<MvPlanContext> planContexts = force ?
                CachingMvPlanContextBuilder.getInstance()
                        .getOrLoadPlanContext(mv, timeoutMs) :
                CachingMvPlanContextBuilder.getInstance()
                        .getPlanContextIfPresent(mv, timeoutMs);
        // if mv is not in plan cache, we cannot determine whether it's valid
        if (isNoPlanAsInvalid && CollectionUtils.isEmpty(planContexts)) {
            return MVPlanValidationResult.unknown("MV plan is not in cache, valid check is unknown");
        }
        if (CollectionUtils.isNotEmpty(planContexts) &&
                planContexts.stream().noneMatch(MvPlanContext::isValidMvPlan)) {
            logMVPrepare(connectContext, "MV {} has no valid plan from {} plan contexts",
                    mv.getName(), planContexts.size());
            String message = planContexts.stream()
                    .map(MvPlanContext::getInvalidReason)
                    .collect(Collectors.joining(";"));
            OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), message);
            return MVPlanValidationResult.invalid("no valid plan: " + message);
        }
        return MVPlanValidationResult.valid();
    }

    private List<MaterializedViewWrapper> chooseBestRelatedMVsByCorrelations(Set<Table> queryTables,
                                                                             Set<MaterializedViewWrapper> validMVs,
                                                                             OptExpression queryOptExpression,
                                                                             int maxRelatedMVsLimit) {
        int queryScanOpNum = MvUtils.getOlapScanNode(queryOptExpression).size();
        Set<String> queryTableNames = queryTables.stream().map(t -> t.getName()).collect(Collectors.toSet());
        List<MVCorrelation> mvCorrelations = Lists.newArrayList();
        long timeoutMs = getPrepareTimeoutMsPerMV(Math.min(validMVs.size(), maxRelatedMVsLimit));
        for (MaterializedViewWrapper wrapper : validMVs) {
            MaterializedView mv = wrapper.getMV();
            List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
            long mvQueryInteractedTableNum = MVCorrelation.getMvQueryIntersectedTableNum(baseTableInfos, queryTableNames);
            List<MvPlanContext> planContexts = CachingMvPlanContextBuilder.getInstance()
                            .getPlanContextIfPresent(mv, timeoutMs);
            int mvQueryScanOpDiff = MVCorrelation.getMvQueryScanOpDiff(planContexts, baseTableInfos.size(), queryScanOpNum);
            MVCorrelation mvCorrelation = new MVCorrelation(mv, mvQueryInteractedTableNum,
                    mvQueryScanOpDiff, mv.getLastRefreshTime(), wrapper.getLevel());
            mvCorrelations.add(mvCorrelation);
        }
        return chooseBestRelatedMVsByCorrelations(mvCorrelations, maxRelatedMVsLimit);
    }

    @VisibleForTesting
    public List<MaterializedViewWrapper> chooseBestRelatedMVsByCorrelations(Collection<MVCorrelation> mvCorrelations,
                                                                            int maxRelatedMVsLimit) {
        Queue<MVCorrelation> queue = new PriorityQueue<>(maxRelatedMVsLimit);
        for (MVCorrelation mvCorrelation : mvCorrelations) {
            if (queue.size() < maxRelatedMVsLimit) {
                queue.add(mvCorrelation);
            } else if (queue.peek().compareTo(mvCorrelation) < 0) {
                // if the peek is less than new mv(larger is better), poll it and add new one
                queue.poll();
                queue.add(mvCorrelation);
            }
        }
        logMVPrepare(connectContext, "Choose the best {} related mvs from all {} mvs because related " +
                        "mv exceeds max config limit {}",
                queue.size(), mvCorrelations.size(), maxRelatedMVsLimit);
        // ensure the best related mv is at the first
        List<MaterializedViewWrapper> result = Lists.newArrayList();
        while (!queue.isEmpty()) {
            result.add(queue.poll().getWrapper());
        }
        Collections.reverse(result);
        return result;
    }

    /**
     * Choose the best related mvs for the query rewrite that ensures the best related mv is at the first.
     */
    @VisibleForTesting
    public List<MaterializedViewWrapper> chooseBestRelatedMVs(Set<Table> queryTables,
                                                              Set<MaterializedViewWrapper> relatedMVs,
                                                              OptExpression queryOptExpression) {
        // choose all valid mvs and filter mvs that cannot be rewritten for the query
        int maxRelatedMVsLimit = connectContext.getSessionVariable().getCboMaterializedViewRewriteRelatedMVsLimit();
        long timeoutMs = getPrepareTimeoutMsPerMV(Math.min(relatedMVs.size(), maxRelatedMVsLimit));
        Set<MaterializedViewWrapper> validMVs = relatedMVs.stream()
                .filter(wrapper -> isMVValidToRewriteQuery(connectContext, wrapper.getMV(),
                        queryTables, false, false, timeoutMs).isValid())
                .collect(Collectors.toSet());
        logMVPrepare(connectContext, "Choose {}/{} valid mvs after checking valid",
                validMVs.size(), relatedMVs.size());

        // choose max config related mvs for mv rewrite to avoid too much optimize time
        return chooseBestRelatedMVsByCorrelations(queryTables, validMVs, queryOptExpression, maxRelatedMVsLimit);
    }

    @VisibleForTesting
    public List<MaterializedViewWrapper> getMvWithPlanContext(List<MaterializedViewWrapper> validMVs) {
        // filter mvs which are active and have valid plans
        final List<MaterializedViewWrapper> mvWithPlanContexts = Lists.newArrayList();
        final long timeoutMs = getPrepareTimeoutMsPerMV(validMVs.size());
        for (MaterializedViewWrapper wrapper : validMVs) {
            MaterializedView mv = wrapper.getMV();
            try {
                final List<MaterializedViewWrapper> mvWithPlanContext = getMVWithContext(wrapper, timeoutMs);
                if (CollectionUtils.isNotEmpty(mvWithPlanContext)) {
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

    private Set<MaterializedViewWrapper> getRelatedAsyncMVs(Set<Table> queryTables) {
        int maxLevel = connectContext.getSessionVariable().getNestedMvRewriteMaxLevel();
        // get all related materialized views, include nested mvs
        return MvUtils.getRelatedMvs(connectContext, maxLevel, queryTables);
    }

    private Set<MaterializedViewWrapper> getRelatedSyncMVs(Set<Table> queryTables) {
        // get all related materialized views, include nested mvs
        return queryTables.stream()
                .filter(table -> table instanceof OlapTable)
                .map(table -> getTableRelatedSyncMVs((OlapTable) table))
                .flatMap(Set::stream)
                .map(mv -> MaterializedViewWrapper.create(mv, 0))
                .collect(Collectors.toSet());
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
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);

                // distribution info
                DistributionInfo baseTableDistributionInfo = olapTable.getDefaultDistributionInfo();
                DistributionInfo mvDistributionInfo = baseTableDistributionInfo.copy();
                Set<String> mvColumnNames =
                        indexMeta.getSchema().stream().map(Column::getName).collect(Collectors.toSet());
                if (baseTableDistributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) baseTableDistributionInfo;
                    Set<String> distributedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
                    distributedColumns.addAll(MetaUtils.getColumnNamesByColumnIds(
                            olapTable.getIdToColumn(), hashDistributionInfo.getDistributionColumns()));
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
                    List<Column> partitionColumns = basePartitionInfo.getPartitionColumns(olapTable.getIdToColumn());
                    if (partitionColumns.stream()
                            .anyMatch(x -> !mvColumnNames.contains(x.getName())) ||
                            !(basePartitionInfo instanceof ExpressionRangePartitionInfo)) {
                        mvPartitionInfo = new SinglePartitionInfo();
                    }
                }
                // refresh schema
                MaterializedView.MvRefreshScheme mvRefreshScheme =
                        new MaterializedView.MvRefreshScheme(MaterializedViewRefreshType.SYNC);
                MaterializedView mv;
                if (olapTable.isCloudNativeTable()) {
                    mv = new LakeMaterializedView(db, mvName, indexMeta, olapTable,
                            mvPartitionInfo, mvDistributionInfo, mvRefreshScheme);
                } else {
                    mv = new MaterializedView(db, mvName, indexMeta, olapTable,
                            mvPartitionInfo, mvDistributionInfo, mvRefreshScheme);
                }

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
                                  List<MaterializedViewWrapper> mvWithPlanContexts) {
        if (mvWithPlanContexts.isEmpty()) {
            return;
        }

        List<Pair<MaterializedViewWrapper, MvUpdateInfo>> mvInfos =
                Lists.newArrayListWithExpectedSize(mvWithPlanContexts.size());
        for (MaterializedViewWrapper wrapper : mvWithPlanContexts) {
            MaterializedView mv = wrapper.getMV();
            try {
                // mv's partitions to refresh
                MvUpdateInfo mvUpdateInfo = queryMaterializationContext.getOrInitMVTimelinessInfos(mv);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "stale partitions {}", mvUpdateInfo);
                    continue;
                }
                mvInfos.add(Pair.create(wrapper, mvUpdateInfo));
            } catch (Exception e) {
                List<String> tableNames = queryTables.stream().map(Table::getName).collect(Collectors.toList());
                logMVPrepare(connectContext, "Preprocess MV {} failed: {}", mv.getName(), DebugUtil.getStackTrace(e));
                LOG.warn("Preprocess mv {} failed for query tables:{}", mv.getName(), tableNames, e);
            }
        }
        Tracers tracers = Tracers.get();
        Executor exec = Config.enable_materialized_view_concurrent_prepare &&
                connectContext.getSessionVariable().isEnableMaterializedViewConcurrentPrepare() ? MV_PREPARE_EXECUTOR :
                newDirectExecutorService();
        
        // Process MVs with individual timeouts to allow continuation even if some timeout
        processMVsWithIndividualTimeouts(tracers, queryTables, mvInfos, exec);
        // all base table related mvs
        List<String> relatedMvNames = mvWithPlanContexts.stream()
                .map(mvWithPlanContext -> mvWithPlanContext.getMV().getName())
                .collect(Collectors.toList());
        // all mvs that match SPJG pattern and can ben used to try mv rewrite
        List<String> candidateMvNames = context.getCandidateMvs().stream()
                .map(materializationContext -> materializationContext.getMv().getName())
                .collect(Collectors.toList());
        logMVPrepare(connectContext, "RelatedMVs: {}, CandidateMVs: {}", relatedMvNames,
                candidateMvNames);
    }

    private Void prepareMV(Tracers tracers, Set<Table> queryTables, MaterializedViewWrapper mvWithPlanContext,
                           MvUpdateInfo mvUpdateInfo) {
        MaterializedView mv = mvWithPlanContext.getMV();
        MvPlanContext mvPlanContext = mvWithPlanContext.getMvPlanContext();
        Set<String> partitionNamesToRefresh = mvUpdateInfo.getMvToRefreshPartitionNames();
        if (!checkMvPartitionNamesToRefresh(connectContext, mv, partitionNamesToRefresh, mvPlanContext)) {
            return null;
        }
        if (partitionNamesToRefresh.isEmpty()) {
            logMVPrepare(tracers, connectContext, mv, "MV {} has no partitions to refresh", mv.getName());
        } else {
            logMVPrepare(tracers, mv, "MV' partitions to refresh: {}/{}", partitionNamesToRefresh.size(),
                    MvUtils.shrinkToSize(partitionNamesToRefresh, Config.max_mv_task_run_meta_message_values_length));
        }

        MaterializationContext materializationContext = buildMaterializationContext(context, mv, mvPlanContext,
                mvUpdateInfo, queryTables, mvWithPlanContext.getLevel());
        if (materializationContext == null) {
            return null;
        }
        synchronized (materializationContext) {
            queryMaterializationContext.addValidCandidateMV(materializationContext);
        }
        logMVPrepare(tracers, connectContext, mv, "Prepare MV {} success", mv.getName());
        return null;
    }

    /**
     * MV Preprocessing timeout is calculated based on the number of MVs to ensure mv preprocessor does not take too long.
     * @param mvCount: the number of MVs to process
     * @return: timeout in milliseconds for each MV preparation
     */
    private long getPrepareTimeoutMsPerMV(int mvCount) {
        long defaultTimeout = connectContext.getSessionVariable().getOptimizerExecuteTimeout() / 2;
        if (mvCount == 0) {
            return defaultTimeout;
        }
        // Ensure at least 1 second per MV, but not more than the total timeout
        return Math.max(1000, defaultTimeout / mvCount);
    }

    /**
     * Process MVs with individual timeouts to allow continuation even if some timeout.
     * Each MV gets a timeout of total_timeout / number_of_mvs to ensure fair distribution.
     * 
     * @param tracers Tracers for logging
     * @param queryTables Query tables
     * @param mvInfos List of MV info pairs to process
     * @param exec Executor for async processing
     */
    private void processMVsWithIndividualTimeouts(Tracers tracers, Set<Table> queryTables,
                                                 List<Pair<MaterializedViewWrapper, MvUpdateInfo>> mvInfos,
                                                 Executor exec) {
        if (mvInfos.isEmpty()) {
            return;
        }
        long individualTimeoutMs = getPrepareTimeoutMsPerMV(mvInfos.size());
        logMVPrepare(connectContext, "Processing {} MVs with individual timeout of {} ms each", mvInfos.size(),
                individualTimeoutMs);
        
        List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(mvInfos.size());
        List<String> timeoutMvNames = Lists.newArrayList();
        List<String> failedMvNames = Lists.newArrayList();
        
        // Create futures for each MV
        for (Pair<MaterializedViewWrapper, MvUpdateInfo> mvInfo : mvInfos) {
            MaterializedView mv = mvInfo.first.getMV();
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(
                    () -> prepareMV(tracers, queryTables, mvInfo.first, mvInfo.second), exec)
                    .thenAccept(result -> {
                        // Success case - result is already handled in prepareMV
                    })
                    .exceptionally(throwable -> {
                        LOG.warn("Failed to prepare MV {}: {}", mv.getName(), throwable.getMessage());
                        failedMvNames.add(mv.getName());
                        return null;
                    });
            
            futures.add(future);
        }
        
        // Process each future with individual timeout
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<Void> future = futures.get(i);
            MaterializedView mv = mvInfos.get(i).first.getMV();
            
            try {
                future.get(individualTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.warn("MV {} preparation timeout after {} ms", mv.getName(), individualTimeoutMs);
                timeoutMvNames.add(mv.getName());
                // Don't throw exception, continue with other MVs
            } catch (InterruptedException e) {
                LOG.warn("MV {} preparation interrupted", mv.getName());
                Thread.currentThread().interrupt();
                throw new RuntimeException("MV preparation interrupted", e);
            } catch (ExecutionException e) {
                LOG.warn("MV {} preparation failed with execution exception", mv.getName(), e);
                failedMvNames.add(mv.getName());
                // Don't throw exception, continue with other MVs
            }
        }
        
        // Log summary
        int successCount = mvInfos.size() - timeoutMvNames.size() - failedMvNames.size();
        logMVPrepare(connectContext, "MV preparation summary: {} successful, {} timeout, {} failed out of {} total",
                successCount, timeoutMvNames.size(), failedMvNames.size(), mvInfos.size());
    }

    /**
     * Get mv's partition names to refresh for partitioned MV.
     * @param mv:  input materialized view
     * @param mvPlanContext: the associated materialized view context
     * @return partition names to refresh if the mv is valid for rewrite, otherwise null
     */
    public static boolean checkMvPartitionNamesToRefresh(ConnectContext context,
                                                         MaterializedView mv,
                                                         Set<String> partitionNamesToRefresh,
                                                         MvPlanContext mvPlanContext) {
        Preconditions.checkState(mvPlanContext != null);
        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        Preconditions.checkState(mvPlan != null);
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            if (!partitionNamesToRefresh.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (BaseTableInfo base : mv.getBaseTableInfos()) {
                    Optional<Table> baseTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, base);
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
                                                                     MvUpdateInfo mvUpdateInfo,
                                                                     Set<Table> queryTables,
                                                                     int level) {
        Preconditions.checkState(mvPlanContext != null);
        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        Preconditions.checkState(mvPlan != null);

        // Add mv info into dump info
        if (context.getDumpInfo() != null) {
            String dbName = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId()).getFullName();
            synchronized (context) {
                context.getDumpInfo().addTable(dbName, mv);
            }
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
                        mvUpdateInfo, mvOutputColumns, level);
        // generate scan mv plan here to reuse it in rule applications
        LogicalOlapScanOperator scanMvOp;
        synchronized (materializationContext.getQueryRefFactory()) {
            scanMvOp = createScanMvOperator(mv, materializationContext.getQueryRefFactory(),
                    mvUpdateInfo.getMvToRefreshPartitionNames());
        }
        materializationContext.setScanMvOperator(scanMvOp);
        // should keep the sequence of schema
        List<ColumnRefOperator> scanMvOutputColumns = Lists.newArrayList();
        for (Column column : copiedMV.getOrderedOutputColumns()) {
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
        for (Column column : mv.getBaseSchemaWithoutGeneratedColumn()) {
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
                    selectTabletIds.addAll(materializedIndex.getTabletIds());
                }
            }
        }
        final PartitionNames partitionNames = new PartitionNames(false, selectedPartitionNames);
        // MV's selected partition ids/tablet ids are necessary for MV rewrite.
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
            List<Column> distributedColumns = MetaUtils.getColumnsByColumnIds(mv,
                    hashDistributionInfo.getDistributionColumns());
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