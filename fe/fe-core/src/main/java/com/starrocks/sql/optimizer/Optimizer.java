// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.Explain;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.join.ReorderJoinRule;
import com.starrocks.sql.optimizer.rule.mv.MaterializedViewRule;
import com.starrocks.sql.optimizer.rule.transformation.ApplyExceptionRule;
import com.starrocks.sql.optimizer.rule.transformation.GroupByCountDistinctRewriteRule;
import com.starrocks.sql.optimizer.rule.transformation.LimitPruneTabletsRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoAggRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownAggToMetaScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnExpressionToChildProject;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitRankingWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateRankingWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownProjectLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.PushLimitAndFilterToCTEProduceRule;
import com.starrocks.sql.optimizer.rule.transformation.RemoveAggregationFromAggTable;
import com.starrocks.sql.optimizer.rule.transformation.RewriteGroupingSetsByCTERule;
import com.starrocks.sql.optimizer.rule.transformation.SemiReorderRule;
import com.starrocks.sql.optimizer.rule.tree.AddDecodeNodeForDictStringRule;
import com.starrocks.sql.optimizer.rule.tree.ExchangeSortToMergeRule;
import com.starrocks.sql.optimizer.rule.tree.PreAggregateTurnOnRule;
import com.starrocks.sql.optimizer.rule.tree.PredicateReorderRule;
import com.starrocks.sql.optimizer.rule.tree.PruneAggregateNodeRule;
import com.starrocks.sql.optimizer.rule.tree.PruneShuffleColumnRule;
import com.starrocks.sql.optimizer.rule.tree.PushDownAggregateRule;
import com.starrocks.sql.optimizer.rule.tree.ScalarOperatorsReuseRule;
import com.starrocks.sql.optimizer.rule.tree.UseSortAggregateRule;
import com.starrocks.sql.optimizer.task.OptimizeGroupTask;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Optimizer's entrance class
 */
public class Optimizer {
    private static final Logger LOG = LogManager.getLogger(Optimizer.class);
    private OptimizerContext context;
    private final OptimizerConfig optimizerConfig;

    public Optimizer() {
        this(OptimizerConfig.defaultConfig());
    }

    public Optimizer(OptimizerConfig config) {
        this.optimizerConfig = config;
    }

    @VisibleForTesting
    public OptimizerConfig getOptimizerConfig() {
        return optimizerConfig;
    }

    public OptimizerContext getContext() {
        return context;
    }

    public OptExpression optimize(ConnectContext connectContext,
                                  OptExpression logicOperatorTree,
                                  PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns,
                                  ColumnRefFactory columnRefFactory) {
        prepare(connectContext, logicOperatorTree, columnRefFactory);
        if (optimizerConfig.isRuleBased()) {
            return optimizeByRule(connectContext, logicOperatorTree, requiredProperty, requiredColumns);
        } else {
            return optimizeByCost(connectContext, logicOperatorTree, requiredProperty, requiredColumns);
        }
    }

    // Optimize by rule will return logical plan.
    // Used by materialized view query rewrite optimization.
    private OptExpression optimizeByRule(ConnectContext connectContext,
                                         OptExpression logicOperatorTree,
                                         PhysicalPropertySet requiredProperty,
                                         ColumnRefSet requiredColumns) {
        OptimizerTraceUtil.logOptExpression(connectContext, "origin logicOperatorTree:\n%s", logicOperatorTree);
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);
        logicOperatorTree = logicalRuleRewrite(logicOperatorTree, rootTaskContext);
        OptimizerTraceUtil.log(connectContext, "after logical rewrite, new logicOperatorTree:\n%s", logicOperatorTree);
        return logicOperatorTree;
    }

    /**
     * Optimizer will transform and implement the logical operator based on
     * the {@see Rule}, then cost the physical operator, and finally find the
     * lowest cost physical operator tree
     *
     * @param logicOperatorTree the input for query Optimizer
     * @param requiredProperty  the required physical property from sql or groupExpression
     * @param requiredColumns   the required output columns from sql or groupExpression
     * @return the lowest cost physical operator for this query
     */
    private OptExpression optimizeByCost(ConnectContext connectContext,
                                  OptExpression logicOperatorTree,
                                  PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns) {
        // Phase 1: none
        OptimizerTraceUtil.logOptExpression(connectContext, "origin logicOperatorTree:\n%s", logicOperatorTree);
        // Phase 2: rewrite based on memo and group
        Memo memo = context.getMemo();
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Optimizer.RuleBaseOptimize")) {
            logicOperatorTree = logicalRuleRewrite(logicOperatorTree, rootTaskContext);
        }

        memo.init(logicOperatorTree);
        OptimizerTraceUtil.log(connectContext, "after logical rewrite, root group:\n%s", memo.getRootGroup());

        // collect all olap scan operator
        collectAllScanOperators(memo, rootTaskContext);

        // Currently, we cache output columns in logic property.
        // We derive logic property Bottom Up firstly when new group added to memo,
        // but we do column prune rewrite top down later.
        // So after column prune rewrite, the output columns for each operator maybe change,
        // but the logic property is cached and never change.
        // So we need to explicitly derive all group logic property again
        memo.deriveAllGroupLogicalProperty();

        // Phase 3: optimize based on memo and group
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Optimizer.CostBaseOptimize")) {
            memoOptimize(connectContext, memo, rootTaskContext);
        }

        OptExpression result;
        if (!connectContext.getSessionVariable().isSetUseNthExecPlan()) {
            result = extractBestPlan(requiredProperty, memo.getRootGroup());
        } else {
            // extract the nth execution plan
            int nthExecPlan = connectContext.getSessionVariable().getUseNthExecPlan();
            result = EnumeratePlan.extractNthPlan(requiredProperty, memo.getRootGroup(), nthExecPlan);
        }
        OptimizerTraceUtil.logOptExpression(connectContext, "after extract best plan:\n%s", result);

        // set costs audio log before physicalRuleRewrite
        // statistics won't set correctly after physicalRuleRewrite.
        // we need set plan costs before physical rewrite stage.
        final CostEstimate costs = Explain.buildCost(result);
        connectContext.getAuditEventBuilder().setPlanCpuCosts(costs.getCpuCost())
                .setPlanMemCosts(costs.getMemoryCost());
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Optimizer.PhysicalRewrite")) {
            OptExpression finalPlan = physicalRuleRewrite(rootTaskContext, result);
            OptimizerTraceUtil.logOptExpression(connectContext, "final plan after physical rewrite:\n%s", finalPlan);
            OptimizerTraceUtil.log(connectContext, context.getTraceInfo());

            return finalPlan;
        }
    }

    private void prepare(ConnectContext connectContext, OptExpression logicOperatorTree, ColumnRefFactory columnRefFactory) {
        Memo memo = null;
        if (!optimizerConfig.isRuleBased()) {
            memo = new Memo();
        }

        context = new OptimizerContext(memo, columnRefFactory, connectContext);
        context.setTraceInfo(new OptimizerTraceInfo(connectContext.getQueryId()));

        // process materialized views
        // TODO: add session variable
        if (Config.enable_experimental_mv && !optimizerConfig.isRuleBased()) {
            // TODO: register materialized views
            // register materialized views
            registerMaterializedViews(logicOperatorTree, connectContext);
        }
    }

    private void registerMaterializedViews(OptExpression logicOperatorTree, ConnectContext connectContext) {
        List<Table> tables = Utils.getAllTables(logicOperatorTree);

        // include nested materialized views
        Set<MaterializedView> relatedMvs = Sets.newHashSet();
        Utils.getRelatedMvs(tables, relatedMvs);

        for (MaterializedView mv : relatedMvs) {
            if (!mv.isActive()) {
                continue;
            }
            Set<String> partitionNamesToRefresh = Sets.newHashSet();
            // Set<String> partitionNamesToRefresh = mv.getPartitionNamesToRefreshForMv();
            PartitionInfo partitionInfo = mv.getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                if (!partitionNamesToRefresh.isEmpty()) {
                    continue;
                }
            } else if (partitionNamesToRefresh.containsAll(mv.getPartitionNames())) {
                // if the mv is partitioned, and all partitions need refresh,
                // then it can not be an candidate
                continue;
            }

            // 1. build mv query logical plan
            String mvSql = mv.getViewDefineSql();
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            Pair<OptExpression, LogicalPlan> plans = Utils.getRuleOptimizedLogicalPlan(mvSql, columnRefFactory, connectContext);
            if (plans == null) {
                continue;
            }
            OptExpression mvPlan = plans.first;
            if (!Utils.isValidMVPlan(mvPlan)) {
                continue;
            }
            if (mv.getPartitionInfo() instanceof ExpressionRangePartitionInfo && !partitionNamesToRefresh.isEmpty()) {
                Utils.updatePartialPartitionPredicate(mv, columnRefFactory, partitionNamesToRefresh, mvPlan);
            }

            List<ColumnRefOperator> outputExpressions = plans.second.getOutputColumn();
            MaterializationContext materializationContext = new MaterializationContext(mv, mvPlan,
                    columnRefFactory, outputExpressions, partitionNamesToRefresh);

            // generate scan mv plan
            Database db = context.getCatalog().getDb(mv.getDbId());
            TableName tableName = new TableName(db.getFullName(), mv.getName());
            String selectMvSql = "select * from " + tableName.toSql();
            Pair<OptExpression, LogicalPlan> scanMvPlans =
                    Utils.getRuleOptimizedLogicalPlan(selectMvSql, context.getColumnRefFactory(), connectContext);
            OptExpression mvOptimizedPlan = scanMvPlans.first;
            if (!Utils.isLogicalSPJ(mvOptimizedPlan)) {
                continue;
            }
            if (!(mvOptimizedPlan.getOp() instanceof LogicalOlapScanOperator)) {
                continue;
            }
            materializationContext.setScanMvOperator(mvOptimizedPlan.getOp());
            List<ColumnRefOperator> mvOutputExpressions = scanMvPlans.second.getOutputColumn();
            materializationContext.setScanMvOutputExpressions(mvOutputExpressions);
            context.addCandidateMvs(materializationContext);
        }
    }

    private OptExpression logicalRuleRewrite(OptExpression tree, TaskContext rootTaskContext) {
        tree = OptExpression.create(new LogicalTreeAnchorOperator(), tree);
        ColumnRefSet requiredColumns = rootTaskContext.getRequiredColumns().clone();
        deriveLogicalProperty(tree);

        SessionVariable sessionVariable = rootTaskContext.getOptimizerContext().getSessionVariable();
        CTEContext cteContext = context.getCteContext();
        CTEUtils.collectCteOperators(tree, context);
        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.INLINE_CTE);
            CTEUtils.collectCteOperators(tree, context);
        }

        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.AGGREGATE_REWRITE);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_SUBQUERY);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SUBQUERY_REWRITE_COMMON);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SUBQUERY_REWRITE_TO_WINDOW);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SUBQUERY_REWRITE_TO_JOIN);
        ruleRewriteOnlyOnce(tree, rootTaskContext, new ApplyExceptionRule());
        CTEUtils.collectCteOperators(tree, context);

        // Note: PUSH_DOWN_PREDICATE tasks should be executed before MERGE_LIMIT tasks
        // because of the Filter node needs to be merged first to avoid the Limit node
        // cannot merge
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);

        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownAggToMetaScanRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownPredicateRankingWindowRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownJoinOnExpressionToChildProject());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
        deriveLogicalProperty(tree);

        ruleRewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        // @todo: resolve recursive optimization question:
        //  MergeAgg -> PruneColumn -> PruneEmptyWindow -> MergeAgg/Project -> PruneColumn...
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoAggRule());
        rootTaskContext.setRequiredColumns(requiredColumns.clone());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);

        ruleRewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        //Limit push must be after the column prune,
        //otherwise the Node containing limit may be prune
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.MERGE_LIMIT);
        ruleRewriteIterative(tree, rootTaskContext, new PushDownProjectLimitRule());

        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownLimitRankingWindowRule());
        if (sessionVariable.isEnableRewriteGroupingsetsToUnionAll()) {
            ruleRewriteIterative(tree, rootTaskContext, new RewriteGroupingSetsByCTERule());
        }

        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_ASSERT_ROW);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_PROJECT);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_SET_OPERATOR);

        CTEUtils.collectCteOperators(tree, context);
        if (cteContext.needOptimizeCTE()) {
            cteContext.reset();
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.COLLECT_CTE);
            rootTaskContext.setRequiredColumns(requiredColumns.clone());
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
            if (cteContext.needPushLimit() || cteContext.needPushPredicate()) {
                ruleRewriteOnlyOnce(tree, rootTaskContext, new PushLimitAndFilterToCTEProduceRule());
            }

            if (cteContext.needPushPredicate()) {
                ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
            }

            if (cteContext.needPushLimit()) {
                ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.MERGE_LIMIT);
            }
        }

        tree = new MaterializedViewRule().transform(tree, context).get(0);
        deriveLogicalProperty(tree);

        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.MULTI_DISTINCT_REWRITE);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);

        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PARTITION_PRUNE);
        ruleRewriteOnlyOnce(tree, rootTaskContext, LimitPruneTabletsRule.getInstance());
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_PROJECT);

        tree = pushDownAggregation(tree, rootTaskContext, requiredColumns);

        CTEUtils.collectCteOperators(tree, context);
        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.INLINE_CTE);
            CTEUtils.collectCteOperators(tree, context);
        }

        ruleRewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteIterative(tree, rootTaskContext, new MergeProjectWithChildRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new GroupByCountDistinctRewriteRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.INTERSECT_REWRITE);
        ruleRewriteIterative(tree, rootTaskContext, new RemoveAggregationFromAggTable());

        return tree.getInputs().get(0);
    }

    private OptExpression pushDownAggregation(OptExpression tree, TaskContext rootTaskContext,
                                              ColumnRefSet requiredColumns) {
        if (context.getSessionVariable().getCboPushDownAggregateMode() == -1) {
            return tree;
        }

        tree = new PushDownAggregateRule().rewrite(tree, rootTaskContext);
        deriveLogicalProperty(tree);

        rootTaskContext.setRequiredColumns(requiredColumns.clone());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
        return tree;
    }

    private void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        ExpressionContext context = new ExpressionContext(root);
        context.deriveLogicalProperty();
        root.setLogicalProperty(context.getRootProperty());
    }

    void memoOptimize(ConnectContext connectContext, Memo memo, TaskContext rootTaskContext) {
        OptExpression tree = memo.getRootGroup().extractLogicalTree();
        // Join reorder
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (!sessionVariable.isDisableJoinReorder()
                && Utils.countInnerJoinNodeSize(tree) < sessionVariable.getCboMaxReorderNode()) {
            if (Utils.countInnerJoinNodeSize(tree) > sessionVariable.getCboMaxReorderNodeUseExhaustive()) {
                CTEUtils.collectForceCteStatistics(memo, context);
                new ReorderJoinRule().transform(tree, context);
                context.getRuleSet().addJoinCommutativityWithOutInnerRule();
            } else {
                if (Utils.capableSemiReorder(tree, false, 0, sessionVariable.getCboMaxReorderNodeUseExhaustive())) {
                    context.getRuleSet().getTransformRules().add(new SemiReorderRule());
                }
                context.getRuleSet().addJoinTransformationRules();
            }
        }

        //add join implementRule
        String joinImplementationMode = ConnectContext.get().getSessionVariable().getJoinImplementationMode();
        if ("merge".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addMergeJoinImplementationRule();
        } else if ("hash".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addHashJoinImplementationRule();
        } else if ("nestloop".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addNestLoopJoinImplementationRule();
        } else {
            context.getRuleSet().addAutoJoinImplementationRule();
        }

        // add realtime mv rules
        if (sessionVariable.isMVPlanner() && sessionVariable.isEnableRealtimeRefreshMV()) {
            context.getRuleSet().addRealtimeMVRules();
        }

        context.getTaskScheduler().pushTask(new OptimizeGroupTask(rootTaskContext, memo.getRootGroup()));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private OptExpression physicalRuleRewrite(TaskContext rootTaskContext, OptExpression result) {
        Preconditions.checkState(result.getOp().isPhysical());

        int planCount = result.getPlanCount();

        // Since there may be many different plans in the logic phase, it's possible
        // that this switch can't turned on after logical optimization, so we only determine
        // whether the PreAggregate can be turned on in the final
        result = new PreAggregateTurnOnRule().rewrite(result, rootTaskContext);

        // Rewrite Exchange on top of Sort to Final Sort
        result = new ExchangeSortToMergeRule().rewrite(result, rootTaskContext);
        result = new PruneAggregateNodeRule().rewrite(result, rootTaskContext);
        result = new PruneShuffleColumnRule().rewrite(result, rootTaskContext);
        result = new AddDecodeNodeForDictStringRule().rewrite(result, rootTaskContext);
        // This rule should be last
        result = new ScalarOperatorsReuseRule().rewrite(result, rootTaskContext);
        // Reorder predicates
        result = new PredicateReorderRule(rootTaskContext.getOptimizerContext().getSessionVariable()).rewrite(result,
                rootTaskContext);
        result = new UseSortAggregateRule().rewrite(result, rootTaskContext);

        result.setPlanCount(planCount);
        return result;
    }

    /**
     * Extract the lowest cost physical operator tree from memo
     *
     * @param requiredProperty the required physical property from sql or groupExpression
     * @param rootGroup        the current group to find the lowest cost physical operator
     * @return the lowest cost physical operator for this query
     */
    private OptExpression extractBestPlan(PhysicalPropertySet requiredProperty,
                                          Group rootGroup) {
        GroupExpression groupExpression = rootGroup.getBestExpression(requiredProperty);
        Preconditions.checkNotNull(groupExpression, "no executable plan for this sql");
        List<PhysicalPropertySet> inputProperties = groupExpression.getInputProperties(requiredProperty);

        List<OptExpression> childPlans = Lists.newArrayList();
        for (int i = 0; i < groupExpression.arity(); ++i) {
            OptExpression childPlan = extractBestPlan(inputProperties.get(i), groupExpression.inputAt(i));
            childPlans.add(childPlan);
        }

        OptExpression expression = OptExpression.create(groupExpression.getOp(),
                childPlans);
        // record inputProperties at optExpression, used for planFragment builder to determine join type
        expression.setRequiredProperties(inputProperties);
        expression.setOutputProperty(groupExpression.getOutputProperty(requiredProperty));
        expression.setStatistics(groupExpression.getGroup().hasConfidenceStatistic(requiredProperty) ?
                groupExpression.getGroup().getConfidenceStatistic(requiredProperty) :
                groupExpression.getGroup().getStatistics());
        expression.setCost(groupExpression.getCost(requiredProperty));

        // When build plan fragment, we need the output column of logical property
        expression.setLogicalProperty(rootGroup.getLogicalProperty());
        return expression;
    }

    private void collectAllScanOperators(Memo memo, TaskContext rootTaskContext) {
        OptExpression tree = memo.getRootGroup().extractLogicalTree();
        List<LogicalOlapScanOperator> list = Lists.newArrayList();
        Utils.extractOlapScanOperator(tree.getGroupExpression(), list);
        rootTaskContext.setAllScanOperators(Collections.unmodifiableList(list));
    }

    private void ruleRewriteIterative(OptExpression tree, TaskContext rootTaskContext, RuleSetType ruleSetType) {
        if (optimizerConfig.isRuleSetTypeDisable(ruleSetType)) {
            return;
        }
        List<Rule> rules = rootTaskContext.getOptimizerContext().getRuleSet().getRewriteRulesByType(ruleSetType);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, false));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteIterative(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        if (optimizerConfig.isRuleDisable(rule.type())) {
            return;
        }
        List<Rule> rules = Collections.singletonList(rule);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, false));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteOnlyOnce(OptExpression tree, TaskContext rootTaskContext, RuleSetType ruleSetType) {
        if (optimizerConfig.isRuleSetTypeDisable(ruleSetType)) {
            return;
        }
        List<Rule> rules = rootTaskContext.getOptimizerContext().getRuleSet().getRewriteRulesByType(ruleSetType);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, true));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteOnlyOnce(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        if (optimizerConfig.isRuleDisable(rule.type())) {
            return;
        }
        List<Rule> rules = Collections.singletonList(rule);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, true));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }
}
