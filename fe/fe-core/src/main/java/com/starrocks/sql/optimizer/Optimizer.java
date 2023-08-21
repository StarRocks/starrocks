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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.VectorSearchOptions;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.Explain;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.rewrite.JoinPredicatePushdown;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.implementation.OlapScanImplementationRule;
import com.starrocks.sql.optimizer.rule.join.ReorderJoinRule;
import com.starrocks.sql.optimizer.rule.mv.MaterializedViewRule;
import com.starrocks.sql.optimizer.rule.transformation.ApplyExceptionRule;
import com.starrocks.sql.optimizer.rule.transformation.ArrayDistinctAfterAggRule;
import com.starrocks.sql.optimizer.rule.transformation.CTEProduceAddProjectionRule;
import com.starrocks.sql.optimizer.rule.transformation.ConvertToEqualForNullRule;
import com.starrocks.sql.optimizer.rule.transformation.DeriveRangeJoinPredicateRule;
import com.starrocks.sql.optimizer.rule.transformation.EliminateAggRule;
import com.starrocks.sql.optimizer.rule.transformation.ForceCTEReuseRule;
import com.starrocks.sql.optimizer.rule.transformation.GroupByCountDistinctRewriteRule;
import com.starrocks.sql.optimizer.rule.transformation.IcebergPartitionsTableRewriteRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinLeftAsscomRule;
import com.starrocks.sql.optimizer.rule.transformation.MaterializedViewTransparentRewriteRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoAggRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.OnPredicateMoveAroundRule;
import com.starrocks.sql.optimizer.rule.transformation.PartitionColumnMinMaxRewriteRule;
import com.starrocks.sql.optimizer.rule.transformation.PartitionColumnValueOnlyOnScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownAggregateGroupingSetsRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnExpressionToChildProject;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitRankingWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateRankingWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownProjectLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownTopNBelowOuterJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownTopNBelowUnionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushLimitAndFilterToCTEProduceRule;
import com.starrocks.sql.optimizer.rule.transformation.RemoveAggregationFromAggTable;
import com.starrocks.sql.optimizer.rule.transformation.RewriteGroupingSetsByCTERule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteMultiDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteSimpleAggToHDFSScanRule;
import com.starrocks.sql.optimizer.rule.transformation.SchemaTableEvaluateRule;
import com.starrocks.sql.optimizer.rule.transformation.SeparateProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.SkewJoinOptimizeRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitScanORToUnionRule;
import com.starrocks.sql.optimizer.rule.transformation.UnionToValuesRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvRewriteStrategy;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.TextMatchBasedRewriteRule;
import com.starrocks.sql.optimizer.rule.transformation.pruner.CboTablePruneRule;
import com.starrocks.sql.optimizer.rule.transformation.pruner.PrimaryKeyUpdateTableRule;
import com.starrocks.sql.optimizer.rule.transformation.pruner.RboTablePruneRule;
import com.starrocks.sql.optimizer.rule.transformation.pruner.UniquenessBasedTablePruneRule;
import com.starrocks.sql.optimizer.rule.tree.AddDecodeNodeForDictStringRule;
import com.starrocks.sql.optimizer.rule.tree.AddIndexOnlyPredicateRule;
import com.starrocks.sql.optimizer.rule.tree.CloneDuplicateColRefRule;
import com.starrocks.sql.optimizer.rule.tree.DataCachePopulateRewriteRule;
import com.starrocks.sql.optimizer.rule.tree.ExchangeSortToMergeRule;
import com.starrocks.sql.optimizer.rule.tree.ExtractAggregateColumn;
import com.starrocks.sql.optimizer.rule.tree.InlineCteProjectPruneRule;
import com.starrocks.sql.optimizer.rule.tree.JoinLocalShuffleRule;
import com.starrocks.sql.optimizer.rule.tree.MarkParentRequiredDistributionRule;
import com.starrocks.sql.optimizer.rule.tree.PhysicalDistributionAggOptRule;
import com.starrocks.sql.optimizer.rule.tree.PreAggregateTurnOnRule;
import com.starrocks.sql.optimizer.rule.tree.PredicateReorderRule;
import com.starrocks.sql.optimizer.rule.tree.PruneAggregateNodeRule;
import com.starrocks.sql.optimizer.rule.tree.PruneShuffleColumnRule;
import com.starrocks.sql.optimizer.rule.tree.PruneSubfieldsForComplexType;
import com.starrocks.sql.optimizer.rule.tree.PushDownAggregateRule;
import com.starrocks.sql.optimizer.rule.tree.PushDownDistinctAggregateRule;
import com.starrocks.sql.optimizer.rule.tree.ScalarOperatorsReuseRule;
import com.starrocks.sql.optimizer.rule.tree.SimplifyCaseWhenPredicateRule;
import com.starrocks.sql.optimizer.rule.tree.SubfieldExprNoCopyRule;
import com.starrocks.sql.optimizer.rule.tree.lowcardinality.LowCardinalityRewriteRule;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.PruneSubfieldRule;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.PushDownSubfieldRule;
import com.starrocks.sql.optimizer.task.OptimizeGroupTask;
import com.starrocks.sql.optimizer.task.PrepareCollectMetaTask;
import com.starrocks.sql.optimizer.task.RewriteAtMostOnceTask;
import com.starrocks.sql.optimizer.task.RewriteDownTopTask;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.sql.optimizer.validate.MVRewriteValidator;
import com.starrocks.sql.optimizer.validate.OptExpressionValidator;
import com.starrocks.sql.optimizer.validate.PlanValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.RuleType.TF_MATERIALIZED_VIEW;

/**
 * Optimizer's entrance class
 */
public class Optimizer {
    private static final Logger LOG = LogManager.getLogger(Optimizer.class);
    private OptimizerContext context;
    private final OptimizerConfig optimizerConfig;
    private MvRewriteStrategy mvRewriteStrategy = new MvRewriteStrategy();

    private long updateTableId = -1;

    private Set<OlapTable> queryTables;

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

    @VisibleForTesting
    public MvRewriteStrategy getMvRewriteStrategy() {
        return mvRewriteStrategy;
    }

    public OptExpression optimize(ConnectContext connectContext,
                                  OptExpression logicOperatorTree,
                                  PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns,
                                  ColumnRefFactory columnRefFactory) {
        return optimize(connectContext, logicOperatorTree, null, null, requiredProperty,
                requiredColumns, columnRefFactory, new VectorSearchOptions());
    }

    public OptExpression optimize(ConnectContext connectContext,
                                  OptExpression logicOperatorTree,
                                  MVTransformerContext mvTransformerContext,
                                  StatementBase stmt,
                                  PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns,
                                  ColumnRefFactory columnRefFactory,
                                  VectorSearchOptions vectorSearchOptions) {
        try {
            // prepare for optimizer
            prepare(connectContext, columnRefFactory, logicOperatorTree);

            context.setVectorSearchOptions(vectorSearchOptions);

            // prepare for mv rewrite
            prepareMvRewrite(connectContext, logicOperatorTree, columnRefFactory, requiredColumns);
            try (Timer ignored = Tracers.watchScope("MVTextRewrite")) {
                logicOperatorTree = new TextMatchBasedRewriteRule(connectContext, stmt, mvTransformerContext)
                        .transform(logicOperatorTree, context).get(0);
            }

            OptExpression result = optimizerConfig.isRuleBased() ?
                    optimizeByRule(logicOperatorTree, requiredProperty, requiredColumns) :
                    optimizeByCost(connectContext, logicOperatorTree, requiredProperty, requiredColumns);
            return result;
        } finally {
            // make sure clear caches in OptimizerContext
            context.clear();
            connectContext.setQueryMVContext(null);
        }
    }

    public void setQueryTables(Set<OlapTable> queryTables) {
        this.queryTables = queryTables;
    }

    public void setUpdateTableId(long updateTableId) {
        this.updateTableId = updateTableId;
    }

    // Optimize by rule will return logical plan.
    // Used by materialized view query rewrite optimization.
    private OptExpression optimizeByRule(OptExpression logicOperatorTree,
                                         PhysicalPropertySet requiredProperty,
                                         ColumnRefSet requiredColumns) {
        OptimizerTraceUtil.logOptExpression("origin logicOperatorTree:\n%s", logicOperatorTree);
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);
        logicOperatorTree = rewriteAndValidatePlan(logicOperatorTree, rootTaskContext);
        OptimizerTraceUtil.log("after logical rewrite, new logicOperatorTree:\n%s", logicOperatorTree);
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
        OptimizerTraceUtil.logOptExpression("origin logicOperatorTree:\n%s", logicOperatorTree);
        // Phase 2: rewrite based on memo and group
        Memo memo = context.getMemo();
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);

        try (Timer ignored = Tracers.watchScope("RuleBaseOptimize")) {
            logicOperatorTree = rewriteAndValidatePlan(logicOperatorTree, rootTaskContext);
        }

        if (logicOperatorTree.getShortCircuit()) {
            return logicOperatorTree;
        }

        memo.init(logicOperatorTree);
        if (context.getQueryMaterializationContext() != null) {
            // LogicalTreeWithView is logically equivalent to logicOperatorTree
            addViewBasedPlanIntoMemo(context.getQueryMaterializationContext().getQueryOptPlanWithView());
        }
        OptimizerTraceUtil.log("after logical rewrite, root group:\n%s", memo.getRootGroup());

        // Currently, we cache output columns in logic property.
        // We derive logic property Bottom Up firstly when new group added to memo,
        // but we do column prune rewrite top down later.
        // So after column prune rewrite, the output columns for each operator maybe change,
        // but the logic property is cached and never change.
        // So we need to explicitly derive all group logic property again
        memo.deriveAllGroupLogicalProperty();

        // Phase 3: optimize based on memo and group
        try (Timer ignored = Tracers.watchScope("CostBaseOptimize")) {
            memoOptimize(connectContext, memo, rootTaskContext);
        }

        OptExpression result;
        if (connectContext.getSessionVariable().isSetUseNthExecPlan()) {
            // extract the nth execution plan
            int nthExecPlan = connectContext.getSessionVariable().getUseNthExecPlan();
            result = EnumeratePlan.extractNthPlan(requiredProperty, memo.getRootGroup(), nthExecPlan);
        } else {
            result = extractBestPlan(requiredProperty, memo.getRootGroup());
        }
        OptimizerTraceUtil.logOptExpression("after extract best plan:\n%s", result);

        // set costs audio log before physicalRuleRewrite
        // statistics won't set correctly after physicalRuleRewrite.
        // we need set plan costs before physical rewrite stage.
        final CostEstimate costs = Explain.buildCost(result);
        connectContext.getAuditEventBuilder().setPlanCpuCosts(costs.getCpuCost())
                .setPlanMemCosts(costs.getMemoryCost());
        OptExpression finalPlan;
        try (Timer ignored = Tracers.watchScope("PhysicalRewrite")) {
            finalPlan = physicalRuleRewrite(connectContext, rootTaskContext, result);
            OptimizerTraceUtil.logOptExpression("final plan after physical rewrite:\n%s", finalPlan);
        }

        try (Timer ignored = Tracers.watchScope("DynamicRewrite")) {
            finalPlan = dynamicRewrite(connectContext, rootTaskContext, finalPlan);
            OptimizerTraceUtil.logOptExpression("final plan after dynamic rewrite:\n%s", finalPlan);
        }

        // collect all mv scan operator
        collectAllPhysicalOlapScanOperators(result, rootTaskContext);
        List<PhysicalOlapScanOperator> mvScan = rootTaskContext.getAllPhysicalOlapScanOperators().stream().
                filter(scan -> scan.getTable().isMaterializedView()).collect(Collectors.toList());
        // add mv db id to currentSqlDbIds, the resource group could use this to distinguish sql patterns
        Set<Long> currentSqlDbIds = rootTaskContext.getOptimizerContext().getCurrentSqlDbIds();
        mvScan.stream().map(scan -> ((MaterializedView) scan.getTable()).getDbId()).forEach(currentSqlDbIds::add);

        try (Timer ignored = Tracers.watchScope("PlanValidate")) {
            // valid the final plan
            PlanValidator.getInstance().validatePlan(finalPlan, rootTaskContext);
            // validate mv and log tracer if needed
            MVRewriteValidator.getInstance().validateMV(connectContext, finalPlan, rootTaskContext);
            // audit mv
            MVRewriteValidator.getInstance().auditMv(connectContext, finalPlan, rootTaskContext);
            return finalPlan;
        }
    }

    private void addViewBasedPlanIntoMemo(OptExpression logicalTreeWithView) {
        if (logicalTreeWithView == null) {
            return;
        }
        Memo memo = context.getMemo();
        memo.copyIn(memo.getRootGroup(), logicalTreeWithView);
    }

    private void prepare(ConnectContext connectContext,
                         ColumnRefFactory columnRefFactory,
                         OptExpression logicOperatorTree) {
        Memo memo = null;
        if (!optimizerConfig.isRuleBased()) {
            memo = new Memo();
        }

        context = new OptimizerContext(memo, columnRefFactory, connectContext, optimizerConfig);
        context.setQueryTables(queryTables);
        context.setUpdateTableId(updateTableId);

        // collect all olap scan operator
        collectAllLogicalOlapScanOperators(logicOperatorTree, context);

    }

    private void prepareMvRewrite(ConnectContext connectContext, OptExpression logicOperatorTree,
                                  ColumnRefFactory columnRefFactory, ColumnRefSet requiredColumns) {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        // MV Rewrite will be used when cbo is enabled.
        if (context.getOptimizerConfig().isRuleBased() || sessionVariable.isDisableMaterializedViewRewrite() ||
                !sessionVariable.isEnableMaterializedViewRewrite()) {
            return;
        }
        // prepare related mvs if needed and initialize mv rewrite strategy
        new MvRewritePreprocessor(connectContext, columnRefFactory, context, requiredColumns)
                .prepare(logicOperatorTree);

        // initialize mv rewrite strategy finally
        mvRewriteStrategy = MvRewriteStrategy.prepareRewriteStrategy(context, connectContext, logicOperatorTree);
        OptimizerTraceUtil.logMVPrepare("MV rewrite strategy: {}", mvRewriteStrategy);
    }

    private void pruneTables(OptExpression tree, TaskContext rootTaskContext, ColumnRefSet requiredColumns) {
        if (rootTaskContext.getOptimizerContext().getSessionVariable().isEnableRboTablePrune()) {
            if (!Utils.hasPrunableJoin(tree)) {
                return;
            }
            // PARTITION_PRUNE is required to run before ReorderJoinRule because ReorderJoinRule's
            // Statistics calculation on Operators depends on row count yielded by the PARTITION_PRUNE.
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PARTITION_PRUNE);
            // ReorderJoinRule is a in-memo rule, when it is used outside memo, we must apply
            // MergeProjectWithChildRule to merge LogicalProjectionOperator into its child's
            // projection before ReorderJoinRule's application, after that, we must separate operator's
            // projection as LogicalProjectionOperator from the operator by applying SeparateProjectRule.
            ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
            ruleRewriteIterative(tree, rootTaskContext, new MergeProjectWithChildRule());
            CTEUtils.collectForceCteStatisticsOutsideMemo(tree, context);
            tree = new UniquenessBasedTablePruneRule().rewrite(tree, rootTaskContext);
            deriveLogicalProperty(tree);
            tree = new ReorderJoinRule().rewrite(tree, context);
            tree = new SeparateProjectRule().rewrite(tree, rootTaskContext);
            deriveLogicalProperty(tree);
            // TODO(by satanson): bucket shuffle join interpolation in PK table's update query can adjust layout
            //  of the data ingested by OlapTableSink and eliminate race introduced by multiple concurrent write
            //  operations on the same tablets, pruning this bucket shuffle join make update statement performance
            //  regression, so we can turn on this rule after we put an bucket-shuffle exchange in front of
            //  OlapTableSink in future, at present we turn off this rule.
            if (rootTaskContext.getOptimizerContext().getSessionVariable().isEnableTablePruneOnUpdate()) {
                tree = new PrimaryKeyUpdateTableRule().rewrite(tree, rootTaskContext);
                deriveLogicalProperty(tree);
            }
            tree = new RboTablePruneRule().rewrite(tree, rootTaskContext);
            ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
            rootTaskContext.setRequiredColumns(requiredColumns.clone());
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
            ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
        }
    }

    /**
     * Rewrite transparent materialized view.
     */
    private OptExpression transparentMVRewrite(OptExpression tree, TaskContext rootTaskContext) {
        ruleRewriteOnlyOnce(tree, rootTaskContext, new MaterializedViewTransparentRewriteRule());
        if (Utils.isOptHasAppliedRule(tree, Operator.OP_TRANSPARENT_MV_BIT)) {
            tree = new SeparateProjectRule().rewrite(tree, rootTaskContext);
        }
        return tree;
    }

    private void ruleBasedMaterializedViewRewrite(OptExpression tree,
                                                  TaskContext rootTaskContext) {
        if (!mvRewriteStrategy.enableMaterializedViewRewrite || context.getQueryMaterializationContext() == null ||
                context.getQueryMaterializationContext().hasRewrittenSuccess()) {
            return;
        }

        // do rule based mv rewrite
        doRuleBasedMaterializedViewRewrite(tree, rootTaskContext);

        // NOTE: Since union rewrite will generate Filter -> Union -> OlapScan -> OlapScan, need to push filter below Union
        // and do partition predicate again.
        // TODO: Do it in CBO if needed later.
        if (MvUtils.isAppliedMVUnionRewrite(tree)) {
            // Do predicate push down if union rewrite successes.
            tree = new SeparateProjectRule().rewrite(tree, rootTaskContext);
            deriveLogicalProperty(tree);
            ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
            // It's necessary for external table since its predicate is not used directly after push down.
            ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PARTITION_PRUNE);
            ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        }
    }

    private void doRuleBasedMaterializedViewRewrite(OptExpression tree,
                                                    TaskContext rootTaskContext) {
        if (mvRewriteStrategy.enableViewBasedRewrite) {
            // try view based mv rewrite first, then try normal mv rewrite rules
            viewBasedMvRuleRewrite(tree, rootTaskContext);
        }
        if (mvRewriteStrategy.enableForceRBORewrite) {
            // use rule based mv rewrite strategy to do mv rewrite for multi tables query
            if (mvRewriteStrategy.enableMultiTableRewrite) {
                ruleRewriteIterative(tree, rootTaskContext, RuleSetType.MULTI_TABLE_MV_REWRITE);
            }
            if (mvRewriteStrategy.enableSingleTableRewrite) {
                ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SINGLE_TABLE_MV_REWRITE);
            }
        } else if (mvRewriteStrategy.enableSingleTableRewrite) {
            // now add single table materialized view rewrite rules in rule based rewrite phase to boost optimization
            ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SINGLE_TABLE_MV_REWRITE);
        }
    }

    private void doMVRewriteWithMultiStages(OptExpression tree,
                                            TaskContext rootTaskContext) {
        if (!mvRewriteStrategy.mvStrategy.isMultiStages()) {
            return;
        }
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PARTITION_PRUNE);
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteIterative(tree, rootTaskContext, new MergeProjectWithChildRule());
        // do rule based mv rewrite
        doRuleBasedMaterializedViewRewrite(tree, rootTaskContext);
        new SeparateProjectRule().rewrite(tree, rootTaskContext);
        deriveLogicalProperty(tree);
    }

    private OptExpression logicalRuleRewrite(
            OptExpression tree,
            TaskContext rootTaskContext) {
        rootTaskContext.getOptimizerContext().setShortCircuit(tree.getShortCircuit());
        tree = OptExpression.createForShortCircuit(new LogicalTreeAnchorOperator(), tree, tree.getShortCircuit());
        // for short circuit
        Optional<OptExpression> result = ruleRewriteForShortCircuit(tree, rootTaskContext);
        if (result.isPresent()) {
            return result.get();
        }

        ColumnRefSet requiredColumns = rootTaskContext.getRequiredColumns().clone();
        deriveLogicalProperty(tree);

        SessionVariable sessionVariable = rootTaskContext.getOptimizerContext().getSessionVariable();
        CTEContext cteContext = context.getCteContext();
        CTEUtils.collectCteOperators(tree, context);

        // see JoinPredicatePushdown
        JoinPredicatePushdown.JoinPredicatePushDownContext joinPredicatePushDownContext = context.getJoinPushDownParams();
        joinPredicatePushDownContext.prepare(context, sessionVariable, mvRewriteStrategy);

        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.INLINE_CTE);
            CTEUtils.collectCteOperators(tree, context);
        }

        ruleRewriteOnlyOnce(tree, rootTaskContext, new IcebergPartitionsTableRewriteRule());
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.AGGREGATE_REWRITE);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_SUBQUERY);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SUBQUERY_REWRITE_COMMON);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SUBQUERY_REWRITE_TO_WINDOW);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SUBQUERY_REWRITE_TO_JOIN);
        ruleRewriteOnlyOnce(tree, rootTaskContext, new ApplyExceptionRule());
        CTEUtils.collectCteOperators(tree, context);

        if (sessionVariable.isEnableFineGrainedRangePredicate()) {
            ruleRewriteAtMostOnce(tree, rootTaskContext, RuleSetType.FINE_GRAINED_RANGE_PREDICATE);
        }

        // rewrite transparent materialized view
        tree = transparentMVRewrite(tree, rootTaskContext);

        // Note: PUSH_DOWN_PREDICATE tasks should be executed before MERGE_LIMIT tasks
        // because of the Filter node needs to be merged first to avoid the Limit node
        // cannot merge
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
        ruleRewriteOnlyOnce(tree, rootTaskContext, SchemaTableEvaluateRule.getInstance());

        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.ELIMINATE_OP_WITH_CONSTANT);
        ruleRewriteOnlyOnce(tree, rootTaskContext, EliminateAggRule.getInstance());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownPredicateRankingWindowRule());

        ruleRewriteOnlyOnce(tree, rootTaskContext, new ConvertToEqualForNullRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_UKFK_JOIN);
        deriveLogicalProperty(tree);

        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownJoinOnExpressionToChildProject());

        ruleRewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        // @todo: resolve recursive optimization question:
        //  MergeAgg -> PruneColumn -> PruneEmptyWindow -> MergeAgg/Project -> PruneColumn...
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoAggRule());

        rootTaskContext.setRequiredColumns(requiredColumns.clone());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);

        pruneTables(tree, rootTaskContext, requiredColumns);

        ruleRewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());

        // rule-based materialized view rewrite: early stage
        doMVRewriteWithMultiStages(tree, rootTaskContext);
        joinPredicatePushDownContext.reset();

        // Limit push must be after the column prune,
        // otherwise the Node containing limit may be prune
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.MERGE_LIMIT);
        ruleRewriteIterative(tree, rootTaskContext, new PushDownProjectLimitRule());

        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownLimitRankingWindowRule());
        rewriteGroupingSets(tree, rootTaskContext, sessionVariable);

        // No heavy metadata operation before external table partition prune
        prepareMetaOnlyOnce(tree, rootTaskContext);

        // apply skew join optimize after push down join on expression to child project,
        // we need to compute the stats of child project(like subfield).
        skewJoinOptimize(tree, rootTaskContext);

        tree = pruneSubfield(tree, rootTaskContext, requiredColumns);

        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_ASSERT_ROW);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_PROJECT);

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

            ruleRewriteOnlyOnce(tree, rootTaskContext, new ForceCTEReuseRule());
        }

        // Add a config to decide whether to rewrite sync mv.
        if (!optimizerConfig.isRuleDisable(TF_MATERIALIZED_VIEW)
                && sessionVariable.isEnableSyncMaterializedViewRewrite()) {
            // Split or predicates to union all so can be used by mv rewrite to choose the best sort key indexes.
            // TODO: support adaptive for or-predicates to union all.
            if (SplitScanORToUnionRule.isForceRewrite()) {
                ruleRewriteOnlyOnce(tree, rootTaskContext, SplitScanORToUnionRule.getInstance());
            }

            OptimizerTraceUtil.logOptExpression("before MaterializedViewRule:\n%s", tree);
            tree = new MaterializedViewRule().transform(tree, context).get(0);
            OptimizerTraceUtil.logOptExpression("after MaterializedViewRule:\n%s", tree);

            deriveLogicalProperty(tree);
        }

        ruleRewriteDownTop(tree, rootTaskContext, OnPredicateMoveAroundRule.INSTANCE);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);

        ruleRewriteIterative(tree, rootTaskContext, new PartitionColumnMinMaxRewriteRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PARTITION_PRUNE);
        ruleRewriteIterative(tree, rootTaskContext, new RewriteMultiDistinctRule());
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_EMPTY_OPERATOR);
        ruleRewriteIterative(tree, rootTaskContext, new CTEProduceAddProjectionRule());
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_PROJECT);

        // ArrayDistinctAfterAggRule must run before pushDownAggregation,
        // because push down agg won't have array_distinct project
        if (sessionVariable.getEnableArrayDistinctAfterAggOpt()) {
            ruleRewriteOnlyOnce(tree, rootTaskContext, new ArrayDistinctAfterAggRule());
        }

        tree = pushDownAggregation(tree, rootTaskContext, requiredColumns);
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.MERGE_LIMIT);

        CTEUtils.collectCteOperators(tree, context);
        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.INLINE_CTE);
            CTEUtils.collectCteOperators(tree, context);
        }

        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.META_SCAN_REWRITE);
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PartitionColumnValueOnlyOnScanRule());

        // After this rule, we shouldn't generate logical project operator
        ruleRewriteIterative(tree, rootTaskContext, new MergeProjectWithChildRule());

        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownTopNBelowOuterJoinRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.INTERSECT_REWRITE);
        ruleRewriteIterative(tree, rootTaskContext, new RemoveAggregationFromAggTable());

        ruleRewriteOnlyOnce(tree, rootTaskContext, SplitScanORToUnionRule.getInstance());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownTopNBelowUnionRule());

        // rule based materialized view rewrite
        ruleBasedMaterializedViewRewrite(tree, rootTaskContext);

        // this rewrite rule should be after mv.
        ruleRewriteIterative(tree, rootTaskContext, RewriteSimpleAggToHDFSScanRule.HIVE_SCAN_NO_PROJECT);
        ruleRewriteIterative(tree, rootTaskContext, RewriteSimpleAggToHDFSScanRule.ICEBERG_SCAN_NO_PROJECT);
        ruleRewriteIterative(tree, rootTaskContext, RewriteSimpleAggToHDFSScanRule.FILE_SCAN_NO_PROJECT);

        // NOTE: This rule should be after MV Rewrite because MV Rewrite cannot handle
        // select count(distinct c) from t group by a, b
        // if this rule has applied before MV.
        ruleRewriteOnlyOnce(tree, rootTaskContext, new GroupByCountDistinctRewriteRule());

        ruleRewriteOnlyOnce(tree, rootTaskContext, new DeriveRangeJoinPredicateRule());

        ruleRewriteOnlyOnce(tree, rootTaskContext, UnionToValuesRule.getInstance());

        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.VECTOR_REWRITE);

        tree = SimplifyCaseWhenPredicateRule.INSTANCE.rewrite(tree, rootTaskContext);
        deriveLogicalProperty(tree);
        return tree.getInputs().get(0);
    }

    private void rewriteGroupingSets(OptExpression tree, TaskContext rootTaskContext, SessionVariable sessionVariable) {
        if (sessionVariable.isEnableRewriteGroupingsetsToUnionAll()) {
            ruleRewriteIterative(tree, rootTaskContext, new RewriteGroupingSetsByCTERule());
        }
        if (sessionVariable.isCboPushDownGroupingSet()) {
            ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownAggregateGroupingSetsRule());
        }
    }

    private Optional<OptExpression> ruleRewriteForShortCircuit(OptExpression tree, TaskContext rootTaskContext) {
        Boolean isShortCircuit = tree.getShortCircuit();

        if (isShortCircuit) {
            deriveLogicalProperty(tree);
            ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SHORT_CIRCUIT_SET);
            ruleRewriteOnlyOnce(tree, rootTaskContext, new MergeProjectWithChildRule());
            OptExpression result = tree.getInputs().get(0);
            result.setShortCircuit(true);
            return Optional.of(result);
        }
        return Optional.empty();
    }

    // for single scan node, to make sure we can rewrite
    private void viewBasedMvRuleRewrite(OptExpression tree, TaskContext rootTaskContext) {
        QueryMaterializationContext queryMaterializationContext = context.getQueryMaterializationContext();
        Preconditions.checkArgument(queryMaterializationContext != null);

        try (Timer ignored = Tracers.watchScope("MVViewRewrite")) {
            OptimizerTraceUtil.logMVRewriteRule("VIEW_BASED_MV_REWRITE", "try VIEW_BASED_MV_REWRITE");
            OptExpression treeWithView = queryMaterializationContext.getQueryOptPlanWithView();
            // should add a LogicalTreeAnchorOperator for rewrite
            treeWithView = OptExpression.create(new LogicalTreeAnchorOperator(), treeWithView);
            if (mvRewriteStrategy.enableMultiTableRewrite) {
                ruleRewriteIterative(treeWithView, rootTaskContext, RuleSetType.MULTI_TABLE_MV_REWRITE);
            }
            if (mvRewriteStrategy.enableSingleTableRewrite) {
                ruleRewriteIterative(treeWithView, rootTaskContext, RuleSetType.SINGLE_TABLE_MV_REWRITE);
            }

            List<Operator> leftViewScanOperators = Lists.newArrayList();
            MvUtils.collectViewScanOperator(treeWithView, leftViewScanOperators);
            List<LogicalViewScanOperator> origQueryViewScanOperators = queryMaterializationContext.getQueryViewScanOps();
            if (leftViewScanOperators.size() < origQueryViewScanOperators.size()) {
                // replace original tree plan
                tree.setChild(0, treeWithView.inputAt(0));
                deriveLogicalProperty(tree);

                // if there are view scan operator left, we should replace it back to original plans
                if (!leftViewScanOperators.isEmpty()) {
                    MvUtils.replaceLogicalViewScanOperator(tree);
                }
            }
            OptimizerTraceUtil.logMVRewriteRule("VIEW_BASED_MV_REWRITE", "original view scans size: {}, " +
                            "left view scans size: {}", origQueryViewScanOperators.size(), leftViewScanOperators.size());
        } catch (Exception e) {
            OptimizerTraceUtil.logMVRewriteRule("VIEW_BASED_MV_REWRITE",
                    "single table view based mv rule rewrite failed.", e);
        }
    }

    private OptExpression rewriteAndValidatePlan(
            OptExpression tree,
            TaskContext rootTaskContext) {
        OptExpression result = logicalRuleRewrite(tree, rootTaskContext);
        OptExpressionValidator validator = new OptExpressionValidator();
        validator.validate(result);
        // skip memo
        if (result.getShortCircuit()) {
            result = new OlapScanImplementationRule().transform(result, null).get(0);
            result.setShortCircuit(true);
        }
        return result;
    }

    private OptExpression pushDownAggregation(OptExpression tree, TaskContext rootTaskContext,
                                              ColumnRefSet requiredColumns) {
        boolean pushDistinctFlag = false;
        boolean pushAggFlag = false;
        if (context.getSessionVariable().isCboPushDownDistinctBelowWindow()) {
            // TODO(by satanson): in future, PushDownDistinctAggregateRule and PushDownAggregateRule should be
            //  fused one rule to tackle with all scenarios of agg push-down.
            PushDownDistinctAggregateRule rule = new PushDownDistinctAggregateRule(rootTaskContext);
            tree = rule.rewrite(tree, rootTaskContext);
            pushDistinctFlag = rule.getRewriter().hasRewrite();
        }

        if (context.getSessionVariable().getCboPushDownAggregateMode() != -1) {
            PushDownAggregateRule rule = new PushDownAggregateRule(rootTaskContext);
            rule.getRewriter().collectRewriteContext(tree);
            if (rule.getRewriter().isNeedRewrite()) {
                pushAggFlag = true;
                tree = rule.rewrite(tree, rootTaskContext);
            }
        }

        if (pushDistinctFlag || pushAggFlag) {
            deriveLogicalProperty(tree);
            rootTaskContext.setRequiredColumns(requiredColumns.clone());
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
        }

        return tree;
    }

    private void skewJoinOptimize(OptExpression tree, TaskContext rootTaskContext) {
        SkewJoinOptimizeRule rule = new SkewJoinOptimizeRule();
        if (context.getSessionVariable().isEnableStatsToOptimizeSkewJoin()) {
            // merge projects before calculate statistics
            ruleRewriteOnlyOnce(tree, rootTaskContext, new MergeTwoProjectRule());
            Utils.calculateStatistics(tree, rootTaskContext.getOptimizerContext());
        }
        if (ruleRewriteOnlyOnce(tree, rootTaskContext, rule)) {
            // skew join generate new join and on predicate, need to push down join on expression to child project again
            ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownJoinOnExpressionToChildProject());
        }
    }

    private OptExpression pruneSubfield(OptExpression tree, TaskContext rootTaskContext, ColumnRefSet requiredColumns) {
        if (!context.getSessionVariable().isCboPruneSubfield()) {
            return tree;
        }

        PushDownSubfieldRule pushDownRule = new PushDownSubfieldRule();
        tree = pushDownRule.rewrite(tree, rootTaskContext);

        if (pushDownRule.hasRewrite()) {
            rootTaskContext.setRequiredColumns(requiredColumns.clone());
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
        }
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PruneSubfieldRule());

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
        context.setInMemoPhase(true);
        OptExpression tree = memo.getRootGroup().extractLogicalTree();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        // add CboTablePruneRule
        if (Utils.countJoinNodeSize(tree, CboTablePruneRule.JOIN_TYPES) < 10 &&
                sessionVariable.isEnableCboTablePrune()) {
            context.getRuleSet().addCboTablePruneRule();
        }
        // Join reorder
        int innerCrossJoinNode = Utils.countJoinNodeSize(tree, JoinOperator.innerCrossJoinSet());
        if (!sessionVariable.isDisableJoinReorder() && innerCrossJoinNode < sessionVariable.getCboMaxReorderNode()) {
            if (innerCrossJoinNode > sessionVariable.getCboMaxReorderNodeUseExhaustive()) {
                CTEUtils.collectForceCteStatistics(memo, context);

                OptimizerTraceUtil.logOptExpression("before ReorderJoinRule:\n%s", tree);
                new ReorderJoinRule().transform(tree, context);
                OptimizerTraceUtil.logOptExpression("after ReorderJoinRule:\n%s", tree);

                context.getRuleSet().addJoinCommutativityWithoutInnerRule();
            } else {
                if (Utils.countJoinNodeSize(tree, JoinOperator.semiAntiJoinSet()) <
                        sessionVariable.getCboMaxReorderNodeUseExhaustive()) {
                    context.getRuleSet().getTransformRules().add(JoinLeftAsscomRule.INNER_JOIN_LEFT_ASSCOM_RULE);
                }
                context.getRuleSet().addJoinTransformationRules();
            }
        }

        if (!sessionVariable.isDisableJoinReorder() && sessionVariable.isEnableOuterJoinReorder()
                && Utils.capableOuterReorder(tree, sessionVariable.getCboReorderThresholdUseExhaustive())) {
            context.getRuleSet().addOuterJoinTransformationRules();
        }

        if (!sessionVariable.isMVPlanner()) {
            // add join implementRule
            String joinImplementationMode = connectContext.getSessionVariable().getJoinImplementationMode();
            if ("merge".equalsIgnoreCase(joinImplementationMode)) {
                context.getRuleSet().addMergeJoinImplementationRule();
            } else if ("hash".equalsIgnoreCase(joinImplementationMode)) {
                context.getRuleSet().addHashJoinImplementationRule();
            } else if ("nestloop".equalsIgnoreCase(joinImplementationMode)) {
                context.getRuleSet().addNestLoopJoinImplementationRule();
            } else {
                context.getRuleSet().addAutoJoinImplementationRule();
            }
        } else {
            context.getRuleSet().addRealtimeMVRules();
        }

        if (mvRewriteStrategy.enableMultiTableRewrite) {
            context.getRuleSet().addSingleTableMvRewriteRule();
            context.getRuleSet().addMultiTableMvRewriteRule();
        }

        context.getTaskScheduler().pushTask(new OptimizeGroupTask(rootTaskContext, memo.getRootGroup()));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private OptExpression physicalRuleRewrite(ConnectContext connectContext, TaskContext rootTaskContext, OptExpression result) {
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
        result = new PhysicalDistributionAggOptRule().rewrite(result, rootTaskContext);
        result = new AddDecodeNodeForDictStringRule().rewrite(result, rootTaskContext);
        result = new LowCardinalityRewriteRule().rewrite(result, rootTaskContext);
        // Put before ScalarOperatorsReuseRule
        result = new PruneSubfieldsForComplexType().rewrite(result, rootTaskContext);
        result = new InlineCteProjectPruneRule().rewrite(result, rootTaskContext);
        // This rule should be last
        result = new ScalarOperatorsReuseRule().rewrite(result, rootTaskContext);
        // Reorder predicates
        result = new PredicateReorderRule(rootTaskContext.getOptimizerContext().getSessionVariable()).rewrite(result,
                rootTaskContext);
        result = new ExtractAggregateColumn().rewrite(result, rootTaskContext);
        result = new JoinLocalShuffleRule().rewrite(result, rootTaskContext);

        // This must be put at last of the optimization. Because wrapping reused ColumnRefOperator with CloneOperator
        // too early will prevent it from certain optimizations that depend on the equivalence of the ColumnRefOperator.
        result = new CloneDuplicateColRefRule().rewrite(result, rootTaskContext);

        // set subfield expr copy flag
        if (rootTaskContext.getOptimizerContext().getSessionVariable().getEnableSubfieldNoCopy()) {
            result = new SubfieldExprNoCopyRule().rewrite(result, rootTaskContext);
        }

        result = new AddIndexOnlyPredicateRule().rewrite(result, rootTaskContext);
        result = new DataCachePopulateRewriteRule(connectContext).rewrite(result, rootTaskContext);

        result.setPlanCount(planCount);
        return result;
    }

    private OptExpression dynamicRewrite(ConnectContext connectContext, TaskContext rootTaskContext, OptExpression result) {
        // update the existRequiredDistribution value in optExpression. The next rules need it to determine
        // if we can change the distribution to adjust the plan because of skew data, bad statistics or something else.
        result = new MarkParentRequiredDistributionRule().rewrite(result, rootTaskContext);
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
        if (groupExpression == null) {
            String msg = "no executable plan for this sql. group: %s. required property: %s";
            throw new IllegalArgumentException(String.format(msg, rootGroup, requiredProperty));
        }
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
        expression.setStatistics(groupExpression.getGroup().getStatistics());
        expression.setCost(groupExpression.getCost(requiredProperty));
        expression.setOutputProperty(requiredProperty);

        // When build plan fragment, we need the output column of logical property
        expression.setLogicalProperty(rootGroup.getLogicalProperty());
        return expression;
    }

    private void collectAllLogicalOlapScanOperators(OptExpression tree, OptimizerContext optimizerContext) {
        List<LogicalOlapScanOperator> list = Lists.newArrayList();
        Utils.extractOperator(tree, list, op -> op instanceof LogicalOlapScanOperator);
        optimizerContext.setAllLogicalOlapScanOperators(Collections.unmodifiableList(list));
    }

    private void collectAllPhysicalOlapScanOperators(OptExpression tree, TaskContext rootTaskContext) {
        List<PhysicalOlapScanOperator> list = Lists.newArrayList();
        Utils.extractOperator(tree, list, op -> op instanceof PhysicalOlapScanOperator);
        rootTaskContext.setAllPhysicalOlapScanOperators(Collections.unmodifiableList(list));
    }

    private void ruleRewriteIterative(OptExpression tree, TaskContext rootTaskContext, RuleSetType ruleSetType) {
        if (optimizerConfig.isRuleSetTypeDisable(ruleSetType)) {
            return;
        }
        List<Rule> rules = rootTaskContext.getOptimizerContext().getRuleSet().getRewriteRulesByType(ruleSetType);
        if (optimizerConfig.isRuleBased()) {
            rules = rules.stream().filter(r -> !optimizerConfig.isRuleDisable(r.type())).collect(Collectors.toList());
        }
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
        if (optimizerConfig.isRuleBased()) {
            rules = rules.stream().filter(r -> !optimizerConfig.isRuleDisable(r.type())).collect(Collectors.toList());
        }
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, true));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private boolean ruleRewriteOnlyOnce(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        if (optimizerConfig.isRuleDisable(rule.type())) {
            return false;
        }
        List<Rule> rules = Collections.singletonList(rule);
        RewriteTreeTask rewriteTreeTask = new RewriteTreeTask(rootTaskContext, tree, rules, true);
        context.getTaskScheduler().pushTask(rewriteTreeTask);
        context.getTaskScheduler().executeTasks(rootTaskContext);
        return rewriteTreeTask.hasChange();
    }

    private void prepareMetaOnlyOnce(OptExpression tree, TaskContext rootTaskContext) {
        if (rootTaskContext.getOptimizerContext().getSessionVariable().enableParallelPrepareMetadata()) {
            context.getTaskScheduler().pushTask(new PrepareCollectMetaTask(rootTaskContext, tree));
            context.getTaskScheduler().executeTasks(rootTaskContext);
        }
    }

    private void ruleRewriteAtMostOnce(OptExpression tree, TaskContext rootTaskContext, RuleSetType ruleSetType) {
        if (optimizerConfig.isRuleSetTypeDisable(ruleSetType)) {
            return;
        }
        List<Rule> rules = rootTaskContext.getOptimizerContext().getRuleSet().getRewriteRulesByType(ruleSetType);
        if (optimizerConfig.isRuleBased()) {
            rules = rules.stream().filter(r -> !optimizerConfig.isRuleDisable(r.type())).collect(Collectors.toList());
        }
        context.getTaskScheduler().pushTask(new RewriteAtMostOnceTask(rootTaskContext, tree, rules));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteAtMostOnce(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        if (optimizerConfig.isRuleDisable(rule.type())) {
            return;
        }
        List<Rule> rules = Collections.singletonList(rule);
        context.getTaskScheduler().pushTask(new RewriteAtMostOnceTask(rootTaskContext, tree, rules));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteDownTop(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        if (optimizerConfig.isRuleDisable(rule.type())) {
            return;
        }
        List<Rule> rules = Collections.singletonList(rule);
        context.getTaskScheduler().pushTask(new RewriteDownTopTask(rootTaskContext, tree, rules));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }
}
