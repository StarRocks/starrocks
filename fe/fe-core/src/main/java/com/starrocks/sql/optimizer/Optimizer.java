// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.Explain;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
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
import com.starrocks.sql.optimizer.rule.transformation.ReorderIntersectRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteGroupingSetsByCTERule;
import com.starrocks.sql.optimizer.rule.transformation.SemiReorderRule;
import com.starrocks.sql.optimizer.rule.transformation.materialize.MaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialize.RewriteUtils;
import com.starrocks.sql.optimizer.rule.tree.AddDecodeNodeForDictStringRule;
import com.starrocks.sql.optimizer.rule.tree.ExchangeSortToMergeRule;
import com.starrocks.sql.optimizer.rule.tree.PreAggregateTurnOnRule;
import com.starrocks.sql.optimizer.rule.tree.PredicateReorderRule;
import com.starrocks.sql.optimizer.rule.tree.PruneAggregateNodeRule;
import com.starrocks.sql.optimizer.rule.tree.PruneShuffleColumnRule;
import com.starrocks.sql.optimizer.rule.tree.ScalarOperatorsReuseRule;
import com.starrocks.sql.optimizer.task.OptimizeGroupTask;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.ParsingException;
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

    public OptimizerContext getContext() {
        return context;
    }

    public OptExpression optimizeByRule(ConnectContext connectContext,
                                        OptExpression logicOperatorTree,
                                        PhysicalPropertySet requiredProperty,
                                        ColumnRefSet requiredColumns,
                                        ColumnRefFactory columnRefFactory) {
        // Phase 1: none
        OptimizerTraceUtil.logOptExpression(connectContext, "origin logicOperatorTree:\n%s", logicOperatorTree);
        // Phase 2: rewrite based on memo and group
        Memo memo = new Memo();

        context = new OptimizerContext(memo, columnRefFactory, connectContext);
        context.setTraceInfo(new OptimizerTraceInfo(connectContext.getQueryId()));
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);
        logicOperatorTree = logicalRuleRewrite(logicOperatorTree, rootTaskContext);
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
    public OptExpression optimize(ConnectContext connectContext,
                                  OptExpression logicOperatorTree,
                                  PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns,
                                  ColumnRefFactory columnRefFactory) {
        // Phase 1: none
        OptimizerTraceUtil.logOptExpression(connectContext, "origin logicOperatorTree:\n%s", logicOperatorTree);
        // Phase 2: rewrite based on memo and group
        Memo memo = new Memo();

        context = new OptimizerContext(memo, columnRefFactory, connectContext);
        context.setTraceInfo(new OptimizerTraceInfo(connectContext.getQueryId()));
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);

        // register materialized views
        // TODO(hkp): add session variables and config to control
        if (Config.enable_experimental_mv) {
            try {
                registerMaterializedViews(logicOperatorTree, context);
            } catch (AnalysisException e) {
                e.printStackTrace();
            }
        }

        logicOperatorTree = logicalRuleRewrite(logicOperatorTree, rootTaskContext);

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
        memoOptimize(connectContext, memo, rootTaskContext);

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

        OptExpression finalPlan = physicalRuleRewrite(rootTaskContext, result);
        OptimizerTraceUtil.logOptExpression(connectContext, "final plan after physical rewrite:\n%s", finalPlan);
        OptimizerTraceUtil.log(connectContext, context.getTraceInfo());
        return finalPlan;
    }

    private OptExpression logicalRuleRewrite(OptExpression tree, TaskContext rootTaskContext) {
        tree = OptExpression.create(new LogicalTreeAnchorOperator(), tree);
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

        // save required columns
        ColumnRefSet requiredColumns = rootTaskContext.getRequiredColumns().clone();

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
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        //Limit push must be after the column prune,
        //otherwise the Node containing limit may be prune
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.MERGE_LIMIT);
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoAggRule());
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

        CTEUtils.collectCteOperators(tree, context);
        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.INLINE_CTE);
            CTEUtils.collectCteOperators(tree, context);
        }

        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteIterative(tree, rootTaskContext, new MergeProjectWithChildRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new GroupByCountDistinctRewriteRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new ReorderIntersectRule());
        ruleRewriteIterative(tree, rootTaskContext, new RemoveAggregationFromAggTable());

        return tree.getInputs().get(0);
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

        context.getTaskScheduler().pushTask(new OptimizeGroupTask(rootTaskContext, memo.getRootGroup()));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private OptExpression physicalRuleRewrite(TaskContext rootTaskContext, OptExpression result) {
        Preconditions.checkState(result.getOp().isPhysical());

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
        List<Rule> rules = rootTaskContext.getOptimizerContext().getRuleSet().getRewriteRulesByType(ruleSetType);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, false));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteIterative(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        List<Rule> rules = Collections.singletonList(rule);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, false));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteOnlyOnce(OptExpression tree, TaskContext rootTaskContext, RuleSetType ruleSetType) {
        List<Rule> rules = rootTaskContext.getOptimizerContext().getRuleSet().getRewriteRulesByType(ruleSetType);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, true));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteOnlyOnce(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        List<Rule> rules = Collections.singletonList(rule);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, true));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    // TODO(hkp): put it in right place
    private void registerMaterializedViews(OptExpression logicOperatorTree, OptimizerContext context)
            throws AnalysisException {
        List<Table> tables = RewriteUtils.getAllTables(logicOperatorTree);
        Set<Long[]> relatedMvs = Sets.newHashSet();
        for (Table table : tables) {
            Set<Long[]> mvs = table.getRelatedMaterializedViews();
            if (mvs != null) {
                relatedMvs.addAll(mvs);
            }
        }
        for (Long[] mvIds : relatedMvs) {
            Preconditions.checkState(mvIds.length == 2);
            long dbId = mvIds[0];
            Database db = context.getCatalog().getDb(dbId);
            if (db == null) {
                continue;
            }
            long mvId = mvIds[1];
            Table table = db.getTable(mvId);
            if (table == null) {
                continue;
            }
            Preconditions.checkState(table instanceof MaterializedView);
            MaterializedView mv = (MaterializedView) table;
            MaterializationContext materializationContext = mv.getMaterializationContext();
            if (materializationContext != null) {
                context.addCandidateMvs(materializationContext);
                continue;
            }

            // 1. build mv query logical plan
            String mvSql = mv.getViewDefineSql();
            StatementBase mvStmt;
            try {
                mvStmt = com.starrocks.sql.parser.SqlParser.parseSingleSql(mvSql, context.getSessionVariable());
            } catch (ParsingException parsingException) {
                throw new AnalysisException(parsingException.getMessage());
            }
            Preconditions.checkState(mvStmt instanceof QueryStatement);
            ConnectContext connectContext = ConnectContext.get();
            Analyzer.analyze(mvStmt, connectContext);
            QueryRelation query = ((QueryStatement) mvStmt).getQueryRelation();
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan =
                    new RelationTransformer(columnRefFactory, connectContext).transformWithSelectLimit(query);
            Optimizer optimizer = new Optimizer();
            OptExpression optimizedPlan = optimizer.optimizeByRule(
                    connectContext,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);

            if (!RewriteUtils.isLogicalSPJG(optimizedPlan)) {
                continue;
            }

            // check up-to-date

            // 2. create scan mv operator
            TableName tableName = new TableName(db.getFullName(), mv.getName());
            TableRelation tableRelation = new TableRelation(tableName);
            LogicalPlan mvQueryPlan =
                    new RelationTransformer(columnRefFactory, connectContext).transformWithSelectLimit(tableRelation);
            materializationContext = new MaterializationContext(mv,
                    mvQueryPlan.getRoot().getOp(), optimizedPlan);
            mv.setMaterializationContext(materializationContext);
            context.addCandidateMvs(materializationContext);
        }
    }
}
