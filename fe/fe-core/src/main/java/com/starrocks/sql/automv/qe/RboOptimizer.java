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

package com.starrocks.sql.automv.qe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LogUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pattern.PlanPiecePattern;
import com.starrocks.sql.automv.pattern.PlanPiecePatterns;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceBuilder;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.join.JoinReorderFactory;
import com.starrocks.sql.optimizer.rule.join.ReorderJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.CombinationRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoFiltersRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnClauseRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateAggRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateCTEAnchor;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateCTEConsumeRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateExceptRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateIntersectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateRepeatRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateTableFunctionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateToExternalTableScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateUnionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateWindowRule;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;
import com.starrocks.sql.parser.SqlParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

// AutoMV recommends MVs according to LogicalPlan instead of AST, so we need
// convert textual SQL into LogicalPlan via this Optimizer, the present optimizer.Optimizer
// does not satisfy the AutoMV's requirement, since:
// 1. Only some RBO rule is required, PushDownAgg rule is not required;
// 2. CTE must be inlined
// 3. JoinReorderRule must used to eliminate CROSS JOIN(especially in TPCH benchmark) which
//    damage performance of multi-column cardinality estimation.
// 4. {Project,Filter}Operator must be fold into its' child operator.
public class RboOptimizer {
    // remove CastToEmptyRule and PruneTrueFilterRule
    private static final Rule PUSH_DOWN_PREDICATE_RULES =
            new CombinationRule(RuleType.GP_PUSH_DOWN_PREDICATE, ImmutableList.of(
                    new PushDownPredicateCTEAnchor(),
                    new PushDownPredicateScanRule(),
                    new PushDownPredicateAggRule(),
                    new PushDownPredicateWindowRule(),
                    new PushDownPredicateJoinRule(),
                    new PushDownJoinOnClauseRule(),
                    new PushDownPredicateProjectRule(),
                    new PushDownPredicateUnionRule(),
                    new PushDownPredicateExceptRule(),
                    new PushDownPredicateIntersectRule(),
                    new PushDownPredicateTableFunctionRule(),
                    new PushDownPredicateRepeatRule(),
                    new PushDownPredicateToExternalTableScanRule(),
                    new MergeTwoFiltersRule(),
                    new PushDownPredicateCTEConsumeRule()
            ));
    private final OptimizerOptions optimizerOptions;
    private final OptimizerContext optimizerContext;
    private final TaskContext taskContext;
    private OptExpression tree;

    public RboOptimizer(LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory, ConnectContext connectContext) {
        optimizerOptions = OptimizerOptions.newRuleBaseOpt();
        optimizerContext = OptimizerFactory.initContext(connectContext, columnRefFactory, optimizerOptions);
        optimizerContext.setTaskScheduler(new TaskScheduler());
        // CTE must be inlined to extract sub-queries
        optimizerContext.getCteContext().setEnableCTE(false);
        taskContext =
                new TaskContext(optimizerContext, new PhysicalPropertySet(),
                        new ColumnRefSet(logicalPlan.getOutputColumn()),
                        Double.MAX_VALUE);
        tree = logicalPlan.getRoot();
        if (!tree.getOp().getOpType().equals(OperatorType.LOGICAL)) {
            tree = OptExpression.create(new LogicalTreeAnchorOperator(), tree);
        }
    }

    private static void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        ExpressionContext context = new ExpressionContext(root);
        context.deriveLogicalProperty();
        root.setLogicalProperty(context.getRootProperty());
    }

    private static RboOptimizer getOptimizer(QueryStatement stmt, ConnectContext connectContext) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        QueryRelation query = stmt.getQueryRelation();
        boolean inlineView = connectContext.getSessionVariable().isAutoMVEnableViewInline();
        TransformerContext transformerContext =
                new TransformerContext(columnRefFactory, connectContext, inlineView, null);
        RelationTransformer relationTransformer = new RelationTransformer(transformerContext);
        LogicalPlan logicalPlan = relationTransformer.transformWithSelectLimit(query);
        RboOptimizer optimizer =
                new RboOptimizer(logicalPlan, columnRefFactory, connectContext);
        optimizer.optimize();
        return optimizer;
    }

    public static RboOptimizer getOptimizer(String sql, ConnectContext connectContext) {
        QueryStatementPlus stmtPlus = RboOptimizer.getQueryStatement(connectContext, sql);
        return getOptimizer(stmtPlus.getQueryStatement(), connectContext);
    }

    public static String getLogicalPlan(String query, ConnectContext ctx) {
        OptExpression plan =
                RboOptimizer.getLogicalPlan(RboOptimizer.getQueryStatement(ctx, query).getQueryStatement(), ctx);
        return LogicalPlanPrinter.print(plan, true);
    }

    public static OptExpression getLogicalPlan(QueryStatement queryStmt, ConnectContext connectContext) {
        return getOptimizer(queryStmt, connectContext).getPlan();
    }

    public static List<OptExpression> getSubPlans(QueryStatement queryStmt, ConnectContext connectContext,
                                                  PlanPiecePattern pattern) {
        return PlanPiecePattern.extract(getLogicalPlan(queryStmt, connectContext), pattern);
    }

    public static Optional<OptExpression> getEntirePlan(QueryStatement queryStmt, ConnectContext connectContext,
                                                        PlanPiecePattern pattern) {
        return PlanPiecePattern.extractEntire(getLogicalPlan(queryStmt, connectContext), pattern);
    }

    public static StatementBase parseAndAnalyze(ConnectContext connectContext, String query) {
        query = LogUtil.removeLineSeparator(query);
        List<StatementBase> statements = SqlParser.parse(query, connectContext.getSessionVariable());
        SessionVariable oldSessionVariable = connectContext.getSessionVariable();
        StatementBase statementBase = statements.get(0);
        try {
            // update session variable by adding optional hints.
            Optional<List<HintNode>> optHints = Util.downcast(statementBase, QueryStatement.class)
                    .flatMap(queryStmt -> Util.downcast(queryStmt.getQueryRelation(), SelectRelation.class))
                    .flatMap(selectRelation -> Optional.ofNullable(selectRelation.getSelectList().getHintNodes()));

            if (optHints.isPresent()) {
                Map<String, String> variables = optHints.get().stream()
                        .flatMap(hint -> hint.getValue().entrySet().stream())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                SessionVariable sessionVariable = (SessionVariable) oldSessionVariable.clone();
                for (Map.Entry<String, String> var : variables.entrySet()) {
                    GlobalStateMgr.getCurrentState().getVariableMgr().setSystemVariable(sessionVariable,
                            new SystemVariable(var.getKey(), new StringLiteral(var.getValue())), true);
                }
                connectContext.setSessionVariable(sessionVariable);
            }
            Analyzer.analyze(statementBase, connectContext);
            return statementBase;
        } catch (DdlException e) {
            throw new RuntimeException(e);
        } finally {
            connectContext.setSessionVariable(oldSessionVariable);
        }
    }

    public static QueryStatementPlus collectFQTables(QueryStatement queryStmt, ConnectContext connectContext) {
        return CollectAstVisitor.collectFQTables(queryStmt, connectContext);
    }

    private static Function<OptExpression, PlanPiece> subPlanToPiece(AutoMVOptions options,
                                                                     String name,
                                                                     Map<String, FQTable> fqTableMap) {
        return subPlan -> {
            ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
            Optional<AggregatePiece> optPlanPiece =
                    PlanPieceBuilder.createPlanPiece(name, subPlan, idConverter, fqTableMap).cast(AggregatePiece.class);
            Preconditions.checkArgument(optPlanPiece.isPresent());
            AggregatePolicy policy = AggregatePolicies.defaultPolicies(options, null);
            return Objects.requireNonNull(policy.convert(optPlanPiece.get()).orElse(null));
        };
    }

    private static Function<OptExpression, PlanPiece> subPlanTo11MVPiece(AutoMVOptions options,
                                                                         String name,
                                                                         Map<String, FQTable> fqTableMap) {
        return subPlan -> {
            ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
            return PlanPieceBuilder.createPlanPiece(name, subPlan, idConverter, fqTableMap);
        };
    }

    public static Optional<PlanPiece> getPlanPieceFromLegacyMV(MaterializedViewPlus mvPlus, ConnectContext context) {
        String createMvSql = mvPlus.getCreateMaterializedViewSql();
        StatementBase stmt = RboOptimizer.parseAndAnalyze(context, createMvSql);
        Preconditions.checkArgument(stmt instanceof CreateMaterializedViewStatement);
        CreateMaterializedViewStatement createMvStmt = (CreateMaterializedViewStatement) stmt;
        QueryStatement queryStmt = createMvStmt.getQueryStatement();
        QueryStatementPlus queryStmtPlus = RboOptimizer.collectFQTables(queryStmt, context);
        Map<String, FQTable> fqTableMap = queryStmtPlus.getFqTableMap();

        Optional<OptExpression> optEntirePlan =
                RboOptimizer.getEntirePlan(queryStmt, context, PlanPiecePatterns.getSPJG());
        if (!optEntirePlan.isPresent()) {
            return Optional.empty();
        }
        OptExpression entirePlan = optEntirePlan.get();
        ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
        PlanPiece planPiece =
                PlanPieceBuilder.createPlanPiece(mvPlus.getMv().getName(), entirePlan, idConverter, fqTableMap);
        return Optional.of(planPiece);
    }

    @VisibleForTesting
    public static List<PlanPiece> getPlanPieces(String sql, ConnectContext ctx) {
        return getPlanPieces("Q", sql, ctx);
    }

    public static List<PlanPiece> getPlanPieces(String name, String sql, ConnectContext ctx) {
        QueryStatementPlus stmt = RboOptimizer.getQueryStatement(ctx, sql);
        QueryStatement queryStmt = stmt.getQueryStatement();
        Map<String, FQTable> fqTableMap = stmt.getFqTableMap();
        AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), ctx.getSessionVariable());
        Function<OptExpression, PlanPiece> subPlanToPieceConverter = subPlanToPiece(options, name, fqTableMap);
        return RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePatterns.getSPJG())
                .stream()
                .map(subPlanToPieceConverter)
                .collect(Collectors.toList());
    }

    public static List<PlanPiece> get11MVPlanPieces(String name, String sql, ConnectContext ctx) {
        QueryStatementPlus stmt = RboOptimizer.getQueryStatement(ctx, sql);
        QueryStatement queryStmt = stmt.getQueryStatement();
        Map<String, FQTable> fqTableMap = stmt.getFqTableMap();
        AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), ctx.getSessionVariable());
        Function<OptExpression, PlanPiece> subPlanToPieceConverter = subPlanTo11MVPiece(options, name, fqTableMap);
        return RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePatterns.get11MV())
                .stream()
                .map(subPlanToPieceConverter)
                .collect(Collectors.toList());
    }

    public static QueryStatementPlus getQueryStatement(ConnectContext connectContext, String query) {
        StatementBase stmt = parseAndAnalyze(connectContext, query);
        Preconditions.checkArgument(stmt instanceof QueryStatement);
        return collectFQTables((QueryStatement) stmt, connectContext);
    }

    public static Pair<Map<String, FQTable>, List<OptExpression>> getSubPlans(String query,
                                                                              ConnectContext connectContext,
                                                                              PlanPiecePattern pattern) {
        QueryStatementPlus queryStmt = getQueryStatement(connectContext, query);
        List<OptExpression> subPlans = getSubPlans(queryStmt.getQueryStatement(), connectContext, pattern);
        return Pair.create(queryStmt.getFqTableMap(), subPlans);
    }

    public OptimizerOptions getOptimizerOptions() {
        return optimizerOptions;
    }

    public OptimizerContext getOptimizerContext() {
        return optimizerContext;
    }

    public TaskContext getTaskContext() {
        return taskContext;
    }

    public RboOptimizer applyRules(Rule rules) {
        optimizerContext.getTaskScheduler()
                .pushTask(new RewriteTreeTask(taskContext, tree, List.of(rules), false));
        optimizerContext.getTaskScheduler().executeTasks(taskContext);
        deriveLogicalProperty(tree);
        return this;
    }

    public RboOptimizer applyRulesOnlyOnce(Rule rules) {
        optimizerContext.getTaskScheduler()
                .pushTask(new RewriteTreeTask(taskContext, tree, List.of(rules), true));
        optimizerContext.getTaskScheduler().executeTasks(taskContext);
        deriveLogicalProperty(tree);
        return this;
    }

    public RboOptimizer applyRules(Rule... rules) {
        for (Rule rule : rules) {
            optimizerContext.getTaskScheduler()
                    .pushTask(new RewriteTreeTask(taskContext, tree, Collections.singletonList(rule), false));
            optimizerContext.getTaskScheduler().executeTasks(taskContext);
            deriveLogicalProperty(tree);
        }
        return this;
    }

    public RboOptimizer applyRulesOnlyOnce(Rule... rules) {
        for (Rule rule : rules) {
            optimizerContext.getTaskScheduler()
                    .pushTask(new RewriteTreeTask(taskContext, tree, Collections.singletonList(rule), true));
            optimizerContext.getTaskScheduler().executeTasks(taskContext);
            deriveLogicalProperty(tree);
        }
        return this;
    }

    public OptExpression optimize() {

        applyRules(RuleSet.INLINE_CTE_RULES);
        applyRules(RuleSet.PUSH_DOWN_SUBQUERY_RULES);
        applyRules(RuleSet.SUBQUERY_EXTRACT_CORRELATION_PREDICATE_RULES);
        applyRules(RuleSet.SUBQUERY_REWRITE_TO_WINDOW_RULES);
        applyRules(RuleSet.SUBQUERY_REWRITE_TO_JOIN_RULES);
        applyRules(PUSH_DOWN_PREDICATE_RULES);
        applyRulesOnlyOnce(RuleSet.PRUNE_COLUMNS_RULES);
        applyRules(new MergeTwoProjectRule());
        applyRules(new MergeProjectWithChildRule());
        deriveLogicalProperty(tree);

        boolean hasCrossJoin = Util.getStream(tree)
                .filter(op -> op.getOpType().equals(OperatorType.LOGICAL_JOIN))
                .anyMatch(op -> ((LogicalJoinOperator) op).getJoinType().isCrossJoin());
        if (hasCrossJoin) {
            // Frankly speaking, the default ReorderJoinRule depends on StatisticsCalculator
            // and StatisticsCalculator depends on PartitionPruneRule, however PartitionPruneRule
            // would mutate LogicalScanOperator.predicates and even eliminate partition predicate
            // it is unexpected behavior in AutoMV's predicate hoisting mechanism, so we never
            // invoke PartitionPruneRule, instead we introduce DummyStatisticsCalculator to generate
            // ColumnStatistic.unknown() for each column when ReorderJoinRule require the column
            // statistics.
            tree = new ReorderJoinRule().rewrite(tree, JoinReorderFactory.createJoinReorderDummyStatisticsFactory(),
                    optimizerContext);
            deriveLogicalProperty(tree);
        }
        return getPlan();
    }

    public OptExpression getPlan() {
        return tree;
    }
}
