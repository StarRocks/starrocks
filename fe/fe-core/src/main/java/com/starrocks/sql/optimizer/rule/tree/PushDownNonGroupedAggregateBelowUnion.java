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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.tree.exprreuse.ScalarOperatorsReuse;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/*
 * For non-group-by two-phase split aggregate over UNION ALL, the original plan is like:
 *
 *      Agg(Global, merge finalize)
 *              |
 *         Distribution
 *              |
 *      Agg(Local, update serialize)
 *              |
 *          Union All
 *          /       \
 *      child1     child2
 *
 * The local aggregate above UNION ALL has to consume all raw rows from every union branch.
 * This rule pushes the local split aggregate into each UNION ALL branch, and changes the original
 * local aggregate into a merge-serialize aggregate.
 *
 * Optimized plan:
 *
 *      Agg(Global, merge finalize)
 *              |
 *         Distribution
 *              |
 *      Agg(Global, merge serialize)
 *              |
 *          Union All
 *          /       \
 *  Agg(Local)   Agg(Local)
 *      |            |
 *   child1        child2
 *
 * After rewrite, UNION ALL transfers aggregate intermediate states instead of raw input rows.
 */
public class PushDownNonGroupedAggregateBelowUnion implements TreeRewriteRule {
    private static final Set<String> NON_PUSH_DOWN_AGGREGATE_FUNCTIONS = Sets.newHashSet(
            FunctionSet.GROUP_CONCAT, FunctionSet.ARRAY_AGG,
            FunctionSet.ARRAY_AGG_DISTINCT, FunctionSet.ARRAY_UNIQUE_AGG,
            FunctionSet.MULTI_DISTINCT_COUNT, FunctionSet.MULTI_DISTINCT_SUM,
            FunctionSet.FUSED_MULTI_DISTINCT_COUNT, FunctionSet.FUSED_MULTI_DISTINCT_COUNT_SUM,
            FunctionSet.FUSED_MULTI_DISTINCT_COUNT_AVG, FunctionSet.FUSED_MULTI_DISTINCT_COUNT_SUM_AVG);

    public PushDownNonGroupedAggregateBelowUnion() {
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (!Config.push_down_non_grouped_aggregate_below_union) {
            return root;
        }

        ColumnRefFactory factory = taskContext.getOptimizerContext().getColumnRefFactory();
        return root.getOp().accept(new PushDownNonGroupedAggregateBelowUnionVisitor(factory), root, null);
    }

    private static class PushDownNonGroupedAggregateBelowUnionVisitor
            extends OptExpressionVisitor<OptExpression, Void> {
        private final ColumnRefFactory factory;

        PushDownNonGroupedAggregateBelowUnionVisitor(ColumnRefFactory factory) {
            this.factory = factory;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            for (int i = 0; i < optExpression.arity(); ++i) {
                optExpression.setChild(i,
                        optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), null));
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            visit(optExpression, context);
            Optional<RewriteContext> ctx = match(optExpression);
            if (ctx.isPresent()) {
                return rewrite(ctx.get());
            }
            return optExpression;
        }

        // ----------------------------------------------------------------------------------------
        // Match phase: identify the LOCAL split aggregate above a UNION ALL and validate every
        // push-down precondition. No new plan node is constructed here.
        // ----------------------------------------------------------------------------------------

        private Optional<RewriteContext> match(OptExpression aggExpr) {
            PhysicalHashAggregateOperator localAgg = aggExpr.getOp().cast();
            if (!isEligibleSplitAgg(aggExpr, localAgg, AggType.LOCAL)) {
                return Optional.empty();
            }

            OptExpression projectExpr = null;
            OptExpression child = aggExpr.inputAt(0);
            if (child.getOp().getOpType() == OperatorType.PHYSICAL_PROJECT) {
                projectExpr = child;
                child = child.inputAt(0);
            }

            OptExpression unionExpr = child;
            if (unionExpr.getOp().getOpType() != OperatorType.PHYSICAL_UNION) {
                return Optional.empty();
            }
            PhysicalUnionOperator union = unionExpr.getOp().cast();
            if (!union.isUnionAll() || union.getPredicate() != null || union.hasLimit()) {
                return Optional.empty();
            }
            return Optional.of(new RewriteContext(aggExpr, projectExpr, unionExpr));
        }

        private boolean isEligibleSplitAgg(OptExpression aggExpr, PhysicalHashAggregateOperator agg,
                                           AggType expectedType) {
            if (agg.getType() != expectedType || !agg.isSplit() || !agg.getGroupBys().isEmpty() ||
                    agg.getPredicate() != null || agg.hasLimit()) {
                return false;
            }
            return agg.getAggregations().values().stream().noneMatch(this::cannotPushDownAggFunction);
        }

        private boolean cannotPushDownAggFunction(CallOperator call) {
            return call.isDistinct() || call.isRemovedDistinct() ||
                    NON_PUSH_DOWN_AGGREGATE_FUNCTIONS.contains(call.getFnName());
        }

        // ----------------------------------------------------------------------------------------
        // Rewrite phase: build the pushed-down plan from a validated context. No eligibility check
        // is performed here.
        // ----------------------------------------------------------------------------------------

        private OptExpression rewrite(RewriteContext ctx) {
            PhysicalUnionOperator union = ctx.union();

            // 0. Build new union output columns from the original local aggregate outputs.
            List<ColumnRefOperator> unionOutputColumns = Lists.newArrayList();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : ctx.localAgg().getAggregations().entrySet()) {
                ColumnRefOperator output = entry.getKey();
                unionOutputColumns.add(new ColumnRefOperator(output.getId(), entry.getValue().getType(),
                        output.getName(), output.isNullable()));
            }

            // 1. Rewrite each branch of UNION ALL.
            List<List<ColumnRefOperator>> newChildOutputColumns = Lists.newArrayList();
            List<OptExpression> newUnionChildren = Lists.newArrayList();
            for (int i = 0; i < ctx.unionExpr().arity(); i++) {
                BranchRewriteResult branch = rewriteBranch(ctx, unionOutputColumns, i);
                newChildOutputColumns.add(branch.outputColumns());
                newUnionChildren.add(branch.expression());
            }

            // 2. Build a new UNION ALL operator.
            PhysicalUnionOperator newUnion = new PhysicalUnionOperator(unionOutputColumns, newChildOutputColumns,
                    true, union.getLimit(), union.getPredicate(), null,
                    union.isFromIcebergEqualityDeleteRewrite());
            OptExpression newUnionExpr = OptExpression.builder()
                    .with(ctx.unionExpr())
                    .setOp(newUnion)
                    .setInputs(newUnionChildren)
                    .setLogicalProperty(rewriteLogicalProperty(ctx.localAggExpr().getLogicalProperty(),
                            unionOutputColumns))
                    .setStatistics(ctx.localAggExpr().getStatistics())
                    .setCost(ctx.localAggExpr().getCost())
                    .build();

            // 3. Build a new aggregate operator for the merge serialize stage.
            PhysicalHashAggregateOperator mergeSerializeAgg =
                    buildMergeSerializeAggregate(ctx.localAgg(), unionOutputColumns);
            return OptExpression.builder()
                    .with(ctx.localAggExpr())
                    .setOp(mergeSerializeAgg)
                    .setInputs(Lists.newArrayList(newUnionExpr))
                    .setLogicalProperty(ctx.localAggExpr().getLogicalProperty())
                    .setStatistics(ctx.localAggExpr().getStatistics())
                    .setCost(ctx.localAggExpr().getCost())
                    .build();
        }

        private boolean canPushDownThroughExchange(OptExpression unionChild) {
            Operator op = unionChild.getOp();
            if (op.getOpType() != OperatorType.PHYSICAL_DISTRIBUTION || op.getPredicate() != null ||
                    op.hasLimit() || op.getProjection() != null || unionChild.arity() != 1) {
                return false;
            }
            PhysicalDistributionOperator exchange = (PhysicalDistributionOperator) op;
            return exchange.getDistributionSpec().getType() == DistributionSpec.DistributionType.ROUND_ROBIN;
        }

        private BranchRewriteResult rewriteBranch(RewriteContext ctx, List<ColumnRefOperator> unionOutputColumns,
                                                  int childIndex) {
            PhysicalHashAggregateOperator localAgg = ctx.localAgg();
            PhysicalUnionOperator union = ctx.union();
            OptExpression unionChild = ctx.unionExpr().inputAt(childIndex);
            Statistics statistics = ctx.localAggExpr().getStatistics();
            double cost = ctx.localAggExpr().getCost();

            // Map local aggregate inputs from UNION outputs to this child's outputs.
            // LocalAgg(union output) is rewritten to localAgg(child output).
            Map<ColumnRefOperator, ScalarOperator> baseRewriteMap = Maps.newHashMap();
            List<ColumnRefOperator> unionOutputs = union.getOutputColumnRefOp();
            List<ColumnRefOperator> childOutputs = union.getChildOutputColumns().get(childIndex);
            for (int i = 0; i < unionOutputs.size(); i++) {
                baseRewriteMap.put(unionOutputs.get(i), childOutputs.get(i));
            }

            // Push the aggregate through the exchange operator.
            OptExpression exchangeExpr = null;
            if (canPushDownThroughExchange(unionChild)) {
                exchangeExpr = unionChild;
                unionChild = unionChild.inputAt(0);
            }

            // Rewrite the aggregate input for this branch.
            AggInputRewriteResult inputRewrite = rewriteAggInput(ctx, union, unionChild, baseRewriteMap);

            // Rewrite aggregate calls and build new aggregate output columns.
            // sum(union_output) becomes new_sum := sum(child_output).
            ReplaceColumnRefRewriter aggRewriter = new ReplaceColumnRefRewriter(inputRewrite.rewriteMap());
            Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newLinkedHashMap();
            Map<ColumnRefOperator, ScalarOperator> localAggOutputRewriteMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : localAgg.getAggregations().entrySet()) {
                ColumnRefOperator oldOutput = entry.getKey();
                CallOperator newCall = (CallOperator) aggRewriter.rewrite(entry.getValue());
                ColumnRefOperator newOutput = factory.create(oldOutput.getName(), newCall.getType(),
                        oldOutput.isNullable());
                newAggregations.put(newOutput, newCall);
                localAggOutputRewriteMap.put(oldOutput, newOutput);
            }

            List<ColumnRefOperator> branchOutputColumns = Lists.newArrayList();
            unionOutputColumns.forEach(c ->
                    branchOutputColumns.add((ColumnRefOperator) localAggOutputRewriteMap.get(c)));

            // Build the new local aggregate operator for this UNION ALL branch.
            PhysicalHashAggregateOperator newLocalAgg = new PhysicalHashAggregateOperator(AggType.LOCAL,
                    Lists.newArrayList(), Lists.newArrayList(localAgg.getPartitionByColumns()), newAggregations,
                    true, localAgg.getLimit(), localAgg.getPredicate(), null);
            OptExpression newLocalAggExpr = OptExpression.builder()
                    .with(ctx.localAggExpr())
                    .setOp(newLocalAgg)
                    .setInputs(Lists.newArrayList(inputRewrite.input()))
                    .setLogicalProperty(rewriteLogicalProperty(ctx.localAggExpr().getLogicalProperty(),
                            branchOutputColumns))
                    .setStatistics(statistics)
                    .setCost(cost)
                    .build();
            if (exchangeExpr == null) {
                return new BranchRewriteResult(newLocalAggExpr, branchOutputColumns);
            }

            // Rebuild the exchange operator above the new local aggregate.
            PhysicalDistributionOperator oldExchange = exchangeExpr.getOp().cast();
            PhysicalDistributionOperator newExchange =
                    new PhysicalDistributionOperator(oldExchange.getDistributionSpec());
            OptExpression newExchangeExpr = OptExpression.builder()
                    .with(exchangeExpr)
                    .setOp(newExchange)
                    .setInputs(Lists.newArrayList(newLocalAggExpr))
                    .setLogicalProperty(rewriteLogicalProperty(exchangeExpr.getLogicalProperty(), branchOutputColumns))
                    .setStatistics(statistics)
                    .setCost(cost)
                    .build();
            return new BranchRewriteResult(newExchangeExpr, branchOutputColumns);
        }

        private AggInputRewriteResult rewriteAggInput(RewriteContext ctx, PhysicalUnionOperator union,
                                                      OptExpression unionChild,
                                                      Map<ColumnRefOperator, ScalarOperator> baseRewriteMap) {
            if (ctx.projectExpr() != null) {
                Map<ColumnRefOperator, ScalarOperator> projectInputRewriteMap = Maps.newHashMap(baseRewriteMap);
                if (union.getProjection() != null) {
                    appendProjectionRewriteMap(union.getProjection(), projectInputRewriteMap);
                }
                PhysicalProjectOperator project = ctx.projectExpr().getOp().cast();
                ProjectRewriteResult result = rewriteProject(project.getColumnRefMap(),
                        project.getCommonSubOperatorMap(), ctx.projectExpr(), projectInputRewriteMap, unionChild);
                Map<ColumnRefOperator, ScalarOperator> aggRewriteMap = Maps.newHashMap(projectInputRewriteMap);
                aggRewriteMap.putAll(result.outputRewriteMap());
                return new AggInputRewriteResult(result.expression(), aggRewriteMap);
            }

            if (union.getProjection() != null) {
                ProjectRewriteResult result = rewriteProject(union.getProjection().getColumnRefMap(),
                        union.getProjection().getCommonSubOperatorMap(), unionChild, baseRewriteMap, unionChild);
                Map<ColumnRefOperator, ScalarOperator> aggRewriteMap = Maps.newHashMap(baseRewriteMap);
                aggRewriteMap.putAll(result.outputRewriteMap());
                return new AggInputRewriteResult(result.expression(), aggRewriteMap);
            }

            return new AggInputRewriteResult(unionChild, Maps.newHashMap(baseRewriteMap));
        }

        private void appendProjectionRewriteMap(Projection projection,
                                                Map<ColumnRefOperator, ScalarOperator> rewriteMap) {
            ReplaceColumnRefRewriter projectionRewriter = new ReplaceColumnRefRewriter(rewriteMap);
            Set<ColumnRefOperator> resolved = Sets.newHashSet();
            for (ColumnRefOperator key : projection.getCommonSubOperatorMap().keySet()) {
                inlineCommonSubOperator(key, projection.getCommonSubOperatorMap(), rewriteMap,
                        projectionRewriter, resolved);
            }
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                rewriteMap.put(entry.getKey(), projectionRewriter.rewrite(entry.getValue()));
            }
        }

        private ProjectRewriteResult rewriteProject(Map<ColumnRefOperator, ScalarOperator> projectMap,
                                                    Map<ColumnRefOperator, ScalarOperator> commonProjectMap,
                                                    OptExpression template,
                                                    Map<ColumnRefOperator, ScalarOperator> childRewriteMap,
                                                    OptExpression child) {
            // Prepare column mappings for rewriting project expressions on this branch.
            Map<ColumnRefOperator, ScalarOperator> projectRewriteMap = Maps.newHashMap(childRewriteMap);
            ReplaceColumnRefRewriter childRewriter = new ReplaceColumnRefRewriter(projectRewriteMap);
            Map<ColumnRefOperator, ScalarOperator> projectOutputRewriteMap = Maps.newHashMap();

            // Inline common sub-operators in dependency order.
            Set<ColumnRefOperator> resolved = Sets.newHashSet();
            for (ColumnRefOperator key : commonProjectMap.keySet()) {
                inlineCommonSubOperator(key, commonProjectMap, projectRewriteMap, childRewriter, resolved);
            }

            // Rewrite each project output expression and create a fresh output column for it.
            Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newLinkedHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectMap.entrySet()) {
                ColumnRefOperator oldOutput = entry.getKey();
                ScalarOperator newExpression = childRewriter.rewrite(entry.getValue());
                ColumnRefOperator newOutput = factory.create(oldOutput.getName(), newExpression.getType(),
                        oldOutput.isNullable());
                newProjectMap.put(newOutput, newExpression);
                projectOutputRewriteMap.put(oldOutput, newOutput);
            }

            // Build the new project operator above this branch.
            Projection newProjection =
                    ScalarOperatorsReuse.rewriteProjectionOrLambdaExpr(new Projection(newProjectMap), factory);
            PhysicalProjectOperator newProject = new PhysicalProjectOperator(newProjection.getColumnRefMap(),
                    newProjection.getCommonSubOperatorMap());
            OptExpression newProjectExpr = OptExpression.builder()
                    .with(template)
                    .setOp(newProject)
                    .setInputs(Lists.newArrayList(child))
                    .setLogicalProperty(rewriteLogicalProperty(template.getLogicalProperty(),
                            newProjection.getColumnRefMap().keySet()))
                    .setStatistics(template.getStatistics())
                    .setCost(template.getCost())
                    .build();
            return new ProjectRewriteResult(newProjectExpr, projectOutputRewriteMap);
        }

        private void inlineCommonSubOperator(ColumnRefOperator key,
                                             Map<ColumnRefOperator, ScalarOperator> commonProjectMap,
                                             Map<ColumnRefOperator, ScalarOperator> projectRewriteMap,
                                             ReplaceColumnRefRewriter childRewriter,
                                             Set<ColumnRefOperator> resolved) {
            if (!resolved.add(key)) {
                return;
            }
            ScalarOperator expr = commonProjectMap.get(key);
            ColumnRefSet used = expr.getUsedColumns();
            for (ColumnRefOperator dep : commonProjectMap.keySet()) {
                if (!dep.equals(key) && used.contains(dep)) {
                    inlineCommonSubOperator(dep, commonProjectMap, projectRewriteMap, childRewriter, resolved);
                }
            }
            projectRewriteMap.put(key, childRewriter.rewrite(expr));
        }

        private PhysicalHashAggregateOperator buildMergeSerializeAggregate(
                PhysicalHashAggregateOperator localAgg, List<ColumnRefOperator> inputColumns) {
            Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newLinkedHashMap();
            int index = 0;
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : localAgg.getAggregations().entrySet()) {
                CallOperator localCall = entry.getValue();
                List<ScalarOperator> localArgs = localCall.getArguments();
                List<ScalarOperator> arguments = Lists.newArrayList(inputColumns.get(index++));
                localArgs.stream().skip(1).filter(ScalarOperator::isConstant).forEach(arguments::add);
                newAggregations.put(entry.getKey(), new CallOperator(localCall.getFnName(), localCall.getType(),
                        arguments, localCall.getFunction(), localCall.isDistinct(), localCall.isRemovedDistinct()));
            }
            return new PhysicalHashAggregateOperator(AggType.DISTINCT_GLOBAL, Lists.newArrayList(),
                    Lists.newArrayList(localAgg.getPartitionByColumns()), newAggregations, true,
                    localAgg.getLimit(), localAgg.getPredicate(), null);
        }

        private LogicalProperty rewriteLogicalProperty(LogicalProperty property,
                                                       Collection<ColumnRefOperator> outputColumns) {
            LogicalProperty newProperty = new LogicalProperty(property);
            newProperty.setOutputColumns(new ColumnRefSet(outputColumns));
            return newProperty;
        }
    }

    private record RewriteContext(OptExpression localAggExpr, OptExpression projectExpr, OptExpression unionExpr) {
        PhysicalHashAggregateOperator localAgg() {
            return localAggExpr.getOp().cast();
        }

        PhysicalUnionOperator union() {
            return unionExpr.getOp().cast();
        }
    }

    private record BranchRewriteResult(OptExpression expression, List<ColumnRefOperator> outputColumns) {
    }

    private record AggInputRewriteResult(OptExpression input, Map<ColumnRefOperator, ScalarOperator> rewriteMap) {
    }

    private record ProjectRewriteResult(OptExpression expression,
                                        Map<ColumnRefOperator, ScalarOperator> outputRewriteMap) {
    }
}
