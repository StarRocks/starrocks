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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.RewriteEquivalent;
import com.starrocks.sql.optimizer.rule.tree.pdagg.AggColumnRefRemapping;
import com.starrocks.sql.optimizer.rule.tree.pdagg.AggRewriteInfo;
import com.starrocks.sql.optimizer.rule.tree.pdagg.AggregatePushDownContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.AggregateFunctionRollupUtils.REWRITE_ROLLUP_FUNCTION_MAP;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.AggregateFunctionRollupUtils.genRollupProject;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.AggregateFunctionRollupUtils.getRollupFunctionName;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getQuerySplitPredicate;

/**
 * A mv rewriter that supports to push down aggregate functions below join operator and rewrite the query by mv transparently.
 */
public class AggregatedMaterializedViewPushDownRewriter extends MaterializedViewRewriter {
    private final MvRewriteContext mvRewriteContext;
    private final ColumnRefFactory queryColumnRefFactory;
    private final OptimizerContext optimizerContext;
    private final Rule rule;
    private final PreVisitor preVisitor = new PreVisitor();
    private final PostVisitor postVisitor = new PostVisitor();

    public AggregatedMaterializedViewPushDownRewriter(MvRewriteContext mvRewriteContext,
                                                      OptimizerContext optimizerContext,
                                                      Rule rule) {
        super(mvRewriteContext);

        this.mvRewriteContext = mvRewriteContext;
        this.queryColumnRefFactory = mvRewriteContext.getMaterializationContext().getQueryRefFactory();
        this.optimizerContext = optimizerContext;
        this.rule = rule;
    }

    @Override
    public OptExpression doRewrite(MvRewriteContext mvContext) {
        OptExpression input = mvContext.getQueryExpression();

        // try push down
        OptExpression inputDuplicator = duplicateOptExpression(mvContext, input);
        AggRewriteInfo rewriteInfo = process(inputDuplicator, AggregatePushDownContext.EMPTY);
        if (rewriteInfo.hasRewritten()) {
            Optional<OptExpression> res = rewriteInfo.getOp();
            logMVRewrite(mvContext, "AggregateJoin pushdown rewrite success");
            OptExpression result = res.get();
            setOptScanOpsHavePushDown(result);
            return result;
        } else {
            logMVRewrite(mvContext, "AggregateJoin pushdown rewrite failed");
            setOpHasPushDown(input);
            return null;
        }
    }

    /**
     * Since the aggregate pushdown rewriter may break original opt expression's structure, duplicate it first.
     */
    private static OptExpression duplicateOptExpression(MvRewriteContext mvRewriteContext,
                                                        OptExpression input) {
        Map<ColumnRefOperator, ScalarOperator> queryColumnRefMap =
                MvUtils.getColumnRefMap(input, mvRewriteContext.getMaterializationContext().getQueryRefFactory());

        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(mvRewriteContext.getMaterializationContext());
        OptExpression newQueryInput = duplicator.duplicate(input);

        List<ColumnRefOperator> originalOutputColumns =
                queryColumnRefMap.keySet().stream().collect(Collectors.toList());
        List<ColumnRefOperator> newQueryOutputColumns = duplicator.getMappedColumns(originalOutputColumns);
        Map<ColumnRefOperator, ScalarOperator> newProjectionMap = Maps.newHashMap();
        for (int i = 0; i < originalOutputColumns.size(); i++) {
            newProjectionMap.put(originalOutputColumns.get(i), newQueryOutputColumns.get(i));
        }
        Operator newOp = newQueryInput.getOp();
        if (newOp.getProjection() == null) {
            newOp.setProjection(new Projection(newProjectionMap));
        } else {
            // merge two projections
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(newOp.getProjection().getColumnRefMap());
            Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : newProjectionMap.entrySet()) {
                ScalarOperator result = rewriter.rewrite(entry.getValue());
                resultMap.put(entry.getKey(), result);
            }
            newOp.setProjection(new Projection(resultMap));
        }
        deriveLogicalProperty(newQueryInput);
        return newQueryInput;
    }

    public static void setOptScanOpsHavePushDown(OptExpression input) {
        List<LogicalScanOperator> scanOps = MvUtils.getScanOperator(input);
        scanOps.stream().forEach(op -> op.setOpRuleMask(op.getOpRuleMask() | Operator.OP_PUSH_DOWN_BIT));
    }

    public static void setOpHasPushDown(OptExpression input) {
        input.getOp().setOpRuleMask(input.getOp().getOpRuleMask() | Operator.OP_PUSH_DOWN_BIT);
    }

    @VisibleForTesting
    public boolean checkAggOpt(OptExpression optExpression) {
        LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) optExpression.getOp();
        ColumnRefSet inputCols = optExpression.inputAt(0).getRowOutputInfo().getOutputColumnRefSet();
        ColumnRefSet usedCols = optExpression.getRowOutputInfo().getUsedColumnRefSet();
        if (aggOperator.getPredicate() != null) {
            usedCols.union(aggOperator.getPredicate().getUsedColumns());
        }
        if (aggOperator.getProjection() != null) {
            usedCols.union(aggOperator.getProjection().getUsedColumns());
        }
        // except aggregate's column refs
        usedCols.except(aggOperator.getAggregations().keySet());
        return checkInputCols(inputCols, usedCols);
    }

    private boolean checkJoinOpt(OptExpression optExpression) {
        ColumnRefSet inputCols = new ColumnRefSet();
        for (OptExpression input : optExpression.getInputs()) {
            inputCols.union(input.getRowOutputInfo().getOutputColumnRefSet());
        }
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
        ColumnRefSet usedCols = optExpression.getRowOutputInfo().getUsedColumnRefSet();
        if (joinOperator.getOnPredicate() != null) {
            usedCols.union(joinOperator.getOnPredicate().getUsedColumns());
        }
        if (joinOperator.getPredicate() != null) {
            usedCols.union(joinOperator.getPredicate().getUsedColumns());
        }
        if (joinOperator.getProjection() != null) {
            usedCols.union(joinOperator.getProjection().getUsedColumns());
        }
        return checkInputCols(inputCols, usedCols);
    }

    private boolean checkInputCols(ColumnRefSet inputCols, ColumnRefSet usedCols) {
        ColumnRefSet missedCols = usedCols.clone();
        missedCols.except(inputCols);
        return missedCols.isEmpty();
    }

    private class PreVisitor extends OptExpressionVisitor<AggregatePushDownContext, AggregatePushDownContext> {
        // Default visit method short-circuit top-down visiting
        @Override
        public AggregatePushDownContext visit(OptExpression optExpression, AggregatePushDownContext context) {
            optExpression.getInputs()
                    .replaceAll(input -> process(input, AggregatePushDownContext.EMPTY)
                            .getOp().orElse(input));
            return AggregatePushDownContext.EMPTY;
        }

        private boolean canNotPushDown(OptExpression optExpression, AggregatePushDownContext context) {
            return context.isEmpty() || optExpression.getOp().hasLimit();
        }

        @Override
        public AggregatePushDownContext visitLogicalJoin(OptExpression optExpression,
                                                         AggregatePushDownContext context) {
            if (canNotPushDown(optExpression, context)) {
                return visit(optExpression, context);
            }
            return context;
        }

        boolean isSupportedAggFunctionPushDown(CallOperator call) {
            String funcName = call.getFnName();
            // case1: rollup map functions
            if (REWRITE_ROLLUP_FUNCTION_MAP.containsKey(funcName)) {
                return true;
            }

            // case2: equivalent supported functions
            if (RewriteEquivalent.AGGREGATE_EQUIVALENTS.stream().anyMatch(x -> x.isSupportPushDownRewrite(call))) {
                return true;
            }
            return false;
        }

        @Override
        public AggregatePushDownContext visitLogicalAggregate(OptExpression optExpression,
                                                              AggregatePushDownContext context) {
            LogicalAggregationOperator aggOp = optExpression.getOp().cast();
            // check whether agg function is supported
            if (aggOp.getAggregations().values().stream().anyMatch(c -> !isSupportedAggFunctionPushDown(c))) {
                logMVRewrite(mvRewriteContext, "Agg function {} is not supported for push down", aggOp.getAggregations());
                return visit(optExpression, context);
            }
            // all constant can't push down
            if (!aggOp.getAggregations().isEmpty() &&
                    aggOp.getAggregations().values().stream().allMatch(ScalarOperator::isConstant)) {
                logMVRewrite(mvRewriteContext, "All constant agg function can't push down");
                return visit(optExpression, context);
            }

            // no-group-by don't push down
            if (aggOp.getGroupingKeys().isEmpty()) {
                logMVRewrite(mvRewriteContext, "No group by can't push down");
                return visit(optExpression, context);
            }

            context = new AggregatePushDownContext();
            context.setAggregator(aggOp);
            return context;
        }

        @Override
        public AggregatePushDownContext visitLogicalProject(OptExpression optExpression,
                                                            AggregatePushDownContext context) {
            if (canNotPushDown(optExpression, context)) {
                logMVRewrite(mvRewriteContext, "Can't push down for project node");
                return visit(optExpression, context);
            }

            LogicalProjectOperator projectOp = optExpression.getOp().cast();
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = projectOp.getColumnRefMap();
            if (columnRefMap.entrySet().stream().allMatch(e -> e.getValue().equals(e.getKey()))) {
                return context;
            }

            // rewrite by original column ref
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(columnRefMap);
            context.aggregations.replaceAll((k, v) -> (CallOperator) rewriter.rewrite(v));

            Set<ColumnRefOperator> groupByUsedCols = context.groupBys.values().stream()
                    .flatMap(v -> rewriter.rewrite(v).getColumnRefs().stream()).collect(Collectors.toSet());
            context.groupBys.clear();
            groupByUsedCols.forEach(col -> context.groupBys.put(col, col));
            if (columnRefMap.values().stream().allMatch(ScalarOperator::isColumnRef)) {
                return context;
            }

            if (!context.aggregations.isEmpty() &&
                    !context.aggregations.values().stream().allMatch(c -> c.getChildren().stream().allMatch(
                            ScalarOperator::isColumnRef))) {
                logMVRewrite(mvRewriteContext, "Project node constructs agg arg {} remapping failed",
                        context.aggregations);
                return visit(optExpression, context);
            }
            return context;
        }

        @Override
        public AggregatePushDownContext visitLogicalFilter(OptExpression optExpression,
                                                           AggregatePushDownContext context) {
            if (canNotPushDown(optExpression, context)) {
                return visit(optExpression, context);
            }

            // add filter columns in groupBys
            LogicalFilterOperator filter = optExpression.getOp().cast();
            filter.getRequiredChildInputColumns().getStream().map(queryColumnRefFactory::getColumnRef)
                    .forEach(v -> context.groupBys.put(v, v));
            return context;
        }

        @Override
        public AggregatePushDownContext visitLogicalTableScan(OptExpression optExpression,
                                                              AggregatePushDownContext context) {
            if (canNotPushDown(optExpression, context)) {
                return visit(optExpression, context);
            }
            return context;
        }
    }

    // PostVisitor is used to rewrite the current node, the visit function must satisfy:
    // 1. return AggRewriteInfo.NOT_REWRITE to short-circuit the later bottom-up rewrite operation.
    // 2. put new-created OptExpression into AggRewriteInfo by invoke AggRewriteInfo.setOp if the new
    // OptExpression is created.
    // 3. return input AggRewriteInfo as return value if you want to rewrite upper nodes.
    private class PostVisitor extends OptExpressionVisitor<AggRewriteInfo, AggRewriteInfo> {
        private boolean isInvalid(OptExpression optExpression, AggregatePushDownContext context) {
            return context.isEmpty() || context.groupBys.isEmpty() ||
                    context.aggregations.isEmpty() || optExpression.getOp().hasLimit();
        }

        // Default visit method do nothing but just pass the AggRewriteInfo to its parent
        @Override
        public AggRewriteInfo visit(OptExpression optExpression, AggRewriteInfo rewriteInfo) {
            if (rewriteInfo != AggRewriteInfo.NOT_REWRITE) {
                rewriteInfo.setOp(optExpression);
            }
            return rewriteInfo;
        }

        @Override
        public AggRewriteInfo visitLogicalAggregate(OptExpression optExpression, AggRewriteInfo rewriteInfo) {
            if (!rewriteInfo.getRemapping().isPresent()) {
                return AggRewriteInfo.NOT_REWRITE;
            }

            final LogicalAggregationOperator aggregate = optExpression.getOp().cast();
            final Map<ColumnRefOperator, CallOperator> aggregations = aggregate.getAggregations();
            Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
            final AggregatePushDownContext ctx = rewriteInfo.getCtx();

            Map<ColumnRefOperator, ColumnRefOperator> remapping = rewriteInfo.getRemapping().get().getRemapping();
            Map<ColumnRefOperator, ScalarOperator> aggProjection = Maps.newHashMap();
            Map<ColumnRefOperator, ScalarOperator> aggColRefToAggMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregations.entrySet()) {
                ColumnRefOperator origAggColRef = entry.getKey();
                CallOperator aggCall = entry.getValue();
                CallOperator newAggregate = null;
                // If rewritten function is not an aggregation function, it could be like ScalarFunc(AggregateFunc(...))
                // We need to decompose it into Projection function and Aggregation function
                // E.g. count(distinct x) => array_length(array_unique_agg(x))
                // The array_length is a ScalarFunction and array_unique_agg is AggregateFunction
                // So it's decomposed into 1: array_length(slot_2), 2: array_unique_agg(x)
                CallOperator realAggregate = null;
                int foundIndex = 0;

                CallOperator newAggCall = ctx.aggColRefToPushDownAggMap.get(origAggColRef);
                Preconditions.checkState(newAggCall != null, "newAggCall is null");
                if (ctx.isRewrittenByEquivalent(newAggCall)) {
                    newAggregate = ctx.aggToFinalAggMap.get(newAggCall);
                    if (newAggregate == null) {
                        logMVRewrite(mvRewriteContext, "Aggregation's final stage function is not found, aggColRef:{}, " +
                                "aggCall:{}", origAggColRef, aggCall);
                        return AggRewriteInfo.NOT_REWRITE;
                    }

                    realAggregate = newAggregate;
                    if (!newAggregate.isAggregate()) {
                        foundIndex = -1;
                        for (int i = 0; i < newAggregate.getChildren().size(); i++) {
                            if (newAggregate.getChild(i) instanceof CallOperator) {
                                CallOperator call = (CallOperator) newAggregate.getChild(i);
                                if (call.isAggregate()) {
                                    foundIndex = i;
                                    realAggregate = call;
                                    break;
                                }
                            }
                        }
                        Preconditions.checkState(foundIndex != -1,
                                "no aggregate functions found: " + newAggregate.getChildren());
                    }
                } else {
                    ScalarOperator newArg0 = remapping.get(origAggColRef);
                    if (newArg0 == null) {
                        logMVRewrite(mvRewriteContext, "Aggregation's arg0 is not rewritten after remapping, " +
                                "aggColRef:{}, aggCall:{}", origAggColRef, aggCall);
                        return AggRewriteInfo.NOT_REWRITE;
                    }
                    List<ScalarOperator> newArgs = aggCall.getChildren();
                    newArgs.set(0, newArg0);
                    String rollupFuncName = getRollupFunctionName(aggCall, false);
                    Type[] argTypes = newArgs.stream().map(ScalarOperator::getType).toArray(Type[]::new);
                    Function newFunc = Expr.getBuiltinFunction(rollupFuncName, argTypes,
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    Preconditions.checkState(newFunc != null,
                            "Get rollup function is null, rollupFuncName:", rollupFuncName);
                    newAggregate = new CallOperator(rollupFuncName, newFunc.getReturnType(), newArgs, newFunc);
                    realAggregate = newAggregate;
                }

                // rewrite it with remapping and final aggregate should use the new input as its argument.
                Preconditions.checkState(realAggregate != null, "realAggregate is null");
                realAggregate = replaceAggFuncArgument(remapping, origAggColRef, realAggregate, foundIndex);

                ColumnRefOperator newAggColRef = queryColumnRefFactory.create(realAggregate,
                        realAggregate.getType(), realAggregate.isNullable());
                newAggregations.put(newAggColRef, realAggregate);
                if (!newAggregate.isAggregate()) {
                    CallOperator copyProject = (CallOperator) newAggregate.clone();
                    copyProject.setChild(foundIndex, newAggColRef);

                    ColumnRefOperator newProjColRef = queryColumnRefFactory
                            .create(copyProject, copyProject.getType(), copyProject.isNullable());
                    // keeps original output column, otherwise upstream operators may be affected
                    aggProjection.put(newProjColRef, copyProject);

                    // replace original projection to newProjColRef.
                    aggColRefToAggMap.put(origAggColRef, copyProject);
                } else {
                    // keeps original output column, otherwise upstream operators may be affected
                    aggProjection.put(newAggColRef, genRollupProject(aggCall, newAggColRef, true));

                    // replace original projection to newAggColRef or no need to change?
                    aggColRefToAggMap.put(origAggColRef, newAggColRef);
                }
            }

            // add projection to make sure that the output columns keep the same with the origin query
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(aggColRefToAggMap);
            if (aggregate.getProjection() != null) {
                Map<ColumnRefOperator, ScalarOperator> originalMap = aggregate.getProjection().getColumnRefMap();
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : originalMap.entrySet()) {
                    ScalarOperator rewritten = rewriter.rewrite(entry.getValue());
                    aggProjection.put(entry.getKey(), rewritten);
                }
            } else {
                // If there is no projections before, aggregations are already in the aggProjection.
                // group by keys
                for (ColumnRefOperator columnRefOperator : aggregate.getGroupingKeys()) {
                    aggProjection.put(columnRefOperator, columnRefOperator);
                }
            }

            // rewrite aggregate's predicate
            ScalarOperator predicate = aggregate.getPredicate();
            if (aggregate.getPredicate() != null) {
                predicate = rewriter.rewrite(aggregate.getPredicate());
            }

            Projection projection = new Projection(aggProjection);
            LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                    .withOperator(aggregate)
                    .setAggregations(newAggregations)
                    .setProjection(projection)
                    .setPredicate(predicate)
                    .build();
            optExpression = OptExpression.create(newAgg, optExpression.getInputs());
            if (!checkAggOpt(optExpression)) {
                return AggRewriteInfo.NOT_REWRITE;
            }
            rewriteInfo.setOp(optExpression);
            return rewriteInfo;
        }

        // rewrite it with remapping and final aggregate should use the new input as its argument.
        private CallOperator replaceAggFuncArgument(Map<ColumnRefOperator, ColumnRefOperator> remapping,
                                                    ColumnRefOperator origAggColRef,
                                                    CallOperator aggCall,
                                                    int argIdx) {
            ScalarOperator newArg0 = remapping.get(origAggColRef);
            if (newArg0 == null) {
                logMVRewrite(mvRewriteContext, "Aggregation's arg0 is not rewritten after remapping, " +
                        "aggColRef:{}, aggCall:{}", origAggColRef, aggCall);
                return null;
            }
            CallOperator newAggCall = (CallOperator) aggCall.clone();
            if (argIdx >= newAggCall.getChildren().size()) {
                logMVRewrite(mvRewriteContext, "Aggregation's arg index is out of range, " +
                        "aggColRef:{}, aggCall:{}", origAggColRef, aggCall);
                return null;
            }
            newAggCall.setChild(argIdx, newArg0);
            return newAggCall;
        }

        @Override
        public AggRewriteInfo visitLogicalJoin(OptExpression optExpression,
                                               AggRewriteInfo rewriteInfo) {
            AggregatePushDownContext context = rewriteInfo.getCtx();
            if (isInvalid(optExpression, context)) {
                return AggRewriteInfo.NOT_REWRITE;
            }
            // split aggregate to left/right child
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            Projection projection = joinOperator.getProjection();
            ReplaceColumnRefRewriter replacer = null;
            if (projection != null) {
                replacer = new ReplaceColumnRefRewriter(projection.getColumnRefMap());
            }
            AggregatePushDownContext leftContext = processJoinChild(replacer, optExpression, context, 0);
            AggregatePushDownContext rightContext = processJoinChild(replacer, optExpression, context, 1);
            AggRewriteInfo aggRewriteInfo0 = process(optExpression.inputAt(0), leftContext);
            AggRewriteInfo aggRewriteInfo1 = process(optExpression.inputAt(1), rightContext);

            AggColumnRefRemapping combinedRemapping = new AggColumnRefRemapping();
            if (aggRewriteInfo0.hasRewritten()) {
                optExpression.setChild(0, aggRewriteInfo0.getOp().get());
                aggRewriteInfo0.output(combinedRemapping, context);
            }
            if (aggRewriteInfo1.hasRewritten()) {
                optExpression.setChild(1, aggRewriteInfo1.getOp().get());
                aggRewriteInfo1.output(combinedRemapping, context);
            }
            if (!combinedRemapping.isEmpty()) {
                Map<ColumnRefOperator, ScalarOperator> newColRefMap = replaceColumnRefMap(context, combinedRemapping,
                        joinOperator.getProjection().getColumnRefMap());
                Projection newProjection = new Projection(newColRefMap);
                joinOperator.setProjection(newProjection);

            }
            if (!checkJoinOpt(optExpression)) {
                return AggRewriteInfo.NOT_REWRITE;
            }
            return combinedRemapping.isEmpty() ? AggRewriteInfo.NOT_REWRITE : new AggRewriteInfo(true, combinedRemapping,
                    optExpression,
                    context);
        }

        private Map<ColumnRefOperator, CallOperator> replaceAggregationExprs(
                ReplaceColumnRefRewriter replacer,
                Map<ColumnRefOperator, CallOperator> aggregations) {
            // If without replacer which means there is no projections or remapping, we don't need to rewrite
            if (replacer == null) {
                return aggregations;
            }

            Map<ColumnRefOperator, CallOperator> newColRefToAggMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, CallOperator> e : aggregations.entrySet()) {
                ColumnRefOperator colRef = e.getKey();
                CallOperator aggCall = e.getValue();

                // push down aggregate expression below join
                // replace with new mapping if replacer existed
                CallOperator newAggCall = (CallOperator) replacer.rewrite(aggCall);
                newColRefToAggMap.put(colRef, newAggCall);
            }
            return newColRefToAggMap;
        }

        private Map<ColumnRefOperator, ScalarOperator> replaceGroupByExprs(
                ReplaceColumnRefRewriter replacer,
                Map<ColumnRefOperator, ScalarOperator> groupBys) {
            // If without replacer which means there is no projections or remapping, we don't need to rewrite
            if (replacer == null) {
                return groupBys;
            }

            Map<ColumnRefOperator, ScalarOperator> newColRefToExprMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : groupBys.entrySet()) {
                ColumnRefOperator colRef = e.getKey();
                ScalarOperator expr = e.getValue();
                // push down aggregate expression below join
                // replace with new mapping if replacer existed
                ScalarOperator newGroupBy = replacer.rewrite(expr);
                newColRefToExprMap.put(colRef, newGroupBy);
            }
            return newColRefToExprMap;
        }

        private AggregatePushDownContext processJoinChild(ReplaceColumnRefRewriter replacer,
                                                          OptExpression expression,
                                                          AggregatePushDownContext context,
                                                          int child) {
            LogicalJoinOperator join = (LogicalJoinOperator) expression.getOp();
            ColumnRefSet childOutput = expression.getChildOutputColumns(child);
            // check aggregations
            ColumnRefSet aggregationsRefs = new ColumnRefSet();
            Map<ColumnRefOperator, CallOperator> rewriteAggregations = replaceAggregationExprs(replacer,
                    context.aggregations);
            if (rewriteAggregations == null) {
                logMVRewrite(mvRewriteContext, "Join's child {} rewrite aggregations failed", child);
                return AggregatePushDownContext.EMPTY;
            }
            rewriteAggregations.values().stream().map(CallOperator::getUsedColumns).forEach(aggregationsRefs::union);
            if (!childOutput.containsAll(aggregationsRefs)) {
                logMVRewrite(mvRewriteContext, "Join's child {} column refs {} not contains all aggregate column refs: {}",
                        child, childOutput, aggregationsRefs);
                return AggregatePushDownContext.EMPTY;
            }

            AggregatePushDownContext childContext = new AggregatePushDownContext();
            childContext.aggregations.putAll(rewriteAggregations);

            // check group by
            Map<ColumnRefOperator, ScalarOperator> rewriteGroupBys = replaceGroupByExprs(replacer, context.groupBys);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rewriteGroupBys.entrySet()) {
                ColumnRefSet groupByUseColumns = entry.getValue().getUsedColumns();
                if (childOutput.containsAll(groupByUseColumns)) {
                    childContext.groupBys.put(entry.getKey(), entry.getValue());
                } else {
                    // TODO: e.g. group by abs(a + b), we can derive group by a
                    return AggregatePushDownContext.EMPTY;
                }
            }

            if (join.getOnPredicate() != null) {
                join.getOnPredicate().getUsedColumns().getStream().map(queryColumnRefFactory::getColumnRef)
                        .filter(childOutput::contains)
                        .forEach(c -> childContext.groupBys.put(c, c));
            }

            if (join.getPredicate() != null) {
                join.getPredicate().getUsedColumns().getStream().map(queryColumnRefFactory::getColumnRef)
                        .filter(childOutput::contains)
                        .forEach(v -> childContext.groupBys.put(v, v));
            }

            childContext.origAggregator = context.origAggregator;
            childContext.pushPaths.addAll(context.pushPaths);
            childContext.pushPaths.add(child);
            return childContext;
        }

        @Override
        public AggRewriteInfo visitLogicalTableScan(OptExpression optExpression, AggRewriteInfo rewriteInfo) {
            AggregatePushDownContext ctx = rewriteInfo.getCtx();
            if (isInvalid(optExpression, ctx)) {
                logMVRewrite(mvRewriteContext, "Table scan node is invalid for push down");
                return AggRewriteInfo.NOT_REWRITE;
            }

            // build group bys
            List<ColumnRefOperator> groupBys = ctx.groupBys.values().stream()
                    .map(ScalarOperator::getUsedColumns)
                    .flatMap(colSet -> colSet.getStream().map(queryColumnRefFactory::getColumnRef)).distinct()
                    .collect(Collectors.toList());

            LogicalScanOperator scanOp = optExpression.getOp().cast();
            ColumnRefSet scanOutputColRefSet = new ColumnRefSet(scanOp.getOutputColumns());
            Preconditions.checkArgument(scanOutputColRefSet.containsAll(new ColumnRefSet(groupBys)));

            // New agg function to new generated column ref
            Map<CallOperator, ColumnRefOperator> uniqueAggregations = Maps.newHashMap();
            Map<ColumnRefOperator, ColumnRefOperator> remapping = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : ctx.aggregations.entrySet()) {
                ColumnRefOperator aggColRef = entry.getKey();
                CallOperator aggCall = entry.getValue();
                Preconditions.checkArgument(aggCall.getChildren().size() >= 1);

                if (uniqueAggregations.containsKey(aggCall)) {
                    ctx.aggColRefToPushDownAggMap.put(aggColRef, aggCall);
                    continue;
                }
                // NOTE: This new aggregate type is final stage's type not the immediate/partial stage type.
                ColumnRefOperator newColRef = queryColumnRefFactory.create(aggCall, aggCall.getType(), aggCall.isNullable());
                uniqueAggregations.put(aggCall, newColRef);
                remapping.put(aggColRef, newColRef);
                ctx.aggColRefToPushDownAggMap.put(aggColRef, aggCall);
            }
            Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
            uniqueAggregations.forEach((k, v) -> newAggregations.put(v, k));
            LogicalAggregationOperator newAggOp = LogicalAggregationOperator.builder()
                    .setAggregations(newAggregations)
                    .setType(AggType.GLOBAL)
                    .setGroupingKeys(groupBys)
                    .setPartitionByColumns(groupBys)
                    .build();
            OptExpression optAggOp = OptExpression.create(newAggOp, optExpression);

            // rewrite by mv.
            OptExpression rewritten = doRewritePushDownAgg(ctx, optAggOp);
            if (rewritten == null) {
                logMVRewrite(mvRewriteContext, "Rewrite table scan node by mv failed");
                return AggRewriteInfo.NOT_REWRITE;
            }
            // Generate the push down aggregate function for the given call operator.
            // NOTE: Here we put the original agg function as the push down agg function,  but the mv rewrite only need to
            // get the partial(rollup) agg function, and not the final agg function which means the input agg is not equivalent.
            // This is because we can not deduce which agg function to push down or rewrite by mv.
            // eg:
            // query: select count(distinct t1.a) from t1 join t2 on t1.id = t2.id group by t1.b;
            // mv1  : select bitmap_union(to_bitmap(a)) from t1.a  group by b;
            // mv2  : select array_agg_distinct(a) from t1.a  group by b;
            // Both mv1 and mv2 can be used to rewrite query, so here we push down the original agg function(count(distinct a)
            // ) to mv
            // rewrite.If mv1 is used for mv rewrite, the mv rewrite's result is:
            // select bitmap_union(to_bitmap(a) from t1.a group by b, not: bitmap_union_count(bitmap_union(to_bitmap(a)))!
            // and a final agg function into `AggregatePushDownContext` to be used in the final stage.
            for (Map.Entry<ColumnRefOperator, ColumnRefOperator> e : remapping.entrySet()) {
                ColumnRefOperator newAggColRef = e.getValue();
                CallOperator aggCall = ctx.aggregations.get(e.getKey());
                if (ctx.isRewrittenByEquivalent(aggCall)) {
                    CallOperator partialFn = ctx.aggToPartialAggMap.get(aggCall);
                    ColumnRefOperator realPartialColRef = new ColumnRefOperator(newAggColRef.getId(), partialFn.getType(),
                            newAggColRef.getName(), partialFn.isNullable());
                    remapping.put(e.getKey(), realPartialColRef);
                }
            }
            return new AggRewriteInfo(true, new AggColumnRefRemapping(remapping), rewritten, ctx);
        }

        /**
         * Rewrite query plan which has been pushed down by materialized view
         *
         * @param optExpression: push down query plan
         * @return: rewritten query plan if rewrite success, otherwise return null
         */
        private OptExpression doRewritePushDownAgg(AggregatePushDownContext ctx,
                                                   OptExpression optExpression) {
            List<Table> queryTables = MvUtils.getAllTables(optExpression);
            final ReplaceColumnRefRewriter queryColumnRefRewriter =
                    MvUtils.getReplaceColumnRefWriter(optExpression, queryColumnRefFactory);

            PredicateSplit queryPredicateSplit = getQuerySplitPredicate(optimizerContext,
                    mvRewriteContext.getMaterializationContext(), optExpression, queryColumnRefFactory,
                    queryColumnRefRewriter, rule);
            if (queryPredicateSplit == null) {
                logMVRewrite(mvRewriteContext, "Rewrite push down agg failed: get query split predicate failed");
                return null;
            }
            logMVRewrite(mvRewriteContext, "Push down agg query split predicate: {}", queryPredicateSplit);

            MvRewriteContext newMvRewriteContext = new MvRewriteContext(mvRewriteContext.getMaterializationContext(),
                    queryTables, optExpression, queryColumnRefRewriter, queryPredicateSplit, null, rule);
            // set aggregate push down context to be used in the final stage
            newMvRewriteContext.setAggregatePushDownContext(ctx);
            AggregatedMaterializedViewRewriter rewriter = new AggregatedMaterializedViewRewriter(newMvRewriteContext);
            OptExpression result = rewriter.doRewrite(mvRewriteContext);
            if (result == null) {
                logMVRewrite(mvRewriteContext, "doRewrite phase failed in AggregatedMaterializedViewRewriter");
                return null;
            }
            deriveLogicalProperty(result);
            return result;
        }

        @Override
        public AggRewriteInfo visitLogicalProject(OptExpression optExpression, AggRewriteInfo rewriteInfo) {
            if (!rewriteInfo.getRemapping().isPresent()) {
                return AggRewriteInfo.NOT_REWRITE;
            }
            LogicalProjectOperator project = optExpression.getOp().cast();
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = project.getColumnRefMap();
            ColumnRefSet columnRefSet = new ColumnRefSet();
            columnRefSet.union(columnRefMap.keySet());
            columnRefSet.union(getReferencedColumnRef(columnRefMap.values()));
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap =
                    replaceColumnRefMap(rewriteInfo.getCtx(), rewriteInfo.getRemapping().get(),
                            columnRefMap);
            LogicalProjectOperator newProject = LogicalProjectOperator.builder()
                    .withOperator(project)
                    .setColumnRefMap(newColumnRefMap)
                    .build();
            OptExpression newOpt = OptExpression.create(newProject, optExpression.getInputs());
            rewriteInfo.setOp(newOpt);
            return rewriteInfo;
        }

        private Map<ColumnRefOperator, ScalarOperator> replaceColumnRefMap(AggregatePushDownContext ctx,
                                                                           AggColumnRefRemapping aggColumnRefRemapping,
                                                                           Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();

            // Add into new generated push down aggregate column refs into new projection, otherwise the upstream final aggregate
            // cannot ref the push down aggregate column refs as argument.
            for (Map.Entry<ColumnRefOperator, ColumnRefOperator> e : aggColumnRefRemapping.getRemapping().entrySet()) {
                newColumnRefMap.put(e.getValue(), e.getValue());
            }

            // Remove original aggregate column ref from column ref map
            final ColumnRefSet aggColumnRefSet = getReferencedColumnRef(new ArrayList<>(ctx.aggregations.values()));
            final ColumnRefSet groupColumnRefSet = getReferencedColumnRef(new ArrayList<>(ctx.groupBys.values()));
            aggColumnRefSet.except(groupColumnRefSet);
            if (columnRefMap != null) {
                for (Map.Entry<ColumnRefOperator, ScalarOperator> e : columnRefMap.entrySet()) {
                    if (aggColumnRefSet.contains(e.getKey())) {
                        continue;
                    }
                    newColumnRefMap.put(e.getKey(), e.getValue());
                }
            }
            return newColumnRefMap;
        }
    }

    public static ColumnRefSet getReferencedColumnRef(Collection<ScalarOperator> operators) {
        ColumnRefSet refSet = new ColumnRefSet();
        operators.stream().map(ScalarOperator::getUsedColumns).forEach(refSet::union);
        return refSet;
    }

    // process each child to gather and merge AggRewriteInfo
    private AggRewriteInfo processChildren(OptExpression optExpression,
                                           AggregatePushDownContext context) {
        if (optExpression.getInputs().isEmpty()) {
            return new AggRewriteInfo(false, null, null, context);
        }
        // Join will handle its children in the `processPost`
        if (optExpression.getOp().getOpType() == OperatorType.LOGICAL_JOIN) {
            return new AggRewriteInfo(false, null, null, context);
        }

        List<AggRewriteInfo> childAggRewriteInfoList = optExpression.getInputs().stream()
                .map(input -> process(input, context))
                .collect(Collectors.toList());
        if (childAggRewriteInfoList.stream().noneMatch(AggRewriteInfo::hasRewritten)) {
            return AggRewriteInfo.NOT_REWRITE;
        }

        // merge ColumnRefMapping generated by each child into a total one
        Iterator<AggRewriteInfo> nextAggRewriteInfo = childAggRewriteInfoList.iterator();
        optExpression.getInputs().replaceAll(input -> nextAggRewriteInfo.next().getOp().orElse(input));
        AggColumnRefRemapping combinedRemapping = new AggColumnRefRemapping();
        childAggRewriteInfoList.forEach(rewriteInfo -> rewriteInfo.getRemapping().ifPresent(
                combinedRemapping::combine));
        return new AggRewriteInfo(true, combinedRemapping, optExpression, context);
    }

    // Check current opt to see whether push distinct agg down or not using PreVisitor
    private Optional<AggregatePushDownContext> processPre(OptExpression opt,
                                                          AggregatePushDownContext context) {
        AggregatePushDownContext newContext = opt.getOp().accept(preVisitor, opt, context);
        // short-circuit if the returning context is EMPTY
        if (newContext == AggregatePushDownContext.EMPTY) {
            return Optional.empty();
        } else {
            return Optional.of(newContext);
        }
    }

    // Rewrite current opt according to rewriteInfo using PostVisitor, short-circuit if
    // rewriteInfo is AggRewriteInfo.NOT_REWRITE
    private AggRewriteInfo processPost(OptExpression opt, AggRewriteInfo rewriteInfo) {
        return opt.getOp().accept(postVisitor, opt, rewriteInfo);
    }

    // When rewrite a tree, we visit each node of this tree in top-down style, process function is
    // used to visit tree node, the visit operation is decomposed into three steps:
    // 1. processPre: check whether visiting should stop or not, collect info used to rewrite the node.
    // 2. processChildren: visit children of current node, merge infos from children into final one.
    // 3. processPost: rewrite current node.
    private AggRewriteInfo process(OptExpression opt,
                                   AggregatePushDownContext context) {
        return processPre(opt, context)
                .map(ctx -> processPost(opt, processChildren(opt, ctx)))
                .orElse(AggRewriteInfo.NOT_REWRITE);
    }
}