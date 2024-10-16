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
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
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
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.isSupportedAggFunctionPushDown;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.doRewritePushDownAgg;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.getPushDownRollupFinalAggregateOpt;

/**
 * A mv rewriter that supports to push down aggregate functions below join operator and rewrite the query by mv transparently.
 */
public final class AggregatedMaterializedViewPushDownRewriter extends MaterializedViewRewriter {
    private final MvRewriteContext mvRewriteContext;
    private final ColumnRefFactory queryColumnRefFactory;
    private final Rule rule;
    private final PreVisitor preVisitor = new PreVisitor();
    private final PostVisitor postVisitor = new PostVisitor();

    public AggregatedMaterializedViewPushDownRewriter(MvRewriteContext mvRewriteContext,
                                                      Rule rule) {
        super(mvRewriteContext);

        this.mvRewriteContext = mvRewriteContext;
        this.queryColumnRefFactory = mvRewriteContext.getMaterializationContext().getQueryRefFactory();
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
            Utils.setOptScanOpsBit(result, Operator.OP_PUSH_DOWN_BIT);
            return result;
        } else {
            logMVRewrite(mvContext, "AggregateJoin pushdown rewrite failed");
            Utils.setOpBit(input, Operator.OP_PUSH_DOWN_BIT);
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

    @VisibleForTesting
    public boolean checkAggOpt(OptExpression optExpression) {
        if (optExpression == null) {
            return false;
        }
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

        @Override
        public AggregatePushDownContext visitLogicalAggregate(OptExpression optExpression,
                                                              AggregatePushDownContext context) {
            LogicalAggregationOperator aggOp = optExpression.getOp().cast();
            // check whether agg function is supported
            if (aggOp.getAggregations().values().stream().anyMatch(c -> !isSupportedAggFunctionPushDown(c))) {
                logMVRewrite(mvRewriteContext, "Agg function {} is not supported for push down", aggOp.getAggregations());
                return visit(optExpression, context);
            }
            // all constants can't push down
            if (!aggOp.getAggregations().isEmpty() &&
                    aggOp.getAggregations().values().stream().allMatch(ScalarOperator::isConstant)) {
                logMVRewrite(mvRewriteContext, "All constant agg function can't push down");
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
            return context.isEmpty() || optExpression.getOp().hasLimit();
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
            if (!rewriteInfo.getRemappingUnChecked().isPresent()) {
                return AggRewriteInfo.NOT_REWRITE;
            }

            final Map<ColumnRefOperator, ColumnRefOperator> remapping = rewriteInfo.getRemappingUnChecked().get().getRemapping();
            optExpression = getPushDownRollupFinalAggregateOpt(mvRewriteContext, rewriteInfo.getCtx(),
                    remapping, optExpression, optExpression.getInputs());
            if (!checkAggOpt(optExpression)) {
                logMVRewrite(mvRewriteContext, "Rollup aggregate node is invalid after agg push down");
                return AggRewriteInfo.NOT_REWRITE;
            }
            rewriteInfo.setOp(optExpression);
            return rewriteInfo;
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
                logMVRewrite(mvRewriteContext, "Join node is invalid after agg push down");
                return AggRewriteInfo.NOT_REWRITE;
            }
            return !aggRewriteInfo0.hasRewritten() && !aggRewriteInfo1.hasRewritten() ? AggRewriteInfo.NOT_REWRITE :
                    new AggRewriteInfo(true, combinedRemapping, optExpression, context);
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
            Map<ColumnRefOperator, CallOperator> rewriteAggregations = replaceAggregationExprs(replacer,
                    context.aggregations);
            if (rewriteAggregations == null) {
                logMVRewrite(mvRewriteContext, "Join's child {} rewrite aggregations failed", child);
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

            if (childContext.groupBys.isEmpty()) {
                logMVRewrite(mvRewriteContext, "Join's child {} push down group by empty, childOutput: {}",
                        child, childOutput);
                return AggregatePushDownContext.EMPTY;
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
            List<Table> queryTables = MvUtils.getAllTables(optExpression);

            final List<Table> mvTables = MvUtils.getAllTables(materializationContext.getMvExpression());
            MatchMode matchMode = MaterializedViewRewriter.getMatchMode(queryTables, mvTables);
            if (matchMode == MatchMode.NOT_MATCH && mvTables.stream().noneMatch(queryTables::contains)) {
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
            OptExpression rewritten = doRewritePushDownAgg(mvRewriteContext, ctx, optAggOp, rule);
            if (rewritten == null) {
                logMVRewrite(mvRewriteContext,
                        "Rewrite table " + scanOp.getTable().getTableIdentifier() + " scan node by mv failed");
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

        @Override
        public AggRewriteInfo visitLogicalProject(OptExpression optExpression, AggRewriteInfo rewriteInfo) {
            if (!rewriteInfo.getRemappingUnChecked().isPresent()) {
                return AggRewriteInfo.NOT_REWRITE;
            }
            LogicalProjectOperator project = optExpression.getOp().cast();
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = project.getColumnRefMap();
            ColumnRefSet columnRefSet = new ColumnRefSet();
            columnRefSet.union(columnRefMap.keySet());
            columnRefSet.union(getReferencedColumnRef(columnRefMap.values()));
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap =
                    replaceColumnRefMap(rewriteInfo.getCtx(), rewriteInfo.getRemappingUnChecked().get(),
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
        childAggRewriteInfoList.forEach(rewriteInfo -> rewriteInfo.getRemappingUnChecked().ifPresent(
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