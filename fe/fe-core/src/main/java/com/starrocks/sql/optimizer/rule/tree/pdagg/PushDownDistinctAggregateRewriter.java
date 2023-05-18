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

package com.starrocks.sql.optimizer.rule.tree.pdagg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// Push Distinct Agg down below WindowOperators, for an example:
// Q1: select distinct a, b, c, sum(d) over(partition by a, b order by c) from t;
// Q2: select distinct a, b, c, sum(d) over(partition by a, b order by c) from (
//      select a,b,c,sum(d) as d from t group by a,b,c) t
// Q1 is equivalent to Q2, however Q2 is far more efficient than Q1 when a, b and c columns are
// low-cardinality and there are many duplicates of (a,b,c) in table t, since group-by aggregation
// below WindowOperators reduces the number of rows processed by WindowOperators drastically.
// This Rewriter is used to transform Q1 into Q2
public class PushDownDistinctAggregateRewriter {
    private final TaskContext taskContext;
    private final OptimizerContext optimizerContext;
    private final ColumnRefFactory factory;
    private final SessionVariable sessionVariable;

    public PushDownDistinctAggregateRewriter(TaskContext taskContext) {
        this.taskContext = taskContext;
        optimizerContext = taskContext.getOptimizerContext();
        factory = taskContext.getOptimizerContext().getColumnRefFactory();
        sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
    }

    public OptExpression rewrite(OptExpression tree) {
        return process(tree, AggregatePushDownContext.EMPTY).getOp().orElse(tree);
    }

    // After rewrite, the post-rewrite tree must replace the old slotId with the new one,
    // ColumnRefRemapping is used to keep the mapping: old slotId->new slotId
    public static class ColumnRefRemapping {
        private final Map<ColumnRefOperator, ColumnRefOperator> remapping;
        private Optional<ReplaceColumnRefRewriter> cachedReplacer = Optional.empty();
        private Optional<ColumnRefSet> cachedColumnRefSet = Optional.empty();
        public static final ColumnRefRemapping EMPTY_REMAPPING = new ColumnRefRemapping();

        public ColumnRefRemapping() {
            remapping = Maps.newHashMap();
        }

        public ColumnRefRemapping(Map<ColumnRefOperator, ColumnRefOperator> remapping) {
            this.remapping = remapping;
        }

        public void combine(ColumnRefRemapping other) {
            remapping.putAll(other.remapping);
            cachedReplacer = Optional.empty();
            cachedColumnRefSet = Optional.empty();
        }

        public ReplaceColumnRefRewriter getReplacer() {
            if (!cachedReplacer.isPresent()) {
                ReplaceColumnRefRewriter replacer =
                        new ReplaceColumnRefRewriter(remapping.entrySet().stream().collect(
                                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)), true);
                cachedReplacer = Optional.of(replacer);
            }
            return cachedReplacer.get();
        }

        public ColumnRefSet getColumnRefSet() {
            if (!cachedColumnRefSet.isPresent()) {
                cachedColumnRefSet = Optional.of(new ColumnRefSet(remapping.keySet()));
            }
            return cachedColumnRefSet.get();
        }

        public boolean isEmpty() {
            return remapping.isEmpty();
        }
    }

    private static class RewriteInfo {
        private boolean rewritten = false;
        private ColumnRefRemapping remapping;
        private OptExpression op;

        public AggregatePushDownContext getCtx() {
            return ctx;
        }

        public void setCtx(AggregatePushDownContext ctx) {
            this.ctx = ctx;
        }

        private AggregatePushDownContext ctx;

        public static final RewriteInfo NOT_REWRITE = new RewriteInfo(false, null, null, null);

        public RewriteInfo(boolean rewritten, ColumnRefRemapping remapping,
                           OptExpression op, AggregatePushDownContext ctx) {
            this.rewritten = rewritten;
            this.remapping = remapping;
            this.op = op;
            this.ctx = ctx;
        }

        public boolean hasRewritten() {
            return rewritten;
        }

        public void setRewritten(boolean rewritten) {
            this.rewritten = rewritten;
        }

        public Optional<ColumnRefRemapping> getRemapping() {
            if (!rewritten || remapping.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(remapping);
        }

        public void setRemapping(ColumnRefRemapping remapping) {
            this.remapping = remapping;
        }

        public Optional<OptExpression> getOp() {
            return rewritten ? Optional.of(op) : Optional.empty();
        }

        public void setOp(OptExpression op) {
            this.op = op;
        }
    }

    private class PreVisitor extends OptExpressionVisitor<AggregatePushDownContext, AggregatePushDownContext> {
        // Default visit method short-circuit top-down visiting
        @Override
        public AggregatePushDownContext visit(OptExpression optExpression, AggregatePushDownContext context) {
            optExpression.getInputs().replaceAll(
                    input -> process(input, AggregatePushDownContext.EMPTY).getOp().orElse(input));
            return AggregatePushDownContext.EMPTY;
        }

        private boolean canNotPushDown(OptExpression optExpression, AggregatePushDownContext context) {
            return context.isEmpty() || optExpression.getOp().hasLimit();
        }

        @Override
        public AggregatePushDownContext visitLogicalAggregate(OptExpression optExpression,
                                                              AggregatePushDownContext context) {
            LogicalAggregationOperator aggOp = optExpression.getOp().cast();
            // distinct/count* aggregate can't push down
            if (aggOp.getAggregations().values().stream().anyMatch(c -> c.isDistinct() || c.isCountStar())) {
                return visit(optExpression, context);
            }

            // all constant can't push down
            if (!aggOp.getAggregations().isEmpty() &&
                    aggOp.getAggregations().values().stream().allMatch(ScalarOperator::isConstant)) {
                return visit(optExpression, context);
            }

            // no-group-by don't push down
            if (aggOp.getGroupingKeys().isEmpty()) {
                return visit(optExpression, context);
            }

            context = new AggregatePushDownContext();
            context.setAggregator(aggOp);
            return context;
        }

        private boolean canPushDownAgg(LogicalWindowOperator windowOp) {
            //TODO(by satanson): support AVG and COUNT in future
            Set<String> supportWindowFun = Sets.newHashSet(FunctionSet.SUM);
            AnalyticWindow window = windowOp.getAnalyticWindow();
            return window == null || window.getType().equals(AnalyticWindow.Type.RANGE) &&
                    windowOp.getWindowCall().values().stream()
                            .allMatch(call -> !call.isDistinct() && supportWindowFun.contains(call.getFnName()) &&
                                    call.getChild(0).isColumnRef());
        }

        @Override
        public AggregatePushDownContext visitLogicalWindow(OptExpression optExpression,
                                                           AggregatePushDownContext context) {
            if (canNotPushDown(optExpression, context)) {
                return visit(optExpression, context);
            }
            LogicalWindowOperator windowOp = optExpression.getOp().cast();
            // only support distinct aggregation push down window
            if (!context.origAggregator.getAggregations().isEmpty() || !canPushDownAgg(windowOp)) {
                return visit(optExpression, context);
            }

            List<ColumnRefOperator> additionalGroupBys = Lists.newArrayList();
            windowOp.getPartitionExpressions().stream()
                    .flatMap(e -> e.getUsedColumns().getStream().map(factory::getColumnRef)).forEach(
                            additionalGroupBys::add);

            windowOp.getWindowCall().keySet().forEach(context.groupBys::remove);
            windowOp.getOrderByElements().forEach(o -> additionalGroupBys.add(o.getColumnRef()));
            additionalGroupBys.forEach(c -> context.groupBys.put(c, c));
            context.aggregations.putAll(windowOp.getWindowCall());
            context.hasWindow = true;
            return context;
        }

        @Override
        public AggregatePushDownContext visitLogicalProject(OptExpression optExpression,
                                                            AggregatePushDownContext context) {
            if (canNotPushDown(optExpression, context)) {
                return visit(optExpression, context);
            }

            LogicalProjectOperator projectOp = optExpression.getOp().cast();

            if (projectOp.getColumnRefMap().entrySet().stream().allMatch(e -> e.getValue().equals(e.getKey()))) {
                return context;
            }

            // rewrite
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectOp.getColumnRefMap());
            context.aggregations.replaceAll((k, v) -> (CallOperator) rewriter.rewrite(v));
            context.groupBys.replaceAll((k, v) -> rewriter.rewrite(v));

            if (projectOp.getColumnRefMap().values().stream().allMatch(ScalarOperator::isColumnRef)) {
                return context;
            }

            if (!context.aggregations.isEmpty() &&
                    !context.aggregations.values().stream().allMatch(c -> c.getChildren().stream().allMatch(
                            ScalarOperator::isColumnRef))) {
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

            // add filter columns in groupBys, LogicalFilterOperator would not appear in distinct-agg + window
            // plan. In other agg push down scenarios, it may appear. When a AggregateOperator is pushed down below
            // LogicalFilterOperator, the predicates in latter will referenced the columns produced by the former.
            LogicalFilterOperator filter = optExpression.getOp().cast();
            filter.getRequiredChildInputColumns().getStream().map(factory::getColumnRef)
                    .forEach(v -> context.groupBys.put(v, v));
            return context;
        }

        @Override
        public AggregatePushDownContext visitLogicalTableScan(OptExpression optExpression,
                                                              AggregatePushDownContext context) {
            if (canNotPushDown(optExpression, context) || !context.hasWindow) {
                return visit(optExpression, context);
            }
            return context;
        }
    }

    // PostVisitor is used to rewrite the current node, the visit function must satisfy:
    // 1. return RewriteInfo.NOT_REWRITE to short-circuit the later bottom-up rewrite operation.
    // 2. put new-created OptExpression into RewriteInfo by invoke RewriteInfo.setOp if the new
    // OptExpression is created.
    // 3. return input RewriteInfo as return value if you want to rewrite upper nodes.
    private class PostVisitor extends OptExpressionVisitor<RewriteInfo, RewriteInfo> {

        // Default visit method do nothing but just pass the RewriteInfo to its parent
        @Override
        public RewriteInfo visit(OptExpression optExpression, RewriteInfo rewriteInfo) {
            if (rewriteInfo != RewriteInfo.NOT_REWRITE) {
                rewriteInfo.setOp(optExpression);
            }
            return rewriteInfo;
        }

        private CallOperator genAggregation(CallOperator origin) {
            ScalarOperator arg = origin.getChild(0);
            Function fn = Expr.getBuiltinFunction(origin.getFunction().getFunctionName().getFunction(),
                    new Type[] {arg.getType()}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            Preconditions.checkState(fn instanceof AggregateFunction);
            if (arg.getType().isDecimalOfAnyVersion()) {
                fn = DecimalV3FunctionAnalyzer.rectifyAggregationFunction((AggregateFunction) fn, arg.getType(),
                        origin.getType());
            }

            return new CallOperator(fn.getFunctionName().getFunction(), fn.getReturnType(),
                    Lists.newArrayList(arg.clone()), fn);
        }

        @Override
        public RewriteInfo visitLogicalTableScan(OptExpression optExpression, RewriteInfo rewriteInfo) {
            AggregatePushDownContext ctx = rewriteInfo.getCtx();
            if (ctx.groupBys.isEmpty() || ctx.aggregations.isEmpty()) {
                return RewriteInfo.NOT_REWRITE;
            }
            List<ColumnRefOperator> groupBys = ctx.groupBys.values().stream().map(ScalarOperator::getUsedColumns)
                    .flatMap(colSet -> colSet.getStream().map(factory::getColumnRef)).distinct()
                    .collect(Collectors.toList());

            LogicalScanOperator scanOp = optExpression.getOp().cast();
            ColumnRefSet scanOutputColRefSet = new ColumnRefSet(scanOp.getOutputColumns());
            Map<CallOperator, ColumnRefOperator> uniqueAggregations = Maps.newHashMap();
            Map<ColumnRefOperator, ColumnRefOperator> remapping = Maps.newHashMap();
            Preconditions.checkArgument(scanOutputColRefSet.containsAll(new ColumnRefSet(groupBys)));

            for (Map.Entry<ColumnRefOperator, CallOperator> entry : ctx.aggregations.entrySet()) {
                CallOperator aggCall = entry.getValue();
                Preconditions.checkArgument(aggCall.getChildren().size() == 1 && aggCall.getChild(0).isColumnRef());
                ColumnRefOperator oldRef = aggCall.getChild(0).cast();
                if (!uniqueAggregations.containsKey(aggCall)) {
                    CallOperator newAgg = genAggregation(aggCall);
                    ColumnRefOperator newColRef = factory.create(newAgg, newAgg.getType(), newAgg.isNullable());
                    uniqueAggregations.put(newAgg, newColRef);
                    remapping.put(oldRef, newColRef);
                } else {
                    remapping.put(oldRef, uniqueAggregations.get(aggCall));
                }
            }
            Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
            uniqueAggregations.forEach((k, v) -> newAggregations.put(v, k));
            LogicalAggregationOperator newAggOp =
                    LogicalAggregationOperator.builder().setAggregations(newAggregations).setType(AggType.GLOBAL)
                            .setGroupingKeys(groupBys).setPartitionByColumns(groupBys).build();
            OptExpression optAggOp = OptExpression.create(newAggOp, optExpression);
            return new RewriteInfo(true, new ColumnRefRemapping(remapping), optAggOp, ctx);
        }

        @Override
        public RewriteInfo visitLogicalProject(OptExpression optExpression, RewriteInfo rewriteInfo) {
            if (!rewriteInfo.getRemapping().isPresent()) {
                return RewriteInfo.NOT_REWRITE;
            }
            ReplaceColumnRefRewriter replacer = rewriteInfo.getRemapping().get().getReplacer();
            LogicalProjectOperator projectOp = optExpression.getOp().cast();
            ColumnRefSet columnRefSet = new ColumnRefSet();
            columnRefSet.union(projectOp.getColumnRefMap().keySet());
            columnRefSet.union(getReferencedColumnRef(projectOp.getColumnRefMap().values()));

            if (!rewriteInfo.getRemapping().get().getColumnRefSet().isIntersect(columnRefSet)) {
                return RewriteInfo.NOT_REWRITE;
            }

            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectOp.getColumnRefMap().entrySet()) {
                newColumnRefMap.put((ColumnRefOperator) replacer.rewrite(entry.getKey()),
                        replacer.rewrite(entry.getValue()));
            }
            projectOp.getColumnRefMap().clear();
            projectOp.getColumnRefMap().putAll(newColumnRefMap);
            rewriteInfo.setOp(optExpression);
            return rewriteInfo;
        }

        @Override
        public RewriteInfo visitLogicalWindow(OptExpression optExpression, RewriteInfo rewriteInfo) {
            LogicalWindowOperator windowOp = optExpression.getOp().cast();
            if (!rewriteInfo.getRemapping().isPresent()) {
                return RewriteInfo.NOT_REWRITE;
            }

            ReplaceColumnRefRewriter replacer = rewriteInfo.getRemapping().get().getReplacer();
            ColumnRefSet columnRefSet = getReferencedColumnRef(new ArrayList<>(windowOp.getWindowCall().values()));
            if (!rewriteInfo.getRemapping().get().getColumnRefSet().isIntersect(columnRefSet)) {
                return RewriteInfo.NOT_REWRITE;
            }
            Map<ColumnRefOperator, CallOperator> newWindowCall = Maps.newHashMap();
            windowOp.getWindowCall()
                    .forEach((key, value) -> newWindowCall.put(key, (CallOperator) replacer.rewrite(value)));
            LogicalWindowOperator newWindowOp =
                    new LogicalWindowOperator.Builder().withOperator(windowOp).setWindowCall(newWindowCall).build();
            rewriteInfo.setOp(OptExpression.create(newWindowOp, optExpression.getInputs()));
            return rewriteInfo;
        }

        @Override
        public RewriteInfo visitLogicalAggregate(OptExpression optExpression, RewriteInfo rewriteInfo) {
            return RewriteInfo.NOT_REWRITE;
        }
    }

    public PreVisitor preVisitor = new PreVisitor();
    public PostVisitor postVisitor = new PostVisitor();

    public static ColumnRefSet getReferencedColumnRef(Collection<ScalarOperator> operators) {
        ColumnRefSet refSet = new ColumnRefSet();
        operators.stream().map(ScalarOperator::getUsedColumns).forEach(refSet::union);
        return refSet;
    }

    // process each child to gather and merge RewriteInfo
    private RewriteInfo processChildren(OptExpression optExpression,
                                        AggregatePushDownContext context) {
        if (optExpression.getInputs().isEmpty()) {
            return new RewriteInfo(false, null, null, context);
        }

        List<RewriteInfo> childRewriteInfoList =
                optExpression.getInputs().stream().map(input -> process(input, context)).collect(
                        Collectors.toList());

        if (childRewriteInfoList.stream().noneMatch(RewriteInfo::hasRewritten)) {
            return RewriteInfo.NOT_REWRITE;
        }

        // merge ColumnRefMapping generated by each child into a total one
        Iterator<RewriteInfo> nextRewriteInfo = childRewriteInfoList.iterator();
        optExpression.getInputs().replaceAll(input -> nextRewriteInfo.next().getOp().orElse(input));
        ColumnRefRemapping combinedRemapping = new ColumnRefRemapping();
        childRewriteInfoList.forEach(rewriteInfo -> rewriteInfo.getRemapping().ifPresent(
                combinedRemapping::combine));
        return new RewriteInfo(true, combinedRemapping, optExpression, context);
    }

    // Check current opt to see whether push distinct agg down or not using PreVisitor
    private Optional<AggregatePushDownContext> processPre(OptExpression opt, AggregatePushDownContext context) {
        AggregatePushDownContext newContext = opt.getOp().accept(preVisitor, opt, context);
        // short-circuit if the returning context is EMPTY
        if (newContext == AggregatePushDownContext.EMPTY) {
            return Optional.empty();
        } else {
            return Optional.of(newContext);
        }
    }

    // Rewrite current opt according to rewriteInfo using PostVisitor, short-circuit if
    // rewriteInfo is RewriteInfo.NOT_REWRITE
    private RewriteInfo processPost(OptExpression opt, RewriteInfo rewriteInfo) {
        return opt.getOp().accept(postVisitor, opt, rewriteInfo);
    }

    // When rewrite a tree, we visit each node of this tree in top-down style, process function is
    // used to visit tree node, the visit operation is decomposed into three steps:
    // 1. processPre: check whether visiting should stop or not, collect info used to rewrite the node.
    // 2. processChildren: visit children of current node, merge infos from children into final one.
    // 3. processPost: rewrite current node.
    private RewriteInfo process(OptExpression opt, AggregatePushDownContext context) {
        return processPre(opt, context).map(ctx -> processPost(opt, processChildren(opt, ctx)))
                .orElse(RewriteInfo.NOT_REWRITE);
    }
}