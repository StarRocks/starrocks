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

package com.starrocks.sql.optimizer.rule.tree.pdaggb4mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.tree.pdaggb4mv.AggregatePushDownBeforeMVContext;
import com.starrocks.sql.optimizer.rule.tree.pdaggb4mv.PushDownAggregateBeforeMVCollector;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.collections4.MapUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * Rewrite and push down aggregation by Context.
 *
 * And in this phase, the AggregateContext use to record the temporary status in the
 * push down process, it's different with Collector
 *
 * AggregateContext's groupBys only record final group columns, the key/value always be columnRef
 * AggregateContext's aggregations only record final aggregation function, the key always be columnRef, and
 * the value always be an aggregate function
 *
 * And will insert new AggregateNode on scan node finally
 *
 * */
public class PushDownAggregateBeforeMVRewriter extends OptExpressionVisitor<OptExpression, AggregatePushDownBeforeMVContext> {
    private final ColumnRefFactory factory;
    private final PushDownAggregateBeforeMVCollector collector;
    private final SessionVariable sessionVariable;

    private Map<LogicalAggregationOperator, List<AggregatePushDownBeforeMVContext>> allRewriteContext;
    // record all push down column on scan node
    // for check the group bys which is generated in join node(on/where)
    private ColumnRefSet allPushDownGroupBys;

    public PushDownAggregateBeforeMVRewriter(TaskContext taskContext) {
        this.factory = taskContext.getOptimizerContext().getColumnRefFactory();
        this.collector = new PushDownAggregateBeforeMVCollector(taskContext);
        this.sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
    }

    public void collectRewriteContext(OptExpression root) {
        collector.collect(root);
        allRewriteContext = collector.getAllRewriteContext();
    }

    public boolean isNeedRewrite() {
        return MapUtils.isNotEmpty(allRewriteContext);
    }

    public OptExpression rewrite(OptExpression root) {
        if (!isNeedRewrite()) {
            return root;
        }
        allPushDownGroupBys = new ColumnRefSet();
        allRewriteContext.values().stream()
                .flatMap(Collection::stream)
                .map(c -> c.groupBys.values())
                .flatMap(Collection::stream)
                .map(ScalarOperator::getUsedColumns).forEach(allPushDownGroupBys::union);

        return root.getOp().accept(this, root, AggregatePushDownBeforeMVContext.EMPTY);
    }

    @Override
    public OptExpression visit(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        for (int i = 0; i < optExpression.getInputs().size(); i++) {
            optExpression.getInputs().set(i, process(optExpression.inputAt(i), AggregatePushDownBeforeMVContext.EMPTY));
        }
        return optExpression;
    }

    private OptExpression processChild(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        for (int i = 0; i < optExpression.getInputs().size(); i++) {
            optExpression.getInputs().set(i, process(optExpression.inputAt(i), context));
        }
        return optExpression;
    }

    private OptExpression process(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        return optExpression.getOp().accept(this, optExpression, context);
    }

    @Override
    public OptExpression visitLogicalFilter(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        LogicalFilterOperator filter = (LogicalFilterOperator) optExpression.getOp();
        filter.getRequiredChildInputColumns().getStream().map(factory::getColumnRef)
                .forEach(v -> context.groupBys.put(v, v));
        return processChild(optExpression, context);
    }

    @Override
    public OptExpression visitLogicalProject(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        LogicalProjectOperator project = (LogicalProjectOperator) optExpression.getOp();
        Map<ColumnRefOperator, ScalarOperator> originProjectMap = Maps.newHashMap(project.getColumnRefMap());

        if (!originProjectMap.values().stream().allMatch(ScalarOperator::isColumnRef)) {
            rewriteProject(context, originProjectMap);
        }

        context.aggregations.keySet().forEach(k -> originProjectMap.put(k, k));
        OptExpression newOpt = OptExpression.create(
                LogicalProjectOperator.builder().withOperator(project).setColumnRefMap(originProjectMap).build(),
                optExpression.getInputs());
        return processChild(newOpt, context);
    }

    // rewrite groupBys/aggregation by project expression, maybe needs push down
    // expression with aggregation or rewrite project expression
    private void rewriteProject(AggregatePushDownBeforeMVContext context,
                                Map<ColumnRefOperator, ScalarOperator> originProjectMap) {
        // rewrite group bys
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(originProjectMap);
        context.groupBys.replaceAll((k, v) -> rewriter.rewrite(v));
        ColumnRefSet refSet = new ColumnRefSet();
        context.groupBys.values().forEach(v -> refSet.union(v.getUsedColumns()));
        context.groupBys.clear();
        refSet.getStream().map(factory::getColumnRef).forEach(k -> context.groupBys.put(k, k));

        // rewrite aggregation & push down expression
        // special case-when/if only push down values
        List<ColumnRefOperator> keys = Lists.newArrayList(context.aggregations.keySet());
        for (ColumnRefOperator key : keys) {
            CallOperator aggFn = context.aggregations.get(key);
            ScalarOperator aggInput = aggFn.getChild(0);

            if (!(aggInput instanceof ColumnRefOperator)) {
                context.aggregations.put(key, (CallOperator) rewriter.rewrite(aggFn));
                continue;
            }

            ScalarOperator aggExpr = originProjectMap.get(aggInput);
            boolean isCaseWhen = aggExpr instanceof CaseWhenOperator;
            boolean isIfFn = aggExpr instanceof CallOperator
                    && ((CallOperator) aggExpr).getFunction() != null
                    && FunctionSet.IF.equals(((CallOperator) aggExpr).getFunction().getFunctionName().getFunction());

            if (isCaseWhen) {
                CaseWhenOperator caseWhen = (CaseWhenOperator) aggExpr;
                for (ScalarOperator condition : caseWhen.getAllConditionClause()) {
                    condition.getUsedColumns().getStream().map(factory::getColumnRef)
                            .forEach(v -> context.groupBys.put(v, v));
                }

                for (int i = 0; i < caseWhen.getWhenClauseSize(); i++) {
                    if (caseWhen.getThenClause(i).isConstant()) {
                        Preconditions.checkState(caseWhen.getThenClause(i).isConstantNull());
                        caseWhen.setThenClause(i, ConstantOperator.createNull(key.getType()));
                        continue;
                    }
                    ColumnRefOperator ref = replaceByNewAggregation(aggFn, caseWhen.getThenClause(i), context);
                    caseWhen.setThenClause(i, ref);
                }

                if (caseWhen.hasElse()) {
                    if (caseWhen.getElseClause().isConstant()) {
                        Preconditions.checkState(caseWhen.getElseClause().isConstantNull());
                        caseWhen.setElseClause(ConstantOperator.createNull(key.getType()));
                    } else {
                        ColumnRefOperator ref = replaceByNewAggregation(aggFn, caseWhen.getElseClause(), context);
                        caseWhen.setElseClause(ref);
                    }
                }

                key.setName(aggFn.getFnName().equals(FunctionSet.COUNT) ? FunctionSet.SUM : aggFn.getFnName());
                context.aggregations.remove(key);
                originProjectMap.put(key, new CaseWhenOperator(key.getType(), caseWhen));
            } else if (isIfFn) {
                CallOperator ifFn = (CallOperator) aggExpr;
                ifFn.getChild(0).getUsedColumns().getStream().map(factory::getColumnRef)
                        .forEach(v -> context.groupBys.put(v, v));

                for (int i = 1; i < ifFn.getChildren().size(); i++) {
                    if (ifFn.getChild(i).isConstant()) {
                        Preconditions.checkState(ifFn.getChild(i).isConstantNull());
                        ifFn.setChild(i, ConstantOperator.createNull(key.getType()));
                        continue;
                    }
                    ColumnRefOperator ref = replaceByNewAggregation(aggFn, ifFn.getChild(i), context);
                    ifFn.setChild(i, ref);
                }

                key.setName(aggFn.getFnName().equals(FunctionSet.COUNT) ? FunctionSet.SUM : aggFn.getFnName());
                context.aggregations.remove(key);
                originProjectMap.put(key,
                        new CallOperator(ifFn.getFnName(), key.getType(), ifFn.getChildren(), ifFn.getFunction()));
            } else {
                context.aggregations.put(key, (CallOperator) rewriter.rewrite(aggFn));
            }
        }
    }

    private ColumnRefOperator replaceByNewAggregation(CallOperator originAggFn, ScalarOperator input,
                                                      AggregatePushDownBeforeMVContext context) {
        CallOperator newAgg = genAggregation(originAggFn, input, false);
        ColumnRefOperator ref;
        if (context.aggregations.containsValue(newAgg)) {
            ref = context.aggregations.entrySet().stream().filter(e -> e.getValue().equals(newAgg))
                    .findFirst().map(Map.Entry::getKey).orElseThrow(IllegalArgumentException::new);
        } else {
            ref = factory.create(newAgg, newAgg.getType(), newAgg.isNullable());
        }
        context.aggregations.put(ref, newAgg);
        return ref;
    }

    @Override
    public OptExpression visitLogicalAggregate(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) optExpression.getOp();
        if (!allRewriteContext.containsKey(aggregate)) {
            return visit(optExpression, context);
        }

        List<AggregatePushDownBeforeMVContext> allRewrite = allRewriteContext.get(aggregate);
        // rewrite
        AggregatePushDownBeforeMVContext childContext = new AggregatePushDownBeforeMVContext();
        childContext.origAggregator = aggregate;

        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap(aggregate.getAggregations());

        // flat aggregate
        List<ColumnRefOperator> allAggregateRefs = allRewrite.stream()
                .map(a -> a.aggregations.keySet())
                .flatMap(Collection::stream)
                .distinct().collect(Collectors.toList());

        // rewrite origin aggregation
        for (ColumnRefOperator ref : allAggregateRefs) {
            CallOperator call = aggregate.getAggregations().get(ref);
            ColumnRefOperator newRef = factory.create(call.getFnName(), call.getType(), call.isNullable());
            childContext.aggregations.put(newRef, call);

            CallOperator newCall = genAggregation(call, newRef, true);
            ref.setName(call.getFnName().equals(FunctionSet.COUNT) ? FunctionSet.SUM : call.getFnName());
            newAggregations.put(ref, newCall);
        }

        // group by
        allRewrite.stream()
                .map(a -> a.groupBys.keySet())
                .flatMap(Collection::stream)
                .filter(c -> aggregate.getGroupingKeys().contains(c))
                .distinct().forEach(c -> childContext.groupBys.put(c, c));

        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder().withOperator(aggregate)
                .setAggregations(newAggregations).build();
        optExpression = OptExpression.create(newAgg, optExpression.getInputs());
        return processChild(optExpression, childContext);
    }

    private CallOperator genAggregation(CallOperator origin, ScalarOperator args, boolean enableTransform) {
        String realFunction = origin.getFunction().getFunctionName().getFunction();
        if(enableTransform && origin.getFunction().getFunctionName().getFunction().equals(FunctionSet.COUNT)) {
            realFunction = FunctionSet.SUM;
        }

        Function fn = Expr.getBuiltinFunction(realFunction,
                new Type[] {args.getType()}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        Preconditions.checkState(fn instanceof AggregateFunction);
        if (args.getType().isDecimalOfAnyVersion()) {
            fn = DecimalV3FunctionAnalyzer.rectifyAggregationFunction((AggregateFunction) fn, args.getType(),
                    origin.getType());
        }

        return new CallOperator(fn.getFunctionName().getFunction(), fn.getReturnType(),
                Lists.newArrayList(args), fn);
    }

    @Override
    public OptExpression visitLogicalJoin(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }
        // push down aggregate
        optExpression.getInputs().set(0, pushDownAggregateAfterJoin(optExpression, context, 0));
        optExpression.getInputs().set(1, pushDownAggregateAfterJoin(optExpression, context, 1));
        return optExpression;
    }

    private OptExpression pushDownAggregateAfterJoin(OptExpression joinOpt, AggregatePushDownBeforeMVContext context, int child) {
        LogicalJoinOperator join = (LogicalJoinOperator) joinOpt.getOp();
        ColumnRefSet childOutput = joinOpt.inputAt(child).getOutputColumns();

        ColumnRefSet aggregationsRefs = new ColumnRefSet();
        context.aggregations.values().stream().map(CallOperator::getUsedColumns).forEach(aggregationsRefs::union);

        if (!childOutput.containsAll(aggregationsRefs)) {
            return process(joinOpt.inputAt(child), AggregatePushDownBeforeMVContext.EMPTY);
        }

        AggregatePushDownBeforeMVContext childContext = new AggregatePushDownBeforeMVContext();
        childContext.aggregations.putAll(context.aggregations);

        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.groupBys.entrySet()) {
            if (childOutput.containsAll(entry.getValue().getUsedColumns())) {
                childContext.groupBys.put(entry.getKey(), entry.getValue());
            } else if(childContext.aggregations.values().stream().anyMatch(CallOperator::isCountStar)) {
                // forbidden push down when child don't contain all group by columns
                return process(joinOpt.inputAt(child), AggregatePushDownBeforeMVContext.EMPTY);
            }
        }

        childContext.origAggregator = context.origAggregator;

        if (join.getOnPredicate() != null) {
            join.getOnPredicate().getUsedColumns().getStream().filter(childOutput::contains)
                    .map(factory::getColumnRef).forEach(c -> childContext.groupBys.put(c, c));
        }

        if (join.getPredicate() != null) {
            join.getPredicate().getUsedColumns().getStream().filter(childOutput::contains)
                    .map(factory::getColumnRef).forEach(v -> childContext.groupBys.put(v, v));
        }

        OptExpression result = joinOpt.inputAt(child);

        // add a aggregation node
        if (childContext.aggregations.values().stream().filter(c -> !c.isCountStar()).map(c -> c.getChild(0)).anyMatch(s -> !s.isColumnRef())) {
            Map<ColumnRefOperator, ScalarOperator> refs = Maps.newHashMap();
            joinOpt.inputAt(child).getOutputColumns().getColumnRefOperators(factory)
                    .forEach(c -> refs.put(c, c));

            for (Map.Entry<ColumnRefOperator, CallOperator> entry : childContext.aggregations.entrySet()) {
                if(entry.getValue().isCountStar()) continue;
                ScalarOperator input = entry.getValue().getChild(0);
                if (!input.isColumnRef()) {
                    ColumnRefOperator ref = factory.create(input, input.getType(), input.isNullable());
                    refs.put(ref, input);
                    entry.getValue().setChild(0, ref);
                }
            }
            result = OptExpression.create(new LogicalProjectOperator(refs), result);
        }


        LogicalAggregationOperator aggregate;
        List<ColumnRefOperator> groupBys = Lists.newArrayList(childContext.groupBys.keySet());
        if ("local".equalsIgnoreCase(sessionVariable.getCboPushDownAggregate()) || ("auto".equalsIgnoreCase(sessionVariable.getCboPushDownAggregate()) && groupBys.size() <= 1)) {

            // local && un-split
            aggregate = new LogicalAggregationOperator(AggType.LOCAL, groupBys, childContext.aggregations);
            aggregate.setOnlyLocalAggregate();
        } else {
            aggregate = new LogicalAggregationOperator(AggType.GLOBAL, groupBys, childContext.aggregations);
        }

        // don't process children
        return OptExpression.create(aggregate, result);
    }

    @Override
    public OptExpression visitLogicalCTEAnchor(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        optExpression.setChild(0, process(optExpression.inputAt(0), AggregatePushDownBeforeMVContext.EMPTY));
        optExpression.setChild(1, process(optExpression.inputAt(1), context));
        return optExpression;
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        // do nothing
        return visit(optExpression, context);
    }

    @Override
    public OptExpression visitLogicalUnion(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        // replace (union and children)'s output column
        LogicalUnionOperator union = (LogicalUnionOperator) optExpression.getOp();
        List<AggregatePushDownBeforeMVContext> childContexts = Lists.newArrayList();
        for (int i = 0; i < optExpression.getInputs().size(); i++) {
            List<ColumnRefOperator> childOutput = union.getChildOutputColumns().get(i);
            Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
            Preconditions.checkState(childOutput.size() == union.getOutputColumnRefOp().size());
            for (int k = 0; k < union.getOutputColumnRefOp().size(); k++) {
                rewriteMap.put(union.getOutputColumnRefOp().get(k), childOutput.get(k));
            }

            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
            AggregatePushDownBeforeMVContext childContext = new AggregatePushDownBeforeMVContext();
            childContext.origAggregator = context.origAggregator;
            childContext.aggregations.putAll(context.aggregations);
            childContext.aggregations.replaceAll((k, v) -> (CallOperator) rewriter.rewrite(v));

            context.groupBys.values().stream()
                    .map(rewriter::rewrite)
                    .map(ScalarOperator::getUsedColumns)
                    .forEach(c -> c.getStream().map(factory::getColumnRef)
                            .forEach(ref -> childContext.groupBys.put(ref, ref)));
            childContexts.add(childContext);
        }

        List<List<ColumnRefOperator>> newChildOutputs = Lists.newArrayList();
        List<ColumnRefOperator> newUnionOutput = Lists.newArrayList(union.getOutputColumnRefOp());
        union.getChildOutputColumns().forEach(c -> newChildOutputs.add(Lists.newArrayList(c)));

        List<ColumnRefOperator> keys = Lists.newArrayList(context.aggregations.keySet());
        for (ColumnRefOperator key : keys) {
            newUnionOutput.add(key);

            for (int i = 0; i < optExpression.getInputs().size(); i++) {
                ColumnRefOperator childRef = factory.create(key, key.getType(), key.isNullable());
                newChildOutputs.get(i).add(childRef);
                childContexts.get(i).aggregations.put(childRef, childContexts.get(i).aggregations.get(key));
                childContexts.get(i).aggregations.remove(key);
            }
        }

        for (int i = 0; i < optExpression.getInputs().size(); i++) {
            optExpression.setChild(i, process(optExpression.inputAt(i), childContexts.get(i)));
        }

        return OptExpression.create(LogicalUnionOperator.builder().withOperator(union)
                        .setOutputColumnRefOp(newUnionOutput)
                        .setChildOutputColumns(newChildOutputs).build(),
                optExpression.getInputs());
    }

    private boolean isInvalid(OptExpression optExpression, AggregatePushDownBeforeMVContext context) {
        return context.isEmpty() || optExpression.getOp().hasLimit();
    }
}
