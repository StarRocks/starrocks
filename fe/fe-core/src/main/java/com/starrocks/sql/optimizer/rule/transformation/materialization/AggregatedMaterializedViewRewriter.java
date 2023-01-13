// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * SPJG materialized view rewriter, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 *  This rewriter is for aggregated query rewrite
 */
public class AggregatedMaterializedViewRewriter extends MaterializedViewRewriter {
    private static final Logger LOG = LogManager.getLogger(AggregatedMaterializedViewRewriter.class);

    private static Map<String, String> ROLLUP_FUNCTION_MAP = ImmutableMap.<String, String>builder()
            .put(FunctionSet.COUNT, FunctionSet.SUM)
            .build();

    public AggregatedMaterializedViewRewriter(MaterializationContext materializationContext) {
        super(materializationContext);
    }

    @Override
    public boolean isValidPlan(OptExpression expression) {
        // TODO: to support grouping set/rollup/cube
        return MvUtils.isLogicalSPJG(expression);
    }

    @Override
    protected OptExpression viewBasedRewrite(RewriteContext rewriteContext, OptExpression targetExpr) {
        LogicalAggregationOperator mvAgg = (LogicalAggregationOperator) rewriteContext.getMvExpression().getOp();
        List<ScalarOperator> swappedMvKeys = Lists.newArrayList();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (ColumnRefOperator key : mvAgg.getGroupingKeys()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(key.clone());
            ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
            swappedMvKeys.add(swapped);
        }

        LogicalAggregationOperator queryAgg = (LogicalAggregationOperator) rewriteContext.getQueryExpression().getOp();
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        List<ScalarOperator> queryGroupingKeys = Lists.newArrayList();
        for (ColumnRefOperator key : originalGroupKeys) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(key.clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            queryGroupingKeys.add(swapped);
        }

        List<ScalarOperator> distinctMvKeys = swappedMvKeys.stream().distinct().collect(Collectors.toList());
        GroupKeyChecker groupKeyChecker = new GroupKeyChecker(distinctMvKeys);
        boolean keyMatched = groupKeyChecker.check(queryGroupingKeys);
        if (!keyMatched) {
            return null;
        }

        // check aggregates of query
        // normalize mv's aggs by using query's table ref and query ec
        Map<ColumnRefOperator, ScalarOperator> mvProjection =
                MvUtils.getColumnRefMap(rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());

        List<ScalarOperator> swappedMvAggs = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : mvProjection.entrySet()) {
            if (mvAgg.getGroupingKeys().contains(entry.getKey())) {
                continue;
            }
            // here must be aggreate values
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(entry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
            swappedMvAggs.add(swapped);
        }

        Map<ColumnRefOperator, ScalarOperator> queryProjection =
                MvUtils.getColumnRefMap(rewriteContext.getQueryExpression(), rewriteContext.getQueryRefFactory());
        Map<ColumnRefOperator, ScalarOperator> queryAggregations = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryProjection.entrySet()) {
            if (queryAgg.getGroupingKeys().contains(entry.getKey())) {
                continue;
            }
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(entry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            queryAggregations.put(entry.getKey(), swapped);
        }

        AggregateRewriteChecker aggregateRewriteChecker = new AggregateRewriteChecker(swappedMvAggs);
        boolean aggMatched = aggregateRewriteChecker.check(queryAggregations.values().stream().collect(Collectors.toList()));
        if (!aggMatched) {
            return null;
        }

        // normalize view projection by query relation and ec
        Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap =
                normalizeAndReverseProjection(mvProjection, rewriteContext, false);
        boolean isRollup = groupKeyChecker.isRollup();
        if (isRollup && aggregateRewriteChecker.hasDistinct()) {
            return null;
        }
        if (isRollup && mvAgg.getProjection() != null) {
            // rollup on projection is not supported
            // a, b, sum(c) + 1 can not be aggregated further
            return null;
        }
        if (isRollup) {
            // the following situations are not supported
            // 1. can not support rollup of disctinct agg
            // 2. rollup on projection is not supported, eg: a, b, sum(c) + 1 can not be aggregated further
            if (aggregateRewriteChecker.hasDistinct() || mvAgg.getProjection() != null) {
                return null;
            }
            return rewriteForRollup(queryAgg, queryAggregations,
                    queryGroupingKeys, normalizedViewMap, rewriteContext, targetExpr);
        } else {
            return rewriteProjection(rewriteContext, normalizedViewMap, targetExpr);
        }
    }

    private OptExpression rewriteForRollup(
            LogicalAggregationOperator queryAgg,
            Map<ColumnRefOperator, ScalarOperator> queryAggregation,
            List<ScalarOperator> swappedQueryGroupingKeys,
            Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
            RewriteContext rewriteContext,
            OptExpression targetExpr) {
        // generate group keys against scan mv plan
        List<ColumnRefOperator> newGroupKeys = rewriteGroupKeys(
                swappedQueryGroupingKeys, normalizedViewMap, rewriteContext.getOutputMapping(),
                new ColumnRefSet(rewriteContext.getQueryColumnSet()));
        if (newGroupKeys == null) {
            return null;
        }
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        Preconditions.checkState(originalGroupKeys.size() == newGroupKeys.size());
        Map<ColumnRefOperator, ScalarOperator> aggregateMapping = Maps.newHashMap();
        for (int i = 0; i < originalGroupKeys.size(); i++) {
            aggregateMapping.put(originalGroupKeys.get(i), newGroupKeys.get(i));
        }

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : queryAgg.getAggregations().entrySet()) {
            // create new column ref for aggregations
            ColumnRefOperator mapped = rewriteContext.getQueryRefFactory().create(
                    entry.getKey(), entry.getKey().getType(), entry.getKey().isNullable());
            aggregateMapping.put(entry.getKey(), mapped);
        }

        // generate new agg exprs(rollup functions)
        Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregates(
                queryAggregation, normalizedViewMap, rewriteContext.getOutputMapping(),
                new ColumnRefSet(rewriteContext.getQueryColumnSet()), aggregateMapping);
        if (newAggregations == null) {
            return null;
        }
        return createNewAggregate(queryAgg, newAggregations, aggregateMapping, targetExpr);
    }

    @Override
    protected OptExpression queryBasedRewrite(RewriteContext rewriteContext, ScalarOperator compensationPredicates,
                                              OptExpression queryExpression) {
        // query predicate and (not viewToQueryCompensationPredicate) is the final query compensation predicate
        ScalarOperator queryCompensationPredicate = MvUtils.canonizePredicate(
                Utils.compoundAnd(
                        rewriteContext.getQueryPredicateSplit().toScalarOperator(),
                        CompoundPredicateOperator.not(compensationPredicates)));
        // add filter above input and put filter under aggExpr
        OptExpression input = queryExpression.inputAt(0);
        if (!ConstantOperator.TRUE.equals(queryCompensationPredicate)) {
            // add filter
            Operator.Builder builder = OperatorBuilderFactory.build(input.getOp());
            builder.withOperator(input.getOp());
            builder.setPredicate(queryCompensationPredicate);
            Operator newInputOp = builder.build();
            OptExpression newInputExpr = OptExpression.create(newInputOp, input.getInputs());
            // create new OptExpression to strip GroupExpression
            return OptExpression.create(queryExpression.getOp(), newInputExpr);
        }
        return null;
    }

    @Override
    protected OptExpression createUnion(OptExpression queryInput, OptExpression viewInput, RewriteContext rewriteContext) {
        Map<ColumnRefOperator, ScalarOperator> queryColumnRefMap =
                MvUtils.getColumnRefMap(queryInput, rewriteContext.getQueryRefFactory());
        // keys of queryColumnRefMap and mvColumnRefMap are the same
        List<ColumnRefOperator> originalOutputColumns = queryColumnRefMap.keySet().stream().collect(Collectors.toList());
        // rewrite query
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(materializationContext);
        OptExpression newQueryInput = duplicator.duplicate(queryInput);
        List<ColumnRefOperator> newQueryOutputColumns = duplicator.getMappedColumns(originalOutputColumns);

        Preconditions.checkState(viewInput.getOp().getProjection() != null);
        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> mvProjection = viewInput.getOp().getProjection().getColumnRefMap();
        List<ColumnRefOperator> newViewOutputColumns = Lists.newArrayList();
        for (ColumnRefOperator columnRef : originalOutputColumns) {
            ColumnRefOperator newColumn = rewriteContext.getQueryRefFactory().create(
                    columnRef, columnRef.getType(), columnRef.isNullable());
            newViewOutputColumns.add(newColumn);
            newColumnRefMap.put(newColumn, mvProjection.get(columnRef));
        }
        Projection newMvProjection = new Projection(newColumnRefMap);
        viewInput.getOp().setProjection(newMvProjection);
        // reset the logical property, should be derived later again because output columns changed
        viewInput.setLogicalProperty(null);

        List<ColumnRefOperator> unionOutputColumns = originalOutputColumns.stream()
                .map(c -> rewriteContext.getQueryRefFactory().create(c, c.getType(), c.isNullable()))
                .collect(Collectors.toList());
        LogicalUnionOperator unionOperator = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(unionOutputColumns)
                .setChildOutputColumns(Lists.newArrayList(newQueryOutputColumns, newViewOutputColumns))
                .isUnionAll(true)
                .build();
        OptExpression unionExpr = OptExpression.create(unionOperator, newQueryInput, viewInput);

        // construct Aggregate on unionExpr with rollup aggregate functions
        Map<ColumnRefOperator, ColumnRefOperator> columnMapping = Maps.newHashMap();
        for (int i = 0; i < originalOutputColumns.size(); i++) {
            columnMapping.put(originalOutputColumns.get(i), unionOutputColumns.get(i));
        }

        LogicalAggregationOperator queryAgg = (LogicalAggregationOperator) rewriteContext.getQueryExpression().getOp();
        if (queryAgg.getProjection() != null) {
            // if query has projection, is not supported now
            return null;
        }
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        Map<ColumnRefOperator, ScalarOperator> aggregateMapping = Maps.newHashMap();
        for (ColumnRefOperator groupKey : originalGroupKeys) {
            aggregateMapping.put(groupKey, columnMapping.get(groupKey));
        }

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : queryAgg.getAggregations().entrySet()) {
            // create new column ref for aggregation's keys
            ColumnRefOperator mapped = rewriteContext.getQueryRefFactory().create(
                    entry.getKey(), entry.getKey().getType(), entry.getKey().isNullable());
            aggregateMapping.put(entry.getKey(), mapped);
            // aggregateMapping.put(entry.getKey(), columnMapping.get(entry.getKey()));
        }

        Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregatesForUnion(
                queryAgg.getAggregations(), columnMapping, aggregateMapping);
        if (newAggregations == null) {
            return null;
        }
        return createNewAggregate(queryAgg, newAggregations, aggregateMapping, unionExpr);
    }

    private OptExpression createNewAggregate(LogicalAggregationOperator queryAgg,
                                             Map<ColumnRefOperator, CallOperator> newAggregations,
                                             Map<ColumnRefOperator, ScalarOperator> aggregateMapping,
                                             OptExpression inputExpr) {
        // newGroupKeys may have duplicate because of EquivalenceClasses
        // remove duplicate here as new grouping keys
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        List<ColumnRefOperator> newGroupingKeys = originalGroupKeys.stream()
                .map(key -> (ColumnRefOperator) aggregateMapping.get(key)).collect(Collectors.toList());
        List<ColumnRefOperator> finalGroupKeys = newGroupingKeys.stream().distinct().collect(Collectors.toList());
        LogicalAggregationOperator.Builder aggBuilder = new LogicalAggregationOperator.Builder();
        aggBuilder.withOperator(queryAgg);
        aggBuilder.setGroupingKeys(finalGroupKeys);
        // can not be distinct agg here, so partitionByColumns is the same as groupingKeys
        aggBuilder.setPartitionByColumns(finalGroupKeys);
        aggBuilder.setAggregations(newAggregations);
        aggBuilder.setPredicate(queryAgg.getPredicate());
        // add projection to make sure that the output columns keep the same with the origin query
        Map<ColumnRefOperator, ScalarOperator> newProjection = Maps.newHashMap();
        if (queryAgg.getProjection() == null) {
            for (int i = 0; i < originalGroupKeys.size(); i++) {
                newProjection.put(originalGroupKeys.get(i), newGroupingKeys.get(i));
            }
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : queryAgg.getAggregations().entrySet()) {
                newProjection.put(entry.getKey(), aggregateMapping.get(entry.getKey()));
            }
        } else {
            Map<ColumnRefOperator, ScalarOperator> originalMap = queryAgg.getProjection().getColumnRefMap();
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(aggregateMapping);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : originalMap.entrySet()) {
                if (queryAgg.getGroupingKeys().contains(entry.getValue())) {
                    ScalarOperator rewritten = rewriter.rewrite(entry.getValue());
                    newProjection.put(entry.getKey(), rewritten);
                } else {
                    // Aggregates map's keys are changed, so here entry values should be rewritten by aggregateMapping
                    ScalarOperator newValue = rewriter.rewrite(entry.getValue());
                    newProjection.put(entry.getKey(), newValue);
                }
            }
        }
        Projection projection = new Projection(newProjection);
        aggBuilder.setProjection(projection);
        LogicalAggregationOperator newAggOp = aggBuilder.build();
        return OptExpression.create(newAggOp, inputExpr);
    }

    private List<ColumnRefOperator> rewriteGroupKeys(List<ScalarOperator> groupKeys,
                                                     Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
                                                     Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                     ColumnRefSet queryColumnSet) {
        List<ColumnRefOperator> rewrittens = Lists.newArrayList();
        for (ScalarOperator key : groupKeys) {
            ScalarOperator targetColumn = replaceExprWithTarget(key, normalizedViewMap, mapping);
            if (!isAllExprReplaced(targetColumn, queryColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                return null;
            }
            rewrittens.add((ColumnRefOperator) targetColumn);
        }
        return rewrittens;
    }

    private Map<ColumnRefOperator, CallOperator> rewriteAggregates(Map<ColumnRefOperator, ScalarOperator> aggregates,
                                                                   Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
                                                                   Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                                   ColumnRefSet queryColumnSet,
                                                                   Map<ColumnRefOperator, ScalarOperator> aggregateMapping) {
        Map<ColumnRefOperator, CallOperator> rewrittens = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : aggregates.entrySet()) {
            Preconditions.checkState(entry.getValue() instanceof CallOperator);
            CallOperator aggCall = (CallOperator) entry.getValue();
            ScalarOperator targetColumn = replaceExprWithTarget(aggCall, normalizedViewMap, mapping);
            if (!isAllExprReplaced(targetColumn, queryColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                return null;
            }
            // Aggregate must be CallOperator
            Preconditions.checkState(targetColumn instanceof ColumnRefOperator);
            CallOperator newAggregate = getRollupAggregate(aggCall, (ColumnRefOperator) targetColumn);
            if (newAggregate == null) {
                return null;
            }
            rewrittens.put((ColumnRefOperator) aggregateMapping.get(entry.getKey()), newAggregate);
        }

        return rewrittens;
    }

    private Map<ColumnRefOperator, CallOperator> rewriteAggregatesForUnion(
            Map<ColumnRefOperator, CallOperator> aggregates,
            Map<ColumnRefOperator, ColumnRefOperator> mapping,
            Map<ColumnRefOperator, ScalarOperator> aggregateMapping) {
        Map<ColumnRefOperator, CallOperator> rewrittens = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregates.entrySet()) {
            Preconditions.checkState(entry.getValue() instanceof CallOperator);
            CallOperator aggCall = entry.getValue();
            ColumnRefOperator targetColumn = mapping.get(entry.getKey());
            if (targetColumn == null) {
                return null;
            }
            // Aggregate must be CallOperator
            CallOperator newAggregate = getRollupAggregate(aggCall, targetColumn);
            if (newAggregate == null) {
                return null;
            }
            rewrittens.put((ColumnRefOperator) aggregateMapping.get(entry.getKey()), newAggregate);
        }

        return rewrittens;
    }

    // generate new aggregates for rollup
    // eg: count(col) -> sum(col)
    private CallOperator getRollupAggregate(CallOperator aggCall, ColumnRefOperator targetColumn) {
        if (ROLLUP_FUNCTION_MAP.containsKey(aggCall.getFnName())) {
            if (aggCall.getFnName().equals(FunctionSet.COUNT)) {
                Type[] argTypes = {targetColumn.getType()};
                Function sumFn = findArithmeticFunction(argTypes, FunctionSet.SUM);
                return new CallOperator(FunctionSet.SUM, aggCall.getFunction().getReturnType(),
                        Lists.newArrayList(targetColumn), sumFn);
            } else {
                // impossible to reach here
                LOG.warn("unsupported rollup function:{}", aggCall.getFnName());
                return null;
            }
        } else {
            // the rollup function is the same as origin, but use the new column as argument
            CallOperator newAggCall = (CallOperator) aggCall.clone();
            newAggCall.setChild(0, targetColumn);
            return newAggCall;
        }
    }

    private Function findArithmeticFunction(Type[] argsType, String fnName) {
        return Expr.getBuiltinFunction(fnName, argsType, Function.CompareMode.IS_IDENTICAL);
    }
}
