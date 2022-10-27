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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
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
        if (expression == null) {
            return false;
        }
        Operator op = expression.getOp();
        if (!(op instanceof LogicalAggregationOperator)) {
            return false;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) op;
        if (!agg.getType().equals(AggType.GLOBAL)) {
            return false;
        }
        // TODO: to support grouping set/rollup/cube
        return RewriteUtils.isLogicalSPJ(expression.inputAt(0));
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
            ScalarOperator normalized = columnRewriter.rewriteByQueryEc(rewritten);
            queryGroupingKeys.add(normalized);
        }

        List<ScalarOperator> distinctMvKeys = swappedMvKeys.stream().distinct().collect(Collectors.toList());
        GroupKeyChecker groupKeyChecker = new GroupKeyChecker(distinctMvKeys);
        boolean keyMatched = groupKeyChecker.check(queryGroupingKeys);
        if (!keyMatched) {
            return null;
        }

        // check aggregates of query
        // normalize mv's aggs by using query's table ref and query ec
        List<ScalarOperator> swappedMvAggs = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggEntry : mvAgg.getAggregations().entrySet()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(aggEntry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
            swappedMvAggs.add(swapped);
        }

        Map<ColumnRefOperator, ScalarOperator> queryAggs = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggEntry : queryAgg.getAggregations().entrySet()) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(aggEntry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            queryAggs.put(aggEntry.getKey(), swapped);
        }

        AggregateChecker aggregateChecker = new AggregateChecker(swappedMvAggs);
        boolean aggMatched = aggregateChecker.check(queryAggs.values().stream().collect(Collectors.toList()));
        if (!aggMatched) {
            return null;
        }
        Map<ColumnRefOperator, ScalarOperator> mvProjection = getProjectionMap(rewriteContext.getMvProjection(),
                rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());
        // normalize view projection by query relation and ec
        Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap =
                normalizeAndReverseProjection(mvProjection, rewriteContext, false);
        boolean isRollup = groupKeyChecker.isRollup();
        if (isRollup) {
            if (aggregateChecker.hasDistinct()) {
                // can not support rollup of disctinct agg
                return null;
            }
            // generate group keys against scan mv plan
            List<ColumnRefOperator> newGroupKeys = rewriteGroupKeys(
                    queryGroupingKeys, normalizedViewMap, rewriteContext.getOutputMapping(),
                    rewriteContext.getQueryColumnSet());
            if (newGroupKeys == null) {
                return null;
            }
            Preconditions.checkState(originalGroupKeys.size() == newGroupKeys.size());

            // generate new agg exprs(rollup functions)
            Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregates(
                    queryAggs, normalizedViewMap, rewriteContext.getOutputMapping(),
                    rewriteContext.getQueryColumnSet());
            if (newAggregations == null) {
                return null;
            }
            // newGroupKeys may have duplicate because of EquivalenceClasses
            // remove duplicate here as new grouping keys
            List<ColumnRefOperator> finalGroupKeys = newGroupKeys.stream().distinct().collect(Collectors.toList());

            LogicalAggregationOperator.Builder aggBuilder = new LogicalAggregationOperator.Builder();
            aggBuilder.withOperator(queryAgg);
            aggBuilder.setGroupingKeys(finalGroupKeys);
            // can not be distinct agg here, so partitionByColumns is the same as groupingKeys
            aggBuilder.setPartitionByColumns(finalGroupKeys);
            aggBuilder.setAggregations(newAggregations);
            aggBuilder.setProjection(queryAgg.getProjection());
            aggBuilder.setPredicate(queryAgg.getPredicate());

            // add projection for group keys
            // aggregates are already mapped, so here just process group keys
            Map<ColumnRefOperator, ScalarOperator> newProjection = Maps.newHashMap();
            if (queryAgg.getProjection() == null) {
                for (int i = 0; i < originalGroupKeys.size(); i++) {
                    newProjection.put(originalGroupKeys.get(i), newGroupKeys.get(i));
                }
                newProjection.putAll(newAggregations);
            } else {
                Map<ColumnRefOperator, ScalarOperator> originalMap = queryAgg.getProjection().getColumnRefMap();
                Map<ColumnRefOperator, ScalarOperator> groupKeyMap = Maps.newHashMap();
                for (int i = 0; i < originalGroupKeys.size(); i++) {
                    groupKeyMap.put(originalGroupKeys.get(i), newGroupKeys.get(i));
                }
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(groupKeyMap);
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : originalMap.entrySet()) {
                    if (groupKeyMap.containsKey(entry.getValue())) {
                        ScalarOperator rewritten = rewriter.rewrite(entry.getValue());
                        newProjection.put(entry.getKey(), rewritten);
                    } else {
                        newProjection.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            Projection projection = new Projection(newProjection);
            aggBuilder.setProjection(projection);
            LogicalAggregationOperator newAggOp = aggBuilder.build();
            OptExpression aggExpr = OptExpression.create(newAggOp, targetExpr);
            return aggExpr;
        } else {
            return rewriteProjection(rewriteContext, normalizedViewMap, targetExpr);
        }
    }

    @Override
    protected OptExpression queryBasedRewrite(RewriteContext rewriteContext, PredicateSplit compensationPredicates,
                                              OptExpression queryExpression) {
        OptExpression aggExpr = queryExpression;
        // add filter above input and put filter under aggExpr
        OptExpression input = queryExpression.inputAt(0);

        ScalarOperator equalPredicates = RewriteUtils.canonizeNode(compensationPredicates.getEqualPredicates());
        ScalarOperator otherPredicates = RewriteUtils.canonizeNode(Utils.compoundAnd(
                compensationPredicates.getRangePredicates(), compensationPredicates.getResidualPredicates()));
        if (!RewriteUtils.isAlwaysTrue(equalPredicates) || !RewriteUtils.isAlwaysTrue(otherPredicates)) {
            Map<ColumnRefOperator, ScalarOperator> queryExprMap = getProjectionMap(null,
                    queryExpression, rewriteContext.getQueryRefFactory());

            if (!RewriteUtils.isAlwaysTrue(equalPredicates)) {
                Multimap<ScalarOperator, ColumnRefOperator> normalizedMap =
                        normalizeAndReverseProjection(queryExprMap, rewriteContext, false);
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(equalPredicates);
                // swapped by query based view ec
                List<ScalarOperator> swappedConjuncts = conjuncts.stream().map(conjunct -> {
                    ColumnRewriter rewriter = new ColumnRewriter(rewriteContext);
                    return rewriter.rewriteByViewEc(conjunct);
                }).collect(Collectors.toList());
                List<ScalarOperator> rewrittens = rewriteQueryScalarOpToTarget(swappedConjuncts, normalizedMap,
                        null, null);
                if (rewrittens == null) {
                    return null;
                }
                // TODO: consider normalizing it
                equalPredicates = Utils.compoundAnd(rewrittens);
            }

            if (!RewriteUtils.isAlwaysTrue(otherPredicates)) {
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(otherPredicates);
                // swapped by query ec
                List<ScalarOperator> swappedConjuncts = conjuncts.stream().map(conjunct -> {
                    ColumnRewriter rewriter = new ColumnRewriter(rewriteContext);
                    return rewriter.rewriteByQueryEc(conjunct);
                }).collect(Collectors.toList());
                Multimap<ScalarOperator, ColumnRefOperator> normalizedMap =
                        normalizeAndReverseProjection(queryExprMap, rewriteContext, true);
                List<ScalarOperator> rewrittens = rewriteQueryScalarOpToTarget(swappedConjuncts, normalizedMap,
                        null, null);
                if (rewrittens == null) {
                    return null;
                }
                otherPredicates = Utils.compoundAnd(rewrittens);
            }
        }
        ScalarOperator viewToQueryCompensationPredicate =
                RewriteUtils.canonizeNode(Utils.compoundAnd(equalPredicates, otherPredicates));
        // query predicate and (not viewToQueryCompensationPredicate) is the final query compensation predicate
        ScalarOperator queryCompensationPredicate = RewriteUtils.canonizeNode(
                Utils.compoundAnd(
                        rewriteContext.getQueryPredicateSplit().toScalarOperator(),
                        CompoundPredicateOperator.not(viewToQueryCompensationPredicate)));
        if (!RewriteUtils.isAlwaysTrue(queryCompensationPredicate)) {
            // add filter
            Operator.Builder builder = OperatorBuilderFactory.build(input.getOp());
            builder.withOperator(input.getOp());
            builder.setPredicate(queryCompensationPredicate);
            Operator newInputOp = builder.build();
            OptExpression newInputExpr = OptExpression.create(newInputOp, input.getInputs());
            aggExpr.setChild(0, newInputExpr);
            deriveLogicalProperty(aggExpr);
            return aggExpr;
        }
        return null;
    }

    @Override
    protected OptExpression createUnion(OptExpression queryInput, OptExpression viewInput, RewriteContext rewriteContext) {
        Map<ColumnRefOperator, ColumnRefOperator> columnMapping = Maps.newHashMap();
        List<ColumnRefOperator> unionOutputColumns = Lists.newArrayList();
        List<ColumnRefOperator> queryOutputColumns = Lists.newArrayList();
        LogicalAggregationOperator aggOp = (LogicalAggregationOperator) queryInput.getOp();
        List<ColumnRefOperator> groupKeys = aggOp.getGroupingKeys();
        for (ColumnRefOperator groupKey : groupKeys) {
            ColumnRefOperator unionColumn = rewriteContext.getQueryRefFactory()
                    .create(groupKey, groupKey.getType(), groupKey.isNullable());
            columnMapping.put(groupKey, unionColumn);
            unionOutputColumns.add(unionColumn);
            queryOutputColumns.add(groupKey);
        }

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            ColumnRefOperator unionColumn = rewriteContext.getQueryRefFactory()
                    .create(entry.getKey(), entry.getKey().getType(), entry.getKey().isNullable());
            columnMapping.put(entry.getKey(), unionColumn);
            unionOutputColumns.add(unionColumn);
            queryOutputColumns.add(entry.getKey());
        }
        List<ColumnRefOperator> viewOutputColumns = queryOutputColumns;
        LogicalUnionOperator unionOperator = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(unionOutputColumns)
                .setChildOutputColumns(Lists.newArrayList(queryOutputColumns, viewOutputColumns))
                .isUnionAll(true)
                .build();
        OptExpression unionExpr = OptExpression.create(unionOperator, queryInput, viewInput);

        // construct Aggregate on union with rollup aggregate functions
        // generate group keys against scan mv plan
        LogicalAggregationOperator queryAgg = (LogicalAggregationOperator) rewriteContext.getQueryExpression().getOp();
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        List<ColumnRefOperator> newGroupingKeys = Lists.newArrayList();
        for (ColumnRefOperator groupKey : originalGroupKeys) {
            newGroupingKeys.add(columnMapping.get(groupKey));
        }

        // generate new agg exprs(rollup functions)
        Map<ColumnRefOperator, CallOperator> queryAggs = queryAgg.getAggregations();
        Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregatesForUnion(
                queryAggs, columnMapping);
        if (newAggregations == null) {
            return null;
        }

        LogicalAggregationOperator.Builder aggBuilder = new LogicalAggregationOperator.Builder();
        aggBuilder.withOperator(queryAgg);
        aggBuilder.setGroupingKeys(newGroupingKeys);
        // can not be distinct agg here, so partitionByColumns is the same as groupingKeys
        aggBuilder.setPartitionByColumns(newGroupingKeys);
        aggBuilder.setAggregations(newAggregations);
        aggBuilder.setProjection(queryAgg.getProjection());
        aggBuilder.setPredicate(queryAgg.getPredicate());

        // TODO: maybe we should change the mapping key of queryInput and viewInput
        // add projection for group keys
        // aggregates are already mapped, so here just process group keys
        Map<ColumnRefOperator, ScalarOperator> newProjection = Maps.newHashMap();
        if (queryAgg.getProjection() == null) {
            for (int i = 0; i < originalGroupKeys.size(); i++) {
                newProjection.put(originalGroupKeys.get(i), newGroupingKeys.get(i));
            }
            newProjection.putAll(newAggregations);
        } else {
            Map<ColumnRefOperator, ScalarOperator> originalMap = queryAgg.getProjection().getColumnRefMap();
            Map<ColumnRefOperator, ScalarOperator> groupKeyMap = Maps.newHashMap();
            for (int i = 0; i < originalGroupKeys.size(); i++) {
                groupKeyMap.put(originalGroupKeys.get(i), newGroupingKeys.get(i));
            }
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(groupKeyMap);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : originalMap.entrySet()) {
                if (groupKeyMap.containsKey(entry.getValue())) {
                    ScalarOperator rewritten = rewriter.rewrite(entry.getValue());
                    newProjection.put(entry.getKey(), rewritten);
                } else {
                    // Aggregates map's keys are kept, so projection for aggregates remains the same
                    newProjection.put(entry.getKey(), entry.getValue());
                }
            }
        }
        Projection projection = new Projection(newProjection);
        aggBuilder.setProjection(projection);
        LogicalAggregationOperator newAggOp = aggBuilder.build();
        OptExpression aggExpr = OptExpression.create(newAggOp, unionExpr);
        return aggExpr;
    }

    private List<ColumnRefOperator> rewriteGroupKeys(List<ScalarOperator> groupKeys,
                                                     Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
                                                     Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                     Set<ColumnRefOperator> queryColumnSet) {
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
                                                           Set<ColumnRefOperator> queryColumnSet) {
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
            rewrittens.put(entry.getKey(), newAggregate);
        }

        return rewrittens;
    }

    private Map<ColumnRefOperator, CallOperator> rewriteAggregatesForUnion(Map<ColumnRefOperator, CallOperator> aggregates,
                                                                           Map<ColumnRefOperator, ColumnRefOperator> mapping) {
        Map<ColumnRefOperator, CallOperator> rewrittens = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregates.entrySet()) {
            Preconditions.checkState(entry.getValue() instanceof CallOperator);
            CallOperator aggCall = entry.getValue();
            ColumnRefOperator targetColumn = mapping.get(entry.getKey());
            // Aggregate must be CallOperator
            Preconditions.checkState(targetColumn instanceof ColumnRefOperator);
            CallOperator newAggregate = getRollupAggregate(aggCall, targetColumn);
            if (newAggregate == null) {
                return null;
            }
            rewrittens.put(entry.getKey(), newAggregate);
        }

        return rewrittens;
    }

    // generate new aggregates for rollup
    // eg: count(col) -> sum(col)
    private CallOperator getRollupAggregate(CallOperator aggCall, ColumnRefOperator targetColumn) {
        if (ROLLUP_FUNCTION_MAP.containsKey(aggCall.getFnName())) {
            if (aggCall.getFnName().equals(FunctionSet.COUNT)) {
                Function sumFn = findArithmeticFunction(aggCall.getFunction().getArgs(), FunctionSet.SUM);
                return new CallOperator(FunctionSet.SUM, aggCall.getFunction().getReturnType(),
                        Lists.newArrayList(targetColumn), sumFn);
            } else {
                // impossible to reach here
                LOG.warn("unsupported rollup function:{}", aggCall.getFnName());
                return null;
            }
        } else {
            // the rollup funcation is the same as origin, but use the new column as argument
            CallOperator newAggCall = (CallOperator) aggCall.clone();
            newAggCall.setChild(0, targetColumn);
            return newAggCall;
        }
    }

    private Function findArithmeticFunction(Type[] argsType, String fnName) {
        return Expr.getBuiltinFunction(fnName, argsType, Function.CompareMode.IS_IDENTICAL);
    }
}
