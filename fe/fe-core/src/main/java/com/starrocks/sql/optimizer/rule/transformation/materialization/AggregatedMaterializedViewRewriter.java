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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SPJG materialized view rewriter, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 *  This rewriter is for aggregated query rewrite
 */
public class AggregatedMaterializedViewRewriter extends MaterializedViewRewriter {
    private static final Logger LOG = LogManager.getLogger(AggregatedMaterializedViewRewriter.class);

    private static final Map<String, String> ROLLUP_FUNCTION_MAP = ImmutableMap.<String, String>builder()
            .put(FunctionSet.COUNT, FunctionSet.SUM)
            .build();

    private static final Set<String> SUPPORTED_ROLLUP_FUNCTIONS = ImmutableSet.<String>builder()
            .add(FunctionSet.SUM)
            .add(FunctionSet.COUNT)
            .add(FunctionSet.MAX)
            .add(FunctionSet.MIN)
            .add(FunctionSet.APPROX_COUNT_DISTINCT)
            .build();

    public AggregatedMaterializedViewRewriter(MaterializationContext materializationContext) {
        super(materializationContext);
    }

    @Override
    public boolean isValidPlan(OptExpression expression) {
        // TODO: to support grouping sets/rollup/cube
        return MvUtils.isLogicalSPJG(expression);
    }

    // NOTE:
    // - there may be a projection on LogicalAggregationOperator.
    // - agg rewrite should be based on projection of mv.
    // - `rollup`: if mv's group-by keys is subset of query's group by keys,
    //    need add extra aggregate to compensate.
    // Example:
    // mv:
    //     select a, b,  abs(a) as col1, length(b) as col2, sum(c) as col3
    //     from t
    //     group by a, b
    // 1. query needs no `rolllup`
    //      query: select abs(a), length(b), sum(c) from t group by a, b
    //      rewrite: select col1, col2, col3 from mv
    // 2. query needs `rolllup`
    //      query: select a, abs(a), sum(c) from t group by a
    //      rewrite:
    //      select a, col1, sum(col5)
    //      (
    //          select a, b, col1, col2, col3 from mv
    //      ) t
    //      group by a
    // 3. the following query can not be rewritten because a + 1 do not reside in mv
    //      query:
    //      select a+1, abs(a), sum(c) from t group by a
    @Override
    protected OptExpression viewBasedRewrite(RewriteContext rewriteContext, OptExpression targetExpr) {
        LogicalAggregationOperator mvAgg = (LogicalAggregationOperator) rewriteContext.getMvExpression().getOp();
        List<ScalarOperator> swappedMvGroupingKeys = Lists.newArrayList();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (ColumnRefOperator key : mvAgg.getGroupingKeys()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(key.clone());
            ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
            swappedMvGroupingKeys.add(swapped);
        }

        LogicalAggregationOperator queryAgg = (LogicalAggregationOperator) rewriteContext.getQueryExpression().getOp();
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        List<ScalarOperator> queryAggGroupingKeys = Lists.newArrayList();
        for (ColumnRefOperator key : originalGroupKeys) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(key.clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            queryAggGroupingKeys.add(swapped);
        }

        // after equivalence class rewrite, there may be same group keys, so here just get the distinct grouping keys
        List<ScalarOperator> distinctMvKeys = swappedMvGroupingKeys.stream().distinct().collect(Collectors.toList());
        GroupKeyChecker aggGroupKeyChecker = new GroupKeyChecker(distinctMvKeys);
        boolean keyMatched = aggGroupKeyChecker.check(queryAggGroupingKeys);
        if (!keyMatched) {
            return null;
        }

        boolean isRollup = aggGroupKeyChecker.isRollup();
        List<ScalarOperator> swappedMvAggs = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : mvAgg.getAggregations().entrySet()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(entry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
            swappedMvAggs.add(swapped);
        }
        Map<ColumnRefOperator, ScalarOperator> swappedQueryAggs = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : queryAgg.getAggregations().entrySet()) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(entry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            swappedQueryAggs.put(entry.getKey(), swapped);
        }
        AggregateRewriteChecker aggregateRewriteChecker = new AggregateRewriteChecker(swappedMvAggs);
        boolean aggMatched = aggregateRewriteChecker.check(new ArrayList<>(swappedQueryAggs.values()));
        if (!aggMatched) {
            return null;
        }

        if (isRollup && aggregateRewriteChecker.hasDistinct()) {
            // can not support rollup of distinct agg
            return null;
        }

        // check aggregates of query
        // normalize mv's aggs by using query's table ref and query ec
        Map<ColumnRefOperator, ScalarOperator> mvProjection =
                MvUtils.getColumnRefMap(rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());
        // normalize view projection by query relation and ec
        Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap =
                normalizeAndReverseProjection(mvProjection, rewriteContext, false);


        if (isRollup) {
            return rewriteForRollup(queryAgg, swappedQueryAggs, swappedMvGroupingKeys,
                    queryAggGroupingKeys, normalizedViewMap, rewriteContext, targetExpr);
        } else {
            return rewriteProjection(rewriteContext, normalizedViewMap, targetExpr);
        }
    }

    // judge whether the output column is a projection on grouping key
    private boolean isBasedOnKey(List<ScalarOperator> keys, ScalarOperator targetScalar) {
        ScalarOperatorVisitor<Boolean, Void> keyJudger = new ScalarOperatorVisitor<Boolean, Void>() {
            @Override
            public Boolean visit(ScalarOperator scalarOperator, Void context) {
                for (ScalarOperator child : scalarOperator.getChildren()) {
                    Boolean ret = child.accept(this, context);
                    if (ret) {
                        return true;
                    }
                }
                return keys.contains(scalarOperator);
            }
        };
        Boolean ret = targetScalar.accept(keyJudger, null);
        return ret == Boolean.TRUE;
    }

    // the core idea for aggregation rollup rewrite is:
    // 1. rewrite grouping keys
    // 2. rewrite aggregations
    // 3. rewrite the projections on LogicalAggregationOperator by using the columns mapping constructed from the 2 steps ahead
    private OptExpression rewriteForRollup(
            LogicalAggregationOperator queryAgg,
            Map<ColumnRefOperator, ScalarOperator> queryAggregation,
            List<ScalarOperator> swappedMvGroupingKeys,
            List<ScalarOperator> swappedQueryGroupingKeys,
            Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
            RewriteContext rewriteContext,
            OptExpression targetExpr) {
        // check aggregates of query
        // normalize mv's aggs by using query's table ref and query ec
        Map<ColumnRefOperator, ScalarOperator> mvProjection =
                MvUtils.getColumnRefMap(rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());

        // for mv: select a, abs(a), sum(c) + 1 from t group by a
        // mvProjectionKeys will be: a, abs(a)
        // mvProjectionAggs will be: sum(c) + 1
        List<ScalarOperator> mvProjectionKeys = Lists.newArrayList();
        List<ScalarOperator> mvProjectionAggs = Lists.newArrayList();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : mvProjection.entrySet()) {
            ScalarOperator swapped = rewriteContext.getMvColumnRefRewriter().rewrite(entry.getValue());
            ScalarOperator rewritten = columnRewriter.rewriteViewToQueryWithQueryEc(swapped);
            if (isBasedOnKey(swappedMvGroupingKeys, rewritten)) {
                mvProjectionKeys.add(rewritten);
            } else {
                mvProjectionAggs.add(rewritten);
            }
        }

        // the same as mv
        Map<ColumnRefOperator, ScalarOperator> queryProjection =
                MvUtils.getColumnRefMap(rewriteContext.getQueryExpression(), rewriteContext.getQueryRefFactory());
        List<ScalarOperator> queryProjectionKeys = Lists.newArrayList();
        List<ScalarOperator> queryProjectionAggs = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryProjection.entrySet()) {
            ScalarOperator swapped = rewriteContext.getQueryColumnRefRewriter().rewrite(entry.getValue());
            ScalarOperator rewritten = columnRewriter.rewriteByQueryEc(swapped);
            if (isBasedOnKey(swappedQueryGroupingKeys, rewritten)) {
                queryProjectionKeys.add(rewritten);
            } else {
                queryProjectionAggs.add(rewritten);
            }
        }

        // check that query keys should be in mv keys
        if (queryProjectionKeys.stream().anyMatch(key -> !mvProjectionKeys.contains(key))) {
            return null;
        }

        // check that query aggs should be in mv aggs
        if (queryProjectionAggs.stream().anyMatch(agg -> !mvProjectionAggs.contains(agg))) {
            return null;
        }

        // TODO: consider avg(x) -> sum(x) / count(x)
        // avg can not be rolled up, but after changed into sum(x) / count(x),
        // then it can be rolled up
        if (queryProjectionAggs.stream().anyMatch(agg -> !queryAggregation.containsValue(agg))) {
            // aggregation projections with rollup is not supported
            // the following query can not be rewritten by mv
            // mv: sum(c) + 1 group by a, b
            // query: sum(c) + 1 group a
            return null;
        }

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
        List<ColumnRefOperator> originalOutputColumns = new ArrayList<>(queryColumnRefMap.keySet());
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
        }

        Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregatesForUnion(
                queryAgg.getAggregations(), columnMapping, aggregateMapping);
        if (newAggregations == null) {
            return null;
        }
        OptExpression result = createNewAggregate(queryAgg, newAggregations, aggregateMapping, unionExpr);
        deriveLogicalProperty(result);
        return result;
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
                // Aggregates map's keys are changed, so here entry values should be rewritten by aggregateMapping
                ScalarOperator newValue = rewriter.rewrite(entry.getValue());
                newProjection.put(entry.getKey(), newValue);
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
            Preconditions.checkState(entry.getValue() != null);
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
        if (!SUPPORTED_ROLLUP_FUNCTIONS.contains(aggCall.getFnName())) {
            return null;
        }
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
