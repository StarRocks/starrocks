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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.combinator.AggStateMergeCombinator;
import com.starrocks.catalog.combinator.AggStateUnionCombinator;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.EquivalentShuttleContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.IRewriteEquivalent;
import com.starrocks.sql.optimizer.rule.tree.pdagg.AggregatePushDownContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.addExtraPredicate;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.genRollupProject;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.getRollupAggregateFunc;

/**
 * SPJG materialized view rewriter, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 * <p>
 * This rewriter is for aggregated query rewrite
 */
public final class AggregatedMaterializedViewRewriter extends MaterializedViewRewriter {
    private static final Logger LOG = LogManager.getLogger(AggregatedMaterializedViewRewriter.class);

    public AggregatedMaterializedViewRewriter(MvRewriteContext mvRewriteContext) {
        super(mvRewriteContext);
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
    public OptExpression doViewBasedRewrite(RewriteContext rewriteContext, OptExpression mvOptExpr) {
        LogicalAggregationOperator mvAggOp = (LogicalAggregationOperator) rewriteContext.getMvExpression().getOp();
        LogicalAggregationOperator queryAggOp = (LogicalAggregationOperator) rewriteContext.getQueryExpression().getOp();

        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        List<ScalarOperator> mvGroupingKeys =
                rewriteGroupByKeys(rewriteContext, columnRewriter, mvAggOp.getGroupingKeys(), true);
        List<ScalarOperator> queryGroupingKeys =
                rewriteGroupByKeys(rewriteContext, columnRewriter, queryAggOp.getGroupingKeys(), false);
        ScalarOperator queryRangePredicate = rewriteContext.getQueryPredicateSplit().getRangePredicates();

        AggregatePushDownContext aggRewriteInfo = rewriteContext.getAggregatePushDownContext();
        // if it's agg push down rewrite, rewrite it by rollup.
        boolean isRollup = aggRewriteInfo != null ? true : isRollupAggregate(mvGroupingKeys, queryGroupingKeys,
                queryRangePredicate);

        // Cannot ROLLUP distinct
        if (isRollup) {
            boolean mvHasDistinctAggFunc = mvAggOp.getAggregations().values().stream().anyMatch(callOp -> callOp.isDistinct()
                            && !callOp.getFnName().equalsIgnoreCase(FunctionSet.ARRAY_AGG));
            boolean queryHasDistinctAggFunc =
                    queryAggOp.getAggregations().values().stream().anyMatch(callOp -> callOp.isDistinct());
            if (mvHasDistinctAggFunc && queryHasDistinctAggFunc) {
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rollup aggregate cannot contain distinct aggregate functions," +
                        "mv:{}, query:{}", mvAggOp.getAggregations().values(), queryAggOp.getAggregations().values());
                return null;
            }
        } else {
            // agg_state combinator cannot be used for non-rollup rewrite(convert agg_state to final result),
            // so change isRollup to true if mv has agg_state combinator.
            boolean isContainAggStateCombinator = mvAggOp.getAggregations()
                    .values()
                    .stream()
                    .anyMatch(callOp -> callOp.getFunction() instanceof AggStateUnionCombinator
                            || callOp.getFunction() instanceof AggStateMergeCombinator);
            if (isContainAggStateCombinator) {
                isRollup = true;
            }
        }
        rewriteContext.setRollup(isRollup);

        // normalize mv's aggs by using query's table ref and query ec
        Map<ColumnRefOperator, ScalarOperator> mvProjection =
                MvUtils.getColumnRefMap(rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());
        // normalize view projection by query relation and ec
        EquationRewriter queryExprToMvExprRewriter =
                buildEquationRewriter(mvProjection, rewriteContext, false);

        if (isRollup) {
            return rewriteForRollup(queryAggOp, queryGroupingKeys, columnRewriter, queryExprToMvExprRewriter,
                    rewriteContext, mvOptExpr);
        } else {
            Pair<OptExpression, Boolean> result =
                    rewriteProjection(rewriteContext, queryAggOp, queryExprToMvExprRewriter, mvOptExpr);
            // even if query and mv's group-by keys are the same, it may still need rollup
            // eg:
            // example1:
            // mv           : select dt from t group by dt
            // query        : select count(dt) from t where dt='2024-11-27';
            // rewritten    : select count(dt) from mv where dt='2024-11-27'
            // example2:
            // mv           : select dt, avg_union(avg_state(c1)) as s from t group by dt
            // query        : select dt, avg(c1) from t group by dt
            // rewritten    : select avg_merge(s) from mv group by dt
            if (result.first != null && !result.second) {
                return result.first;
            } else if (result.second) {
                return rewriteForRollup(queryAggOp, queryGroupingKeys, columnRewriter, queryExprToMvExprRewriter,
                        rewriteContext, mvOptExpr);
            } else {
                return null;
            }
        }
    }

    private boolean isAggregate(ScalarOperator rewritten) {
        if (rewritten == null || !(rewritten instanceof CallOperator)) {
            return false;
        }
        CallOperator callOp = (CallOperator) rewritten;
        return callOp.isAggregate();
    }

    /**
     * If rewritten aggregate expr contains aggregate functions, we can still try rollup rewrite again.
     */
    protected Pair<OptExpression, Boolean> rewriteProjection(RewriteContext rewriteContext,
                                                             LogicalAggregationOperator queryAggregationOperator,
                                                             EquationRewriter queryExprToMvExprRewriter,
                                                             OptExpression mvOptExpr) {
        Map<ColumnRefOperator, ScalarOperator> queryMap = MvUtils.getColumnRefMap(
                rewriteContext.getQueryExpression(), rewriteContext.getQueryRefFactory());
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);

        Map<ColumnRefOperator, CallOperator> oldAggregations = queryAggregationOperator.getAggregations();

        Map<ColumnRefOperator, ScalarOperator> swappedQueryColumnMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryMap.entrySet()) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(entry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            swappedQueryColumnMap.put(entry.getKey(), swapped);
        }

        Map<ColumnRefOperator, ScalarOperator> newQueryProjection = Maps.newHashMap();
        AggregateFunctionRewriter aggregateFunctionRewriter =
                new AggregateFunctionRewriter(queryExprToMvExprRewriter, rewriteContext.getQueryRefFactory(), oldAggregations);
        ColumnRefSet originalColumnSet = new ColumnRefSet(rewriteContext.getQueryColumnSet());

        // rewrite group by + aggregate functions
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : swappedQueryColumnMap.entrySet()) {
            ScalarOperator scalarOp = entry.getValue();
            ScalarOperator rewritten = rewriteScalarOperator(rewriteContext, scalarOp,
                        queryExprToMvExprRewriter, rewriteContext.getOutputMapping(),
                        originalColumnSet, aggregateFunctionRewriter);
            // for non-rollup rewrite, the rewritten result should not contain aggregate functions.
            boolean isAggregate = isAggregate(rewritten);
            if (rewritten == null || isAggregate) {
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rewrite projection with aggregate group-by/agg expr " +
                        "failed: {}", scalarOp.toString());
                return Pair.create(null, isAggregate);
            }
            newQueryProjection.put(entry.getKey(), rewritten);
        }
        Projection newProjection = new Projection(newQueryProjection);
        mvOptExpr.getOp().setProjection(newProjection);

        // rewrite aggregate having expr: add aggregate's predicate compensation after group by.
        if (queryAggregationOperator.getPredicate() != null) {
            // NOTE: If there are having expr in agg, ensure all aggregate functions should put into new projections
            Map<ColumnRefOperator, ScalarOperator> queryColumnRefToScalarMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : oldAggregations.entrySet()) {
                ScalarOperator scalarOp = entry.getValue();
                ScalarOperator mapped = rewriteContext.getQueryColumnRefRewriter().rewrite(scalarOp.clone());
                ScalarOperator swapped = columnRewriter.rewriteByQueryEc(mapped);
                ScalarOperator rewritten = rewriteScalarOperator(rewriteContext, swapped,
                        queryExprToMvExprRewriter, rewriteContext.getOutputMapping(),
                        originalColumnSet, aggregateFunctionRewriter);
                // for non-rollup rewrite, the rewritten result should not contain aggregate functions.
                boolean isAggregate = isAggregate(rewritten);
                if (rewritten == null || isAggregate) {
                    OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                            "Rewrite aggregate with having expr failed: {}", scalarOp.toString());
                    return Pair.create(null, isAggregate);
                }
                queryColumnRefToScalarMap.put(entry.getKey(), rewritten);
            }
            for (ColumnRefOperator groupKey : queryAggregationOperator.getGroupingKeys()) {
                ScalarOperator mapped = rewriteContext.getQueryColumnRefRewriter().rewrite(groupKey.clone());
                ScalarOperator swapped = columnRewriter.rewriteByQueryEc(mapped);
                ScalarOperator rewritten = rewriteScalarOperator(rewriteContext, swapped,
                        queryExprToMvExprRewriter, rewriteContext.getOutputMapping(),
                        originalColumnSet, aggregateFunctionRewriter);
                boolean isAggregate = isAggregate(rewritten);
                if (rewritten == null || isAggregate) {
                    OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                            "Mapping grouping key failed: {}", groupKey.toString());
                    return Pair.create(null, isAggregate);
                }
                queryColumnRefToScalarMap.put(groupKey, rewritten);
            }
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(queryColumnRefToScalarMap);
            ScalarOperator aggPredicate = queryAggregationOperator.getPredicate();
            ScalarOperator rewrittenPred = rewriter.rewrite(aggPredicate);
            if (rewrittenPred == null) {
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rewrite aggregate with having failed, cannot compensate aggregate having predicates: {}",
                        queryAggregationOperator.getPredicate().toString());
                return Pair.create(null, false);
            }
            Operator op = mvOptExpr.getOp().cast();
            // take care original scan predicates and new having exprs
            ScalarOperator newPredicate = Utils.compoundAnd(rewrittenPred, op.getPredicate());
            mvOptExpr = addExtraPredicate(mvOptExpr, newPredicate);
        }

        return Pair.create(mvOptExpr, false);
    }

    private ScalarOperator rewriteScalarOperator(RewriteContext rewriteContext,
                                                 ScalarOperator scalarOp,
                                                 EquationRewriter equationRewriter,
                                                 Map<ColumnRefOperator, ColumnRefOperator> columnMapping,
                                                 ColumnRefSet originalColumnSet,
                                                 AggregateFunctionRewriter aggregateFunctionRewriter) {
        if (scalarOp.isConstantRef()) {
            return scalarOp;
        }
        equationRewriter.setAggregateFunctionRewriter(aggregateFunctionRewriter);
        equationRewriter.setOutputMapping(columnMapping);

        Pair<ScalarOperator, EquivalentShuttleContext> result =
                equationRewriter.replaceExprWithEquivalent(rewriteContext, scalarOp);
        ScalarOperator rewritten = result.first;
        if (rewritten == null || scalarOp == rewritten) {
            return null;
        }
        if (!isAllExprReplaced(rewritten, originalColumnSet)) {
            // it means there is some column that cannot be rewritten by outputs of mv
            return null;
        }
        return rewritten;
    }

    // NOTE: this method is not exactly right to check whether it's a rollup aggregate:
    // - all matched group by keys bit is less than mvGroupByKeys
    // - if query contains one non-mv-existed agg, set it `rollup` and use `replaceExprWithTarget` to
    //    - check whether to rewrite later.
    // NOTE: It's not safe to check rollup by using group by keys only, we may still rollup even if there are the
    // same group by keys.
    private boolean isRollupAggregate(List<ScalarOperator> mvGroupingKeys,
                                      List<ScalarOperator> queryGroupingKeys,
                                      ScalarOperator queryRangePredicate) {
        MaterializedView mv = mvRewriteContext.getMaterializationContext().getMv();
        if (mv.getRefreshScheme().isSync() && mv.getDefaultDistributionInfo() instanceof RandomDistributionInfo) {
            return true;
        }
        // after equivalence class rewrite, there may be same group keys, so here just get the distinct grouping keys
        List<ScalarOperator> distinctMvKeys = mvGroupingKeys.stream().distinct().collect(Collectors.toList());
        BitSet matchedGroupByKeySet = new BitSet(distinctMvKeys.size());
        for (ScalarOperator queryGroupByKey : queryGroupingKeys) {
            boolean isMatched = false;
            for (int i = 0; i < distinctMvKeys.size(); i++) {
                if (queryGroupByKey.equals(distinctMvKeys.get(i))) {
                    isMatched = true;
                    matchedGroupByKeySet.set(i);
                    break;
                }
            }
            if (!isMatched) {
                return true;
            }
        }
        if (matchedGroupByKeySet.cardinality() >= distinctMvKeys.size()) {
            return false;
        }

        Set<ScalarOperator> equalsPredicateColumns = new HashSet<>();
        for (ScalarOperator operator : Utils.extractConjuncts(queryRangePredicate)) {
            if (operator instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binaryPredicateOperator = ((BinaryPredicateOperator) operator);
                if (!binaryPredicateOperator.getBinaryType().isEqual()) {
                    continue;
                }

                if (operator.getChild(0).isColumnRef() && operator.getChild(1).isConstantRef()) {
                    equalsPredicateColumns.add(operator.getChild(0));
                }
            }
        }

        for (int i = 0; i < distinctMvKeys.size(); i++) {
            if (!matchedGroupByKeySet.get(i)) {
                ScalarOperator missedKey = distinctMvKeys.get(i);
                if (!equalsPredicateColumns.contains(missedKey)) {
                    return true;
                }
            }
        }

        return false;
    }

    private List<ScalarOperator> rewriteGroupByKeys(RewriteContext rewriteContext,
                                                    ColumnRewriter columnRewriter,
                                                    List<ColumnRefOperator> groupByKeys,
                                                    boolean rewriteViewToQuery) {
        List<ScalarOperator> rewriteGroupingKeys = Lists.newArrayList();
        for (ColumnRefOperator key : groupByKeys) {
            if (rewriteViewToQuery) {
                ScalarOperator rewriteKey = rewriteContext.getMvColumnRefRewriter().rewrite(key.clone());
                rewriteGroupingKeys.add(columnRewriter.rewriteViewToQueryWithQueryEc(rewriteKey));
            } else {
                ScalarOperator rewriteKey = rewriteContext.getQueryColumnRefRewriter().rewrite(key.clone());
                rewriteGroupingKeys.add(columnRewriter.rewriteByQueryEc(rewriteKey));
            }
        }
        return rewriteGroupingKeys;
    }

    private Map<ColumnRefOperator, CallOperator> rewriteAggregations(RewriteContext rewriteContext,
                                                                       ColumnRewriter columnRewriter,
                                                                       Map<ColumnRefOperator, CallOperator> aggregations,
                                                                       boolean rewriteViewToQuery) {
        Map<ColumnRefOperator, CallOperator> rewriteAggregations = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregations.entrySet()) {
            if (rewriteViewToQuery) {
                ScalarOperator rewriteAgg = rewriteContext.getMvColumnRefRewriter().rewrite(entry.getValue().clone());
                ScalarOperator rewritten = columnRewriter.rewriteViewToQueryWithQueryEc(rewriteAgg);
                rewriteAggregations.put(entry.getKey(), (CallOperator) rewritten);
            } else {
                ScalarOperator rewriteAgg = rewriteContext.getQueryColumnRefRewriter().rewrite(entry.getValue().clone());
                ScalarOperator rewritten = columnRewriter.rewriteByQueryEc(rewriteAgg);
                rewriteAggregations.put(entry.getKey(), (CallOperator) rewritten);
            }
        }
        return rewriteAggregations;
    }

    // the core idea for aggregation rollup rewrite is:
    // 1. rewrite grouping keys
    // 2. rewrite aggregations
    // 3. rewrite the projections on LogicalAggregationOperator by using the columns mapping constructed from the 2 steps ahead
    private OptExpression rewriteForRollup(
            LogicalAggregationOperator queryAggOp,
            List<ScalarOperator> queryGroupingKeys,
            ColumnRewriter columnRewriter,
            EquationRewriter equationRewriter,
            RewriteContext rewriteContext,
            OptExpression mvOptExpr) {
        Map<ColumnRefOperator, ScalarOperator> queryColumnRefToScalarMap = Maps.newHashMap();

        // rewrite group by keys by using mv
        List<ScalarOperator> newQueryGroupKeys = rewriteGroupKeys(rewriteContext, queryGroupingKeys, equationRewriter,
                rewriteContext.getOutputMapping(), new ColumnRefSet(rewriteContext.getQueryColumnSet()));
        if (newQueryGroupKeys == null) {
            OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                    "Rewrite rollup aggregate failed, cannot rewrite group by keys: {}",
                    queryGroupingKeys);
            return null;
        }
        List<ColumnRefOperator> queryGroupKeys = queryAggOp.getGroupingKeys();
        Preconditions.checkState(queryGroupKeys.size() == newQueryGroupKeys.size());
        for (int i = 0; i < newQueryGroupKeys.size(); i++) {
            queryColumnRefToScalarMap.put(queryGroupKeys.get(i), newQueryGroupKeys.get(i));
        }

        Map<ColumnRefOperator, CallOperator> rewrittenAggregations = queryAggOp.getAggregations();
        Map<ColumnRefOperator, CallOperator> queryAggregation = rewriteAggregations(
                rewriteContext, columnRewriter, rewrittenAggregations, false);

        // generate new agg exprs(rollup functions)
        final Map<ColumnRefOperator, ScalarOperator> newProjection = new HashMap<>();
        Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregates(
                queryAggregation, equationRewriter, rewriteContext.getOutputMapping(),
                new ColumnRefSet(rewriteContext.getQueryColumnSet()), queryColumnRefToScalarMap,
                newProjection, !newQueryGroupKeys.isEmpty(), rewriteContext);
        if (newAggregations == null) {
            OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                    "Rewrite rollup aggregate failed: cannot rewrite aggregate functions");
            return null;
        }

        return createNewAggregate(rewriteContext, queryAggOp, newAggregations,
                queryColumnRefToScalarMap, mvOptExpr, newProjection);
    }

    @Override
    public OptExpression doQueryBasedRewrite(RewriteContext rewriteContext,
                                             ScalarOperator compensationPredicates,
                                             OptExpression queryExpression) {
        OptExpression child = super.doQueryBasedRewrite(rewriteContext, compensationPredicates, queryExpression.inputAt(0));
        if (child == null) {
            return null;
        }
        return OptExpression.create(queryExpression.getOp(), child);
    }

    @Override
    public OptExpression doUnionRewrite(OptExpression queryInput,
                                        OptExpression viewInput,
                                        RewriteContext rewriteContext) {
        Map<ColumnRefOperator, ScalarOperator> queryColumnRefMap =
                MvUtils.getColumnRefMap(queryInput, rewriteContext.getQueryRefFactory());
        // keys of queryColumnRefMap and mvColumnRefMap are the same
        List<ColumnRefOperator> originalOutputColumns = new ArrayList<>(queryColumnRefMap.keySet());
        // rewrite query
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(materializationContext);
        // don't reset selected partition ids for query input, because query's partition ranges should not be extended.
        OptExpression newQueryInput = duplicator.duplicate(queryInput, false);
        List<ColumnRefOperator> newQueryOutputColumns = duplicator.getMappedColumns(originalOutputColumns);

        Projection projection = getMvOptExprProjection(viewInput);
        Preconditions.checkState(projection != null);
        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> mvProjection = projection.getColumnRefMap();
        List<ColumnRefOperator> newViewOutputColumns = Lists.newArrayList();
        for (ColumnRefOperator columnRef : originalOutputColumns) {
            ColumnRefOperator newColumn = rewriteContext.getQueryRefFactory().create(
                    columnRef, columnRef.getType(), columnRef.isNullable());
            newViewOutputColumns.add(newColumn);
            Preconditions.checkArgument(mvProjection.containsKey(columnRef));
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
        Map<ColumnRefOperator, ColumnRefOperator> originColRefToNewMap = Maps.newHashMap();
        for (int i = 0; i < originalOutputColumns.size(); i++) {
            originColRefToNewMap.put(originalOutputColumns.get(i), unionOutputColumns.get(i));
        }

        LogicalAggregationOperator queryAgg = (LogicalAggregationOperator) rewriteContext.getQueryExpression().getOp();
        if (queryAgg.getProjection() != null) {
            // if query has projection, is not supported now
            OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                    "Rewrite aggregate with union failed: aggregate with projection is still not supported");
            return null;
        }

        final List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        final Map<ColumnRefOperator, ScalarOperator> aggregateMapping = Maps.newHashMap();
        // group by keys
        for (ColumnRefOperator groupKey : originalGroupKeys) {
            aggregateMapping.put(groupKey, originColRefToNewMap.get(groupKey));
        }
        final Map<ColumnRefOperator, ScalarOperator> newProjection = new HashMap<>();
        // aggregations
        Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregatesForUnion(
                rewriteContext, queryAgg.getAggregations(), originColRefToNewMap, aggregateMapping, newProjection,
                !originalGroupKeys.isEmpty());
        if (newAggregations == null) {
            OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                    "Rewrite aggregate with union failed: rewrite aggregate for union failed");
            return null;
        }

        // create aggregate above union all
        OptExpression result = createNewAggregate(rewriteContext, queryAgg, newAggregations, aggregateMapping,
                unionExpr, newProjection);
        // Add extra union all predicates above union all operator.
        if (rewriteContext.getUnionRewriteQueryExtraPredicate() != null) {
            MvUtils.addExtraPredicate(result, rewriteContext.getUnionRewriteQueryExtraPredicate());
        }
        deriveLogicalProperty(result);

        return result;
    }

    /**
     * Return required column ref and associated scalar operator for scan operator.
     */
    private  Map<ColumnRefOperator, ScalarOperator> getRequiredScanColumns(
            OptExpression mvOptExpr,
            Map<ColumnRefOperator, CallOperator> newAggregations,
            Map<ColumnRefOperator, ScalarOperator> queryColumnRefToScalarMap,
            Map<ColumnRefOperator, ScalarOperator> newProjection) {
        ColumnRefSet requiredColumns = new ColumnRefSet();
        newAggregations.values().stream().map(ScalarOperator::getUsedColumns).forEach(requiredColumns::union);
        newProjection.values().stream().map(ScalarOperator::getUsedColumns).forEach(requiredColumns::union);
        queryColumnRefToScalarMap.values().stream().map(ScalarOperator::getUsedColumns).forEach(requiredColumns::union);
        Map<ColumnRefOperator, ScalarOperator> newQueryProjection = Maps.newHashMap();
        mvOptExpr.getRowOutputInfo().getColumnRefMap().entrySet().stream()
                .filter(x -> requiredColumns.contains(x.getKey()))
                .forEach(x -> newQueryProjection.put(x.getKey(), x.getValue()));
        return newQueryProjection;
    }

    private OptExpression createNewAggregate(
            RewriteContext rewriteContext,
            LogicalAggregationOperator queryAgg,
            Map<ColumnRefOperator, CallOperator> newAggregations,
            Map<ColumnRefOperator, ScalarOperator> queryColumnRefToScalarMap,
            OptExpression mvOptExpr,
            Map<ColumnRefOperator, ScalarOperator> newProjection) {
        // newGroupKeys may have duplicate because of EquivalenceClasses
        // remove duplicate here as new grouping keys
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        boolean isAllGroupByKeyColumnRefs = originalGroupKeys.stream()
                .map(key -> queryColumnRefToScalarMap.get(key))
                .allMatch(ColumnRefOperator.class::isInstance);

        List<ColumnRefOperator> newGroupByKeyColumnRefs = Lists.newArrayList();
        // If not all group by keys are column ref, need add new project to input expr.
        if (!isAllGroupByKeyColumnRefs) {
            Map<ColumnRefOperator, ScalarOperator> newQueryProjection = getRequiredScanColumns(
                    mvOptExpr, newAggregations, queryColumnRefToScalarMap, newProjection);
            for (ColumnRefOperator oldGroupByKey : originalGroupKeys) {
                ScalarOperator newGroupByKey = queryColumnRefToScalarMap.get(oldGroupByKey);
                if (newGroupByKey instanceof ColumnRefOperator) {
                    newGroupByKeyColumnRefs.add((ColumnRefOperator) newGroupByKey);
                    newQueryProjection.put((ColumnRefOperator) newGroupByKey, newGroupByKey);
                } else {
                    ColumnRefOperator newColumnRef = rewriteContext.getQueryRefFactory().create(
                            newGroupByKey, newGroupByKey.getType(), newGroupByKey.isNullable());
                    newGroupByKeyColumnRefs.add(newColumnRef);
                    newQueryProjection.put(newColumnRef, newGroupByKey);

                    // Use new column ref in projection node replace original rewritten scalar op.
                    queryColumnRefToScalarMap.put(oldGroupByKey, newColumnRef);
                }
            }
            Projection newProject = new Projection(newQueryProjection);
            mvOptExpr.getOp().setProjection(newProject);
        } else {
            newGroupByKeyColumnRefs = originalGroupKeys.stream()
                    .map(key -> (ColumnRefOperator) queryColumnRefToScalarMap.get(key))
                    .collect(Collectors.toList());
        }
        List<ColumnRefOperator> distinctGroupKeys = newGroupByKeyColumnRefs.stream()
                .distinct()
                .collect(Collectors.toList());

        LogicalAggregationOperator.Builder aggBuilder = new LogicalAggregationOperator.Builder();
        aggBuilder.withOperator(queryAgg);
        // rewrite grouping by keys
        aggBuilder.setGroupingKeys(distinctGroupKeys);

        // rewrite aggregations
        // can not be distinct agg here, so partitionByColumns is the same as groupingKeys
        aggBuilder.setPartitionByColumns(distinctGroupKeys);
        aggBuilder.setAggregations(newAggregations);

        // Add aggregate's predicate compensation here because aggregate predicates should be taken care
        // by self.
        if (queryAgg.getPredicate() != null) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(queryColumnRefToScalarMap);
            aggBuilder.setPredicate(rewriter.rewrite(queryAgg.getPredicate()));
        }

        // add projection to make sure that the output columns keep the same with the origin query
        if (queryAgg.getProjection() == null) {
            for (int i = 0; i < originalGroupKeys.size(); i++) {
                newProjection.put(originalGroupKeys.get(i), newGroupByKeyColumnRefs.get(i));
            }
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : queryAgg.getAggregations().entrySet()) {
                newProjection.put(entry.getKey(), queryColumnRefToScalarMap.get(entry.getKey()));
            }
        } else {
            Map<ColumnRefOperator, ScalarOperator> originalMap = queryAgg.getProjection().getColumnRefMap();
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(queryColumnRefToScalarMap);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : originalMap.entrySet()) {
                ScalarOperator rewritten = rewriter.rewrite(entry.getValue());
                newProjection.put(entry.getKey(), rewritten);
            }
        }
        Projection projection = new Projection(newProjection);
        aggBuilder.setProjection(projection);
        LogicalAggregationOperator newAggOp = aggBuilder.build();
        OptExpression rewriteOp = OptExpression.create(newAggOp, mvOptExpr);
        deriveLogicalProperty(rewriteOp);
        return rewriteOp;
    }

    /**
     * Rewrite group by keys by using MV.
     */
    private List<ScalarOperator> rewriteGroupKeys(RewriteContext rewriteContext,
                                                  List<ScalarOperator> groupKeys,
                                                  EquationRewriter equationRewriter,
                                                  Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                  ColumnRefSet queryColumnSet) {
        List<ScalarOperator> newGroupByKeys = Lists.newArrayList();
        equationRewriter.setOutputMapping(mapping);
        for (ScalarOperator key : groupKeys) {
            Pair<ScalarOperator, EquivalentShuttleContext> result = equationRewriter.replaceExprWithEquivalent(rewriteContext,
                    key, IRewriteEquivalent.RewriteEquivalentType.PREDICATE);
            ScalarOperator newGroupByKey = result.first;
            if (key.isVariable() && key == newGroupByKey) {
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rewrite group by key failed: {}", key.toString());
                return null;
            }
            if (newGroupByKey == null || !isAllExprReplaced(newGroupByKey, queryColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rewrite group by key failed: {}", key.toString());
                return null;
            }
            newGroupByKeys.add(newGroupByKey);
        }
        return newGroupByKeys;
    }

    /**
     * Rewrite aggregation by using MV.
     * @param aggregates aggregation column ref -> scalar op to be rewritten
     * @param equationRewriter equivalence class rewriter
     * @param mapping output mapping for rewrite: column ref -> column ref
     * @param queryColumnSet column set of query
     * @param aggColRefToAggMap aggregate query column ref -> new scalar op which is used for rewrite mapping
     * @param newProjection new projection mapping: col ref -> scalar op which is used for projection of new rewritten aggregate
     * @param hasGroupByKeys whether query has group by keys or not
     * @param context rewrite context
     */
    private Map<ColumnRefOperator, CallOperator> rewriteAggregates(Map<ColumnRefOperator, CallOperator> aggregates,
                                                                   EquationRewriter equationRewriter,
                                                                   Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                                   ColumnRefSet queryColumnSet,
                                                                   Map<ColumnRefOperator, ScalarOperator> aggColRefToAggMap,
                                                                   Map<ColumnRefOperator, ScalarOperator> newProjection,
                                                                   boolean hasGroupByKeys,
                                                                   RewriteContext context) {
        final Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        equationRewriter.setOutputMapping(mapping);
        AggregateFunctionRewriter aggregateFunctionRewriter = new AggregateFunctionRewriter(equationRewriter,
                context.getQueryRefFactory(), aggregates);
        equationRewriter.setAggregateFunctionRewriter(aggregateFunctionRewriter);
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregates.entrySet()) {
            CallOperator aggCall = entry.getValue();
            // Aggregate must be CallOperator
            Pair<CallOperator, EquivalentShuttleContext> rewritten = getRollupAggregate(context, equationRewriter,
                    queryColumnSet, aggCall);
            if (rewritten == null || rewritten.first == null) {
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rewrite aggregate function failed, cannot get rollup function: {}",
                        aggCall.toString());
                return null;
            }
            CallOperator newAggregate = rewritten.first;
            EquivalentShuttleContext eqContext = rewritten.second;

            ColumnRefOperator origColRef = entry.getKey();
            if (eqContext.isRewrittenByRewriter()) {
                newAggregations.putAll(eqContext.getNewColumnRefToAggFuncMap());
                aggColRefToAggMap.put(origColRef, newAggregate);
            } else if (!newAggregate.isAggregate()) {
                // If rewritten function is not an aggregation function, it could be like ScalarFunc(AggregateFunc(...))
                // We need to decompose it into Projection function and Aggregation function
                // E.g. count(distinct x) => array_length(array_unique_agg(x))
                // The array_length is a ScalarFunction and array_unique_agg is AggregateFunction
                // So it's decomposed into 1: array_length(slot_2), 2: array_unique_agg(x)
                CallOperator realAggregate = null;
                int foundIndex = -1;
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

                ColumnRefOperator innerAgg = context.getQueryRefFactory()
                                .create(realAggregate, realAggregate.getType(), realAggregate.isNullable());
                CallOperator copyProject = (CallOperator) newAggregate.clone();
                copyProject.setChild(foundIndex, innerAgg);

                ColumnRefOperator outerProject = context.getQueryRefFactory()
                                .create(copyProject, copyProject.getType(), copyProject.isNullable());
                newProjection.put(outerProject, copyProject);
                newAggregations.put(innerAgg, realAggregate);

                // replace original projection
                aggColRefToAggMap.put(origColRef, copyProject);
            } else {
                ColumnRefOperator newAggColRef = context.getQueryRefFactory().create(
                        origColRef, newAggregate.getType(), newAggregate.isNullable());
                newAggregations.put(newAggColRef, newAggregate);
                // No needs to set `newProjections` since it will use aggColRefToAggMap to construct new projections,
                // otherwise it will cause duplicate projections(or wrong projections).
                // eg:
                // query: oldCol1 -> count()
                // newAggregations: newCol1 -> sum(oldCol1)
                // aggColRefToAggMap:  oldCol1 -> coalesce(newCol1, 0)
                // It will generate new projections as below:
                // newProjections: oldCol1 -> coalesce(newCol1, 0)
                ScalarOperator newProjectOp = mvRewriteContext.isInAggregatePushDown() ?
                        newAggColRef : genRollupProject(aggCall, newAggColRef, hasGroupByKeys);
                aggColRefToAggMap.put(origColRef, newProjectOp);
            }
        }

        return newAggregations;
    }

    private Pair<CallOperator, EquivalentShuttleContext> getRollupAggregate(RewriteContext rewriteContext,
                                                                            EquationRewriter equationRewriter,
                                                                            ColumnRefSet queryColumnSet,
                                                                            CallOperator aggCall) {
        Pair<ScalarOperator, EquivalentShuttleContext> result = equationRewriter.replaceExprWithRollup(rewriteContext, aggCall);
        ScalarOperator targetColumn = result.first;
        if (targetColumn == null || !isAllExprReplaced(targetColumn, queryColumnSet)) {
            // it means there is some column that can not be rewritten by outputs of mv
            OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                    "Rewrite aggregate rollup {} failed with equivalent", aggCall.toString());
            return null;
        }
        EquivalentShuttleContext eqContext = result.second;

        boolean isRewrittenByEquivalent = eqContext.isRewrittenByEquivalent() || eqContext.isRewrittenByRewriter();
        if (isRewrittenByEquivalent) {
            Preconditions.checkState(targetColumn instanceof CallOperator);
            return Pair.create((CallOperator) targetColumn, eqContext);
        } else {
            if (!targetColumn.isColumnRef()) {
                if (targetColumn instanceof CallOperator
                        && AggregateFunctionRollupUtils.isNonCumulativeFunction(aggCall)
                        && equationRewriter.isColWithOnlyGroupByKeys(aggCall)) {
                    return Pair.create((CallOperator) targetColumn, eqContext);
                }
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rewrite aggregate rollup {} failed: only column-ref is supported after rewrite",
                        aggCall.toString());
                return null;
            }
            CallOperator newAggregate = getRollupAggregateFunc(aggCall, (ColumnRefOperator) targetColumn, false);
            if (newAggregate == null) {
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rewrite aggregate {} failed: cannot get rollup aggregate",
                        aggCall.toString());
                return null;
            }
            return Pair.create(newAggregate, eqContext);
        }
    }

    private Map<ColumnRefOperator, CallOperator> rewriteAggregatesForUnion(
            RewriteContext rewriteContext,
            Map<ColumnRefOperator, CallOperator> aggregates,
            Map<ColumnRefOperator, ColumnRefOperator> columnRefMapping,
            Map<ColumnRefOperator, ScalarOperator> aggregateMapping,
            Map<ColumnRefOperator, ScalarOperator> newProjection,
            boolean hasGroupByKeys) {
        Map<ColumnRefOperator, CallOperator> rewrittens = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregates.entrySet()) {
            Preconditions.checkState(entry.getValue() != null);
            ColumnRefOperator aggColRef = entry.getKey();
            CallOperator aggCall = entry.getValue();

            ColumnRefOperator targetColumn = columnRefMapping.get(entry.getKey());
            if (targetColumn == null) {
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rewrite aggregate {} for union failed: target column is null", aggCall.toString());
                return null;
            }

            // Aggregate must be CallOperator
            CallOperator newAggregate = getRollupAggregateFunc(aggCall, targetColumn, true);
            if (newAggregate == null) {
                OptimizerTraceUtil.logMVRewriteFailReason(mvRewriteContext,
                        "Rewrite aggregate {} for union failed: cannot get rollup aggregate", aggCall.toString());
                return null;
            }

            // create new column ref for aggregation's keys
            ColumnRefOperator newAggColRef = rewriteContext.getQueryRefFactory().create(aggColRef, aggColRef.getType(),
                    aggColRef.isNullable());
            aggregateMapping.put(aggColRef, newAggColRef);
            rewrittens.put(newAggColRef, newAggregate);
            newProjection.put(newAggColRef, genRollupProject(aggCall, newAggColRef, hasGroupByKeys));
        }

        return rewrittens;
    }
}
