// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
<<<<<<< HEAD
=======
import com.starrocks.analysis.Expr;
>>>>>>> branch-2.5-mrs
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
<<<<<<< HEAD
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
=======
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
>>>>>>> branch-2.5-mrs
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
<<<<<<< HEAD
import java.util.HashSet;
=======
>>>>>>> branch-2.5-mrs
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil.findArithmeticFunction;
import static java.util.function.UnaryOperator.identity;

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
            .add(FunctionSet.BITMAP_UNION)
            .add(FunctionSet.HLL_UNION)
            .add(FunctionSet.PERCENTILE_UNION)
            .build();

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
    protected OptExpression viewBasedRewrite(RewriteContext rewriteContext, OptExpression mvOptExpr) {
        LogicalAggregationOperator mvAggOp = (LogicalAggregationOperator) rewriteContext.getMvExpression().getOp();
        LogicalAggregationOperator queryAggOp = (LogicalAggregationOperator) rewriteContext.getQueryExpression().getOp();

        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        List<ScalarOperator> mvGroupingKeys =
                rewriteGroupByKeys(rewriteContext, columnRewriter, mvAggOp.getGroupingKeys(), true);
        List<ScalarOperator> queryGroupingKeys =
                rewriteGroupByKeys(rewriteContext, columnRewriter, queryAggOp.getGroupingKeys(), false);
        ScalarOperator queryRangePredicate = rewriteContext.getQueryPredicateSplit().getRangePredicates();
        boolean isRollup = isRollupAggregate(mvGroupingKeys, queryGroupingKeys, queryRangePredicate);

        // Cannot ROLLUP distinct
<<<<<<< HEAD
        if (isRollup) {
            boolean mvHasDistinctAggFunc = 
                    mvAggOp.getAggregations().values().stream().anyMatch(callOp -> callOp.isDistinct());
            boolean queryHasDistinctAggFunc = 
                    queryAggOp.getAggregations().values().stream().anyMatch(callOp -> callOp.isDistinct());
            if (mvHasDistinctAggFunc && queryHasDistinctAggFunc) {
                logMVRewrite(mvRewriteContext, "Rollup aggregate cannot contain distinct aggregate functions," +
                        "mv:%s, query:%s", mvAggOp.getAggregations().values(), queryAggOp.getAggregations().values());
                return null;
            }
=======
        if (isRollup && !canRewriteForRollup(mvAggOp)) {
            return null;
>>>>>>> branch-2.5-mrs
        }

        // normalize mv's aggs by using query's table ref and query ec
        Map<ColumnRefOperator, ScalarOperator> mvProjection =
                MvUtils.getColumnRefMap(rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());
        // normalize view projection by query relation and ec
        EquationRewriter queryExprToMvExprRewriter =
                buildEquationRewriter(mvProjection, rewriteContext, false);

        if (isRollup) {
<<<<<<< HEAD
            return rewriteForRollup(queryAggOp, queryGroupingKeys, columnRewriter, queryExprToMvExprRewriter,
                    rewriteContext, mvOptExpr);
        } else {
            return rewriteProjection(rewriteContext, queryAggOp, queryExprToMvExprRewriter, mvOptExpr);
        }
    }

    protected OptExpression rewriteProjection(RewriteContext rewriteContext,
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
        AggregateFunctionRewriter aggregateFunctionRewriter = new AggregateFunctionRewriter(rewriteContext.getQueryRefFactory(),
                oldAggregations);
        ColumnRefSet originalColumnSet = new ColumnRefSet(rewriteContext.getQueryColumnSet());

        // rewrite group by + aggregate functions
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : swappedQueryColumnMap.entrySet()) {
            ScalarOperator scalarOp = entry.getValue();
            ScalarOperator rewritten = rewriteScalarOperator(entry.getValue(),
                        queryExprToMvExprRewriter, rewriteContext.getOutputMapping(),
                        originalColumnSet, aggregateFunctionRewriter);
            if (rewritten == null) {
                logMVRewrite(mvRewriteContext, "Rewrite aggregate group-by/agg expr failed: %s", scalarOp.toString());
                return null;
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
                ScalarOperator rewritten = rewriteScalarOperator(entry.getValue(),
                        queryExprToMvExprRewriter, rewriteContext.getOutputMapping(),
                        originalColumnSet, aggregateFunctionRewriter);
                if (rewritten == null) {
                    logMVRewrite(mvRewriteContext, "Rewrite aggregate with having expr failed: %s", scalarOp.toString());
                    return null;
                }
                queryColumnRefToScalarMap.put(entry.getKey(), rewritten);
            }
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(queryColumnRefToScalarMap);
            ScalarOperator aggPredicate = queryAggregationOperator.getPredicate();
            ScalarOperator rewrittenPred = rewriter.rewrite(aggPredicate);
            if (rewrittenPred == null) {
                logMVRewrite(mvRewriteContext, "Rewrite aggregate wth having failed, " +
                        "cannot compensate aggregate having predicates: %s", queryAggregationOperator.getPredicate().toString());
                return null;
            }
            LogicalScanOperator scanOperator = mvOptExpr.getOp().cast();
            Operator.Builder builder = OperatorBuilderFactory.build(scanOperator);
            builder.withOperator(scanOperator);
            // take care original scan predicates and new having exprs
            builder.setPredicate(Utils.compoundAnd(rewrittenPred, scanOperator.getPredicate()));
            mvOptExpr = OptExpression.create(builder.build());
=======
            return rewriteForRollup(queryAggOp, queryGroupingKeys,
                    columnRewriter,
                    queryExprToMvExprRewriter, rewriteContext, mvOptExpr);
        } else {
            // Add aggregate's predicate compensation here because aggregate predicates should be taken care
            // by self.
            if (queryAggOp.getPredicate() != null) {
                ScalarOperator rewrittenPred =
                        queryExprToMvExprRewriter.replaceExprWithTarget(queryAggOp.getPredicate());
                if (rewrittenPred == null || rewrittenPred.equals(queryAggOp.getPredicate())) {
                    return null;
                }
                mvOptExpr = OptExpression.create(new LogicalFilterOperator(rewrittenPred), mvOptExpr);
            }
            return rewriteProjection(rewriteContext, queryExprToMvExprRewriter, mvOptExpr);
>>>>>>> branch-2.5-mrs
        }

        return mvOptExpr;
    }

    private ScalarOperator rewriteScalarOperator(ScalarOperator scalarOp,
                                                 EquationRewriter equationRewriter,
                                                 Map<ColumnRefOperator, ColumnRefOperator> columnMapping,
                                                 ColumnRefSet originalColumnSet,
                                                 AggregateFunctionRewriter aggregateFunctionRewriter) {
        if (scalarOp.isConstantRef()) {
            return scalarOp;
        }
        equationRewriter.setAggregateFunctionRewriter(aggregateFunctionRewriter);
        equationRewriter.setOutputMapping(columnMapping);
        ScalarOperator rewritten = equationRewriter.replaceExprWithTarget(scalarOp);
        if (rewritten == null || scalarOp == rewritten) {
            return null;
        }
        if (!isAllExprReplaced(rewritten, originalColumnSet)) {
            // it means there is some column that can not be rewritten by outputs of mv
            return null;
        }
        return rewritten;
    }

    private boolean canRewriteForRollup(LogicalAggregationOperator aggOp) {
        return aggOp.getAggregations().values().stream().noneMatch(callOp -> callOp.isDistinct());
    }

    // NOTE: this method is not exactly right to check whether it's a rollup aggregate:
    // - all matched group by keys bit is less than mvGroupByKeys
    // - if query contains one non-mv-existed agg, set it `rollup` and use `replaceExprWithTarget` to
    //    - check whether to rewrite later.
    private boolean isRollupAggregate(List<ScalarOperator> mvGroupingKeys, List<ScalarOperator> queryGroupingKeys,
                                      ScalarOperator queryRangePredicate) {
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


    private Map<ColumnRefOperator, ScalarOperator> rewriteAggregations(RewriteContext rewriteContext,
                                                                       ColumnRewriter columnRewriter,
                                                                       Map<ColumnRefOperator, CallOperator> aggregations,
                                                                       boolean rewriteViewToQuery) {
        Map<ColumnRefOperator, ScalarOperator> rewriteAggregations = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregations.entrySet()) {
            if (rewriteViewToQuery) {
                ScalarOperator rewriteAgg = rewriteContext.getMvColumnRefRewriter().rewrite(entry.getValue().clone());
                rewriteAggregations.put(entry.getKey(), columnRewriter.rewriteViewToQueryWithQueryEc(rewriteAgg));
            } else {
                ScalarOperator rewriteAgg = rewriteContext.getQueryColumnRefRewriter().rewrite(entry.getValue().clone());
                rewriteAggregations.put(entry.getKey(), columnRewriter.rewriteByQueryEc(rewriteAgg));
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
<<<<<<< HEAD
            EquationRewriter equationRewriter,
=======
            EquationRewriter queryExprToMvExprRewriter,
>>>>>>> branch-2.5-mrs
            RewriteContext rewriteContext,
            OptExpression mvOptExpr) {
        Map<ColumnRefOperator, ScalarOperator> queryColumnRefToScalarMap = Maps.newHashMap();

        // rewrite group by keys by using mv
        List<ScalarOperator> newQueryGroupKeys = rewriteGroupKeys(
<<<<<<< HEAD
                queryGroupingKeys, equationRewriter, rewriteContext.getOutputMapping(),
=======
                queryGroupingKeys, queryExprToMvExprRewriter, rewriteContext.getOutputMapping(),
>>>>>>> branch-2.5-mrs
                new ColumnRefSet(rewriteContext.getQueryColumnSet()));
        if (newQueryGroupKeys == null) {
            logMVRewrite(mvRewriteContext, "Rewrite rollup aggregate failed: cannot rewrite group by keys");
            return null;
        }
        List<ColumnRefOperator> queryGroupKeys = queryAggOp.getGroupingKeys();
        Preconditions.checkState(queryGroupKeys.size() == newQueryGroupKeys.size());
        for (int i = 0; i < newQueryGroupKeys.size(); i++) {
            queryColumnRefToScalarMap.put(queryGroupKeys.get(i), newQueryGroupKeys.get(i));
        }

<<<<<<< HEAD
        // rewrite agg func to be better for rollup.
=======
        // rewrite query agg operator: rewrite avg to sum/ count
>>>>>>> branch-2.5-mrs
        LogicalAggregationOperator rewrittenQueryAggOp =
                rewriteAggregationOperatorByRules(rewriteContext.getQueryRefFactory(), queryAggOp);
        Map<ColumnRefOperator, ScalarOperator> queryAggregation = rewriteAggregations(
                rewriteContext, columnRewriter, rewrittenQueryAggOp.getAggregations(), false);
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : rewrittenQueryAggOp.getAggregations().entrySet()) {
            ColumnRefOperator newAggColumnRef = entry.getKey();
            ColumnRefOperator rewriteAggColumnRef = rewriteContext.getQueryRefFactory().create(
                    newAggColumnRef, newAggColumnRef.getType(), newAggColumnRef.isNullable());
            queryColumnRefToScalarMap.put(entry.getKey(), rewriteAggColumnRef);
        }
        // generate new agg exprs(rollup functions)
        Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregates(
<<<<<<< HEAD
                queryAggregation, equationRewriter, rewriteContext.getOutputMapping(),
=======
                queryAggregation, queryExprToMvExprRewriter, rewriteContext.getOutputMapping(),
>>>>>>> branch-2.5-mrs
                new ColumnRefSet(rewriteContext.getQueryColumnSet()), queryColumnRefToScalarMap);
        if (newAggregations == null) {
            logMVRewrite(mvRewriteContext, "Rewrite rollup aggregate failed: cannot rewrite aggregate functions");
            return null;
        }

        return createNewAggregate(rewriteContext, rewrittenQueryAggOp, newAggregations, queryColumnRefToScalarMap, mvOptExpr);
    }

    @Override
    protected OptExpression queryBasedRewrite(RewriteContext rewriteContext, ScalarOperator compensationPredicates,
                                              OptExpression queryExpression) {
        OptExpression child = super.queryBasedRewrite(rewriteContext, compensationPredicates, queryExpression.inputAt(0));
        if (child == null) {
            return null;
        }
        return OptExpression.create(queryExpression.getOp(), child);
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
            logMVRewrite(mvRewriteContext, "Rewrite aggregate with union failed: aggregate with projection is " +
                    "still not supported");
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
            logMVRewrite(mvRewriteContext, "Rewrite aggregate with union failed: rewrite aggregate for union failed");
            return null;
        }
        return createNewAggregate(rewriteContext, queryAgg, newAggregations, aggregateMapping, unionExpr);
    }

    private OptExpression createNewAggregate(
            RewriteContext rewriteContext,
            LogicalAggregationOperator queryAgg,
            Map<ColumnRefOperator, CallOperator> newAggregations,
            Map<ColumnRefOperator, ScalarOperator> queryColumnRefToScalarMap,
            OptExpression mvOptExpr) {
        // newGroupKeys may have duplicate because of EquivalenceClasses
        // remove duplicate here as new grouping keys
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        boolean isAllGroupByKeyColumnRefs = originalGroupKeys.stream()
                .map(key -> queryColumnRefToScalarMap.get(key))
                .allMatch(ColumnRefOperator.class::isInstance);

        List<ColumnRefOperator> newGroupByKeyColumnRefs = Lists.newArrayList();
        // If not all group by keys are column ref, need add new project to input expr.
        if (!isAllGroupByKeyColumnRefs) {
            // Push group by scalar operators into new projection node.
            Map<ColumnRefOperator, ScalarOperator> newQueryProjection = mvOptExpr.getOutputColumns().getStream()
                    .map(id -> rewriteContext.getQueryRefFactory().getColumnRef(id))
                    .collect(Collectors.toMap(identity(), identity()));
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
        Map<ColumnRefOperator, ScalarOperator> newProjection = Maps.newHashMap();
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
    private List<ScalarOperator> rewriteGroupKeys(List<ScalarOperator> groupKeys,
<<<<<<< HEAD
                                                  EquationRewriter equationRewriter,
=======
                                                  EquationRewriter queryExprToMvExprRewriter,
>>>>>>> branch-2.5-mrs
                                                  Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                  ColumnRefSet queryColumnSet) {
        List<ScalarOperator> newGroupByKeys = Lists.newArrayList();
        equationRewriter.setOutputMapping(mapping);
        for (ScalarOperator key : groupKeys) {
<<<<<<< HEAD
            ScalarOperator newGroupByKey = equationRewriter.replaceExprWithTarget(key);
            if (key.isVariable() && key == newGroupByKey) {
                logMVRewrite(mvRewriteContext, "Rewrite group by key %s failed", key.toString());
=======
            ScalarOperator newGroupByKey = replaceExprWithTarget(key, queryExprToMvExprRewriter, mapping);
            if (key.isVariable() && key == newGroupByKey) {
>>>>>>> branch-2.5-mrs
                return null;
            }
            if (newGroupByKey == null || !isAllExprReplaced(newGroupByKey, queryColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                logMVRewrite(mvRewriteContext, "Rewrite group by key %s failed: partially rewrite", key.toString());
                return null;
            }
            newGroupByKeys.add(newGroupByKey);
        }
        return newGroupByKeys;
    }

    /**
     * Rewrite aggregation by using MV.
     */
    private Map<ColumnRefOperator, CallOperator> rewriteAggregates(Map<ColumnRefOperator, ScalarOperator> aggregates,
<<<<<<< HEAD
                                                                   EquationRewriter equationRewriter,
=======
                                                                   EquationRewriter normalizedViewMap,
>>>>>>> branch-2.5-mrs
                                                                   Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                                   ColumnRefSet queryColumnSet,
                                                                   Map<ColumnRefOperator, ScalarOperator> aggregateMapping) {
        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        equationRewriter.setOutputMapping(mapping);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : aggregates.entrySet()) {
            Preconditions.checkState(entry.getValue() instanceof CallOperator);
            CallOperator aggCall = (CallOperator) entry.getValue();
            ScalarOperator targetColumn = equationRewriter.replaceExprWithTarget(aggCall);
            if (targetColumn == null || !isAllExprReplaced(targetColumn, queryColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                logMVRewrite(mvRewriteContext, "Rewrite aggregate %s failed: partially rewrite", aggCall.toString());
                return null;
            }
            // TODO: Support non column-ref agg function.
            if (!(targetColumn instanceof ColumnRefOperator)) {
                logMVRewrite(mvRewriteContext, "Rewrite aggregate %s failed: only column-ref is supported after rewrite",
                        aggCall.toString());
                return null;
            }
            // Aggregate must be CallOperator
            CallOperator newAggregate = getRollupAggregate(aggCall, (ColumnRefOperator) targetColumn);
            if (newAggregate == null) {
                logMVRewrite(mvRewriteContext, "Rewrite aggregate %s failed: cannot get rollup aggregate",
                        aggCall.toString());
                return null;
            }
            ColumnRefOperator oldColRef = (ColumnRefOperator) aggregateMapping.get(entry.getKey());
            newAggregations.put(oldColRef, newAggregate);
        }

        return newAggregations;
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
                logMVRewrite(mvRewriteContext, "Rewrite aggregate %s for union failed: target column is null",
                        aggCall.toString());
                return null;
            }
            // Aggregate must be CallOperator
            CallOperator newAggregate = getRollupAggregate(aggCall, targetColumn);
            if (newAggregate == null) {
                logMVRewrite(mvRewriteContext, "Rewrite aggregate %s failed: cannot get rollup aggregate",
                        aggCall.toString());
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
<<<<<<< HEAD
            // NOTE:
            // 1. Change fn's type  as 1th child has change, otherwise physical plan
            // will still use old arg input's type.
            // 2. the rollup function is the same as origin, but use the new column as argument
            Function newFunc = aggCall.getFunction()
                    .updateArgType(new Type[] {targetColumn.getType()});
            return new CallOperator(aggCall.getFnName(), aggCall.getType(), Lists.newArrayList(targetColumn),
                    newFunc);
=======
            // the rollup function is the same as origin, but use the new column as argument
            CallOperator newAggCall = (CallOperator) aggCall.clone();
            newAggCall.setChild(0, targetColumn);
            // NOTE: Change fn's type  as 1th child has change, otherwise physical plan
            // will still use old arg input's type.
            newAggCall.getFunction().setArgsType(new Type[] { targetColumn.getType() });
            return newAggCall;
>>>>>>> branch-2.5-mrs
        }
    }

    // Rewrite query agg operator by rule:
    //  - now only support rewrite avg to sum/ count
    private LogicalAggregationOperator rewriteAggregationOperatorByRules(
            ColumnRefFactory queryColumnRefFactory,
            LogicalAggregationOperator aggregationOperator) {
        Map<ColumnRefOperator, CallOperator> oldAggregations = aggregationOperator.getAggregations();
        final Map<ColumnRefOperator, CallOperator> newColumnRefToAggFuncMap = Maps.newHashMap();
        final AggregateFunctionRewriter aggFuncRewriter = new AggregateFunctionRewriter(queryColumnRefFactory,
                oldAggregations, newColumnRefToAggFuncMap);
        if (!oldAggregations.values().stream().anyMatch(func ->
                aggFuncRewriter.canRewriteAggFunction(func))) {
            return aggregationOperator;
        }

        final Map<ColumnRefOperator, ScalarOperator> newProjection = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggEntry : oldAggregations.entrySet()) {
            CallOperator aggFunc = aggEntry.getValue();
            if (aggFuncRewriter.canRewriteAggFunction(aggFunc)) {
                CallOperator newAggFunc = (CallOperator) aggFuncRewriter.rewriteAggFunction(aggFunc);
                newProjection.put(aggEntry.getKey(), newAggFunc);
            } else {
                newProjection.put(aggEntry.getKey(), aggEntry.getKey());
                newColumnRefToAggFuncMap.put(aggEntry.getKey(), aggEntry.getValue());
            }
        }
        // Copy group by keys as projection
        aggregationOperator.getGroupingKeys().forEach(c -> newProjection.put(c, c));
        // Copy original projection mappings.
        if (aggregationOperator.getProjection() != null) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(newProjection);
            aggregationOperator.getProjection().getColumnRefMap().forEach((k, v) ->
                    newProjection.put(k, rewriter.rewrite(v)));
        }

        // Make a new logical agg with new projections.
        LogicalAggregationOperator newAggOp = LogicalAggregationOperator.builder()
                .withOperator(aggregationOperator)
                .setType(AggType.GLOBAL)
                .setGroupingKeys(aggregationOperator.getGroupingKeys())
                .setAggregations(newColumnRefToAggFuncMap)
                .setProjection(new Projection(newProjection))
                .build();

        return newAggOp;
    }

    // Rewrite query agg operator by rule:
    //  - now only support rewrite avg to sum/ count
    // TODO: maybe we can rewrite the whole query before MV's rewrite.
    private LogicalAggregationOperator rewriteAggregationOperatorByRules(
            ColumnRefFactory queryColumnRefFactory,
            LogicalAggregationOperator aggregationOperator) {
        Map<ColumnRefOperator, CallOperator> oldAggregations = aggregationOperator.getAggregations();
        if (oldAggregations.values().stream().allMatch(x -> !x.getFnName().equals(FunctionSet.AVG))) {
            return aggregationOperator;
        }

        final Map<ColumnRefOperator, CallOperator> newAggMap = Maps.newHashMap();
        final Map<ColumnRefOperator, ScalarOperator> projections = new HashMap<>();
        final ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggEntry : oldAggregations.entrySet()) {
            CallOperator oldFunctionCall = aggEntry.getValue();
            if (oldFunctionCall.getFunction().getFunctionName().getFunction().equals(FunctionSet.AVG)) {
                Type argType = oldFunctionCall.getChild(0).getType();

                // construct `sum` agg
                Function sumFn = findArithmeticFunction(oldFunctionCall.getFunction().getArgs(), FunctionSet.SUM);
                Preconditions.checkState(sumFn != null);
                Type sumReturnType;
                if (argType.isDecimalV3()) {
                    sumReturnType =
                            ScalarType.createDecimalV3NarrowestType(38, ((ScalarType) argType).getScalarScale());
                } else {
                    sumReturnType = sumFn.getReturnType();
                }
                sumFn.setArgsType(oldFunctionCall.getFunction().getArgs());
                sumFn.setRetType(sumReturnType);
                Pair<ColumnRefOperator, CallOperator> sumCallOp =
                        createNewCallOperator(queryColumnRefFactory, sumFn, oldFunctionCall.getChildren(), oldAggregations);

                Function countFn = findArithmeticFunction(oldFunctionCall.getFunction().getArgs(), FunctionSet.COUNT);
                Pair<ColumnRefOperator, CallOperator> countCallOp = createNewCallOperator(queryColumnRefFactory,
                        countFn, oldFunctionCall.getChildren(), oldAggregations);

                // add sum/count agg into aggregations map
                newAggMap.put(sumCallOp.first, sumCallOp.second);
                newAggMap.put(countCallOp.first, countCallOp.second);

                // add sum/count into projection
                CallOperator newAvg = new CallOperator(FunctionSet.DIVIDE, oldFunctionCall.getType(),
                        Lists.newArrayList(sumCallOp.first, countCallOp.first));
                if (argType.isDecimalV3()) {
                    // There is not need to apply ImplicitCastRule to divide operator of decimal types.
                    // but we should cast BIGINT-typed countColRef into DECIMAL(38,0).
                    ScalarType decimal128p38s0 = ScalarType.createDecimalV3NarrowestType(38, 0);
                    newAvg.getChildren().set(1, new CastOperator(decimal128p38s0, newAvg.getChild(1), true));
                } else {
                    newAvg = (CallOperator) scalarRewriter.rewrite(newAvg, Lists.newArrayList(new ImplicitCastRule()));
                }
                projections.put(aggEntry.getKey(), newAvg);
            } else {
                projections.put(aggEntry.getKey(), aggEntry.getKey());
                newAggMap.put(aggEntry.getKey(), aggEntry.getValue());
            }
        }

        aggregationOperator.getGroupingKeys().forEach(c -> projections.put(c, c));
        LogicalAggregationOperator newAggOp =
                new LogicalAggregationOperator(AggType.GLOBAL, aggregationOperator.getGroupingKeys(), newAggMap);
        newAggOp.setProjection(new Projection(projections));
        return newAggOp;
    }
    private Pair<ColumnRefOperator, CallOperator> createNewCallOperator(ColumnRefFactory queryColumnRefFactory,
                                                                        Function newFn,
                                                                        List<ScalarOperator> args,
                                                                        Map<ColumnRefOperator, CallOperator> oldAggregations) {
        Preconditions.checkState(newFn != null);
        CallOperator newCallOp = new CallOperator(newFn.functionName(), newFn.getReturnType(), args, newFn);
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : oldAggregations.entrySet()) {
            if (entry.getValue().equals(newCallOp))  {
                return Pair.create(entry.getKey(), newCallOp);
            }
        }
        ColumnRefOperator newColRef =
                queryColumnRefFactory.create(newCallOp, newCallOp.getType(), newCallOp.isNullable());
        return Pair.create(newColRef, newCallOp);
    }
}
