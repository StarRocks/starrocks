// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public AggregatedMaterializedViewRewriter(Triple<ScalarOperator, ScalarOperator, ScalarOperator> queryPredicateTriple,
                                              EquivalenceClasses queryEc, LogicalProjectOperator queryProjection,
                                              OptExpression query, List<Table> queryTableSet, List<Table> mvTableSet,
                                              MaterializationContext materializationContext,
                                              OptimizerContext optimizerContext) {
        super(queryPredicateTriple, queryEc, queryProjection, query,
                queryTableSet, mvTableSet, materializationContext, optimizerContext);
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
        // TODO: 是否支持grouping set/rollup/cube
        return RewriteUtils.isLogicalSPJG(expression.inputAt(0));
    }

    @Override
    public List<OptExpression> rewrite() {
        // the rewritten expression to replace query
        // TODO: generate scan mv operator by ColumnRefFactory of query
        OptExpression rewrittenExpression = OptExpression.create(materializationContext.getScanMvOperator());
        deriveLogicalProperty(rewrittenExpression);

        LogicalProjectOperator mvTopProjection;
        OptExpression mvExpression;
        if (materializationContext.getMvExpression().getOp() instanceof LogicalProjectOperator) {
            mvTopProjection = (LogicalProjectOperator) materializationContext.getMvExpression().getOp();
            mvExpression = materializationContext.getMvExpression().getInputs().get(0);
        } else {
            mvTopProjection = null;
            mvExpression = materializationContext.getMvExpression();
        }
        if (!isValidPlan(mvExpression)) {
            return null;
        }
        List<ScalarOperator> mvConjuncts = RewriteUtils.getAllPredicates(mvExpression);
        ScalarOperator mvPredicate = Utils.compoundAnd(mvConjuncts);
        final Triple<ScalarOperator, ScalarOperator, ScalarOperator> mvPredicateTriple =
                RewriteUtils.splitPredicateToTriple(mvPredicate);

        boolean isQueryAllEqualJoin = RewriteUtils.isAllEqualInnerJoin(query);
        boolean isMvAllEqualJoin = RewriteUtils.isAllEqualInnerJoin(mvExpression);
        MatchMode matchMode = MatchMode.NOT_MATCH;
        if (isQueryAllEqualJoin && isMvAllEqualJoin) {
            // process use table match
            // TODO: should not use queryTableSet, should use list, and from ColumnRefFactory table values list
            if (queryTableSet.size() == mvTableSet.size() && queryTableSet.containsAll(mvTableSet)) {
                matchMode = MatchMode.COMPLETE;
            } else if (queryTableSet.containsAll(mvTableSet)) {
                // TODO: query delta
                matchMode = MatchMode.QUERY_DELTA;
                return null;
            } else if (mvTableSet.containsAll(queryTableSet)) {
                // TODO: view delta
                matchMode = MatchMode.VIEW_DELTA;
                return null;
            } else {
                // can not be rewritten, return null
                return null;
            }
        } else {
            // TODO: process for outer join and not equal join
            // check whether query can be rewritten
            if (mvTableSet.size() > queryTableSet.size()) {
                // check view delta
                return null;
            } else {
                // check table joins' type must match
                return null;
            }
        }

        Map<Integer, List<ColumnRefOperator>> queryRelationIdToColumns =
                getRelationIdToColumns(optimizerContext.getColumnRefFactory());

        Map<Integer, List<ColumnRefOperator>> mvRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getMvColumnRefFactory());

        // for query: A1 join A2 join B, mv A1 join A2 join B
        // there may be two mapping:
        //    1. A1 -> A1, A2 -> A2, B -> B
        //    2. A1 -> A2, A2 -> A1, B -> B
        List<BiMap<Integer, Integer>> relationIdMappings = generateRelationIdMap(optimizerContext.getColumnRefFactory(),
                materializationContext.getMvColumnRefFactory());
        List<OptExpression> results = Lists.newArrayList();
        for (BiMap<Integer, Integer> relationIdMapping : relationIdMappings) {
            // construct query based view EC
            EquivalenceClasses queryBaseViewEc = new EquivalenceClasses();
            if (mvPredicateTriple.getLeft() != null) {
                ScalarOperator queryBasedViewEqualPredicate = rewriteColumnByRelationIdMap(mvPredicateTriple.getLeft(),
                        relationIdMapping, queryRelationIdToColumns, optimizerContext.getColumnRefFactory(),
                        mvRelationIdToColumns, materializationContext.getMvColumnRefFactory());
                for (ScalarOperator conjunct : Utils.extractConjuncts(queryBasedViewEqualPredicate)) {
                    queryBaseViewEc.addEquivalence((ColumnRefOperator) conjunct.getChild(0),
                            (ColumnRefOperator) conjunct.getChild(1));
                }
            }

            Pair<ScalarOperator, ScalarOperator> compensationPredicates =
                    getCompensationPredicates(queryEc, queryPredicateTriple, optimizerContext.getColumnRefFactory(),
                            queryBaseViewEc, mvPredicateTriple, materializationContext.getMvColumnRefFactory(),
                            relationIdMapping, queryRelationIdToColumns, mvRelationIdToColumns);
            if (compensationPredicates == null) {
                // TODO: should try to rewrite by union
                return null;
            } else {
                // generate projection
                // add compensation predicate to plan
                // 现在compensationPredicates都是query的，并且是基于query的ec的表达式，如何能够变成view上的表达式
                // 第一步，判断compensation predicates需要的列都在view上存在
                // 第二步：构造filter节点，尝试谓词下推
                // 第三步：构造projection表达式
                ScalarOperator left = RewriteUtils.canonizeNode(compensationPredicates.first);
                ScalarOperator right = RewriteUtils.canonizeNode(compensationPredicates.second);
                if (!isAlwaysTrue(left) || !isAlwaysTrue(right)) {
                    Map<ColumnRefOperator, ScalarOperator> viewExprMap =
                            getProjectionMap(mvTopProjection, mvExpression, materializationContext.getMvColumnRefFactory());

                    if (!isAlwaysTrue(left)) {
                        List<ScalarOperator> conjuncts = Utils.extractConjuncts(left);
                        List<ScalarOperator> rewritten = rewriteScalarOperators(conjuncts, viewExprMap, rewrittenExpression,
                                relationIdMapping.inverse(), mvRelationIdToColumns,
                                materializationContext.getMvColumnRefFactory(), queryRelationIdToColumns,
                                optimizerContext.getColumnRefFactory(), queryBaseViewEc);
                        if (rewritten == null) {
                            continue;
                        }
                        // TODO: consider normalizing it
                        left = Utils.compoundAnd(rewritten);
                    }

                    if (!isAlwaysTrue(right)) {
                        List<ScalarOperator> conjuncts = Utils.extractConjuncts(right);
                        List<ScalarOperator> rewritten = rewriteScalarOperators(conjuncts, viewExprMap, rewrittenExpression,
                                relationIdMapping.inverse(), mvRelationIdToColumns,
                                materializationContext.getMvColumnRefFactory(), queryRelationIdToColumns,
                                optimizerContext.getColumnRefFactory(), queryEc);
                        if (rewritten == null) {
                            continue;
                        }
                        right = Utils.compoundAnd(rewritten);
                    }
                }
                ScalarOperator compensationPredicate = RewriteUtils.canonizeNode(Utils.compoundAnd(left, right));
                if (!isAlwaysTrue(compensationPredicate)) {
                    // add filter operator
                    LogicalFilterOperator filter = new LogicalFilterOperator(compensationPredicate);
                    rewrittenExpression = OptExpression.create(filter, rewrittenExpression);
                    deriveLogicalProperty(rewrittenExpression);
                }

                // rewrite aggregation
                rewrittenExpression = rewriteAggregation(rewrittenExpression, query, mvExpression, mvTopProjection,
                        relationIdMapping.inverse(), queryRelationIdToColumns, optimizerContext.getColumnRefFactory(),
                        mvRelationIdToColumns, materializationContext.getMvColumnRefFactory(), queryEc);
                if (rewrittenExpression == null) {
                    continue;
                }

                /*
                // add projection operator
                // TODO: should add projection to map group keys
                rewrittenExpression = rewriteProjection(queryProjection, query, mvTopProjection, mvExpression,
                        rewrittenExpression, relationIdMapping.inverse(), mvRelationIdToColumns,
                        materializationContext.getMvColumnRefFactory(), queryRelationIdToColumns,
                        optimizerContext.getColumnRefFactory(), queryEc);

                if (rewrittenExpression == null) {
                    continue;
                }

                 */
                results.add(rewrittenExpression);

            }
        }
        return results;
    }

    // get projection lineage of expression
    // used to compute the equality of group key and aggregate
    Map<ColumnRefOperator, ScalarOperator> getLineage(OptExpression expression, ColumnRefFactory refFactory) {
        LineageFactory factory = new LineageFactory(expression, refFactory);
        return factory.getLineage();
    }

    // consider rollup or not.
    // should consider aggregate function
    // 支持key进行表达式计算
    // relationIdMap: mv -> query
    private OptExpression rewriteAggregation(OptExpression rewrittenExpression, OptExpression query, OptExpression mv,
                                             LogicalProjectOperator mvTopProjection, Map<Integer, Integer> relationIdMap,
                                             Map<Integer, List<ColumnRefOperator>> queryRelationIdToColumns,
                                             ColumnRefFactory queryRefFactory,
                                             Map<Integer, List<ColumnRefOperator>> mvRelationIdToColumns,
                                             ColumnRefFactory mvRefFactory, EquivalenceClasses ec) {
        // should judge whether it is a rollup
        LogicalAggregationOperator mvAgg = (LogicalAggregationOperator) mv.getOp();
        Map<ColumnRefOperator, ScalarOperator> mvLineage = getLineage(mv, mvRefFactory);
        ReplaceColumnRefRewriter mvColumnRefRewriter = new ReplaceColumnRefRewriter(mvLineage, true);

        // TODO: consider clone expr
        List<ScalarOperator> normalizedMvKeys = Lists.newArrayList();
        for (ColumnRefOperator key : mvAgg.getGroupingKeys()) {
            ScalarOperator rewritten = mvColumnRefRewriter.rewrite(key.clone());
            ScalarOperator normalized = rewriteColumnByRelationIdMapAndEc(rewritten, relationIdMap, mvRelationIdToColumns,
                    mvRefFactory, queryRelationIdToColumns, queryRefFactory, ec);
            normalizedMvKeys.add(normalized);
        }

        // 是否需要index之间的映射？
        LogicalAggregationOperator queryAgg = (LogicalAggregationOperator) query.getOp();
        Map<ColumnRefOperator, ScalarOperator> queryLineage = getLineage(query, queryRefFactory);
        ReplaceColumnRefRewriter queryColumnRefRewriter = new ReplaceColumnRefRewriter(queryLineage, true);
        List<ScalarOperator> queryGroupingKeys = Lists.newArrayList();
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        for (ColumnRefOperator key : originalGroupKeys) {
            ScalarOperator op = queryColumnRefRewriter.rewrite(key.clone());
            ScalarOperator normalizedOp = rewriteColumnByEc(op, ec);
            queryGroupingKeys.add(normalizedOp);
        }

        // 判断queryGroupingKeys是否是mvGroupingKeys的子集
        // 支持rollup计算
        // 将view的group keys改写到query上，使用query的ec（因为这个时候已经计算完补偿谓词，会将缺少的等价类加到view中)
        // normalize mv's group keys by using query's table ref and query ec
        List<ScalarOperator> distinctMvKeys = normalizedMvKeys.stream().distinct().collect(Collectors.toList());
        GroupKeyChecker groupKeyChecker = new GroupKeyChecker(distinctMvKeys);
        boolean keyMatched = groupKeyChecker.check(queryGroupingKeys);
        if (!keyMatched) {
            return null;
        }

        // check aggregate of query
        // normalize mv's aggs by using query's table ref and query ec
        List<ScalarOperator> normalizedMvAggs = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggEntry : mvAgg.getAggregations().entrySet()) {
            ScalarOperator rewritten = mvColumnRefRewriter.rewrite(aggEntry.getValue().clone());
            ScalarOperator swapped = rewriteColumnByRelationIdMapAndEc(rewritten, relationIdMap, mvRelationIdToColumns,
                    mvRefFactory, queryRelationIdToColumns, queryRefFactory, ec);
            normalizedMvAggs.add(swapped);
        }

        Map<ColumnRefOperator, ScalarOperator> queryAggs = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggEntry : queryAgg.getAggregations().entrySet()) {
            ScalarOperator rewritten = queryColumnRefRewriter.rewrite(aggEntry.getValue().clone());
            ScalarOperator swapped = rewriteColumnByEc(rewritten, ec);
            queryAggs.put(aggEntry.getKey(), swapped);
        }

        AggregateChecker aggregateChecker = new AggregateChecker(normalizedMvAggs);
        boolean queryMatched = aggregateChecker.check(queryAggs.values().stream().collect(Collectors.toList()));
        if (!queryMatched) {
            return null;
        }

        // TODO: consider more rollup cases
        Map<ColumnRefOperator, ColumnRefOperator> mapping = Maps.newHashMap();
        for (int i = 0; i < materializationContext.getMvOutputExpressions().size(); i++) {
            mapping.put(materializationContext.getMvOutputExpressions().get(i),
                    materializationContext.getScanMvOutputExpressions().get(i));
        }
        Set<ColumnRefOperator> originalColumnSet = queryRelationIdToColumns.values()
                .stream().flatMap(List::stream).collect(Collectors.toSet());
        originalColumnSet.removeAll(materializationContext.getScanMvOutputExpressions());
        boolean isRollup = groupKeyChecker.isRollup();
        if (isRollup) {
            // should add group by to rewritten expression
            if (aggregateChecker.hasDistinct()) {
                // can not support rollup of disctinct type
                return null;
            }
            // generate group keys
            // generate new agg exprs(rollup functions)
            // 把group key和exprs改过去，改到scan mv的output上，才算成功。然后保留Aggregate节点

            Map<ColumnRefOperator, ScalarOperator> mvProjection = getProjectionMap(mvTopProjection, mv, mvRefFactory);
            Map<ColumnRefOperator, ScalarOperator> swappedMvProjection = Maps.newHashMap();

            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : mvProjection.entrySet()) {
                ScalarOperator rewritten = mvColumnRefRewriter.rewrite(entry.getValue().clone());
                ScalarOperator normalized = rewriteColumnByRelationIdMapAndEc(rewritten, relationIdMap, mvRelationIdToColumns,
                        mvRefFactory, queryRelationIdToColumns, queryRefFactory, ec);
                swappedMvProjection.put(entry.getKey(), normalized);
            }
            List<ColumnRefOperator> newGroupKeys = rewriteGroupKeys(queryGroupingKeys, swappedMvProjection, mapping);
            if (newGroupKeys == null) {
                return null;
            }
            Preconditions.checkState(originalGroupKeys.size() == newGroupKeys.size());

            Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregates(
                    queryAggs, swappedMvProjection, mapping, originalColumnSet);

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
            OptExpression aggExpr = OptExpression.create(newAggOp, rewrittenExpression);
            // LogicalProjectOperator projectOperator = new LogicalProjectOperator(newProjection);
            // rewrittenExpression = OptExpression.create(projectOperator, aggExpr);
            // deriveLogicalProperty(rewrittenExpression);
            return aggExpr;
        } else {
            // should not add group by to rewritten expression
            // directly return original rewrittenExpression, which is going to process projection
            Map<ColumnRefOperator, ScalarOperator> queryMap = getProjectionMap(queryProjection, query, queryRefFactory);
            Map<ColumnRefOperator, ScalarOperator> swappedQueryColumnMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryMap.entrySet()) {
                ScalarOperator rewritten = queryColumnRefRewriter.rewrite(entry.getValue().clone());
                ScalarOperator swapped = rewriteColumnByEc(rewritten, ec);
                swappedQueryColumnMap.put(entry.getKey(), swapped);
            }

            Map<ColumnRefOperator, ScalarOperator> mvMap = getProjectionMap(mvTopProjection, mv, mvRefFactory);
            Map<ColumnRefOperator, ScalarOperator> swappedMvColumnMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : mvMap.entrySet()) {
                ScalarOperator rewritten = mvColumnRefRewriter.rewrite(entry.getValue().clone());
                ScalarOperator swapped = rewriteColumnByRelationIdMapAndEc(rewritten, relationIdMap, mvRelationIdToColumns,
                        mvRefFactory, queryRelationIdToColumns, queryRefFactory, ec);
                swappedMvColumnMap.put(entry.getKey(), swapped);
            }
            rewrittenExpression = rewriteProjectionForAgg(swappedQueryColumnMap,
                    swappedMvColumnMap, rewrittenExpression, mapping, originalColumnSet);
            return rewrittenExpression;
        }
    }

    // swapped query projection by lineage
    // swapped mv projection by lineage

    OptExpression rewriteProjectionForAgg(Map<ColumnRefOperator, ScalarOperator> queryProjection,
                                          Map<ColumnRefOperator, ScalarOperator> mvProjection,
                                          OptExpression rewrittenExpression,
                                          Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                          Set<ColumnRefOperator> originalColumnSet) {
        Multimap<ScalarOperator, ColumnRefOperator> reversed = ArrayListMultimap.create();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : mvProjection.entrySet()) {
            reversed.put(entry.getValue(), entry.getKey());
        }

        Map<ColumnRefOperator, ScalarOperator> newQueryProjection = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryProjection.entrySet()) {
            ScalarOperator rewritten = replaceExprWithTarget(entry.getValue(), reversed, mapping);
            if (!isAllExprReplaced(rewritten, originalColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                return null;
            }
            newQueryProjection.put(entry.getKey(), rewritten);
        }
        LogicalProjectOperator projectOperator = new LogicalProjectOperator(newQueryProjection);
        OptExpression projection = OptExpression.create(projectOperator, rewrittenExpression);
        return projection;
    }

    // mvLineage: swapped mv lineage
    // mvProjection: swapped mv projection
    List<ColumnRefOperator> rewriteGroupKeys(List<ScalarOperator> groupKeys,
                                          Map<ColumnRefOperator, ScalarOperator> mvProjection,
                                          Map<ColumnRefOperator, ColumnRefOperator> mapping) {
        Multimap<ScalarOperator, ColumnRefOperator> reversed = ArrayListMultimap.create();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : mvProjection.entrySet()) {
            reversed.put(entry.getValue(), entry.getKey());
        }

        List<ColumnRefOperator> rewrittens = Lists.newArrayList();
        // TODO: consider rollup
        for (ScalarOperator key : groupKeys) {
            if (reversed.containsKey(key)) {
                Optional refOpt = reversed.get(key).stream().findFirst();
                if (!refOpt.isPresent()) {
                    return null;
                }
                ColumnRefOperator found = mapping.get(refOpt.get());
                if (found == null) {
                    return null;
                }
                rewrittens.add(found);
            } else {
                // group key do not exists in mv
                // can not be rewritten
                return null;
            }
        }
        return rewrittens;
    }

    Map<ColumnRefOperator, CallOperator> rewriteAggregates(Map<ColumnRefOperator, ScalarOperator> aggregates,
                                          Map<ColumnRefOperator, ScalarOperator> mvProjection,
                                          Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                           Set<ColumnRefOperator> queryColumnSet) {
        Multimap<ScalarOperator, ColumnRefOperator> reversed = ArrayListMultimap.create();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : mvProjection.entrySet()) {
            reversed.put(entry.getValue(), entry.getKey());
        }

        Map<ColumnRefOperator, CallOperator> rewrittens = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : aggregates.entrySet()) {
            Preconditions.checkState(entry.getValue() instanceof CallOperator);
            CallOperator aggCall = (CallOperator) entry.getValue();
            ScalarOperator targetColumn = replaceExprWithTarget(aggCall, reversed, mapping);
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

    // generate new aggregates for rollup
    // eg: count(col) -> sum(col)
    CallOperator getRollupAggregate(CallOperator aggCall, ColumnRefOperator targetColumn) {
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

    // TODO: 考虑表达式改写是否需要copy一份表达式，不能够更改原来的节点，否则，会影响后续的改写
    // TODO: 考虑w无损类型转换cast
    private OptExpression rewriteProjection(LogicalProjectOperator queryProjection, OptExpression queryExpression,
                                            LogicalProjectOperator mvProjection, OptExpression mvExpression,
                                            OptExpression targetExpr,
                                            Map<Integer, Integer> relationIdMap,
                                            Map<Integer, List<ColumnRefOperator>> srcRelationIdToColumns,
                                            ColumnRefFactory srcRefFactory,
                                            Map<Integer, List<ColumnRefOperator>> targetRelationIdToColumns,
                                            ColumnRefFactory targetRefFactory, EquivalenceClasses ec) {
        // TODO: consider lineage of expression
        Map<ColumnRefOperator, ScalarOperator> queryProjectionMap =
                getProjectionMap(queryProjection, queryExpression, targetRefFactory);

        Map<ColumnRefOperator, ScalarOperator> viewExprMap = getProjectionMap(mvProjection, mvExpression, srcRefFactory);

        Map<ColumnRefOperator, ScalarOperator> newProjectionMap = Maps.newHashMap();

        // key and value have the same index
        List<ColumnRefOperator> keys = Lists.newArrayListWithCapacity(queryProjectionMap.size());
        List<ScalarOperator> values = Lists.newArrayListWithCapacity(queryProjectionMap.size());
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryProjectionMap.entrySet()) {
            keys.add(entry.getKey());
            values.add(entry.getValue());
        }

        // rewrittenValues has the same size with values
        List<ScalarOperator> rewrittenValues = rewriteScalarOperators(values, viewExprMap, targetExpr,
                relationIdMap, srcRelationIdToColumns, srcRefFactory, targetRelationIdToColumns, targetRefFactory, ec);
        if (rewrittenValues == null) {
            return null;
        }
        Preconditions.checkState(rewrittenValues.size() == values.size());
        for (int i = 0; i < keys.size(); i++) {
            newProjectionMap.put(keys.get(i), rewrittenValues.get(i));
        }
        LogicalProjectOperator projection = new LogicalProjectOperator(newProjectionMap);
        return OptExpression.create(projection, targetExpr);
    }

    private Map<ColumnRefOperator, ScalarOperator> getProjectionMap(LogicalProjectOperator projection,
                                                                    OptExpression expression, ColumnRefFactory refFactory) {
        Map<ColumnRefOperator, ScalarOperator> projectionMap;
        if (projection != null) {
            projectionMap = projection.getColumnRefMap();
        } else {
            if (expression.getOp().getProjection() != null) {
                projectionMap = expression.getOp().getProjection().getColumnRefMap();
            } else {
                projectionMap = Maps.newHashMap();
                if (expression.getOp() instanceof LogicalAggregationOperator) {
                    LogicalAggregationOperator agg = (LogicalAggregationOperator) expression.getOp();
                    Map<ColumnRefOperator, ScalarOperator> keyMap = agg.getGroupingKeys().stream().collect(Collectors.toMap(
                            java.util.function.Function.identity(),
                            java.util.function.Function.identity()));
                    projectionMap.putAll(keyMap);
                    projectionMap.putAll(agg.getAggregations());
                } else {
                    ColumnRefSet refSet = expression.getOutputColumns();
                    for (int columnId : refSet.getColumnIds()) {
                        ColumnRefOperator columnRef = refFactory.getColumnRef(columnId);
                        projectionMap.put(columnRef, columnRef);
                    }
                }
            }
        }
        return projectionMap;
    }

    // rewrite predicates by using target expression
    // temp:
    //     relationIdMap is view to query
    //     srcRelationIdToColumns and srcRefFactory is view
    //     targetRelationIdToColumns and targetRefFactory is query
    private List<ScalarOperator> rewriteScalarOperators(List<ScalarOperator> exprsToRewrites,
                                                        Map<ColumnRefOperator, ScalarOperator> viewExprMap,
                                                        OptExpression targetExpr,
                                                        Map<Integer, Integer> relationIdMap,
                                                        Map<Integer, List<ColumnRefOperator>> srcRelationIdToColumns,
                                                        ColumnRefFactory srcRefFactory,
                                                        Map<Integer, List<ColumnRefOperator>> targetRelationIdToColumns,
                                                        ColumnRefFactory targetRefFactory,
                                                        EquivalenceClasses ec) {
        // rewrite viewExprMap.values to query relation and query ec
        // now we only support SPJG pattern rewrite, so viewExprMap.values should be directly based on join or TableScan
        Multimap<ScalarOperator, ColumnRefOperator> rewrittenExprMap = ArrayListMultimap.create();

        // TODO: viewExprMap can be processed outside
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : viewExprMap.entrySet()) {
            ScalarOperator mappedExpr = rewriteColumnByRelationIdMapAndEc(entry.getValue(), relationIdMap,
                    srcRelationIdToColumns, srcRefFactory, targetRelationIdToColumns, targetRefFactory, ec);
            rewrittenExprMap.put(mappedExpr, entry.getKey());
        }
        Map<ColumnRefOperator, ScalarOperator> mvScanProjection = getProjectionMap(null, targetExpr, targetRefFactory);

        Preconditions.checkState(mvScanProjection.size() == viewExprMap.size());
        Map<ColumnRefOperator, ColumnRefOperator> mapping = Maps.newHashMap();
        Preconditions.checkState(materializationContext.getMvOutputExpressions().size()
                == materializationContext.getScanMvOutputExpressions().size());

        // construct output column mapping from mv sql to mv scan operator
        // eg: for mv1 sql define: select a, (b + 1) as c2, (a * b) as c3 from table;
        // select sql plan output columns:    a, b + 1, a * b
        //                                    |    |      |
        //                                    v    v      V
        // mv scan operator output columns:  a,   c2,    c3
        for (int i = 0; i < materializationContext.getMvOutputExpressions().size(); i++) {
            mapping.put(materializationContext.getMvOutputExpressions().get(i),
                    materializationContext.getScanMvOutputExpressions().get(i));
        }

        Preconditions.checkState(mapping.size() == viewExprMap.size());

        // try to rewrite predicatesToRewrite by rewrittenExprMap

        Set<ColumnRefOperator> originalColumnSet = targetRelationIdToColumns.values()
                .stream().flatMap(List::stream).collect(Collectors.toSet());
        originalColumnSet.removeAll(materializationContext.getScanMvOutputExpressions());
        List<ScalarOperator> rewrittenExprs = Lists.newArrayList();

        List<ScalarOperator> swappedExprs = Lists.newArrayList();
        for (ScalarOperator expr : exprsToRewrites) {
            ScalarOperator swappedScalarOperator = rewriteColumnByEc(expr, ec);
            if (swappedScalarOperator == null) {
                return null;
            }
            swappedExprs.add(swappedScalarOperator);
        }

        for (ScalarOperator expr : swappedExprs) {
            ScalarOperator rewritten = replaceExprWithTarget(expr, rewrittenExprMap, mapping);
            if (!isAllExprReplaced(rewritten, originalColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                return null;
            }
            rewrittenExprs.add(rewritten);
        }
        return rewrittenExprs;
    }

    boolean isAllExprReplaced(ScalarOperator rewritten, Set<ColumnRefOperator> originalColumnSet) {
        ScalarOperatorVisitor visitor = new ScalarOperatorVisitor<Void, Void>() {
            @Override
            public Void visit(ScalarOperator scalarOperator, Void context) {
                for (ScalarOperator child : scalarOperator.getChildren()) {
                    child.accept(this, null);
                }
                return null;
            }

            @Override
            public Void visitVariableReference(ColumnRefOperator variable, Void context) {
                if (originalColumnSet.contains(variable)) {
                    throw new UnsupportedOperationException("predicate can not be rewritten");
                }
                return null;
            }
        };
        try {
            rewritten.accept(visitor, null);
        } catch (UnsupportedOperationException e) {
            return false;
        }
        return true;
    }

    private ScalarOperator replaceExprWithTarget(ScalarOperator expr,
                                                 Multimap<ScalarOperator, ColumnRefOperator> exprMap,
                                                 Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
        ScalarOperatorVisitor shuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitBinaryPredicate(predicate, context);
            }

            @Override
            public ScalarOperator visitCall(CallOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitCall(predicate, context);
            }

            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                ScalarOperator tmp = replace(variable);
                return tmp != null ? tmp : super.visitVariableReference(variable, context);
            }

            ScalarOperator replace(ScalarOperator scalarOperator) {
                if (exprMap.containsKey(scalarOperator)) {
                    Optional<ColumnRefOperator> mappedColumnRef = exprMap.get(scalarOperator).stream().findFirst();
                    if (!mappedColumnRef.isPresent()) {
                        return null;
                    }
                    ColumnRefOperator replaced =  columnMapping.get(mappedColumnRef.get());
                    if (replaced == null) {
                        return null;
                    }
                    return replaced.clone();
                }
                return null;
            }
        };
        return (ScalarOperator) expr.accept(shuttle, null);
    }

    private boolean isAlwaysTrue(ScalarOperator predicate) {
        if (predicate instanceof ConstantOperator) {
            ConstantOperator constant = (ConstantOperator) predicate;
            if (constant.getType() == Type.BOOLEAN && constant.getBoolean() == true) {
                return true;
            }
        }
        return false;
    }

    private class RewriteContext {
        public Map<Integer, Integer> relationIdMap;
        public Map<Integer, List<ColumnRefOperator>> srcRelationIdToColumns;
        public ColumnRefFactory srcRefFactory;
        public Map<Integer, List<ColumnRefOperator>> targetRelationIdToColumns;
        public ColumnRefFactory targetRefFactory;
        public EquivalenceClasses ec;

        public RewriteContext(Map<Integer, Integer> relationIdMap,
                              Map<Integer, List<ColumnRefOperator>> srcRelationIdToColumns,
                              ColumnRefFactory srcRefFactory,
                              Map<Integer, List<ColumnRefOperator>> targetRelationIdToColumns,
                              ColumnRefFactory targetRefFactory,
                              EquivalenceClasses ec) {
            this.relationIdMap = relationIdMap;
            this.srcRelationIdToColumns = srcRelationIdToColumns;
            this.srcRefFactory = srcRefFactory;
            this.targetRelationIdToColumns = targetRelationIdToColumns;
            this.targetRefFactory = targetRefFactory;
            this.ec = ec;
        }
    }

    private ScalarOperator rewriteColumnByRelationIdMap(ScalarOperator predicate, Map<Integer, Integer> relationIdMap,
                                                        Map<Integer, List<ColumnRefOperator>> srcRelationIdToColumns,
                                                        ColumnRefFactory srcRefFactory,
                                                        Map<Integer, List<ColumnRefOperator>> targetRelationIdToColumns,
                                                        ColumnRefFactory targetRefFactory) {
        return rewriteColumnByRelationIdMapAndEc(predicate, relationIdMap, srcRelationIdToColumns,
                srcRefFactory, targetRelationIdToColumns, targetRefFactory, null);
    }

    private ScalarOperator rewriteColumnByEc(ScalarOperator predicate, EquivalenceClasses ec) {
        return rewriteColumnByRelationIdMapAndEc(predicate, null, null,
                null, null, null, ec);
    }

    private ScalarOperator rewriteColumnByRelationIdMapAndEc(ScalarOperator predicate, Map<Integer, Integer> relationIdMap,
                                                             Map<Integer, List<ColumnRefOperator>> srcRelationIdToColumns,
                                                             ColumnRefFactory srcRefFactory,
                                                             Map<Integer, List<ColumnRefOperator>> targetRelationIdToColumns,
                                                             ColumnRefFactory targetRefFactory,
                                                             EquivalenceClasses ec) {
        RewriteContext rewriteContext = new RewriteContext(relationIdMap,
                srcRelationIdToColumns, srcRefFactory, targetRelationIdToColumns, targetRefFactory, ec);
        ScalarOperatorVisitor<ScalarOperator, RewriteContext> rewriteVisitor =
                new ScalarOperatorVisitor<ScalarOperator, RewriteContext>() {
                    @Override
                    public ScalarOperator visit(ScalarOperator scalarOperator, RewriteContext context) {
                        List<ScalarOperator> children = Lists.newArrayList(scalarOperator.getChildren());
                        for (int i = 0; i < children.size(); ++i) {
                            ScalarOperator child = scalarOperator.getChild(i).accept(this, context);
                            if (child == null) {
                                return null;
                            }
                            scalarOperator.setChild(i, child);
                        }
                        return scalarOperator;
                    }

                    @Override
                    public ScalarOperator visitVariableReference(ColumnRefOperator columnRef, RewriteContext context) {
                        ColumnRefOperator result = columnRef;
                        if (context.relationIdMap != null) {
                            Integer srcRelationId = context.srcRefFactory.getRelationId(columnRef.getId());
                            if (srcRelationId < 0) {
                                LOG.warn("invalid columnRef:%s", columnRef);
                                return null;
                            }
                            Integer targetRelationId = context.relationIdMap.get(srcRelationId);
                            List<ColumnRefOperator> relationColumns = context.targetRelationIdToColumns.get(targetRelationId);
                            if (relationColumns == null) {
                                LOG.warn("no columns for relation id:%d", targetRelationId);
                                return null;
                            }
                            boolean found = false;
                            for (ColumnRefOperator dstColumnRef : relationColumns) {
                                if (columnRef.getName().equals(dstColumnRef.getName())) {
                                    result = dstColumnRef;
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                LOG.warn("can not find column ref:%s in target relation:%d", columnRef, targetRelationId);
                            }
                        }
                        if (context.ec != null) {
                            Set<ColumnRefOperator> equalities = context.ec.getEquivalenceClass(result);
                            if (equalities != null) {
                                // equalities can not be empty.
                                // and for every item in equalities, the equalities is the same.
                                // so this will convert each equality column ref to the first one in the equalities.
                                result = equalities.iterator().next();
                            }
                        }
                        return result;
                    }
                };
        ScalarOperator result = predicate.accept(rewriteVisitor, rewriteContext);
        return result;
    }

    private List<BiMap<Integer, Integer>> generateRelationIdMap(
            ColumnRefFactory queryRefFactory, ColumnRefFactory mvRefFactory) {
        Map<Table, Set<Integer>> queryTableToRelationId = getTableToRelationid(queryRefFactory, queryTableSet);
        Map<Table, Set<Integer>> mvTableToRelationId = getTableToRelationid(mvRefFactory, mvTableSet);
        Preconditions.checkState(queryTableToRelationId.keySet().equals(mvTableToRelationId.keySet()));
        List<BiMap<Integer, Integer>> result = ImmutableList.of(HashBiMap.create());
        for (Map.Entry<Table, Set<Integer>> queryEntry : queryTableToRelationId.entrySet()) {
            Preconditions.checkState(queryEntry.getValue().size() > 0);
            if (queryEntry.getValue().size() == 1) {
                Integer src = queryEntry.getValue().iterator().next();
                // TODO: should make sure equals for external tables
                Integer target = mvTableToRelationId.get(queryEntry.getKey()).iterator().next();
                for (BiMap<Integer, Integer> m : result) {
                    m.put(src, target);
                }
            } else {
                ImmutableList.Builder<BiMap<Integer, Integer>> newResult = ImmutableList.builder();
                for (Integer src : queryEntry.getValue()) {
                    for (Integer target : mvTableToRelationId.get(queryEntry.getKey())) {
                        for (BiMap<Integer, Integer> m : result) {
                            if (!m.containsValue(target)) {
                                final BiMap<Integer, Integer> newM = HashBiMap.create(m);
                                newM.put(src, target);
                                newResult.add(newM);
                            }
                        }
                    }
                }
                result = newResult.build();
            }
        }
        return result;
    }

    Map<Table, Set<Integer>> getTableToRelationid(ColumnRefFactory refFactory, List<Table> tableList) {
        Map<Table, Set<Integer>> tableToRelationId = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, Table> entry : refFactory.getColumnRefToTable().entrySet()) {
            if (!tableList.contains(entry.getValue())) {
                continue;
            }
            if (tableToRelationId.containsKey(entry.getValue())) {
                Integer relationId = refFactory.getRelationId(entry.getKey().getId());
                tableToRelationId.get(entry.getValue()).add(relationId);
            } else {
                Set<Integer> relationids = Sets.newHashSet();
                relationids.add(refFactory.getRelationId(entry.getKey().getId()));
                tableToRelationId.put(entry.getValue(), relationids);
            }
        }
        return tableToRelationId;
    }

    private Map<Integer, List<ColumnRefOperator>> getRelationIdToColumns(ColumnRefFactory refFactory) {
        // relationId -> column list
        Map<Integer, List<ColumnRefOperator>> result = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> entry : refFactory.getColumnToRelationIds().entrySet()) {
            if (result.containsKey(entry.getValue())) {
                ColumnRefOperator columnRef = refFactory.getColumnRef(entry.getKey());
                result.get(entry.getValue()).add(columnRef);
            } else {
                ColumnRefOperator columnRef = refFactory.getColumnRef(entry.getKey());
                List<ColumnRefOperator> columnRefs = Lists.newArrayList(columnRef);
                result.put(entry.getValue(), columnRefs);
            }
        }
        return result;
    }

    private Pair<ScalarOperator, ScalarOperator> getCompensationPredicates(
            EquivalenceClasses sourceEc,
            Triple<ScalarOperator, ScalarOperator, ScalarOperator> srcPredicateTriple,
            ColumnRefFactory srcColumnRefFactory,
            EquivalenceClasses targetEc,
            Triple<ScalarOperator, ScalarOperator, ScalarOperator> targetPredicateTriple,
            ColumnRefFactory targetColumnRefFactory, Map<Integer, Integer> relationIdMap,
            Map<Integer, List<ColumnRefOperator>> srcRelationIdToColumns,
            Map<Integer, List<ColumnRefOperator>> targetRelationIdToColumns) {
        // 1. equality join subsumption test
        // return null if test failed
        // or return the compensation equal
        final ScalarOperator compensationEqualPredicate = getCompensationEqualPredicate(sourceEc, targetEc);
        if (compensationEqualPredicate == null) {
            // means source cannot be rewritten by target
            return null;
        }
        // 2. range and residual subsumption test
        // get compensation range and residual predicates if pass

        ScalarOperator srcPr = srcPredicateTriple.getMiddle();
        ScalarOperator targetPr = targetPredicateTriple.getMiddle();
        ScalarOperator compensationPr;
        if (srcPr == null && targetPr == null) {
            compensationPr = ConstantOperator.createBoolean(true);
        } else if (srcPr == null && targetPr != null) {
            return null;
        } else if (srcPr != null && targetPr == null) {
            compensationPr = srcPr;
        } else {
            ScalarOperator canonizedSrcPr = RewriteUtils.canonizeNode(srcPredicateTriple.getMiddle().clone());
            ScalarOperator canonizedTargetPr = RewriteUtils.canonizeNode(targetPredicateTriple.getMiddle().clone());

            // swap column by source EC
            ScalarOperator rewrittenSrcPr = rewriteColumnByEc(canonizedSrcPr, sourceEc);

            // swap target with source relation and source EC
            ScalarOperator rewrittenTargetPr = rewriteColumnByRelationIdMapAndEc(canonizedTargetPr,
                    relationIdMap, srcRelationIdToColumns, srcColumnRefFactory,
                    targetRelationIdToColumns, targetColumnRefFactory, sourceEc);

            compensationPr = getCompensationRangePredicate(rewrittenSrcPr, rewrittenTargetPr);
        }

        if (compensationPr == null) {
            return null;
        }

        ScalarOperator compensationPu;
        ScalarOperator srcPu = srcPredicateTriple.getRight();
        ScalarOperator targetPu = targetPredicateTriple.getRight();
        if (srcPu == null && targetPu == null) {
            compensationPu = ConstantOperator.createBoolean(true);
        } else if (srcPu == null && targetPu != null) {
            return null;
        } else if (srcPu != null && targetPu == null) {
            compensationPu = srcPu;
        } else {
            ScalarOperator canonizedSrcPu = RewriteUtils.canonizeNode(srcPredicateTriple.getRight().clone());
            ScalarOperator canonizedTargetPu = RewriteUtils.canonizeNode(targetPredicateTriple.getRight().clone());
            ScalarOperator rewrittenSrcPu = rewriteColumnByEc(canonizedSrcPu, sourceEc);;
            ScalarOperator rewrittenTargetPu = rewriteColumnByRelationIdMapAndEc(canonizedTargetPu,
                    relationIdMap, srcRelationIdToColumns, srcColumnRefFactory,
                    targetRelationIdToColumns, targetColumnRefFactory, sourceEc);

            compensationPu = RewriteUtils.splitOr(rewrittenSrcPu, rewrittenTargetPu);
            if (compensationPu == null) {
                compensationPu = getCompensationResidualPredicate(rewrittenSrcPu, rewrittenTargetPu);
                if (compensationPu == null) {
                    return null;
                }
            }
        }

        ScalarOperator otherQueryPredicates = Utils.compoundAnd(compensationPr, compensationPu);
        return Pair.create(compensationEqualPredicate, RewriteUtils.canonizeNode(otherQueryPredicates));
    }

    private ScalarOperator getCompensationResidualPredicate(ScalarOperator srcPu, ScalarOperator targetPu) {
        List<ScalarOperator> srcConjuncts = Utils.extractConjuncts(srcPu);
        List<ScalarOperator> targetConjuncts = Utils.extractConjuncts(targetPu);
        if (srcConjuncts.containsAll(targetConjuncts)) {
            srcConjuncts.removeAll(targetConjuncts);
            if (srcConjuncts.isEmpty()) {
                return ConstantOperator.createBoolean(true);
            } else {
                return Utils.compoundAnd(srcConjuncts);
            }
        }
        return null;
    }

    private ScalarOperator getCompensationRangePredicate(ScalarOperator srcPr, ScalarOperator targetPr) {
        RangeSimplifier simplifier = new RangeSimplifier(Utils.extractConjuncts(srcPr));
        ScalarOperator compensationPr = simplifier.simplify(Utils.extractConjuncts(targetPr));
        return compensationPr;
    }

    // compute the compensation equality predicates
    // here do the equality join subsumption test
    // if targetEc is not contained in sourceEc, return null
    // if sourceEc equals targetEc, return true literal
    private ScalarOperator getCompensationEqualPredicate(EquivalenceClasses sourceEc, EquivalenceClasses targetEc) {
        if (sourceEc.getEquivalenceClasses().isEmpty() && targetEc.getEquivalenceClasses().isEmpty()) {
            return ConstantOperator.createBoolean(true);
        }
        if (sourceEc.getEquivalenceClasses().isEmpty() && !targetEc.getEquivalenceClasses().isEmpty()) {
            // targetEc must not be contained in sourceEc, just return null
            return null;
        }
        final List<Set<ColumnRefOperator>> sourceEcs = sourceEc.getEquivalenceClasses();
        final List<Set<ColumnRefOperator>> targetEcs = targetEc.getEquivalenceClasses();
        // it is a mapping from source to target
        // it may be 1 to n
        final Multimap<Integer, Integer> mapping = computeECMapping(sourceEcs, targetEcs);
        if (mapping == null) {
            // means that the targetEc can not be contained in sourceEc
            // it means Equijoin subsumption test fails
            return null;
        }
        // compute compensation equality predicate
        // if targetEc equals sourceEc, return true literal, so init to true here
        ScalarOperator compensation = ConstantOperator.createBoolean(true);
        for (int i = 0; i < sourceEcs.size(); i++) {
            if (!mapping.containsKey(i)) {
                // it means that the targeEc do not have the corresponding mapping ec
                // we should all equality predicates between each column in ec into compensation
                Iterator<ColumnRefOperator> it = sourceEcs.get(i).iterator();
                ScalarOperator first = it.next();
                while (it.hasNext()) {
                    ScalarOperator equalPredicate = BinaryPredicateOperator.eq(first, it.next());
                    compensation = Utils.compoundAnd(compensation, equalPredicate);
                }
            } else {
                // remove columns exists in target and add remain equality predicate in source into compensation
                for (int j : mapping.get(i)) {
                    Set<ScalarOperator> difference = Sets.newHashSet(sourceEcs.get(i));
                    difference.removeAll(targetEcs.get(j));
                    Iterator<ColumnRefOperator> it = targetEcs.get(j).iterator();
                    ScalarOperator targetFirst = it.next();
                    for (ScalarOperator remain : difference) {
                        ScalarOperator equalPredicate = BinaryPredicateOperator.eq(remain, targetFirst);
                        compensation = Utils.compoundAnd(compensation, equalPredicate);
                    }
                }
            }
        }
        return compensation;
    }

    // check whether each target equivalence classes is contained in source equivalence classes.
    // if any of target equivalence class cannot be contained, return null
    private Multimap<Integer, Integer> computeECMapping(List<Set<ColumnRefOperator>> sourceEcs,
                                                        List<Set<ColumnRefOperator>> targetEcs) {
        Multimap<Integer, Integer> mapping = ArrayListMultimap.create();
        for (int i = 0; i < targetEcs.size(); i++) {
            final Set<ColumnRefOperator> targetSet = targetEcs.get(i);
            boolean contained = false;
            for (int j = 0; j < sourceEcs.size(); j++) {
                final Set<ColumnRefOperator> srcSet = sourceEcs.get(j);
                // targetSet is converted into the same relationId, so just use containAll
                if (srcSet.containsAll(targetSet)) {
                    mapping.put(j, i);
                    contained = true;
                }
            }
            if (!contained) {
                return null;
            }
        }
        return mapping;
    }
}
