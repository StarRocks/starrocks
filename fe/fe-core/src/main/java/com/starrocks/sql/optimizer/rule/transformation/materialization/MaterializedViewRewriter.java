// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
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
 *  This rewriter is for single or multi table join query rewrite
 */
public class MaterializedViewRewriter {
    protected static final Logger LOG = LogManager.getLogger(MaterializedViewRewriter.class);
    protected final MaterializationContext materializationContext;

    protected enum MatchMode {
        // all tables and join types match
        COMPLETE,
        // all tables match but join types do not
        PARTIAL,
        // all join types match but query has more tables
        QUERY_DELTA,
        // all join types match but view has more tables
        VIEW_DELTA,
        NOT_MATCH
    }

    public MaterializedViewRewriter(MaterializationContext materializationContext) {
        this.materializationContext = materializationContext;
    }

    public boolean isValidPlan(OptExpression expression) {
        return Utils.isLogicalSPJ(expression);
    }

    public List<OptExpression> rewrite() {
        if (!isValidPlan(materializationContext.getMvExpression())) {
            return Lists.newArrayList();
        }

        OptExpression queryExpression = materializationContext.getQueryExpression();
        Map<ColumnRefOperator, ScalarOperator> queryLineage =
                getLineage(queryExpression, materializationContext.getQueryRefFactory());
        ReplaceColumnRefRewriter queryColumnRefRewriter = new ReplaceColumnRefRewriter(queryLineage, true);
        List<ScalarOperator> queryConjuncts = Utils.getAllPredicates(queryExpression);
        ScalarOperator queryPredicate = null;
        if (!queryConjuncts.isEmpty()) {
            queryPredicate = Utils.compoundAnd(queryConjuncts);
            queryPredicate = queryColumnRefRewriter.rewrite(queryPredicate.clone());
        }
        final PredicateSplit queryPredicateSplit = PredicateSplit.splitPredicate(queryPredicate);
        EquivalenceClasses queryEc = createEquivalenceClasses(queryPredicateSplit.getEqualPredicates());

        // should get all query tables
        List<Table> queryTables = Utils.getAllTables(queryExpression);

        OptExpression mvExpression = materializationContext.getMvExpression();
        List<Table> mvTables = Utils.getAllTables(mvExpression);
        if (Collections.disjoint(queryTables, mvTables)) {
            // if table lists do not intersect, can not be rewritten
            return Lists.newArrayList();
        }

        MatchMode matchMode = getMatchMode(queryExpression, queryTables, mvExpression, mvTables);
        if (matchMode != MatchMode.COMPLETE) {
            // Now only MatchMode.COMPLETE is supported.
            // It will be extended later
            return Lists.newArrayList();
        }

        // construct output column mapping from mv sql to mv scan operator
        // eg: for mv1 sql define: select a, (b + 1) as c2, (a * b) as c3 from table;
        // select sql plan output columns:    a, b + 1, a * b
        //                                    |    |      |
        //                                    v    v      V
        // mv scan operator output columns:  a,   c2,    c3
        Map<ColumnRefOperator, ColumnRefOperator> outputMapping = Maps.newHashMap();
        for (int i = 0; i < materializationContext.getMvOutputExpressions().size(); i++) {
            outputMapping.put(materializationContext.getMvOutputExpressions().get(i),
                    materializationContext.getScanMvOutputExpressions().get(i));
        }

        Map<ColumnRefOperator, ScalarOperator> mvLineage =
                getLineage(mvExpression, materializationContext.getMvColumnRefFactory());
        ReplaceColumnRefRewriter mvColumnRefRewriter = new ReplaceColumnRefRewriter(mvLineage, true);
        List<ScalarOperator> mvConjuncts = Utils.getAllPredicates(mvExpression);
        ScalarOperator mvPredicate = null;
        if (!mvConjuncts.isEmpty()) {
            mvPredicate = Utils.compoundAnd(mvConjuncts);
            mvPredicate = mvColumnRefRewriter.rewrite(mvPredicate.clone());
        }
        final PredicateSplit mvPredicateSplit = PredicateSplit.splitPredicate(mvPredicate);

        Map<Integer, List<ColumnRefOperator>> queryRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getQueryRefFactory());

        Map<Integer, List<ColumnRefOperator>> mvRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getMvColumnRefFactory());

        // for query: A1 join A2 join B, mv: A1 join A2 join B
        // there may be two mapping:
        //    1. A1 -> A1, A2 -> A2, B -> B
        //    2. A1 -> A2, A2 -> A1, B -> B
        List<BiMap<Integer, Integer>> relationIdMappings = generateRelationIdMap(materializationContext.getQueryRefFactory(),
                queryTables, materializationContext.getMvColumnRefFactory(), mvTables);

        // used to judge whether query scalar ops can be rewritten
        Set<ColumnRefOperator> queryColumnSet = queryRelationIdToColumns.values()
                .stream().flatMap(List::stream)
                .filter(columnRef -> !materializationContext.getScanMvOutputExpressions().contains(columnRef))
                .collect(Collectors.toSet());
        RewriteContext rewriteContext = new RewriteContext(queryExpression, /*queryProjection,*/
                queryPredicateSplit, queryEc, queryRelationIdToColumns, materializationContext.getQueryRefFactory(),
                queryColumnRefRewriter, mvExpression, /* mvTopProjection ,*/ mvPredicateSplit, mvRelationIdToColumns,
                materializationContext.getMvColumnRefFactory(), mvColumnRefRewriter, outputMapping, queryColumnSet);
        List<OptExpression> results = Lists.newArrayList();
        for (BiMap<Integer, Integer> relationIdMapping : relationIdMappings) {
            rewriteContext.setQueryToMvRelationIdMapping(relationIdMapping);
            OptExpression rewrittenExpression = tryRewriteForRelationMapping(rewriteContext);
            if (rewrittenExpression == null) {
                continue;
            }
            results.add(rewrittenExpression);
        }

        return results;
    }

    private OptExpression tryRewriteForRelationMapping(RewriteContext rewriteContext) {
        // the rewritten expression to replace query
        OptExpression rewrittenExpression = OptExpression.create(materializationContext.getScanMvOperator());
        deriveLogicalProperty(rewrittenExpression);

        // construct query based view EC
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        EquivalenceClasses queryBaseViewEc = new EquivalenceClasses();
        if (rewriteContext.getMvPredicateSplit().getEqualPredicates() != null) {
            ScalarOperator equalPredicates = rewriteContext.getMvPredicateSplit().getEqualPredicates();
            ScalarOperator queryBasedViewEqualPredicate = columnRewriter.rewriteViewToQuery(equalPredicates);
            queryBaseViewEc = createEquivalenceClasses(queryBasedViewEqualPredicate);
        }
        rewriteContext.setQueryBasedViewEquivalenceClasses(queryBaseViewEc);

        PredicateSplit compensationPredicates = getCompensationPredicates(rewriteContext, true);
        if (compensationPredicates == null) {
            if (!materializationContext.getOptimizerContext().getSessionVariable().isEnableMaterializedViewUnionRewrite()) {
                return null;
            }
            return tryUnionRewrite(rewriteContext, rewrittenExpression);
        } else {
            // all predicates are now query based
            ScalarOperator equalPredicates = Utils.canonizePredicate(compensationPredicates.getEqualPredicates());
            ScalarOperator otherPredicates = Utils.canonizePredicate(Utils.compoundAnd(
                    compensationPredicates.getRangePredicates(), compensationPredicates.getResidualPredicates()));
            if (!ConstantOperator.TRUE.equals(equalPredicates) || !ConstantOperator.TRUE.equals(otherPredicates)) {
                Map<ColumnRefOperator, ScalarOperator> viewExprMap = Utils.getColumnRefMap(
                        rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());

                if (!ConstantOperator.TRUE.equals(equalPredicates)) {
                    Multimap<ScalarOperator, ColumnRefOperator> normalizedMap =
                            normalizeAndReverseProjection(viewExprMap, rewriteContext, true);
                    List<ScalarOperator> conjuncts = Utils.extractConjuncts(equalPredicates);
                    // swapped by query based view ec
                    List<ScalarOperator> swappedConjuncts = conjuncts.stream().map(conjunct -> {
                        ColumnRewriter rewriter = new ColumnRewriter(rewriteContext);
                        return rewriter.rewriteByViewEc(conjunct);
                    }).collect(Collectors.toList());
                    List<ScalarOperator> rewrittens = rewriteQueryScalarOpToTarget(swappedConjuncts, normalizedMap,
                            rewriteContext.getOutputMapping(), rewriteContext.getQueryColumnSet());
                    if (rewrittens == null || rewrittens.isEmpty()) {
                        return null;
                    }
                    equalPredicates = Utils.compoundAnd(rewrittens);
                }

                if (!ConstantOperator.TRUE.equals(otherPredicates)) {
                    List<ScalarOperator> conjuncts = Utils.extractConjuncts(otherPredicates);
                    // swapped by query ec
                    List<ScalarOperator> swappedConjuncts = conjuncts.stream().map(conjunct -> {
                        ColumnRewriter rewriter = new ColumnRewriter(rewriteContext);
                        return rewriter.rewriteByQueryEc(conjunct);
                    }).collect(Collectors.toList());
                    Multimap<ScalarOperator, ColumnRefOperator> normalizedMap =
                            normalizeAndReverseProjection(viewExprMap, rewriteContext, false);
                    List<ScalarOperator> rewrittens = rewriteQueryScalarOpToTarget(swappedConjuncts, normalizedMap,
                            rewriteContext.getOutputMapping(), rewriteContext.getQueryColumnSet());
                    if (rewrittens == null || rewrittens.isEmpty()) {
                        return null;
                    }
                    otherPredicates = Utils.compoundAnd(rewrittens);
                }
            }
            ScalarOperator compensationPredicate = Utils.canonizePredicate(Utils.compoundAnd(equalPredicates, otherPredicates));
            if (!ConstantOperator.TRUE.equals(compensationPredicate)) {
                // add filter operator
                LogicalFilterOperator filter = new LogicalFilterOperator(compensationPredicate);
                rewrittenExpression = OptExpression.create(filter, rewrittenExpression);
                deriveLogicalProperty(rewrittenExpression);
            }

            // add projection
            rewrittenExpression = viewBasedRewrite(rewriteContext, rewrittenExpression);
        }
        return rewrittenExpression;
    }

    private OptExpression tryUnionRewrite(RewriteContext rewriteContext, OptExpression targetExpr) {
        // TODO: add union rewrite
        return null;
    }

    protected Multimap<ScalarOperator, ColumnRefOperator> normalizeAndReverseProjection(
            Map<ColumnRefOperator, ScalarOperator> viewProjection, RewriteContext rewriteContext, boolean isViewBased) {
        Multimap<ScalarOperator, ColumnRefOperator> reversedMap = ArrayListMultimap.create();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : viewProjection.entrySet()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(entry.getValue());
            if (isViewBased) {
                ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithViewEc(rewritten);
                reversedMap.put(swapped, entry.getKey());
            } else {
                ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
                reversedMap.put(swapped, entry.getKey());
            }
        }
        return reversedMap;
    }

    protected Map<ColumnRefOperator, ScalarOperator> getLineage(OptExpression expression, ColumnRefFactory refFactory) {
        LineageFactory factory = new LineageFactory(expression, refFactory);
        return factory.getLineage();
    }

    // equalPredicates eg: t1.a = t2.b and t1.c = t2.d
    private EquivalenceClasses createEquivalenceClasses(ScalarOperator equalPredicates) {
        EquivalenceClasses ec = new EquivalenceClasses();
        if (equalPredicates == null) {
            return ec;
        }
        for (ScalarOperator equalPredicate : Utils.extractConjuncts(equalPredicates)) {
            Preconditions.checkState(equalPredicate.getChild(0).isColumnRef());
            ColumnRefOperator left = (ColumnRefOperator) equalPredicate.getChild(0);
            Preconditions.checkState(equalPredicate.getChild(1).isColumnRef());
            ColumnRefOperator right = (ColumnRefOperator) equalPredicate.getChild(1);
            ec.addEquivalence(left, right);
        }
        return ec;
    }

    private MatchMode getMatchMode(OptExpression query, List<Table> queryTables,
                                   OptExpression mvExpression, List<Table> mvTables) {
        boolean isQueryAllEqualJoin = Utils.isAllEqualInnerJoin(query);
        boolean isMvAllEqualJoin = Utils.isAllEqualInnerJoin(mvExpression);
        MatchMode matchMode = MatchMode.NOT_MATCH;
        if (isQueryAllEqualJoin && isMvAllEqualJoin) {
            // process table match
            if (queryTables.size() == mvTables.size() && queryTables.containsAll(mvTables)) {
                matchMode = MatchMode.COMPLETE;
            } else if (queryTables.containsAll(mvTables)) {
                // TODO: query delta
                matchMode = MatchMode.QUERY_DELTA;
            } else if (mvTables.containsAll(queryTables)) {
                // TODO: view delta
                matchMode = MatchMode.VIEW_DELTA;
            }
        } else {
            // TODO: process for outer join and not equal join
            // check whether query can be rewritten
        }
        return matchMode;
    }

    protected void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        if (root.getLogicalProperty() == null) {
            ExpressionContext context = new ExpressionContext(root);
            context.deriveLogicalProperty();
            root.setLogicalProperty(context.getRootProperty());
        }
    }

    // TODO: consider no-loss type cast
    protected OptExpression viewBasedRewrite(RewriteContext rewriteContext, OptExpression targetExpr) {
        Map<ColumnRefOperator, ScalarOperator> viewExprMap = Utils.getColumnRefMap(
                rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());
        // normalize view projection by query relation and ec
        Multimap<ScalarOperator, ColumnRefOperator> normalizedMap =
                normalizeAndReverseProjection(viewExprMap, rewriteContext, false);

        return rewriteProjection(rewriteContext, normalizedMap, targetExpr);
    }

    protected OptExpression rewriteProjection(RewriteContext rewriteContext,
                                    Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
                                    OptExpression targetExpr) {
        Map<ColumnRefOperator, ScalarOperator> queryMap = Utils.getColumnRefMap(
                rewriteContext.getQueryExpression(), rewriteContext.getQueryRefFactory());
        Map<ColumnRefOperator, ScalarOperator> swappedQueryColumnMap = Maps.newHashMap();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryMap.entrySet()) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(entry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            swappedQueryColumnMap.put(entry.getKey(), swapped);
        }

        Map<ColumnRefOperator, ScalarOperator> newQueryProjection = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : swappedQueryColumnMap.entrySet()) {
            ScalarOperator rewritten = replaceExprWithTarget(entry.getValue(),
                    normalizedViewMap, rewriteContext.getOutputMapping());
            if (!isAllExprReplaced(rewritten, rewriteContext.getQueryColumnSet())) {
                // it means there is some column that can not be rewritten by outputs of mv
                return null;
            }
            newQueryProjection.put(entry.getKey(), rewritten);
        }
        Projection newProjection = new Projection(newQueryProjection);
        targetExpr.getOp().setProjection(newProjection);
        return targetExpr;
    }

    protected List<ScalarOperator> rewriteQueryScalarOpToTarget(List<ScalarOperator> exprsToRewrites,
                                                              Multimap<ScalarOperator, ColumnRefOperator> reversedViewProjection,
                                                              Map<ColumnRefOperator, ColumnRefOperator> outputMapping,
                                                              Set<ColumnRefOperator> originalColumnSet) {
        List<ScalarOperator> rewrittenExprs = Lists.newArrayList();
        for (ScalarOperator expr : exprsToRewrites) {
            ScalarOperator rewritten = replaceExprWithTarget(expr, reversedViewProjection, outputMapping);
            if (originalColumnSet != null && !isAllExprReplaced(rewritten, originalColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                return Lists.newArrayList();
            }
            rewrittenExprs.add(rewritten);
        }
        return rewrittenExprs;
    }

    protected boolean isAllExprReplaced(ScalarOperator rewritten, Set<ColumnRefOperator> originalColumnSet) {
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

    protected ScalarOperator replaceExprWithTarget(ScalarOperator expr,
                                                 Multimap<ScalarOperator, ColumnRefOperator> exprMap,
                                                 Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
        BaseScalarOperatorShuttle shuttle = new BaseScalarOperatorShuttle() {
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
                    if (columnMapping != null) {
                        ColumnRefOperator replaced = columnMapping.get(mappedColumnRef.get());
                        if (replaced == null) {
                            return null;
                        }
                        return replaced.clone();
                    } else {
                        return mappedColumnRef.get().clone();
                    }
                }
                return null;
            }
        };
        return expr.accept(shuttle, null);
    }

    private List<BiMap<Integer, Integer>> generateRelationIdMap(
            ColumnRefFactory queryRefFactory, List<Table> queryTables, ColumnRefFactory mvRefFactory, List<Table> mvTables) {
        Map<Table, Set<Integer>> queryTableToRelationId = getTableToRelationid(queryRefFactory, queryTables);
        Map<Table, Set<Integer>> mvTableToRelationId = getTableToRelationid(mvRefFactory, mvTables);
        Preconditions.checkState(queryTableToRelationId.keySet().equals(mvTableToRelationId.keySet()));
        List<BiMap<Integer, Integer>> result = ImmutableList.of(HashBiMap.create());
        for (Map.Entry<Table, Set<Integer>> queryEntry : queryTableToRelationId.entrySet()) {
            Preconditions.checkState(!queryEntry.getValue().isEmpty());
            if (queryEntry.getValue().size() == 1) {
                Integer src = queryEntry.getValue().iterator().next();
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

    private Map<Table, Set<Integer>> getTableToRelationid(ColumnRefFactory refFactory, List<Table> tableList) {
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
            result.computeIfAbsent(entry.getValue(), k -> Lists.newArrayList());
            ColumnRefOperator columnRef = refFactory.getColumnRef(entry.getKey());
            result.get(entry.getValue()).add(columnRef);
        }
        return result;
    }

    // when isQueryAgainstView is true, get compensation predicates of query against view
    // or get compensation predicates of view against query
    private PredicateSplit getCompensationPredicates(RewriteContext rewriteContext, boolean isQueryAgainstView) {
        // 1. equality join subsumption test
        EquivalenceClasses sourceEquivalenceClasses = isQueryAgainstView ?
                rewriteContext.getQueryEquivalenceClasses() : rewriteContext.getQueryBasedViewEquivalenceClasses();
        EquivalenceClasses targetEquivalenceClasses = isQueryAgainstView ?
                rewriteContext.getQueryBasedViewEquivalenceClasses() : rewriteContext.getQueryEquivalenceClasses();
        final ScalarOperator compensationEqualPredicate =
                getCompensationEqualPredicate(sourceEquivalenceClasses, targetEquivalenceClasses);
        if (compensationEqualPredicate == null) {
            // means source equal predicates cannot be rewritten by target
            return null;
        }

        // 2. range subsumption test
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        ScalarOperator srcPr = isQueryAgainstView ? rewriteContext.getQueryPredicateSplit().getRangePredicates()
                : rewriteContext.getMvPredicateSplit().getRangePredicates();
        ScalarOperator targetPr = isQueryAgainstView ? rewriteContext.getMvPredicateSplit().getRangePredicates()
                : rewriteContext.getQueryPredicateSplit().getRangePredicates();
        ScalarOperator compensationPr = getCompensationRangePredicate(srcPr, targetPr, columnRewriter);
        if (compensationPr == null) {
            return null;
        }

        // 3. residual subsumption test
        ScalarOperator srcPu = isQueryAgainstView ? rewriteContext.getQueryPredicateSplit().getResidualPredicates()
                : rewriteContext.getMvPredicateSplit().getResidualPredicates();
        ScalarOperator targetPu = isQueryAgainstView ? rewriteContext.getMvPredicateSplit().getResidualPredicates()
                : rewriteContext.getQueryPredicateSplit().getResidualPredicates();
        if (srcPu == null && targetPu != null) {
            // query: empid < 5
            // mv: empid < 5 or salary > 100
            srcPu = Utils.compoundAnd(compensationEqualPredicate, compensationPr);
        }
        ScalarOperator compensationPu = getCompensationResidualPredicate(srcPu, targetPu, columnRewriter);
        if (compensationPu == null) {
            return null;
        }

        return PredicateSplit.of(compensationEqualPredicate, compensationPr, compensationPu);
    }

    private ScalarOperator getCompensationResidualPredicate(ScalarOperator srcPu,
                                                            ScalarOperator targetPu,
                                                            ColumnRewriter columnRewriter) {
        ScalarOperator compensationPu;
        if (srcPu == null && targetPu == null) {
            compensationPu = ConstantOperator.createBoolean(true);
        } else if (srcPu == null && targetPu != null) {
            return null;
        } else if (srcPu != null && targetPu == null) {
            compensationPu = srcPu;
        } else {
            ScalarOperator canonizedSrcPu = Utils.canonizePredicateForRewrite(srcPu.clone());
            ScalarOperator canonizedTargetPu = Utils.canonizePredicateForRewrite(targetPu.clone());
            ScalarOperator swappedSrcPu = columnRewriter.rewriteByQueryEc(canonizedSrcPu);
            ScalarOperator swappedTargetPu = columnRewriter.rewriteViewToQueryWithQueryEc(canonizedTargetPu);

            compensationPu = Utils.getCompensationPredicateForDisjunctive(swappedSrcPu, swappedTargetPu);
            if (compensationPu == null) {
                compensationPu = getCompensationResidualPredicate(swappedSrcPu, swappedTargetPu);
                if (compensationPu == null) {
                    return null;
                }
            }
        }
        compensationPu = Utils.canonizePredicate(compensationPu);
        return compensationPu;
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

    private ScalarOperator getCompensationRangePredicate(ScalarOperator srcPr,
                                                         ScalarOperator targetPr,
                                                         ColumnRewriter columnRewriter) {
        ScalarOperator compensationPr;
        if (srcPr == null && targetPr == null) {
            compensationPr = ConstantOperator.createBoolean(true);
        } else if (srcPr == null && targetPr != null) {
            return null;
        } else if (srcPr != null && targetPr == null) {
            compensationPr = srcPr;
        } else {
            ScalarOperator canonizedSrcPr = Utils.canonizePredicateForRewrite(srcPr.clone());
            ScalarOperator canonizedTargetPr = Utils.canonizePredicateForRewrite(targetPr.clone());

            // swap column by query EC
            ScalarOperator swappedSrcPr = columnRewriter.rewriteByQueryEc(canonizedSrcPr);
            // swap column by relation mapping and query ec
            ScalarOperator swappedTargetPr = columnRewriter.rewriteViewToQueryWithQueryEc(canonizedTargetPr);
            compensationPr = getCompensationRangePredicate(swappedSrcPr, swappedTargetPr);
        }
        compensationPr = Utils.canonizePredicate(compensationPr);
        return compensationPr;
    }

    private ScalarOperator getCompensationRangePredicate(ScalarOperator srcPr, ScalarOperator targetPr) {
        RangeSimplifier simplifier = new RangeSimplifier(Utils.extractConjuncts(srcPr));
        return simplifier.simplify(Utils.extractConjuncts(targetPr));
    }

    // compute the compensation equality predicates
    // here do the equality join subsumption test
    // if targetEc is not contained in sourceEc, return null
    // if sourceEc equals targetEc, return true literal
    private ScalarOperator getCompensationEqualPredicate(EquivalenceClasses sourceEquivalenceClasses,
                                                         EquivalenceClasses targetEquivalenceClasses) {
        if (sourceEquivalenceClasses.getEquivalenceClasses().isEmpty()
                && targetEquivalenceClasses.getEquivalenceClasses().isEmpty()) {
            return ConstantOperator.createBoolean(true);
        }
        if (sourceEquivalenceClasses.getEquivalenceClasses().isEmpty()
                && !targetEquivalenceClasses.getEquivalenceClasses().isEmpty()) {
            // targetEc must not be contained in sourceEc, just return null
            return null;
        }
        final List<Set<ColumnRefOperator>> sourceEquivalenceClassesList = sourceEquivalenceClasses.getEquivalenceClasses();
        final List<Set<ColumnRefOperator>> targetEquivalenceClassesList = targetEquivalenceClasses.getEquivalenceClasses();
        // it is a mapping from source to target
        // it may be 1 to n
        final Multimap<Integer, Integer> mapping = computeECMapping(sourceEquivalenceClassesList, targetEquivalenceClassesList);
        if (mapping == null) {
            // means that the targetEc can not be contained in sourceEc
            // it means Equijoin subsumption test fails
            return null;
        }
        // compute compensation equality predicate
        // if targetEc equals sourceEc, return true literal, so init to true here
        ScalarOperator compensation = ConstantOperator.createBoolean(true);
        for (int i = 0; i < sourceEquivalenceClassesList.size(); i++) {
            if (!mapping.containsKey(i)) {
                // it means that the targeEc do not have the corresponding mapping ec
                // we should all equality predicates between each column in ec into compensation
                Iterator<ColumnRefOperator> it = sourceEquivalenceClassesList.get(i).iterator();
                ScalarOperator first = it.next();
                while (it.hasNext()) {
                    ScalarOperator equalPredicate = BinaryPredicateOperator.eq(first, it.next());
                    compensation = Utils.compoundAnd(compensation, equalPredicate);
                }
            } else {
                // remove columns exists in target and add remain equality predicate in source into compensation
                for (int j : mapping.get(i)) {
                    Set<ScalarOperator> difference = Sets.newHashSet(sourceEquivalenceClassesList.get(i));
                    difference.removeAll(targetEquivalenceClassesList.get(j));
                    Iterator<ColumnRefOperator> it = targetEquivalenceClassesList.get(j).iterator();
                    ScalarOperator targetFirst = it.next();
                    for (ScalarOperator remain : difference) {
                        ScalarOperator equalPredicate = BinaryPredicateOperator.eq(remain, targetFirst);
                        compensation = Utils.compoundAnd(compensation, equalPredicate);
                    }
                }
            }
        }
        compensation = Utils.canonizePredicate(compensation);
        return compensation;
    }

    // check whether each target equivalence classes is contained in source equivalence classes.
    // if any of target equivalence class cannot be contained, return null
    private Multimap<Integer, Integer> computeECMapping(List<Set<ColumnRefOperator>> sourceEquivalenceClassesList,
                                                        List<Set<ColumnRefOperator>> targetEquivalenceClassesList) {
        Multimap<Integer, Integer> mapping = ArrayListMultimap.create();
        for (int i = 0; i < targetEquivalenceClassesList.size(); i++) {
            final Set<ColumnRefOperator> targetSet = targetEquivalenceClassesList.get(i);
            boolean contained = false;
            for (int j = 0; j < sourceEquivalenceClassesList.size(); j++) {
                final Set<ColumnRefOperator> srcSet = sourceEquivalenceClassesList.get(j);
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
