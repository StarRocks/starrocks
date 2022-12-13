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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.commons.collections4.iterators.PermutationIterator;
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
 *  This rewriter is for single table or multi table join query rewrite
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
        return MvUtils.isLogicalSPJ(expression);
    }

    public List<OptExpression> rewrite() {
        if (!isValidPlan(materializationContext.getMvExpression())) {
            return Lists.newArrayList();
        }

        OptExpression queryExpression = materializationContext.getQueryExpression();
        Map<ColumnRefOperator, ScalarOperator> queryLineage =
                getLineage(queryExpression, materializationContext.getQueryRefFactory());
        ReplaceColumnRefRewriter queryColumnRefRewriter = new ReplaceColumnRefRewriter(queryLineage, true);
        List<ScalarOperator> queryConjuncts = MvUtils.getAllPredicates(queryExpression);
        ScalarOperator queryPredicate = null;
        if (!queryConjuncts.isEmpty()) {
            queryPredicate = Utils.compoundAnd(queryConjuncts);
            queryPredicate = queryColumnRefRewriter.rewrite(queryPredicate.clone());
        }
        ScalarOperator queryPartitionPredicate =
                compensatePartitionPredicate(queryExpression, materializationContext.getQueryRefFactory());
        if (queryPartitionPredicate == null) {
            return Lists.newArrayList();
        }
        if (!ConstantOperator.TRUE.equals(queryPartitionPredicate)) {
            queryPredicate = MvUtils.canonizePredicate(Utils.compoundAnd(queryPredicate, queryPartitionPredicate));
        }
        final PredicateSplit queryPredicateSplit = PredicateSplit.splitPredicate(queryPredicate);
        EquivalenceClasses queryEc = createEquivalenceClasses(queryPredicateSplit.getEqualPredicates());

        // should get all query tables
        List<Table> queryTables = MvUtils.getAllTables(queryExpression);

        OptExpression mvExpression = materializationContext.getMvExpression();
        List<Table> mvTables = MvUtils.getAllTables(mvExpression);
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

        Map<ColumnRefOperator, ScalarOperator> mvLineage =
                getLineage(mvExpression, materializationContext.getMvColumnRefFactory());
        ReplaceColumnRefRewriter mvColumnRefRewriter = new ReplaceColumnRefRewriter(mvLineage, true);
        List<ScalarOperator> mvConjuncts = MvUtils.getAllPredicates(mvExpression);
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
                queryTables, queryExpression, materializationContext.getMvColumnRefFactory(), mvTables, mvExpression);

        // used to judge whether query scalar ops can be rewritten
        List<ColumnRefOperator> scanMvOutputColumns =
                ((LogicalOlapScanOperator) materializationContext.getScanMvOperator()).getOutputColumns();
        Set<ColumnRefOperator> queryColumnSet = queryRelationIdToColumns.values()
                .stream().flatMap(List::stream)
                .filter(columnRef -> !scanMvOutputColumns.contains(columnRef))
                .collect(Collectors.toSet());
        RewriteContext rewriteContext = new RewriteContext(queryExpression, queryPredicateSplit, queryEc,
                queryRelationIdToColumns, materializationContext.getQueryRefFactory(),
                queryColumnRefRewriter, mvExpression, mvPredicateSplit, mvRelationIdToColumns,
                materializationContext.getMvColumnRefFactory(), mvColumnRefRewriter,
                materializationContext.getOutputMapping(), queryColumnSet);
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
        // should copy the op because the op will be modified and reused
        Operator.Builder builder = OperatorBuilderFactory.build(materializationContext.getScanMvOperator());
        builder.withOperator(materializationContext.getScanMvOperator());
        OptExpression rewrittenExpression = OptExpression.create(builder.build());
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
            ScalarOperator equalPredicates = MvUtils.canonizePredicate(compensationPredicates.getEqualPredicates());
            ScalarOperator otherPredicates = MvUtils.canonizePredicate(Utils.compoundAnd(
                    compensationPredicates.getRangePredicates(), compensationPredicates.getResidualPredicates()));
            if (!ConstantOperator.TRUE.equals(equalPredicates) || !ConstantOperator.TRUE.equals(otherPredicates)) {
                Map<ColumnRefOperator, ScalarOperator> viewExprMap = MvUtils.getColumnRefMap(
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
            ScalarOperator compensationPredicate = MvUtils.canonizePredicate(Utils.compoundAnd(equalPredicates, otherPredicates));
            if (!ConstantOperator.TRUE.equals(compensationPredicate)) {
                // add filter operator
                ScalarOperator originalPredicate = rewrittenExpression.getOp().getPredicate();
                ScalarOperator finalPredicate = Utils.compoundAnd(originalPredicate, compensationPredicate);
                Operator.Builder newOpBuilder = OperatorBuilderFactory.build(rewrittenExpression.getOp());
                newOpBuilder.withOperator(rewrittenExpression.getOp());
                newOpBuilder.setPredicate(finalPredicate);
                Operator newOp = newOpBuilder.build();
                rewrittenExpression = OptExpression.create(newOp);
                rewrittenExpression.setLogicalProperty(null);
                deriveLogicalProperty(rewrittenExpression);
            }

            // add projection
            rewrittenExpression = viewBasedRewrite(rewriteContext, rewrittenExpression);
        }
        return rewrittenExpression;
    }

    private OptExpression tryUnionRewrite(RewriteContext rewriteContext, OptExpression targetExpr) {
        PredicateSplit compensationPredicates = getCompensationPredicates(rewriteContext, false);
        if (compensationPredicates == null) {
            return null;
        }
        Preconditions.checkState(!ConstantOperator.TRUE.equals(compensationPredicates.getEqualPredicates())
                || !ConstantOperator.TRUE.equals(compensationPredicates.getRangePredicates())
                || !ConstantOperator.TRUE.equals(compensationPredicates.getResidualPredicates()));

        // for mv: select a, b from t where a < 10;
        // query: select a, b from t where a < 20;
        // queryBasedRewrite will return the tree of "select a, b from t where a >= 10 and a < 20"
        // which is realized by adding the compensation predicate to original query expression
        OptExpression queryInput = queryBasedRewrite(rewriteContext,
                compensationPredicates, materializationContext.getQueryExpression());
        if (queryInput == null) {
            return null;
        }
        // viewBasedRewrite will return the tree of "select a, b from t where a < 10" based on mv expression
        OptExpression viewInput = viewBasedRewrite(rewriteContext, targetExpr);
        if (viewInput == null) {
            return null;
        }
        // createUnion will return the union all result of queryInput and viewInput
        //           Union
        //       /          |
        // partial query   view
        return createUnion(queryInput, viewInput, rewriteContext);
    }

    protected OptExpression queryBasedRewrite(RewriteContext rewriteContext, PredicateSplit compensationPredicates,
                                              OptExpression queryExpression) {
        // query predicate and (not viewToQueryCompensationPredicate) is the final query compensation predicate
        ScalarOperator queryCompensationPredicate = MvUtils.canonizePredicate(
                Utils.compoundAnd(
                        rewriteContext.getQueryPredicateSplit().toScalarOperator(),
                        CompoundPredicateOperator.not(compensationPredicates.toScalarOperator())));
        if (!ConstantOperator.TRUE.equals(queryCompensationPredicate)) {
            // add filter to op
            Operator.Builder builder = OperatorBuilderFactory.build(queryExpression.getOp());
            builder.withOperator(queryExpression.getOp());
            builder.setPredicate(queryCompensationPredicate);
            Operator newQueryOp = builder.build();
            OptExpression newQueryExpr = OptExpression.create(newQueryOp, queryExpression.getInputs());
            deriveLogicalProperty(newQueryExpr);
            return newQueryExpr;
        }
        return null;
    }

    protected OptExpression createUnion(OptExpression queryInput, OptExpression viewInput, RewriteContext rewriteContext) {
        Map<ColumnRefOperator, ScalarOperator> queryColumnRefMap =
                MvUtils.getColumnRefMap(queryInput, rewriteContext.getQueryRefFactory());

        // keys of queryColumnRefMap and mvColumnRefMap are the same
        List<ColumnRefOperator> originalOutputColumns = queryColumnRefMap.keySet().stream().collect(Collectors.toList());

        // rewrite query
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(materializationContext);
        OptExpression newQueryInput = duplicator.duplicate(queryInput);
        List<ColumnRefOperator> newQueryOutputColumns = duplicator.getMappedColumns(originalOutputColumns);

        // rewrite viewInput
        // for viewInput, there must be a projection,
        // the keyset of the projection is the same as the keyset of queryColumnRefMap
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

        // generate new projection
        Map<ColumnRefOperator, ScalarOperator> unionProjection = Maps.newHashMap();
        for (int i = 0; i < originalOutputColumns.size(); i++) {
            unionProjection.put(originalOutputColumns.get(i), unionOutputColumns.get(i));
        }
        Projection projection = new Projection(unionProjection);
        LogicalUnionOperator unionOperator = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(unionOutputColumns)
                .setChildOutputColumns(Lists.newArrayList(newQueryOutputColumns, newViewOutputColumns))
                .setProjection(projection)
                .isUnionAll(true)
                .build();
        return OptExpression.create(unionOperator, newQueryInput, viewInput);
    }

    protected Multimap<ScalarOperator, ColumnRefOperator> normalizeAndReverseProjection(
            Map<ColumnRefOperator, ScalarOperator> viewProjection, RewriteContext rewriteContext, boolean isViewBased) {
        return normalizeAndReverseProjection(viewProjection, rewriteContext, isViewBased, true);
    }

    protected Multimap<ScalarOperator, ColumnRefOperator> normalizeAndReverseProjection(
            Map<ColumnRefOperator, ScalarOperator> columnRefMap,
            RewriteContext rewriteContext,
            boolean isViewBased, boolean isQueryAgainstView) {
        Multimap<ScalarOperator, ColumnRefOperator> reversedMap = ArrayListMultimap.create();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(entry.getValue());
            if (isQueryAgainstView) {
                if (isViewBased) {
                    ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithViewEc(rewritten);
                    reversedMap.put(swapped, entry.getKey());
                } else {
                    ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
                    reversedMap.put(swapped, entry.getKey());
                }
            } else {
                if (isViewBased) {
                    ScalarOperator swapped = columnRewriter.rewriteByViewEc(rewritten);
                    reversedMap.put(swapped, entry.getKey());
                } else {
                    ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
                    reversedMap.put(swapped, entry.getKey());
                }
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
        boolean isQueryAllEqualJoin = MvUtils.isAllEqualInnerJoin(query);
        boolean isMvAllEqualJoin = MvUtils.isAllEqualInnerJoin(mvExpression);
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
        Map<ColumnRefOperator, ScalarOperator> viewExprMap = MvUtils.getColumnRefMap(
                rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());
        // normalize view projection by query relation and ec
        Multimap<ScalarOperator, ColumnRefOperator> normalizedMap =
                normalizeAndReverseProjection(viewExprMap, rewriteContext, false);

        return rewriteProjection(rewriteContext, normalizedMap, targetExpr);
    }

    protected OptExpression rewriteProjection(RewriteContext rewriteContext,
                                    Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
                                    OptExpression targetExpr) {
        Map<ColumnRefOperator, ScalarOperator> queryMap = MvUtils.getColumnRefMap(
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

    private ScalarOperator compensatePartitionPredicate(OptExpression plan, ColumnRefFactory columnRefFactory) {
        List<LogicalOlapScanOperator> olapScanOperators = MvUtils.getOlapScanNode(plan);
        if (olapScanOperators.isEmpty()) {
            return ConstantOperator.createBoolean(true);
        }
        List<ScalarOperator> partitionPredicates = Lists.newArrayList();
        for (LogicalOlapScanOperator olapScanOperator : olapScanOperators) {
            Preconditions.checkState(olapScanOperator.getTable().isNativeTable());
            OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
            if (olapScanOperator.getSelectedPartitionId() != null
                    && olapScanOperator.getSelectedPartitionId().size() == olapTable.getPartitions().size()) {
                continue;
            }

            if (olapTable.getPartitionInfo() instanceof ExpressionRangePartitionInfo) {
                ExpressionRangePartitionInfo partitionInfo = (ExpressionRangePartitionInfo) olapTable.getPartitionInfo();
                Expr partitionExpr = partitionInfo.getPartitionExprs().get(0);
                List<SlotRef> slotRefs = Lists.newArrayList();
                partitionExpr.collect(SlotRef.class, slotRefs);
                Preconditions.checkState(slotRefs.size() == 1);
                Optional<ColumnRefOperator> partitionColumn = olapScanOperator.getColRefToColumnMetaMap().keySet().stream()
                        .filter(columnRefOperator -> columnRefOperator.getName().equals(slotRefs.get(0).getColumnName()))
                        .findFirst();
                if (!partitionColumn.isPresent()) {
                    return null;
                }
                ExpressionMapping mapping = new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()));
                mapping.put(slotRefs.get(0), partitionColumn.get());
                ScalarOperator partitionScalarOperator =
                        SqlToScalarOperatorTranslator.translate(partitionExpr, mapping, columnRefFactory);
                List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
                for (long pid : olapScanOperator.getSelectedPartitionId()) {
                    selectedRanges.add(partitionInfo.getRange(pid));
                }
                List<Range<PartitionKey>> mergedRanges = MvUtils.mergeRanges(selectedRanges);
                List<ScalarOperator> rangePredicates = MvUtils.convertRanges(partitionScalarOperator, mergedRanges);
                ScalarOperator partitionPredicate = Utils.compoundOr(rangePredicates);
                partitionPredicates.add(partitionPredicate);
            } else if (olapTable.getPartitionInfo() instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                if (partitionColumns.size() != 1) {
                    // now do not support more than one partition columns
                    return null;
                }
                List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
                for (long pid : olapScanOperator.getSelectedPartitionId()) {
                    selectedRanges.add(rangePartitionInfo.getRange(pid));
                }
                List<Range<PartitionKey>> mergedRanges = MvUtils.mergeRanges(selectedRanges);
                ColumnRefOperator partitionColumnRef = olapScanOperator.getColumnReference(partitionColumns.get(0));
                List<ScalarOperator> rangePredicates = MvUtils.convertRanges(partitionColumnRef, mergedRanges);
                ScalarOperator partitionPredicate = Utils.compoundOr(rangePredicates);
                if (partitionPredicate != null) {
                    partitionPredicates.add(partitionPredicate);
                }
            } else {
                return null;
            }
        }
        return partitionPredicates.isEmpty() ? ConstantOperator.createBoolean(true) : Utils.compoundAnd(partitionPredicates);
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
            ColumnRefFactory queryRefFactory, List<Table> queryTables, OptExpression queryExpression,
            ColumnRefFactory mvRefFactory, List<Table> mvTables, OptExpression mvExpression) {
        Map<Table, Set<Integer>> queryTableToRelationId = getTableToRelationid(queryExpression, queryRefFactory, queryTables);
        Map<Table, Set<Integer>> mvTableToRelationId = getTableToRelationid(mvExpression, mvRefFactory, mvTables);
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
                PermutationIterator<Integer> permutationIterator =
                        new PermutationIterator<>(mvTableToRelationId.get(queryEntry.getKey()));
                List<Integer> queryList = queryEntry.getValue().stream().collect(Collectors.toList());
                while (permutationIterator.hasNext()) {
                    List<Integer> permutation = permutationIterator.next();
                    for (BiMap<Integer, Integer> m : result) {
                        final BiMap<Integer, Integer> newM = HashBiMap.create(m);
                        for (int i = 0; i < queryList.size(); i++) {
                            newM.put(queryList.get(i), permutation.get(i));
                        }
                        newResult.add(newM);
                    }
                }
                result = newResult.build();
            }
        }
        return result;
    }

    private Map<Table, Set<Integer>> getTableToRelationid(
            OptExpression optExpression, ColumnRefFactory refFactory, List<Table> tableList) {
        Map<Table, Set<Integer>> tableToRelationId = Maps.newHashMap();
        List<ColumnRefOperator> validColumnRefs = MvUtils.collectScanColumn(optExpression);
        for (Map.Entry<ColumnRefOperator, Table> entry : refFactory.getColumnRefToTable().entrySet()) {
            if (!tableList.contains(entry.getValue())) {
                continue;
            }
            if (!validColumnRefs.contains(entry.getKey())) {
                continue;
            }
            Set<Integer> relationIds = tableToRelationId.computeIfAbsent(entry.getValue(), k -> Sets.newHashSet());
            Integer relationId = refFactory.getRelationId(entry.getKey().getId());
            relationIds.add(relationId);
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
        ScalarOperator compensationPr = getCompensationRangePredicate(srcPr, targetPr, columnRewriter, isQueryAgainstView);
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
        ScalarOperator compensationPu = getCompensationResidualPredicate(srcPu, targetPu, columnRewriter, isQueryAgainstView);
        if (compensationPu == null) {
            return null;
        }

        return PredicateSplit.of(compensationEqualPredicate, compensationPr, compensationPu);
    }

    private ScalarOperator getCompensationResidualPredicate(ScalarOperator srcPu,
                                                            ScalarOperator targetPu,
                                                            ColumnRewriter columnRewriter,
                                                            boolean isQueryAgainstView) {
        ScalarOperator compensationPu;
        if (srcPu == null && targetPu == null) {
            compensationPu = ConstantOperator.createBoolean(true);
        } else if (srcPu == null && targetPu != null) {
            return null;
        } else if (srcPu != null && targetPu == null) {
            compensationPu = srcPu;
        } else {
            ScalarOperator canonizedSrcPu = MvUtils.canonizePredicateForRewrite(srcPu.clone());
            ScalarOperator canonizedTargetPu = MvUtils.canonizePredicateForRewrite(targetPu.clone());
            ScalarOperator swappedSrcPu;
            ScalarOperator swappedTargetPu;
            if (isQueryAgainstView) {
                // src is query
                swappedSrcPu = columnRewriter.rewriteByQueryEc(canonizedSrcPu);
                // target is view
                swappedTargetPu = columnRewriter.rewriteViewToQueryWithQueryEc(canonizedTargetPu);
            } else {
                // src is view
                swappedSrcPu = columnRewriter.rewriteViewToQueryWithViewEc(canonizedSrcPu);
                // target is query
                swappedTargetPu = columnRewriter.rewriteByViewEc(canonizedTargetPu);
            }

            compensationPu = MvUtils.getCompensationPredicateForDisjunctive(swappedSrcPu, swappedTargetPu);
            if (compensationPu == null) {
                compensationPu = getCompensationResidualPredicate(swappedSrcPu, swappedTargetPu);
                if (compensationPu == null) {
                    return null;
                }
            }
        }
        compensationPu = MvUtils.canonizePredicate(compensationPu);
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
                                                         ColumnRewriter columnRewriter,
                                                         boolean isQueryAgainstView) {
        ScalarOperator compensationPr;
        if (srcPr == null && targetPr == null) {
            compensationPr = ConstantOperator.createBoolean(true);
        } else if (srcPr == null && targetPr != null) {
            return null;
        } else {
            ScalarOperator canonizedSrcPr = MvUtils.canonizePredicateForRewrite(srcPr.clone());
            ScalarOperator canonizedTargetPr = targetPr == null ? null : MvUtils.canonizePredicateForRewrite(targetPr.clone());

            // swap column by query EC
            ScalarOperator swappedSrcPr;
            ScalarOperator swappedTargetPr;
            if (isQueryAgainstView) {
                // for query
                swappedSrcPr = columnRewriter.rewriteByQueryEc(canonizedSrcPr);
                // for view, swap column by relation mapping and query ec
                swappedTargetPr = columnRewriter.rewriteViewToQueryWithQueryEc(canonizedTargetPr);
            } else {
                // for view
                swappedSrcPr = columnRewriter.rewriteViewToQueryWithViewEc(canonizedSrcPr);
                // for query
                swappedTargetPr = columnRewriter.rewriteByViewEc(canonizedTargetPr);
            }
            compensationPr = getCompensationRangePredicate(swappedSrcPr, swappedTargetPr);
        }
        return MvUtils.canonizePredicate(compensationPr);
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
        compensation = MvUtils.canonizePredicate(compensation);
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
