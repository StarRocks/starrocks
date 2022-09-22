// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * SPJG materialized view rewriter, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 *  This rewriter is for single or multi table join query rewrite
 */
public class MaterializedViewRewriter {
    private final Pair<ScalarOperator, ScalarOperator> queryPredicatesPair;
    private final EquivalenceClasses queryEc;
    // top projection from query, null if not exist
    private final LogicalProjectOperator queryProjection;
    // query expression below projection
    private final OptExpression query;
    private final MaterializationContext materializationContext;
    private final OptimizerContext optimizerContext;
    private final List<Table> queryTableSet;
    private final List<Table> mvTableSet;

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

    public MaterializedViewRewriter(Pair<ScalarOperator, ScalarOperator> queryPredicatesPair,
                                    EquivalenceClasses queryEc, LogicalProjectOperator queryProjection,
                                    OptExpression query, List<Table> queryTableSet, List<Table> mvTableSet,
                                    MaterializationContext materializationContext,
                                    OptimizerContext optimizerContext) {
        this.queryPredicatesPair = queryPredicatesPair;
        this.queryEc = queryEc;
        this.queryProjection = queryProjection;
        this.query = query;
        this.queryTableSet = queryTableSet;
        this.mvTableSet = mvTableSet;
        this.materializationContext = materializationContext;
        this.optimizerContext = optimizerContext;
    }

    public OptExpression rewriteQuery() {
        // the rewritten expression to replace query
        OptExpression rewrittenExpression = OptExpression.create(materializationContext.getScanMvOperator());

        LogicalProjectOperator mvTopProjection;
        OptExpression mvExpression;
        if (materializationContext.getMvExpression().getOp() instanceof LogicalProjectOperator) {
            mvTopProjection = (LogicalProjectOperator) materializationContext.getMvExpression().getOp();
            mvExpression = materializationContext.getMvExpression().getInputs().get(0);
        } else {
            mvTopProjection = null;
            mvExpression = materializationContext.getMvExpression();
        }
        List<ScalarOperator> mvConjuncts = RewriteUtils.getAllPredicates(mvExpression);
        ScalarOperator mvPredicate = Utils.compoundAnd(mvConjuncts);
        final Pair<ScalarOperator, ScalarOperator> splitedMvPredicate = RewriteUtils.splitPredicate(mvPredicate);

        boolean isQueryAllEqualJoin = RewriteUtils.isAllEqualInnerJoin(query);
        boolean isMvAllEqualJoin = RewriteUtils.isAllEqualInnerJoin(mvExpression);
        MatchMode matchMode = MatchMode.NOT_MATCH;
        if (isQueryAllEqualJoin && isMvAllEqualJoin) {
            // process use table match
            if (queryTableSet.size() == mvTableSet.size() && queryTableSet.containsAll(mvTableSet)) {
                matchMode = MatchMode.COMPLETE;
            } else if (queryTableSet.containsAll(mvTableSet)) {
                // TODO: query delta
                matchMode = MatchMode.QUERY_DELTA;
            } else if (mvTableSet.containsAll(queryTableSet)) {
                // TODO: view delta
                matchMode = MatchMode.VIEW_DELTA;
            } else {
                // can not be rewritten, return null
                return null;
            }
        } else {
            // TODO: process for outer join and not equal join
            // check whether query can be rewritten
            if (mvTableSet.size() > queryTableSet.size()) {
                // check view delta
            } else {
                // check table joins' type must match
            }
        }

        /*
        // generate table mapping
        final Multimap<LogicalScanOperator, LogicalScanOperator> tableMapping = ArrayListMultimap.create();
        List<LogicalScanOperator> queryScanOperators = RewriteUtils.getAllScanOperator(query);
        // List<LogicalScanOperator> mvScanOperators = RewriteUtils.getAllScanOperator(mvExpression);
        for (LogicalScanOperator queryScanOperator1 : queryScanOperators) {
            for (LogicalScanOperator queryScanOperator2 : queryScanOperators) {
                if (queryScanOperator1.getTable().getId() == queryScanOperator2.getTable().getId()) {
                    tableMapping.put(queryScanOperator1, queryScanOperator2);
                }
            }
        }

         */

        EquivalenceClasses mvEc = null;
        Pair<ScalarOperator, ScalarOperator> compensationPredicates =
                getCompensationPredicates(queryEc, queryPredicatesPair, mvEc, splitedMvPredicate);

        return null;
    }

    private Pair<ScalarOperator, ScalarOperator> getCompensationPredicates(
            EquivalenceClasses sourceEc, Pair<ScalarOperator, ScalarOperator> sourcePredicates,
            EquivalenceClasses targetEc, Pair<ScalarOperator, ScalarOperator> targetPredicates) {

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
        return null;
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
        // it is mapping from source to target
        // it may be 1 to n
        final Multimap<Integer, Integer> mapping = computeMapping(sourceEcs, targetEcs);
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
    private Multimap<Integer, Integer> computeMapping(List<Set<ColumnRefOperator>> sourceEcs,
                                                      List<Set<ColumnRefOperator>> targetEcs) {
        Multimap<Integer, Integer> mapping = ArrayListMultimap.create();
        for (int i = 0; i < targetEcs.size(); i++) {
            final Set<ColumnRefOperator> targetSet = targetEcs.get(i);
            final Set<String> targetColumnSet =
                    targetSet.stream().map(columnRefOperator -> columnRefOperator.getName()).collect(Collectors.toSet());
            boolean contained = false;
            for (int j = 0; j < sourceEcs.size(); j++) {
                final Set<ColumnRefOperator> srcSet = sourceEcs.get(j);
                // check column name set
                final Set<String> srcColumnSet =
                        srcSet.stream().map(columnRefOperator -> columnRefOperator.getName()).collect(Collectors.toSet());
                if (srcColumnSet.containsAll(targetColumnSet)) {
                    mapping.put(j, i);
                    contained = true;
                    break;
                }
            }
            if (!contained) {
                return null;
            }
        }
        return mapping;
    }
}
