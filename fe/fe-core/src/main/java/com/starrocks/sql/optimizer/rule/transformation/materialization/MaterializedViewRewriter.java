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
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * SPJG materialized view rewriter, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 *  This rewriter is for single or multi table join query rewrite
 */
public class MaterializedViewRewriter {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewRewriter.class);
    private final Triple<ScalarOperator, ScalarOperator, ScalarOperator> queryPredicateTriple;
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

    public MaterializedViewRewriter(Triple<ScalarOperator, ScalarOperator, ScalarOperator> queryPredicateTriple,
                                    EquivalenceClasses queryEc, LogicalProjectOperator queryProjection,
                                    OptExpression query, List<Table> queryTableSet, List<Table> mvTableSet,
                                    MaterializationContext materializationContext,
                                    OptimizerContext optimizerContext) {
        this.queryPredicateTriple = queryPredicateTriple;
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

        Map<Integer, List<ColumnRefOperator>> queryRelationIdToColumns =
                getRelationIdToColumns(optimizerContext.getColumnRefFactory());
        Map<Integer, List<ColumnRefOperator>> mvRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getMvColumnRefFactory());
        List<BiMap<Integer, Integer>> relationIdMappings = generateRelationIdMap(optimizerContext.getColumnRefFactory(),
                materializationContext.getMvColumnRefFactory());


        for (BiMap<Integer, Integer> relationIdMapping : relationIdMappings) {
            // construct query based view EC
            ScalarOperator queryBasedViewEqualPredicate = rewriteColumnByRelationIdMap(mvPredicateTriple.getLeft(),
                    relationIdMapping, queryRelationIdToColumns, optimizerContext.getColumnRefFactory(),
                    mvRelationIdToColumns, materializationContext.getMvColumnRefFactory());
            EquivalenceClasses queryBaseViewEc = new EquivalenceClasses();
            for (ScalarOperator conjunct : Utils.extractConjuncts(queryBasedViewEqualPredicate)) {
                queryBaseViewEc.addEquivalence((ColumnRefOperator)conjunct.getChild(0),
                        (ColumnRefOperator)conjunct.getChild(1));
            }

            Pair<ScalarOperator, ScalarOperator> compensationPredicates =
                    getCompensationPredicates(queryEc, queryPredicateTriple, optimizerContext.getColumnRefFactory(),
                            queryBaseViewEc, mvPredicateTriple, materializationContext.getMvColumnRefFactory(),
                            relationIdMapping, queryRelationIdToColumns, mvRelationIdToColumns);
            if (compensationPredicates == null) {
                // TODO: should try to rewrite by union
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

                }
            }
        }
        return null;
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
                srcRefFactory, targetRelationIdToColumns, targetRefFactory,null);
    }

    private ScalarOperator rewriteColumnByEc(ScalarOperator predicate, EquivalenceClasses ec) {
        return rewriteColumnByRelationIdMapAndEc(predicate, null, null,
                null, null, null,ec);
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
                ColumnRefOperator result = null;
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
                    for (ColumnRefOperator dstColumnRef : relationColumns) {
                        if (columnRef.getName().equals(dstColumnRef.getName())) {
                            result = dstColumnRef;
                            break;
                        }
                    }
                    LOG.warn("can not find column ref:%s in target relation:%d", columnRef, targetRelationId);
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
        Map<Table, Set<Integer>> queryTableToRelationId = getTableToRelationid(queryRefFactory);
        Map<Table, Set<Integer>> mvTableToRelationId = getTableToRelationid(mvRefFactory);
        Preconditions.checkState(queryTableToRelationId.keySet().equals(mvTableToRelationId.keySet()));
        List<BiMap<Integer, Integer>> result = ImmutableList.of(HashBiMap.create());
        for (Map.Entry<Table, Set<Integer>> queryEntry : queryTableToRelationId.entrySet()) {
            Preconditions.checkState(queryEntry.getValue().size() > 0);
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

    Map<Table, Set<Integer>> getTableToRelationid(ColumnRefFactory refFactory) {
        Map<Table, Set<Integer>> tableToRelationId = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, Table> entry : refFactory.getColumnRefToTable().entrySet()) {
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
        for(Map.Entry<Integer, Integer> entry : refFactory.getColumnToRelationIds().entrySet()) {
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
        ScalarOperator canonizedSrcPr = RewriteUtils.canonizeNode(srcPredicateTriple.getMiddle());
        ScalarOperator canonizedTargetPr = RewriteUtils.canonizeNode(targetPredicateTriple.getMiddle());

        // swap column by source EC
        ScalarOperator srcPr = rewriteColumnByEc(canonizedSrcPr, sourceEc);

        // swap target with source relation and source EC
        ScalarOperator targetPr = rewriteColumnByRelationIdMapAndEc(canonizedTargetPr,
                relationIdMap, srcRelationIdToColumns, srcColumnRefFactory,
                targetRelationIdToColumns, targetColumnRefFactory, sourceEc);

        ScalarOperator compensationPr = getCompensationRangePredicate(srcPr, targetPr);
        if (compensationPr == null) {
            return null;
        }

        ScalarOperator canonizedSrcPu = RewriteUtils.canonizeNode(srcPredicateTriple.getRight());
        ScalarOperator canonizedTargetPu = RewriteUtils.canonizeNode(targetPredicateTriple.getRight());
        ScalarOperator srcPu = rewriteColumnByEc(canonizedSrcPu, sourceEc);;
        ScalarOperator targetPu = rewriteColumnByRelationIdMapAndEc(canonizedTargetPu,
                relationIdMap, srcRelationIdToColumns, srcColumnRefFactory,
                targetRelationIdToColumns, targetColumnRefFactory, sourceEc);

        ScalarOperator compensationPu = RewriteUtils.splitOr(srcPu, targetPu);
        if (compensationPu == null) {
            compensationPu = getCompensationResidualPredicate(srcPu, targetPu);
            if (compensationPu == null) {
                return null;
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
