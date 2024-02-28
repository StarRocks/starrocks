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

import com.google.common.base.Joiner;
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
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.StringUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.PermutationGenerator;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.JoinPredicatePushdown;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.MvNormalizePredicateRule;
import com.starrocks.sql.optimizer.rule.mv.JoinDeriveContext;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

/*
 * SPJG materialized view rewriter, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 *  This rewriter is for single table or multi table join query rewrite
 */
public class MaterializedViewRewriter {
    protected static final Logger LOG = LogManager.getLogger(MaterializedViewRewriter.class);
    protected final MvRewriteContext mvRewriteContext;
    protected final MaterializationContext materializationContext;
    protected final QueryMaterializationContext queryMaterializationContext;
    protected final OptimizerContext optimizerContext;
    // Mark whether query's plan is rewritten by materialized view.
    public static final String REWRITE_SUCCESS = "Rewrite Succeed";
    public static final int OP_UNION_ALL_BIT = 1 << 0;

    private static final Map<JoinOperator, List<JoinOperator>> JOIN_COMPATIBLE_MAP =
            ImmutableMap.<JoinOperator, List<JoinOperator>>builder()
                    .put(JoinOperator.INNER_JOIN, Lists.newArrayList(JoinOperator.LEFT_SEMI_JOIN, JoinOperator.RIGHT_SEMI_JOIN))
                    .put(JoinOperator.LEFT_OUTER_JOIN, Lists.newArrayList(JoinOperator.INNER_JOIN, JoinOperator.LEFT_ANTI_JOIN))
                    .put(JoinOperator.RIGHT_OUTER_JOIN, Lists.newArrayList(JoinOperator.INNER_JOIN, JoinOperator.RIGHT_ANTI_JOIN))
                    .put(JoinOperator.FULL_OUTER_JOIN, Lists.newArrayList(JoinOperator.INNER_JOIN,
                            JoinOperator.LEFT_OUTER_JOIN, JoinOperator.RIGHT_OUTER_JOIN))
                    .build();

    public enum MatchMode {
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

    public MaterializedViewRewriter(MvRewriteContext mvRewriteContext) {
        this.mvRewriteContext = mvRewriteContext;
        this.materializationContext = mvRewriteContext.getMaterializationContext();
        this.optimizerContext = materializationContext.getOptimizerContext();
        this.queryMaterializationContext = optimizerContext.getQueryMaterializationContext();
    }

    /**
     * Whether the MV expression can be satisfiable for input query, eg AggregateScanRule require
     * MV expression should contain Aggregation operator.
     *
     * @param expression: MV expression.
     */
    public boolean isValidPlan(OptExpression expression) {
        return MvUtils.isLogicalSPJ(expression);
    }

    private boolean isMVApplicable(OptExpression mvExpression, List<Table> queryTables, List<Table> mvTables,
                                   MatchMode matchMode, OptExpression queryExpression) {
        // Only care MatchMode.COMPLETE and VIEW_DELTA here, QUERY_DELTA also can be supported
        // because optimizer will match MV's pattern which is subset of query opt tree
        // from top-down iteration.
        if (matchMode == MatchMode.COMPLETE) {
            // If all join types are inner/cross, no need check join orders: eg a inner join b or b inner join a.
            boolean isQueryAllEqualInnerJoin = MvUtils.isAllEqualInnerOrCrossJoin(queryExpression);
            boolean isMVAllEqualInnerJoin = MvUtils.isAllEqualInnerOrCrossJoin(mvExpression);
            if (isQueryAllEqualInnerJoin && isMVAllEqualInnerJoin) {
                // do nothing.
            } else {
                // If not all join types are InnerJoin, need to check whether MV's join tables' order
                // matches Query's join tables' order.
                // eg. MV   : a left join b inner join c
                //     Query: b left join a inner join c (cannot rewrite)
                //     Query: a left join b inner join c (can rewrite)
                //     Query: c inner join a left join b (can rewrite)
                // NOTE: Only support all MV's join tables' order exactly match with the query's join tables'
                // order for now.
                // Use traverse order to check whether all joins' order and operator are exactly matched.
                return computeCompatibility(queryExpression, mvExpression);
            }
        }

        if (!isValidPlan(mvExpression)) {
            logMVRewrite(mvRewriteContext, "MV is not applicable: MV's plan is not valid");
            return false;
        }

        return true;
    }

    /**
     * Checks whether the join-on predicate of a query is equivalent to the join-on predicate
     * of a materialized view (MV).
     */
    boolean checkJoinOnPredicateFromOnPredicates(Set<ScalarOperator> queryPredicates, Set<ScalarOperator> diff) {
        for (ScalarOperator queryPredicate : queryPredicates) {
            if (!diff.removeIf(p -> ScalarOperator.isEquivalent(queryPredicate, p))) {
                return false;
            }
        }
        return true;
    }

    boolean checkJoinOnPredicate(LogicalJoinOperator mvJoinOp,
                                 LogicalJoinOperator queryJoinOp) {
        final JoinOperator mvJoinType = mvJoinOp.getJoinType();
        final JoinOperator queryJoinType = queryJoinOp.getJoinType();
        // ignore if join's type both are inner join or cross join
        if ((mvJoinType.isInnerJoin() && queryJoinType.isInnerJoin()) ||
                (mvJoinType.isCrossJoin() && queryJoinType.isCrossJoin())) {
            return true;
        }

        // Checks whether the join-on predicate of a query is equivalent to the join-on predicate
        // of a materialized view (MV).
        final ScalarOperator mvJoinOnPredicate = mvJoinOp.getOnPredicate();
        final ScalarOperator queryJoinOnPredicate = queryJoinOp.getOnPredicate();
        final ScalarOperator normQueryJoinOnPredicate = MvUtils.canonizePredicateForRewrite(queryMaterializationContext,
                queryJoinOnPredicate);
        final ScalarOperator normMVJoinOnPredicate = MvUtils.canonizePredicateForRewrite(queryMaterializationContext,
                mvJoinOnPredicate);
        final Set<ScalarOperator> queryJoinOnPredicates = Sets.newHashSet(Utils.extractConjuncts(normQueryJoinOnPredicate));
        final Set<ScalarOperator> diffPredicates = Sets.newHashSet(Utils.extractConjuncts(normMVJoinOnPredicate));
        if (!checkJoinOnPredicateFromOnPredicates(queryJoinOnPredicates, diffPredicates)) {
            logMVRewrite(mvRewriteContext, "join predicate is different {}(query) != (mv){}, " +
                            "diff: {}", queryJoinOnPredicate, mvJoinOnPredicate,
                    Joiner.on(",").join(diffPredicates));
            return false;
        }

        if (diffPredicates.isEmpty()) {
            return true;
        }

        // try to deduce from join's on predicates or other predicates.
        List<ScalarOperator> queryPredicates =
                Utils.extractConjuncts(mvRewriteContext.getQueryPredicateSplit().getRangePredicates());
        if (!checkJoinOnPredicateFromPredicates(queryPredicates, diffPredicates)) {
            logMVRewrite(mvRewriteContext, "join predicate is different after compensate {}(query) != (mv){}, " +
                            "diff: {}", queryJoinOnPredicate, mvJoinOnPredicate,
                    Joiner.on(",").join(diffPredicates));
            return false;
        }
        return true;
    }

    /**
     * Check whether use range equal predicates can eliminate the diff predicates.
     * @param diffPredicates : predicates that query join on predicates differ from the mv join on predicates
     * @return
     */
    boolean checkJoinOnPredicateFromPredicates(List<ScalarOperator> predicates,
                                               Set<ScalarOperator> diffPredicates) {
        for (ScalarOperator diffPredicate : diffPredicates) {
            if (!(diffPredicate instanceof BinaryPredicateOperator)) {
                return false;
            }

            BinaryPredicateOperator diffBinaryPredicate = (BinaryPredicateOperator) diffPredicate;
            if (!diffBinaryPredicate.getBinaryType().isEqual()) {
                return false;
            }

            ScalarOperator left = diffBinaryPredicate.getChild(0);
            ScalarOperator right = diffBinaryPredicate.getChild(1);
            if (!left.isColumnRef() || !right.isColumnRef()) {
                return false;
            }
        }

        EquivalenceClasses queryEc = new EquivalenceClasses();
        deduceEquivalenceClassesFromRangePredicates(predicates, queryEc, false);

        for (ScalarOperator diffPredicate : diffPredicates) {
            BinaryPredicateOperator diffBinaryPredicate = (BinaryPredicateOperator) diffPredicate;
            Preconditions.checkState(diffBinaryPredicate.getBinaryType().isEqual());

            ScalarOperator left = diffBinaryPredicate.getChild(0);
            ScalarOperator right = diffBinaryPredicate.getChild(1);
            Preconditions.checkState(left.isColumnRef() && right.isColumnRef());
            ColumnRefOperator l = (ColumnRefOperator) left;
            ColumnRefOperator r = (ColumnRefOperator) right;
            if (!queryEc.containsEquivalentKey(l) || !queryEc.containsEquivalentKey(r)) {
                return false;
            }
        }
        return true;
    }

    // Post-order traversal
    public boolean computeCompatibility(OptExpression queryExpr, OptExpression mvExpr) {
        LogicalOperator queryOp = (LogicalOperator) queryExpr.getOp();
        LogicalOperator mvOp = (LogicalOperator) mvExpr.getOp();
        if (!queryOp.getOpType().equals(mvOp.getOpType())) {
            return false;
        }
        if (queryOp instanceof LogicalJoinOperator) {
            boolean leftCompatability = computeCompatibility(queryExpr.inputAt(0), mvExpr.inputAt(0));
            boolean rightCompatability = computeCompatibility(queryExpr.inputAt(1), mvExpr.inputAt(1));
            if (!leftCompatability || !rightCompatability) {
                return false;
            }
            // compute join compatibility
            LogicalJoinOperator queryJoin = (LogicalJoinOperator) queryExpr.getOp();
            JoinOperator queryJoinType = queryJoin.getJoinType();

            LogicalJoinOperator mvJoin = (LogicalJoinOperator) mvExpr.getOp();
            JoinOperator mvJoinType = mvJoin.getJoinType();

            // Rewrite non-inner/cross join's on-predicates, all on-predicates should not compensate.
            // For non-inner/cross join, we must ensure all on-predicates are not compensated, otherwise there may
            // be some correctness bugs.
            if (!checkJoinOnPredicate(mvJoin, queryJoin)) {
                return false;
            }

            if (queryJoinType.equals(mvJoinType)) {
                // it means both joins' type and onPredicate are equal
                // for outer join, we should check some extra conditions
                if (!checkJoinMatch(queryJoinType, queryExpr, mvExpr)) {
                    logMVRewrite(mvRewriteContext, "join match check failed, joinType: {}", queryJoinType);
                    return false;
                }
                return true;
            }

            if (!JOIN_COMPATIBLE_MAP.containsKey(mvJoinType) ||
                    !JOIN_COMPATIBLE_MAP.get(mvJoinType).contains(queryJoinType)) {
                logMVRewrite(mvRewriteContext, "join type is not compatible {} not contains {}", mvJoinType,
                        queryJoinType);
                return false;
            }

            // only support table column equality predicates like A.c1 = B.c1 && A.c2 = B.c2
            // should know the right table
            ScalarOperator queryOnPredicate = queryJoin.getOnPredicate();
            // relationId -> join on predicate used columns
            Map<Integer, ColumnRefSet> tableToJoinColumns = Maps.newHashMap();
            // first is from left, second is from right
            List<Pair<ColumnRefOperator, ColumnRefOperator>> joinColumnPairs = Lists.newArrayList();
            ColumnRefSet leftColumns = queryExpr.inputAt(0).getOutputColumns();
            boolean isSupported = isSupportedPredicate(queryOnPredicate, materializationContext.getQueryRefFactory(),
                    leftColumns, tableToJoinColumns, joinColumnPairs);
            if (!isSupported) {
                logMVRewrite(mvRewriteContext, "join predicate is not supported {}", queryOnPredicate);
                return false;
            }
            // use join columns from query
            Map<ColumnRefSet, Table> usedColumnsToTable = Maps.newHashMap();
            for (Map.Entry<Integer, ColumnRefSet> entry : tableToJoinColumns.entrySet()) {
                Table table = materializationContext.getQueryRefFactory().getTableForColumn(entry.getValue().getFirstId());
                usedColumnsToTable.put(entry.getValue(), table);
            }

            ColumnRefSet leftJoinColumns = new ColumnRefSet();
            ColumnRefSet rightJoinColumns = new ColumnRefSet();
            for (Pair<ColumnRefOperator, ColumnRefOperator> pair : joinColumnPairs) {
                leftJoinColumns.union(pair.first);
                rightJoinColumns.union(pair.second);
            }
            List<List<ColumnRefOperator>> joinColumnRefs = Lists.newArrayList(Lists.newArrayList(), Lists.newArrayList());
            // query based join columns pair
            List<Pair<ColumnRefOperator, ColumnRefOperator>> compensatedEquivalenceColumns = Lists.newArrayList();
            boolean isCompatible = isJoinCompatible(usedColumnsToTable, queryJoinType, mvJoinType,
                    leftJoinColumns, rightJoinColumns, joinColumnPairs, joinColumnRefs, compensatedEquivalenceColumns);
            if (!isCompatible) {
                logMVRewrite(mvRewriteContext, "join columns not compatible {} != {}, query join type: {}, mv join type: {}",
                        leftJoinColumns, rightJoinColumns, queryJoinType, mvJoinType);
                return false;
            }
            // here collect output columns to compensate the derived predicates if necessary.
            // use mv output columns because the output is decided by mv.
            // if the column is not in mv's output, it can not be used as compensated predicate.
            List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
            childOutputColumns.add(mvExpr.getChildOutputColumns(0)
                    .getColumnRefOperators(materializationContext.getMvColumnRefFactory()));
            childOutputColumns.add(mvExpr.getChildOutputColumns(1)
                    .getColumnRefOperators(materializationContext.getMvColumnRefFactory()));
            JoinDeriveContext joinDeriveContext = new JoinDeriveContext(
                    queryJoinType, mvJoinType, joinColumnRefs, compensatedEquivalenceColumns, childOutputColumns);
            mvRewriteContext.addJoinDeriveContext(joinDeriveContext);
            return true;
        } else if (queryOp instanceof LogicalScanOperator) {
            Preconditions.checkState(mvOp instanceof LogicalScanOperator);
            Table queryTable = ((LogicalScanOperator) queryOp).getTable();
            Table mvTable = ((LogicalScanOperator) mvOp).getTable();
            return mvTable.equals(queryTable);
        } else if (queryOp instanceof LogicalAggregationOperator) {
            // consider aggregation
            return computeCompatibility(queryExpr.inputAt(0), mvExpr.inputAt(0));
        } else {
            throw new UnsupportedOperationException("unsupported operator:" + queryOp.getClass());
        }
    }

    private boolean checkJoinMatch(JoinOperator joinOperator, OptExpression queryExpr, OptExpression mvExpr) {
        if (joinOperator.isInnerJoin() || joinOperator.isCrossJoin() || joinOperator.isSemiJoin()) {
            return true;
        } else if (joinOperator.isLeftOuterJoin() || joinOperator.isLeftAntiJoin()) {
            // for left outer/anti join, we should make sure the predicates of right child should be equivalent
            return checkJoinChildPredicate(queryExpr, mvExpr, 1);
        } else if (joinOperator.isRightOuterJoin() || joinOperator.isRightAntiJoin()) {
            // for right outer/anti join, we should make sure the predicates of left child should be equivalent
            return checkJoinChildPredicate(queryExpr, mvExpr, 0);
        } else if (joinOperator.isFullOuterJoin()) {
            // for full outer join, we should make sure the predicates of both children should be equivalent
            return checkJoinChildPredicate(queryExpr, mvExpr, 0) && checkJoinChildPredicate(queryExpr, mvExpr, 1);
        }
        return false;
    }

    private boolean checkJoinChildPredicate(OptExpression queryExpr, OptExpression mvExpr, int index) {
        // extract the ith child's all conjuncts in query
        Collection<ScalarOperator> queryPredicates = MvUtils.canonizePredicatesForRewrite(queryMaterializationContext,
                MvUtils.getAllValidPredicates(queryExpr.inputAt(index)));

        // extract the ith child's all conjuncts in mv
        Collection<ScalarOperator> mvPredicates = MvUtils.canonizePredicatesForRewrite(queryMaterializationContext,
                MvUtils.getAllValidPredicates(mvExpr.inputAt(index)));

        if (queryPredicates.isEmpty() && mvPredicates.isEmpty()) {
            return true;
        }
        if (!isAllPredicateEquivalent(mvPredicates, queryPredicates)) {
            logMVRewrite(mvRewriteContext, "join child predicate not matched, queryPredicates: {}, " +
                            "mvPredicates: {}, index: {}", queryPredicates, mvPredicates, index);
            return false;
        }
        return true;
    }

    private boolean isAllPredicateEquivalent(Collection<ScalarOperator> queryPredicates,
                                             final Collection<ScalarOperator> mvPredicates) {
        return queryPredicates.size() == mvPredicates.size()
                && queryPredicates.stream().allMatch(queryPredicate ->
                mvPredicates.stream().anyMatch(mvPredicate -> mvPredicate.equivalent(queryPredicate)));
    }

    private boolean isJoinCompatible(
            Map<ColumnRefSet, Table> usedColumnsToTable, JoinOperator queryJoinType,
            JoinOperator mvJoinType,
            ColumnRefSet leftColumns,
            ColumnRefSet rightColumns,
            List<Pair<ColumnRefOperator, ColumnRefOperator>> joinColumnPairs,
            List<List<ColumnRefOperator>> joinColumnRefs,
            List<Pair<ColumnRefOperator, ColumnRefOperator>> compensatedEquivalenceColumns) {
        boolean isCompatible = true;
        if (mvJoinType.isInnerJoin() && queryJoinType.isLeftSemiJoin()) {
            // rewrite left semi join to inner join
            // check join keys of the right table are unique keys or primary keys
            Optional<ColumnRefSet> rightColumnRefSet = usedColumnsToTable.keySet()
                    .stream().filter(columnSet -> rightColumns.containsAll(columnSet)).findFirst();
            if (!rightColumnRefSet.isPresent()) {
                return false;
            }
            Table rightTable = usedColumnsToTable.get(rightColumnRefSet.get());
            List<ColumnRefOperator> rightJoinColumnRefs =
                    rightColumnRefSet.get().getColumnRefOperators(materializationContext.getQueryRefFactory());
            List<String> columnNames =
                    rightJoinColumnRefs.stream().map(columnRef -> columnRef.getName()).collect(Collectors.toList());
            Preconditions.checkNotNull(rightTable);
            isCompatible = isUniqueColumns(rightTable, columnNames);
        } else if (mvJoinType.isInnerJoin() && queryJoinType.isRightSemiJoin()) {
            // rewrite right semi join to inner join
            // check join keys of the left table are unique keys or primary keys
            Optional<ColumnRefSet> leftColumnRefSet = usedColumnsToTable.keySet()
                    .stream().filter(columnSet -> leftColumns.containsAll(columnSet)).findFirst();
            if (!leftColumnRefSet.isPresent()) {
                return false;
            }
            Table leftTable = usedColumnsToTable.get(leftColumnRefSet.get());
            List<ColumnRefOperator> leftJoinColumnRefs =
                    leftColumnRefSet.get().getColumnRefOperators(materializationContext.getQueryRefFactory());
            List<String> columnNames =
                    leftJoinColumnRefs.stream().map(columnRef -> columnRef.getName()).collect(Collectors.toList());
            Preconditions.checkNotNull(leftTable);
            isCompatible = isUniqueColumns(leftTable, columnNames);
        } else if (mvJoinType.isLeftOuterJoin() && (queryJoinType.isInnerJoin() || queryJoinType.isLeftAntiJoin())) {
            List<ColumnRefOperator> rightJoinColumnRefs =
                    rightColumns.getColumnRefOperators(materializationContext.getQueryRefFactory());
            joinColumnRefs.set(1, rightJoinColumnRefs);
            if (queryJoinType.isInnerJoin()) {
                // for inner join, we should add the join columns into equivalence
                compensatedEquivalenceColumns.addAll(joinColumnPairs);
            }
        } else if (mvJoinType.isRightOuterJoin() && (queryJoinType.isInnerJoin() || queryJoinType.isRightAntiJoin())) {
            List<ColumnRefOperator> leftJoinColumnRefs =
                    leftColumns.getColumnRefOperators(materializationContext.getQueryRefFactory());
            joinColumnRefs.set(0, leftJoinColumnRefs);
            if (queryJoinType.isInnerJoin()) {
                // for inner join, we should add the join columns into equivalence
                compensatedEquivalenceColumns.addAll(joinColumnPairs);
            }
        } else if (mvJoinType.isFullOuterJoin() && queryJoinType.isLeftOuterJoin()) {
            List<ColumnRefOperator> leftJoinColumnRefs =
                    leftColumns.getColumnRefOperators(materializationContext.getQueryRefFactory());
            joinColumnRefs.set(0, leftJoinColumnRefs);
        } else if (mvJoinType.isFullOuterJoin() && queryJoinType.isRightOuterJoin()) {
            List<ColumnRefOperator> rightJoinColumnRefs =
                    rightColumns.getColumnRefOperators(materializationContext.getQueryRefFactory());
            joinColumnRefs.set(1, rightJoinColumnRefs);
        } else if (mvJoinType.isFullOuterJoin() && queryJoinType.isInnerJoin()) {
            List<ColumnRefOperator> leftJoinColumnRefs =
                    leftColumns.getColumnRefOperators(materializationContext.getQueryRefFactory());
            joinColumnRefs.set(0, leftJoinColumnRefs);
            List<ColumnRefOperator> rightJoinColumnRefs =
                    rightColumns.getColumnRefOperators(materializationContext.getQueryRefFactory());
            joinColumnRefs.set(1, rightJoinColumnRefs);
            // for inner join, we should add the join columns into equivalence
            compensatedEquivalenceColumns.addAll(joinColumnPairs);
        }
        return isCompatible;
    }

    private boolean isUniqueColumns(Table table, List<String> columnNames) {
        if (table.isNativeTable()) {
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS || olapTable.getKeysType() == KeysType.UNIQUE_KEYS) {
                List<String> keyColumnNames =
                        olapTable.getKeyColumns().stream().map(column -> column.getName()).collect(Collectors.toList());
                return columnNames.containsAll(keyColumnNames);
            }
        }
        return table.hasUniqueConstraints() && table.getUniqueConstraints().stream().anyMatch(
                uniqueConstraint -> columnNames.containsAll(uniqueConstraint.getUniqueColumns()));
    }

    private boolean isSupportedPredicate(
            ScalarOperator onPredicate, ColumnRefFactory columnRefFactory, ColumnRefSet leftColumns,
            Map<Integer, ColumnRefSet> tableToJoinColumns, List<Pair<ColumnRefOperator, ColumnRefOperator>> joinColumnPairs) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(onPredicate);
        List<ScalarOperator> binaryPredicates = conjuncts.stream()
                .filter(conjunct -> ScalarOperator.isColumnEqualBinaryPredicate(conjunct)).collect(Collectors.toList());
        if (binaryPredicates.isEmpty()) {
            return false;
        }

        for (ScalarOperator scalarOperator : binaryPredicates) {
            if (leftColumns.containsAll(scalarOperator.getChild(0).getUsedColumns())) {
                joinColumnPairs.add(Pair.create(scalarOperator.getChild(0).cast(), scalarOperator.getChild(1).cast()));
            } else {
                joinColumnPairs.add(Pair.create(scalarOperator.getChild(1).cast(), scalarOperator.getChild(0).cast()));
            }
        }

        ColumnRefSet usedColumns = Utils.compoundAnd(binaryPredicates).getUsedColumns();
        for (int columnId : usedColumns.getColumnIds()) {
            int relationId = columnRefFactory.getRelationId(columnId);
            if (relationId == -1) {
                // not use the column from table scan, unsupported
                continue;
            }
            ColumnRefSet refColumns = tableToJoinColumns.computeIfAbsent(relationId, k -> new ColumnRefSet());
            refColumns.union(columnId);
        }
        return true;
    }

    public OptExpression rewrite() {
        final OptExpression queryExpression = mvRewriteContext.getQueryExpression();
        final OptExpression mvExpression = materializationContext.getMvExpression();
        final List<Table> queryTables = mvRewriteContext.getQueryTables();
        final List<Table> mvTables = MvUtils.getAllTables(mvExpression);

        MatchMode matchMode = getMatchMode(queryTables, mvTables);

        // Check whether mv can be applicable for the query.
        if (!isMVApplicable(mvExpression, queryTables, mvTables, matchMode, queryExpression)) {
            return null;
        }

        final ColumnRefFactory mvColumnRefFactory = materializationContext.getMvColumnRefFactory();
        final ReplaceColumnRefRewriter mvColumnRefRewriter =
                MvUtils.getReplaceColumnRefWriter(mvExpression, mvColumnRefFactory);

        final Set<ScalarOperator> mvConjuncts = MvUtils.getPredicateForRewrite(mvExpression);
        ScalarOperator mvPartitionCompensate = compensateMVPartitionPredicate(mvConjuncts, mvColumnRefRewriter);
        if (mvPartitionCompensate != ConstantOperator.TRUE) {
            mvConjuncts.addAll(MvUtils.getAllValidPredicates(mvPartitionCompensate));
        }
        final PredicateSplit mvPredicateSplit =
                queryMaterializationContext.getPredicateSplit(mvConjuncts, mvColumnRefRewriter);

        logMVRewrite(mvRewriteContext, "Try to rewrite query with mv:{}, match mode:{}",
                materializationContext.getMv().getName(), matchMode);
        if (matchMode == MatchMode.VIEW_DELTA) {
            return rewriteViewDelta(queryTables, mvTables, mvPredicateSplit, mvColumnRefRewriter,
                    queryExpression, mvExpression);
        } else {
            Preconditions.checkState(matchMode == MatchMode.COMPLETE);
            return rewriteComplete(queryTables, mvTables, matchMode, mvPredicateSplit, mvColumnRefRewriter,
                    null, null, null);
        }
    }

    /**
     * If materialized view has to-refreshed partitions, need to add compensated partition predicate
     * into materialized view's predicate split. so it can be used for predicate compensate or union all
     * rewrite.
     */
    private ScalarOperator compensateMVPartitionPredicate(Set<ScalarOperator> mvPredicates,
                                                          ReplaceColumnRefRewriter mvColumnRefRewriter) {
        ScalarOperator mvPrunedPartitionPredicate = getMVCompensatePartitionPredicate();
        if (mvPrunedPartitionPredicate != null) {
            List<ScalarOperator> partitionPredicates =
                    getPartitionRelatedPredicates(mvPredicates, mvRewriteContext.getMaterializationContext().getMv());
            if (partitionPredicates.stream().noneMatch(p -> (p instanceof IsNullPredicateOperator)
                    && !((IsNullPredicateOperator) p).isNotNull())) {
                // there is no partition column is null predicate
                // add latest partition predicate to mv predicate
                return mvColumnRefRewriter.rewrite(mvPrunedPartitionPredicate);
            }
        }
        return ConstantOperator.TRUE;
    }

    /**
     * Only compensate mv's partition predicate when mv's freshness cannot satisfy query's need.
     * When mv's freshness is ok, return constant true because
     * {@link com.starrocks.sql.optimizer.MaterializedViewOptimizer#optimize} disabled partition pruning and no need
     * compensate pruned predicates.
     * @return
     */
    private ScalarOperator getMVCompensatePartitionPredicate() {
        return  materializationContext.isCompensatePartitionPredicate() ?
                materializationContext.getMvPartialPartitionPredicate() : ConstantOperator.TRUE;
    }

    private OptExpression rewriteViewDelta(List<Table> queryTables,
                                           List<Table> mvTables,
                                           PredicateSplit mvPredicateSplit,
                                           ReplaceColumnRefRewriter mvColumnRefRewriter,
                                           OptExpression queryExpression,
                                           OptExpression mvExpression) {
        List<TableScanDesc> queryTableScanDescs = MvUtils.getTableScanDescs(queryExpression);
        List<TableScanDesc> mvTableScanDescs = MvUtils.getTableScanDescs(mvExpression);

        // NOTE: When queries/mvs have multi same tables in snowflake-schema mode, eg:
        // MV   : B <-> A <-> D <-> C <-> E <-> A <-> B
        // QUERY:  A <-> D <-> C <-> E <-> A <-> B
        // It's not easy to decide which tables are needed to compensate into query,
        // so iterate the possible permutations to tryRewrite.
        Map<Table, List<TableScanDesc>> mvTableToTableScanDecs = Maps.newHashMap();
        for (TableScanDesc mvTableScanDesc : mvTableScanDescs) {
            mvTableToTableScanDecs.computeIfAbsent(mvTableScanDesc.getTable(), x -> Lists.newArrayList())
                    .add(mvTableScanDesc);
        }
        List<List<TableScanDesc>> mvExtraTableScanDescLists = Lists.newArrayList();
        for (TableScanDesc mvTableScanDesc : mvTableScanDescs) {
            if (!queryTableScanDescs.contains(mvTableScanDesc)) {
                mvExtraTableScanDescLists.add(mvTableToTableScanDecs.get(mvTableScanDesc.getTable()));
            }
        }

        List<Set<TableScanDesc>> mvDistinctScanDescs = Lists.newArrayList();
        // `mvExtraTableScanDescLists` may generate duplicated tables, eg:
        // MV   : B(1) <-> A <-> D <-> C <-> E <-> A <-> B (2)
        // QUERY:  A <-> D <-> C <-> E <-> A
        // `mvExtraTableScanDescLists`: [B1, B2], [B1, B2]
        // `PermutationGenerator` will generate all permutations of input tables:
        // [B1, B1], [B1, B2], [B2, B1], [B2, B1].
        // In the next step, remove some redundant permutations below.
        PermutationGenerator generator = new PermutationGenerator(mvExtraTableScanDescLists);
        while (generator.hasNext()) {
            List<TableScanDesc> mvExtraTableScanDescs = generator.next();
            if (mvExtraTableScanDescs.stream().distinct().count() != mvExtraTableScanDescs.size()) {
                continue;
            }
            Set<TableScanDesc> mvExtraTableScanDescsSet = new HashSet<>(mvExtraTableScanDescs);
            if (mvDistinctScanDescs.contains(mvExtraTableScanDescsSet)) {
                continue;
            }
            mvDistinctScanDescs.add(mvExtraTableScanDescsSet);

            final Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns = ArrayListMultimap.create();
            final Map<Table, Set<Integer>> compensationRelations = Maps.newHashMap();
            final Map<Integer, Integer> expectedExtraQueryToMVRelationIds = Maps.newHashMap();
            if (!compensateViewDelta(mvTableScanDescs, mvExtraTableScanDescs,
                    compensationJoinColumns, compensationRelations, expectedExtraQueryToMVRelationIds,
                    materializationContext)) {
                logMVRewrite(mvRewriteContext, "Rewrite ViewDelta failed: cannot compensate query by using PK/FK constraints");
                continue;
            }

            OptExpression rewritten = rewriteComplete(queryTables, mvTables,
                    MatchMode.VIEW_DELTA, mvPredicateSplit, mvColumnRefRewriter,
                    compensationJoinColumns, compensationRelations, expectedExtraQueryToMVRelationIds);
            if (rewritten != null) {
                logMVRewrite(mvRewriteContext, "ViewDelta " + REWRITE_SUCCESS);
                Tracers.record(Tracers.Module.MV, mvRewriteContext.getMaterializationContext().getMv().getName(),
                        "ViewDelta " + REWRITE_SUCCESS);
                return rewritten;
            }
        }
        return null;
    }

    private OptExpression rewriteComplete(List<Table> queryTables,
                                          List<Table> mvTables,
                                          MatchMode matchMode,
                                          PredicateSplit mvPredicateSplit,
                                          ReplaceColumnRefRewriter mvColumnRefRewriter,
                                          Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns,
                                          Map<Table, Set<Integer>> compensationRelations,
                                          Map<Integer, Integer> expectedExtraQueryToMVRelationIds) {
        final OptExpression queryExpression = mvRewriteContext.getQueryExpression();
        final OptExpression mvExpression = materializationContext.getMvExpression();
        final ScalarOperator mvEqualPredicate = mvPredicateSplit.getEqualPredicates();
        final Map<Integer, Map<String, ColumnRefOperator>> queryRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getQueryRefFactory());
        final Map<Integer, Map<String, ColumnRefOperator>> mvRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getMvColumnRefFactory());
        // for query: A1 join A2 join B, mv: A1 join A2 join B
        // there may be two mapping:
        //    1. A1 -> A1, A2 -> A2, B -> B
        //    2. A1 -> A2, A2 -> A1, B -> B
        List<BiMap<Integer, Integer>> relationIdMappings = generateRelationIdMap(
                materializationContext.getQueryRefFactory(),
                queryTables, queryExpression, materializationContext.getMvColumnRefFactory(),
                mvTables, mvExpression, matchMode, compensationRelations, expectedExtraQueryToMVRelationIds);
        if (relationIdMappings.isEmpty()) {
            logMVRewrite(mvRewriteContext, "Rewrite complete failed: relation id mapping is empty");
            return null;
        }

        // used to judge whether query scalar ops can be rewritten
        final List<ColumnRefOperator> scanMvOutputColumns =
                materializationContext.getScanMvOperator().getOutputColumns();
        final Set<ColumnRefOperator> queryColumnSet = queryRelationIdToColumns.values()
                .stream().flatMap(x -> x.values().stream())
                .filter(columnRef -> !scanMvOutputColumns.contains(columnRef))
                .collect(Collectors.toSet());

        final PredicateSplit queryPredicateSplit = mvRewriteContext.getQueryPredicateSplit();
        final EquivalenceClasses queryEc = createEquivalenceClasses(
                queryPredicateSplit.getEqualPredicates(), queryExpression, mvRewriteContext.getQueryColumnRefRewriter(), null);
        if (queryEc == null) {
            logMVRewrite(mvRewriteContext, "Cannot construct query equivalence classes");
            return null;
        }
        // deduce extra equivalence classes if possible
        List<ScalarOperator> rangePredicates = Utils.extractConjuncts(queryPredicateSplit.getRangePredicates());
        deduceEquivalenceClassesFromRangePredicates(rangePredicates, queryEc, true);

        final RewriteContext rewriteContext = new RewriteContext(
                queryExpression, queryPredicateSplit, queryEc,
                queryRelationIdToColumns, materializationContext.getQueryRefFactory(),
                mvRewriteContext.getQueryColumnRefRewriter(), mvExpression, mvPredicateSplit, mvRelationIdToColumns,
                materializationContext.getMvColumnRefFactory(), mvColumnRefRewriter,
                materializationContext.getOutputMapping(), queryColumnSet);

        // collect partition and distribution related predicates in mv
        // used to prune partition and buckets after mv rewrite
        ScalarOperator mvPrunePredicate = collectMvPrunePredicate(materializationContext);

        logMVRewrite(mvRewriteContext, "MV predicate split:{}", mvPredicateSplit);
        logMVRewrite(mvRewriteContext, "Query predicate split:{}", queryPredicateSplit);
        logMVRewrite(mvRewriteContext, "Construct {} relation id mappings from query to mv", relationIdMappings.size());
        for (BiMap<Integer, Integer> relationIdMapping : relationIdMappings) {
            mvRewriteContext.setMvPruneConjunct(mvPrunePredicate);
            rewriteContext.setQueryToMvRelationIdMapping(relationIdMapping);

            final ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
            // for view delta, should add compensation join columns to query ec
            if (matchMode == MatchMode.VIEW_DELTA) {
                final EquivalenceClasses newQueryEc = queryEc.clone();
                // convert mv-based compensation join columns into query based after we get relationId mapping
                if (!addCompensationJoinColumnsIntoEquivalenceClasses(columnRewriter, newQueryEc,
                        compensationJoinColumns)) {
                    logMVRewrite(mvRewriteContext, "Cannot add compensation join columns into " +
                            "equivalence classes");
                    return null;
                }
                rewriteContext.setQueryEquivalenceClasses(newQueryEc);
            }

            // construct query based view EC
            final EquivalenceClasses queryBasedViewEqualPredicate =
                    createEquivalenceClasses(mvEqualPredicate, mvExpression, mvColumnRefRewriter, columnRewriter);
            if (queryBasedViewEqualPredicate == null) {
                logMVRewrite(mvRewriteContext, "Cannot construct query based mv equivalence classes");
                return null;
            }
            for (JoinDeriveContext deriveContext : mvRewriteContext.getJoinDeriveContexts()) {
                for (Pair<ColumnRefOperator, ColumnRefOperator> pair : deriveContext.getCompensatedEquivalenceColumns()) {
                    ScalarOperator first = mvRewriteContext.getQueryColumnRefRewriter().rewrite(pair.first);
                    ScalarOperator second = mvRewriteContext.getQueryColumnRefRewriter().rewrite(pair.second);
                    // now only support this pattern ec
                    if (first.isColumnRef() && second.isColumnRef()) {
                        queryBasedViewEqualPredicate.addEquivalence(first.cast(), second.cast());
                    }
                }
            }
            rewriteContext.setQueryBasedViewEquivalenceClasses(queryBasedViewEqualPredicate);

            OptExpression rewrittenExpression = tryRewriteForRelationMapping(rewriteContext);
            if (rewrittenExpression != null) {
                // copy limit into rewritten plan
                // limit will affect the statistics of rewritten plan
                if (rewriteContext.getQueryExpression().getOp().hasLimit()) {
                    rewrittenExpression.getOp().setLimit(rewriteContext.getQueryExpression().getOp().getLimit());
                }
                logMVRewrite(mvRewriteContext, REWRITE_SUCCESS);
                Tracers.record(Tracers.Module.MV, mvRewriteContext.getMaterializationContext().getMv().getName(),
                        REWRITE_SUCCESS);
                return rewrittenExpression;
            }
        }
        return null;
    }

    // check whether query can be rewritten by view even though the view has additional tables.
    // In order to do that, we should make sure that the additional joins(which exists in view
    // but not in query) are lossless join.
    // for inner join:
    //      1. join on all key columns
    //      2. one side is foreign key and the other side is unique/primary key
    //      3. foreign key columns are not null
    // for outer join:
    //      1. left outer join
    //      2. join keys in additional table should be unique/primary key
    //
    // return true if it can be rewritten. Further, it will add the missing equal-join predicates to the
    // compensationJoinColumns and compensation table into compensationRelations.
    // return false if it can not be rewritten.
    private boolean compensateViewDelta(
            List<TableScanDesc> mvTableScanDescs,
            List<TableScanDesc> mvExtraTableScanDescs,
            Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns,
            Map<Table, Set<Integer>> compensationRelations,
            Map<Integer, Integer> expectedExtraQueryToMVRelationIds,
            MaterializationContext materializationContext) {
        MaterializedView materializedView = materializationContext.getMv();
        ColumnRefFactory queryRefFactory = materializationContext.getQueryRefFactory();
        ColumnRefFactory mvRefFactory = materializationContext.getMvColumnRefFactory();

        // use directed graph to construct foreign key join graph
        MutableGraph<TableScanDesc> mvGraph = GraphBuilder.directed().build();
        Map<TableScanDesc, List<ColumnRefOperator>> extraTableColumns = Maps.newHashMap();
        Multimap<String, TableScanDesc> mvNameToTable = ArrayListMultimap.create();
        for (TableScanDesc mvTableScanDesc : mvTableScanDescs) {
            mvGraph.addNode(mvTableScanDesc);
            mvNameToTable.put(mvTableScanDesc.getName(), mvTableScanDesc);
        }

        // add edges to directed graph by FK-UK
        for (TableScanDesc mvTableScanDesc : mvGraph.nodes()) {
            Table mvChildTable = mvTableScanDesc.getTable();
            List<ForeignKeyConstraint> foreignKeyConstraints = mvChildTable.getForeignKeyConstraints();
            List<ForeignKeyConstraint> mvForeignKeyConstraints = Lists.newArrayList();
            if (materializedView.getForeignKeyConstraints() != null) {
                // add ForeignKeyConstraint from mv
                materializedView.getForeignKeyConstraints().stream().filter(foreignKeyConstraint -> {
                    if (foreignKeyConstraint.getChildTableInfo() == null) {
                        return false;
                    }
                    Table table = foreignKeyConstraint.getChildTableInfo().getTableChecked();
                    return table.equals(mvChildTable);
                }).forEach(mvForeignKeyConstraints::add);
            }

            if (foreignKeyConstraints == null) {
                foreignKeyConstraints = mvForeignKeyConstraints;
            } else if (materializedView.getForeignKeyConstraints() != null) {
                foreignKeyConstraints.addAll(mvForeignKeyConstraints);
            }
            if (foreignKeyConstraints.isEmpty()) {
                continue;
            }

            for (ForeignKeyConstraint foreignKeyConstraint : foreignKeyConstraints) {
                Collection<TableScanDesc> mvParentTableScanDescs =
                        mvNameToTable.get(foreignKeyConstraint.getParentTableInfo().getTableName());
                if (mvParentTableScanDescs == null || mvParentTableScanDescs.isEmpty()) {
                    continue;
                }
                List<Pair<String, String>> columnPairs = foreignKeyConstraint.getColumnRefPairs();
                List<String> childKeys = columnPairs.stream().map(pair -> pair.first)
                        .map(String::toLowerCase).collect(Collectors.toList());
                List<String> parentKeys = columnPairs.stream().map(pair -> pair.second)
                        .map(String::toLowerCase).collect(Collectors.toList());

                Table foreignKeyParentTable = foreignKeyConstraint.getParentTableInfo().getTableChecked();
                for (TableScanDesc mvParentTableScanDesc : mvParentTableScanDescs) {
                    Table parentTable = mvParentTableScanDesc.getTable();
                    // check the parent table is the same table in the foreign key constraint
                    if (!parentTable.equals(foreignKeyParentTable)) {
                        continue;
                    }

                    Multimap<ColumnRefOperator, ColumnRefOperator> constraintCompensationJoinColumns
                            = ArrayListMultimap.create();
                    Optional<OptExpression> joinOperatorOptional = findJoin(mvParentTableScanDesc,
                            mvTableScanDesc, columnPairs, materializationContext.getMvExpression());
                    if (!joinOperatorOptional.isPresent()) {
                        continue;
                    }
                    if (!extraJoinCheck(mvParentTableScanDesc, mvTableScanDesc, columnPairs, childKeys,
                            parentKeys, constraintCompensationJoinColumns, materializedView,
                            materializationContext.getMvExpression(), joinOperatorOptional.get())) {
                        continue;
                    }

                    // If `mvParentTableScanDesc` is not included in query's plan, add it
                    // to extraColumns.
                    LogicalJoinOperator joinOperator = joinOperatorOptional.get().getOp().cast();
                    JoinOperator joinType = joinOperator.getJoinType();
                    if (mvExtraTableScanDescs.contains(mvParentTableScanDesc)) {
                        if (joinType.isInnerJoin() || joinType.isCrossJoin() || joinType.isSemiJoin()) {
                            compensationJoinColumns.putAll(constraintCompensationJoinColumns);
                        }
                        List<ColumnRefOperator> parentTableCompensationColumns =
                                constraintCompensationJoinColumns.keys().stream().collect(Collectors.toList());
                        extraTableColumns.computeIfAbsent(mvParentTableScanDesc, x -> Lists.newArrayList())
                                .addAll(parentTableCompensationColumns);
                    }
                    if (mvExtraTableScanDescs.contains(mvTableScanDesc)) {
                        if (joinType.isInnerJoin() || joinType.isCrossJoin() || joinType.isSemiJoin()) {
                            compensationJoinColumns.putAll(constraintCompensationJoinColumns);
                        }
                        List<ColumnRefOperator> childTableCompensationColumns =
                                constraintCompensationJoinColumns.values().stream().collect(Collectors.toList());
                        extraTableColumns.computeIfAbsent(mvTableScanDesc, k -> Lists.newArrayList())
                                .addAll(childTableCompensationColumns);
                    }

                    // Add an edge (mvTableScanDesc -> mvParentTableScanDesc) into graph
                    if (!mvGraph.successors(mvTableScanDesc).contains(mvParentTableScanDesc)) {
                        mvGraph.putEdge(mvTableScanDesc, mvParentTableScanDesc);
                    }
                }
            }
        }
        if (!graphBasedCheck(mvGraph, mvExtraTableScanDescs)) {
            return false;
        }

        // should add new tables/columnRefs into query ColumnRefFactory if pass test
        // should collect additional tables and the related columns
        getCompensationRelations(extraTableColumns, queryRefFactory, mvRefFactory,
                compensationRelations, expectedExtraQueryToMVRelationIds);
        return true;
    }

    private boolean hasForeignKeyConstraintInMv(Table childTable, MaterializedView materializedView,
                                                List<String> childKeys) {
        if (materializedView.getForeignKeyConstraints() == null) {
            return false;
        }
        Set<String> childKeySet = Sets.newHashSet(childKeys);

        for (ForeignKeyConstraint foreignKeyConstraint : materializedView.getForeignKeyConstraints()) {
            if (foreignKeyConstraint.getChildTableInfo() != null &&
                    foreignKeyConstraint.getChildTableInfo().getTableChecked().equals(childTable)) {
                List<Pair<String, String>> columnPairs = foreignKeyConstraint.getColumnRefPairs();
                Set<String> mvChildKeySet = columnPairs.stream().map(pair -> pair.first)
                        .map(String::toLowerCase).collect(Collectors.toSet());
                if (childKeySet.equals(mvChildKeySet)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean extraJoinCheck(
            TableScanDesc parentTableScanDesc, TableScanDesc tableScanDesc,
            List<Pair<String, String>> columnPairs, List<String> childKeys, List<String> parentKeys,
            Multimap<ColumnRefOperator, ColumnRefOperator> constraintCompensationJoinColumns,
            MaterializedView materializedView, OptExpression mvExpression, OptExpression extraJoin) {
        Table parentTable = parentTableScanDesc.getTable();
        Table childTable = tableScanDesc.getTable();
        LogicalJoinOperator joinOperator = extraJoin.getOp().cast();
        JoinOperator parentJoinType = joinOperator.getJoinType();
        if (parentJoinType.isInnerJoin()) {
            // to check:
            // 1. childKeys should be foreign key
            // 2. childKeys should be not null
            // 3. parentKeys should be unique
            if (!isUniqueKeys(materializedView, parentTable, parentKeys)) {
                logMVRewrite(mvRewriteContext, "FKs {} are not unique keys of parent table {} for inner join",
                        parentKeys, parentTable.getName());
                return false;
            }
            // foreign keys are not null
            // if child table has foreign key constraint in mv, we assume that the foreign key is not null
            if (childKeys.stream().anyMatch(column -> childTable.getColumn(column).isAllowNull()) &&
                    !hasForeignKeyConstraintInMv(childTable, materializedView, childKeys)) {
                logMVRewrite(mvRewriteContext, "FKs {} are not totally not-null in child table {}",
                        childKeys, childTable.getName());
                return false;
            }
        } else if (parentJoinType.isLeftOuterJoin()) {
            // make sure that all join keys are in foreign keys
            // the join keys of parent table should be unique
            if (!isUniqueKeys(materializedView, parentTable, parentKeys)) {
                logMVRewrite(mvRewriteContext, "FKs {} are not unique keys of parent table {} for left join",
                        parentKeys, parentTable.getName());
                return false;
            }
        } else {
            // other joins are not supported to rewrite
            return false;
        }
        // foreign keys should be join keys,
        // it means that there should be a join between childTable and parentTable
        for (Pair<String, String> pair : columnPairs) {
            ColumnRefOperator childColumn = getColumnRef(pair.first, tableScanDesc.getScanOperator());
            ColumnRefOperator parentColumn = getColumnRef(pair.second, parentTableScanDesc.getScanOperator());
            if (childColumn == null || parentColumn == null) {
                logMVRewrite(mvRewriteContext, "child column {}/parent column {} cannot be found " +
                        "in scan operator, is_child_found:{}, is_parent_found:{}", childColumn, parentColumn, 
                        childColumn == null, parentColumn == null);
                return false;
            }
            constraintCompensationJoinColumns.put(parentColumn, childColumn);
        }
        return true;
    }

    private Optional<OptExpression> findJoin(
            TableScanDesc parentTableScanDesc, TableScanDesc tableScanDesc,
            List<Pair<String, String>> columnPairs, OptExpression mvExpression) {
        List<OptExpression> joins = MvUtils.collectJoinExpr(mvExpression);
        for (OptExpression joinOptExpr : joins) {
            boolean allMatched = true;
            for (Pair<String, String> pair : columnPairs) {
                ColumnRefOperator childColumn = getColumnRef(pair.first, tableScanDesc.getScanOperator());
                ColumnRefOperator parentColumn = getColumnRef(pair.second, parentTableScanDesc.getScanOperator());
                if (childColumn == null || parentColumn == null) {
                    return Optional.empty();
                }
                if (!isJoinOnCondition(joinOptExpr.getOp().cast(), childColumn, parentColumn)) {
                    allMatched = false;
                    break;
                }
            }
            if (allMatched) {
                return Optional.of(joinOptExpr);
            }
        }
        return Optional.empty();
    }

    private boolean isJoinOnCondition(
            LogicalJoinOperator joinOperator,
            ColumnRefOperator childColumn,
            ColumnRefOperator parentColumn) {
        List<ScalarOperator> onConjuncts = Utils.extractConjuncts(joinOperator.getOnPredicate());
        List<ScalarOperator> binaryConjuncts =
                onConjuncts.stream().filter(ScalarOperator::isColumnEqualBinaryPredicate).collect(Collectors.toList());
        ColumnRefSet columns = new ColumnRefSet(Lists.newArrayList(childColumn, parentColumn));
        return binaryConjuncts.stream().anyMatch(conjunct -> conjunct.getUsedColumns().containsAll(columns));
    }

    private boolean graphBasedCheck(MutableGraph<TableScanDesc> graph,
                                    List<TableScanDesc> extraTableScanDescs) {
        // remove one in-degree and zero out-degree's node from graph repeatedly
        boolean done;
        do {
            List<TableScanDesc> nodesToRemove = Lists.newArrayList();
            for (TableScanDesc node : graph.nodes()) {
                if (graph.predecessors(node).size() == 1 && graph.successors(node).size() == 0) {
                    nodesToRemove.add(node);
                }
            }
            done = nodesToRemove.isEmpty();
            nodesToRemove.stream().forEach(node -> graph.removeNode(node));
        } while (!done);

        // If all nodes left after removing the preprocessor's size is 1 and successor's size is zero are
        // disjoint with `extraTables`, means `query` can be view-delta compensated.
        return Collections.disjoint(graph.nodes(), extraTableScanDescs);
    }

    private void getCompensationRelations(Map<TableScanDesc, List<ColumnRefOperator>> extraTableColumns,
                                          ColumnRefFactory queryRefFactory,
                                          ColumnRefFactory mvRefFactory,
                                          Map<Table, Set<Integer>> compensationRelations,
                                          Map<Integer, Integer> expectedExtraQueryToMVRelationIds) {
        // Extra table should always link with MV's relation id.
        for (Map.Entry<TableScanDesc, List<ColumnRefOperator>> entry : extraTableColumns.entrySet()) {
            int relationId = queryRefFactory.getNextRelationId();
            List<ColumnRefOperator> columnRefOperators = entry.getValue();
            int mvRelationId = -1;
            for (ColumnRefOperator columnRef : columnRefOperators) {
                if (mvRelationId == -1) {
                    mvRelationId = mvRefFactory.getRelationId(columnRef.getId());
                }

                Table table = entry.getKey().getTable();
                Column column = table.getColumn(columnRef.getName());
                ColumnRefOperator newColumn =
                        queryRefFactory.create(columnRef.getName(), columnRef.getType(), columnRef.isNullable());
                queryRefFactory.updateColumnToRelationIds(newColumn.getId(), relationId);
                queryRefFactory.updateColumnRefToColumns(newColumn, column, table);
            }
            Set<Integer> relationIds =
                    compensationRelations.computeIfAbsent(entry.getKey().getTable(), table -> Sets.newHashSet());
            relationIds.add(relationId);
            expectedExtraQueryToMVRelationIds.put(relationId, mvRelationId);
        }
    }

    private boolean isUniqueKeys(MaterializedView materializedView, Table table, List<String> lowerCasekeys) {
        List<UniqueConstraint> mvUniqueConstraints = Lists.newArrayList();
        if (materializedView.hasUniqueConstraints()) {
            mvUniqueConstraints = materializedView.getUniqueConstraints().stream().filter(
                           uniqueConstraint -> StringUtils.areTableNamesEqual(table, uniqueConstraint.getTableName()))
                   .collect(Collectors.toList());
        }

        KeysType tableKeyType = KeysType.DUP_KEYS;
        Set<String> keySet = Sets.newHashSet(lowerCasekeys);
        if (table.isNativeTableOrMaterializedView()) {
            OlapTable olapTable = (OlapTable) table;
            tableKeyType = olapTable.getKeysType();
            if (tableKeyType == KeysType.PRIMARY_KEYS || tableKeyType == KeysType.UNIQUE_KEYS) {
                return olapTable.isKeySet(keySet);
            }
        }
        if (tableKeyType == KeysType.DUP_KEYS) {
            List<UniqueConstraint> uniqueConstraints = Lists.newArrayList();
            uniqueConstraints.addAll(ListUtils.emptyIfNull(table.getUniqueConstraints()));
            uniqueConstraints.addAll(ListUtils.emptyIfNull(mvUniqueConstraints));
            for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
                if (uniqueConstraint.isMatch(table, keySet)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    private ColumnRefOperator getColumnRef(String columnName, LogicalScanOperator scanOperator) {
        Optional<ColumnRefOperator> columnRef = scanOperator.getColumnMetaToColRefMap().values()
                .stream().filter(col -> col.getName().equalsIgnoreCase(columnName)).findFirst();
        return columnRef.isPresent() ? columnRef.get() : null;
    }

    private ScalarOperator collectMvPrunePredicate(MaterializationContext mvContext) {
        final OptExpression mvExpression = mvContext.getMvExpression();
        final Set<ScalarOperator> conjuncts = MvUtils.getAllValidPredicates(mvExpression);
        final ColumnRefSet mvOutputColumnRefSet = mvExpression.getOutputColumns();
        // conjuncts related to partition and distribution
        final List<ScalarOperator> mvPrunePredicates = Lists.newArrayList();

        // Construct partition/distribution key column refs to filter conjunctions which need to retain.
        Set<String> mvPruneKeyColNames = Sets.newHashSet();
        MaterializedView mv = mvContext.getMv();
        DistributionInfo distributionInfo = mv.getDefaultDistributionInfo();
        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            List<Column> distributedColumns = hashDistributionInfo.getDistributionColumns();
            distributedColumns.stream().forEach(distKey -> mvPruneKeyColNames.add(distKey.getName()));
        }

        mv.getPartitionColumnNames().stream().forEach(partName -> mvPruneKeyColNames.add(partName));
        final Set<Integer> mvPruneColumnIdSet = mvOutputColumnRefSet.getStream().map(
                        id -> mvContext.getMvColumnRefFactory().getColumnRef(id))
                .filter(colRef -> mvPruneKeyColNames.contains(colRef.getName()))
                .map(colRef -> colRef.getId())
                .collect(Collectors.toSet());
        for (ScalarOperator conj : conjuncts) {
            // ignore binary predicates which cannot be used for pruning.
            if (conj instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator conjOp = (BinaryPredicateOperator) conj;
                if (conjOp.getChild(0).isVariable() && conjOp.getChild(1).isVariable()) {
                    continue;
                }
            }
            final List<Integer> conjColumnRefOperators =
                    Utils.extractColumnRef(conj).stream().map(ref -> ref.getId()).collect(Collectors.toList());
            if (mvPruneColumnIdSet.containsAll(conjColumnRefOperators)) {
                mvPrunePredicates.add(conj);
            }
        }

        return Utils.compoundAnd(mvPrunePredicates);
    }

    private OptExpression buildMVScanOptExpression(RewriteContext rewriteContext,
                                                   ColumnRewriter columnRewriter,
                                                   Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp) {
        // the rewritten expression to replace query
        // should copy the op because the op will be modified and reused
        final LogicalOlapScanOperator mvScanOperator = materializationContext.getScanMvOperator();
        final Operator.Builder mvScanBuilder = OperatorBuilderFactory.build(mvScanOperator);
        mvScanBuilder.withOperator(mvScanOperator);

        // Rewrite original mv's predicates into query if needed.
        if (mvRewriteContext.getMvPruneConjunct() != null && !mvRewriteContext.getMvPruneConjunct().isTrue()) {
            ScalarOperator rewrittenPrunePredicate = rewriteMVCompensationExpression(rewriteContext, columnRewriter,
                    mvColumnRefToScalarOp, mvRewriteContext.getMvPruneConjunct(), false);
            mvRewriteContext.setMvPruneConjunct(MvUtils.canonizePredicate(rewrittenPrunePredicate));
        }
        OptExpression mvScanOptExpression = OptExpression.create(mvScanBuilder.build());
        deriveLogicalProperty(mvScanOptExpression);
        return mvScanOptExpression;
    }

    private OptExpression tryRewriteForRelationMapping(RewriteContext rewriteContext) {
        final ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        final Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp = rewriteContext.getMVColumnRefToScalarOp();
        OptExpression mvScanOptExpression = buildMVScanOptExpression(rewriteContext, columnRewriter, mvColumnRefToScalarOp);

        final PredicateSplit compensationPredicates = getCompensationPredicates(columnRewriter,
                rewriteContext.getQueryEquivalenceClasses(),
                rewriteContext.getQueryBasedViewEquivalenceClasses(),
                rewriteContext.getQueryPredicateSplit(),
                rewriteContext.getMvPredicateSplit(),
                true);
        if (compensationPredicates == null) {
            logMVRewrite(mvRewriteContext, "Cannot compensate predicates from mv rewrite, " +
                    "try to use union rewrite.");
            if (!optimizerContext.getSessionVariable().isEnableMaterializedViewUnionRewrite()) {
                return null;
            }
            List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(rewriteContext.getMvExpression());
            if (scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
                // TODO: support union rewrite for view based mv rewrite
                return null;
            }
            return tryUnionRewrite(rewriteContext, mvScanOptExpression);
        } else {
            // all predicates are now query based
            final ScalarOperator equalPredicates = MvUtils.canonizePredicate(compensationPredicates.getEqualPredicates());
            final ScalarOperator otherPredicates = MvUtils.canonizePredicate(Utils.compoundAnd(
                    compensationPredicates.getRangePredicates(), compensationPredicates.getResidualPredicates()));
            ScalarOperator compensationPredicate = getMVCompensationPredicate(rewriteContext,
                    columnRewriter, mvColumnRefToScalarOp, equalPredicates, otherPredicates);
            if (compensationPredicate == null) {
                logMVRewrite(mvRewriteContext, "Success to convert query compensation predicates to MV " +
                        "but rewrite compensation failed");
                return null;
            }

            if (!compensationPredicate.isTrue()) {
                // NOTE: Keep mv's original predicates which have been already rewritten.
                ScalarOperator finalCompensationPredicate = compensationPredicate;
                if (mvScanOptExpression.getOp().getPredicate() != null) {
                    finalCompensationPredicate = Utils.compoundAnd(finalCompensationPredicate,
                            mvScanOptExpression.getOp().getPredicate());
                }
                final Operator.Builder newScanOpBuilder = OperatorBuilderFactory.build(mvScanOptExpression.getOp());
                newScanOpBuilder.withOperator(mvScanOptExpression.getOp());

                ScalarOperator normalizedPredicate = normalizePredicate(finalCompensationPredicate);
                // Canonize predicates to make uts more stable.
                if (optimizerContext.getSessionVariable().getQueryDebugOptions().isEnableNormalizePredicateAfterMVRewrite()) {
                    normalizedPredicate = MvUtils.canonizePredicateForRewrite(queryMaterializationContext, normalizedPredicate);
                }
                newScanOpBuilder.setPredicate(normalizedPredicate);

                mvScanOptExpression = OptExpression.create(newScanOpBuilder.build());
                mvScanOptExpression.setLogicalProperty(null);
                deriveLogicalProperty(mvScanOptExpression);
            }

            // add projection
            return viewBasedRewrite(rewriteContext, mvScanOptExpression);
        }
    }

    private ScalarOperator normalizePredicate(ScalarOperator predicate) {
        ScalarOperator pruneFinalCompensationPredicate =
                MvNormalizePredicateRule.pruneRedundantPredicates(predicate);
        PredicateSplit split = PredicateSplit.splitPredicate(pruneFinalCompensationPredicate);
        return Utils.compoundAnd(split.getEqualPredicates(), split.getRangePredicates(), split.getResidualPredicates());
    }

    private ScalarOperator getMVCompensationPredicate(RewriteContext rewriteContext,
                                                      ColumnRewriter rewriter,
                                                      Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp,
                                                      ScalarOperator equalPredicates,
                                                      ScalarOperator otherPredicates) {
        ScalarOperator newEqualPredicates = ConstantOperator.TRUE;
        if (!ConstantOperator.TRUE.equals(equalPredicates)) {
            newEqualPredicates = rewriteMVCompensationExpression(rewriteContext, rewriter,
                    mvColumnRefToScalarOp, equalPredicates, true);
            if (newEqualPredicates == null) {
                logMVRewrite(mvRewriteContext, "Rewrite equal predicates compensation failed: {}", equalPredicates);
                return null;
            }
        }

        ScalarOperator newOtherPredicates = ConstantOperator.TRUE;
        if (!ConstantOperator.TRUE.equals(otherPredicates)) {
            newOtherPredicates = rewriteMVCompensationExpression(rewriteContext, rewriter,
                    mvColumnRefToScalarOp, otherPredicates, false);
            if (newOtherPredicates == null) {
                logMVRewrite(mvRewriteContext, "Rewrite other predicates compensation failed: {}", otherPredicates);
                return null;
            }
        }

        ScalarOperator compensationPredicate = MvUtils.canonizePredicate(Utils.compoundAnd(newEqualPredicates,
                newOtherPredicates));
        if (compensationPredicate == null) {
            logMVRewrite(mvRewriteContext, "Canonize compensate predicates failed: {} , {}",
                    equalPredicates, otherPredicates);
            return null;
        }
        return addJoinDerivePredicate(rewriteContext, rewriter, mvColumnRefToScalarOp, compensationPredicate);
    }

    private ScalarOperator addJoinDerivePredicate(
            RewriteContext rewriteContext,
            ColumnRewriter rewriter,
            Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp,
            ScalarOperator compensationPredicate) {
        // unnecessary to add the derived compensation predicates if compensationPredicate has related null rejecting predicate
        List<ScalarOperator> predicates = Utils.extractConjuncts(compensationPredicate);
        List<ScalarOperator> derivedPredicates = Lists.newArrayList();
        for (JoinDeriveContext joinDeriveContext : mvRewriteContext.getJoinDeriveContexts()) {
            JoinOperator mvJoinOp = joinDeriveContext.getMvJoinType();
            JoinOperator queryJoinOp = joinDeriveContext.getQueryJoinType();

            if (mvJoinOp.isInnerJoin() && queryJoinOp.isSemiJoin()) {
                continue;
            }

            Optional<ScalarOperator> derivedPredicateOpt = Optional.empty();
            if (mvJoinOp.isLeftOuterJoin() && queryJoinOp.isInnerJoin()) {
                List<ColumnRefOperator> rightJoinColumns = joinDeriveContext.getRightJoinColumns();
                derivedPredicateOpt = getDerivedPredicate(rewriteContext, rewriter,
                        mvColumnRefToScalarOp, rightJoinColumns, joinDeriveContext.getRightChildOutputColumns(),
                        predicates, true, true, false);
            } else if (mvJoinOp.isLeftOuterJoin() && queryJoinOp.isLeftAntiJoin()) {
                List<ColumnRefOperator> rightJoinColumns = joinDeriveContext.getRightJoinColumns();
                derivedPredicateOpt = getDerivedPredicate(rewriteContext, rewriter,
                        mvColumnRefToScalarOp, rightJoinColumns, joinDeriveContext.getRightChildOutputColumns(),
                        predicates, false, true, false);
            } else if (mvJoinOp.isRightOuterJoin() && queryJoinOp.isInnerJoin()) {
                List<ColumnRefOperator> leftJoinColumns = joinDeriveContext.getLeftJoinColumns();
                derivedPredicateOpt = getDerivedPredicate(rewriteContext, rewriter,
                        mvColumnRefToScalarOp, leftJoinColumns, joinDeriveContext.getLeftChildOutputColumns(),
                        predicates, true, true, false);
            } else if (mvJoinOp.isRightOuterJoin()
                    && queryJoinOp.isRightAntiJoin()) {
                List<ColumnRefOperator> leftJoinColumns = joinDeriveContext.getLeftJoinColumns();
                derivedPredicateOpt = getDerivedPredicate(rewriteContext, rewriter,
                        mvColumnRefToScalarOp, leftJoinColumns, joinDeriveContext.getLeftChildOutputColumns(),
                        predicates, false, true, false);
            } else if (mvJoinOp.isFullOuterJoin() && queryJoinOp.isLeftOuterJoin()) {
                List<ColumnRefOperator> leftJoinColumns = joinDeriveContext.getLeftJoinColumns();
                derivedPredicateOpt = getDerivedPredicate(rewriteContext, rewriter,
                        mvColumnRefToScalarOp, leftJoinColumns, joinDeriveContext.getLeftChildOutputColumns(),
                        predicates, true, false, true);
            } else if (mvJoinOp.isFullOuterJoin() && queryJoinOp.isRightOuterJoin()) {
                List<ColumnRefOperator> rightJoinColumns = joinDeriveContext.getRightJoinColumns();
                derivedPredicateOpt = getDerivedPredicate(rewriteContext, rewriter,
                        mvColumnRefToScalarOp, rightJoinColumns, joinDeriveContext.getRightChildOutputColumns(),
                        predicates, true, false, true);
            } else if (mvJoinOp.isFullOuterJoin() && queryJoinOp.isInnerJoin()) {
                List<ColumnRefOperator> rightJoinColumns = joinDeriveContext.getRightJoinColumns();
                derivedPredicateOpt = getDerivedPredicate(rewriteContext, rewriter,
                        mvColumnRefToScalarOp, rightJoinColumns, joinDeriveContext.getRightChildOutputColumns(),
                        predicates, true, false, true);
                if (!derivedPredicateOpt.isPresent()) {
                    // can not get derived predicates
                    logMVRewrite(mvRewriteContext, "derive join's extra predicate failed, mv join type: {}, " +
                            "query join type: {}", mvJoinOp, queryJoinOp);
                    return null;
                }
                ScalarOperator derivedPredicate = derivedPredicateOpt.get();
                if (!derivedPredicate.equals(ConstantOperator.TRUE)) {
                    derivedPredicates.add(derivedPredicate);
                }

                List<ColumnRefOperator> leftJoinColumns = joinDeriveContext.getLeftJoinColumns();
                derivedPredicateOpt = getDerivedPredicate(rewriteContext, rewriter,
                        mvColumnRefToScalarOp, leftJoinColumns, joinDeriveContext.getLeftChildOutputColumns(),
                        predicates, true, false, true);
            }
            if (!derivedPredicateOpt.isPresent()) {
                // can not get derived predicates
                logMVRewrite(mvRewriteContext, "derive join's extra predicate failed, mv join type: {}, " +
                        "query join type: {}", mvJoinOp, queryJoinOp);
                return null;
            }
            ScalarOperator derivedPredicate = derivedPredicateOpt.get();
            if (!derivedPredicate.equals(ConstantOperator.TRUE)) {
                derivedPredicates.add(derivedPredicate);
            }
        }
        return Utils.compoundAnd(compensationPredicate, Utils.compoundAnd(derivedPredicates));
    }

    private Optional<ScalarOperator> getDerivedPredicate(
            RewriteContext rewriteContext,
            ColumnRewriter rewriter,
            Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp,
            List<ColumnRefOperator> joinColumns,
            List<ColumnRefOperator> outputColumns,
            List<ScalarOperator> compensationPredicates,
            boolean isNotNull, boolean onlyJoinColumns, boolean onlyNotNullColumns) {
        // output column to target mv expr
        Map<ColumnRefOperator, ScalarOperator> compensatedColumnsInMv = Maps.newHashMap();
        // output column to query based expr
        Map<ColumnRefOperator, ScalarOperator> outputExprMap = Maps.newHashMap();
        Set<ColumnRefOperator> relatedColumns = Sets.newHashSet();
        // outputColumns are mv based, should be rewritten by MvColumnRefRewriter first to base tables
        // and then rewritten to mv to construct the derived predicates
        for (ColumnRefOperator outputColumn : outputColumns) {
            ScalarOperator rewrittenColumnRef = rewriteContext.getMvColumnRefRewriter().rewrite(outputColumn);
            rewrittenColumnRef = rewriter.rewriteViewToQuery(rewrittenColumnRef);
            ScalarOperator targetExpr = rewriteMVCompensationExpression(rewriteContext, rewriter,
                    mvColumnRefToScalarOp, rewrittenColumnRef, false, false);
            if (targetExpr != null) {
                relatedColumns.addAll(targetExpr.getColumnRefs());
                compensatedColumnsInMv.put(outputColumn, targetExpr);
                outputExprMap.put(outputColumn, rewrittenColumnRef);
            }
        }

        List<ScalarOperator> relatedPredicates = compensationPredicates.stream().filter(
                predicate -> predicate.getUsedColumns().containsAny(relatedColumns)).collect(Collectors.toList());
        if (needExtraNotNullDerive(relatedPredicates, relatedColumns)) {
            if (compensatedColumnsInMv.isEmpty()) {
                // there is no output of compensation table
                return Optional.empty();
            }
            final List<ColumnRefOperator> candidateColumns = Lists.newArrayList();
            if (onlyJoinColumns) {
                List<ScalarOperator> joinExprs = Lists.newArrayList();
                for (ColumnRefOperator joinColumn : joinColumns) {
                    joinExprs.add(rewriteContext.getQueryColumnRefRewriter().rewrite(joinColumn));
                }
                for (ColumnRefOperator outputColumn : compensatedColumnsInMv.keySet()) {
                    if (joinExprs.contains(outputExprMap.get(outputColumn))) {
                        candidateColumns.addAll(compensatedColumnsInMv.get(outputColumn).getColumnRefs());
                    }
                }
            } else {
                candidateColumns.addAll(relatedColumns);
            }
            if (onlyNotNullColumns) {
                for (ColumnRefOperator columnRef : compensatedColumnsInMv.keySet()) {
                    // remove nullable columns
                    if (columnRef.isNullable()) {
                        candidateColumns.remove(compensatedColumnsInMv.get(columnRef));
                    }
                }
            }
            if (candidateColumns.isEmpty()) {
                logMVRewrite(mvRewriteContext, "derive extra predicates from mv failed: candidate columns are empty. " +
                                "isNotNull: {}, onlyJoinColumns:{}, onlyNotNullColumns:{}, compensatedColumnsInMv:{}, " +
                                "joinColumns:{}", isNotNull, onlyJoinColumns,
                        onlyNotNullColumns, compensatedColumnsInMv, joinColumns);
                return Optional.empty();
            }
            IsNullPredicateOperator columnIsNotNull = new IsNullPredicateOperator(isNotNull, candidateColumns.get(0));
            return Optional.of(columnIsNotNull);
        } else {
            // there is null-rejecting predicate for compensated table, do not add any more predicate
            return Optional.of(ConstantOperator.TRUE);
        }
    }

    private boolean needExtraNotNullDerive(
            List<ScalarOperator> relatedPredicates, Collection<ColumnRefOperator> relatedColumnRefs) {
        if (relatedPredicates.isEmpty()) {
            return true;
        }

        if (relatedPredicates.stream().allMatch(relatedPredicate ->
                !Utils.canEliminateNull(Sets.newHashSet(relatedColumnRefs), relatedPredicate))) {
            return true;
        }

        return false;
    }

    private ScalarOperator rewriteMVCompensationExpression(RewriteContext rewriteContext,
                                                           ColumnRewriter rewriter,
                                                           Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp,
                                                           ScalarOperator predicate,
                                                           boolean isMVBased) {
        return rewriteMVCompensationExpression(rewriteContext, rewriter, mvColumnRefToScalarOp, predicate, isMVBased, true);
    }

    private ScalarOperator rewriteMVCompensationExpression(RewriteContext rewriteContext,
                                                           ColumnRewriter rewriter,
                                                           Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp,
                                                           ScalarOperator predicate,
                                                           boolean isMVBased, boolean useEc) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        // swapped by query based view ec
        List<ScalarOperator> rewrittenConjuncts = useEc ? conjuncts.stream()
                .map(conjunct -> rewriter.rewriteByEc(conjunct, isMVBased))
                .distinct() // equal-class scalar operators may generate the same rewritten conjunct.
                .collect(Collectors.toList()) : conjuncts;
        if (rewrittenConjuncts.isEmpty()) {
            return null;
        }

        EquationRewriter queryExprToMvExprRewriter =
                buildEquationRewriter(mvColumnRefToScalarOp, rewriteContext, isMVBased, true, useEc);
        List<ScalarOperator> candidates = rewriteScalarOpToTarget(rewrittenConjuncts, queryExprToMvExprRewriter,
                rewriteContext.getOutputMapping(), new ColumnRefSet(rewriteContext.getQueryColumnSet()), false, null);
        if (candidates == null || candidates.isEmpty()) {
            logMVRewrite(mvRewriteContext, "rewrite predicates from query to mv failed. isMVBased:{}, useEc:{}, predicate:{}",
                    isMVBased, useEc, predicate);
            return null;
        }
        return Utils.compoundAnd(candidates);
    }

    private boolean isPullUpQueryPredicate(ScalarOperator predicate,
                                           Set<ColumnRefOperator> mvPredicateUsedColRefs,
                                           ColumnRefSet queryOnPredicateUsedColRefs,
                                           ColumnRefSet queryOutputColumnRefs) {
        return predicate.getColumnRefs().stream()
                .filter(col -> {
                    //  pull-up extra predicates should not contain any non-(inner/cross) join on-predicates
                    if (queryOnPredicateUsedColRefs != null && queryOnPredicateUsedColRefs.contains(col)) {
                        return false;
                    }
                    return true;
                })
                .anyMatch(col -> {
                    // filter out query's predicates that its column refs are not contained in mv's predicate:
                    // when column ref is not contained in mv's predicates, query's predicate cannot be rewritten.
                    // eg:
                    //    query: select dt, k1, k2 from tblA where dt >= '2023-11-01' and k2 > 1
                    //      mv : select dt, k2 from tblA where dt = '2023-11-01'
                    // we can rewrite by Union-All after rewrite as below:
                    //    query: select * from ( select dt, k1, k2 from tblA where dt >= '2023-11-01' ) t where k2 > 1
                    if (mvPredicateUsedColRefs != null && !mvPredicateUsedColRefs.contains(col)) {
                        return true;
                    }

                    // 2. filter out predicates whose column refs are in query's output columns because we can always compensate
                    // by output column using Union-All.
                    // eg:
                    //    query: select dt, k2 from tblA where date_trunc('day', dt) >= '2023-11-01'
                    //      mv : select dt, k2 from tblA where dt = '2023-11-01'
                    // NOTE: This is not supported by default because it can generate some bad cases (not the best plan):
                    // eg:
                    //    query: select dt, k2 from tblA where a < 1
                    //    mv   : select dt, k2 from tblA where a > 2
                    // this rule still can rewrite query by mv like this:
                    //    select * from (select dt, k2 from tblA) t where t.a < 1
                    // rewrite to:
                    //    select * from (select * from mv union all select dt, k2 from tblA where a <= 2 or a is null)
                    //          t where t.a < 1
                    // If you want to use this feature, you can use it by the setting:
                    //      set query_debug_options = "{'enableMVEagerUnionAllRewrite':true}";
                    if (queryOutputColumnRefs != null && queryOutputColumnRefs.contains(col)) {
                        return true;
                    }
                    return false;
                });
    }

    private void setAppliedUnionAllRewrite(Operator op) {
        int opRuleMask = op.getOpRuleMask() | OP_UNION_ALL_BIT;
        op.setOpRuleMask(opRuleMask);
    }

    private boolean isAppliedUnionAllRewrite(Operator op) {
        int opRuleMask = op.getOpRuleMask();
        return (opRuleMask & OP_UNION_ALL_BIT) != 0;
    }

    private PredicateSplit getUnionRewriteQueryCompensation(RewriteContext rewriteContext,
                                                            ColumnRewriter columnRewriter) {
        final PredicateSplit mvCompensationToQuery = getCompensationPredicates(columnRewriter,
                rewriteContext.getQueryBasedViewEquivalenceClasses(),
                rewriteContext.getQueryEquivalenceClasses(),
                rewriteContext.getMvPredicateSplit(),
                rewriteContext.getQueryPredicateSplit(),
                false);
        if (mvCompensationToQuery != null) {
            return mvCompensationToQuery;
        }
        // To avoid dead-loop rewrite, no rewrite when query extra predicate is not changed
        if (isAppliedUnionAllRewrite(rewriteContext.getQueryExpression().getOp())) {
            return null;
        }

        logMVRewrite(mvRewriteContext, "Try to pull up query's predicates to make possible for union rewrite");
        // try to pull up query's predicates to make possible for rewrite from mv to query.
        PredicateSplit mvPredicateSplit = rewriteContext.getMvPredicateSplit();
        PredicateSplit queryPredicateSplit = rewriteContext.getQueryPredicateSplit();
        Set<ColumnRefOperator> mvPredicateUsedColRefs = Utils.compoundAnd(mvPredicateSplit.getPredicates())
                .getColumnRefs().stream()
                .map(colRef -> (ColumnRefOperator) columnRewriter.rewriteViewToQuery(colRef))
                .collect(Collectors.toSet());

        List<ScalarOperator> queryPredicates = Lists.newArrayList();
        queryPredicates.addAll(Utils.extractConjuncts(queryPredicateSplit.getRangePredicates()));
        queryPredicates.addAll(Utils.extractConjuncts(queryPredicateSplit.getResidualPredicates()));

        Set<ScalarOperator> queryOnPredicates = MvUtils.getJoinOnPredicates(rewriteContext.getQueryExpression());
        ColumnRefSet queryOnPredicateUsedColRefs = Optional.ofNullable(Utils.compoundAnd(queryOnPredicates))
                .map(x -> x.getUsedColumns())
                .orElse(new ColumnRefSet());
        // use union-all rewrite eagerly if union-all rewrite can be used.
        QueryDebugOptions debugOptions  = optimizerContext.getSessionVariable().getQueryDebugOptions();
        ColumnRefSet queryOutputColumnRefs = debugOptions.isEnableMVEagerUnionAllRewrite() ?
                rewriteContext.getQueryExpression().getOutputColumns() : null;
        Set<ScalarOperator> queryExtraPredicates = queryPredicates.stream()
                .filter(pred -> isPullUpQueryPredicate(pred, mvPredicateUsedColRefs,
                        queryOnPredicateUsedColRefs, queryOutputColumnRefs))
                .collect(Collectors.toSet());

        if (queryExtraPredicates.isEmpty()) {
            return null;
        }

        final ScalarOperator queryExtraPredicate = Utils.compoundAnd(queryExtraPredicates);

        // query's output should contain all the extra predicates otherwise it cannot be pulled then.
        ColumnRefSet queryOutputColumnSet = rewriteContext.getQueryExpression().getOutputColumns();
        if (!queryOutputColumnSet.containsAll(queryExtraPredicate.getUsedColumns())) {
            return null;
        }
        // update query predicate after pull up
        Set<ScalarOperator> queryRetainPredicates = queryPredicates.stream()
                .filter(pred -> !queryExtraPredicates.contains(pred))
                .collect(Collectors.toSet());
        queryRetainPredicates.add(queryPredicateSplit.getEqualPredicates());
        // To avoid dead-loop rewrite, no rewrite when query extra predicate is not changed.
        // eg: MV's UNION-ALL RULE:
        //                 UNION                         UNION
        //               /        \                    /       \
        //  OP -->   EXTRA-OP    MV-SCAN  -->     UNION    MV-SCAN     ---> ....
        //                                       /      \
        //                                  EXTRA-OP    MV-SCAN
        if (queryRetainPredicates.isEmpty()) {
            return null;
        }
        final PredicateSplit newQuerySplit = PredicateSplit.splitPredicate(Utils.compoundAnd(queryRetainPredicates));
        rewriteContext.setQueryPredicateSplit(newQuerySplit);
        // update union rewrite extra predicate by pull up predicates
        rewriteContext.setUnionRewriteQueryExtraPredicate(queryExtraPredicate);
        return getCompensationPredicates(columnRewriter,
                rewriteContext.getQueryBasedViewEquivalenceClasses(),
                rewriteContext.getQueryEquivalenceClasses(),
                rewriteContext.getMvPredicateSplit(),
                rewriteContext.getQueryPredicateSplit(),
                false);
    }

    private OptExpression tryUnionRewrite(RewriteContext rewriteContext,
                                          OptExpression mvOptExpr) {
        final ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        final PredicateSplit mvCompensationToQuery = getUnionRewriteQueryCompensation(rewriteContext,
                columnRewriter);
        if (mvCompensationToQuery == null) {
            logMVRewrite(mvRewriteContext, "Rewrite union failed: cannot get compensation from view to query");
            return null;
        }
        Preconditions.checkState(mvCompensationToQuery.getPredicates()
                .stream().anyMatch(predicate -> !ConstantOperator.TRUE.equals(predicate)));

        // construct viewToQueryRefSet
        final Set<ColumnRefOperator> mvColumnRefSet = rewriteContext.getMvRefFactory().getColumnRefToColumns().keySet();
        final Set<ColumnRefOperator> viewToQueryColRefs = mvColumnRefSet.stream()
                .map(columnRef -> (ColumnRefOperator) columnRewriter.rewriteViewToQueryWithViewEc(columnRef))
                .collect(Collectors.toSet());
        final ColumnRefSet mvToQueryRefSet = new ColumnRefSet(viewToQueryColRefs);

        // construct queryScanOutputColRefs
        final List<LogicalScanOperator> queryScanOps = MvUtils.getScanOperator(rewriteContext.getQueryExpression());
        final List<ColumnRefOperator> queryScanOutputColRefs = queryScanOps.stream()
                .map(scan -> scan.getOutputColumns())
                .flatMap(List::stream)
                .collect(Collectors.toList());

        // should exclude the columns that are both in query scan output columns and mv ref set, which are valid columns
        // left columns are only in mv scan output columns
        // if the columns used in compensation predicates exist in the mvRefSets, but not in the scanOutputColumns, it means
        // the predicate can not be rewritten.
        // for example:
        //  predicate: empid:1 < 10
        //  query scan output: name: 2, salary: 3
        //  mvRefSets: empid:1, name: 2, salary: 3
        // the predicate empid:1 < 10 can not be rewritten
        mvToQueryRefSet.except(queryScanOutputColRefs);

        final Map<ColumnRefOperator, ScalarOperator> queryExprMap = MvUtils.getColumnRefMap(
                rewriteContext.getQueryExpression(), rewriteContext.getQueryRefFactory());
        ScalarOperator mvEqualPreds = MvUtils.canonizePredicate(mvCompensationToQuery.getEqualPredicates());
        ScalarOperator mvOtherPreds = MvUtils.canonizePredicate(Utils.compoundAnd(
                mvCompensationToQuery.getRangePredicates(),
                mvCompensationToQuery.getResidualPredicates()));
        List<ScalarOperator> rewrittenFailedPredicates = Lists.newArrayList();
        OptExpression queryExpression = rewriteContext.getQueryExpression();
        if (!ConstantOperator.TRUE.equals(mvEqualPreds)) {
            mvEqualPreds = columnRewriter.rewriteViewToQueryWithQueryEc(mvEqualPreds);
            mvEqualPreds = rewriteScalarOperatorToTarget(
                    mvEqualPreds, queryExprMap, rewriteContext, mvToQueryRefSet, true, true, rewrittenFailedPredicates);
        }
        if (!ConstantOperator.TRUE.equals(mvOtherPreds)) {
            mvOtherPreds = columnRewriter.rewriteViewToQueryWithViewEc(mvOtherPreds);
            mvOtherPreds = rewriteScalarOperatorToTarget(
                    mvOtherPreds, queryExprMap, rewriteContext, mvToQueryRefSet, false, true, rewrittenFailedPredicates);
        }
        ScalarOperator mvCompensationPredicates = Utils.compoundAnd(mvEqualPreds, mvOtherPreds);
        if (!rewrittenFailedPredicates.isEmpty()) {
            Pair<ScalarOperator, OptExpression> rewrittenPair = tryRewritePredicates(rewriteContext, mvToQueryRefSet,
                    rewriteContext.getQueryExpression(), rewriteContext.getQueryRefFactory(), rewrittenFailedPredicates);
            if (rewrittenPair == null) {
                logMVRewrite(mvRewriteContext, "Rewrite union failed: cannot rewrite compensations other predicates" +
                        " from view to query, rewrittenFailedPredicates:{}", rewrittenFailedPredicates);
                return null;
            }
            logMVRewrite(mvRewriteContext, "Success to rewrite union with extra failed compensate predicates:{}",
                    rewrittenPair.first);
            mvCompensationPredicates = Utils.compoundAnd(mvCompensationPredicates, rewrittenPair.first);
            queryExpression = rewrittenPair.second;
        }
        if (mvCompensationPredicates == null) {
            logMVRewrite(mvRewriteContext, "Rewrite union failed: cannot rewrite compensations from view to query, " +
                    "mvEqualPreds:{}, mvOtherPreds:{}", (mvEqualPreds == null), (mvOtherPreds == null));
            return null;
        }

        // for mv: select a, b from t where a < 10;
        // query: select a, b from t where a < 20;
        // queryBasedRewrite will return the tree of "select a, b from t where a >= 10 and a < 20"
        // which is realized by adding the compensation predicate to original query expression
        final OptExpression queryInput = queryBasedRewrite(rewriteContext, mvCompensationPredicates, queryExpression);
        if (queryInput == null) {
            logMVRewrite(mvRewriteContext, "Rewrite union failed: cannot rewrite MV based on query");
            return null;
        }
        logMVRewrite(mvRewriteContext, "Success to rewrite union with compensate predicates:{}",
                mvCompensationPredicates);

        // viewBasedRewrite will return the tree of "select a, b from t where a < 10" based on mv expression
        final OptExpression viewInput = viewBasedRewrite(rewriteContext, mvOptExpr);
        if (viewInput == null) {
            logMVRewrite(mvRewriteContext, "Rewrite union failed: cannot rewrite query based on MV");
            return null;
        }

        // Avoid UNION_ALL rule's dead loop, set a bit mask for the new query operator:
        //                 UNION                         UNION
        //               /        \                    /       \
        //  OP -->   EXTRA-OP    MV-SCAN  -->     UNION    MV-SCAN     ---> ....
        //                                       /      \
        //                                  EXTRA-OP    MV-SCAN
        setAppliedUnionAllRewrite(queryInput.getOp());

        // createUnion will return the union all result of queryInput and viewInput
        //           Union
        //       /          |
        // partial query   view
        return createUnion(queryInput, viewInput, rewriteContext);
    }

    // retry to rewrite the rewritten failed predicates by enforcing the columns not exists in query
    private Pair<ScalarOperator, OptExpression> tryRewritePredicates(
            RewriteContext rewriteContext,
            ColumnRefSet mvToQueryRefSet,
            OptExpression query,
            ColumnRefFactory queryRefFactory,
            List<ScalarOperator> predicatesToEnforce) {
        List<ColumnRefOperator> columns = Lists.newArrayList();
        for (ScalarOperator predicate : predicatesToEnforce) {
            predicate.getColumnRefs(columns);
        }
        // check each column to enforce should be in the Scan Operator
        for (ColumnRefOperator column : columns) {
            if (queryRefFactory.getRelationId(column.getId()) == -1) {
                return null;
            }
        }
        ColumnEnforcer columnEnforcer = new ColumnEnforcer(query, columns);
        OptExpression newQuery = columnEnforcer.enforce();
        mvRewriteContext.setEnforcedNonExistedColumns(columnEnforcer.getEnforcedNonExistedColumns());

        final List<LogicalScanOperator> queryScanOps = MvUtils.getScanOperator(newQuery);
        final List<ColumnRefOperator> queryScanOutputColRefs = queryScanOps.stream()
                .map(scan -> scan.getOutputColumns())
                .flatMap(List::stream)
                .collect(Collectors.toList());

        mvToQueryRefSet.except(queryScanOutputColRefs);

        OptExpression expr = newQuery.getOp() instanceof LogicalAggregationOperator ? newQuery.inputAt(0) : newQuery;
        final Map<ColumnRefOperator, ScalarOperator> queryExprMap =
                MvUtils.getColumnRefMap(expr, rewriteContext.getQueryRefFactory());

        ScalarOperator newPredicate = rewriteScalarOperatorToTarget(Utils.compoundAnd(predicatesToEnforce), queryExprMap,
                rewriteContext, mvToQueryRefSet, false, false, null);
        if (newPredicate == null) {
            return null;
        }
        return new Pair<>(newPredicate, newQuery);
    }


    private ScalarOperator rewriteScalarOperatorToTarget(
            ScalarOperator predicate,
            Map<ColumnRefOperator, ScalarOperator> exprMap,
            RewriteContext rewriteContext,
            ColumnRefSet originalRefSet,
            boolean isEqual,
            boolean isUnion,
            List<ScalarOperator> rewrittenFailedPredicates) {
        final EquationRewriter equationRewriter = isEqual ?
                buildEquationRewriter(exprMap, rewriteContext, false, false) :
                buildEquationRewriter(exprMap, rewriteContext, true, false);
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        final List<ScalarOperator> rewrittens =
                rewriteScalarOpToTarget(conjuncts, equationRewriter, null, originalRefSet, isUnion, rewrittenFailedPredicates);
        if (rewrittens == null || rewrittens.isEmpty()) {
            logMVRewrite(mvRewriteContext, "rewrite scalar operator failed. isEqual:{}, isUnion:{}, predicate:{},",
                    isEqual, isUnion, predicate);
            return null;
        }
        return Utils.compoundAnd(rewrittens);
    }

    protected OptExpression queryBasedRewrite(RewriteContext rewriteContext, ScalarOperator compensationPredicates,
                                              OptExpression queryExpression) {
        queryExpression = MvUtils.replaceLogicalViewScanOperator(queryExpression,
                materializationContext.getOptimizerContext().getQueryMaterializationContext());
        if (queryExpression == null) {
            return null;
        }
        // query predicate and (not viewToQueryCompensationPredicate) is the final query compensation predicate
        ScalarOperator queryCompensationPredicate = CompoundPredicateOperator.not(compensationPredicates);
        List<ScalarOperator> predicates = Utils.extractConjuncts(queryCompensationPredicate);
        predicates.removeAll(mvRewriteContext.getOnPredicates());
        queryCompensationPredicate = Utils.compoundAnd(predicates);
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        queryCompensationPredicate = columnRewriter.rewriteByQueryEc(queryCompensationPredicate);
        queryCompensationPredicate = MvUtils.canonizePredicate(queryCompensationPredicate);
        if (!ConstantOperator.TRUE.equals(queryCompensationPredicate)) {
            if (queryExpression.getOp().getProjection() != null) {
                ReplaceColumnRefRewriter rewriter =
                        new ReplaceColumnRefRewriter(queryExpression.getOp().getProjection().getColumnRefMap());
                queryCompensationPredicate = rewriter.rewrite(queryCompensationPredicate);
            }
            queryCompensationPredicate = processNullPartition(rewriteContext, queryCompensationPredicate);
            if (queryCompensationPredicate == null) {
                return null;
            }

            OptExpression newQueryExpr = pushdownPredicatesForJoin(queryExpression, queryCompensationPredicate);
            deriveLogicalProperty(newQueryExpr);
            if (mvRewriteContext.getEnforcedNonExistedColumns() != null &&
                    !mvRewriteContext.getEnforcedNonExistedColumns().isEmpty()) {
                newQueryExpr = pruneEnforcedColumns(newQueryExpr);
                deriveLogicalProperty(newQueryExpr);
            }
            return newQueryExpr;

        }
        return null;
    }

    private OptExpression pruneEnforcedColumns(OptExpression queryExpr) {
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression input : queryExpr.getInputs()) {
            OptExpression newInput = pruneEnforcedColumns(input);
            newInputs.add(newInput);
        }
        Operator newOp = doPruneEnforcedColumns(queryExpr);
        return OptExpression.create(newOp, newInputs);
    }

    private Operator doPruneEnforcedColumns(OptExpression queryExpr) {
        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
        if (queryExpr.getOp().getProjection() != null) {
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = queryExpr.getOp().getProjection().getColumnRefMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
                if (mvRewriteContext.getEnforcedNonExistedColumns().contains(entry.getKey())) {
                    continue;
                }
                newColumnRefMap.put(entry.getKey(), entry.getValue());
            }
        } else {
            List<ColumnRefOperator> outputColumns =
                    queryExpr.getOutputColumns().getColumnRefOperators(materializationContext.getQueryRefFactory());
            outputColumns = outputColumns.stream()
                    .filter(column -> !mvRewriteContext.getEnforcedNonExistedColumns().contains(column))
                    .collect(Collectors.toList());
            outputColumns.stream().forEach(column -> newColumnRefMap.put(column, column));
        }
        Projection newProjection = new Projection(newColumnRefMap);
        Operator.Builder builder = OperatorBuilderFactory.build(queryExpr.getOp());
        builder.withOperator(queryExpr.getOp());
        builder.setProjection(newProjection);
        return builder.build();
    }

    // pushdown predicates on join nodes
    // the OptExpression will be modified in place
    private OptExpression pushdownPredicatesForJoin(OptExpression optExpression, ScalarOperator predicate) {
        if (!(optExpression.getOp() instanceof LogicalJoinOperator)) {
            if (predicate != null) {
                // predicate can not be pushdown, we should add it it optExpression
                Operator.Builder builder = OperatorBuilderFactory.build(optExpression.getOp());
                builder.withOperator(optExpression.getOp());
                PredicateSplit predicateSplit = PredicateSplit.splitPredicate(
                        Utils.compoundAnd(predicate, optExpression.getOp().getPredicate()));
                ScalarOperator canonizePredicates = MvUtils.canonizePredicateForRewrite(
                        queryMaterializationContext, predicateSplit.toScalarOperator());
                builder.setPredicate(canonizePredicates);
                Operator newQueryOp = builder.build();
                return OptExpression.create(newQueryOp, optExpression.getInputs());
            } else {
                return optExpression;
            }
        }

        OptExpression newJoin = doPushdownPredicate(optExpression, predicate);
        List<OptExpression> children = Lists.newArrayList();
        for (OptExpression child : newJoin.getInputs()) {
            children.add(pushdownPredicatesForJoin(child, null));
        }
        return OptExpression.create(newJoin.getOp(), children);
    }

    private OptExpression doPushdownPredicate(OptExpression joinOptExpression, ScalarOperator predicate) {
        // To ensure join's property is deduced which is needed in `predicate-push-down`, derive its logical property.
        deriveLogicalProperty(joinOptExpression);
        Preconditions.checkState(joinOptExpression.getOp() instanceof LogicalJoinOperator);
        optimizerContext.clearNotNullPredicates();
        JoinPredicatePushdown joinPredicatePushdown = new JoinPredicatePushdown(joinOptExpression,
                false, true, materializationContext.getQueryRefFactory(),
                true, optimizerContext);
        return joinPredicatePushdown.pushdown(predicate);
    }

    private List<ScalarOperator> getPartitionRelatedPredicates(Set<ScalarOperator> conjuncts, MaterializedView mv) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            return Lists.newArrayList();
        }
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        List<Column> partitionColumns = expressionRangePartitionInfo.getPartitionColumns();
        Preconditions.checkState(partitionColumns.size() == 1);
        List<ScalarOperator> partitionRelatedPredicates = conjuncts.stream()
                .filter(p -> isRelatedPredicate(p, partitionColumns.get(0).getName()))
                .collect(Collectors.toList());
        return partitionRelatedPredicates;
    }

    // process null value partition
    // when query select all data from base tables,
    // should add partition column is null predicate into queryCompensationPredicate to get null partition value related data
    private ScalarOperator processNullPartition(RewriteContext rewriteContext, ScalarOperator queryCompensationPredicate) {
        MaterializedView mv = mvRewriteContext.getMaterializationContext().getMv();
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            return queryCompensationPredicate;
        }
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        List<Column> partitionColumns = expressionRangePartitionInfo.getPartitionColumns();
        Preconditions.checkState(partitionColumns.size() == 1);
        ScalarOperator queryPredicate = rewriteContext.getQueryPredicateSplit().toScalarOperator();
        Set<ScalarOperator> queryPredicates = Utils.extractConjunctSet(queryPredicate);
        List<ScalarOperator> queryPartitionRelatedScalars = getPartitionRelatedPredicates(queryPredicates, mv);

        ScalarOperator mvPredicate = rewriteContext.getMvPredicateSplit().toScalarOperator();
        Set<ScalarOperator> mvPredicates = Utils.extractConjunctSet(mvPredicate);
        List<ScalarOperator> mvPartitionRelatedScalars = getPartitionRelatedPredicates(mvPredicates, mv);
        if (queryPartitionRelatedScalars.isEmpty() && !mvPartitionRelatedScalars.isEmpty()) {
            // when query has no partition related predicates and mv has,
            // we should consider to add null value predicate into compensation predicates
            ColumnRefOperator partitionColumnRef =
                    findQueryPartitionColumnRef(mvPredicate, partitionColumns.get(0), rewriteContext);
            if (!partitionColumnRef.isNullable()) {
                // only add null value into compensation predicate when partition column is nullable
                return queryCompensationPredicate;
            }
            IsNullPredicateOperator isNullPredicateOperator = new IsNullPredicateOperator(partitionColumnRef);
            IsNullPredicateOperator isNotNullPredicateOperator = new IsNullPredicateOperator(true, partitionColumnRef);
            List<ScalarOperator> predicates = Utils.extractConjuncts(queryCompensationPredicate);
            List<ScalarOperator> partitionRelatedPredicates = predicates.stream()
                    .filter(predicate -> isRelatedPredicate(predicate, partitionColumnRef.getName()))
                    .collect(Collectors.toList());
            predicates.removeAll(partitionRelatedPredicates);
            if (partitionRelatedPredicates.contains(isNullPredicateOperator)
                    || partitionRelatedPredicates.contains(isNotNullPredicateOperator)) {
                if (partitionRelatedPredicates.size() != 1) {
                    // has other partition predicates except partition column is null
                    // do not support now
                    // can it happened?
                    return null;
                }
                predicates.addAll(partitionRelatedPredicates);
            } else {
                // add partition column is null into compensation predicates
                ScalarOperator partitionPredicate =
                        Utils.compoundOr(Utils.compoundAnd(partitionRelatedPredicates), isNullPredicateOperator);
                predicates.add(partitionPredicate);
            }
            queryCompensationPredicate = MvUtils.canonizePredicate(Utils.compoundAnd(predicates));
        }
        return queryCompensationPredicate;
    }

    private ColumnRefOperator findQueryPartitionColumnRef(
            ScalarOperator mvPredicate, Column partitionColumn, RewriteContext rewriteContext) {
        // when query has no partition related predicates and mv has,
        // we should consider to add null value predicate into compensation predicates
        ColumnRefOperator partitionColumnRef =
                getColumnRefFromPredicate(mvPredicate, partitionColumn.getName());
        Preconditions.checkState(partitionColumnRef != null);
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        partitionColumnRef = columnRewriter.rewriteViewToQuery(partitionColumnRef).cast();

        // for view based mv rewrite, we should mapping the partition column to output column of view scan
        if (queryMaterializationContext.getViewScans() != null) {
            for (LogicalViewScanOperator viewScanOperator : queryMaterializationContext.getViewScans()) {
                Projection projection = viewScanOperator.getProjection();
                if (viewScanOperator.getProjection() != null) {
                    for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                        if (entry.getValue().equivalent(partitionColumnRef)) {
                            return entry.getKey();
                        }
                    }
                }
            }
        }
        return partitionColumnRef;
    }

    private boolean isRelatedPredicate(ScalarOperator scalarOperator, String name) {
        ScalarOperatorVisitor<Boolean, Void> visitor = new ScalarOperatorVisitor<Boolean, Void>() {
            @Override
            public Boolean visit(ScalarOperator scalarOperator, Void context) {
                for (ScalarOperator child : scalarOperator.getChildren()) {
                    boolean ret = child.accept(this, null);
                    if (ret) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Boolean visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                return columnRefOperator.getName().equalsIgnoreCase(name);
            }
        };
        return scalarOperator.accept(visitor, null);
    }

    private ColumnRefOperator getColumnRefFromPredicate(ScalarOperator predicate, String name) {
        ScalarOperatorVisitor<ColumnRefOperator, Void> visitor = new ScalarOperatorVisitor<ColumnRefOperator, Void>() {
            @Override
            public ColumnRefOperator visit(ScalarOperator scalarOperator, Void context) {
                for (ScalarOperator child : scalarOperator.getChildren()) {
                    ColumnRefOperator ret = child.accept(this, null);
                    if (ret != null) {
                        return ret;
                    }
                }
                return null;
            }

            @Override
            public ColumnRefOperator visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                if (columnRefOperator.getName().equalsIgnoreCase(name)) {
                    return columnRefOperator;
                }
                return null;
            }
        };
        return predicate.accept(visitor, null);
    }

    protected OptExpression createUnion(OptExpression queryInput, OptExpression viewInput,
                                        RewriteContext rewriteContext) {
        Map<ColumnRefOperator, ScalarOperator> queryColumnRefMap =
                MvUtils.getColumnRefMap(queryInput, rewriteContext.getQueryRefFactory());

        // keys of queryColumnRefMap and mvColumnRefMap are the same
        List<ColumnRefOperator> originalOutputColumns =
                queryColumnRefMap.keySet().stream().collect(Collectors.toList());

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
        OptExpression result = OptExpression.create(unionOperator, newQueryInput, viewInput);

        // Add extra union all predicates above union all operator.
        if (rewriteContext.getUnionRewriteQueryExtraPredicate() != null) {
            addUnionAllExtraPredicate(result.getOp(), rewriteContext.getUnionRewriteQueryExtraPredicate());
        }

        deriveLogicalProperty(result);
        return result;
    }

    protected void addUnionAllExtraPredicate(Operator unionOp,
                                             ScalarOperator extraPredicate) {
        Preconditions.checkState(unionOp.getProjection() != null);
        ScalarOperator origPredicate = unionOp.getPredicate();
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(unionOp.getProjection().getColumnRefMap());
        ScalarOperator rewrittenExtraPredicate = rewriter.rewrite(extraPredicate);
        unionOp.setPredicate(Utils.compoundAnd(origPredicate, rewrittenExtraPredicate));
    }

    protected EquationRewriter buildEquationRewriter(
            Map<ColumnRefOperator, ScalarOperator> viewProjection, RewriteContext rewriteContext, boolean isViewBased) {
        return buildEquationRewriter(viewProjection, rewriteContext, isViewBased, true, true);
    }

    protected EquationRewriter buildEquationRewriter(
            Map<ColumnRefOperator, ScalarOperator> columnRefMap,
            RewriteContext rewriteContext,
            boolean isViewBased, boolean isQueryToMV) {
        return buildEquationRewriter(columnRefMap, rewriteContext, isViewBased, isQueryToMV, true);
    }

    protected EquationRewriter buildEquationRewriter(
            Map<ColumnRefOperator, ScalarOperator> columnRefMap,
            RewriteContext rewriteContext,
            boolean isViewBased, boolean isQueryToMV, boolean useEc) {
        EquationRewriter rewriter = new EquationRewriter();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(entry.getValue());
            ScalarOperator rewriteScalarOp;
            if (isQueryToMV) {
                rewriteScalarOp = useEc ? columnRewriter.rewriteToTargetWithEc(rewritten, isViewBased)
                        : columnRewriter.rewriteViewToQuery(rewritten);
                // if rewriteScalarOp == rewritten and !rewritten.getUsedColumns().isEmpty(),
                // it means the rewritten can not be mapped from mv to query.
                // and ColumnRefOperator may conflict between mv and query(same id but not same name),
                // so do not put it into the reversedMap
                if (rewriteScalarOp != rewritten || rewritten.getUsedColumns().isEmpty()) {
                    rewriter.addMapping(rewriteScalarOp, entry.getKey());
                }
            } else {
                rewriteScalarOp = columnRewriter.rewriteByEc(rewritten, isViewBased);
                rewriter.addMapping(rewriteScalarOp, entry.getKey());
            }

        }
        return rewriter;
    }

    private ColumnRefOperator getColumnRef(ScalarOperator operator, int index) {
        Preconditions.checkState(operator.getChild(index).isColumnRef());
        return (ColumnRefOperator) operator.getChild(index);
    }

    private EquivalenceClasses createEquivalenceClasses(ScalarOperator equalPredicates,
                                                        OptExpression expression,
                                                        ReplaceColumnRefRewriter refRewriter,
                                                        ColumnRewriter columnRewriter) {
        EquivalenceClasses ec = new EquivalenceClasses();
        if (equalPredicates == null) {
            return ec;
        }
        equalPredicates = MvUtils.canonizePredicateForRewrite(queryMaterializationContext, equalPredicates);

        List<ScalarOperator> outerJoinOnPredicate = MvUtils.collectOuterAntiJoinOnPredicate(expression);
        final PredicateSplit outerJoinPredicateSplit = PredicateSplit.splitPredicate(Utils.compoundAnd(outerJoinOnPredicate));
        // should use ReplaceColumnRefRewriter to rewrite outerJoinPredicateSplit to base tables
        ScalarOperator outerJoinEqualPredicates = refRewriter.rewrite(outerJoinPredicateSplit.getEqualPredicates());
        outerJoinEqualPredicates = MvUtils.canonizePredicateForRewrite(queryMaterializationContext, outerJoinEqualPredicates);
        List<ScalarOperator> outerJoinConjuncts = Utils.extractConjuncts(outerJoinEqualPredicates);

        for (ScalarOperator equalPredicate : Utils.extractConjuncts(equalPredicates)) {
            if (outerJoinConjuncts.contains(equalPredicate)) {
                // outer join on predicates can not be used to construct equivalence class
                continue;
            }
            ColumnRefOperator left = getColumnRef(equalPredicate, 0);
            ColumnRefOperator right = getColumnRef(equalPredicate, 1);
            ScalarOperator leftTarget = (columnRewriter == null) ? left : columnRewriter.rewriteViewToQuery(left);
            ScalarOperator rightTarget = (columnRewriter == null) ? right : columnRewriter.rewriteViewToQuery(right);
            if (leftTarget == null || rightTarget == null) {
                return null;
            }
            ec.addEquivalence(leftTarget.cast(), rightTarget.cast());
        }
        return ec;
    }

    private void deduceEquivalenceClassesFromRangePredicates(List<ScalarOperator> rangePredicates,
                                                             EquivalenceClasses ec,
                                                             boolean addIntoEC) {
        Preconditions.checkState(ec != null);
        Map<ConstantOperator, Set<ColumnRefOperator>> constantOperatorSetMap = Maps.newHashMap();
        for (ScalarOperator rangePredicate : rangePredicates) {
            if (rangePredicate instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) rangePredicate;
                if (binaryPredicate.getBinaryType().isEqual()) {
                    ScalarOperator left = binaryPredicate.getChild(0);
                    ScalarOperator right = binaryPredicate.getChild(1);
                    if (left.isColumnRef() && right.isConstant()) {
                        constantOperatorSetMap
                                .computeIfAbsent((ConstantOperator) right, x -> new HashSet<>())
                                .add((ColumnRefOperator) left);
                    } else if (right.isColumnRef() && left.isConstant()) {
                        constantOperatorSetMap
                                .computeIfAbsent((ConstantOperator) left, x -> new HashSet<>())
                                .add((ColumnRefOperator) right);
                    }
                }
            }
        }

        for (Set<ColumnRefOperator> equalPredicates : constantOperatorSetMap.values()) {
            if (equalPredicates.size() > 1) {
                Iterator<ColumnRefOperator> iterator = equalPredicates.iterator();
                ColumnRefOperator first = iterator.next();
                while (iterator.hasNext()) {
                    ColumnRefOperator next = iterator.next();
                    if (addIntoEC) {
                        if (ec.addRedundantEquivalence(first, next)) {
                            ec.addEquivalence(first, next);
                        }
                    } else {
                        ec.addRedundantEquivalence(first, next);
                    }
                }
            }
        }
    }

    private boolean addCompensationJoinColumnsIntoEquivalenceClasses(
            ColumnRewriter columnRewriter,
            EquivalenceClasses ec,
            Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns) {
        if (compensationJoinColumns == null || compensationJoinColumns.isEmpty()) {
            return true;
        }

        for (final Map.Entry<ColumnRefOperator, ColumnRefOperator> entry : compensationJoinColumns.entries()) {
            final ColumnRefOperator left = entry.getKey();
            final ColumnRefOperator right = entry.getValue();
            ScalarOperator newKey = columnRewriter.rewriteViewToQuery(left);
            ScalarOperator newValue = columnRewriter.rewriteViewToQuery(right);
            if (newKey == null || newValue == null) {
                return false;
            }
            ec.addEquivalence(newKey.cast(), newValue.cast());
        }
        return true;
    }

    public static MatchMode getMatchMode(List<Table> queryTables, List<Table> mvTables) {
        MatchMode matchMode = MatchMode.NOT_MATCH;
        if (queryTables.size() == mvTables.size() && Sets.newHashSet(queryTables).containsAll(mvTables)) {
            matchMode = MatchMode.COMPLETE;
        } else if (queryTables.size() > mvTables.size() && queryTables.containsAll(mvTables)) {
            matchMode = MatchMode.QUERY_DELTA;
        } else if (queryTables.size() < mvTables.size() && Sets.newHashSet(mvTables).containsAll(queryTables)) {
            matchMode = MatchMode.VIEW_DELTA;
        }
        return matchMode;
    }

    protected OptExpression deriveOptExpression(OptExpression root) {
        deriveLogicalProperty(root);
        return root;
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
    protected OptExpression viewBasedRewrite(RewriteContext rewriteContext,
                                             OptExpression mvScanOptExpression) {
        final Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp =
                rewriteContext.getMVColumnRefToScalarOp();

        EquationRewriter queryExprToMvExprRewriter =
                buildEquationRewriter(mvColumnRefToScalarOp, rewriteContext, false);

        return rewriteProjection(rewriteContext, queryExprToMvExprRewriter, mvScanOptExpression);
    }

    protected OptExpression rewriteProjection(RewriteContext rewriteContext,
                                              EquationRewriter equationRewriter,
                                              OptExpression mvOptExpr) {
        Map<ColumnRefOperator, ScalarOperator> queryMap = MvUtils.getColumnRefMap(
                rewriteContext.getQueryExpression(), rewriteContext.getQueryRefFactory());
        Map<ColumnRefOperator, ScalarOperator> swappedQueryColumnMap = Maps.newHashMap();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryMap.entrySet()) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(entry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            swappedQueryColumnMap.put(entry.getKey(), swapped);
        }

        Map<ColumnRefOperator, ColumnRefOperator> outputMapping = rewriteContext.getOutputMapping();
        // Generate different column-refs for multi use of the same mv to avoid bugs later.
        if (materializationContext.getMVUsedCount() > 0) {
            OptExpressionDuplicator duplicator = new OptExpressionDuplicator(materializationContext);
            mvOptExpr = duplicator.duplicate(mvOptExpr);
            Map<ColumnRefOperator, ScalarOperator> replacedOutputMapping = duplicator.getColumnMapping();
            Map<ColumnRefOperator, ColumnRefOperator> newOutputMapping = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ColumnRefOperator> entry : outputMapping.entrySet()) {
                ColumnRefOperator newDuplicatorColRef = (ColumnRefOperator) replacedOutputMapping.get(entry.getValue());
                if (newDuplicatorColRef != null) {
                    newOutputMapping.put(entry.getKey(), newDuplicatorColRef);
                }
            }
            outputMapping = newOutputMapping;
        }

        Map<ColumnRefOperator, ScalarOperator> newQueryProjection = Maps.newHashMap();
        equationRewriter.setOutputMapping(outputMapping);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : swappedQueryColumnMap.entrySet()) {
            ScalarOperator rewritten = equationRewriter.replaceExprWithTarget(entry.getValue());
            if (rewritten == null) {
                logMVRewrite(mvRewriteContext, "Rewrite projection failed: cannot rewrite expr {}",
                        entry.getValue().toString());
                return null;
            }
            if (!isAllExprReplaced(rewritten, new ColumnRefSet(rewriteContext.getQueryColumnSet()))) {
                // it means there is some column that can not be rewritten by outputs of mv
                logMVRewrite(mvRewriteContext, "Rewrite projection failed: cannot totally rewrite expr {}",
                        entry.getValue().toString());
                return null;
            }
            newQueryProjection.put(entry.getKey(), rewritten);
        }
        Projection newProjection = new Projection(newQueryProjection);
        mvOptExpr.getOp().setProjection(newProjection);
        return mvOptExpr;
    }

    protected List<ScalarOperator> rewriteScalarOpToTarget(
            List<ScalarOperator> exprsToRewrites,
            EquationRewriter equationRewriter,
            Map<ColumnRefOperator, ColumnRefOperator> outputMapping,
            ColumnRefSet originalColumnSet, boolean isUnion,
            List<ScalarOperator> rewrittenFailedPredicates) {
        List<ScalarOperator> rewrittenExprs = Lists.newArrayList();
        equationRewriter.setOutputMapping(outputMapping);
        for (ScalarOperator expr : exprsToRewrites) {
            ScalarOperator rewritten = equationRewriter.replaceExprWithTarget(expr);
            if (expr.isVariable() && expr == rewritten) {
                // it means it can not be rewritten  by target
                if (isUnion) {
                    rewrittenFailedPredicates.add(expr);
                    continue;
                } else {
                    logMVRewrite(mvRewriteContext, "Rewrite scalar operator failed: {} cannot be rewritten",
                            expr);
                    return Lists.newArrayList();
                }
            }
            if (originalColumnSet != null && !isAllExprReplaced(rewritten, originalColumnSet)) {
                if (isUnion) {
                    rewrittenFailedPredicates.add(expr);
                    continue;
                } else {
                    // it means there is some column that can not be rewritten by outputs of mv
                    logMVRewrite(mvRewriteContext, "Rewrite scalar operator failed: {} cannot be all replaced", expr);
                    return Lists.newArrayList();
                }
            }
            rewrittenExprs.add(rewritten);
        }
        return rewrittenExprs;
    }

    protected boolean isAllExprReplaced(ScalarOperator rewritten, ColumnRefSet originalColumnSet) {
        ScalarOperatorVisitor<Void, Void> visitor = new ScalarOperatorVisitor<Void, Void>() {
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

    private List<BiMap<Integer, Integer>> generateRelationIdMap(
            ColumnRefFactory queryRefFactory, List<Table> queryTables, OptExpression queryExpression,
            ColumnRefFactory mvRefFactory, List<Table> mvTables, OptExpression mvExpression,
            MatchMode matchMode, Map<Table, Set<Integer>> compensationRelations,
            Map<Integer, Integer> expectedExtraQueryToMVRelationIds) {
        Map<Table, Set<Integer>> queryTableToRelationId =
                getTableToRelationid(queryExpression, queryRefFactory, queryTables);
        if (matchMode == MatchMode.VIEW_DELTA) {
            for (Map.Entry<Table, Set<Integer>> entry : compensationRelations.entrySet()) {
                if (queryTableToRelationId.containsKey(entry.getKey())) {
                    queryTableToRelationId.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    queryTableToRelationId.put(entry.getKey(), entry.getValue());
                }
            }
        }
        Map<Table, Set<Integer>> mvTableToRelationId =
                getTableToRelationid(mvExpression, mvRefFactory, mvTables);
        // `queryTableToRelationId` may not equal to `mvTableToRelationId` when mv/query's columns are not
        // satisfied for the query.
        if (!queryTableToRelationId.keySet().equals(mvTableToRelationId.keySet())) {
            return Lists.newArrayList();
        }
        List<BiMap<Integer, Integer>> result = ImmutableList.of(HashBiMap.create());
        for (Map.Entry<Table, Set<Integer>> queryEntry : queryTableToRelationId.entrySet()) {
            Preconditions.checkState(!queryEntry.getValue().isEmpty());
            Set<Integer> queryTableIds = queryEntry.getValue();
            Set<Integer> mvTableIds = mvTableToRelationId.get(queryEntry.getKey());
            if (queryTableIds.size() == 1) {
                Integer queryTableId = queryTableIds.iterator().next();
                if (mvTableIds.size() == 1) {
                    Integer mvTableId = mvTableIds.iterator().next();
                    for (BiMap<Integer, Integer> m : result) {
                        m.put(queryTableId, mvTableId);
                    }
                } else {
                    // query's table id is one and mv's tables are greater than one:
                    // Query table ids: a
                    // MV    table ids: a1, a2
                    // output:
                    //  a -> a1
                    //  a -> a2
                    ImmutableList.Builder<BiMap<Integer, Integer>> newResult = ImmutableList.builder();
                    Iterator<Integer> mvTableIdsIter = mvTableIds.iterator();
                    while (mvTableIdsIter.hasNext()) {
                        Integer mvTableId = mvTableIdsIter.next();
                        for (BiMap<Integer, Integer> m : result) {
                            final BiMap<Integer, Integer> newQueryToMvMap = HashBiMap.create(m);
                            newQueryToMvMap.put(queryTableId, mvTableId);
                            newResult.add(newQueryToMvMap);
                        }
                    }
                    result = newResult.build();
                }
            } else {
                // query's table ids are greater than one and mv's tables are greater than one:
                // Query table ids: a1, a2
                // MV    table ids: A1, A2
                // output:
                //  a1 -> A1, a2 -> A1 (x)
                //  a1 -> A2, a2 -> A2 (x) : cannot contain same values.
                //  a1 -> A1, a2 -> A2
                //  a1 -> A2, a2 -> A1
                List<Integer> queryList = new ArrayList<>(queryTableIds);
                List<Integer> mvList = new ArrayList<>(mvTableIds);
                List<List<Integer>> mvTableIdPermutations = getPermutationsOfTableIds(mvList, queryList.size());
                ImmutableList.Builder<BiMap<Integer, Integer>> newResult = ImmutableList.builder();
                for (List<Integer> mvPermutation : mvTableIdPermutations) {
                    Preconditions.checkState(mvPermutation.size() == queryList.size());
                    for (BiMap<Integer, Integer> m : result) {
                        final BiMap<Integer, Integer> newQueryToMvMap = HashBiMap.create(m);
                        for (int i = 0; i < queryList.size(); i++) {
                            newQueryToMvMap.put(queryList.get(i), mvPermutation.get(i));
                        }
                        newResult.add(newQueryToMvMap);
                    }
                }
                result = newResult.build();
            }
        }

        if (expectedExtraQueryToMVRelationIds == null || expectedExtraQueryToMVRelationIds.isEmpty()) {
            return result;
        } else {
            List<BiMap<Integer, Integer>> finalResult = Lists.newArrayList();
            for (BiMap<Integer, Integer> queryToMVRelations : result) {
                // Ignore permutations which not contain expected extra query to mv relation id mappings.
                boolean hasExpectedExtraQueryToMVRelationIds = true;
                for (Map.Entry<Integer, Integer> expect : expectedExtraQueryToMVRelationIds.entrySet()) {
                    if (!queryToMVRelations.get(expect.getKey()).equals(expect.getValue())) {
                        hasExpectedExtraQueryToMVRelationIds = false;
                        break;
                    }
                }
                if (hasExpectedExtraQueryToMVRelationIds) {
                    finalResult.add(queryToMVRelations);
                }
            }
            return finalResult;
        }
    }

    // TODO: optimize the search algorithm to prune search space
    private List<List<Integer>> getPermutationsOfTableIds(List<Integer> tableIds, int n) {
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            int limit = context.getSessionVariable().getMaterializedViewJoinSameTablePermutationLimit();
            if (tableIds.size() > limit || n > limit) {
                String message = String.format("MV or query use the same table too many times(mvTables=%d, " +
                                "queryTables=%d), the optimizer cannot rewrite this query within the limited time. " +
                                "You can enlarge the variable %s to break this limitation, current limit is %d",
                        tableIds.size(), n, SessionVariable.MATERIALIZED_VIEW_JOIN_SAME_TABLE_PERMUTATION_LIMIT, limit);
                OptimizerTraceUtil.logMVRewrite(mvRewriteContext, message);
                throw new StarRocksPlannerException(message, ErrorType.RULE_EXHAUSTED);
            }
        }
        List<Integer> t = new ArrayList<>();
        List<List<Integer>> ret = new ArrayList<>();
        getPermutationsOfTableIds(tableIds, n, t, ret);
        return ret;
    }

    private static void getPermutationsOfTableIds(List<Integer> tableIds, int targetSize,
                                                  List<Integer> tmpResult, List<List<Integer>> ret) {
        if (tmpResult.size() == targetSize) {
            ret.add(new ArrayList<>(tmpResult));
            return;
        }
        for (int i = 0; i < tableIds.size(); i++) {
            if (tmpResult.contains(tableIds.get(i))) {
                continue;
            }
            tmpResult.add(tableIds.get(i));
            getPermutationsOfTableIds(tableIds, targetSize, tmpResult, ret);
            tmpResult.remove(tmpResult.size() - 1);
        }
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

    private Map<Integer, Map<String, ColumnRefOperator>> getRelationIdToColumns(ColumnRefFactory refFactory) {
        // relationId -> column map
        Map<Integer, Map<String, ColumnRefOperator>> result = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> entry : refFactory.getColumnToRelationIds().entrySet()) {
            result.computeIfAbsent(entry.getValue(), k -> Maps.newHashMap());
            ColumnRefOperator columnRef = refFactory.getColumnRef(entry.getKey());
            result.get(entry.getValue()).put(columnRef.getName(), columnRef);
        }
        return result;
    }

    // when isQueryToMV is true, get compensation predicates of query against view
    // or get compensation predicates of view against query
    private PredicateSplit getCompensationPredicates(ColumnRewriter columnRewriter,
                                                     EquivalenceClasses sourceEquivalenceClasses,
                                                     EquivalenceClasses targetEquivalenceClasses,
                                                     PredicateSplit srcPredicateSplit,
                                                     PredicateSplit targetPredicateSplit,
                                                     boolean isQueryToMV) {
        // 1. equality join subsumption test
        final ScalarOperator compensationEqualPredicate =
                getCompensationEqualPredicate(sourceEquivalenceClasses, targetEquivalenceClasses);
        if (compensationEqualPredicate == null) {
            logMVRewrite(mvRewriteContext, "Compensate equal predicates failed, src: {}, target:{}",
                    Joiner.on(",").join(sourceEquivalenceClasses.getEquivalenceClasses()),
                    Joiner.on(",").join(targetEquivalenceClasses.getEquivalenceClasses()));
            return null;
        }

        // 2. range subsumption test
        ScalarOperator srcPr = srcPredicateSplit.getRangePredicates();
        ScalarOperator targetPr = targetPredicateSplit.getRangePredicates();
        ScalarOperator compensationPr =
                getCompensationPredicate(srcPr, targetPr, columnRewriter, true, isQueryToMV);
        if (compensationPr == null) {
            logMVRewrite(mvRewriteContext, "Compensate range predicates failed," +
                    "srcPr:{}, targetPr:{}", MvUtils.toString(srcPr), MvUtils.toString(targetPr));
            return null;
        }

        // 3. residual subsumption test
        ScalarOperator srcPu = srcPredicateSplit.getResidualPredicates();
        ScalarOperator targetPu = targetPredicateSplit.getResidualPredicates();
        if (srcPu == null && targetPu != null) {
            // query: empid < 5
            // mv: empid < 5 or salary > 100
            if (!isQueryToMV) {
                // compensationEqualPredicate and compensationPr is based on query, need to change it to view based
                srcPu = Utils.compoundAnd(columnRewriter.rewriteQueryToView(compensationEqualPredicate),
                        columnRewriter.rewriteQueryToView(compensationPr));
            } else {
                srcPu = Utils.compoundAnd(compensationEqualPredicate, compensationPr);
            }
        }
        ScalarOperator compensationPu =
                getCompensationPredicate(srcPu, targetPu, columnRewriter, false, isQueryToMV);
        if (compensationPu == null) {
            logMVRewrite(mvRewriteContext, "Compensate residual predicates failed," +
                    "srcPr:{}, targetPr:{}", MvUtils.toString(srcPu), MvUtils.toString(targetPu));
            return null;
        }

        return PredicateSplit.of(compensationEqualPredicate, compensationPr, compensationPu);
    }

    private ScalarOperator getCompensationPredicate(ScalarOperator srcPu,
                                                    ScalarOperator targetPu,
                                                    ColumnRewriter columnRewriter,
                                                    boolean isRangePredicate,
                                                    boolean isQueryToMV) {
        if (srcPu == null && targetPu == null) {
            return ConstantOperator.TRUE;
        }

        ScalarOperator compensationPu;
        if (srcPu == null) {
            return null;
        } else if (targetPu == null) {
            if (isQueryToMV) {
                // src is query
                compensationPu = columnRewriter.rewriteByQueryEc(srcPu.clone());
            } else {
                compensationPu = columnRewriter.rewriteViewToQueryWithViewEc(srcPu.clone());
            }
        } else {
            Pair<ScalarOperator, ScalarOperator> rewrittenSrcTarget =
                    columnRewriter.rewriteSrcTargetWithEc(srcPu.clone(), targetPu.clone(), isQueryToMV);
            ScalarOperator canonizedSrcPu = MvUtils.canonizePredicateForRewrite(
                    queryMaterializationContext, rewrittenSrcTarget.first);
            ScalarOperator canonizedTargetPu = MvUtils.canonizePredicateForRewrite(
                    queryMaterializationContext, rewrittenSrcTarget.second);

            if (isRangePredicate) {
                compensationPu = getCompensationRangePredicate(canonizedSrcPu, canonizedTargetPu);
            } else {
                compensationPu = MvUtils.getCompensationPredicateForDisjunctive(canonizedSrcPu, canonizedTargetPu);
                if (compensationPu == null) {
                    compensationPu = getCompensationResidualPredicate(canonizedSrcPu, canonizedTargetPu);
                    if (compensationPu == null) {
                        return null;
                    }
                }
            }
        }
        return MvUtils.canonizePredicate(compensationPu);
    }

    private ScalarOperator getCompensationResidualPredicate(ScalarOperator srcPu, ScalarOperator targetPu) {
        List<ScalarOperator> srcConjuncts = Utils.extractConjuncts(srcPu);
        List<ScalarOperator> targetConjuncts = Utils.extractConjuncts(targetPu);
        if (new HashSet<>(srcConjuncts).containsAll(targetConjuncts)) {
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
        RangeSimplifier simplifier = new RangeSimplifier(srcPr);
        return simplifier.simplify(targetPr);
    }

    // remove redundant from equivalence

    /**
     * Remove redundant keys from the different equivalence classes. Return true if the diff is empty after pruning.
     */
    private boolean removeRedundantKeysInDiff(EquivalenceClasses ec,
                                              Set<ColumnRefOperator> difference) {
        int size = difference.size();
        Iterator<ColumnRefOperator> it = difference.iterator();
        // skip if diff is redundant
        while (it.hasNext()) {
            ColumnRefOperator next = it.next();
            if (ec.containsRedundantKey(next)) {
                ec.deleteRedundantKey(next);
                it.remove();
                size--;
            }
        }
        return size == 0;
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
        final List<Set<ColumnRefOperator>> sourceEquivalenceClassesList =
                sourceEquivalenceClasses.getEquivalenceClasses();
        final List<Set<ColumnRefOperator>> targetEquivalenceClassesList =
                targetEquivalenceClasses.getEquivalenceClasses();

        // it is a mapping from source to target
        // it may be 1 to n
        final Multimap<Integer, Integer> mapping =
                computeECMapping(sourceEquivalenceClassesList, targetEquivalenceClassesList);
        if (mapping == null) {
            // means that the targetEc can not be contained in sourceEc
            // it means Equal-join subsumption test fails
            return null;
        }
        // compute compensation equality predicate
        // if targetEc equals sourceEc, return true literal, so init to true here
        ScalarOperator compensation = ConstantOperator.createBoolean(true);
        for (int i = 0; i < sourceEquivalenceClassesList.size(); i++) {
            if (!mapping.containsKey(i)) {
                Set<ColumnRefOperator> difference = Sets.newHashSet(sourceEquivalenceClassesList.get(i));

                // remove redundant from equivalence
                if (removeRedundantKeysInDiff(sourceEquivalenceClasses, difference)) {
                    continue;
                }

                // it means that the targetEc do not have the corresponding mapping ec
                // we should all equality predicates between each column in ec into compensation
                Iterator<ColumnRefOperator> it = difference.iterator();
                ColumnRefOperator first = it.next();

                while (it.hasNext()) {
                    ColumnRefOperator next = it.next();
                    ScalarOperator equalPredicate = BinaryPredicateOperator.eq(first, next);
                    compensation = Utils.compoundAnd(compensation, equalPredicate);
                }
            } else {
                // remove columns exists in target and add remain equality predicate in source into compensation
                for (int j : mapping.get(i)) {
                    Set<ColumnRefOperator> difference = Sets.newHashSet(sourceEquivalenceClassesList.get(i));
                    Set<ColumnRefOperator> targetColumnRefs = targetEquivalenceClassesList.get(j);
                    difference.removeAll(targetColumnRefs);
                    if (difference.size() == 0) {
                        continue;
                    }

                    // remove redundant from equivalence
                    if (removeRedundantKeysInDiff(sourceEquivalenceClasses, difference)) {
                        continue;
                    }

                    ScalarOperator targetFirst = targetColumnRefs.iterator().next();
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
                    // once there is a mapping from src -> target, compensations can be done.
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
