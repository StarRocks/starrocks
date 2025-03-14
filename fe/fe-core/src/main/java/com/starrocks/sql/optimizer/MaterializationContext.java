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

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.MultiOpPattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.TableScanDesc;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.MVCompensation;
import org.apache.commons.collections4.SetUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.catalog.TableProperty.QueryRewriteConsistencyMode.CHECKED;
import static com.starrocks.sql.common.TimeUnitUtils.DATE_TRUNC_SUPPORTED_TIME_MAP;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

public class MaterializationContext {
    private final MaterializedView mv;
    // scan materialized view operator
    private LogicalOlapScanOperator scanMvOperator;
    // logical OptExpression for query of materialized view
    private final OptExpression mvExpression;

    private final ColumnRefFactory mvColumnRefFactory;

    private final ColumnRefFactory queryRefFactory;

    private final OptimizerContext optimizerContext;

    private Map<ColumnRefOperator, ColumnRefOperator> outputMapping;

    // Updated partition names of the ref base table which will be used in compensating partition predicates
    private final MvUpdateInfo mvUpdateInfo;

    private final List<Table> baseTables;

    // tables both in query and mv
    private final List<Table> intersectingTables;

    // group ids that are rewritten by this mv
    // used to reduce the rewrite times for the same group and mv
    private final List<Integer> matchedGroups;

    // The output column refs of the MV which is ordered by user's select list.
    private final List<ColumnRefOperator> mvOutputColumnRefs;

    // THe used count for MV used as the rewrite result in a query.
    // NOTE: mvUsedCount is a not exact value because MV may be rewritten multi times
    // in Optimizer Transformation phase but not be really used.
    private long mvUsedCount = 0;

    //// Caches for the mv rewrite lifecycle

    // Cache whether to need to compensate partition predicates or not, and it's not changed
    // during one query, so it's safe to cache it and be used for each optimizer rule.
    // But it is different for each materialized view, compensate partition predicate from the plan's
    // `selectedPartitionIds`, and check `isNeedCompensatePartitionPredicate` to get more information.
    private MVCompensation mvCompensation = null;

    // Cache partition compensates predicates for each ScanNode and isCompensate pair.
    private Map<LogicalScanOperator, List<ScalarOperator>> scanOpToPartitionCompensatePredicates;
    private final int level;

    public MaterializationContext(OptimizerContext optimizerContext,
                                  MaterializedView mv,
                                  OptExpression mvExpression,
                                  ColumnRefFactory queryColumnRefFactory,
                                  ColumnRefFactory mvColumnRefFactory,
                                  List<Table> baseTables,
                                  List<Table> intersectingTables,
                                  MvUpdateInfo mvUpdateInfo,
                                  List<ColumnRefOperator> mvOutputColumnRefs,
                                  int level) {
        this.optimizerContext = optimizerContext;
        this.mv = mv;
        this.mvExpression = mvExpression;
        this.queryRefFactory = queryColumnRefFactory;
        this.mvColumnRefFactory = mvColumnRefFactory;
        this.baseTables = baseTables;
        this.intersectingTables = intersectingTables;
        this.matchedGroups = Lists.newArrayList();
        this.mvUpdateInfo = mvUpdateInfo;
        this.mvOutputColumnRefs = mvOutputColumnRefs;
        this.scanOpToPartitionCompensatePredicates = Maps.newHashMap();
        this.level = level;
    }

    public MaterializedView getMv() {
        return mv;
    }

    public LogicalOlapScanOperator getScanMvOperator() {
        return scanMvOperator;
    }

    public void setScanMvOperator(LogicalOlapScanOperator scanMvOperator) {
        this.scanMvOperator = scanMvOperator;
    }

    public OptExpression getMvExpression() {
        return mvExpression;
    }

    public ColumnRefFactory getMvColumnRefFactory() {
        return mvColumnRefFactory;
    }

    public ColumnRefFactory getQueryRefFactory() {
        return queryRefFactory;
    }

    public OptimizerContext getOptimizerContext() {
        return optimizerContext;
    }

    public Map<ColumnRefOperator, ColumnRefOperator> getOutputMapping() {
        return outputMapping;
    }

    public void setOutputMapping(Map<ColumnRefOperator, ColumnRefOperator> outputMapping) {
        this.outputMapping = outputMapping;
    }

    public List<Table> getBaseTables() {
        return baseTables;
    }

    public boolean hasMultiTables() {
        return baseTables != null && baseTables.size() > 1;
    }

    public boolean isSingleTable() {
        return baseTables != null && baseTables.size() == 1;
    }

    public List<Table> getIntersectingTables() {
        return intersectingTables;
    }

    public void addMatchedGroup(int matchedGroupId) {
        matchedGroups.add(matchedGroupId);
    }

    public boolean isMatchedGroup(int groupId) {
        return matchedGroups.contains(groupId);
    }

    public long getMVUsedCount() {
        return mvUsedCount;
    }

    // whether this mv has been used multi times
    public boolean isMvDuplicateUsed() {
        return mvUsedCount > 0;
    }

    public void updateMVUsedCount() {
        this.mvUsedCount += 1;
    }

    public MvUpdateInfo getMvUpdateInfo() {
        return mvUpdateInfo;
    }

    private boolean checkOperatorCompatible(OperatorType query) {
        // Prune based on query operator
        if (query == OperatorType.LOGICAL_AGGR) {
            return MvUtils.isLogicalSPJG(mvExpression);
        }
        return MvUtils.isLogicalSPJ(mvExpression);
    }

    public List<ColumnRefOperator> getMvOutputColumnRefs() {
        return mvOutputColumnRefs;
    }

    public int getLevel() {
        return level;
    }

    /**
     * Try to prune this MV during MV rewrite
     *
     * @return false if this MV is not applicable
     */
    public boolean prune(OptimizerContext ctx, OptExpression queryExpression) {
        final String mvName = getMv().getName();
        final OptExpression mvExpression = getMvExpression();
        final List<Table> queryTables = MvUtils.getAllTables(queryExpression);
        final List<Table> mvTables = getBaseTables();
        final OperatorType queryOp = queryExpression.getOp().getOpType();

        // if a query has been applied this mv, return false directly.
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        if (scanOperators.stream().anyMatch(op -> op.isOpAppliedMV(mv.getId()))) {
            return false;
        }

        if (!checkOperatorCompatible(queryOp)) {
            return false;
        }

        MaterializedViewRewriter.MatchMode matchMode = MaterializedViewRewriter.getMatchMode(queryTables, mvTables);
        // Only care MatchMode.COMPLETE and VIEW_DELTA here, QUERY_DELTA also can be supported
        // because optimizer will match MV's pattern which is subset of query opt tree
        // from top-down iteration.
        if (matchMode == MaterializedViewRewriter.MatchMode.COMPLETE) {
            // Q  : A JOIN B JOIN C JOIN D
            // MV : A JOIN B JOIN C
            // To fast rewrite, only need to check `A JOIN B JOIN C` pattern rather than
            // `A JOIN B JOIN C JOIN D`.
            if (!optimizerContext.getSessionVariable().isEnableMaterializedViewRewriteGreedyMode()) {
                for (OptExpression child : queryExpression.getInputs()) {
                    final List<Table> childTables = MvUtils.getAllTables(child);
                    if (Sets.newHashSet(childTables).contains(mvTables)) {
                        logMVRewrite(mvName, "MV is pruned since subjoin could be rewritten");
                        return false;
                    }
                }
            }
        } else if (matchMode == MaterializedViewRewriter.MatchMode.VIEW_DELTA) {
            if (!optimizerContext.getSessionVariable().isEnableMaterializedViewViewDeltaRewrite()) {
                return false;
            }
            // only consider query with most common tables to optimize performance
            // To avoid join reorder producing plan bomb, record query's max tables to be only matched.
            // But if query contains non inner/left outer joins which cannot be used to view delta join,
            // not use `intersectingTables` anymore.
            if (!optimizerContext.getSessionVariable().isEnableMaterializedViewRewriteGreedyMode() &&
                    !new HashSet<>(queryTables).containsAll(getIntersectingTables())) {
                return false;
            }

            if (!MvUtils.isSupportViewDelta(queryExpression)) {
                logMVRewrite(mvName, "MV is not applicable in view delta mode: " +
                        "only support inner/left outer join type for now");
                return false;
            }

            List<TableScanDesc> queryTableScanDescs = MvUtils.getTableScanDescs(queryExpression, queryRefFactory);
            List<TableScanDesc> mvTableScanDescs = MvUtils.getTableScanDescs(mvExpression, mvColumnRefFactory);
            // there should be at least one same join type in mv scan descs for every query scan desc.
            // to forbid rewrite for:
            // query: a left outer join b
            // mv: a inner join b inner join c
            for (TableScanDesc queryScanDesc : queryTableScanDescs) {
                if (queryScanDesc.getJoinOptExpression() != null
                        && mvTableScanDescs.stream().noneMatch(scanDesc -> scanDesc.isCompatible(queryScanDesc))) {
                    logMVRewrite(mvName, "MV is not applicable in view delta mode: " +
                            "at least one same join type should be existed");
                    return false;
                }
            }
        } else {
            return false;
        }

        // If table lists do not intersect, can not be rewritten
        if (Collections.disjoint(queryTables, mvTables)) {
            logMVRewrite(mvName, "MV is not applicable: query tables are disjoint with mvs' tables");
            return false;
        }
        return true;
    }

    public static class RewriteOrdering implements Comparator<MaterializationContext> {

        private static final int LOWEST_ORDERING = 100;
        private final OptExpression query;
        private Set<String> queryDimensionNames;

        public RewriteOrdering(OptExpression query, ColumnRefFactory factory) {
            this.query = query;
            resolveAggregation(query);
        }

        private void resolveAggregation(OptExpression query) {
            if (query.getOp().getOpType() == OperatorType.LOGICAL_AGGR) {
                LogicalAggregationOperator queryAgg = (LogicalAggregationOperator) query.getOp();
                List<ColumnRefOperator> quDimensions = Lists.newArrayList(queryAgg.getGroupingKeys());
                quDimensions.addAll(MvUtils.getPredicateColumns(this.query));
                this.queryDimensionNames =
                        quDimensions.stream().map(ColumnRefOperator::getName).collect(Collectors.toSet());
            }
        }

        private static int getOperatorOrdering(OperatorType op) {
            if (op == OperatorType.LOGICAL_AGGR) {
                return 1;
            } else if (op == OperatorType.LOGICAL_JOIN) {
                return 2;
            } else if (MultiOpPattern.ALL_SCAN_TYPES.contains(op)) {
                return 3;
            } else {
                return 4;
            }
        }

        /**
         * Prefer MV with similar dimensions
         */
        private int orderingAggregation(MaterializationContext mv) {
            if (mv.getMvExpression().getOp().getOpType() == OperatorType.LOGICAL_AGGR) {
                // TODO: consider moving the dimension extraction to MV prepare
                LogicalAggregationOperator aggregation = (LogicalAggregationOperator) mv.getMvExpression().getOp();
                List<ColumnRefOperator> mvDimensions = aggregation.getGroupingKeys();
                Set<String> mvDimensionNames =
                        mvDimensions.stream().map(ColumnRefOperator::getName).collect(Collectors.toSet());

                if (this.queryDimensionNames.isEmpty()) {
                    return 0;
                } else if (mvDimensionNames.equals(queryDimensionNames)) {
                    // MV dimensions equal to query dimensions
                    return 0;
                } else if (mvDimensionNames.containsAll(queryDimensionNames)) {
                    // MV dimensions contains all query dimensions
                    return SetUtils.difference(mvDimensionNames, queryDimensionNames).size();
                } else {
                    // Could not cover, usually it could not rewrite
                    return mvDimensionNames.size() + queryDimensionNames.size();
                }
            }
            return LOWEST_ORDERING;
        }

        /**
         * If mv is partitioned by time, prefer the one with the larger time granularity.
         * eg: mv partitioned by date_trunc('day', ts) is preferred over mv partitioned by date_trunc('hour', ts)
         */
        private int orderingTimeGranularity(MaterializationContext materializationContext) {
            if (materializationContext.getMvExpression().getOp().getOpType() == OperatorType.LOGICAL_AGGR) {
                MaterializedView mv = materializationContext.getMv();
                PartitionInfo partitionInfo = mv.getPartitionInfo();
                if (!partitionInfo.isRangePartition()) {
                    return LOWEST_ORDERING;
                }
                Optional<Expr> mvPartitionExprOpt = mv.getRangePartitionFirstExpr();
                if (mvPartitionExprOpt.isEmpty()) {
                    return LOWEST_ORDERING;
                }
                Expr mvPartitionExpr = mvPartitionExprOpt.get();
                if (mvPartitionExpr == null || !(mvPartitionExpr instanceof FunctionCallExpr)) {
                    return LOWEST_ORDERING;
                }
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) mvPartitionExpr;
                if (!functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                    return LOWEST_ORDERING;
                }
                StringLiteral timeUnit = (StringLiteral) mvPartitionExpr.getChild(0);
                if (timeUnit == null) {
                    return LOWEST_ORDERING;
                }
                Integer priority = DATE_TRUNC_SUPPORTED_TIME_MAP.get(timeUnit.getStringValue());
                if (priority == null) {
                    return LOWEST_ORDERING;
                }
                return -priority;
            } else {
                return LOWEST_ORDERING;
            }
        }

        /**
         * Prefer exact-intersecting than partial-intersecting
         */
        private static int orderingIntersectTables(MaterializationContext mvContext) {
            return Math.abs(mvContext.getIntersectingTables().size() - mvContext.getBaseTables().size());
        }

        /**
         * Prefer small table to large table
         */
        private static long orderingRowCount(MaterializationContext mvContext) {
            // prefer max partition row count to mv's total row count,
            // eg: a mv is with less partitions and smaller total row count but maxPartitionRowCount is larger.
            return mvContext.getMv().getMaxPartitionRowCount();
        }

        @Override
        public int compare(MaterializationContext o1, MaterializationContext o2) {
            OperatorType o1Type = o1.getMvExpression().getOp().getOpType();
            OperatorType o2Type = o2.getMvExpression().getOp().getOpType();

            if (o1Type == o2Type && (o1Type == OperatorType.LOGICAL_AGGR)) {
                // -- rows: 100 mv1:
                // select sum(v1) from t1 group by a, b, c, f;
                // -- rows: 10000 mv2:
                // select sum(v1) from t1 group by a, b, d;
                // query: select sum(v1) from t1 where b = 'a' group by a;

                // When many mvs satisfy query, prefer mv with fewer rows, like mv1.
                // But `RewriteOrdering` is only used for sorting when there are too many candidate mvs
                // and candidate mv list need to be trimmed to a limited size.
                // So `mv1` in above example is just a candidate, it doesn't mean mv1 is the final chosen mv.
                // Actually `BestMvSelector` is the final place to judge which mv is used after rewrite rule.
                boolean mvHasDifferentRows = orderingRowCount(o1) != 0 && orderingRowCount(o2) != 0
                        && orderingRowCount(o1) != orderingRowCount(o2);
                return Comparator
                        .comparing((Function<MaterializationContext, Long>) mv -> {
                            int r = orderingAggregation(mv);
                            if (r > 0 && mvHasDifferentRows) {
                                return orderingRowCount(mv);
                            }
                            return (long) r;
                        })
                        .thenComparing(RewriteOrdering::orderingRowCount)
                        .thenComparing(MaterializationContext::getMVUsedCount)
                        .thenComparing(this::orderingTimeGranularity)
                        .compare(o1, o2);
            } else if (o1Type == o2Type && o1Type == OperatorType.LOGICAL_JOIN) {
                return Comparator.comparing(RewriteOrdering::orderingIntersectTables)
                        .thenComparing(RewriteOrdering::orderingRowCount)
                        .thenComparing(MaterializationContext::getMVUsedCount)
                        .compare(o1, o2);
            } else {
                return Comparator.comparing(((MaterializationContext x) ->
                                getOperatorOrdering(x.getMvExpression().getOp().getOpType())))
                        .thenComparing(RewriteOrdering::orderingRowCount)
                        .thenComparing(MaterializationContext::getMVUsedCount)
                        .compare(o1, o2);
            }
        }
    }

    /**
     * What's the meaning of partition compensate in mv rewriting?
     * <p>
     * Because `PartitionPruner` will prune predicates associated with partition columns before mv rewrite,
     * so we cannot use both current query/mv plan's predicates for rewrite, otherwise it may generate
     * wrong rewrite results because of loss of pruned predicates.
     * </p>
     *
     * <p>
     *There are two choices to recover partition predicates from the current pruned plan:
     *Method 1:
     *  Because `PartitionPruner` keeps partitions to scan in `selectedPartitionIds` after complex pruning rules,
     *  so use `selectedPartitionIds` to compensate complete partition ranges with lower and upper bound directly.
     *
     * Method 2:
     *  `PartitionPruner` also will keep original predicates before partition pruning, so we can use original
     *  pruned partition predicates as the compensated partition predicates directly.
     *</p>
     *
     * <p>
     * Method1(default) takes advantage of `PartitionPruner`'s prune abilities and deduces a new normalized partition
     * predicate, but it may generate some redundant or noisy partition predicates to disturb rewriting.
     *
     * Method2(preferred) doesn't use the advantage of `PartitionPruner`'s prune abilities and treats query and mv's original
     * queries to compare/rewrite, and it will simplify the rewrite routine.
     * And also, MV rewrite's abilities should cover `PartitionPruner` and no use Method1 either.
     * </p>
     *
     * <p>
     * What's the current strategy to handle this?
     *  Method1 is used by default to avoid breaking compatibility, but optimize it when MV is no need to compensate which
     *  means MV's partitions can cover all needed partitions from Query.
     * </p>
     */
    public MVCompensation getOrInitMVCompensation(OptExpression queryExpression) {
        if (mvCompensation == null) {
            // only set this when `queryExpression` contains ref table, otherwise the cached value maybe dirty.
            this.mvCompensation = MvPartitionCompensator.getMvCompensation(queryExpression, this);
            logMVRewrite(mv.getName(), "MV compensation: {}", mvCompensation);
        }
        return this.mvCompensation;
    }

    public MVCompensation getMvCompensation() {
        Preconditions.checkArgument(mvCompensation != null,
                "MV compensation should be initialized before used");
        return mvCompensation;
    }

    /**
     * Check the mv context can be used for rewrite:
     * - if mv compensation's state is no rewrite, return false
     * - if mv compensation's state is unkwown & check mode is checked, return false
     * - otherwise return true.
     */
    public boolean isNoRewrite() {
        Preconditions.checkArgument(mvCompensation != null,
                "MV compensation should be initialized before used");
        if (mvCompensation.getState().isNoRewrite()) {
            return true;
        }
        if (mvUpdateInfo.getQueryRewriteConsistencyMode() == CHECKED && mvCompensation.getState().isUnknown()) {
            return true;
        }
        return false;
    }

    public Map<LogicalScanOperator, List<ScalarOperator>> getScanOpToPartitionCompensatePredicates() {
        return scanOpToPartitionCompensatePredicates;
    }
}
