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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    // Updated partition names of the materialized view
    private final Set<String> mvPartitionNamesToRefresh;

    // Updated partition names of the ref base table which will be used in compensating partition predicates
    private final Set<String> refTableUpdatePartitionNames;

    private final List<Table> baseTables;

    private final Set<ColumnRefOperator> originQueryColumns;

    // tables both in query and mv
    private final List<Table> intersectingTables;

    // group ids that are rewritten by this mv
    // used to reduce the rewrite times for the same group and mv
    private List<Integer> matchedGroups;

    private final ScalarOperator mvPartialPartitionPredicate;

    // THe used count for MV used as the rewrite result in a query.
    // NOTE: mvUsedCount is a not exact value because MV may be rewritten multi times
    // in Optimizer Transformation phase but not be really used.
    private long mvUsedCount = 0;

    //// Caches for the mv rewrite lifecycle

    // Cache whether to need to compensate partition predicates or not, and it's not changed
    // during one query, so it's safe to cache it and be used for each optimizer rule.
    // But it is different for each materialized view, compensate partition predicate from the plan's
    // `selectedPartitionIds`, and check `isNeedCompensatePartitionPredicate` to get more information.
    private Optional<Boolean> isCompensatePartitionPredicateOpt = Optional.empty();
    // Cache partition compensates predicates for each ScanNode and isCompensate pair.
    private Map<Pair<LogicalScanOperator, Boolean>, List<ScalarOperator>> scanOpToPartitionCompensatePredicates;

    public MaterializationContext(OptimizerContext optimizerContext,
                                  MaterializedView mv,
                                  OptExpression mvExpression,
                                  ColumnRefFactory queryColumnRefFactory,
                                  ColumnRefFactory mvColumnRefFactory,
                                  Set<String> mvPartitionNamesToRefresh,
                                  List<Table> baseTables,
                                  Set<ColumnRefOperator> originQueryColumns,
                                  List<Table> intersectingTables,
                                  ScalarOperator mvPartialPartitionPredicate,
                                  Set<String> refTableUpdatePartitionNames) {
        this.optimizerContext = optimizerContext;
        this.mv = mv;
        this.mvExpression = mvExpression;
        this.queryRefFactory = queryColumnRefFactory;
        this.mvColumnRefFactory = mvColumnRefFactory;
        this.mvPartitionNamesToRefresh = mvPartitionNamesToRefresh;
        this.baseTables = baseTables;
        this.originQueryColumns = originQueryColumns;
        this.intersectingTables = intersectingTables;
        this.matchedGroups = Lists.newArrayList();
        this.mvPartialPartitionPredicate = mvPartialPartitionPredicate;
        this.refTableUpdatePartitionNames = refTableUpdatePartitionNames;
        this.scanOpToPartitionCompensatePredicates = Maps.newHashMap();
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

    public Set<String> getMvPartitionNamesToRefresh() {
        return mvPartitionNamesToRefresh;
    }

    public List<Table> getBaseTables() {
        return baseTables;
    }

    public boolean hasMultiTables() {
        return baseTables != null && baseTables.size() > 1;
    }

    public Set<ColumnRefOperator> getOriginQueryColumns() {
        return originQueryColumns;
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

    public ScalarOperator getMvPartialPartitionPredicate() {
        return mvPartialPartitionPredicate;
    }

    public long getMVUsedCount() {
        return mvUsedCount;
    }

    public void updateMVUsedCount() {
        this.mvUsedCount += 1;
    }

    public Set<String> getRefTableUpdatePartitionNames() {
        return this.refTableUpdatePartitionNames;
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
    public boolean getOrInitCompensatePartitionPredicate(OptExpression queryExpression) {
        if (!isCompensatePartitionPredicateOpt.isPresent()) {
            SessionVariable sessionVariable = optimizerContext.getSessionVariable();
            // only set this when `queryExpression` contains ref table, otherwise the cached value maybe dirty.
            isCompensatePartitionPredicateOpt = sessionVariable.isEnableMaterializedViewRewritePartitionCompensate() ?
                    MvUtils.isNeedCompensatePartitionPredicate(queryExpression, this) : Optional.of(false);
        }
        return isCompensatePartitionPredicateOpt.orElse(true);
    }

    public boolean isCompensatePartitionPredicate() {
        return isCompensatePartitionPredicateOpt.orElse(true);
    }

    public Map<Pair<LogicalScanOperator, Boolean>, List<ScalarOperator>> getScanOpToPartitionCompensatePredicates() {
        return scanOpToPartitionCompensatePredicates;
    }
}
