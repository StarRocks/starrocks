// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
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

    private final Set<String> mvPartitionNamesToRefresh;

    private final List<Table> baseTables;

    private final Set<ColumnRefOperator> originQueryColumns;
<<<<<<< HEAD

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
=======

    // tables both in query and mv
    private final List<Table> intersectingTables;

    // group ids that are rewritten by this mv
    // used to reduce the rewrite times for the same group and mv
    private List<Integer> matchedGroups;

    private final ScalarOperator mvPartialPartitionPredicate;
>>>>>>> branch-2.5-mrs

    public MaterializationContext(OptimizerContext optimizerContext,
                                  MaterializedView mv,
                                  OptExpression mvExpression,
                                  ColumnRefFactory queryColumnRefFactory,
                                  ColumnRefFactory mvColumnRefFactory,
                                  Set<String> mvPartitionNamesToRefresh,
                                  List<Table> baseTables,
                                  Set<ColumnRefOperator> originQueryColumns,
                                  List<Table> intersectingTables,
                                  ScalarOperator mvPartialPartitionPredicate) {
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
<<<<<<< HEAD

    public long getMVUsedCount() {
        return mvUsedCount;
    }

    public void updateMVUsedCount() {
        this.mvUsedCount += 1;
    }
=======
>>>>>>> branch-2.5-mrs
}
