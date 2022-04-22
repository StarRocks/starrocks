// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PhysicalOlapScanOperator extends PhysicalScanOperator {
    private final HashDistributionSpec hashDistributionSpec;
    private final long selectedIndexId;
    private final List<Long> selectedTabletId;
    private final List<Long> selectedPartitionId;

    private boolean isPreAggregation;
    private String turnOffReason;

    private List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
    // For the simple predicate k1 = "olap", could apply global dict optimization,
    // need to store the string column k1 and generate the string slot in plan fragment builder
    private List<ColumnRefOperator> globalDictStringColumns = Lists.newArrayList();
    // TODO: remove this
    private Map<Integer, Integer> dictStringIdToIntIds = Maps.newHashMap();

    public PhysicalOlapScanOperator(Table table,
                                    List<ColumnRefOperator> outputColumns,
                                    Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                    HashDistributionSpec hashDistributionDesc,
                                    long limit,
                                    ScalarOperator predicate,
                                    long selectedIndexId,
                                    List<Long> selectedPartitionId,
                                    List<Long> selectedTabletId,
                                    Projection projection) {
        super(OperatorType.PHYSICAL_OLAP_SCAN, table, outputColumns, colRefToColumnMetaMap, limit, predicate,
                projection);
        this.hashDistributionSpec = hashDistributionDesc;
        this.selectedIndexId = selectedIndexId;
        this.selectedPartitionId = selectedPartitionId;
        this.selectedTabletId = selectedTabletId;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public List<Long> getSelectedPartitionId() {
        return selectedPartitionId;
    }

    public List<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public boolean isPreAggregation() {
        return isPreAggregation;
    }

    public void setPreAggregation(boolean preAggregation) {
        isPreAggregation = preAggregation;
    }

    public String getTurnOffReason() {
        return turnOffReason;
    }

    public void setTurnOffReason(String turnOffReason) {
        this.turnOffReason = turnOffReason;
    }

    public List<Pair<Integer, ColumnDict>> getGlobalDicts() {
        return globalDicts;
    }

    public void setGlobalDicts(
            List<Pair<Integer, ColumnDict>> globalDicts) {
        this.globalDicts = globalDicts;
    }

    public List<ColumnRefOperator> getGlobalDictStringColumns() {
        return globalDictStringColumns;
    }

    public void setGlobalDictStringColumns(
            List<ColumnRefOperator> globalDictStringColumns) {
        this.globalDictStringColumns = globalDictStringColumns;
    }

    public Map<Integer, Integer> getDictStringIdToIntIds() {
        return dictStringIdToIntIds;
    }

    public void setDictStringIdToIntIds(Map<Integer, Integer> dictStringIdToIntIds) {
        this.dictStringIdToIntIds = dictStringIdToIntIds;
    }

    @Override
    public String toString() {
        return "PhysicalOlapScan" + " {" +
                "table='" + table.getId() + '\'' +
                ", outputColumns='" + getOutputColumns() + '\'' +
                '}';
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalOlapScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalOlapScan(optExpression, context);
    }

    public HashDistributionSpec getDistributionSpec() {
        // In UT, the distributionInfo may be null
        if (hashDistributionSpec != null) {
            return hashDistributionSpec;
        } else {
            // 1023 is a placeholder column id, only in order to pass UT
            HashDistributionDesc leftHashDesc = new HashDistributionDesc(Collections.singletonList(1023),
                    HashDistributionDesc.SourceType.LOCAL);
            return DistributionSpec.createHashDistributionSpec(leftHashDesc);
        }
    }

    @Override
    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        return true;
    }
}
