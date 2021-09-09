// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PhysicalOlapScanOperator extends PhysicalOperator {
    private final List<ColumnRefOperator> outputColumns;
    private final Map<ColumnRefOperator, Column> columnRefMap;
    private final ImmutableMap<Column, Integer> columnToIds;
    private OlapTable table;

    private long selectedIndexId;
    private List<Long> selectedPartitionId;
    private List<Long> selectedTabletId;

    private boolean isPreAggregation;
    private String turnOffReason;

    public PhysicalOlapScanOperator(OlapTable table,
                                    List<ColumnRefOperator> outputColumns,
                                    Map<ColumnRefOperator, Column> columnRefMap,
                                    ImmutableMap<Column, Integer> columnToIds) {
        super(OperatorType.PHYSICAL_OLAP_SCAN);
        this.table = table;
        this.outputColumns = outputColumns;
        this.columnRefMap = columnRefMap;
        this.columnToIds = columnToIds;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return outputColumns;
    }

    public Map<ColumnRefOperator, Column> getColumnRefMap() {
        return columnRefMap;
    }

    public ImmutableMap<Column, Integer> getColumnToIds() {
        return columnToIds;
    }

    public void setTable(OlapTable table) {
        this.table = table;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public void setSelectedIndexId(long selectedIndexId) {
        this.selectedIndexId = selectedIndexId;
    }

    public List<Long> getSelectedPartitionId() {
        return selectedPartitionId;
    }

    public void setSelectedPartitionId(List<Long> selectedPartitionId) {
        this.selectedPartitionId = selectedPartitionId;
    }

    public List<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public void setSelectedTabletId(List<Long> selectedTabletId) {
        this.selectedTabletId = selectedTabletId;
    }

    public OlapTable getTable() {
        return table;
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

    @Override
    public int hashCode() {
        int hash = 17;
        hash = Utils.combineHash(hash, opType.hashCode());
        hash = Utils.combineHash(hash, (int) table.getId());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PhysicalOlapScanOperator)) {
            return false;
        }

        PhysicalOlapScanOperator rhs = (PhysicalOlapScanOperator) obj;
        if (this == rhs) {
            return true;
        }

        return table.getId() == rhs.getTable().getId() &&
                outputColumns.equals(rhs.outputColumns);

    }

    @Override
    public String toString() {
        return "PhysicalOlapScan" + " {" +
                "table='" + table.getId() + '\'' +
                ", outputColumns='" + outputColumns + '\'' +
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

    // TODO(kks): combine this method with LogicalOlapScanOperator::getDistributionSpec
    public HashDistributionSpec getDistributionSpec() {
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        // In UT, the distributionInfo may be null
        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            List<Column> distributedColumns = hashDistributionInfo.getDistributionColumns();
            List<Integer> columnList = new ArrayList<>();
            for (Column distributedColumn : distributedColumns) {
                columnList.add(columnToIds.get(distributedColumn));
            }

            HashDistributionDesc leftHashDesc = new HashDistributionDesc(columnList,
                    HashDistributionDesc.SourceType.LOCAL);
            return DistributionSpec.createHashDistributionSpec(leftHashDesc);
        } else {
            // 1023 is a placeholder column id, only in order to pass UT
            HashDistributionDesc leftHashDesc = new HashDistributionDesc(Collections.singletonList(1023),
                    HashDistributionDesc.SourceType.LOCAL);
            return DistributionSpec.createHashDistributionSpec(leftHashDesc);
        }
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        outputColumns.forEach(set::union);
        columnRefMap.keySet().forEach(set::union);
        return set;
    }
}
