// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PhysicalOlapScanOperator extends PhysicalScanOperator {
    private final HashDistributionSpec hashDistributionSpec;
    private long selectedIndexId;
    private List<Long> selectedTabletId;
    private List<Long> selectedPartitionId;

    private boolean isPreAggregation;
    private String turnOffReason;

    public PhysicalOlapScanOperator(Table table,
                                    List<ColumnRefOperator> outputColumns,
                                    Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                    HashDistributionSpec hashDistributionDesc) {
        super(OperatorType.PHYSICAL_OLAP_SCAN, table, outputColumns, colRefToColumnMetaMap);
        this.hashDistributionSpec = hashDistributionDesc;
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
}
