// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class LogicalOlapScanOperator extends LogicalScanOperator {
    private final HashDistributionSpec hashDistributionSpec;
    private final long selectedIndexId;
    private final List<Long> selectedPartitionId;
    private final PartitionNames partitionNames;
    private final List<Long> selectedTabletId;
    private final List<Long> hintsTabletIds;

    // Only for UT
    public LogicalOlapScanOperator(Table table) {
        this(table, Lists.newArrayList(), Maps.newHashMap(), Maps.newHashMap(), null, -1, null);
    }

    public LogicalOlapScanOperator(
            Table table,
            List<ColumnRefOperator> outputColumns,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            HashDistributionSpec hashDistributionSpec,
            long limit,
            ScalarOperator predicate) {
        this(table, outputColumns, colRefToColumnMetaMap, columnMetaToColRefMap, hashDistributionSpec, limit, predicate,
                ((OlapTable) table).getBaseIndexId(),
                null,
                null,
                Lists.newArrayList(),
                Lists.newArrayList());
    }

    public LogicalOlapScanOperator(
            Table table,
            List<ColumnRefOperator> outputColumns,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            HashDistributionSpec hashDistributionSpec,
            long limit,
            ScalarOperator predicate,
            long selectedIndexId,
            List<Long> selectedPartitionId,
            PartitionNames partitionNames,
            List<Long> selectedTabletId,
            List<Long> hintsTabletIds
    ) {
        super(OperatorType.LOGICAL_OLAP_SCAN, table, outputColumns,
                colRefToColumnMetaMap, columnMetaToColRefMap, limit, predicate);

        Preconditions.checkState(table instanceof OlapTable);
        this.hashDistributionSpec = hashDistributionSpec;
        this.selectedIndexId = selectedIndexId;
        this.selectedPartitionId = selectedPartitionId;
        this.partitionNames = partitionNames;
        this.selectedTabletId = selectedTabletId;
        this.hintsTabletIds = hintsTabletIds;
    }

    public HashDistributionSpec getDistributionSpec() {
        return hashDistributionSpec;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public List<Long> getSelectedPartitionId() {
        return selectedPartitionId;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public List<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public List<Long> getHintsTabletIds() {
        return hintsTabletIds;
    }

    public boolean canDoReplicatedJoin() {
        return Utils.canDoReplicatedJoin((OlapTable) table, selectedIndexId, selectedPartitionId, selectedTabletId);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapScan(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalOlapScanOperator that = (LogicalOlapScanOperator) o;
        return selectedIndexId == that.selectedIndexId &&
                Objects.equals(hashDistributionSpec, that.hashDistributionSpec) &&
                Objects.equals(selectedPartitionId, that.selectedPartitionId) &&
                Objects.equals(partitionNames, that.partitionNames) &&
                Objects.equals(selectedTabletId, that.selectedTabletId) &&
                Objects.equals(hintsTabletIds, that.hintsTabletIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hashDistributionSpec, selectedIndexId, selectedPartitionId,
                partitionNames,
                selectedTabletId, hintsTabletIds);
    }
}
