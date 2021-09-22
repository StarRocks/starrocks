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

import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class LogicalOlapScanOperator extends LogicalScanOperator {
    private final HashDistributionSpec hashDistributionSpec;
    private long selectedIndexId;
    private Collection<Long> selectedTabletId;
    private Collection<Long> selectedPartitionId;

    private PartitionNames partitionNames;
    private List<Long> hintsTabletIds;

    // Only for UT
    public LogicalOlapScanOperator(Table table) {
        this(table, Lists.newArrayList(), Maps.newHashMap(), null, -1, null);
    }

    public LogicalOlapScanOperator(
            Table table,
            List<ColumnRefOperator> outputColumns,
            Map<ColumnRefOperator, Column> columnRefMap,
            HashDistributionSpec hashDistributionSpec,
            long limit,
            ScalarOperator predicate) {
        super(OperatorType.LOGICAL_OLAP_SCAN, table, outputColumns, columnRefMap, limit, predicate);

        Preconditions.checkState(table instanceof OlapTable);
        this.hashDistributionSpec = hashDistributionSpec;
        selectedIndexId = ((OlapTable) table).getBaseIndexId();
        selectedTabletId = Lists.newArrayList();
        selectedPartitionId = Lists.newArrayList();
    }

    public OlapTable getOlapTable() {
        return (OlapTable) table;
    }

    public Collection<Long> getSelectedPartitionId() {
        return selectedPartitionId;
    }

    public void setSelectedPartitionId(Collection<Long> selectedPartitionId) {
        this.selectedPartitionId = selectedPartitionId;
    }

    public List<Long> getHintsTabletIds() {
        return hintsTabletIds;
    }

    public void setHintsTabletIds(List<Long> hintsTabletIds) {
        this.hintsTabletIds = hintsTabletIds;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(PartitionNames partitionNames) {
        this.partitionNames = partitionNames;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public void setSelectedIndexId(long selectedIndexId) {
        this.selectedIndexId = selectedIndexId;
    }

    public Collection<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public void setSelectedTabletId(List<Long> selectedTabletId) {
        this.selectedTabletId = selectedTabletId;
    }

    public HashDistributionSpec getDistributionSpec() {
        return hashDistributionSpec;
    }

    public boolean canDoReplicatedJoin() {
        return Utils.canDoReplicatedJoin((OlapTable) table, selectedIndexId, selectedPartitionId, selectedTabletId);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapScan(this, context);
    }
}
