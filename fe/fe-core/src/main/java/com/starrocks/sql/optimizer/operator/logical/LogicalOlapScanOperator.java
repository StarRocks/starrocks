// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class LogicalOlapScanOperator extends LogicalScanOperator {
    private long selectedIndexId;
    private Collection<Long> selectedPartitionId;
    private Collection<Long> selectedTabletId;

    private PartitionNames partitionNames;
    private List<Long> hintsTabletIds;

    private final ImmutableMap<Column, Integer> columnToIds;

    // Only for UT
    public LogicalOlapScanOperator(OlapTable table) {
        super(OperatorType.LOGICAL_OLAP_SCAN, table, Lists.newArrayList(), Maps.newHashMap());
        this.columnToIds = ImmutableMap.of();
        selectedPartitionId = Lists.newArrayList();
        selectedIndexId = table.getBaseIndexId();
        selectedTabletId = Lists.newArrayList();
    }

    public LogicalOlapScanOperator(
            OlapTable table,
            List<ColumnRefOperator> outputColumns,
            Map<ColumnRefOperator, Column> columnRefMap,
            ImmutableMap<Column, Integer> columnToIds) {
        super(OperatorType.LOGICAL_OLAP_SCAN, table, outputColumns, columnRefMap);
        this.columnToIds = columnToIds;
        selectedPartitionId = Lists.newArrayList();
        selectedIndexId = table.getBaseIndexId();
        selectedTabletId = Lists.newArrayList();
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

    public ImmutableMap<Column, Integer> getColumnToIds() {
        return columnToIds;
    }

    // TODO(kks): combine this method with PhysicalOlapScan::getDistributionSpec
    public HashDistributionSpec getDistributionSpec() {
        DistributionInfo distributionInfo = ((OlapTable) table).getDefaultDistributionInfo();
        Preconditions.checkState(distributionInfo instanceof HashDistributionInfo);
        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
        List<Column> distributedColumns = hashDistributionInfo.getDistributionColumns();
        List<Integer> columnList = new ArrayList<>();
        for (Column distributedColumn : distributedColumns) {
            columnList.add(columnToIds.get(distributedColumn));
        }

        HashDistributionDesc leftHashDesc =
                new HashDistributionDesc(columnList, HashDistributionDesc.SourceType.LOCAL);
        return DistributionSpec.createHashDistributionSpec(leftHashDesc);
    }

    public boolean canDoReplicatedJoin() {
        return Utils.canDoReplicatedJoin((OlapTable) table, selectedIndexId, selectedPartitionId, selectedTabletId);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapScan(this, context);
    }
}
