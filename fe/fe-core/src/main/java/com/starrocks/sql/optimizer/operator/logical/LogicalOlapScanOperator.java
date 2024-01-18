// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
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
    private final boolean hasHintsPartitionNames;
    private final List<Long> selectedTabletId;
    private final List<Long> hintsTabletIds;
    private final List<ScalarOperator> prunedPartitionPredicates;
<<<<<<< HEAD
=======
    private final boolean usePkIndex;

    // record if this scan is derived from SplitScanORToUnionRule
    private boolean fromSplitOR;
>>>>>>> 2.5.18

    // Only for UT
    public LogicalOlapScanOperator(Table table) {
        this(table, Maps.newHashMap(), Maps.newHashMap(), null, Operator.DEFAULT_LIMIT, null);
    }

    public LogicalOlapScanOperator(
            Table table,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            HashDistributionSpec hashDistributionSpec,
            long limit,
            ScalarOperator predicate) {
        this(table, colRefToColumnMetaMap, columnMetaToColRefMap, hashDistributionSpec, limit, predicate,
                ((OlapTable) table).getBaseIndexId(),
                null,
                null,
                false,
                Lists.newArrayList(),
                Lists.newArrayList(),
                false);
    }

    public LogicalOlapScanOperator(
            Table table,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            HashDistributionSpec hashDistributionSpec,
            long limit,
            ScalarOperator predicate,
            long selectedIndexId,
            List<Long> selectedPartitionId,
            PartitionNames partitionNames,
            boolean hasHintsPartitionNames,
            List<Long> selectedTabletId,
            List<Long> hintsTabletIds,
            boolean usePkIndex) {
        super(OperatorType.LOGICAL_OLAP_SCAN, table, colRefToColumnMetaMap, columnMetaToColRefMap, limit, predicate,
                null);

        Preconditions.checkState(table instanceof OlapTable);
        this.hashDistributionSpec = hashDistributionSpec;
        this.selectedIndexId = selectedIndexId;
        this.selectedPartitionId = selectedPartitionId;
        this.partitionNames = partitionNames;
        this.hasHintsPartitionNames = hasHintsPartitionNames;
        this.selectedTabletId = selectedTabletId;
        this.hintsTabletIds = hintsTabletIds;
        this.prunedPartitionPredicates = Lists.newArrayList();
<<<<<<< HEAD
=======
        this.usePkIndex = usePkIndex;
>>>>>>> 2.5.18
    }

    public LogicalOlapScanOperator(
            Table table,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            HashDistributionSpec hashDistributionSpec,
            long limit,
            ScalarOperator predicate,
            long selectedIndexId,
            List<Long> selectedPartitionId,
            PartitionNames partitionNames,
            List<Long> selectedTabletId,
            List<Long> hintsTabletIds) {
        this(table, colRefToColumnMetaMap, columnMetaToColRefMap, hashDistributionSpec, limit, predicate,
<<<<<<< HEAD
                selectedIndexId, selectedPartitionId, partitionNames, false, selectedTabletId, hintsTabletIds);
=======
                selectedIndexId, selectedPartitionId, partitionNames, false, selectedTabletId, hintsTabletIds, false);
>>>>>>> 2.5.18
    }

    private LogicalOlapScanOperator(Builder builder) {
        super(OperatorType.LOGICAL_OLAP_SCAN, builder.table,
                builder.colRefToColumnMetaMap, builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());
        this.hashDistributionSpec = builder.hashDistributionSpec;
        this.selectedIndexId = builder.selectedIndexId;
        this.selectedPartitionId = builder.selectedPartitionId;
        this.partitionNames = builder.partitionNames;
        this.hasHintsPartitionNames = builder.hasHintsPartitionNames;
        this.selectedTabletId = builder.selectedTabletId;
        this.hintsTabletIds = builder.hintsTabletIds;
        this.prunedPartitionPredicates = builder.prunedPartitionPredicates;
<<<<<<< HEAD
=======
        this.usePkIndex = builder.usePkIndex;
        this.fromSplitOR = builder.fromSplitOR;
>>>>>>> 2.5.18
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

    public boolean hasTabletOrPartitionHints() {
        return (hintsTabletIds != null && !hintsTabletIds.isEmpty()) || hasHintsPartitionNames;
    }

<<<<<<< HEAD
=======
    public boolean isUsePkIndex() {
        return usePkIndex;
    }

>>>>>>> 2.5.18
    public List<ScalarOperator> getPrunedPartitionPredicates() {
        return prunedPartitionPredicates;
    }

<<<<<<< HEAD
=======

    public boolean isFromSplitOR() {
        return fromSplitOR;
    }
>>>>>>> 2.5.18

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapScan(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
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
        return Objects.hash(super.hashCode(), selectedIndexId, selectedPartitionId,
                selectedTabletId, hintsTabletIds);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalOlapScanOperator, LogicalOlapScanOperator.Builder> {
        private HashDistributionSpec hashDistributionSpec;
        private long selectedIndexId;
        private List<Long> selectedPartitionId;
        private PartitionNames partitionNames;

        private boolean hasHintsPartitionNames;
        private List<Long> selectedTabletId;
        private List<Long> hintsTabletIds;
        private List<ScalarOperator> prunedPartitionPredicates;
<<<<<<< HEAD
=======

        private boolean usePkIndex;

        private boolean fromSplitOR;
>>>>>>> 2.5.18

        @Override
        public LogicalOlapScanOperator build() {
            return new LogicalOlapScanOperator(this);
        }

        @Override
        public Builder withOperator(LogicalOlapScanOperator scanOperator) {
            super.withOperator(scanOperator);

            this.hashDistributionSpec = scanOperator.hashDistributionSpec;
            this.selectedIndexId = scanOperator.selectedIndexId;
            this.selectedPartitionId = scanOperator.selectedPartitionId;
            this.partitionNames = scanOperator.partitionNames;
            this.hasHintsPartitionNames = scanOperator.hasHintsPartitionNames;
            this.selectedTabletId = scanOperator.selectedTabletId;
            this.hintsTabletIds = scanOperator.hintsTabletIds;
            this.prunedPartitionPredicates = scanOperator.prunedPartitionPredicates;
<<<<<<< HEAD
=======
            this.usePkIndex = scanOperator.usePkIndex;
>>>>>>> 2.5.18
            return this;
        }

        public Builder setSelectedIndexId(long selectedIndexId) {
            this.selectedIndexId = selectedIndexId;
            return this;
        }

        public Builder setSelectedTabletId(List<Long> selectedTabletId) {
            this.selectedTabletId = selectedTabletId;
            return this;
        }

        public Builder setSelectedPartitionId(List<Long> selectedPartitionId) {
            this.selectedPartitionId = selectedPartitionId;
            return this;
        }

        public Builder setPrunedPartitionPredicates(List<ScalarOperator> prunedPartitionPredicates) {
            this.prunedPartitionPredicates = prunedPartitionPredicates;
            return this;
        }
<<<<<<< HEAD
=======

        public Builder setHashDistributionSpec(HashDistributionSpec hashDistributionSpec) {
            this.hashDistributionSpec = hashDistributionSpec;
            return this;
        }

        public Builder setUsePkIndex(boolean usePkIndex) {
            this.usePkIndex = usePkIndex;
            return this;
        }

        public Builder setFromSplitOR(boolean fromSplitOR) {
            this.fromSplitOR = fromSplitOR;
            return this;
        }
>>>>>>> 2.5.18
    }
}
