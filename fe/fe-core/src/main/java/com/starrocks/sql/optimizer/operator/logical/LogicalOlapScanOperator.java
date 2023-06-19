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

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class LogicalOlapScanOperator extends LogicalScanOperator {
    private DistributionSpec distributionSpec;
    private long selectedIndexId;
    private List<Long> selectedPartitionId;
    private PartitionNames partitionNames;
    private boolean hasTableHints;
    private List<Long> selectedTabletId;
    private List<Long> hintsTabletIds;

    private List<ScalarOperator> prunedPartitionPredicates;
    private boolean usePkIndex;

    // record if this scan is derived from SplitScanORToUnionRule
    private boolean fromSplitOR;

    // Only for UT
    public LogicalOlapScanOperator(Table table) {
        this(table, Maps.newHashMap(), Maps.newHashMap(), null, Operator.DEFAULT_LIMIT, null);
    }

    public LogicalOlapScanOperator(
            Table table,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            DistributionSpec distributionSpec,
            long limit,
            ScalarOperator predicate) {
        this(table, colRefToColumnMetaMap, columnMetaToColRefMap, distributionSpec, limit, predicate,
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
            DistributionSpec distributionSpec,
            long limit,
            ScalarOperator predicate,
            long selectedIndexId,
            List<Long> selectedPartitionId,
            PartitionNames partitionNames,
            boolean hasTableHints,
            List<Long> selectedTabletId,
            List<Long> hintsTabletIds,
            boolean usePkIndex) {
        super(OperatorType.LOGICAL_OLAP_SCAN, table, colRefToColumnMetaMap, columnMetaToColRefMap, limit, predicate,
                null);

        Preconditions.checkState(table instanceof OlapTable);
        this.distributionSpec = distributionSpec;
        this.selectedIndexId = selectedIndexId;
        this.selectedPartitionId = selectedPartitionId;
        this.partitionNames = partitionNames;
        this.hasTableHints = hasTableHints;
        this.selectedTabletId = selectedTabletId;
        this.hintsTabletIds = hintsTabletIds;
        this.prunedPartitionPredicates = Lists.newArrayList();
        this.usePkIndex = usePkIndex;
    }

    private LogicalOlapScanOperator() {
        super(OperatorType.LOGICAL_OLAP_SCAN);
        this.prunedPartitionPredicates = ImmutableList.of();
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
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

    public boolean hasTableHints() {
        return hasTableHints;
    }

    public boolean isUsePkIndex() {
        return usePkIndex;
    }

    public List<ScalarOperator> getPrunedPartitionPredicates() {
        return prunedPartitionPredicates;
    }

    public boolean isFromSplitOR() {
        return fromSplitOR;
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

        if (!super.equals(o)) {
            return false;
        }

        LogicalOlapScanOperator that = (LogicalOlapScanOperator) o;
        return selectedIndexId == that.selectedIndexId &&
                Objects.equals(distributionSpec, that.distributionSpec) &&
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalOlapScanOperator, LogicalOlapScanOperator.Builder> {
        @Override
        protected LogicalOlapScanOperator newInstance() {
            return new LogicalOlapScanOperator();
        }

        @Override
        public Builder withOperator(LogicalOlapScanOperator scanOperator) {
            super.withOperator(scanOperator);

            builder.distributionSpec = scanOperator.distributionSpec;
            builder.selectedIndexId = scanOperator.selectedIndexId;
            builder.selectedPartitionId = scanOperator.selectedPartitionId;
            builder.partitionNames = scanOperator.partitionNames;
            builder.hasTableHints = scanOperator.hasTableHints;
            builder.selectedTabletId = scanOperator.selectedTabletId;
            builder.hintsTabletIds = scanOperator.hintsTabletIds;
            builder.prunedPartitionPredicates = scanOperator.prunedPartitionPredicates;
            builder.usePkIndex = scanOperator.usePkIndex;
            return this;
        }

        public Builder setSelectedIndexId(long selectedIndexId) {
            builder.selectedIndexId = selectedIndexId;
            return this;
        }

        public Builder setSelectedTabletId(List<Long> selectedTabletId) {
            builder.selectedTabletId = ImmutableList.copyOf(selectedTabletId);
            return this;
        }

        public Builder setSelectedPartitionId(List<Long> selectedPartitionId) {
            builder.selectedPartitionId = ImmutableList.copyOf(selectedPartitionId);
            return this;
        }

        public Builder setPrunedPartitionPredicates(List<ScalarOperator> prunedPartitionPredicates) {
            builder.prunedPartitionPredicates = ImmutableList.copyOf(prunedPartitionPredicates);
            return this;
        }

        public Builder setDistributionSpec(DistributionSpec distributionSpec) {
            builder.distributionSpec = distributionSpec;
            return this;
        }

        public Builder setPartitionNames(PartitionNames partitionNames) {
            builder.partitionNames = partitionNames;
            return this;
        }

        public Builder setHintsTabletIds(List<Long> hintsTabletIds) {
            builder.hintsTabletIds = ImmutableList.copyOf(hintsTabletIds);
            return this;
        }

        public Builder setHasTableHints(boolean hasTableHints) {
            builder.hasTableHints = hasTableHints;
            return this;
        }

        public Builder setFromSplitOR(boolean fromSplitOR) {
            builder.fromSplitOR = fromSplitOR;
            return this;
        }

        public Builder setUsePkIndex(boolean usePkIndex) {
            builder.usePkIndex = usePkIndex;
            return this;
        }
    }
}
