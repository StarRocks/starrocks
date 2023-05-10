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

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
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

    private LogicalOlapScanOperator() {
        super(OperatorType.LOGICAL_OLAP_SCAN);
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

    public List<ScalarOperator> getPrunedPartitionPredicates() {
        return prunedPartitionPredicates;
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

        public Builder setHasHintsPartitionNames(boolean hasHintsPartitionNames) {
            builder.hasHintsPartitionNames = hasHintsPartitionNames;
            return this;
        }

        public Builder setHintsTabletIds(List<Long> hintsTabletIds) {
            builder.hintsTabletIds = ImmutableList.copyOf(hintsTabletIds);
            return this;
        }
    }
}
