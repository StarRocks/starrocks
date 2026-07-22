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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionNames;
import com.starrocks.catalog.Table;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.changes.ChangesMetaDescriptor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class LogicalChangesScanOperator extends LogicalScanOperator {

    private Bookmark base;
    private Bookmark head;
    private BookmarkChange delta;
    // CHANGES metadata descriptors for this relation.
    private List<ChangesMetaDescriptor> changesMetaDescriptors = List.of();
    // User-written PARTITION(...) / TABLET(...) hints on the table reference that narrow the scan,
    // kept verbatim until partition/tablet pruning consumes them; Optional.empty() means the hint was not given.
    private Optional<PartitionNames> partitionNameHints = Optional.empty();
    private Optional<List<Long>> tabletIdHints = Optional.empty();
    // The table's distribution, captured during transformation and carried through implementation
    // onto the physical scan, which advertises it as an output distribution property.
    private DistributionSpec distributionSpec;

    // Selected logical-partition ids after partition pruning on the delta;
    // null means no pruning was applied, so all delta partitions are scanned.
    private List<Long> selectedLogicalPartitionId;
    // Selected tablet ids after tablet pruning; null means all tablets in the selected partitions.
    private List<Long> selectedTabletId;

    public LogicalChangesScanOperator(Table table,
                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                      Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                      Bookmark base,
                                      Bookmark head,
                                      BookmarkChange delta,
                                      long limit,
                                      List<ChangesMetaDescriptor> changesMetaDescriptors) {
        super(OperatorType.LOGICAL_CHANGES_SCAN, table,
                colRefToColumnMetaMap, columnMetaToColRefMap, limit, null, null);
        this.base = base;
        this.head = head;
        this.delta = delta;
        this.changesMetaDescriptors = changesMetaDescriptors;
    }

    private LogicalChangesScanOperator() {
        super(OperatorType.LOGICAL_CHANGES_SCAN);
    }

    public Bookmark getBase() {
        return base;
    }

    public Bookmark getHead() {
        return head;
    }

    public BookmarkChange getDelta() {
        return delta;
    }

    public List<ChangesMetaDescriptor> getChangesMetaDescriptors() {
        return changesMetaDescriptors;
    }

    public List<Long> getSelectedLogicalPartitionId() {
        return selectedLogicalPartitionId;
    }

    public List<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public Optional<PartitionNames> getPartitionNameHints() {
        return partitionNameHints;
    }

    public Optional<List<Long>> getTabletIdHints() {
        return tabletIdHints;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalChangesScanOperator that = (LogicalChangesScanOperator) o;
        return base.getBookmarkId() == that.base.getBookmarkId()
                && head.getBookmarkId() == that.head.getBookmarkId()
                && Objects.equals(changesMetaDescriptors, that.changesMetaDescriptors)
                && Objects.equals(partitionNameHints, that.partitionNameHints)
                && Objects.equals(tabletIdHints, that.tabletIdHints)
                && Objects.equals(selectedLogicalPartitionId, that.selectedLogicalPartitionId)
                && Objects.equals(selectedTabletId, that.selectedTabletId)
                && Objects.equals(distributionSpec, that.distributionSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), base.getBookmarkId(), head.getBookmarkId(),
                changesMetaDescriptors, partitionNameHints, tabletIdHints,
                selectedLogicalPartitionId, selectedTabletId, distributionSpec);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalChangesScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalTableScan(optExpression, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalChangesScanOperator, Builder> {

        @Override
        protected LogicalChangesScanOperator newInstance() {
            return new LogicalChangesScanOperator();
        }

        @Override
        public Builder withOperator(LogicalChangesScanOperator operator) {
            super.withOperator(operator);
            builder.base = operator.base;
            builder.head = operator.head;
            builder.delta = operator.delta;
            builder.changesMetaDescriptors = operator.changesMetaDescriptors;
            builder.partitionNameHints = operator.partitionNameHints;
            builder.tabletIdHints = operator.tabletIdHints;
            builder.selectedLogicalPartitionId = operator.selectedLogicalPartitionId;
            builder.selectedTabletId = operator.selectedTabletId;
            builder.distributionSpec = operator.distributionSpec;
            return this;
        }

        public Builder setDistributionSpec(DistributionSpec distributionSpec) {
            builder.distributionSpec = distributionSpec;
            return this;
        }

        public Builder setPartitionNameHints(PartitionNames partitionNameHints) {
            builder.partitionNameHints = Optional.ofNullable(partitionNameHints);
            return this;
        }

        public Builder setTabletIdHints(List<Long> tabletIdHints) {
            builder.tabletIdHints = (tabletIdHints == null || tabletIdHints.isEmpty())
                    ? Optional.empty() : Optional.of(ImmutableList.copyOf(tabletIdHints));
            return this;
        }

        public Builder setSelectedLogicalPartitionId(List<Long> selectedLogicalPartitionId) {
            builder.selectedLogicalPartitionId = selectedLogicalPartitionId == null
                    ? null : ImmutableList.copyOf(selectedLogicalPartitionId);
            return this;
        }

        public Builder setSelectedTabletId(List<Long> selectedTabletId) {
            builder.selectedTabletId = selectedTabletId == null
                    ? null : ImmutableList.copyOf(selectedTabletId);
            return this;
        }
    }
}
