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

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.changes.ChangesMetaDescriptor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PhysicalChangesScanOperator extends PhysicalScanOperator {

    private final Bookmark base;
    private final Bookmark head;
    private final BookmarkChange delta;
    // CHANGES metadata descriptors for this relation.
    private final List<ChangesMetaDescriptor> changesMetaDescriptors;
    // Selected logical partition ids after partition pruning; null means all delta partitions.
    private final List<Long> selectedLogicalPartitionId;
    // Selected tablet ids after tablet pruning; null means all tablets in the selected partitions.
    private final List<Long> selectedTabletId;

    public PhysicalChangesScanOperator(Table table,
                                       Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                       long limit,
                                       ScalarOperator predicate,
                                       Projection projection,
                                       Bookmark base,
                                       Bookmark head,
                                       BookmarkChange delta,
                                       List<ChangesMetaDescriptor> changesMetaDescriptors,
                                       List<Long> selectedLogicalPartitionId,
                                       List<Long> selectedTabletId) {
        super(OperatorType.PHYSICAL_CHANGES_SCAN, table,
                colRefToColumnMetaMap, limit, predicate, projection);
        this.base = base;
        this.head = head;
        this.delta = delta;
        this.changesMetaDescriptors = changesMetaDescriptors;
        this.selectedLogicalPartitionId = selectedLogicalPartitionId;
        this.selectedTabletId = selectedTabletId;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalChangesScanOperator that = (PhysicalChangesScanOperator) o;
        return base.getBookmarkId() == that.base.getBookmarkId()
                && head.getBookmarkId() == that.head.getBookmarkId()
                && Objects.equals(changesMetaDescriptors, that.changesMetaDescriptors)
                && Objects.equals(selectedLogicalPartitionId, that.selectedLogicalPartitionId)
                && Objects.equals(selectedTabletId, that.selectedTabletId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), base.getBookmarkId(), head.getBookmarkId(),
                changesMetaDescriptors, selectedLogicalPartitionId, selectedTabletId);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalChangesScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalChangesScan(optExpression, context);
    }
}
