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

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class PhysicalTopNOperator extends PhysicalOperator {
    private final long offset;
    private final List<ColumnRefOperator> partitionByColumns;
    private final long partitionLimit;
    private final SortPhase sortPhase;
    private final TopNType topNType;
    private final boolean isSplit;
    private final boolean isEnforced;

    // If limit is -1, means global sort
    public PhysicalTopNOperator(OrderSpec spec, long limit, long offset,
                                List<ColumnRefOperator> partitionByColumns,
                                long partitionLimit,
                                SortPhase sortPhase,
                                TopNType topNType,
                                boolean isSplit,
                                boolean isEnforced,
                                ScalarOperator predicate,
                                Projection projection) {
        super(OperatorType.PHYSICAL_TOPN, spec);
        this.limit = limit;
        this.offset = offset;
        this.partitionByColumns = partitionByColumns;
        this.partitionLimit = partitionLimit;
        this.sortPhase = sortPhase;
        this.topNType = topNType;
        this.isSplit = isSplit;
        this.isEnforced = isEnforced;
        this.predicate = predicate;
        this.projection = projection;
    }

    public List<ColumnRefOperator> getPartitionByColumns() {
        return partitionByColumns;
    }

    public long getPartitionLimit() {
        return partitionLimit;
    }

    public SortPhase getSortPhase() {
        return sortPhase;
    }

    public TopNType getTopNType() {
        return topNType;
    }

    public boolean isSplit() {
        return isSplit;
    }

    public long getOffset() {
        return offset;
    }

    public boolean isEnforced() {
        return isEnforced;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), orderSpec, offset, sortPhase, topNType, isSplit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalTopNOperator that = (PhysicalTopNOperator) o;

        return partitionLimit == that.partitionLimit && offset == that.offset && isSplit == that.isSplit &&
                Objects.equals(partitionByColumns, that.partitionByColumns) &&
                Objects.equals(orderSpec, that.orderSpec) &&
                sortPhase == that.sortPhase && topNType == that.topNType;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalTopN(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalTopN(optExpression, context);
    }

    public void fillDisableDictOptimizeColumns(ColumnRefSet resultSet, Set<Integer> dictColIds) {
        // nothing to do
    }

    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = ColumnRefSet.createByIds(childDictColumns);

        for (Ordering orderDesc : orderSpec.getOrderDescs()) {
            if (orderDesc.getColumnRef().getUsedColumns().isIntersect(dictSet)) {
                return true;
            }
        }

        return false;
    }

}
