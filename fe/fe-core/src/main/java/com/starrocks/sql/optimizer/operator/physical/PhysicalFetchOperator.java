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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PhysicalFetchOperator extends PhysicalOperator {

    // row id column ref -> scan operator
    Map<ColumnRefOperator, PhysicalScanOperator> rowIdToScanOperator;
    Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToRefColumns;
    // row id column ref -> fetched columns
    Map<ColumnRefOperator, Set<ColumnRefOperator>> rowIdToLazyColumns;

    public PhysicalFetchOperator(Map<ColumnRefOperator, PhysicalScanOperator> rowIdToScanOperator,
                                 Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToRefColumns,
                                 Map<ColumnRefOperator, Set<ColumnRefOperator>> rowIdToLazyColumns) {
        super(OperatorType.PHYSICAL_FETCH);
        this.rowIdToScanOperator = rowIdToScanOperator;
        this.rowIdToRefColumns = rowIdToRefColumns;
        this.rowIdToLazyColumns = rowIdToLazyColumns;
    }

    public Map<ColumnRefOperator, PhysicalScanOperator> getRowIdToScanOperator() {
        return rowIdToScanOperator;
    }

    public Map<ColumnRefOperator, Set<ColumnRefOperator>> getRowIdToLazyColumns() {
        return rowIdToLazyColumns;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> entryList = Lists.newArrayList();
        for (OptExpression input : inputs) {
            for (ColumnOutputInfo entry : input.getRowOutputInfo().getColumnOutputInfo()) {
                entryList.add(new ColumnOutputInfo(entry.getColumnRef(), entry.getColumnRef()));
            }
        }
        return new RowOutputInfo(entryList);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalFetch(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalFetch(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalFetchOperator that = (PhysicalFetchOperator) o;
        return Objects.equals(rowIdToScanOperator, that.rowIdToScanOperator)
                && Objects.equals(rowIdToRefColumns, that.rowIdToRefColumns)
                && Objects.equals(rowIdToLazyColumns, that.rowIdToLazyColumns);
    }

}
