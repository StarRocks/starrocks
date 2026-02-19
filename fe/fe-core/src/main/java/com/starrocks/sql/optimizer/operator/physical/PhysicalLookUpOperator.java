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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
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

public class PhysicalLookUpOperator extends PhysicalOperator {
    // key is a virtual column ref, used to distinguish different table
    // row id column ref -> Table
    Map<ColumnRefOperator, Table> rowIdToTable;
    // row id column ref -> ref columns
    // we can use multi columns to describe row position, value is order-sensitive
    // @TODO: maybe we can use Expr? but I can't see any benifits
    Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToFetchRefColumns;
    Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToLookUpRefColumns;
    // row id column ref -> lazy fetched columns
    Map<ColumnRefOperator, Set<ColumnRefOperator>> rowIdToLazyColumns;
    // lazy fetched column -> Column
    Map<ColumnRefOperator, Column> columnRefOperatorColumnMap;

    public PhysicalLookUpOperator(Map<ColumnRefOperator, Table> rowIdToTable,
                                 Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToFetchRefColumns,
                                 Map<ColumnRefOperator, List<ColumnRefOperator>> rowIdToLookUpRefColumns,
                                 Map<ColumnRefOperator, Set<ColumnRefOperator>> rowIdToLazyColumns,
                                 Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        super(OperatorType.PHYSICAL_LOOKUP);
        this.rowIdToTable = rowIdToTable;
        this.rowIdToFetchRefColumns = rowIdToFetchRefColumns;
        this.rowIdToLookUpRefColumns = rowIdToLookUpRefColumns;
        this.rowIdToLazyColumns = rowIdToLazyColumns;
        this.columnRefOperatorColumnMap = columnRefOperatorColumnMap;
    }

    public Map<ColumnRefOperator, Table> getRowIdToTable() {
        return rowIdToTable;
    }

    public Map<ColumnRefOperator, List<ColumnRefOperator>> getRowIdToFetchRefColumns() {
        return rowIdToFetchRefColumns;
    }

    public Map<ColumnRefOperator, List<ColumnRefOperator>> getRowIdToLookUpRefColumns() {
        return rowIdToLookUpRefColumns;
    }

    public Map<ColumnRefOperator, Set<ColumnRefOperator>> getRowIdToLazyColumns() {
        return rowIdToLazyColumns;
    }

    public Map<ColumnRefOperator, Column> getColumnRefOperatorColumnMap() {
        return columnRefOperatorColumnMap;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> entryList = Lists.newArrayList();
        columnRefOperatorColumnMap.keySet().forEach(key -> {
            entryList.add(new ColumnOutputInfo(key, key));
        });
        return new RowOutputInfo(entryList);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLookUp(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalLookUp(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalLookUpOperator that = (PhysicalLookUpOperator) o;
        return Objects.equals(rowIdToTable, that.rowIdToTable)
                && Objects.equals(rowIdToFetchRefColumns, that.rowIdToFetchRefColumns)
                && Objects.equals(rowIdToLookUpRefColumns, that.rowIdToLookUpRefColumns)
                && Objects.equals(rowIdToLazyColumns, rowIdToLazyColumns)
                && Objects.equals(columnRefOperatorColumnMap, that.columnRefOperatorColumnMap);
    }

}
