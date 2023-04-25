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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TempExternalTable;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalTempExtTableScanOperator extends LogicalScanOperator {
    private ScanOperatorPredicates predicates = new ScanOperatorPredicates();
    private boolean hasUnknownColumn;
    public LogicalTempExtTableScanOperator(Table table,
                                           Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                           Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                           long limit,
                                           ScalarOperator predicate) {
        super(OperatorType.LOGICAL_TEMP_EXT_TABLE_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate, null);

        Preconditions.checkState(table instanceof TempExternalTable);
    }

    private LogicalTempExtTableScanOperator(LogicalTempExtTableScanOperator.Builder builder) {
        super(OperatorType.LOGICAL_FILE_SCAN,
                builder.table,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());
    }


    @Override
    public ScanOperatorPredicates getScanOperatorPredicates() {
        return this.predicates;
    }

    @Override
    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) {
        this.predicates = predicates;
    }

    public boolean hasUnknownColumn() {
        return hasUnknownColumn;
    }

    public void setHasUnknownColumn(boolean hasUnknownColumn) {
        this.hasUnknownColumn = hasUnknownColumn;
    }


    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalTempExtTableScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalTempExtTableScanOperator, LogicalTempExtTableScanOperator.Builder> {
        private ScanOperatorPredicates predicates = new ScanOperatorPredicates();

        @Override
        public LogicalTempExtTableScanOperator build() {
            return new LogicalTempExtTableScanOperator(this);
        }

        @Override
        public LogicalTempExtTableScanOperator.Builder withOperator(LogicalTempExtTableScanOperator scanOperator) {
            super.withOperator(scanOperator);

            return this;
        }
    }
}
