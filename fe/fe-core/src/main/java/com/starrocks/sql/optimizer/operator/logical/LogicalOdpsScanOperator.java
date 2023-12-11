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
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalOdpsScanOperator extends LogicalScanOperator {
    private ScanOperatorPredicates predicates = new ScanOperatorPredicates();
    private boolean hasUnknownColumn;

    public LogicalOdpsScanOperator(Table table,
                                   Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                   Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                   long limit,
                                   ScalarOperator predicate) {

        super(OperatorType.LOGICAL_ODPS_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate, null);

        Preconditions.checkState(table instanceof OdpsTable);
        OdpsTable odpsTable = (OdpsTable) table;
        partitionColumns.addAll(odpsTable.getPartitionColumnNames());
    }

    private LogicalOdpsScanOperator() {
        super(OperatorType.LOGICAL_ODPS_SCAN);
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

    public boolean isEmptyOutputRows() {
        return !table.isUnPartitioned() && predicates.getSelectedPartitionIds().isEmpty();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOdpsScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalOdpsScanOperator, LogicalOdpsScanOperator.Builder> {

        @Override
        protected LogicalOdpsScanOperator newInstance() {
            return new LogicalOdpsScanOperator();
        }

        @Override
        public LogicalOdpsScanOperator.Builder withOperator(LogicalOdpsScanOperator scanOperator) {
            super.withOperator(scanOperator);

            builder.predicates = scanOperator.predicates.clone();
            builder.partitionColumns = scanOperator.partitionColumns;
            return this;
        }
    }
}
