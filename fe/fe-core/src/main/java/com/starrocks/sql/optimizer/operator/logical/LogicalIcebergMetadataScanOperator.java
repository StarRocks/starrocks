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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalIcebergMetadataScanOperator extends LogicalScanOperator {
    private ScanOperatorPredicates predicates = new ScanOperatorPredicates();
    private boolean isTransformed;

    public LogicalIcebergMetadataScanOperator(Table table,
                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                      Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                      long limit,
                                      ScalarOperator predicate) {
        this(table, colRefToColumnMetaMap, columnMetaToColRefMap, limit, predicate, TableVersionRange.empty());
    }

    public LogicalIcebergMetadataScanOperator(Table table,
                                              Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                              Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                              long limit,
                                              ScalarOperator predicate,
                                              TableVersionRange versionRange) {
        super(OperatorType.LOGICAL_ICEBERG_METADATA_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate,
                null,
                versionRange);

    }

    private LogicalIcebergMetadataScanOperator() {
        super(OperatorType.LOGICAL_ICEBERG_METADATA_SCAN);
    }

    public ScanOperatorPredicates getScanOperatorPredicates() {
        return predicates;
    }

    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) {
        this.predicates = predicates;
    }

    public boolean isTransformed() {
        return isTransformed;
    }

    public void setTransformed(boolean transformed) {
        isTransformed = transformed;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalIcebergMetadataScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalIcebergMetadataScanOperator, LogicalIcebergMetadataScanOperator.Builder> {

        @Override
        protected LogicalIcebergMetadataScanOperator newInstance() {
            return new LogicalIcebergMetadataScanOperator();
        }

        @Override
        public LogicalIcebergMetadataScanOperator.Builder withOperator(LogicalIcebergMetadataScanOperator scanOperator) {
            super.withOperator(scanOperator);
            builder.predicates = scanOperator.predicates;
            builder.isTransformed = scanOperator.isTransformed;
            return this;
        }
    }
}