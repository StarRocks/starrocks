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
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalMetadataScanOperator extends LogicalScanOperator {
    private ScanOperatorPredicates predicates = new ScanOperatorPredicates();
    private boolean isTransformed;

    public LogicalMetadataScanOperator(Table table,
                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                      Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                      long limit,
                                      ScalarOperator predicate) {
        this(table, colRefToColumnMetaMap, columnMetaToColRefMap, limit, predicate, TvrTableSnapshot.empty());
    }

    public LogicalMetadataScanOperator(Table table,
                                              Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                              Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                              long limit,
                                              ScalarOperator predicate,
                                              TvrVersionRange versionRange) {
        super(OperatorType.LOGICAL_METADATA_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate,
                null,
                versionRange);

    }

    private LogicalMetadataScanOperator() {
        super(OperatorType.LOGICAL_METADATA_SCAN);
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
        return visitor.visitLogicalMetadataScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalMetadataScanOperator, LogicalMetadataScanOperator.Builder> {

        @Override
        protected LogicalMetadataScanOperator newInstance() {
            return new LogicalMetadataScanOperator();
        }

        @Override
        public LogicalMetadataScanOperator.Builder withOperator(LogicalMetadataScanOperator scanOperator) {
            super.withOperator(scanOperator);
            builder.predicates = scanOperator.predicates;
            builder.isTransformed = scanOperator.isTransformed;
            return this;
        }
    }
}