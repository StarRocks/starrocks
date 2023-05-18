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

package com.starrocks.sql.optimizer.operator.stream;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;

public class LogicalBinlogScanOperator extends LogicalScanOperator {

    public LogicalBinlogScanOperator(Table table,
                                     Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                     Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                     long limit) {
        super(OperatorType.LOGICAL_BINLOG_SCAN, table, colRefToColumnMetaMap, columnMetaToColRefMap, limit, null, null);
    }

    private LogicalBinlogScanOperator() {
        super(OperatorType.LOGICAL_BINLOG_SCAN);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalBinlogScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalTableScan(optExpression, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalBinlogScanOperator, LogicalBinlogScanOperator.Builder> {

        @Override
        protected LogicalBinlogScanOperator newInstance() {
            return new LogicalBinlogScanOperator();
        }
    }

}
