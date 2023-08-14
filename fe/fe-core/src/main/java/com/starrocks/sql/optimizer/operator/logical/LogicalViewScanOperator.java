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
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;

public class LogicalViewScanOperator  extends LogicalScanOperator {
    public LogicalViewScanOperator(
            Table table,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        super(OperatorType.LOGICAL_VIEW_SCAN, table, colRefToColumnMetaMap,
                columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null, null);
    }

    private LogicalViewScanOperator() {
        super(OperatorType.LOGICAL_VIEW_SCAN);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalViewScan(this, context);
    }

    public static LogicalOlapScanOperator.Builder builder() {
        return new LogicalOlapScanOperator.Builder();
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalViewScanOperator, LogicalViewScanOperator.Builder> {
        @Override
        protected LogicalViewScanOperator newInstance() {
            return new LogicalViewScanOperator();
        }

        @Override
        public LogicalViewScanOperator.Builder withOperator(LogicalViewScanOperator scanOperator) {
            super.withOperator(scanOperator);
            return this;
        }
    }
}
