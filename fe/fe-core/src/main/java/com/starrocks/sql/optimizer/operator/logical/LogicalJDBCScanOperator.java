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
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalJDBCScanOperator extends LogicalScanOperator {

    public LogicalJDBCScanOperator(Table table,
                                   Map<ColumnRefOperator, Column> columnRefOperatorColumnMap,
                                   Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                   long limit,
                                   ScalarOperator predicate,
                                   Projection projection) {
        super(OperatorType.LOGICAL_JDBC_SCAN,
                table,
                columnRefOperatorColumnMap,
                columnMetaToColRefMap,
                limit, predicate, projection);
        Preconditions.checkState(table instanceof JDBCTable);
    }

    private LogicalJDBCScanOperator() {
        super(OperatorType.LOGICAL_JDBC_SCAN);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJDBCScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalJDBCScanOperator, LogicalJDBCScanOperator.Builder> {
        @Override
        protected LogicalJDBCScanOperator newInstance() {
            return new LogicalJDBCScanOperator();
        }
    }
}
