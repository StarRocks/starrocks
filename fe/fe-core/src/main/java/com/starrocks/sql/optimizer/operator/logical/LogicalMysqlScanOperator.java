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
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalMysqlScanOperator extends LogicalScanOperator {

    private String temporalClause;

    public LogicalMysqlScanOperator(Table table,
                                    Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                    Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                    long limit,
                                    ScalarOperator predicate,
                                    Projection projection) {
        super(OperatorType.LOGICAL_MYSQL_SCAN, table,
                colRefToColumnMetaMap, columnMetaToColRefMap, limit, predicate, projection);
        Preconditions.checkState(table instanceof MysqlTable);
    }

    public String getTemporalClause() {
        return this.temporalClause;
    }

    public void setTemporalClause(String temporalClause) {
        this.temporalClause = temporalClause;
    }

    private LogicalMysqlScanOperator(LogicalMysqlScanOperator.Builder builder) {
        super(OperatorType.LOGICAL_MYSQL_SCAN,
                builder.table,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalMysqlScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalMysqlScanOperator, LogicalMysqlScanOperator.Builder> {
        @Override
        public LogicalMysqlScanOperator build() {
            return new LogicalMysqlScanOperator(this);
        }

        @Override
        public LogicalMysqlScanOperator.Builder withOperator(LogicalMysqlScanOperator operator) {
            super.withOperator(operator);
            return this;
        }
    }
}
