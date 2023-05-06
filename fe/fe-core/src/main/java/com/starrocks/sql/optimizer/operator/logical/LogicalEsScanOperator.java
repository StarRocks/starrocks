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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.connector.elasticsearch.EsTablePartitions;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class LogicalEsScanOperator extends LogicalScanOperator {
    private EsTablePartitions esTablePartitions;
    private List<EsShardPartitions> selectedIndex = Lists.newArrayList();

    public LogicalEsScanOperator(Table table,
                                 Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                 Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                 long limit,
                                 ScalarOperator predicate,
                                 Projection projection) {
        super(OperatorType.LOGICAL_ES_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit, predicate, projection);
        Preconditions.checkState(table instanceof EsTable);
        this.esTablePartitions = ((EsTable) table).getEsTablePartitions();
    }

    private LogicalEsScanOperator() {
        super(OperatorType.LOGICAL_ES_SCAN);
    }

    public EsTablePartitions getEsTablePartitions() {
        return this.esTablePartitions;
    }

    public List<EsShardPartitions> getSelectedIndex() {
        return this.selectedIndex;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalEsScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalEsScanOperator, LogicalEsScanOperator.Builder> {

        @Override
        protected LogicalEsScanOperator newInstance() {
            return new LogicalEsScanOperator();
        }

        @Override
        public LogicalEsScanOperator.Builder withOperator(LogicalEsScanOperator esScanOperator) {
            super.withOperator(esScanOperator);
            builder.esTablePartitions = esScanOperator.esTablePartitions;
            builder.selectedIndex = esScanOperator.selectedIndex;
            Preconditions.checkState(builder.table instanceof EsTable);
            return this;
        }
    }
}
