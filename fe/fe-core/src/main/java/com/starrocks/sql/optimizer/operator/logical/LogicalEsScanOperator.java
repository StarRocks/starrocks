// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.Table;
import com.starrocks.external.elasticsearch.EsShardPartitions;
import com.starrocks.external.elasticsearch.EsTablePartitions;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;

public class LogicalEsScanOperator extends LogicalScanOperator {
    private EsTablePartitions esTablePartitions;
    private List<EsShardPartitions> selectedIndex = Lists.newArrayList();

    public LogicalEsScanOperator(Table table, List<ColumnRefOperator> outputColumns,
                                 Map<ColumnRefOperator, Column> columnRefMap) {
        super(OperatorType.LOGICAL_ES_SCAN, table, outputColumns, columnRefMap);
        this.esTablePartitions = ((EsTable) table).getEsTablePartitions();
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
}
