// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.external.elasticsearch.EsShardPartitions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class PhysicalEsScanOperator extends PhysicalScanOperator {
    private final List<EsShardPartitions> selectedIndex;

    public PhysicalEsScanOperator(Table table,
                                  Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                  List<EsShardPartitions> selectedIndex,
                                  long limit,
                                  ScalarOperator predicate,
                                  Projection projection) {
        super(OperatorType.PHYSICAL_ES_SCAN, table, colRefToColumnMetaMap, limit, predicate, projection);
        this.selectedIndex = selectedIndex;
    }

    public List<EsShardPartitions> getSelectedIndex() {
        return this.selectedIndex;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalEsScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalEsScan(optExpression, context);
    }
}
