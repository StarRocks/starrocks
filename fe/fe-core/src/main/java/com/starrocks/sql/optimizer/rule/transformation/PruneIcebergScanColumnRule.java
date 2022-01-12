// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import avro.shaded.com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.UnaryOperator.identity;

public class PruneIcebergScanColumnRule extends TransformationRule {
    public PruneIcebergScanColumnRule() {
        super(RuleType.TF_PRUNE_OLAP_SCAN_COLUMNS, Pattern.create(OperatorType.LOGICAL_ICEBERG_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalIcebergScanOperator scanOperator = (LogicalIcebergScanOperator) input.getOp();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().get(0).getRequiredColumns();

        Set<ColumnRefOperator> scanColumns =
                scanOperator.getColRefToColumnMetaMap().keySet().stream().filter(requiredOutputColumns::contains)
                        .collect(Collectors.toSet());
        scanColumns.addAll(Utils.extractColumnRef(scanOperator.getPredicate()));

        // make sure there is at least one materialized column in new output columns.
        // if not, we have to choose one materialized column from scan operator output columns
        // with the minimal cost.
        if (scanColumns.size() == 0) {
            List<ColumnRefOperator> outputColumns = new ArrayList<>(scanOperator.getColRefToColumnMetaMap().keySet());

            int smallestIndex = -1;
            int smallestColumnLength = Integer.MAX_VALUE;
            for (int index = 0; index < outputColumns.size(); ++index) {
                if (smallestIndex == -1) {
                    smallestIndex = index;
                }
                Type columnType = outputColumns.get(index).getType();
                if (columnType.isScalarType()) {
                    int columnLength = columnType.getTypeSize();
                    if (columnLength < smallestColumnLength) {
                        smallestIndex = index;
                        smallestColumnLength = columnLength;
                    }
                }
            }
            Preconditions.checkArgument(smallestIndex != -1);
            scanColumns.add(outputColumns.get(smallestIndex));
        }

        if (scanOperator.getOutputColumns().equals(new ArrayList<>(scanColumns))) {
            return Collections.emptyList();
        } else {
            Map<ColumnRefOperator, Column> newColumnRefMap = scanColumns.stream()
                    .collect(Collectors.toMap(identity(), scanOperator.getColRefToColumnMetaMap()::get));

            LogicalIcebergScanOperator icebergScanOperator = new LogicalIcebergScanOperator(
                    scanOperator.getTable(),
                    scanOperator.getTableType(),
                    newColumnRefMap,
                    scanOperator.getColumnMetaToColRefMap(),
                    scanOperator.getLimit(),
                    scanOperator.getPredicate());

            icebergScanOperator.getMinMaxConjuncts().addAll(scanOperator.getMinMaxConjuncts());
            icebergScanOperator.getMinMaxColumnRefMap().putAll(scanOperator.getMinMaxColumnRefMap());

            return Lists.newArrayList(new OptExpression(icebergScanOperator));
        }
    }
}