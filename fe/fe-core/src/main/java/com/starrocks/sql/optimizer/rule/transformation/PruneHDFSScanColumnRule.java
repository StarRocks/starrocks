// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import avro.shaded.com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
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

public class PruneHDFSScanColumnRule extends TransformationRule {
    public static final PruneHDFSScanColumnRule HIVE_SCAN = new PruneHDFSScanColumnRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final PruneHDFSScanColumnRule ICEBERG_SCAN =
            new PruneHDFSScanColumnRule(OperatorType.LOGICAL_ICEBERG_SCAN);
    public static final PruneHDFSScanColumnRule HUDI_SCAN = new PruneHDFSScanColumnRule(OperatorType.LOGICAL_HUDI_SCAN);

    public PruneHDFSScanColumnRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_PRUNE_OLAP_SCAN_COLUMNS, Pattern.create(logicalOperatorType));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getOp();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        Set<ColumnRefOperator> scanColumns =
                scanOperator.getColRefToColumnMetaMap().keySet().stream().filter(requiredOutputColumns::contains)
                        .collect(Collectors.toSet());
        scanColumns.addAll(Utils.extractColumnRef(scanOperator.getPredicate()));

        // make sure there is at least one materialized column in new output columns.
        // if not, we have to choose one materialized column from scan operator output columns
        // with the minimal cost.
        if (!containsMaterializedColumn(scanOperator, scanColumns)) {
            List<ColumnRefOperator> preOutputColumns = new ArrayList<>(scanOperator.getColRefToColumnMetaMap().keySet());
            List<ColumnRefOperator> outputColumns = preOutputColumns.stream()
                    .filter(column -> !column.getType().getPrimitiveType().equals(PrimitiveType.UNKNOWN_TYPE))
                    .collect(Collectors.toList());

            int smallestIndex = -1;
            int smallestColumnLength = Integer.MAX_VALUE;
            for (int index = 0; index < outputColumns.size(); ++index) {
                if (isPartitionColumn(scanOperator, outputColumns.get(index).getName())) {
                    continue;
                }

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
        } else if (scanOperator instanceof LogicalHiveScanOperator) {
            LogicalHiveScanOperator logicalHiveScanOperator = (LogicalHiveScanOperator) scanOperator;
            Map<ColumnRefOperator, Column> newColumnRefMap = scanColumns.stream()
                    .collect(Collectors.toMap(identity(), logicalHiveScanOperator.getColRefToColumnMetaMap()::get));

            LogicalHiveScanOperator hiveScanOperator = new LogicalHiveScanOperator(
                    logicalHiveScanOperator.getTable(),
                    logicalHiveScanOperator.getTableType(),
                    newColumnRefMap,
                    logicalHiveScanOperator.getColumnMetaToColRefMap(),
                    logicalHiveScanOperator.getLimit(),
                    logicalHiveScanOperator.getPredicate());

            hiveScanOperator.setScanOperatorPredicates(logicalHiveScanOperator.getScanOperatorPredicates());

            return Lists.newArrayList(new OptExpression(hiveScanOperator));
        } else if (scanOperator instanceof LogicalIcebergScanOperator) {
            LogicalIcebergScanOperator logicalIcebergScanOperator = (LogicalIcebergScanOperator) scanOperator;
            Map<ColumnRefOperator, Column> newColumnRefMap = scanColumns.stream()
                    .collect(Collectors.toMap(identity(), logicalIcebergScanOperator.getColRefToColumnMetaMap()::get));

            LogicalIcebergScanOperator icebergScanOperator = new LogicalIcebergScanOperator(
                    logicalIcebergScanOperator.getTable(),
                    logicalIcebergScanOperator.getTableType(),
                    newColumnRefMap,
                    logicalIcebergScanOperator.getColumnMetaToColRefMap(),
                    logicalIcebergScanOperator.getLimit(),
                    logicalIcebergScanOperator.getPredicate());

            icebergScanOperator.setScanOperatorPredicates(logicalIcebergScanOperator.getScanOperatorPredicates());

            return Lists.newArrayList(new OptExpression(icebergScanOperator));
        } else if (scanOperator instanceof LogicalHudiScanOperator) {
            LogicalHudiScanOperator logicalHudiScanOperator = (LogicalHudiScanOperator) scanOperator;
            Map<ColumnRefOperator, Column> newColumnRefMap = scanColumns.stream()
                    .collect(Collectors.toMap(identity(), logicalHudiScanOperator.getColRefToColumnMetaMap()::get));

            LogicalHudiScanOperator hudiScanOperator = new LogicalHudiScanOperator(
                    logicalHudiScanOperator.getTable(),
                    logicalHudiScanOperator.getTableType(),
                    newColumnRefMap,
                    logicalHudiScanOperator.getColumnMetaToColRefMap(),
                    logicalHudiScanOperator.getLimit(),
                    logicalHudiScanOperator.getPredicate());

            hudiScanOperator.setScanOperatorPredicates(logicalHudiScanOperator.getScanOperatorPredicates());

            return Lists.newArrayList(new OptExpression(hudiScanOperator));
        } else {
            throw new StarRocksPlannerException("Unsupported logical scan operator type!", ErrorType.INTERNAL_ERROR);
        }
    }

    private boolean containsMaterializedColumn(LogicalScanOperator scanOperator, Set<ColumnRefOperator> scanColumns) {
        return scanColumns.size() != 0 && !scanOperator.getPartitionColumns().containsAll(
                scanColumns.stream().map(ColumnRefOperator::getName).collect(Collectors.toList()));
    }

    private boolean isPartitionColumn(LogicalScanOperator scanOperator, String columnName) {
        // Hive/Hudi partition columns is not materialized column, so except partition columns
        return scanOperator.getPartitionColumns().contains(columnName);
    }
}