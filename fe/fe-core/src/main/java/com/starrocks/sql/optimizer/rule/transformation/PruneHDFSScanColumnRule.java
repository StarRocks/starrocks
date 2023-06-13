// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.UnaryOperator.identity;

public class PruneHDFSScanColumnRule extends TransformationRule {
    public static final PruneHDFSScanColumnRule HIVE_SCAN = new PruneHDFSScanColumnRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final PruneHDFSScanColumnRule ICEBERG_SCAN =
            new PruneHDFSScanColumnRule(OperatorType.LOGICAL_ICEBERG_SCAN);
    public static final PruneHDFSScanColumnRule HUDI_SCAN = new PruneHDFSScanColumnRule(OperatorType.LOGICAL_HUDI_SCAN);
    public static final PruneHDFSScanColumnRule DELTALAKE_SCAN =
            new PruneHDFSScanColumnRule(OperatorType.LOGICAL_DELTALAKE_SCAN);
    public static final PruneHDFSScanColumnRule FILE_SCAN =
            new PruneHDFSScanColumnRule(OperatorType.LOGICAL_FILE_SCAN);
    public static final PruneHDFSScanColumnRule PAIMON_SCAN =
            new PruneHDFSScanColumnRule(OperatorType.LOGICAL_PAIMON_SCAN);

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

        checkPartitionColumnType(scanOperator, scanColumns, context);

        // make sure there is at least one materialized column in new output columns.
        // if not, we have to choose one materialized column from scan operator output columns
        // with the minimal cost.
        if (!containsMaterializedColumn(scanOperator, scanColumns)) {
            List<ColumnRefOperator> preOutputColumns =
                    new ArrayList<>(scanOperator.getColRefToColumnMetaMap().keySet());
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
                if (columnType.isScalarType() && columnType.isSupported()) {
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
            try {
                Class<? extends LogicalScanOperator> classType = scanOperator.getClass();
                Map<ColumnRefOperator, Column> newColumnRefMap = scanColumns.stream()
                        .collect(Collectors.toMap(identity(), scanOperator.getColRefToColumnMetaMap()::get));
                LogicalScanOperator newScanOperator =
                        classType.getConstructor(Table.class, Map.class, Map.class, long.class,
                                ScalarOperator.class).newInstance(
                                scanOperator.getTable(),
                                newColumnRefMap,
                                scanOperator.getColumnMetaToColRefMap(),
                                scanOperator.getLimit(),
                                scanOperator.getPredicate());

                newScanOperator.setScanOperatorPredicates(scanOperator.getScanOperatorPredicates());

                return Lists.newArrayList(new OptExpression(newScanOperator));
            } catch (Exception e) {
                throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
            }
        }
    }

    private void checkPartitionColumnType(LogicalScanOperator scanOperator, Set<ColumnRefOperator> scanColumnRefOperators,
                                          OptimizerContext context) {
        Table table = scanOperator.getTable();
        List<Column> partitionColumns = table.getPartitionColumnNames().stream().filter(Objects::nonNull)
                .map(table::getColumn).collect(Collectors.toList());
        List<Column> scanColumns = scanColumnRefOperators.stream().map(col -> context.getColumnRefFactory().getColumn(col)).
                collect(Collectors.toList());
        partitionColumns.retainAll(scanColumns);
        if (partitionColumns.stream().map(Column::getType).anyMatch(this::notSupportedPartitionColumnType)) {
            throw new StarRocksPlannerException("Table partition by float/timestamp/decimal datatype is not supported",
                    ErrorType.UNSUPPORTED);
        }
    }

    private boolean notSupportedPartitionColumnType(Type type) {
        return type.isFloat() || type.isDecimalOfAnyVersion() || type.isDatetime();
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