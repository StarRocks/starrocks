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

package com.starrocks.sql.optimizer.rule.transformation.materialization.compensation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_PARTITION_PRUNED;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator.SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.convertPartitionKeyRangesToListPredicate;

/**
 * Compensate the scan operator with the partition compensation.
 */
public class OptCompensator extends OptExpressionVisitor<OptExpression, Void> {
    private final OptimizerContext optimizerContext;
    private final MaterializedView mv;
    private final Map<Table, BaseCompensation<?>> compensations;

    // for olap table
    public OptCompensator(OptimizerContext optimizerContext,
                          MaterializedView mv,
                          Map<Table, BaseCompensation<?>> compensations) {
        this.optimizerContext = optimizerContext;
        this.mv = mv;
        this.compensations = compensations;
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
        LogicalScanOperator scanOperator = optExpression.getOp().cast();
        Table refBaseTable = scanOperator.getTable();

        if (refBaseTable.isNativeTableOrMaterializedView()) {
            List<Long> olapTableCompensatePartitionIds = Lists.newArrayList();
            if (compensations.containsKey(refBaseTable)) {
                BaseCompensation<?> compensation = compensations.get(refBaseTable);
                BaseCompensation<Long> olapTableCompensation = (BaseCompensation<Long>) compensation;
                olapTableCompensatePartitionIds = olapTableCompensation.getCompensations();
            }
            LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) scanOperator;
            LogicalScanOperator newScanOperator = getOlapTableCompensatePlan(olapScanOperator, olapTableCompensatePartitionIds);
            // reset the partition prune flag to be pruned again.
            newScanOperator.resetOpRuleBit(OP_PARTITION_PRUNED);
            return OptExpression.create(newScanOperator);
        } else if (SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES.contains(scanOperator.getOpType())) {
            List<PRangeCell> partitionKeys = Lists.newArrayList();
            if (compensations.containsKey(refBaseTable)) {
                BaseCompensation<?> compensation = compensations.get(refBaseTable);
                BaseCompensation<PRangeCell> externalTableCompensation = (BaseCompensation<PRangeCell>) compensation;
                partitionKeys = externalTableCompensation.getCompensations();
            }
            LogicalScanOperator newScanOperator = getExternalTableCompensatePlan(scanOperator, partitionKeys);
            // reset the partition prune flag to be pruned again.
            newScanOperator.resetOpRuleBit(OP_PARTITION_PRUNED);
            return OptExpression.create(newScanOperator);
        } else {
            return optExpression;
        }
    }

    private LogicalScanOperator getOlapTableCompensatePlan(LogicalOlapScanOperator scanOperator,
                                                           List<Long> olapTableCompensatePartitionIds) {
        final LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        Preconditions.checkState(olapTableCompensatePartitionIds != null);
        // reset original partition predicates to prune partitions/tablets again
        builder.withOperator(scanOperator)
                .setSelectedPartitionId(olapTableCompensatePartitionIds)
                .setSelectedTabletId(Lists.newArrayList());
        return builder.build();
    }

    private LogicalScanOperator getExternalTableCompensatePlan(LogicalScanOperator scanOperator,
                                                               List<PRangeCell> partitionKeys) {
        Table refBaseTable = scanOperator.getTable();
        final LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scanOperator);
        // reset original partition predicates to prune partitions/tablets again
        builder.withOperator(scanOperator);

        // NOTE: This is necessary because iceberg's physical plan will not use selectedPartitionIds to
        // prune partitions.
        final Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        if (refBaseTablePartitionColumns == null || !refBaseTablePartitionColumns.containsKey(refBaseTable)) {
            return scanOperator;
        }
        List<Column> refBaseTablePartitionCols = refBaseTablePartitionColumns.get(refBaseTable);
        Preconditions.checkState(refBaseTablePartitionCols != null);
        List<ScalarOperator> partitionColumnRefs = refBaseTablePartitionCols
                .stream()
                .map(col -> scanOperator.getColumnReference(col))
                .collect(Collectors.toList());

        ScalarOperator externalExtraPredicate = null;
        if (scanOperator.getOpType() == OperatorType.LOGICAL_ICEBERG_SCAN) {
            PartitionInfo mvPartitionInfo = mv.getPartitionInfo();
            IcebergTable cachedIcebergTable = (IcebergTable) refBaseTable;
            if (!mvPartitionInfo.isListPartition()) {
                List<ColumnRefOperator> refPartitionColRefs = refBaseTablePartitionCols
                        .stream()
                        .map(col -> scanOperator.getColumnReference(col))
                        .collect(Collectors.toList());
                // check whether the iceberg table contains partition transformations
                final List<PartitionField> partitionFields = Lists.newArrayList();
                for (Column column : refBaseTablePartitionCols) {
                    for (PartitionField field : cachedIcebergTable.getNativeTable().spec().fields()) {
                        final String partitionFieldName = cachedIcebergTable.getNativeTable()
                                .schema().findColumnName(field.sourceId());
                        if (partitionFieldName.equalsIgnoreCase(column.getName())) {
                            partitionFields.add(field);
                        }
                    }
                }
                final boolean isContainPartitionTransform = partitionFields
                        .stream()
                        .anyMatch(field -> field.transform().dedupName().equalsIgnoreCase("time"));
                externalExtraPredicate =  convertPartitionKeyRangesToListPredicate(refPartitionColRefs,
                        partitionKeys, !isContainPartitionTransform);
            } else {
                externalExtraPredicate = convertPartitionKeyRangesToListPredicate(partitionColumnRefs,
                        partitionKeys, true);
            }

            // refresh iceberg table's metadata
            String catalogName = cachedIcebergTable.getCatalogName();
            String dbName = cachedIcebergTable.getCatalogDBName();
            TableName refTableName = new TableName(catalogName, dbName, cachedIcebergTable.getName());
            Table currentTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(refTableName).orElse(null);
            if (currentTable == null) {
                return null;
            }

            builder.setTable(currentTable);
            TableVersionRange versionRange = TableVersionRange.withEnd(
                    Optional.ofNullable(((IcebergTable) currentTable).getNativeTable().currentSnapshot())
                            .map(Snapshot::snapshotId));
            builder.setTableVersionRange(versionRange);
        } else {
            externalExtraPredicate = convertPartitionKeyRangesToListPredicate(partitionColumnRefs, partitionKeys,
                    true);
        }
        Preconditions.checkState(externalExtraPredicate != null);
        externalExtraPredicate.setRedundant(true);

        Preconditions.checkState(externalExtraPredicate != null);
        ScalarOperator finalPredicate = Utils.compoundAnd(scanOperator.getPredicate(), externalExtraPredicate);
        builder.setPredicate(finalPredicate);
        return builder.build();
    }

    @Override
    public OptExpression visit(OptExpression optExpression, Void context) {
        List<OptExpression> children = Lists.newArrayList();
        for (int i = 0; i < optExpression.arity(); ++i) {
            children.add(optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), null));
        }
        return OptExpression.create(optExpression.getOp(), children);
    }

    /**
     * Get the compensation plan for the mv.
     * @param mv the mv to compensate
     * @param compensations the compensations for the mv, including ref base table's compensations
     * @param optExpression query plan with the ref base table
     * @return the compensation plan for the mv
     */
    public static OptExpression getMVCompensatePlan(OptimizerContext optimizerContext,
                                                    MaterializedView mv,
                                                    Map<Table, BaseCompensation<?>> compensations,
                                                    OptExpression optExpression) {
        OptCompensator scanOperatorCompensator = new OptCompensator(optimizerContext, mv, compensations);
        return optExpression.getOp().accept(scanOperatorCompensator, optExpression, null);
    }
}