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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.PartitionKey;
<<<<<<< HEAD
=======
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
>>>>>>> fe10512fe7 ([BugFix] Fix mv refresh bug with iceberg str2date (#60089))
import com.starrocks.common.Config;
import com.starrocks.sql.common.PRangeCell;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

<<<<<<< HEAD
public class ExternalTableCompensation extends BaseCompensation {
    public ExternalTableCompensation(List<PRangeCell> partitionKeys) {
        super(partitionKeys);
=======
import static com.starrocks.connector.iceberg.IcebergPartitionUtils.getIcebergTablePartitionPredicateExpr;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.convertPartitionKeyRangesToListPredicate;

public final class ExternalTableCompensation extends TableCompensation {
    private List<PRangeCell> compensations;

    public ExternalTableCompensation(Table refBaseTable, List<PRangeCell> compensations) {
        super(refBaseTable, MVTransparentState.COMPENSATE);
        this.compensations = compensations;
    }

    @Override
    public boolean isNoCompensate() {
        return super.isNoCompensate() || (state.isCompensate() && CollectionUtils.isEmpty(compensations));
    }

    @Override
    public LogicalScanOperator compensate(OptimizerContext optimizerContext,
                                          MaterializedView mv,
                                          LogicalScanOperator scanOperator) {
        try {
            return compensateImpl(optimizerContext, mv, scanOperator);
        } catch (AnalysisException e) {
            logMVRewrite(mv.getName(), "Failed to compensate external table {}: {}",
                    refBaseTable.getName(), DebugUtil.getStackTrace(e));
            return null;
        }
    }

    public LogicalScanOperator compensateImpl(OptimizerContext optimizerContext,
                                              MaterializedView mv,
                                              LogicalScanOperator scanOperator) throws AnalysisException {
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
        List<ColumnRefOperator> refPartitionColRefs = refBaseTablePartitionCols
                .stream()
                .map(col -> scanOperator.getColumnReference(col))
                .collect(Collectors.toList());
        ScalarOperator externalExtraPredicate;
        if (refBaseTable instanceof IcebergTable) {
            IcebergTable cachedIcebergTable = (IcebergTable) refBaseTable;
            String catalogName = cachedIcebergTable.getCatalogName();
            String dbName = cachedIcebergTable.getCatalogDBName();
            TableName refTableName = new TableName(catalogName, dbName, cachedIcebergTable.getName());
            Table currentTable = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(new ConnectContext(), refTableName).orElse(null);
            if (currentTable == null) {
                return null;
            }
            builder.setTable(currentTable);
            TableVersionRange versionRange = TableVersionRange.withEnd(
                    Optional.ofNullable(((IcebergTable) currentTable).getNativeTable().currentSnapshot())
                            .map(Snapshot::snapshotId));
            builder.setTableVersionRange(versionRange);
            externalExtraPredicate = getIcebergTableCompensation(optimizerContext, mv, (IcebergTable) currentTable,
                    refTableName, refPartitionColRefs);
        } else {
            externalExtraPredicate = convertPartitionKeyRangesToListPredicate(refPartitionColRefs, compensations, true);
        }
        Preconditions.checkState(externalExtraPredicate != null);
        externalExtraPredicate.setRedundant(true);
        ScalarOperator finalPredicate = Utils.compoundAnd(scanOperator.getPredicate(), externalExtraPredicate);
        builder.setPredicate(finalPredicate);
        return builder.build();
    }

    private ScalarOperator getIcebergTableCompensation(OptimizerContext optimizerContext,
                                                       MaterializedView mv,
                                                       IcebergTable icebergTable,
                                                       TableName refTableName,
                                                       List<ColumnRefOperator> refPartitionColRefs) throws AnalysisException {
        PartitionInfo mvPartitionInfo = mv.getPartitionInfo();
        if (!mvPartitionInfo.isListPartition()) {
            // check whether the iceberg table contains partition transformations
            final List<Column> refBaseTablePartitionCols = refPartitionColRefs.stream()
                    .map(ref -> icebergTable.getColumn(ref.getName()))
                    .collect(Collectors.toList());
            final List<PartitionField> partitionFields = Lists.newArrayList();
            for (Column column : refBaseTablePartitionCols) {
                for (PartitionField field : icebergTable.getNativeTable().spec().fields()) {
                    final String partitionFieldName = icebergTable.getNativeTable().schema().findColumnName(field.sourceId());
                    if (partitionFieldName.equalsIgnoreCase(column.getName())) {
                        partitionFields.add(field);
                    }
                }
            }
            final boolean isContainPartitionTransform = partitionFields
                    .stream()
                    .anyMatch(field -> field.transform().dedupName().equalsIgnoreCase("time"));
            return convertPartitionKeyRangesToListPredicate(refPartitionColRefs, compensations, !isContainPartitionTransform);
        }
        List<Column> mvPartitionCols = mv.getPartitionColumns();
        // to iceberg, `partitionKeys` are using LocalTime as partition values which cannot be used to prune iceberg
        // partitions directly because iceberg uses UTC time in its partition metadata.
        // convert `partitionKeys` to iceberg utc time here.
        // Please see MVPCTRefreshListPartitioner#genPartitionPredicate for more details.
        Map<Table, List<SlotRef>> refBaseTablePartitionSlotRefs = mv.getRefBaseTablePartitionSlots();
        Preconditions.checkArgument(refBaseTablePartitionSlotRefs.containsKey(icebergTable));
        final List<SlotRef> refBaseTableSlotRefs = refBaseTablePartitionSlotRefs.get(icebergTable);
        ExpressionMapping expressionMapping =
                new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                        Lists.newArrayList());
        for (int i = 0; i < refPartitionColRefs.size(); i++) {
            ColumnRefOperator refPartitionColRef = refPartitionColRefs.get(i);
            SlotRef refBaseTablePartitionExpr = refBaseTableSlotRefs.get(i);
            expressionMapping.put(refBaseTablePartitionExpr, refPartitionColRef);
        }
        AnalyzeState analyzeState = new AnalyzeState();
        final Scope scope = new Scope(RelationId.anonymous(), new RelationFields(
                icebergTable.getBaseSchema().stream()
                        .map(col -> new Field(col.getName(),
                                col.getType(), refTableName, null))
                        .collect(Collectors.toList())));
        List<ScalarOperator> externalPredicates = Lists.newArrayList();
        final List<Column> refBaseTablePartitionCols = refPartitionColRefs.stream()
                .map(ref -> icebergTable.getColumn(ref.getName()))
                .collect(Collectors.toList());
        for (PRangeCell pRangeCell : compensations) {
            List<LiteralExpr> literalExprs = pRangeCell.getRange().lowerEndpoint().getKeys();
            Preconditions.checkState(literalExprs.size() == refPartitionColRefs.size());
            List<ScalarOperator> predicates = Lists.newArrayList();
            for (int i = 0; i < literalExprs.size(); i++) {
                Column mvColumn = mvPartitionCols.get(i);
                LiteralExpr literalExpr = literalExprs.get(i);
                ColumnRefOperator refPartitionColRef = refPartitionColRefs.get(i);
                ConstantOperator expectPartitionVal =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(literalExpr);
                if (!mvColumn.isGeneratedColumn()) {
                    ScalarOperator eq = new BinaryPredicateOperator(BinaryType.EQ, refPartitionColRef,
                            expectPartitionVal);
                    predicates.add(eq);
                } else {
                    SlotRef refBaseTablePartitionExpr = refBaseTableSlotRefs.get(i);
                    Column refColumn = refBaseTablePartitionCols.get(i);
                    Expr predicateExpr = getIcebergTablePartitionPredicateExpr(icebergTable,
                            refColumn.getName(), refBaseTablePartitionExpr, literalExpr);
                    ExpressionAnalyzer.analyzeExpression(predicateExpr, analyzeState, scope, ConnectContext.get());
                    ScalarOperator predicate = SqlToScalarOperatorTranslator.translate(predicateExpr, expressionMapping,
                            optimizerContext.getColumnRefFactory());
                    predicates.add(predicate);
                }
            }
            externalPredicates.add(Utils.compoundAnd(predicates));
        }
        return Utils.compoundOr(externalPredicates);
>>>>>>> fe10512fe7 ([BugFix] Fix mv refresh bug with iceberg str2date (#60089))
    }

    @Override
    public String toString() {
        List<PRangeCell> compensations = getCompensations();
        if (CollectionUtils.isEmpty(compensations)) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[size=").append(compensations.size()).append("]");
        int size = Math.min(Config.max_mv_task_run_meta_message_values_length, compensations.size());
        sb.append(" [");
        List<String> partitions = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            PRangeCell key = compensations.get(i);
            Range<PartitionKey> range = key.getRange();
            if (range.lowerEndpoint().equals(range.upperEndpoint())) {
                partitions.add(getPartitionKeyString(range.lowerEndpoint()));
            } else {
                sb.append(getPartitionKeyString(key.getRange().lowerEndpoint()))
                        .append(" - ")
                        .append(getPartitionKeyString(key.getRange().upperEndpoint()));
            }
        }
        sb.append(Joiner.on(",").join(partitions));
        sb.append("]");
        return sb.toString();
    }

    private String getPartitionKeyString(PartitionKey key) {
        List<String> keys = key.getKeys()
                .stream()
                .map(LiteralExpr::getStringValue)
                .collect(Collectors.toList());
        return "(" + Joiner.on(",").join(keys) + ")";
    }
}


