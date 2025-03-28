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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
                                                       List<ColumnRefOperator> refPartitionColRefs) {
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
    }

    @Override
    public String toString() {
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

    public static TableCompensation build(Table refBaseTable,
                                          MvUpdateInfo mvUpdateInfo,
                                          Optional<LogicalScanOperator> scanOperatorOpt) {
        MaterializedView mv = mvUpdateInfo.getMv();
        Set<String> toRefreshPartitionNames = mvUpdateInfo.getBaseTableToRefreshPartitionNames(refBaseTable);
        if (toRefreshPartitionNames == null) {
            logMVRewrite(mv.getName(), "MV's ref base table {} to refresh partition is null, unknown state",
                    refBaseTable.getName());
            return null;
        }
        if (toRefreshPartitionNames.isEmpty()) {
            logMVRewrite(mv.getName(), "MV's ref base table {} to refresh partition is empty, no need compensate",
                    refBaseTable.getName());
            return TableCompensation.noCompensation();
        }

        final List<PRangeCell> toRefreshPartitionKeys = Lists.newArrayList();
        MVTransparentState state;
        if (MvPartitionCompensator.isSupportPartitionPruneCompensate(refBaseTable) && scanOperatorOpt.isPresent()) {
            state = getToRefreshPartitionKeysWithPruner(refBaseTable, mv, toRefreshPartitionNames, toRefreshPartitionKeys,
                    scanOperatorOpt.get());
        } else {
            state = getToRefreshPartitionKeysWithoutPruner(refBaseTable, mvUpdateInfo, toRefreshPartitionNames,
                    toRefreshPartitionKeys);
        }
        if (state == null) {
            logMVRewrite(mv.getName(), "Failed to get partition keys for ref base table: {}", refBaseTable.getName());
            return TableCompensation.unknown();
        }
        if (state.isNoCompensate()) {
            return TableCompensation.noCompensation();
        }
        if (state.isUncompensable()) {
            return TableCompensation.create(state);
        }
        return new ExternalTableCompensation(refBaseTable, toRefreshPartitionKeys);
    }


    /**
     * For ref base table that doesn't support partition prune, get the partition keys to refresh.
     * NOTE: for table that doesn't support partition prune, filter the partition keys to refresh by partition names.
     * TODO: unify the logic with `getToRefreshPartitionKeysWithPruner`
     * TODO: it's not accurate since the partition key may be different for null value.
     */
    private static MVTransparentState getToRefreshPartitionKeysWithoutPruner(Table refBaseTable,
                                                                             MvUpdateInfo mvUpdateInfo,
                                                                             Set<String> toRefreshPartitionNames,
                                                                             final List<PRangeCell> toRefreshPartitionKeys) {
        MvBaseTableUpdateInfo baseTableUpdateInfo = mvUpdateInfo.getBaseTableUpdateInfos().get(refBaseTable);
        if (baseTableUpdateInfo == null) {
            return null;
        }
        // use update info's partition to cells since it's accurate.
        Map<String, PCell> nameToPartitionKeys = baseTableUpdateInfo.getPartitionToCells();
        MaterializedView mv = mvUpdateInfo.getMv();
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        if (!refBaseTablePartitionColumns.containsKey(refBaseTable)) {
            return null;
        }
        List<Column> partitionColumns = refBaseTablePartitionColumns.get(refBaseTable);
        try {
            for (String partitionName : toRefreshPartitionNames) {
                if (!nameToPartitionKeys.containsKey(partitionName)) {
                    return null;
                }
                PCell pCell = nameToPartitionKeys.get(partitionName);
                if (pCell instanceof PRangeCell) {
                    toRefreshPartitionKeys.add(((PRangeCell) pCell));
                } else if (pCell instanceof PListCell) {
                    final List<PartitionKey> keys = ((PListCell) pCell).toPartitionKeys(partitionColumns);
                    keys.stream()
                            .map(key -> PRangeCell.of(key))
                            .forEach(toRefreshPartitionKeys::add);
                }
            }
        } catch (Exception e) {
            logMVRewrite("Failed to get partition keys for ref base table: {}", refBaseTable.getName(),
                    DebugUtil.getStackTrace(e));
            return null;
        }
        return MVTransparentState.COMPENSATE;
    }

    /**
     * Get ref base table's compensate state of the mv, and update partition keys to refresh for the ref base table.
     * NOTE: for table that supports partition prune, the selected partition ids/keys are only set for scan operator which
     * can be used to retain the selected partitions to refresh.
     */
    private static MVTransparentState getToRefreshPartitionKeysWithPruner(Table refBaseTable,
                                                                          MaterializedView mv,
                                                                          Set<String> toRefreshPartitionNames,
                                                                          List<PRangeCell> toRefreshPartitionKeys,
                                                                          LogicalScanOperator scanOperator) {
        // selected partition ids/keys are only set for scan operator that supports partition prune.
        List<PartitionKey> selectPartitionKeys = null;
        Collection<Long> selectPartitionIds = null;
        try {
            ScanOperatorPredicates scanOperatorPredicates = scanOperator.getScanOperatorPredicates();
            selectPartitionIds = scanOperatorPredicates.getSelectedPartitionIds();
            selectPartitionKeys = scanOperatorPredicates.getSelectedPartitionKeys();
        } catch (Exception e) {
            return null;
        }
        // For scan operator that supports prune partitions with OptExternalPartitionPruner,
        // we could only compensate partitions which selected partitions need to refresh.
        if (MvPartitionCompensator.isSupportPartitionPruneCompensate(refBaseTable)) {
            if (CollectionUtils.isEmpty(selectPartitionIds)) {
                // see OptExternalPartitionPruner#computePartitionInfo:
                // it's different meaning when selectPartitionIds is null and empty for hive and other tables
                if (refBaseTable instanceof HiveTable) {
                    logMVRewrite(mv.getName(), "Selected partition ids is empty for ref base table: {}, " +
                            "skip to compensate", refBaseTable.getName());
                    return MVTransparentState.NO_COMPENSATE;
                } else {
                    logMVRewrite(mv.getName(), "Selected partition ids is empty for ref base table: {}, " +
                            "unknown state", refBaseTable.getName());
                    return MVTransparentState.UNKNOWN;
                }
            }
        }
        if (selectPartitionKeys == null) {
            logMVRewrite(mv.getName(), "Failed to get partition keys for ref base table: {}", refBaseTable.getName());
            return null;
        }
        List<Integer> colIndexes = PartitionUtil.getRefBaseTablePartitionColumIndexes(mv, refBaseTable);
        if (colIndexes == null) {
            logMVRewrite(mv.getName(), "Failed to get partition column indexes for ref base table: {}",
                    refBaseTable.getName());
            return null;
        }
        Set<PartitionKey> newSelectedPartitionKeys = selectPartitionKeys
                .stream()
                .map(p -> PartitionUtil.getSelectedPartitionKey(p, colIndexes))
                .collect(Collectors.toSet());
        Map<String, PartitionKey> selectPartitionNameToKeys = newSelectedPartitionKeys.stream()
                .map(key -> Pair.create(PartitionUtil.generateMVPartitionName(key), key))
                .collect(Collectors.toMap(p -> p.first, p -> p.second));

        // NOTE: use partition names rather than partition keys since the partition key may be different for null value.
        // if all selected partitions need to refresh, no need to rewrite.
        Set<String> selectPartitionNames = selectPartitionNameToKeys.keySet();
        if (toRefreshPartitionNames.containsAll(selectPartitionNames)) {
            logMVRewrite(mv.getName(), "All external table {}'s selected partitions {} need to refresh, no rewrite",
                    refBaseTable.getName(), selectPartitionKeys);
            return MVTransparentState.NO_REWRITE;
        }
        // filter the selected partitions to refresh.
        toRefreshPartitionNames.retainAll(selectPartitionNames);

        // if no partition needs to refresh, no need to compensate.
        if (toRefreshPartitionNames.isEmpty()) {
            return MVTransparentState.NO_COMPENSATE;
        }
        // only retain the selected partitions to refresh.
        toRefreshPartitionNames
                .stream()
                .map(selectPartitionNameToKeys::get)
                .map(key -> PRangeCell.of(key))
                .forEach(toRefreshPartitionKeys::add);
        return MVTransparentState.COMPENSATE;
    }
}
