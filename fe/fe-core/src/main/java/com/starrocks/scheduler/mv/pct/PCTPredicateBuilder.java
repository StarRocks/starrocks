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

package com.starrocks.scheduler.mv.pct;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.type.Type;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.iceberg.IcebergPartitionUtils.getIcebergTablePartitionPredicateExpr;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getStr2DateExpr;

public class PCTPredicateBuilder {
    private final MVPCTRefreshPartitioner mvRefreshPartitioner;

    public PCTPredicateBuilder(MVPCTRefreshPartitioner mvRefreshPartitioner) {
        this.mvRefreshPartitioner = mvRefreshPartitioner;
    }

    public Expr buildPartitionPredicate(Table refBaseTable,
                                        PCellSortedSet refBaseTablePartitionNames,
                                        List<Expr> mvPartitionSlotRefs) throws AnalysisException {
        if (mvRefreshPartitioner instanceof MVPCTRefreshRangePartitioner) {
            return buildRangePartitionPredicate((MVPCTRefreshRangePartitioner) mvRefreshPartitioner,
                    refBaseTable, refBaseTablePartitionNames, mvPartitionSlotRefs);
        } else if (mvRefreshPartitioner instanceof MVPCTRefreshListPartitioner) {
            return buildListPartitionPredicate((MVPCTRefreshListPartitioner) mvRefreshPartitioner,
                    refBaseTable, refBaseTablePartitionNames, mvPartitionSlotRefs);
        }
        return mvRefreshPartitioner.generatePartitionPredicate(
                refBaseTable, refBaseTablePartitionNames, mvPartitionSlotRefs);
    }

    public Expr buildMVPartitionPredicate(TableName tableName,
                                          PCellSortedSet mvPartitionNames) throws AnalysisException {
        if (mvRefreshPartitioner instanceof MVPCTRefreshRangePartitioner) {
            return buildRangeMVPartitionPredicate((MVPCTRefreshRangePartitioner) mvRefreshPartitioner,
                    tableName, mvPartitionNames);
        } else if (mvRefreshPartitioner instanceof MVPCTRefreshListPartitioner) {
            return buildListMVPartitionPredicate((MVPCTRefreshListPartitioner) mvRefreshPartitioner,
                    tableName, mvPartitionNames);
        }
        return mvRefreshPartitioner.generateMVPartitionPredicate(tableName, mvPartitionNames);
    }

    private Expr buildRangePartitionPredicate(MVPCTRefreshRangePartitioner partitioner,
                                              Table table,
                                              PCellSortedSet refBaseTablePartitionNames,
                                              List<Expr> mvPartitionSlotRefs) throws AnalysisException {
        List<Range<PartitionKey>> sourceTablePartitionRange = Lists.newArrayList();
        PCTPartitionTopology partitionTopology = partitioner.mvContext.getPartitionTopology();
        Map<Table, PCellSortedSet> refBaseTablePartitionCells =
                partitionTopology == null ? null : partitionTopology.getRefBaseTableToCellMap();
        if (refBaseTablePartitionCells == null || !refBaseTablePartitionCells.containsKey(table)) {
            throw new AnalysisException("Cannot generate mv refresh partition predicate because cannot find "
                    + "the ref base table partition cells for table:" + table.getName());
        }
        for (String partitionName : refBaseTablePartitionNames.getPartitionNames()) {
            PRangeCell rangeCell = (PRangeCell) refBaseTablePartitionCells.get(table).getPCell(partitionName);
            sourceTablePartitionRange.add(rangeCell.getRange());
        }
        sourceTablePartitionRange = MvUtils.mergeRanges(sourceTablePartitionRange);
        // for nested mv, the base table may be another mv, which is partition by str2date(dt, '%Y%m%d')
        // here we should convert date into '%Y%m%d' format
        Map<Table, List<Column>> partitionTableAndColumn = partitioner.mv.getRefBaseTablePartitionColumns();
        if (!partitionTableAndColumn.containsKey(table)) {
            partitioner.logger.warn("Cannot generate mv refresh partition predicate because cannot decide "
                    + "the partition column of table {}, partitionTableAndColumn:{}",
                    table.getName(), partitionTableAndColumn);
            return null;
        }
        List<Column> refPartitionColumns = partitionTableAndColumn.get(table);
        Preconditions.checkState(refPartitionColumns.size() == 1);
        Optional<Expr> partitionExprOpt = partitioner.mv.getRangePartitionFirstExpr();
        if (partitionExprOpt.isEmpty()) {
            return null;
        }
        Expr partitionExpr = partitionExprOpt.get();
        boolean isConvertToDate = PartitionUtil.isConvertToDate(partitionExpr, refPartitionColumns.get(0));
        if (isConvertToDate && partitionExpr instanceof FunctionCallExpr
                && !sourceTablePartitionRange.isEmpty() && MvUtils.isDateRange(sourceTablePartitionRange.get(0))) {
            Optional<FunctionCallExpr> functionCallExprOpt = getStr2DateExpr(partitionExpr);
            if (functionCallExprOpt.isEmpty()) {
                partitioner.logger.warn("Invalid partition expr:{}", partitionExpr);
                return null;
            }
            FunctionCallExpr functionCallExpr = functionCallExprOpt.get();
            Preconditions.checkState(functionCallExpr.getFunctionName().equalsIgnoreCase(FunctionSet.STR2DATE));
            String dateFormat = ((StringLiteral) functionCallExpr.getChild(1)).getStringValue();
            List<Range<PartitionKey>> converted = Lists.newArrayList();
            for (Range<PartitionKey> range : sourceTablePartitionRange) {
                Range<PartitionKey> varcharPartitionKey = MvUtils.convertToVarcharRange(range, dateFormat);
                converted.add(varcharPartitionKey);
            }
            sourceTablePartitionRange = converted;
        }
        if (mvPartitionSlotRefs.size() != 1) {
            partitioner.logger.warn("Cannot generate mv refresh partition predicate because mvPartitionSlotRefs size "
                    + "is not 1, mvPartitionSlotRefs:{}", mvPartitionSlotRefs);
            return null;
        }
        Expr mvPartitionSlotRef = mvPartitionSlotRefs.get(0);
        List<Expr> partitionPredicates = MvUtils.convertRange(mvPartitionSlotRef, sourceTablePartitionRange);
        Optional<Range<PartitionKey>> nullRange = sourceTablePartitionRange.stream()
                .filter(range -> range.lowerEndpoint().isMinValue())
                .findAny();
        if (nullRange.isPresent()) {
            partitionPredicates.add(new IsNullPredicate(mvPartitionSlotRef, false));
        }

        return ExprUtils.compoundOr(partitionPredicates);
    }

    private Expr buildRangeMVPartitionPredicate(MVPCTRefreshRangePartitioner partitioner,
                                                TableName tableName,
                                                PCellSortedSet mvPartitionNames) throws AnalysisException {
        if (mvPartitionNames.isEmpty()) {
            return new BoolLiteral(true);
        }
        PartitionInfo partitionInfo = partitioner.mv.getPartitionInfo();
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            partitioner.logger.warn("Cannot generate mv refresh partition predicate because mvPartitionExpr is invalid");
            return null;
        }
        ExpressionRangePartitionInfo rangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        List<Expr> mvPartitionExprs = rangePartitionInfo.getPartitionExprs(partitioner.mv.getIdToColumn());
        if (mvPartitionExprs.size() != 1) {
            partitioner.logger.warn("Cannot generate mv refresh partition predicate because mvPartitionExpr's size is not 1");
            return null;
        }
        Expr partitionExpr = mvPartitionExprs.get(0);

        PCTPartitionTopology partitionTopology = partitioner.mvContext.getPartitionTopology();
        PCellSortedSet mvToCellMap = partitionTopology == null ? PCellSortedSet.of() : partitionTopology.getMvToCellMap();
        List<Range<PartitionKey>> mvPartitionRange = Lists.newArrayList();
        for (String partitionName : mvPartitionNames.getPartitionNames()) {
            Preconditions.checkArgument(mvToCellMap.containsName(partitionName));
            PRangeCell rangeCell = (PRangeCell) mvToCellMap.getPCell(partitionName);
            mvPartitionRange.add(rangeCell.getRange());
        }
        mvPartitionRange = MvUtils.mergeRanges(mvPartitionRange);

        List<Expr> partitionPredicates = MvUtils.convertRange(partitionExpr, mvPartitionRange);
        Optional<Range<PartitionKey>> nullRange = mvPartitionRange.stream()
                .filter(range -> range.lowerEndpoint().isMinValue())
                .findAny();
        if (nullRange.isPresent()) {
            partitionPredicates.add(new IsNullPredicate(partitionExpr, false));
        }
        return ExprUtils.compoundOr(partitionPredicates);
    }

    private Expr buildListPartitionPredicate(MVPCTRefreshListPartitioner partitioner,
                                             Table refBaseTable,
                                             PCellSortedSet refBaseTablePartitionNames,
                                             List<Expr> mvPartitionSlotRefs) throws AnalysisException {
        PCTPartitionTopology partitionTopology = partitioner.mvContext.getPartitionTopology();
        Map<Table, PCellSortedSet> basePartitionMaps =
                partitionTopology == null ? Map.of() : partitionTopology.getRefBaseTableToCellMap();
        if (basePartitionMaps.isEmpty()) {
            return null;
        }
        PCellSortedSet baseListPartitionMap = basePartitionMaps.get(refBaseTable);
        if (baseListPartitionMap == null) {
            partitioner.logger.warn("Generate incremental partition predicate failed, "
                    + "basePartitionMaps:{} contains no refBaseTable:{}", basePartitionMaps, refBaseTable);
            return null;
        }
        if (baseListPartitionMap.isEmpty()) {
            return new BoolLiteral(true);
        }

        Map<Table, List<Column>> refBaseTablePartitionColumns = partitioner.mv.getRefBaseTablePartitionColumns();
        if (refBaseTablePartitionColumns == null || !refBaseTablePartitionColumns.containsKey(refBaseTable)) {
            partitioner.logger.warn("Generate incremental partition failed, partitionTableAndColumn {} contains no ref table {}",
                    refBaseTablePartitionColumns, refBaseTable);
            return null;
        }
        List<Column> refPartitionColumns = refBaseTablePartitionColumns.get(refBaseTable);
        if (refPartitionColumns.size() != mvPartitionSlotRefs.size()) {
            partitioner.logger.warn("Generate incremental partition failed, refPartitionColumns size {} != "
                            + "mvPartitionSlotRefs size {}",
                    refPartitionColumns.size(), mvPartitionSlotRefs.size());
            return null;
        }
        return genPartitionPredicate(refBaseTable, refBaseTablePartitionNames, mvPartitionSlotRefs,
                refPartitionColumns, baseListPartitionMap);
    }

    private Expr buildListMVPartitionPredicate(MVPCTRefreshListPartitioner partitioner,
                                               TableName tableName,
                                               PCellSortedSet mvPartitionNames) throws AnalysisException {
        PCTPartitionTopology partitionTopology = partitioner.mvContext.getPartitionTopology();
        PCellSortedSet mvToCellMap = partitionTopology == null ? PCellSortedSet.of() : partitionTopology.getMvToCellMap();
        if (mvToCellMap.isEmpty()) {
            return new BoolLiteral(true);
        }
        PartitionInfo partitionInfo = partitioner.mv.getPartitionInfo();
        Preconditions.checkArgument(partitionInfo instanceof ListPartitionInfo);

        ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
        List<Expr> mvPartitionExprs = listPartitionInfo.getPartitionExprs(tableName, partitioner.mv.getIdToColumn());
        List<Column> mvPartitionCols = partitioner.mv.getPartitionColumns();
        Preconditions.checkArgument(mvPartitionExprs.size() == mvPartitionCols.size());
        if (mvPartitionCols.size() == 1) {
            boolean isContainsNullPartition = false;
            Column refPartitionColumn = mvPartitionCols.get(0);
            List<Expr> selectedPartitionValues = Lists.newArrayList();
            Type partitionType = refPartitionColumn.getType();
            for (PCell pCell : mvPartitionNames.getPCells()) {
                PListCell cell = (PListCell) pCell;
                for (List<String> values : cell.getPartitionItems()) {
                    if (mvPartitionCols.size() != values.size()) {
                        return null;
                    }
                    LiteralExpr partitionValue = new PartitionValue(values.get(0)).getValue(partitionType);
                    if (partitionValue.isConstantNull()) {
                        isContainsNullPartition = true;
                        continue;
                    }
                    selectedPartitionValues.add(partitionValue);
                }
            }
            Expr mvPartitionExpr = mvPartitionExprs.get(0);
            Expr inPredicate = MvUtils.convertToInPredicate(mvPartitionExpr, selectedPartitionValues);
            if (isContainsNullPartition) {
                return ExprUtils.compoundOr(Lists.newArrayList(inPredicate, new IsNullPredicate(mvPartitionExpr, false)));
            } else {
                return inPredicate;
            }
        } else {
            List<Expr> partitionPredicates = Lists.newArrayList();
            for (PCell pCell : mvPartitionNames.getPCells()) {
                PListCell cell = (PListCell) pCell;
                for (List<String> values : cell.getPartitionItems()) {
                    if (mvPartitionCols.size() != values.size()) {
                        return null;
                    }
                    List<Expr> predicates = Lists.newArrayList();
                    for (int i = 0; i < mvPartitionCols.size(); i++) {
                        Column refPartitionColumn = mvPartitionCols.get(i);
                        Type partitionType = refPartitionColumn.getType();
                        LiteralExpr partitionValue = new PartitionValue(values.get(i)).getValue(partitionType);
                        Expr mvPartitionByExpr = mvPartitionExprs.get(i);
                        Expr predicate = partitionValue.isConstantNull()
                                ? new IsNullPredicate(mvPartitionByExpr, false)
                                : MvUtils.convertToInPredicate(mvPartitionByExpr, Lists.newArrayList(partitionValue));
                        predicates.add(predicate);
                    }
                    partitionPredicates.add(ExprUtils.compoundAnd(predicates));
                }
            }
            return ExprUtils.compoundOr(partitionPredicates);
        }
    }

    private static Expr getRefBaseTablePartitionPredicateExpr(Table table,
                                                              String partitionColumn,
                                                              SlotRef slotRef,
                                                              List<Expr> selectedPartitionValues) {
        if (!(table instanceof IcebergTable)) {
            return MvUtils.convertToInPredicate(slotRef, selectedPartitionValues);
        } else {
            IcebergTable icebergTable = (IcebergTable) table;
            return getIcebergTablePartitionPredicateExpr(icebergTable, partitionColumn, slotRef, selectedPartitionValues);
        }
    }

    private static @Nullable Expr genPartitionPredicate(Table refBaseTable,
                                                        PCellSortedSet refBaseTablePartitionNames,
                                                        List<Expr> refBaseTablePartitionSlotRefs,
                                                        List<Column> refPartitionColumns,
                                                        PCellSortedSet baseListPartitionMap)
            throws AnalysisException {
        Preconditions.checkArgument(refBaseTablePartitionSlotRefs.size() == refPartitionColumns.size());
        if (refPartitionColumns.size() == 1) {
            boolean isContainsNullPartition = false;
            Column refPartitionColumn = refPartitionColumns.get(0);
            List<Expr> selectedPartitionValues = Lists.newArrayList();
            Type partitionType = refPartitionColumn.getType();
            for (PCell pCell : refBaseTablePartitionNames.getPCells()) {
                PListCell cell = (PListCell) pCell;
                for (List<String> values : cell.getPartitionItems()) {
                    if (refPartitionColumns.size() != values.size()) {
                        return null;
                    }
                    LiteralExpr partitionValue = new PartitionValue(values.get(0)).getValue(partitionType);
                    if (partitionValue.isConstantNull()) {
                        isContainsNullPartition = true;
                        continue;
                    }
                    selectedPartitionValues.add(partitionValue);
                }
            }
            SlotRef refBaseTablePartitionSlotRef = (SlotRef) refBaseTablePartitionSlotRefs.get(0);
            Expr inPredicate = getRefBaseTablePartitionPredicateExpr(refBaseTable, refPartitionColumn.getName(),
                    refBaseTablePartitionSlotRef, selectedPartitionValues);
            if (isContainsNullPartition) {
                IsNullPredicate isNullPredicate = new IsNullPredicate(refBaseTablePartitionSlotRef, false);
                return ExprUtils.compoundOr(Lists.newArrayList(inPredicate, isNullPredicate));
            } else {
                return inPredicate;
            }
        } else {
            List<Expr> partitionPredicates = Lists.newArrayList();
            for (PCell pCell : refBaseTablePartitionNames.getPCells()) {
                PListCell cell = (PListCell) pCell;
                for (List<String> values : cell.getPartitionItems()) {
                    if (refPartitionColumns.size() != values.size()) {
                        return null;
                    }
                    List<Expr> predicates = Lists.newArrayList();
                    for (int i = 0; i < refPartitionColumns.size(); i++) {
                        Column refPartitionColumn = refPartitionColumns.get(i);
                        Type partitionType = refPartitionColumn.getType();
                        LiteralExpr partitionValue = new PartitionValue(values.get(i)).getValue(partitionType);
                        Expr refBaseTablePartitionExpr = refBaseTablePartitionSlotRefs.get(i);
                        Expr predicate = partitionValue.isConstantNull()
                                ? new IsNullPredicate(refBaseTablePartitionExpr, false)
                                : getRefBaseTablePartitionPredicateExpr(refBaseTable, refPartitionColumn.getName(),
                                        (SlotRef) refBaseTablePartitionExpr, Lists.newArrayList(partitionValue));
                        predicates.add(predicate);
                    }
                    partitionPredicates.add(ExprUtils.compoundAnd(predicates));
                }
            }
            return ExprUtils.compoundOr(partitionPredicates);
        }
    }
}
