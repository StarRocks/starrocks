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

package com.starrocks.sql.automv.qe;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.iceberg.IcebergPartitionTransform;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.column.OriginalColumn;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.pn.FunctionKind;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.Val;
import com.starrocks.sql.automv.util.Result;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class PartitionPlus {
    private final TablePiece tablePiece;
    private final List<Pair<Integer, GenericColumn>> partitionColumns;
    private final List<Op> partitionOps;
    private final Map<String, List<Op>> partitions;

    private PartitionPlus(TablePiece tablePiece,
                          List<Pair<Integer, GenericColumn>> partitionColumns,
                          List<Op> partitionOps,
                          Map<String, List<Op>> partitions) {
        this.tablePiece = Objects.requireNonNull(tablePiece);
        this.partitionColumns = Objects.requireNonNull(partitionColumns);
        this.partitionOps = Objects.requireNonNull(partitionOps);
        this.partitions = Objects.requireNonNull(partitions);
    }

    public static Optional<List<Op>> getPartitionOpsForExpressionPartitionOfOlapTable(TablePiece tablePiece) {
        Table table = tablePiece.getTable().getTable();
        Optional<OlapTable> optOlapTable = Util.downcast(table, OlapTable.class);
        if (!optOlapTable.isPresent()) {
            return Optional.empty();
        }
        OlapTable olapTable = optOlapTable.get();

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        Optional<List<Expr>> optPartitionExprs = Util.downcast(partitionInfo, ExpressionRangePartitionInfo.class)
                .map(info -> info.getPartitionExprs(table.getIdToColumn()));
        if (!optPartitionExprs.isPresent()) {
            optPartitionExprs = Util.downcast(partitionInfo, ExpressionRangePartitionInfoV2.class)
                    .map(info -> info.getPartitionExprs(table.getIdToColumn()));
        }

        if (!optPartitionExprs.isPresent()) {
            return Optional.empty();
        }

        Map<Integer, Integer> idReverseMap = tablePiece.getCommonState().getIdConverter().getReverseMap();
        Map<String, Integer> columnToColumnRefIds = tablePiece.getColumns().entrySet().stream()
                .map(e -> Pair.create(e.getKey(), e.getValue()))
                .filter(p -> p.second.isOriginal())
                .map(p -> Pair.create(p.first, p.second.mustCast(OriginalColumn.class)))
                .collect(ImmutableMap.toImmutableMap(p -> p.second.getColumnName(), p -> idReverseMap.get(p.first)));

        Function<Expr, Op> exprToOp = expr -> OpUtil.toOpConverter(
                        tablePiece.getCommonState().getIdConverter(),
                        tablePiece.getColumns())
                .apply(SqlToScalarOperatorTranslator.translatePartitionBy(expr, columnToColumnRefIds));
        return optPartitionExprs.map(exprs -> exprs.stream().map(exprToOp).collect(Collectors.toList()));
    }

    public static Optional<List<Op>> getPartitionOpsForTransformPartitionOfIcebergTable(
            TablePiece tablePiece, List<Pair<Integer, GenericColumn>> partitionColumnIds) {
        Table table = tablePiece.getTable().getTable();
        if (!table.isIcebergTable()) {
            return Optional.empty();
        }
        IcebergTable icebergTable = (IcebergTable) table;
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        PartitionSpec partitionSpec = nativeTable.spec();
        if (partitionSpec.isUnpartitioned() || nativeTable.specs().size() > 1) {
            return Optional.empty();
        }

        Map<String, Op> columnNameToVar = partitionColumnIds
                .stream()
                .collect(Collectors.toMap(
                        p -> Objects.requireNonNull(p.second.getColumnName()),
                        p -> OpUtil.columnToOp(p.first, p.second)));
        List<Op> partitionOps = Lists.newArrayList();
        for (PartitionField partitionField : partitionSpec.fields()) {
            IcebergPartitionTransform transform =
                    IcebergPartitionTransform.fromString(partitionField.transform().toString());
            String partitionColumnName = nativeTable.schema().findColumnName(partitionField.sourceId());
            Op var = Objects.requireNonNull(columnNameToVar.get(partitionColumnName));
            Type type = var.getType();
            if (!type.isFixedPointType() && !type.isDateType() && !type.isStringType()) {
                return Optional.empty();
            }
            switch (transform) {
                case YEAR:
                case MONTH:
                case DAY:
                case HOUR:
                    Op timeUnit = Op.val(ConstantOperator.createChar(transform.name().toLowerCase()));
                    List<Op> args = ImmutableList.of(timeUnit, var);
                    Op dateTruncOp = Op.apply(Type.DATE, FunctionKind.of(FunctionSet.DATE_TRUNC), true, args);
                    partitionOps.add(dateTruncOp);
                    break;
                case IDENTITY:
                    partitionOps.add(var);
                    break;
                default:
                    return Optional.empty();
            }
        }
        return Optional.of(partitionOps);
    }

    public static PartitionPlus of(TablePiece tablePiece, PartitionExtractor extractor) {
        Table table = tablePiece.getTable().getTable();
        List<Column> partitionColumns = PartitionUtil.getPartitionColumns(table);
        Function<Column, Pair<Integer, GenericColumn>> getColumn = column ->
                tablePiece.getColumns().entrySet().stream()
                        .filter(e -> Objects.equals(e.getValue().getColumnName(), column.getName()))
                        .findFirst()
                        .map(e -> Pair.create(e.getKey(), e.getValue()))
                        .orElseThrow();

        List<Pair<Integer, GenericColumn>> partitionColumnIds = partitionColumns
                .stream()
                .map(getColumn)
                .collect(Collectors.toList());
        Optional<List<Op>> optPartitionOps = getPartitionOpsForExpressionPartitionOfOlapTable(tablePiece);
        if (!optPartitionOps.isPresent()) {
            optPartitionOps = getPartitionOpsForTransformPartitionOfIcebergTable(tablePiece, partitionColumnIds);
        }

        List<Op> partitionOps = optPartitionOps.orElseGet(() -> partitionColumnIds
                .stream().map(p -> OpUtil.columnToOp(p.first, p.second)).collect(Collectors.toList()));
        Preconditions.checkState(partitionOps.size() == partitionColumnIds.size());
        Preconditions.checkState(partitionColumnIds.isEmpty() || IntStream.range(0, partitionColumnIds.size())
                .anyMatch(i -> partitionOps.get(i).getIds().contains(partitionColumnIds.get(i).first)));

        Map<String, List<Op>> partitions =
                Optional.ofNullable(extractor).map(e -> e.getCachedOrExtract(tablePiece.getTable()))
                        .orElseGet(Collections::emptyMap);

        return new PartitionPlus(tablePiece, partitionColumnIds, partitionOps, partitions);
    }

    public TablePiece getTablePiece() {
        return tablePiece;
    }

    public List<Pair<Integer, GenericColumn>> getPartitionColumns() {
        return partitionColumns;
    }

    public List<Op> getPartitionOps() {
        return partitionOps;
    }

    public Map<String, List<Op>> getPartitions() {
        return partitions;
    }

    Predicate<ConstantOperator> canApplyStr2date(String fmt) {
        ConstantOperator fmtOp = ConstantOperator.createVarchar(fmt);
        return dateOp -> Result.wrap(() -> ScalarOperatorFunctions.str2Date(dateOp, fmtOp)).unwrap().isPresent();
    }

    public boolean isRangePartitionOlapTable() {
        return Util.downcast(tablePiece.getTable().getTable(), OlapTable.class)
                .map(olapTable -> olapTable.getPartitionInfo().isRangePartition())
                .orElse(false);
    }

    public boolean isListPartitionOlapTable() {
        return Util.downcast(tablePiece.getTable().getTable(), OlapTable.class)
                .map(olapTable -> olapTable.getPartitionInfo().isListPartition())
                .orElse(false);
    }

    // timeFormat as follows:
    // "%Y%m%d": 20240101
    // "%Y-%m-%d": 2024-01-01
    Optional<Op> inferTimeFormat(List<String> timeFormats) {
        boolean isOlapTable = Util.downcast(tablePiece.getTable().getTable(), OlapTable.class).isPresent();
        if (isOlapTable) {
            return Optional.empty();
        }

        for (int i = 0; i < partitionColumns.size(); ++i) {
            Pair<Integer, GenericColumn> idAndColumn = partitionColumns.get(i);
            Op var = OpUtil.columnToOp(idAndColumn.first, idAndColumn.second);
            // only string type, need infer str2date fmt
            if (!var.getType().isStringType()) {
                continue;
            }
            int idx = i;
            List<ConstantOperator> values = partitions.values().stream()
                    .map(preds -> preds.get(idx))
                    .map(pred -> pred.collect(op -> op.isVal() && !op.isNullVal() && op.getType().isStringType()))
                    .flatMap(Collection::stream)
                    .map(val -> val.mustCast(Val.class).getValue())
                    .collect(Collectors.toList());
            int sz90Par = (int) (Math.max(values.size(), 1) * 0.90);
            Optional<Op> optStr2dateOp = timeFormats.stream()
                    .filter(fmt -> values.stream().filter(v -> canApplyStr2date(fmt).test(v)).count() >= sz90Par)
                    .findFirst()
                    .map(fmt -> Op.val(ConstantOperator.createChar(fmt)))
                    .map(fmt -> ImmutableList.of(var, fmt))
                    .map(args -> Op.apply(ScalarType.DATE, FunctionKind.of(FunctionSet.STR2DATE), true, args));
            if (optStr2dateOp.isPresent()) {
                return optStr2dateOp;
            }
        }
        return Optional.empty();
    }

    private boolean isPreferRange() {
        return GlobalVariable.isAutoMVPreferRangePartition();
    }

    private List<String> getStringTimeFormats() {
        String csvTimeFmts = GlobalVariable.getAutoMVStringTimeFormats();
        List<String> defaultTimeFmts = ImmutableList.of("%Y%m%d", "%Y-%m-%d");
        if (csvTimeFmts == null) {
            return defaultTimeFmts;
        }

        return Stream.concat(
                        Stream.of(csvTimeFmts.strip().split("\\s*,\\s*")).filter(s -> s != null && !s.isEmpty()),
                        defaultTimeFmts.stream())
                .distinct()
                .collect(Collectors.toList());
    }

    public Optional<Op> chosePartitionOp() {
        if (partitionColumns.isEmpty()) {
            return Optional.empty();
        }
        Optional<Op> op = inferTimeFormat(getStringTimeFormats());
        // only for external table, when prefer a range-partition, we use infer time format
        if (op.isPresent() && isPreferRange()) {
            return op;
        } else if (op.isPresent()) {
            // use the partition column directly
            return op.get().collect(Op::isVar).stream().findFirst();
        } else {
            // prefer date/datetime partition column, then first partition column.
            Pair<Integer, GenericColumn> idAndColumn = partitionColumns
                    .stream().filter(p -> p.second.getType().isDateType())
                    .findFirst()
                    .orElse(partitionColumns.get(0));
            return Optional.of(OpUtil.columnToOp(idAndColumn.first, idAndColumn.second));
        }
    }
}
