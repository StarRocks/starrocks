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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.connector.PartitionUtil;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionPlus {
    private final TablePiece tablePiece;
    private final List<Pair<Integer, GenericColumn>> partitionColumns;
    private final Op partitionOp;
    private final Map<String, List<Op>> partitions;

    private PartitionPlus(TablePiece tablePiece, List<Pair<Integer, GenericColumn>> partitionColumns, Op partitionOp,
                          Map<String, List<Op>> partitions) {
        this.tablePiece = Objects.requireNonNull(tablePiece);
        this.partitionColumns = Objects.requireNonNull(partitionColumns);
        this.partitionOp = Objects.requireNonNull(partitionOp);
        this.partitions = Objects.requireNonNull(partitions);
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

        Map<Integer, Integer> idReserveMap = tablePiece.getCommonState().getIdConverter().getReverseMap();
        Map<String, Integer> columnToColumnRefIds = tablePiece.getColumns().entrySet().stream()
                .map(e -> Pair.create(e.getKey(), e.getValue()))
                .filter(p -> p.second.isOriginal())
                .map(p -> Pair.create(p.first, p.second.mustCast(OriginalColumn.class)))
                .collect(ImmutableMap.toImmutableMap(p -> p.second.getColumnName(), p -> idReserveMap.get(p.first)));

        Function<Expr, Op> exprToOp = expr -> OpUtil.toOpConverter(
                        tablePiece.getCommonState().getIdConverter(),
                        tablePiece.getColumns())
                .apply(SqlToScalarOperatorTranslator.translatePartitionBy(expr, columnToColumnRefIds));

        Op partitionOp = Util.downcast(table, OlapTable.class)
                .map(OlapTable::getPartitionInfo)
                .map(partitionInfo ->
                        Util.downcast(partitionInfo, ExpressionRangePartitionInfo.class)
                                .map(exprPartitionInfo -> exprPartitionInfo.getPartitionExprs(table.getIdToColumn()))
                                .orElseGet(() -> Util.downcast(partitionInfo, ExpressionRangePartitionInfoV2.class)
                                        .map(exprPartitionInfo -> exprPartitionInfo.getPartitionExprs(
                                                table.getIdToColumn()))
                                        .orElse(null)))
                .map(exprs -> exprs.get(0))
                .map(exprToOp)
                .orElse(Val.NULL_VAL);

        Map<String, List<Op>> partitions =
                Optional.ofNullable(extractor).map(e -> e.getCachedOrExtract(tablePiece.getTable()))
                        .orElseGet(Collections::emptyMap);

        return new PartitionPlus(tablePiece, partitionColumnIds, partitionOp, partitions);
    }

    public TablePiece getTablePiece() {
        return tablePiece;
    }

    public List<Pair<Integer, GenericColumn>> getPartitionColumns() {
        return partitionColumns;
    }

    public Op getPartitionOp() {
        return partitionOp;
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
        if (!partitionOp.isNullVal()) {
            Preconditions.checkState(partitionColumns.size() == 1);
            return Optional.of(partitionOp);
        } else {
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
}
