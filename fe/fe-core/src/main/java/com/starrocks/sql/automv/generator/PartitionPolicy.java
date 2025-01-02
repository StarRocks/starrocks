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

package com.starrocks.sql.automv.generator;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.TimeGranule;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.qe.PartitionPlus;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

//TODO(by satanson): PartitionPolicy is too naive to use it in product environment,
// a new sophisticated partition policy will substitute this naive one soon to support MV
// to partition data in reasonable time granules: for examples:
// 1. partition by data_trunc('day', dt);
// 2. partition by str2date(dt, '%Y-%m-%d');
public class PartitionPolicy {

    public static Predicate<Op> getIsPartitionExprPredicate(PlanPiece aggPiece, PartitionExtractor extractor) {
        Map<Integer, PartitionPlus> partitionColumnIdToPartition = aggPiece.getPartitionColumns(extractor)
                .stream()
                .flatMap(pp -> pp.getPartitionColumns().stream().map(pc -> Pair.create(pc.first, pp)))
                .collect(Collectors.toMap(p -> p.first, p -> p.second));
        return op -> {
            if (op.getIds().size() != 1) {
                return false;
            }
            int columnId = op.getIds().getFirstId();
            return Optional.ofNullable(partitionColumnIdToPartition.get(columnId))
                    .map(pp -> !pp.isListPartitionOlapTable() || op.isVar())
                    .orElse(false);
        };
    }

    private static List<Op> getPartitionColumnsFor11MV(PlanPiece piece, PartitionExtractor partitionExtractor) {
        Preconditions.checkState(piece.isTableScan());
        List<PartitionPlus> partitions = piece.getPartitionColumns(partitionExtractor);
        if (partitions.size() != 1) {
            return Collections.emptyList();
        }
        PartitionPlus partitionPlus = partitions.get(0);
        List<Op> partitionOps = partitionPlus.getPartitionOps();
        if (partitionOps.isEmpty()) {
            return Collections.emptyList();
        }
        if (partitionPlus.isRangePartitionOlapTable()) {
            return Collections.singletonList(partitionPlus.getPartitionOps().get(0));
        } else {
            return partitionPlus.getPartitionOps();
        }
    }

    public static Optional<Integer> getPartitionColumnId(AggregatePiece aggPiece, PartitionExtractor extractor) {
        if (aggPiece.getDimensions().isEmpty()) {
            return Optional.empty();
        }

        TieredMap<Integer, GenericColumn> dimensions = aggPiece.getDimensions().merge(aggPiece.getRollupDimensions());

        TieredMap<Integer, Op> dimensionOps = OpUtil.columnsToOpMap(dimensions);

        Predicate<Op> isPartitionExpr = getIsPartitionExprPredicate(aggPiece, extractor);
        List<Pair<Integer, Op>> candidatePartitionOp = dimensionOps.entrySet()
                .stream()
                .filter(e -> isPartitionExpr.test(e.getValue()))
                .map(e -> Pair.create(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        Optional<Pair<Integer, TimeGranule>> optChosenTimeGranule = candidatePartitionOp.stream()
                .map(p -> Pair.create(p.first, Optional.ofNullable(TimeGranule.of(p.second))))
                .filter(p -> p.second.isPresent())
                .map(p -> Pair.create(p.first, p.second.get()))
                .max(Comparator.comparing(p -> p.second, TimeGranule.getComparator()));

        Supplier<Optional<Integer>> optFallbackIdSupplier = () -> candidatePartitionOp.stream()
                .filter(p -> p.second.isVar())
                .findFirst()
                .map(p -> p.first);

        if (optChosenTimeGranule.isEmpty()) {
            return optFallbackIdSupplier.get();
        }

        Pair<Integer, TimeGranule> chosenTimeGranule = optChosenTimeGranule.get();
        int timeGranuleId = chosenTimeGranule.first;
        TimeGranule timeGranule = chosenTimeGranule.second;
        if (timeGranule.isFineGrained(TimeGranule.Unit.MINUTE)) {
            return optFallbackIdSupplier.get();
        }
        return Optional.of(timeGranuleId);
    }

    public static Optional<PrettyPrinter> getPartitionExpr(AggregatePiece aggPiece,
                                                           PartitionExtractor extractor,
                                                           TieredMap<Integer, ColumnAlias> columnAliases) {
        return getPartitionColumnId(aggPiece, extractor).map(timeGranuleId ->
                new PrettyPrinter()
                        .add("PARTITION BY ")
                        .add(Objects.requireNonNull(columnAliases.get(timeGranuleId)).getName()).newLine()
        );
    }

    public static Optional<PrettyPrinter> getPartitionClauseFor11MV(PlanPiece piece,
                                                                    PartitionExtractor extractor,
                                                                    TieredMap<Integer, ColumnAlias> columnAliases) {
        List<Op> partitionOps = getPartitionColumnsFor11MV(piece, extractor);
        if (partitionOps.isEmpty()) {
            return Optional.empty();
        }
        TieredMap<Integer, ColumnAlias> unqualifiedColumnAliases = columnAliases.entrySet().stream()
                .collect(TieredMap.toMap(
                        Map.Entry::getKey,
                        e -> ColumnAlias.of(e.getValue().getName())));

        Function<Op, String> toSqlConverter = OpUtil.toOpToSqlConverter(unqualifiedColumnAliases);
        List<String> partitionExprs = partitionOps.stream().map(toSqlConverter).collect(Collectors.toList());

        if (partitionExprs.size() == 1) {
            String alias = Objects.requireNonNull(partitionExprs.get(0));
            PrettyPrinter printer = new PrettyPrinter()
                    .add("PARTITION BY ")
                    .add(alias)
                    .newLine();
            return Optional.of(printer);
        } else {
            PrettyPrinter printer = new PrettyPrinter()
                    .add("PARTITION BY (")
                    .addItems(",", partitionExprs).add(")")
                    .newLine();
            return Optional.of(printer);
        }
    }

    public static void getPartitionInfo(Table table) {
        List<Column> pColumns = PartitionUtil.getPartitionColumns(table);
    }
}
