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

package com.starrocks.sql.automv.pieces;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.TableName;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.column.OriginalColumn;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PlanPieceNormalizer extends PlanPieceVisitor<PlanPiece, ColumnUnfolder> {

    private static final PlanPieceNormalizer INSTANCE = new PlanPieceNormalizer();

    private static TieredMap<Integer, GenericColumn> normalizeDuplicateOriginalColumns(
            Collection<List<TablePiece>> duplicatePieces) {
        TieredMap<Integer, GenericColumn> allColumnNorms = TieredMap.genesis();
        for (List<TablePiece> pieces : duplicatePieces) {
            Supplier<Integer> idGen = Util.nextIdGenerator();
            FQTable fqTable = pieces.get(0).getTable();
            TableName fqTableName = fqTable.getFqTableName();
            for (TablePiece tablePiece : pieces) {
                Preconditions.checkArgument(tablePiece.getTableName().equals(fqTable.getFQName()));
                String catalog = fqTableName.getCatalog();
                String db = fqTableName.getDb();
                String tableName = fqTableName.getTbl() + "(" + idGen.get() + ")";
                TableName uniqueFqTableName = new TableName(catalog, db, tableName);
                TieredMap<Integer, GenericColumn> columnNorms = tablePiece.getColumns().entrySet().stream()
                        .filter(e -> e.getValue().isOriginal())
                        .map(e -> Pair.create(
                                e.getKey(),
                                e.getValue().mustCast(OriginalColumn.class).normalize(uniqueFqTableName)))
                        .collect(TieredMap.toMap(p -> p.first, p -> p.second));
                allColumnNorms = allColumnNorms.merge(columnNorms);
            }
        }
        return allColumnNorms;
    }

    private static TieredMap<Integer, GenericColumn> normalizeOriginalColumns(
            Collection<TablePiece> pieces) {
        TieredMap<Integer, GenericColumn> allColumnNorms = TieredMap.genesis();
        for (TablePiece piece : pieces) {
            TieredMap<Integer, GenericColumn> columnNorms = piece.getColumns().entrySet().stream()
                    .filter(e -> e.getValue().isOriginal())
                    .map(e -> Pair.create(
                            e.getKey(),
                            e.getValue().mustCast(OriginalColumn.class).normalize()))
                    .collect(TieredMap.toMap(p -> p.first, p -> p.second));
            allColumnNorms = allColumnNorms.merge(columnNorms);
        }
        return allColumnNorms;
    }

    public static PlanPiece normalizeTopPiece(PlanPiece piece, TieredMap<Integer, GenericColumn> norms) {
        ColumnUnfolder unfolder = new ColumnUnfolder(norms);
        return INSTANCE.visit(piece, unfolder);
    }

    public static PlanPiece normalize(PlanPiece piece) {
        List<TablePiece> unorderedPieces = PlanPiece.collect(piece, TablePiece.class);
        TieredMap<Integer, GenericColumn> columnNorms = normalizeOriginalColumns(unorderedPieces);
        PlanPiece normalizedPiece = INSTANCE.normalize(piece, new ColumnUnfolder(columnNorms));

        List<TablePiece> orderedTablePieces = PlanPiece.collect(normalizedPiece, TablePiece.class);
        Map<String, List<TablePiece>> fqNameToTablePieces = orderedTablePieces.stream()
                .collect(Collectors.groupingBy(TablePiece::getTableName));
        Map<String, List<TablePiece>> duplicateTablePieces = fqNameToTablePieces.entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!duplicateTablePieces.isEmpty()) {
            columnNorms = normalizeDuplicateOriginalColumns(duplicateTablePieces.values());
            normalizedPiece = INSTANCE.normalize(piece, new ColumnUnfolder(columnNorms));
        }
        return normalizedPiece;
    }

    private PrettyPrinter normalizeCommonInfo(PlanPiece piece, ColumnUnfolder columnUnfolder) {
        PrettyPrinter printer = new PrettyPrinter();
        Map<Boolean, TieredMap<Integer, GenericColumn>> columnGroups = piece.getColumns().entrySet()
                .stream()
                .collect(Collectors.partitioningBy(e -> e.getValue().isOriginal(), TieredMap.toMap()));
        TieredMap<Integer, GenericColumn> originalColumns = columnGroups.get(true);
        TieredMap<Integer, GenericColumn> derivedColumns = columnGroups.get(false);

        // for flat table under AggregatePiece, we just normalize its conjuncts, but
        // the conjuncts are not taken into consideration in flat table's normalized from.
        Optional<PlanPiece> optParent = Optional.ofNullable(piece.getAuxState().getParent());
        boolean isFlatTableUnderAgg = optParent
                .map(parent -> parent.isTop() && parent.isAggregate())
                .orElse(false);

        List<String> columnList;
        List<String> conjunctList;
        if (isFlatTableUnderAgg) {
            columnList = columnUnfolder.unfold(originalColumns);
            columnUnfolder.unfold(derivedColumns);
            piece.getAuxState().setConjuncts(columnUnfolder.unfold(piece.getConjuncts()));
            conjunctList = Collections.emptyList();
        } else {
            columnList = columnUnfolder.unfold(piece.getColumns());
            conjunctList = columnUnfolder.unfold(piece.getConjuncts());
        }

        List<PrettyPrinter> items = Arrays.asList(
                new PrettyPrinter().addKeyValue("piece", piece.getClass().getSimpleName()),
                new PrettyPrinter().addNameToArray("columns", columnList),
                new PrettyPrinter().addNameToArray("conjuncts", conjunctList)
        );
        return printer.addObject(items);
    }

    @Override
    public PlanPiece visitTable(TablePiece tablePiece, ColumnUnfolder columnUnfolder) {
        PrettyPrinter printer = new PrettyPrinter();
        List<PrettyPrinter> items = Arrays.asList(
                new PrettyPrinter().addNameToObject("common", normalizeCommonInfo(tablePiece, columnUnfolder)),
                new PrettyPrinter().addKeyValue("tableName", tablePiece.getTableName())
        );
        printer.addObject(items);
        tablePiece.getAuxState().setNorm(printer);
        return tablePiece;
    }

    private PrettyPrinter normalizeStarJoinCorner(StarJoinPiece.StarCorner corner, ColumnUnfolder columnUnfolder) {
        PrettyPrinter printer = new PrettyPrinter();
        PlanPiece normalizedCorner = visit(corner.getPiece(), columnUnfolder);
        PrettyPrinter cornerNorm = normalizedCorner.getAuxState().getNorm();
        List<String> eqConjuncts = columnUnfolder.unfold(corner.getEqConjuncts());
        List<String> otherConjuncts = columnUnfolder.unfold(corner.getOtherConjuncts());
        List<PrettyPrinter> items = Arrays.asList(
                new PrettyPrinter().addKeyValue("joinType", corner.getJoinType().toSql()),
                new PrettyPrinter().addNameToArray("eqConjuncts", eqConjuncts),
                new PrettyPrinter().addNameToArray("otherConjuncts", otherConjuncts),
                new PrettyPrinter().addNameToObject("piece", cornerNorm)
        );
        return printer.addObject(items);
    }

    @Override
    public PlanPiece visitStarJoin(StarJoinPiece joinPiece, ColumnUnfolder columnUnfolder) {
        PlanPiece normalizedCentre = visit(joinPiece.getCentre(), columnUnfolder);
        PrettyPrinter centreNorm = normalizedCentre.getAuxState().getNorm();

        List<Pair<StarJoinPiece.StarCorner, PrettyPrinter>> normalizedCornersAndNorms =
                joinPiece.getCorners().stream()
                        .map(corner -> Pair.create(corner, normalizeStarJoinCorner(corner, columnUnfolder)))
                        .sorted(Comparator.comparing(p -> p.second.getResult()))
                        .collect(Collectors.toList());

        List<StarJoinPiece.StarCorner> normalizedCorners =
                normalizedCornersAndNorms.stream().map(p -> p.first).collect(Collectors.toList());
        List<PrettyPrinter> cornerNorms =
                normalizedCornersAndNorms.stream().map(p -> p.second).collect(Collectors.toList());

        PrettyPrinter printer = new PrettyPrinter();
        List<PrettyPrinter> items = Arrays.asList(
                new PrettyPrinter().addNameToObject("common", normalizeCommonInfo(joinPiece, columnUnfolder)),
                new PrettyPrinter().addNameToObject("centre", centreNorm),
                new PrettyPrinter().addNameToSuperStepArray("corners", cornerNorms)
        );

        printer.addObject(items);
        PlanPiece newJoinPiece = joinPiece.builder().mustCast(StarJoinPiece.Builder.class)
                .setCentre(normalizedCentre)
                .setCorners(normalizedCorners)
                .build();

        newJoinPiece.getAuxState().setNorm(printer);
        return newJoinPiece;
    }

    @Override
    public PlanPiece visitAggregate(AggregatePiece aggPiece, ColumnUnfolder columnUnfolder) {
        PlanPiece normalizedFlatTable = visit(aggPiece.getFlatTable(), columnUnfolder);
        PrettyPrinter flatTableNorm = normalizedFlatTable.getAuxState().getNorm();
        List<String> dimensionNorms = columnUnfolder.unfold(aggPiece.getDimensions());
        List<String> rollupDimensionNorms = columnUnfolder.unfold(aggPiece.getRollupDimensions());
        List<String> metricNorms = columnUnfolder.unfold(aggPiece.getMetrics());
        List<String> distinctMetricNorms = columnUnfolder.unfold(aggPiece.getDistinctMetrics());
        columnUnfolder.unfold(aggPiece.getHoistConjuncts());
        columnUnfolder.unfold(aggPiece.getNonHoistConjuncts());
        PrettyPrinter printer = new PrettyPrinter();
        List<PrettyPrinter> items = Arrays.asList(
                new PrettyPrinter().addNameToObject("common", normalizeCommonInfo(aggPiece, columnUnfolder)),
                new PrettyPrinter().addNameToArray("dimension", dimensionNorms),
                new PrettyPrinter().addNameToArray("rollupDimension", rollupDimensionNorms),
                new PrettyPrinter().addNameToArray("metrics", metricNorms),
                new PrettyPrinter().addNameToArray("distinctMetrics", distinctMetricNorms),
                new PrettyPrinter().addNameToObject("flatTable", flatTableNorm)
        );
        printer.addObject(items);
        PlanPiece newAggPiece = aggPiece.builder().mustCast(AggregatePiece.Builder.class)
                .setFlatTable(normalizedFlatTable)
                .build();
        newAggPiece.getAuxState().setNorm(printer);
        return newAggPiece;
    }

    private PlanPiece normalize(PlanPiece piece, ColumnUnfolder columnUnfolder) {
        return piece.accept(this, columnUnfolder);
    }
}
