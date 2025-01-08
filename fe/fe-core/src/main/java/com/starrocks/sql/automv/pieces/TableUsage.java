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

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.DerivedColumn;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.StrictOp;
import com.starrocks.sql.automv.pn.Var;
import com.starrocks.sql.automv.util.Box;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableUsage {
    private final PlanPiece piece;
    private final TablePiece tablePiece;
    private final ColumnRefSet usedColumns;
    private final Map<Integer, TieredList<Op>> pushDownConjuncts;
    private final List<ColumnRefSet> joinKeys;
    private final List<ColumnRefSet> groupByKeys;

    private transient TieredList<Op> whereConjuncts;
    private transient Map<Integer, Map<StrictOp, Long>> conjunctFreq;

    public TableUsage(PlanPiece piece, TablePiece tablePiece, ColumnRefSet usedColumns,
                      Map<Integer, TieredList<Op>> pushDownConjuncts, List<ColumnRefSet> joinKeys,
                      List<ColumnRefSet> groupByKeys) {
        this.piece = piece;
        this.tablePiece = tablePiece;
        this.usedColumns = usedColumns;
        this.pushDownConjuncts = pushDownConjuncts;
        this.joinKeys = joinKeys;
        this.groupByKeys = groupByKeys;
    }

    public static List<TableUsage> analyzeUsage(PlanPiece piece) {
        return Visitor.INSTANCE.analyze(piece);
    }

    public static List<TableUsage> mergeUsages(List<TableUsage> tableUsages) {
        Map<String, List<TableUsage>> tableUsageGroups = tableUsages
                .stream()
                .collect(Collectors.groupingBy(tableUsage -> tableUsage.tablePiece.getTable().getFQName()));

        return tableUsageGroups.values().stream()
                .map(TableUsage::mergeUsagesOfIdenticalTable)
                .collect(Collectors.toList());
    }

    private static TableUsage mergeUsagesOfIdenticalTable(List<TableUsage> tableUsages) {
        Preconditions.checkState(!tableUsages.isEmpty());
        if (tableUsages.size() == 1) {
            return tableUsages.get(0);
        }

        TableUsage initTableUsage = tableUsages.get(0);
        TablePiece initTablePiece = initTableUsage.tablePiece;

        ColumnRefToIdConverter newIdConverter = initTablePiece.getCommonState().getIdConverter().duplicate();

        Map<Boolean, TieredMap<Integer, GenericColumn>> columnGroups = initTablePiece.getColumns().entrySet()
                .stream()
                .collect(Collectors.partitioningBy(e -> e.getValue().isOriginal(), TieredMap.toMap()));

        TieredMap<Integer, GenericColumn> initOriginalColumns = columnGroups.get(true);
        TieredMap<Integer, GenericColumn> initDerivedColumns = columnGroups.get(false);

        Map<String, Integer> initNormToIdMap = initOriginalColumns.entrySet()
                .stream()
                .collect(TieredMap.toMap(
                        e -> e.getValue().getNorm().toString(),
                        Map.Entry::getKey));

        Set<String> uniqueDerivedColumns = initDerivedColumns.values()
                .stream()
                .map(col -> col.getNorm().toString())
                .collect(Collectors.toSet());

        ColumnRefSet mergedUsedColumns = ColumnRefSet.of();
        mergedUsedColumns.union(initTableUsage.usedColumns);
        TieredList<ColumnRefSet> mergedGroupByKeys = TieredList.<ColumnRefSet>genesis()
                .concat(initTableUsage.groupByKeys);
        TieredList<ColumnRefSet> mergedJoinKeys = TieredList.<ColumnRefSet>genesis()
                .concat(initTableUsage.joinKeys);
        Map<Integer, TieredList<Op>> mergedPushDownPredicates = Maps.newHashMap(initTableUsage.pushDownConjuncts);
        TieredMap<Integer, GenericColumn> mergedColumns = initTablePiece.getColumns();
        TieredMap.Builder<Integer, GenericColumn> newDerivedColumnBuilder = TieredMap.newGenesisTier();
        Set<String> mergedCoveredQueries = new HashSet<String>(initTablePiece.getCommonState().getCoveredQueries());

        for (TableUsage tableUsage : tableUsages.subList(1, tableUsages.size())) {
            TablePiece piece = tableUsage.tablePiece;
            Map<Integer, Integer> idToInitId = piece.getColumns().entrySet()
                    .stream()
                    .filter(e -> piece.getUsedColumns().contains(e.getKey()))
                    .collect(ImmutableMap.toImmutableMap(
                            Map.Entry::getKey,
                            e -> initNormToIdMap.get(e.getValue().getNorm().toString())));

            List<TieredList<ColumnRefSet>> keys = Stream.of(
                            Collections.singletonList(tableUsage.usedColumns),
                            tableUsage.groupByKeys,
                            tableUsage.joinKeys)
                    .map(keyList -> keyList.stream()
                            .map(key -> key.getStream().map(idToInitId::get).collect(Collectors.toList()))
                            .map(ColumnRefSet::createByIds)
                            .collect(TieredList.<ColumnRefSet>toList()))
                    .collect(Collectors.toList());

            Map<Integer, TieredList<Op>> pushDownConjuncts = tableUsage.pushDownConjuncts.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> idToInitId.get(e.getKey()),
                            e -> OpUtil.substId(e.getValue(), idToInitId)));

            mergedUsedColumns.union(keys.get(0).get(0));
            mergedGroupByKeys = mergedGroupByKeys.concat(keys.get(1));
            mergedJoinKeys = mergedJoinKeys.concat(keys.get(2));
            pushDownConjuncts.forEach((k, v) -> mergedPushDownPredicates.merge(k, v, TieredList::<Op>concat));

            mergedCoveredQueries.addAll(piece.getCommonState().getCoveredQueries());

            for (Map.Entry<Integer, GenericColumn> e : piece.getColumns().entrySet()) {
                Optional<DerivedColumn> optDerivedColumn = e.getValue().cast(DerivedColumn.class);
                if (optDerivedColumn.isEmpty()) {
                    continue;
                }
                DerivedColumn derivedColumn = optDerivedColumn.get();
                String norm = derivedColumn.getNorm().toString();
                if (uniqueDerivedColumns.contains(norm)) {
                    continue;
                }
                uniqueDerivedColumns.add(norm);
                Op op = OpUtil.substId(derivedColumn.getOp(), idToInitId);
                newDerivedColumnBuilder.put(newIdConverter.nextId(), GenericColumn.derived(op));
            }
        }
        mergedColumns = mergedColumns.merge(newDerivedColumnBuilder.build());
        PieceCommonState newCommonState = new PieceCommonState(
                newIdConverter,
                mergedCoveredQueries,
                initTablePiece.getCommonState().getFqTableMap());

        TieredList<Op> mergedConjuncts = mergedPushDownPredicates.values()
                .stream()
                .flatMap(conjuncts -> conjuncts
                        .stream()
                        .map(Op::strict)
                        .collect(Collectors.toSet())
                        .stream().map(StrictOp::getOp))
                .collect(TieredList.<Op>toList());

        TablePiece mergedTablePiece = initTablePiece.builder().mustCast(TablePiece.Builder.class)
                .setUsedColumns(mergedUsedColumns)
                .setColumns(mergedColumns)
                .setCommonState(newCommonState)
                .setConjuncts(mergedConjuncts)
                .build().cast();
        mergedTablePiece = PlanPieceNormalizer.normalize(mergedTablePiece).cast();
        return new TableUsage(initTableUsage.piece, mergedTablePiece, mergedUsedColumns, mergedPushDownPredicates,
                mergedJoinKeys, mergedGroupByKeys);
    }

    private static Function<ColumnRefSet, Optional<TablePiece>> getTableIfContainsKey(
            Map<Integer, TablePiece> columnIdToTableMap) {
        return key -> {
            TablePiece prevTbl = null;
            Iterator<Integer> nextColumnId = key.getStream().iterator();
            while (nextColumnId.hasNext()) {
                Integer id = nextColumnId.next();
                TablePiece tbl = columnIdToTableMap.get(id);
                if (tbl == null) {
                    return Optional.empty();
                }
                if (prevTbl != null && tbl != prevTbl) {
                    return Optional.empty();
                }
                prevTbl = tbl;
            }
            return Optional.ofNullable(prevTbl);
        };
    }

    private static Map<Box<TablePiece>, Map<Integer, TieredList<Op>>> getPushDownPredicates(
            List<Op> conjuncts,
            Map<Integer, TablePiece> columnIdToTableMap) {
        return conjuncts.stream()
                .map(op -> Pair.create(op, op.getIdSet()))
                .filter(p -> p.second.size() == 1)
                .map(p -> Pair.create(p.second.iterator().next(), p.first))
                .map(p -> Pair.create(Optional.ofNullable(columnIdToTableMap.get(p.first)), p))
                .filter(p -> p.first.isPresent())
                .map(p -> Pair.create(p.first.get(), p.second))
                .collect(Collectors.groupingBy(p -> Box.of(p.first),
                        Collectors.mapping(p -> p.second,
                                Collectors.groupingBy(pp -> pp.first,
                                        Collectors.mapping(pp -> pp.second, TieredList.<Op>toList())))));
    }

    private void analyzeUsageOfPushDownPredicates() {
        Preconditions.checkState(whereConjuncts == null);
        Preconditions.checkState(conjunctFreq == null);
        Map<Integer, Map<StrictOp, Long>> predicateFreq = pushDownConjuncts.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue()
                                .stream()
                                .collect(Collectors.groupingBy(Op::strict, Collectors.counting()))));

        TieredList.Builder<Op> stiffPredicatesBuilder = TieredList.<Op>newGenesisTier();
        Map<Integer, Map<StrictOp, Long>> finalPredicateFreq = Maps.newHashMap();
        for (Map.Entry<Integer, Map<StrictOp, Long>> entry : predicateFreq.entrySet()) {
            Integer columnId = entry.getKey();
            Map<StrictOp, Long> opToFreq = entry.getValue();
            Map<Boolean, TieredMap<StrictOp, Long>> opGroups = opToFreq.entrySet()
                    .stream()
                    .collect(Collectors.partitioningBy(
                            e -> OpUtil.isStiffPredicate(e.getKey().getOp()),
                            TieredMap.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            opGroups.get(true).keySet().forEach(sop -> stiffPredicatesBuilder.add(sop.getOp()));
            if (!opGroups.get(false).isEmpty()) {
                finalPredicateFreq.put(columnId, opGroups.get(false));
            }
        }
        whereConjuncts = stiffPredicatesBuilder.build();
        conjunctFreq = finalPredicateFreq;
    }

    public TieredList<Op> getWhereConjuncts() {
        if (whereConjuncts == null) {
            analyzeUsageOfPushDownPredicates();
        }
        return Objects.requireNonNull(whereConjuncts);
    }

    public Map<Integer, Map<StrictOp, Long>> getConjunctFreq() {
        if (conjunctFreq == null) {
            analyzeUsageOfPushDownPredicates();
        }
        return Objects.requireNonNull(conjunctFreq);
    }

    public PlanPiece getPiece() {
        return piece;
    }

    public TablePiece getTablePiece() {
        return tablePiece;
    }

    public ColumnRefSet getUsedColumns() {
        return usedColumns;
    }

    public Map<Integer, TieredList<Op>> getPushDownConjuncts() {
        return pushDownConjuncts;
    }

    public List<ColumnRefSet> getJoinKeys() {
        return joinKeys;
    }

    public List<ColumnRefSet> getGroupByKeys() {
        return groupByKeys;
    }

    private static final class TableAnalysis {
        ColumnRefSet usedColumns;
        Map<Integer, TieredList<Op>> pushDownConjuncts = Maps.newHashMap();
        List<ColumnRefSet> joinKeys = Lists.newArrayList();
        List<ColumnRefSet> groupByKeys = Lists.newArrayList();
    }

    private static final class AnalysisContext {
        private final Map<Integer, TablePiece> columnIdToTableMap;
        private final Map<Box<TablePiece>, TableAnalysis> analysisMap;

        private AnalysisContext(Map<Integer, TablePiece> columnIdToTableMap,
                                Map<Box<TablePiece>, TableAnalysis> analysisMap) {
            this.columnIdToTableMap = columnIdToTableMap;
            this.analysisMap = analysisMap;
        }

        public static AnalysisContext of(PlanPiece piece) {
            List<TablePiece> tablePieces = PlanPiece.collect(piece, TablePiece.class);
            TieredMap<Integer, TablePiece> columnIdToTable = tablePieces.stream()
                    .map(tbl -> Pair.create(tbl, tbl.getUsedColumns()))
                    .flatMap(p -> p.second.getStream().map(colId -> Pair.create(colId, p.first)))
                    .collect(TieredMap.toMap(p -> p.first, p -> p.second));

            TieredMap<Box<TablePiece>, TableAnalysis> analysisMap = tablePieces.stream()
                    .collect(TieredMap.toMap(Box::of, e -> new TableAnalysis()));
            analysisMap.forEach((tbl, analysis) -> {
                analysis.usedColumns = tbl.unboxed().getUsedColumns();
            });
            return new AnalysisContext(columnIdToTable, analysisMap);
        }

        public Map<Integer, TablePiece> getColumnIdToTableMap() {
            return columnIdToTableMap;
        }

        public Map<Box<TablePiece>, TableAnalysis> getAnalysisMap() {
            return analysisMap;
        }
    }

    private static final class Visitor extends PlanPieceVisitor<AnalysisContext, AnalysisContext> {
        public static final Visitor INSTANCE = new Visitor();

        private Visitor() {
        }

        private void visitConjuncts(List<Op> conjuncts, AnalysisContext context) {
            getPushDownPredicates(conjuncts, context.getColumnIdToTableMap()).forEach((tbl, pushDownPredicates) ->
                    pushDownPredicates.forEach((columnId, predicates) ->
                            context.getAnalysisMap().get(tbl)
                                    .pushDownConjuncts.merge(columnId, predicates, TieredList::concat)
                    ));
        }

        @Override
        public AnalysisContext visitStarJoin(StarJoinPiece joinPiece, AnalysisContext context) {
            joinPiece.getCorners().stream()
                    .map(StarJoinPiece.StarCorner::getEqConjuncts)
                    .filter(Predicate.not(List::isEmpty))
                    .map(ops -> ops.stream()
                            .map(op -> Pair.create(op.arg(0).cast(Var.class), op.arg(1).cast(Var.class)))
                            .collect(Collectors.toList()))
                    .filter(pairs -> pairs.stream().allMatch(p -> p.first.isPresent() && p.second.isPresent()))
                    .flatMap(pairs -> {
                        ColumnRefSet joinKey1 = ColumnRefSet.of();
                        ColumnRefSet joinKey2 = ColumnRefSet.of();
                        pairs.forEach(p -> {
                            joinKey1.union(p.first.get().getId());
                            joinKey2.union(p.second.get().getId());
                        });
                        return Stream.of(joinKey1, joinKey2);
                    })
                    .map(joinKey -> Pair.create(
                            joinKey,
                            getTableIfContainsKey(context.getColumnIdToTableMap()).apply(joinKey)))
                    .filter(p -> p.second.isPresent())
                    .map(p -> Pair.create(p.first, p.second.get()))
                    .forEach(p -> {
                        context.getAnalysisMap().get(Box.of(p.second)).joinKeys.add(p.first);
                    });
            visitConjuncts(joinPiece.getConjuncts(), context);
            return context;
        }

        @Override
        public AnalysisContext visitAggregate(AggregatePiece aggPiece, AnalysisContext context) {
            ColumnRefSet groupByKey = ColumnRefSet.createByIds(aggPiece.getDimensions().keySet());
            Optional<TablePiece> optTbl =
                    getTableIfContainsKey(context.getColumnIdToTableMap()).apply(groupByKey);
            if (optTbl.isPresent()) {
                TablePiece tbl = optTbl.get();
                TableAnalysis analysis = context.getAnalysisMap().get(Box.of(tbl));
                analysis.groupByKeys.clear();
                analysis.groupByKeys.add(groupByKey);
                List<ColumnRefSet> joinKeys = analysis.joinKeys
                        .stream()
                        .filter(joinKey -> joinKey.containsAll(groupByKey))
                        .collect(Collectors.toList());
                analysis.joinKeys.clear();
                analysis.joinKeys.addAll(joinKeys);
            }
            TieredList<Op> conjuncts = aggPiece.getFlatTable().getStiffConjuncts()
                    .concat(aggPiece.getFlatTable().getFlexibleConjuncts());
            visitConjuncts(conjuncts, context);
            return context;
        }

        @Override
        public AnalysisContext visitTable(TablePiece tablePiece, AnalysisContext context) {
            visitConjuncts(tablePiece.getConjuncts(), context);
            return context;
        }

        private void analyzeTopdown(PlanPiece piece, AnalysisContext context) {
            piece.accept(this, context);
            piece.getInputPieces().forEach(inputPiece -> analyzeTopdown(inputPiece, context));
        }

        public List<TableUsage> analyze(PlanPiece piece) {
            PlanPiece normalizedPiece = PlanPieceNormalizer.normalize(piece, false);
            AnalysisContext context = AnalysisContext.of(normalizedPiece);
            analyzeTopdown(normalizedPiece, context);
            return context.getAnalysisMap().entrySet()
                    .stream()
                    .map(e -> new TableUsage(
                            normalizedPiece,
                            e.getKey().unboxed(),
                            e.getValue().usedColumns,
                            e.getValue().pushDownConjuncts,
                            e.getValue().joinKeys,
                            e.getValue().groupByKeys))
                    .collect(Collectors.toList());
        }
    }
}
