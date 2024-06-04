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

package com.starrocks.sql.automv.lattice;

import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.estimation.CardRecord;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PieceCommonState;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceNormalizer;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LatticeNode {
    private final Lattice lattice;
    private final LatticeNodeId id;
    private final transient Map<LatticeNodeId, Optional<AggregatePiece>> hoistMemo = Maps.newHashMap();
    private List<LatticeNode> parent;
    private List<LatticeNode> children;
    private TieredList<AggregatePiece> coverablePieces;
    private TieredList<AggregatePiece> uncoverablePieces;

    private transient CardRecord card;
    private transient Set<LatticeNodeId> ancestors;
    private transient Set<LatticeNodeId> offsprings;

    public LatticeNode(Lattice lattice, LatticeNodeId id, AggregatePiece aggPiece) {
        this.lattice = Objects.requireNonNull(lattice);
        this.id = Objects.requireNonNull(id);
        this.coverablePieces = TieredList.<AggregatePiece>genesis();
        this.uncoverablePieces = TieredList.<AggregatePiece>genesis();
        this.parent = Lists.newArrayList();
        this.children = Lists.newArrayList();
        Preconditions.checkArgument(aggPiece.getDistinctMetrics().isEmpty());
        addPiece(aggPiece);
    }

    public static List<AggregatePiece> consolidate(List<AggregatePiece> pieces) {
        if (pieces.size() < 2) {
            return pieces;
        }
        AggregatePiece firstAggPiece = pieces.get(0);
        ColumnRefToIdConverter idConverter = firstAggPiece.getFlatTable().getCommonState().getIdConverter();
        Map<String, Op> normToOpMap = firstAggPiece.getFlatTable().getColumns().entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        e -> e.getValue().getNorm().toString(),
                        e -> OpUtil.columnToOp(e.getKey(), e.getValue())));

        TieredMap<Integer, GenericColumn> metrics = mergeMetrics(idConverter, normToOpMap, pieces);
        TieredList<Op> hoistConjuncts = mergeHoistConjuncts(normToOpMap, pieces);
        TieredList<Op> nonHoistConjuncts = mergeNonHoistConjuncts(normToOpMap, pieces);
        AggregatePiece newAggPiece = firstAggPiece.builder()
                .mustCast(AggregatePiece.Builder.class)
                .setMetrics(metrics)
                .setHoistConjuncts(hoistConjuncts)
                .setNonHoistConjuncts(nonHoistConjuncts)
                .build();
        TieredMap<Integer, GenericColumn> norms = firstAggPiece.getColumns().entrySet()
                .stream()
                .collect(TieredMap.toMap(Map.Entry::getKey, e -> e.getValue().getNorm()));

        return Collections.singletonList(PlanPieceNormalizer.normalizeTopPiece(newAggPiece, norms).cast());
    }

    public static TieredList<Op> mergeHoistConjuncts(Map<String, Op> normToOpMap, List<AggregatePiece> aggPieces) {
        return mergeConjuncts(normToOpMap, aggPieces, AggregatePiece::getHoistConjuncts);
    }

    public static TieredList<Op> mergeNonHoistConjuncts(Map<String, Op> normToOpMap, List<AggregatePiece> aggPieces) {
        return mergeConjuncts(normToOpMap, aggPieces, AggregatePiece::getNonHoistConjuncts);
    }

    private static TieredList<Op> mergeConjuncts(Map<String, Op> normToOpMap,
                                                 List<AggregatePiece> aggPieces,
                                                 Function<AggregatePiece, TieredList<Op>> conjunctsGetter) {
        Set<String> uniqueConjunctNorms = Sets.newHashSet();
        TieredList<Op> mergedConjuncts = TieredList.genesis();
        for (AggregatePiece aggPiece : aggPieces) {
            TieredList<Op> conjuncts = conjunctsGetter.apply(aggPiece);
            TieredList.Builder<Op> uniqueConjunctsBuilder = TieredList.<Op>newGenesisTier();
            for (Op op : conjuncts) {
                if (uniqueConjunctNorms.contains(op.getNorm().toString())) {
                    continue;
                }
                uniqueConjunctNorms.add(op.getNorm().toString());
                uniqueConjunctsBuilder.add(op);
            }
            TieredList<Op> uniqueConjuncts = uniqueConjunctsBuilder.build();
            if (uniqueConjuncts.isEmpty()) {
                continue;
            }
            ColumnRefSet usedColumns = ColumnRefSet.of();
            uniqueConjuncts.stream().map(Op::getIds).forEach(usedColumns::union);
            Map<Integer, Op> idToOpMap = aggPiece.getFlatTable().getColumns()
                    .entrySet()
                    .stream()
                    .filter(e -> usedColumns.contains(e.getKey()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> Objects.requireNonNull(normToOpMap.get(e.getValue().getNorm().toString()))));
            mergedConjuncts = mergedConjuncts.concat(OpUtil.subst(uniqueConjuncts, idToOpMap));
        }
        return mergedConjuncts;
    }

    public static TieredMap<Integer, GenericColumn> mergeMetrics(ColumnRefToIdConverter idConverter,
                                                                 Map<String, Op> normToOpMap,
                                                                 List<AggregatePiece> aggPieces) {

        Set<String> uniqueMetricNorms = Sets.newHashSet();
        TieredMap<Integer, GenericColumn> mergedMetrics = TieredMap.genesis();
        for (AggregatePiece aggPiece : aggPieces) {
            List<GenericColumn> metrics = aggPiece.getMetrics().values().stream()
                    .filter(metric -> !uniqueMetricNorms.contains(metric.getNorm().toString()))
                    .map(metric -> Pair.create(metric, metric.getNorm().toString()))
                    .sorted(Pair.comparingBySecond())
                    .distinct()
                    .map(p -> p.first)
                    .collect(Collectors.toList());

            uniqueMetricNorms.addAll(metrics.stream().map(metric -> metric.getNorm().toString())
                    .collect(Collectors.toList()));

            ColumnRefSet usedColumns = ColumnRefSet.of();
            metrics.forEach(metric -> metric.getUsedColumns().ifPresent(usedColumns::union));

            Map<Integer, Op> idToOpMap = aggPiece.getFlatTable().getColumns()
                    .entrySet()
                    .stream()
                    .filter(e -> usedColumns.contains(e.getKey()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> Objects.requireNonNull(normToOpMap.get(e.getValue().getNorm().toString()))));

            TieredMap<Integer, GenericColumn> newMetrics =
                    OpUtil.subst(metrics, idToOpMap, idConverter, true);
            mergedMetrics = mergedMetrics.merge(newMetrics);
        }
        return mergedMetrics;
    }

    public static List<PlanPiece> mergeDerivedColumnsOfFlatTables(List<PlanPiece> aggPieces) {
        if (aggPieces.size() < 2) {
            return aggPieces;
        }

        PlanPiece initFlatTable = aggPieces.get(0).mustCast(AggregatePiece.class).getFlatTable();
        Set<String> uniqueColumnNorms = Sets.newHashSet();
        TieredMap.Builder<Integer, GenericColumn> initUniqueOriginalColumnsBuilder = TieredMap.newGenesisTier();
        for (Map.Entry<Integer, GenericColumn> e : initFlatTable.getColumns().entrySet()) {
            String norm = e.getValue().getNorm().toString();
            if (e.getValue().isDerived() || uniqueColumnNorms.contains(norm)) {
                continue;
            }
            uniqueColumnNorms.add(norm);
            initUniqueOriginalColumnsBuilder.put(e);
        }

        TieredMap<Integer, GenericColumn> initUniqueOriginalColumns = initUniqueOriginalColumnsBuilder.build();

        Map<String, Integer> initNormToId = initUniqueOriginalColumns.entrySet()
                .stream()
                .collect(TieredMap.toMap(e -> e.getValue().getNorm().toString(), Map.Entry::getKey));
        Map<String, Op> initNormToOp =
                initUniqueOriginalColumns.entrySet().stream().collect(ImmutableMap.toImmutableMap(
                        e -> e.getValue().getNorm().toString(),
                        e -> OpUtil.columnToOp(e.getKey(), e.getValue())));

        ColumnRefToIdConverter initIdConverter = initFlatTable.getCommonState().getIdConverter();

        TieredMap<Integer, GenericColumn> columns = initUniqueOriginalColumns;

        for (PlanPiece piece : aggPieces) {
            AggregatePiece aggPiece = piece.cast();
            PlanPiece flatTable = aggPiece.getFlatTable();
            Map<Boolean, TieredMap<Integer, GenericColumn>> columnGroups = flatTable.getColumns().entrySet()
                    .stream()
                    .collect(Collectors.partitioningBy(e -> e.getValue().isOriginal(), TieredMap.toMap()));

            TieredMap<Integer, GenericColumn> originalColumns = columnGroups.get(true);
            TieredMap<Integer, GenericColumn> derivedColumns = columnGroups.get(false);

            TieredMap<Integer, Op> idToOpMap = originalColumns.entrySet()
                    .stream()
                    .collect(TieredMap.toMap(
                            Map.Entry::getKey,
                            Objects.requireNonNull(e -> initNormToOp.get(e.getValue().getNorm().toString()))));

            TieredMap<Integer, GenericColumn> newDerivedColumns =
                    OpUtil.subst(derivedColumns.values(), idToOpMap, initIdConverter, true);

            TieredMap.Builder<Integer, GenericColumn> newUniqueDerivedColumnsBuilder = TieredMap.newGenesisTier();

            for (Map.Entry<Integer, GenericColumn> e : newDerivedColumns.entrySet()) {
                String norm = e.getValue().getNorm().toString();
                if (uniqueColumnNorms.contains(norm)) {
                    continue;
                }
                uniqueColumnNorms.add(norm);
                newUniqueDerivedColumnsBuilder.put(e);
            }
            columns = columns.merge(newUniqueDerivedColumnsBuilder.build());
        }

        PieceCommonState commonState = new PieceCommonState(
                initIdConverter.duplicate(),
                initFlatTable.getCommonState().getFqTableMap());

        PlanPiece realFlatTable = initFlatTable.builder()
                .setColumns(columns)
                .setConjuncts(TieredList.genesis())
                .setCommonState(commonState)
                .build();

        Map<String, Op> normToOp = columns.entrySet().stream().collect(ImmutableMap.toImmutableMap(
                e -> e.getValue().getNorm().toString(),
                e -> OpUtil.columnToOp(e.getKey(), e.getValue())));

        List<PlanPiece> newAggPieces = Lists.newArrayListWithCapacity(aggPieces.size());
        for (PlanPiece piece : aggPieces) {
            AggregatePiece aggPiece = piece.cast();
            PlanPiece flatTable = aggPiece.getFlatTable();
            Map<Integer, Op> idToOp = flatTable.getColumns().entrySet()
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(
                            Map.Entry::getKey,
                            e -> Objects.requireNonNull(normToOp.get(e.getValue().getNorm().toString()))
                    ));

            ColumnRefToIdConverter idConverter = realFlatTable.getCommonState().getIdConverter().duplicate();

            List<TieredMap<Integer, GenericColumn>> columnsList = Stream.of(
                            aggPiece.getDimensions(),
                            aggPiece.getRollupDimensions(),
                            aggPiece.getMetrics(),
                            aggPiece.getDistinctMetrics())
                    .map(cols -> OpUtil.subst(cols, initNormToId, idToOp, idConverter))
                    .collect(Collectors.toList());

            List<TieredList<Op>> conjunctsList = Stream.of(
                            flatTable.getConjuncts(),
                            aggPiece.getHoistConjuncts(),
                            aggPiece.getNonHoistConjuncts())
                    .map(conjuncts -> OpUtil.subst(conjuncts, idToOp))
                    .collect(Collectors.toList());

            PieceCommonState pieceCommonState = new PieceCommonState(idConverter, commonState.getFqTableMap());
            PlanPiece newFlatTable = realFlatTable.builder()
                    .setConjuncts(conjunctsList.get(0))
                    .setCommonState(pieceCommonState)
                    .build();

            AggregatePiece newAggPiece = AggregatePiece.newBuilder()
                    .setFlatTable(newFlatTable)
                    .setDimensions(columnsList.get(0))
                    .setRollupDimensions(columnsList.get(1))
                    .setMetrics(columnsList.get(2))
                    .setDistinctMetrics(columnsList.get(3))
                    .setHoistConjuncts(conjunctsList.get(1))
                    .setNonHoistConjuncts(conjunctsList.get(2))
                    .setCommonState(pieceCommonState)
                    .setConjuncts(TieredList.genesis())
                    .build().cast();

            newAggPiece.assignPieceIds();
            PlanPieceNormalizer.normalize(newAggPiece);
            Preconditions.checkArgument(newAggPiece.getNormHash().equals(piece.getNormHash()));
            newAggPieces.add(newAggPiece);
        }
        return newAggPieces;
    }

    private static TieredMap<Integer, GenericColumn> mergeMetrics(ColumnRefToIdConverter idConverter,
                                                                  Map<String, Op> normToOpMap,
                                                                  AggregatePiece... aggPieces) {
        return mergeMetrics(idConverter, normToOpMap, Arrays.asList(aggPieces));
    }

    public void addPiece(AggregatePiece aggPiece) {
        boolean rollupUnable = AggregatePolicies.hasRollupUnable(aggPiece.getMetrics().values());
        if (rollupUnable) {
            uncoverablePieces = uncoverablePieces.concatOne(aggPiece);
        } else {
            coverablePieces = coverablePieces.concatOne(aggPiece);
        }
    }

    public List<AggregatePiece> getCoverablePieces() {
        return coverablePieces;
    }

    private void setCoverablePieces(List<AggregatePiece> coverablePieces) {
        this.coverablePieces = TieredList.<AggregatePiece>genesis().concat(coverablePieces);
    }

    public List<AggregatePiece> getUncoverablePieces() {
        return uncoverablePieces;
    }

    private void setUncoverablePieces(List<AggregatePiece> uncoverablePieces) {
        this.uncoverablePieces = TieredList.<AggregatePiece>genesis().concat(uncoverablePieces);
    }

    public void consolidateCoverable() {
        setCoverablePieces(consolidate(getCoverablePieces()));
    }

    public void consolidateUncoverable() {
        Collection<List<AggregatePiece>> pieceGroups = getUncoverablePieces()
                .stream()
                .collect(Collectors.groupingBy(aggPiece ->
                        aggPiece.getFlatTable().getAuxState().getConjunctsNorm().getResult())).values();
        List<AggregatePiece> pieces = Lists.newArrayListWithCapacity(pieceGroups.size());
        for (List<AggregatePiece> group : pieceGroups) {
            pieces.addAll(consolidate(group));
        }
        setUncoverablePieces(pieces);
    }

    public void consolidateCoverable(List<AggregatePiece> aggPieces) {
        setCoverablePieces(consolidate(aggPieces));
    }

    public void consolidateFully(boolean pruneRollupUnableWithConjuncts) {
        Map<Boolean, TieredList<AggregatePiece>> rollupUnablePieceGroups = getUncoverablePieces().stream()
                .collect(Collectors.partitioningBy(
                        aggPiece -> aggPiece.getFlatTable().getConjuncts().isEmpty(),
                        TieredList.<AggregatePiece>toList()));

        TieredList<AggregatePiece> piecesWithConjuncts = rollupUnablePieceGroups.get(false);
        TieredList<AggregatePiece> piecesWithoutConjuncts = rollupUnablePieceGroups.get(true);
        List<AggregatePiece> pieces = Stream.of(getCoverablePieces(), piecesWithoutConjuncts)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        TieredList<AggregatePiece> uncoverablePieces = TieredList.<AggregatePiece>genesis()
                .concat(consolidate(pieces));
        if (!pruneRollupUnableWithConjuncts) {
            uncoverablePieces = uncoverablePieces.concat(piecesWithConjuncts);
        }
        setCoverablePieces(Collections.emptyList());
        setUncoverablePieces(uncoverablePieces);
    }

    public LatticeNodeId getId() {
        return id;
    }

    public List<LatticeNode> getParents() {
        return parent;
    }

    public void setParent(List<LatticeNode> parent) {
        this.parent = parent;
    }

    public List<LatticeNode> getChildren() {
        return children;
    }

    public void setChildren(List<LatticeNode> children) {
        this.children = children;
    }

    public Set<LatticeNodeId> getAncestors() {
        if (this.ancestors == null) {
            Set<LatticeNodeId> ancestors = Sets.newHashSet();
            this.getParents().forEach(parent -> {
                ancestors.addAll(parent.getAncestors());
                ancestors.add(parent.getId());
            });
            this.ancestors = ancestors;
        }
        return this.ancestors;
    }

    public Set<LatticeNodeId> getOffsprings() {
        if (this.offsprings == null) {
            Set<LatticeNodeId> offsprings = Sets.newHashSet();
            this.getChildren().forEach(child -> {
                offsprings.addAll(child.getOffsprings());
                offsprings.add(child.getId());
            });
            this.offsprings = offsprings;
        }
        return offsprings;
    }

    public CardRecord getCard() {
        return Objects.requireNonNull(this.card);
    }

    public void setCard(CardRecord card) {
        this.card = Objects.requireNonNull(card);
    }

    private AggregatePiece extendDimensions(Lattice lattice, LatticeNodeId targetId, LatticeNodeId id,
                                            AggregatePiece aggPiece) {
        Preconditions.checkArgument(targetId.isCoveringStrictly(id));
        Set<String> extraDimensionNorms = targetId.diff(id).getColumnOrdinals()
                .stream()
                .map(lattice.getColumnNorms()::get)
                .collect(Collectors.toSet());

        TieredMap<Integer, GenericColumn> extraDimensions = aggPiece.getFlatTable().getColumns().entrySet().stream()
                .filter(e -> extraDimensionNorms.contains(e.getValue().getNorm().toString()))
                .collect(TieredMap.toMap());
        TieredMap<Integer, GenericColumn> newDimensions = aggPiece.getDimensions().merge(extraDimensions);

        return aggPiece.builder().mustCast(AggregatePiece.Builder.class)
                .setDimensions(newDimensions)
                .build();
    }

    public Optional<AggregatePiece> hoist() {
        return hoistImpl(this.getId());
    }

    private Optional<AggregatePiece> hoistImpl(LatticeNodeId targetId) {
        Optional<AggregatePiece> savedResult = hoistMemo.get(targetId);
        if (savedResult != null) {
            return savedResult;
        }

        List<AggregatePiece> aggPieces = getChildren().stream()
                .map(child -> child.hoistImpl(getId()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        List<AggregatePiece> newRollupAblePieces = Stream.of(getCoverablePieces(), aggPieces)
                .flatMap(Collection::stream).collect(Collectors.toList());

        consolidateCoverable(newRollupAblePieces);
        if (getCoverablePieces().isEmpty()) {
            hoistMemo.put(targetId, Optional.empty());
            return Optional.empty();
        }

        Preconditions.checkArgument(getCoverablePieces().size() == 1);
        AggregatePiece aggPiece = getCoverablePieces().get(0);
        if (targetId.isCoveringStrictly(getId())) {
            aggPiece = extendDimensions(lattice, targetId, getId(), aggPiece);
        }

        Optional<AggregatePiece> result = Optional.of(aggPiece);
        hoistMemo.put(targetId, result);
        return result;
    }
}