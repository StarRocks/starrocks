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
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.qe.PartitionPlus;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import org.apache.commons.compress.utils.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PlanPiece {
    private final List<PlanPiece> inputPieces;
    private final TieredMap<Integer, GenericColumn> columns;
    private final TieredList<Op> conjuncts;

    private final PieceCommonState commonState;
    private final PieceAuxState auxState;

    protected PlanPiece(List<PlanPiece> inputPieces, TieredMap<Integer, GenericColumn> columns,
                        TieredList<Op> conjuncts, PieceCommonState commonState, PieceAuxState auxState) {
        this.inputPieces = Objects.requireNonNull(inputPieces);
        this.columns = Objects.requireNonNull(columns);
        this.conjuncts = Objects.requireNonNull(conjuncts);
        this.commonState = Objects.requireNonNull(commonState);
        this.auxState = Objects.requireNonNull(auxState);
    }

    public static <T extends PlanPiece> List<T> collect(PlanPiece piece, Class<T> klass, boolean ignoreSemiAntiSide) {
        List<T> pieces = Lists.newArrayList();
        collectImpl(piece, klass, ignoreSemiAntiSide, pieces);
        return pieces;
    }

    public static <T extends PlanPiece> List<T> collect(PlanPiece piece, Class<T> klass) {
        return collect(piece, klass, false);
    }

    @SuppressWarnings("unchecked")
    private static <T extends PlanPiece> void collectImpl(PlanPiece piece,
                                                          Class<T> klass,
                                                          boolean ignoreSemiAntiSide,
                                                          List<T> pieces) {
        if (piece.isStarJoin() && ignoreSemiAntiSide) {
            StarJoinPiece starJoin = piece.mustCast(StarJoinPiece.class);
            starJoin.getCorners()
                    .stream()
                    .filter(corner -> !corner.getJoinType().isSemiAntiJoin())
                    .map(StarJoinPiece.StarCorner::getPiece)
                    .forEach(inputPiece -> collectImpl(inputPiece, klass, true, pieces));
            collectImpl(starJoin.getCentre(), klass, true, pieces);
        } else {
            piece.getInputPieces().forEach(inputPiece -> collectImpl(inputPiece, klass, ignoreSemiAntiSide, pieces));
        }
        if (klass.isAssignableFrom(piece.getClass())) {
            pieces.add((T) piece);
        }
    }

    public abstract Builder<? extends PlanPiece> builder();

    public boolean isTop() {
        return getAuxState().getParent() == null;
    }

    @SuppressWarnings("unchecked")
    public <T extends PlanPiece> T cast() {
        return (T) this;
    }

    public <T extends PlanPiece> Optional<T> cast(Class<T> klass) {
        return Util.downcast(this, klass);
    }

    public <T extends PlanPiece> T mustCast(Class<T> klass) {
        Optional<T> optObj = Util.downcast(this, klass);
        Preconditions.checkArgument(optObj.isPresent());
        return optObj.get();
    }

    public boolean isSPJG() {
        return this.cast(AggregatePiece.class)
                .map(aggPiece -> aggPiece.getInputPieces().get(0).stream().noneMatch(PlanPiece::isAggregate))
                .orElse(false);
    }

    public List<Pair<Integer, GenericColumn>> getOutputColumns(ColumnRefSet superiorInputColumns) {
        if (this.isAggregate()) {
            return getColumns().entrySet()
                    .stream()
                    .map(e -> Pair.create(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
        }

        List<Pair<Integer, GenericColumn>> outputColumns = Lists.newArrayList();
        //TieredMap may have duplicates, just use the first one.
        ColumnRefSet uniqueColumns = new ColumnRefSet();
        getColumns().entrySet().stream()
                .filter(e -> superiorInputColumns.contains(e.getKey()))
                .forEach(e -> {
                    if (!uniqueColumns.contains(e.getKey())) {
                        outputColumns.add(Pair.create(e.getKey(), e.getValue()));
                        uniqueColumns.union(e.getKey());
                    }
                });
        return outputColumns;
    }

    // Superior PlanPiece requires input columns from inferior PlanPieces to construct
    // columns and conjuncts.
    public ColumnRefSet getInputColumnIds(List<Pair<Integer, GenericColumn>> outputColumns) {
        ColumnRefSet inputColumnIds = new ColumnRefSet();
        outputColumns.forEach(e -> {
            final Optional<ColumnRefSet> columns = e.second.getUsedColumns();
            if (columns.isPresent()) {
                inputColumnIds.union(columns.get());
            } else {
                inputColumnIds.union(e.first);
            }
        });
        getConjuncts().forEach(op -> inputColumnIds.union(op.getIds()));
        getTopAggregateIfFlatTable().ifPresent(aggPiece -> {
            Preconditions.checkState(aggPiece.getFlatTable().getFlexibleConjuncts().isEmpty());
            aggPiece.getFlatTable().getStiffConjuncts().forEach(op -> inputColumnIds.union(op.getIds()));
        });
        return inputColumnIds;
    }

    public Optional<AggregatePiece> getTopAggregateIfFlatTable() {
        if (isTop() || !getAuxState().getParent().isTop()) {
            return Optional.empty();
        }
        return getAuxState().getParent().cast(AggregatePiece.class);
    }

    public Optional<AggregatePiece> getParentAggregateIfFlatTable() {
        return Optional.ofNullable(getAuxState().getParent())
                .map(parentPiece -> parentPiece.cast(AggregatePiece.class).orElse(null));
    }

    public List<PlanPiece> getInputPieces() {
        return inputPieces;
    }

    public <R, C> R accept(PlanPieceVisitor<R, C> visitor, C context) {
        return visitor.visitPlanPiece(this, context);
    }

    public boolean isStarJoin() {
        return this instanceof StarJoinPiece;
    }

    public boolean isTableScan() {
        return this instanceof TablePiece;
    }

    public boolean isAggregate() {
        return this instanceof AggregatePiece;
    }

    public boolean isProject() {
        return this instanceof ReferencePiece;
    }

    public TieredMap<Integer, GenericColumn> getColumns() {
        return Objects.requireNonNull(columns);
    }

    public final PlanPiece setColumns(TieredMap<Integer, GenericColumn> columns) {
        return this.builder().setColumns(columns).build();
    }

    public TieredList<Op> getConjuncts() {
        return Objects.requireNonNull(conjuncts);
    }

    public PlanPiece setConjuncts(TieredList<Op> conjuncts) {
        return this.builder().setConjuncts(conjuncts).build();
    }

    public PieceAuxState getAuxState() {
        return auxState;
    }

    public abstract PlanPiece replaceInputPieces(List<PlanPiece> pieces);

    public String getFlatTableNormHash() {
        return this.mustCast(AggregatePiece.class).getFlatTable().getNormHash();
    }

    public String getNormHash() {
        return this.getAuxState().getNormHash();
    }

    public PieceCommonState getCommonState() {
        return commonState;
    }

    public void assignPieceIds() {
        this.getAuxState().setParent(null);
        assignPieceIdsImpl(Util.nextIdGenerator());
    }

    private void assignPieceIdsImpl(Supplier<Integer> idGenerator) {
        getInputPieces().forEach(piece -> piece.assignPieceIdsImpl(idGenerator));
        getInputPieces().forEach(piece -> piece.getAuxState().setParent(this));
        this.getAuxState().setId(idGenerator.get());
    }

    public PlanPiece revise(Function<PlanPiece, PlanPiece> revisor) {
        return this.reviseImpl(revisor);
    }

    public Stream<PlanPiece> stream() {
        return Stream.concat(Stream.of(this), getInputPieces().stream().flatMap(PlanPiece::stream));
    }

    private PlanPiece reviseImpl(Function<PlanPiece, PlanPiece> revisor) {
        List<PlanPiece> newInputPieces =
                this.getInputPieces().stream()
                        .map(child -> child.reviseImpl(revisor))
                        .collect(Collectors.toList());
        return revisor.apply(this.replaceInputPieces(newInputPieces));
    }

    @Override
    public final String toString() {
        return "Columns:\n" + columns + "Conjuncts" + conjuncts;
    }

    public List<PartitionPlus> getPartitionColumns(PartitionExtractor partitionExtractor) {
        // right side of left semi/anti join can not be output, so we can not use it's column as partition column
        List<TablePiece> tablePieces = PlanPiece.collect(this, TablePiece.class, true);
        Preconditions.checkArgument(!tablePieces.isEmpty());
        return tablePieces.stream()
                .map(tablePiece -> PartitionPlus.of(tablePiece, partitionExtractor))
                .collect(Collectors.toList());
    }

    public abstract static class Builder<T extends PlanPiece> {
        private List<PlanPiece> inputPieces;
        private TieredMap<Integer, GenericColumn> columns;
        private TieredList<Op> conjuncts;

        private PieceCommonState commonState;

        private PieceAuxState auxState;

        protected Builder(List<PlanPiece> inputPieces, TieredMap<Integer, GenericColumn> columns,
                          TieredList<Op> conjuncts, PieceCommonState commonState, PieceAuxState auxState) {
            this.inputPieces = inputPieces;
            this.columns = columns;
            this.conjuncts = conjuncts;
            this.commonState = commonState;
            this.auxState = auxState;
        }

        protected Builder() {
        }

        public List<PlanPiece> getInputPieces() {
            return inputPieces;
        }

        public TieredMap<Integer, GenericColumn> getColumns() {
            return columns;
        }

        public Builder<? extends PlanPiece> setColumns(TieredMap<Integer, GenericColumn> columns) {
            this.columns = Objects.requireNonNull(columns);
            return this;
        }

        public TieredList<Op> getConjuncts() {
            return conjuncts;
        }

        public Builder<? extends PlanPiece> setConjuncts(TieredList<Op> conjuncts) {
            this.conjuncts = Objects.requireNonNull(conjuncts);
            return this;
        }

        public PieceCommonState getCommonState() {
            return this.commonState;
        }

        public Builder<? extends PlanPiece> setCommonState(PieceCommonState commonState) {
            this.commonState = commonState;
            return this;
        }

        public PieceAuxState getAuxState() {
            return Optional.ofNullable(this.auxState).map(PieceAuxState::duplicate).orElseGet(PieceAuxState::new);
        }

        public abstract T build();

        @SuppressWarnings("unchecked")
        public <B extends Builder<? extends PlanPiece>> B cast() {
            return (B) this;
        }

        @SuppressWarnings("unchecked")
        public <B extends Builder<? extends PlanPiece>> Optional<B> cast(Class<B> klass) {
            if (this.getClass().equals(klass)) {
                return Optional.of((B) this);
            } else {
                return Optional.empty();
            }
        }

        @SuppressWarnings("unchecked")
        public <B extends Builder<? extends PlanPiece>> B mustCast(Class<B> klass) {
            Preconditions.checkArgument(this.getClass().equals(klass));
            return (B) this;
        }
    }
}

