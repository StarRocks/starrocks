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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import org.apache.commons.compress.utils.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
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

    public static <T extends PlanPiece> List<T> collect(PlanPiece piece, Class<T> klass) {
        List<T> pieces = Lists.newArrayList();
        collectImpl(piece, klass, pieces);
        return pieces;
    }

    @SuppressWarnings("unchecked")
    private static <T extends PlanPiece> void collectImpl(PlanPiece piece, Class<T> klass, List<T> pieces) {
        piece.getInputPieces().forEach(inputPiece -> collectImpl(inputPiece, klass, pieces));
        if (klass.isAssignableFrom(piece.getClass())) {
            pieces.add((T) piece);
        }
    }

    private static Map<Integer, GenericColumn> getPartitionColumns(TablePiece tablePiece) {
        FQTable fqTable = tablePiece.getTable();
        // TODO(by satanson): A MV with list-partition base tables is not supported in present,
        //  it would be supported soon in future.
        if ((fqTable.getTable() instanceof OlapTable) &&
                ((OlapTable) fqTable.getTable()).getPartitionInfo().isListPartition()) {
            return Collections.emptyMap();
        }
        Optional<List<String>> optPartitionColNames =
                Optional.ofNullable(fqTable.getTable().getPartitionColumnNames());

        Function<Column, Optional<Pair<Integer, GenericColumn>>> getGenericColumn = column ->
                tablePiece.getColumns().entrySet().stream()
                        .filter(e -> Objects.equals(e.getValue().getColumnName(), column.getName()))
                        .findFirst().map(e -> Pair.create(e.getKey(), e.getValue()));

        return optPartitionColNames.map(names -> names.stream()
                        .map(name -> fqTable.getTable().getColumn(name))
                        .map(getGenericColumn)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toMap(p -> p.first, p -> p.second)))
                .orElse(Collections.emptyMap());
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

    public final PlanPiece removeConjuncts() {
        return this.builder().setConjuncts(TieredList.genesis()).build();
    }

    public List<Pair<Integer, GenericColumn>> getOutputColumns(ColumnRefSet superiorInputColumns) {
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
        return inputColumnIds;
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

    public TieredMap<Integer, GenericColumn> getColumns() {
        return Objects.requireNonNull(columns);
    }

    public final PlanPiece setColumns(TieredMap<Integer, GenericColumn> columns) {
        return this.builder().setColumns(columns).build();
    }

    public TieredList<Op> getConjuncts() {
        return Objects.requireNonNull(conjuncts);
    }

    public final PlanPiece setConjuncts(TieredList<Op> conjuncts) {
        return this.builder().setConjuncts(conjuncts).build();
    }

    public PieceAuxState getAuxState() {
        return auxState;
    }

    public abstract PlanPiece replaceInputPieces(List<PlanPiece> pieces);

    public String getNormHash() {
        return this.cast(AggregatePiece.class)
                .map(aggPiece -> aggPiece.getFlatTable().getAuxState().getNormHash())
                .orElseGet(() -> this.getAuxState().getNormHash());
    }

    public PieceCommonState getCommonState() {
        return commonState;
    }

    public void assignPieceIds() {
        assignPieceIdsImpl(Util.nextIdGenerator());
    }

    private void assignPieceIdsImpl(Supplier<Integer> idGenerator) {
        getInputPieces().forEach(piece -> piece.assignPieceIdsImpl(idGenerator));
        getInputPieces().forEach(piece -> piece.getAuxState().setParent(this));
        this.getAuxState().setId(idGenerator.get());
    }

    public PlanPiece postOrderTravel(Consumer<PlanPiece> traveler) {
        this.getInputPieces().forEach(traveler);
        traveler.accept(this);
        return this;
    }

    public Optional<TablePiece> getLeftMostTable() {
        if (this.isTableScan()) {
            return Optional.of(this.cast());
        } else if (this.isAggregate()) {
            AggregatePiece aggPiece = this.cast();
            return aggPiece.getFlatTable().getLeftMostTable();
        } else if (this.isStarJoin()) {
            StarJoinPiece starJoinPiece = this.cast();
            return starJoinPiece.getCentre().getLeftMostTable();
        } else {
            return Optional.empty();
        }
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

    public List<Pair<TablePiece, Map<Integer, GenericColumn>>> getPartitionColumns() {
        List<TablePiece> tablePieces = PlanPiece.collect(this, TablePiece.class);
        Preconditions.checkArgument(!tablePieces.isEmpty());
        return tablePieces.stream()
                .map(tablePiece -> Pair.create(tablePiece, getPartitionColumns(tablePiece)))
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

