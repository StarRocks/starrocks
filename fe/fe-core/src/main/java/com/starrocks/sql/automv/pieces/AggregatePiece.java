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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AggregatePiece extends PlanPiece {
    private final FlatTable flatTable;
    private final TieredMap<Integer, GenericColumn> dimensions;
    private final TieredMap<Integer, GenericColumn> rollupDimensions;
    private final TieredMap<Integer, GenericColumn> metrics;
    private final TieredMap<Integer, GenericColumn> distinctMetrics;
    private final TieredList<Op> hoistConjuncts;
    private final TieredList<Op> nonHoistConjuncts;

    private AggregatePiece(
            TieredMap<Integer, GenericColumn> columns,
            TieredList<Op> conjuncts,
            PieceCommonState commonState,
            PieceAuxState auxState,
            FlatTable flatTable,
            TieredMap<Integer, GenericColumn> dimensions,
            TieredMap<Integer, GenericColumn> rollupDimensions,
            TieredMap<Integer, GenericColumn> metrics,
            TieredMap<Integer, GenericColumn> distinctMetrics,
            TieredList<Op> hoistConjuncts,
            TieredList<Op> nonHoistConjuncts) {
        super(ImmutableList.of(flatTable.getPiece()), columns, conjuncts, commonState, auxState);
        Set<Integer> columnIds = dimensions.merge(rollupDimensions).merge(metrics).merge(distinctMetrics).keySet();
        Preconditions.checkState(columnIds.equals(columns.keySet()));
        ColumnRefSet rollupDimensionIds = ColumnRefSet.of();
        flatTable.getFlexibleConjuncts().forEach(op -> rollupDimensionIds.union(op.getIds()));
        rollupDimensionIds.except(ColumnRefSet.createByIds(dimensions.keySet()));
        Preconditions.checkState(ColumnRefSet.createByIds(rollupDimensions.keySet()).equals(rollupDimensionIds));

        this.flatTable = Objects.requireNonNull(flatTable);
        this.dimensions = Objects.requireNonNull(dimensions);
        this.rollupDimensions = Objects.requireNonNull(rollupDimensions);
        this.metrics = Objects.requireNonNull(metrics);
        this.distinctMetrics = Objects.requireNonNull(distinctMetrics);
        this.hoistConjuncts = Objects.requireNonNull(hoistConjuncts);
        this.nonHoistConjuncts = Objects.requireNonNull(nonHoistConjuncts);
    }

    public static AggregatePiece.Builder newBuilder() {
        return new AggregatePiece.Builder();
    }

    public TieredList<Op> getHoistConjuncts() {
        return hoistConjuncts;
    }

    public TieredList<Op> getNonHoistConjuncts() {
        return nonHoistConjuncts;
    }

    public AggregatePiece toRollup() {
        return this.builder().mustCast(AggregatePiece.Builder.class)
                .setFlatTable(flatTable.toRollup())
                .setDimensions(this.dimensions.merge(this.rollupDimensions))
                .setRollupDimensions(TieredMap.genesis())
                .setMetrics(this.getMetrics())
                .setDistinctMetrics(TieredMap.genesis())
                .setConjuncts(TieredList.genesis())
                .build().cast();
    }

    public AggregatePiece mergeDistinctMetricsIntoMetrics() {
        return this.builder().mustCast(AggregatePiece.Builder.class)
                .setMetrics(this.metrics.merge(this.distinctMetrics))
                .setDistinctMetrics(TieredMap.genesis())
                .build();
    }

    public AggregatePiece splitDistinctMetricsFromMetrics() {
        Map<Boolean, TieredMap<Integer, GenericColumn>> metricGroups =
                this.getMetrics().entrySet().stream()
                        .collect(Collectors.partitioningBy(e -> OpUtil.isDistinct(e.getValue()), TieredMap.toMap()));

        return this.builder().mustCast(AggregatePiece.Builder.class)
                .setMetrics(metricGroups.get(false))
                .setDistinctMetrics(metricGroups.get(true))
                .build();
    }

    public AggregatePiece toPerfect() {
        return this.builder().mustCast(AggregatePiece.Builder.class)
                .setFlatTable(flatTable.toPerfect())
                .setRollupDimensions(TieredMap.genesis())
                .setMetrics(this.metrics.merge(distinctMetrics))
                .setDistinctMetrics(TieredMap.genesis())
                .setConjuncts(TieredList.genesis())
                .build().cast();
    }

    public AggregatePiece toPartialRollup(TieredList<Op> hoistingConjuncts, TieredList<Op> reservedConjuncts) {
        Preconditions.checkArgument(this.getDistinctMetrics().isEmpty());
        PlanPiece flatTablePiece = this.getFlatTable().getPiece().revise(Function.identity());
        FlatTable newFlatTable = new FlatTable(flatTablePiece, flatTable.stiffConjuncts.concat(reservedConjuncts),
                TieredList.<Op>genesis());
        ColumnRefSet rollupColumnIds = new ColumnRefSet();
        hoistingConjuncts.forEach(op -> rollupColumnIds.union(op.getIds()));
        ColumnRefSet dimensionIds = ColumnRefSet.createByIds(this.getDimensions().keySet());
        ColumnRefSet flatTableColumnIds = ColumnRefSet.createByIds(flatTablePiece.getColumns().keySet());
        rollupColumnIds.except(dimensionIds);
        Preconditions.checkArgument(flatTableColumnIds.containsAll(rollupColumnIds));
        TieredMap<Integer, GenericColumn> flatTableColumns = flatTablePiece.getColumns();
        TieredMap<Integer, GenericColumn> rollupDimensions =
                rollupColumnIds.getStream().collect(TieredMap.toMap(Function.identity(), flatTableColumns::get));
        return this.builder().mustCast(AggregatePiece.Builder.class)
                .setFlatTable(newFlatTable)
                .setRollupDimensions(TieredMap.genesis())
                .setDimensions(this.getDimensions().merge(rollupDimensions))
                .build().cast();
    }

    @Override
    public PlanPiece.Builder<? extends PlanPiece> builder() {
        return new AggregatePiece.Builder(this);
    }

    @Override
    public <R, C> R accept(PlanPieceVisitor<R, C> visitor, C context) {
        return visitor.visitAggregate(this, context);
    }

    @Override
    public PlanPiece replaceInputPieces(List<PlanPiece> pieces) {
        Preconditions.checkArgument(pieces.size() == 1);
        return this.builder().mustCast(AggregatePiece.Builder.class)
                .setFlatTable(new FlatTable(pieces.get(0), flatTable.stiffConjuncts, flatTable.flexibleConjuncts))
                .build();
    }

    public FlatTable getFlatTable() {
        return flatTable;
    }

    public TieredMap<Integer, GenericColumn> getDimensions() {
        return dimensions;
    }

    public TieredMap<Integer, GenericColumn> getRollupDimensions() {
        return rollupDimensions;
    }

    public TieredMap<Integer, GenericColumn> getMetrics() {
        return metrics;
    }

    public TieredMap<Integer, GenericColumn> getDistinctMetrics() {
        return distinctMetrics;
    }

    public boolean isStem() {
        return this.getHoistConjuncts().isEmpty() &&
                this.getNonHoistConjuncts().isEmpty() &&
                this.getDistinctMetrics().isEmpty() &&
                this.getRollupDimensions().isEmpty() &&
                this.getFlatTable().getFlexibleConjuncts().isEmpty();
    }

    public static final class FlatTable {
        private final PlanPiece piece;
        private final TieredList<Op> stiffConjuncts;
        private final TieredList<Op> flexibleConjuncts;

        private transient PrettyPrinter norm = null;
        private transient String normHash = null;
        private transient PrettyPrinter flexibleConjunctsNorm = null;
        private transient String flexibleConjunctsNormHash = null;

        public FlatTable(PlanPiece piece) {
            this(piece, TieredList.<Op>genesis(), TieredList.<Op>genesis());
        }

        public FlatTable(PlanPiece piece, TieredList<Op> stiffConjuncts, TieredList<Op> flexibleConjuncts) {
            this.piece = piece;
            this.stiffConjuncts = stiffConjuncts;
            this.flexibleConjuncts = flexibleConjuncts;
        }

        public PlanPiece getPiece() {
            return piece;
        }

        public TieredList<Op> getStiffConjuncts() {
            return stiffConjuncts;
        }

        public TieredList<Op> getFlexibleConjuncts() {
            return flexibleConjuncts;
        }

        public FlatTable toRollup() {
            return new FlatTable(piece, stiffConjuncts, TieredList.<Op>genesis());
        }

        public FlatTable toPerfect() {
            return new FlatTable(piece, stiffConjuncts.concat(flexibleConjuncts), TieredList.<Op>genesis());
        }

        public FlatTable toPartialRollup(TieredList<Op> hoistingConjuncts, TieredList<Op> reservedConjuncts) {
            return new FlatTable(piece, stiffConjuncts.concat(reservedConjuncts), hoistingConjuncts);
        }

        public PrettyPrinter getFlexibleConjunctsNorm() {
            if (flexibleConjunctsNorm == null) {
                List<String> conjunctNormItems = flexibleConjuncts
                        .stream()
                        .map(Op::getNorm)
                        .map(Op::toString)
                        .sorted()
                        .collect(Collectors.toList());
                flexibleConjunctsNorm = new PrettyPrinter().addItemsWithDelNl(",", conjunctNormItems);
            }
            return this.flexibleConjunctsNorm;
        }

        public String getFlexibleConjunctsNormHash() {
            if (this.flexibleConjunctsNormHash == null) {
                this.flexibleConjunctsNormHash = Util.md5(getFlexibleConjunctsNorm().getResult());
            }
            return this.flexibleConjunctsNormHash;
        }

        public PrettyPrinter getNorm() {
            return Objects.requireNonNull(this.norm);
        }

        public void setNorm(PrettyPrinter norm) {
            if (this.norm == null) {
                this.norm = norm;
            }
        }

        public String getNormHash() {
            if (normHash == null) {
                normHash = Util.md5(getNorm().getResult());
            }
            return normHash;
        }
    }

    public static class Builder extends PlanPiece.Builder<AggregatePiece> {
        private FlatTable flatTable;
        private TieredMap<Integer, GenericColumn> dimensions;
        private TieredMap<Integer, GenericColumn> rollupDimensions;
        private TieredMap<Integer, GenericColumn> metrics;
        private TieredMap<Integer, GenericColumn> distinctMetrics;

        private TieredList<Op> hoistConjuncts;
        private TieredList<Op> nonHoistConjuncts;

        Builder(AggregatePiece aggPiece) {
            super(ImmutableList.of(aggPiece.getFlatTable().getPiece()), aggPiece.getColumns(), aggPiece.getConjuncts(),
                    aggPiece.getCommonState(), aggPiece.getAuxState());

            this.flatTable = aggPiece.getFlatTable();
            this.dimensions = aggPiece.getDimensions();
            this.rollupDimensions = aggPiece.getRollupDimensions();
            this.metrics = aggPiece.getMetrics();
            this.distinctMetrics = aggPiece.getDistinctMetrics();
            this.hoistConjuncts = aggPiece.getHoistConjuncts();
            this.nonHoistConjuncts = aggPiece.getNonHoistConjuncts();
        }

        Builder() {
            super();
        }

        public TieredList<Op> getHoistConjuncts() {
            return hoistConjuncts;
        }

        public Builder setHoistConjuncts(TieredList<Op> hoistConjuncts) {
            this.hoistConjuncts = hoistConjuncts;
            return this;
        }

        public TieredList<Op> getNonHoistConjuncts() {
            return nonHoistConjuncts;
        }

        public Builder setNonHoistConjuncts(TieredList<Op> nonHoistConjuncts) {
            this.nonHoistConjuncts = nonHoistConjuncts;
            return this;
        }

        public FlatTable getFlatTable() {
            return flatTable;
        }

        public Builder setFlatTable(FlatTable flatTable) {
            this.flatTable = Objects.requireNonNull(flatTable);
            return this;
        }

        public TieredMap<Integer, GenericColumn> getDimensions() {
            return dimensions;
        }

        public Builder setDimensions(TieredMap<Integer, GenericColumn> dimensions) {
            this.dimensions = Objects.requireNonNull(dimensions);
            return this;
        }

        public TieredMap<Integer, GenericColumn> getRollupDimensions() {
            return rollupDimensions;
        }

        public Builder setRollupDimensions(TieredMap<Integer, GenericColumn> rollupDimensions) {
            this.rollupDimensions = Objects.requireNonNull(rollupDimensions);
            return this;
        }

        public TieredMap<Integer, GenericColumn> getMetrics() {
            return metrics;
        }

        public Builder setMetrics(TieredMap<Integer, GenericColumn> metrics) {
            this.metrics = Objects.requireNonNull(metrics);
            return this;
        }

        public TieredMap<Integer, GenericColumn> getDistinctMetrics() {
            return distinctMetrics;
        }

        public Builder setDistinctMetrics(TieredMap<Integer, GenericColumn> distinctMetrics) {
            this.distinctMetrics = Objects.requireNonNull(distinctMetrics);
            return this;
        }

        @Override
        public AggregatePiece build() {

            TieredMap<Integer, GenericColumn> columns =
                    dimensions.merge(rollupDimensions).merge(metrics).merge(distinctMetrics);
            boolean shouldUnique = columns.size() == new HashSet<>(columns.keySet()).size();
            boolean noIdConflict = Sets.intersection(flatTable.getPiece().getColumns().keySet(),
                    metrics.merge(distinctMetrics).keySet()).isEmpty();
            Preconditions.checkArgument(shouldUnique && noIdConflict);
            return new AggregatePiece(columns, getConjuncts(), getCommonState(), getAuxState(), flatTable, dimensions,
                    rollupDimensions, metrics, distinctMetrics, hoistConjuncts, nonHoistConjuncts);
        }
    }
}
