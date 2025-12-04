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
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class StarJoinPiece extends PlanPiece {
    private final PlanPiece centre;
    private final List<StarCorner> corners;

    private StarJoinPiece(TieredMap<Integer, GenericColumn> columns, TieredList<Op> conjuncts,
                          PieceCommonState commonState, PieceAuxState auxState, PlanPiece centre,
                          List<StarCorner> corners) {
        super(ImmutableList.<PlanPiece>builder()
                .add(centre).addAll(corners.stream().map(StarCorner::getPiece).iterator())
                .build(), columns, conjuncts, commonState, auxState);
        this.centre = Objects.requireNonNull(centre);
        this.corners = Collections.unmodifiableList(corners);
    }

    public static StarJoinPiece.Builder newBuilder() {
        return new StarJoinPiece.Builder();
    }

    @Override
    public <R, C> R accept(PlanPieceVisitor<R, C> visitor, C context) {
        return visitor.visitStarJoin(this, context);
    }

    @Override
    public PlanPiece replaceInputPieces(List<PlanPiece> pieces) {
        Preconditions.checkArgument(pieces.size() == getInputPieces().size());
        PlanPiece centre = pieces.get(0);
        List<StarCorner> oldCorners = getCorners();

        List<StarCorner> newCorners = IntStream.range(0, oldCorners.size())
                .mapToObj(i -> Pair.create(oldCorners.get(i), pieces.get(i + 1)))
                .map(p -> new StarCorner(p.first.eqConjuncts, p.first.getOtherConjuncts(), p.first.joinType, p.second))
                .collect(Collectors.toList());

        return this.builder().mustCast(StarJoinPiece.Builder.class)
                .setCentre(centre)
                .setCorners(newCorners)
                .build();
    }

    public PlanPiece getCentre() {
        return centre;
    }

    public List<StarCorner> getCorners() {
        return corners;
    }

    @Override
    public PlanPiece.Builder<? extends PlanPiece> builder() {
        return new StarJoinPiece.Builder(this);
    }

    @Override
    public ColumnRefSet getInputColumnIds(List<Pair<Integer, GenericColumn>> outputColumns) {
        ColumnRefSet inputColumnIds = super.getInputColumnIds(outputColumns);
        // we must add ColumnIds referenced by join conditions.
        getCorners().forEach(corner -> {
            corner.getEqConjuncts().forEach(op -> inputColumnIds.union(op.getIds()));
            corner.getOtherConjuncts().forEach(op -> inputColumnIds.union(op.getIds()));
        });
        return inputColumnIds;
    }

    public static class Builder extends PlanPiece.Builder<StarJoinPiece> {
        private PlanPiece centre;
        private List<StarCorner> corners;

        private Builder(StarJoinPiece joinPiece) {
            super(null, joinPiece.getColumns(), joinPiece.getConjuncts(), joinPiece.getCommonState(),
                    joinPiece.getAuxState());
            this.centre = joinPiece.getCentre();
            this.corners = joinPiece.getCorners();
        }

        private Builder() {
            super();
        }

        public final PlanPiece getCentre() {
            return centre;
        }

        public final Builder setCentre(PlanPiece centre) {
            this.centre = Objects.requireNonNull(centre);
            return this;
        }

        public final List<StarCorner> getCorners() {
            return corners;
        }

        public final Builder setCorners(List<StarCorner> corners) {
            this.corners = Objects.requireNonNull(corners);
            return this;
        }

        @Override
        public StarJoinPiece build() {
            return new StarJoinPiece(getColumns(), getConjuncts(), getCommonState(), getAuxState(), centre, corners);
        }
    }

    public static final class StarCorner {
        private final List<Op> eqConjuncts;
        private final List<Op> otherConjuncts;
        private final JoinOperator joinType;
        private final PlanPiece piece;

        public StarCorner(List<Op> eqConjuncts, List<Op> otherConjuncts, JoinOperator joinType,
                          PlanPiece corner) {
            this.eqConjuncts = Collections.unmodifiableList(eqConjuncts);
            this.otherConjuncts = Collections.unmodifiableList(otherConjuncts);
            Preconditions.checkArgument(!joinType.isRightJoin());
            this.joinType = joinType;
            this.piece = corner;
        }

        public List<Op> getEqConjuncts() {
            return eqConjuncts;
        }

        public List<Op> getOtherConjuncts() {
            return otherConjuncts;
        }

        public JoinOperator getJoinType() {
            return joinType;
        }

        public PlanPiece getPiece() {
            return piece;
        }
    }
}
