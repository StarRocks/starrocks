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

package com.starrocks.sql.automv.policies;

import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceVisitor;
import com.starrocks.sql.automv.pieces.StarJoinPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.pn.ColumnsAndSubstMap;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EliminateDerivedVarPolicy extends AggregatePolicy.SimplePolicy {
    public static final AbstractAggregatePolicy INSTANCE = new EliminateDerivedVarPolicy();

    private EliminateDerivedVarPolicy() {
    }

    @Override
    public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
        return Optional.of(aggPiece.revise(Visitor.getReviser()).cast());
    }

    private static final class Context {
        private TieredMap<Integer, Op> substMap = TieredMap.genesis();

        public TieredMap<Integer, Op> getSubstMap() {
            return substMap;
        }

        public void setSubstMap(TieredMap<Integer, Op> substMap) {
            this.substMap = Objects.requireNonNull(substMap);
        }
    }

    private static final class Visitor extends PlanPieceVisitor<PlanPiece, Context> {

        private static final Visitor INSTANCE = new Visitor();

        public static Function<PlanPiece, PlanPiece> getReviser() {
            Context ctx = new Context();
            return piece -> piece.accept(INSTANCE, ctx);
        }

        @Override
        public PlanPiece visitTable(TablePiece tablePiece, Context context) {
            ColumnsAndSubstMap columnsAndSubstMap = OpUtil.eliminateDerivedVars(
                    ColumnsAndSubstMap.of(tablePiece.getColumns(), context.getSubstMap()));
            TieredList<Op> newConjuncts = OpUtil.subst(tablePiece.getConjuncts(), columnsAndSubstMap.getSubstMap());
            context.setSubstMap(columnsAndSubstMap.getSubstMap());
            return tablePiece.builder().setColumns(columnsAndSubstMap.getColumns()).setConjuncts(newConjuncts).build();
        }

        @Override
        public PlanPiece visitStarJoin(StarJoinPiece joinPiece, Context context) {
            List<StarJoinPiece.StarCorner> newCorners = joinPiece.getCorners()
                    .stream().map(corner ->
                            new StarJoinPiece.StarCorner(
                                    OpUtil.subst(corner.getEqConjuncts(), context.getSubstMap()),
                                    OpUtil.subst(corner.getOtherConjuncts(), context.getSubstMap()),
                                    corner.getJoinType(),
                                    corner.getPiece()))
                    .collect(Collectors.toList());

            ColumnsAndSubstMap columnsAndSubstMap =
                    OpUtil.eliminateDerivedVars(ColumnsAndSubstMap.of(joinPiece.getColumns(), context.getSubstMap()));
            TieredList<Op> newConjuncts = OpUtil.subst(joinPiece.getConjuncts(), context.getSubstMap());
            context.setSubstMap(columnsAndSubstMap.getSubstMap());
            return joinPiece.builder().mustCast(StarJoinPiece.Builder.class)
                    .setCorners(newCorners)
                    .setColumns(columnsAndSubstMap.getColumns())
                    .setConjuncts(newConjuncts)
                    .build();
        }

        @Override
        public PlanPiece visitAggregate(AggregatePiece aggPiece, Context context) {
            return aggPiece.builder().mustCast(AggregatePiece.Builder.class)
                    .setDimensions(OpUtil.subst(aggPiece.getDimensions(), context.getSubstMap()))
                    .setRollupDimensions(OpUtil.subst(aggPiece.getRollupDimensions(), context.getSubstMap()))
                    .setMetrics(OpUtil.subst(aggPiece.getMetrics(), context.getSubstMap()))
                    .setDistinctMetrics(OpUtil.subst(aggPiece.getDistinctMetrics(), context.getSubstMap()))
                    .setHoistConjuncts(OpUtil.subst(aggPiece.getHoistConjuncts(), context.getSubstMap()))
                    .setNonHoistConjuncts(OpUtil.subst(aggPiece.getNonHoistConjuncts(), context.getSubstMap()))
                    .setConjuncts(OpUtil.subst(aggPiece.getConjuncts(), context.getSubstMap()))
                    .build();
        }
    }
}
