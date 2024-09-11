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

import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.StarJoinPiece;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EliminateSemiAntiJoinPolicy extends AggregatePolicy.SimplePolicy {
    public static final EliminateSemiAntiJoinPolicy INSTANCE = new EliminateSemiAntiJoinPolicy();

    private EliminateSemiAntiJoinPolicy() {
    }

    @Override
    public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
        AggregatePiece.FlatTable flatTable = aggPiece.getFlatTable();
        PlanPiece flatTablePiece = flatTable.getPiece();
        if (!flatTablePiece.isStarJoin()) {
            return Optional.empty();
        }
        StarJoinPiece starJoin = aggPiece.getFlatTable().getPiece().cast();

        Map<Boolean, List<StarJoinPiece.StarCorner>> cornerGroups = starJoin.getCorners().stream()
                .collect(Collectors.partitioningBy(corner -> corner.getJoinType().isLeftSemiAntiJoin()));

        List<StarJoinPiece.StarCorner> leftSemiAntiCorners = cornerGroups.get(true);
        List<StarJoinPiece.StarCorner> restCorners = cornerGroups.get(false);

        if (leftSemiAntiCorners.isEmpty()) {
            return Optional.empty();
        }

        ColumnRefSet columnIds = new ColumnRefSet();
        for (StarJoinPiece.StarCorner corner : leftSemiAntiCorners) {
            Stream.of(corner.getOtherConjuncts().stream(), corner.getEqConjuncts().stream())
                    .flatMap(Function.identity())
                    .forEach(op -> columnIds.union(op.getIds()));
        }

        PlanPiece centre = starJoin.getCentre();
        ColumnRefSet centreColumnIds = ColumnRefSet.createByIds(centre.getColumns().keySet());
        columnIds.intersect(centreColumnIds);
        ColumnRefSet aggColumnIds = ColumnRefSet.createByIds(aggPiece.getColumns().keySet());
        columnIds.except(aggColumnIds);

        PlanPiece newFlatTablePiece;
        if (restCorners.isEmpty()) {
            newFlatTablePiece = centre.builder().setConjuncts(TieredList.<Op>genesis()).build();
        } else {
            newFlatTablePiece = starJoin.builder().mustCast(StarJoinPiece.Builder.class)
                    .setCentre(centre)
                    .setCorners(restCorners)
                    .setColumns(starJoin.getColumns())
                    .setConjuncts(TieredList.<Op>genesis())
                    .build().cast();
        }

        TieredMap<Integer, GenericColumn> newAddedColumns = starJoin.getColumns().entrySet().stream()
                .filter(e -> columnIds.contains(e.getKey()))
                .collect(TieredMap.toMap());

        AggregatePiece.FlatTable newFlatTable =
                new AggregatePiece.FlatTable(newFlatTablePiece, flatTable.getStiffConjuncts(),
                        flatTable.getFlexibleConjuncts());
        AggregatePiece newAggPiece = aggPiece.builder().mustCast(AggregatePiece.Builder.class)
                .setFlatTable(newFlatTable)
                .setDimensions(aggPiece.getDimensions().merge(newAddedColumns))
                .build();

        return Optional.of(newAggPiece);
    }
}
