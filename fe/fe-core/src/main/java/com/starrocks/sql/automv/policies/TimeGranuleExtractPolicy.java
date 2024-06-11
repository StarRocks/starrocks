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

import com.google.common.base.Preconditions;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PieceCommonState;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpPlus;
import com.starrocks.sql.automv.pn.OpPlus2;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.StrictOp;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Optional;

// TimeGranuleExtractPolicy is used to extract time granules instead of columns from the conjuncts of
// flat table, then construct columns from these time granules, finally put these columns into both flat table's
// output columns and AggregatePiece's rollupDimensions; later, TimeGranulePartitionPolicy would select
// partition columns among these columns.
public class TimeGranuleExtractPolicy extends AggregatePolicy.SimplePolicy {
    public static final AbstractAggregatePolicy INSTANCE = new TimeGranuleExtractPolicy();

    private TimeGranuleExtractPolicy() {
    }

    @Override
    public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
        Preconditions.checkArgument(aggPiece.getDistinctMetrics().isEmpty());
        Preconditions.checkArgument(aggPiece.getMetrics().values().stream().allMatch(AggregatePolicies::isRollupAble));
        TieredList<Op> conjuncts = aggPiece.getFlatTable().getConjuncts();
        TieredMap<Integer, GenericColumn> columns = aggPiece.getFlatTable().getColumns();
        TieredMap<StrictOp, Integer> alreadyExists = OpUtil.columnsToStrictOpMap(columns);

        ColumnRefToIdConverter newIdConverter = aggPiece.getCommonState().getIdConverter().duplicate();
        TieredList.Builder<Op> newConjunctsBuilder = TieredList.<Op>newGenesisTier();
        TieredMap<Integer, GenericColumn> extraColumns = TieredMap.genesis();
        for (Op op : conjuncts) {
            Optional<OpPlus2> optNewOpPlus2 = OpUtil.rewriteRollupAbleTimeGranule(
                    OpPlus.of(op, 0), newIdConverter::nextId, alreadyExists);

            if (optNewOpPlus2.isEmpty()) {
                newConjunctsBuilder.add(op);
                continue;
            }

            OpPlus2 newOpPlus2 = optNewOpPlus2.get();
            newConjunctsBuilder.add(newOpPlus2.getOp().getOp());

            TieredMap<Integer, GenericColumn> deltaExtraColumns = newOpPlus2.getNewColumns();
            extraColumns = extraColumns.merge(deltaExtraColumns);
            alreadyExists = alreadyExists.merge(OpUtil.columnsToStrictOpMap(deltaExtraColumns));
        }

        // No time granules are extracted, the AggregatePiece need not transformed. if
        // a AggregatePiece has no time granules to pick up partition-by column, TimeGranulePartitionPolicy
        // would complement a default time-granule as partition-by column.
        if (extraColumns.isEmpty()) {
            return Optional.empty();
        }

        TieredMap<Integer, GenericColumn> newColumns = columns.merge(extraColumns);
        TieredList<Op> newConjuncts = newConjunctsBuilder.build();
        ColumnRefSet rollupDimensionIds = ColumnRefSet.of();
        newConjuncts.forEach(op -> rollupDimensionIds.union(op.getIds()));

        TieredMap<Integer, GenericColumn> newRollupDimensions = newColumns.entrySet()
                .stream()
                .filter(e -> rollupDimensionIds.contains(e.getKey()))
                .collect(TieredMap.toMap());
        PieceCommonState newCommonState =
                new PieceCommonState(newIdConverter, aggPiece.getCommonState().getFqTableMap());
        PlanPiece newFlatTable = aggPiece.getFlatTable().builder()
                .setConjuncts(newConjuncts)
                .setColumns(newColumns)
                .setCommonState(newCommonState)
                .build();
        AggregatePiece newAggPiece = aggPiece.builder().mustCast(AggregatePiece.Builder.class)
                .setRollupDimensions(newRollupDimensions)
                .setHoistConjuncts(newConjuncts)
                .setFlatTable(newFlatTable)
                .setCommonState(newCommonState)
                .build().cast();

        return Optional.of(newAggPiece);
    }
}
