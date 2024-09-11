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
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ExpandAggregateMetricsPolicy extends AggregatePolicy.SimplePolicy {
    public static final ExpandAggregateMetricsPolicy INSTANCE = new ExpandAggregateMetricsPolicy();

    private ExpandAggregateMetricsPolicy() {
    }

    @Override
    public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
        Map<Boolean, TieredMap<Integer, GenericColumn>> columnGroups = aggPiece.getFlatTable().getPiece().getColumns()
                .entrySet().stream()
                .collect(Collectors.partitioningBy(e -> e.getValue().isOriginal(), TieredMap.toMap()));
        TieredMap<Integer, GenericColumn> originalColumns = columnGroups.get(true);
        TieredMap<Integer, GenericColumn> derivedColumns = columnGroups.get(false);
        TieredMap<Integer, Op> substMap = derivedColumns.entrySet()
                .stream().collect(TieredMap.toMap(Map.Entry::getKey, e -> e.getValue().getOp()));

        if (substMap.isEmpty()) {
            return Optional.empty();
        }

        ColumnRefSet columnIdsOnlyUsedByMetrics = ColumnRefSet.of();
        aggPiece.getMetrics().merge(aggPiece.getDistinctMetrics()).values()
                .stream()
                .map(GenericColumn::getOp)
                .map(Op::getIds)
                .forEach(columnIdsOnlyUsedByMetrics::union);

        aggPiece.getDimensions().merge(aggPiece.getRollupDimensions()).values()
                .stream()
                .filter(GenericColumn::isDerived)
                .map(GenericColumn::getOp)
                .map(Op::getIds)
                .forEach(columnIdsOnlyUsedByMetrics::except);

        TieredMap<Integer, GenericColumn> reservedDerivedColumns = derivedColumns.entrySet()
                .stream()
                .filter(e -> !columnIdsOnlyUsedByMetrics.contains(e.getKey()))
                .collect(TieredMap.toMap());

        TieredMap<Integer, GenericColumn> newMetrics = OpUtil.subst(aggPiece.getMetrics(), substMap);
        TieredMap<Integer, GenericColumn> newDistinctMetrics = OpUtil.subst(aggPiece.getDistinctMetrics(), substMap);

        PlanPiece newFlatTablePiece = aggPiece.getFlatTable().getPiece().builder()
                .setColumns(originalColumns.merge(reservedDerivedColumns))
                .build();
        AggregatePiece.FlatTable newFlatTable =
                new AggregatePiece.FlatTable(newFlatTablePiece, aggPiece.getFlatTable().getStiffConjuncts(),
                        aggPiece.getFlatTable().getFlexibleConjuncts());
        AggregatePiece newAggPiece = aggPiece.builder().mustCast(AggregatePiece.Builder.class)
                .setFlatTable(newFlatTable)
                .setMetrics(newMetrics)
                .setDistinctMetrics(newDistinctMetrics)
                .build();
        return Optional.of(newAggPiece);
    }
}
