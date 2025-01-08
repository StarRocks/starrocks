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
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ReprogramAggregatePolicy extends AggregatePolicy.SimplePolicy {
    public static final AbstractAggregatePolicy INSTANCE = new ReprogramAggregatePolicy();

    @Override
    public Optional<AggregatePiece> convert(AggregatePiece aggPiece) {
        if (!aggPiece.isStem()) {
            return Optional.empty();
        }
        ColumnRefSet groupKeyColRefSet = ColumnRefSet.createByIds(aggPiece.getDimensions().keySet());
        Map<Boolean, TieredList<Op>> conjGroups = aggPiece.getFlatTable().getStiffConjuncts().stream().collect(
                Collectors.partitioningBy(op -> groupKeyColRefSet.containsAll(op.getIds()), TieredList.<Op>toList()));

        TieredList<Op> hoistConjuncts = conjGroups.get(true);
        TieredList<Op> nonHoistConjuncts = conjGroups.get(false);
        Map<Boolean, TieredList<Op>> nonHoistConjunctGroups = nonHoistConjuncts
                .stream()
                .collect(Collectors.partitioningBy(OpUtil::isStiffPredicate, TieredList.<Op>toList()));

        TieredList<Op> stiffConjuncts = nonHoistConjunctGroups.get(true);
        TieredList<Op> flexibleConjuncts = nonHoistConjunctGroups.get(false);

        Set<Integer> rollupColumnRefs = flexibleConjuncts.stream().flatMap(op -> op.getIdSet().stream())
                .filter(colRef -> !groupKeyColRefSet.contains(colRef)).collect(Collectors.toSet());

        PlanPiece flatTablePiece = aggPiece.getFlatTable().getPiece();
        TieredMap<Integer, GenericColumn> rollupDimension = flatTablePiece.getColumns().entrySet()
                .stream().filter(e -> rollupColumnRefs.contains(e.getKey()))
                .collect(TieredMap.toMap());

        Map<Boolean, TieredMap<Integer, GenericColumn>> metricsGroup = aggPiece.getMetrics().entrySet()
                .stream()
                .collect(Collectors.partitioningBy(e -> OpUtil.isDistinct(e.getValue()), TieredMap.toMap()));

        TieredMap<Integer, GenericColumn> distinctMetrics = metricsGroup.get(true);

        TieredMap<Integer, GenericColumn> metrics = metricsGroup.get(false);
        flatTablePiece = flatTablePiece.setConjuncts(TieredList.<Op>genesis());
        AggregatePiece.FlatTable stemFlatTable =
                new AggregatePiece.FlatTable(flatTablePiece, stiffConjuncts, flexibleConjuncts);

        AggregatePiece stemAggPiece = AggregatePiece.newBuilder()
                .setFlatTable(stemFlatTable)
                .setDimensions(aggPiece.getDimensions())
                .setRollupDimensions(rollupDimension)
                .setMetrics(metrics)
                .setDistinctMetrics(distinctMetrics)
                .setHoistConjuncts(hoistConjuncts)
                .setNonHoistConjuncts(nonHoistConjuncts)
                .setConjuncts(TieredList.genesis())
                .setCommonState(aggPiece.getCommonState())
                .build().cast();
        return Optional.of(stemAggPiece);
    }
}
