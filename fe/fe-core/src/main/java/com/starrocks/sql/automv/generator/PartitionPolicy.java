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

package com.starrocks.sql.automv.generator;

import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.TimeGranule;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

//TODO(by satanson): PartitionPolicy is too naive to use it in product environment,
// a new sophisticated partition policy will substitute this naive one soon to support MV
// to partition data in reasonable time granules: for examples:
// 1. partition by data_trunc('day', dt);
// 2. partition by str2date(dt, '%Y-%m-%d');
public class PartitionPolicy {
    public static Optional<PrettyPrinter> getPartitionExpr(AggregatePiece aggPiece,
                                                           TieredMap<Integer, ColumnAlias> columnAliases) {
        if (aggPiece.getDimensions().isEmpty()) {
            return Optional.empty();
        }

        ColumnRefSet partitionByColumnIds = ColumnRefSet.of();
        aggPiece.getPartitionColumns().forEach(p ->
                partitionByColumnIds.union(ColumnRefSet.createByIds(p.second.keySet())));

        List<Pair<Integer, TimeGranule>> partitionByTimeGranules =
                OpUtil.extractPartitionByTimeGranule(aggPiece.getDimensions(), partitionByColumnIds);

        Optional<Pair<Integer, TimeGranule>> optChosenTimeGranule =
                partitionByTimeGranules.stream().max(Comparator.comparing(p -> p.second, TimeGranule.getComparator()));
        if (optChosenTimeGranule.isEmpty()) {
            return Optional.empty();
        }

        Pair<Integer, TimeGranule> chosenTimeGranule = optChosenTimeGranule.get();
        int timeGranuleId = chosenTimeGranule.first;
        TimeGranule timeGranule = chosenTimeGranule.second;
        if (timeGranule.isFineGrained(TimeGranule.Unit.MINUTE)) {
            return Optional.empty();
        }

        PrettyPrinter printer = new PrettyPrinter()
                .add("PARTITION BY ")
                .add(Objects.requireNonNull(columnAliases.get(timeGranuleId)).getName()).newLine();
        return Optional.of(printer);
    }
}
