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

import com.starrocks.catalog.Type;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

//TODO(by satanson): PartitionPolicy is too naive to use it in product environment,
// a new sophisticated partition policy will substitute this naive one soon to support MV
// to partition data in reasonable time granules: for examples:
// 1. partition by data_trunc('day', dt);
// 2. partition by str2date(dt, '%Y-%m-%d');
public class PartitionPolicy {
    private static int typeWeight(Type type) {
        if (type.isDate()) {
            return 1;
        } else if (type.isDatetime()) {
            return 2;
        } else if (type.isIntegerType()) {
            return 3;
        } else if (type.isStringType()) {
            return 4;
        } else {
            return 5;
        }
    }

    public static Optional<PrettyPrinter> getPartitionExpr(AggregatePiece aggPiece,
                                                           TieredMap<Integer, ColumnAlias> columnAliases) {
        if (aggPiece.getDimensions().isEmpty()) {
            return Optional.empty();
        }
        List<GenericColumn> candiPartitionColumns = aggPiece.getPartitionColumns().stream()
                .flatMap(p -> p.second.values().stream())
                .collect(Collectors.toList());

        if (!candiPartitionColumns.isEmpty()) {
            Set<String> normSet = candiPartitionColumns.stream()
                    .map(GenericColumn::getNorm)
                    .map(GenericColumn::toString)
                    .collect(Collectors.toSet());

            List<Integer> candiPartitionColumnIds = aggPiece.getFlatTable().getColumns().entrySet().stream()
                    .filter(e -> normSet.contains(e.getValue().getNorm().toString()))
                    .sorted(Comparator.comparingInt(e -> typeWeight(e.getValue().getType())))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            for (Integer candiId : candiPartitionColumnIds) {
                Optional<String> optPartitionColumn =
                        Optional.ofNullable(columnAliases.get(candiId)).map(ColumnAlias::getName);
                if (optPartitionColumn.isPresent()) {
                    PrettyPrinter printer = new PrettyPrinter()
                            .add("PARTITION BY ")
                            .add(optPartitionColumn.get()).newLine();
                    return Optional.of(printer);
                }
            }
        }
        return Optional.empty();
    }
}
