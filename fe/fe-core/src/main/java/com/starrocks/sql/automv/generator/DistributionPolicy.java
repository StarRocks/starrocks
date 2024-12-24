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

import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.Util;

import java.util.List;

// DistributionPolicy is used to distribution of MV.
// 1. MV of non-group-by agg, it always output one row, so the bucket num is always 1;
// 2. MV of group-by agg, we use the harmony mean of bucket numbers of base tables as MV's bucket number.
//    MV, and MV's bucket number at least is 64.
public class DistributionPolicy {
    public static PrettyPrinter getDistribution(PlanPiece piece, List<String> bucketColumns) {
        PrettyPrinter printer = new PrettyPrinter();
        List<TablePiece> tablePieces = PlanPiece.collect(piece, TablePiece.class);
        double harmonyDivider = tablePieces.stream().map(tablePiece ->
                1.0 / Util.downcast(tablePiece.getTable().getTable(), OlapTable.class)
                        .map(olapTable -> Math.max(1, olapTable.getDefaultDistributionInfo().getBucketNum()))
                        .orElse(1)
        ).reduce(0.0, Double::sum);
        int harmonyMean = (int) (tablePieces.size() / harmonyDivider);
        int bucketNum = Math.max(harmonyMean, 64);
        // non-group-by agg mv, we use dummy column integer 1 as bucket column and set bucketNum to 1
        if (piece.cast(AggregatePiece.class).map(aggPiece -> aggPiece.getDimensions().isEmpty()).orElse(false)) {
            bucketNum = 1;
        }
        if (!bucketColumns.isEmpty()) {
            bucketColumns = bucketColumns.subList(0, Math.min(6, bucketColumns.size()));
            printer.add("DISTRIBUTED BY HASH").spaces(1).add("(")
                    .addItems(", ", bucketColumns).add(")")
                    .add(" BUCKETS ").add(bucketNum)
                    .newLine();
        } else {
            printer.add("DISTRIBUTED BY RANDOM BUCKETS 1").newLine();
        }
        return printer;
    }
}
