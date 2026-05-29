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

package com.starrocks.sql.common;

import com.starrocks.catalog.Table;
import com.starrocks.mv.pct.BaseToMVPartitionMapping;

import java.util.Map;

public class PartitionDiffResult {
    // The partition range of the base tables: <base table, partition mapping>
    public final Map<Table, BaseToMVPartitionMapping> refBaseTablePartitionMap;
    // The partition range of the materialized view
    public final PCellSortedSet mvPartitionToCells;
    // The diff result of partition range between materialized view and base tables
    public final PartitionDiff diff;

    public PartitionDiffResult(Map<Table, BaseToMVPartitionMapping> refBaseTablePartitionMap,
                               PCellSortedSet mvPartitionToCells,
                               PartitionDiff diff) {
        this.refBaseTablePartitionMap = refBaseTablePartitionMap;
        this.diff = diff;
        this.mvPartitionToCells = mvPartitionToCells;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String  refBaseTablePartitionMapStr = refBaseTablePartitionMap.entrySet()
                .stream()
                .map(e -> e.getKey().getName() + "=" + e.getValue().cells().toString())
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
        sb.append("{")
                .append("refBaseTablePartitionMap:[").append(refBaseTablePartitionMapStr).append("]")
                .append(", mvPartitionToCells:[").append(mvPartitionToCells).append("]")
                .append(", diff:[").append(diff).append("]")
                .append("}");
        return sb.toString();
    }
}
