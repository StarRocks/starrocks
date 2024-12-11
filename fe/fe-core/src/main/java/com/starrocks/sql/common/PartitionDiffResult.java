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

import java.util.Map;
import java.util.Set;

public class PartitionDiffResult {
    // For external table, the mapping of base table partition to mv partition:
    // <base table, <base table partition name, mv partition name>>
    public final Map<Table, Map<String, Set<String>>> refBaseTableMVPartitionMap;
    // The partition range of the base tables: <base table, <base table partition name, base table partition range>>
    public final Map<Table, Map<String, PCell>> refBaseTablePartitionMap;
    // The partition range of the materialized view: <mv partition name, mv partition range>
    public final Map<String, PCell> mvPartitionToCells;
    // The diff result of partition range between materialized view and base tables
    public final PartitionDiff diff;

    public PartitionDiffResult(Map<Table, Map<String, Set<String>>> refBaseTableMVPartitionMap,
                               Map<Table, Map<String, PCell>> refBaseTablePartitionMap,
                               Map<String, PCell> mvPartitionToCells,
                               PartitionDiff diff) {
        this.refBaseTableMVPartitionMap = refBaseTableMVPartitionMap;
        this.refBaseTablePartitionMap = refBaseTablePartitionMap;
        this.diff = diff;
        this.mvPartitionToCells = mvPartitionToCells;
    }

    public Map<Table, Map<String, Set<String>>> getRefBaseTableMVPartitionMap() {
        return refBaseTableMVPartitionMap;
    }
}
