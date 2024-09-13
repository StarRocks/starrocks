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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The result of diffing between materialized view and the base tables.
 */
public class ListPartitionDiffResult extends PartitionDiffResult {
    // The partition range of the materialized view: <mv partition name, mv partition range>
    public final Map<String, PListCell> mvListPartitionMap;
    // The partition range of the base tables: <base table, <base table partition name, base table partition range>>
    public final Map<Table, Map<String, PListCell>> refBaseTablePartitionMap;
    // The diff result of partition range between materialized view and base tables
    public final ListPartitionDiff listPartitionDiff;

    // MV partition column ref index for each base table's partition columns
    public final Map<Table, List<Integer>> refBaseTableRefIdxMap;

    public ListPartitionDiffResult(Map<String, PListCell> mvListPartitionMap,
                                   Map<Table, Map<String, PListCell>> refBaseTablePartitionMap,
                                   ListPartitionDiff listPartitionDiff,
                                   Map<Table, List<Integer>> refBaseTableRefIdxMap,
                                   Map<Table, Map<String, Set<String>>> refBaseTableMVPartitionMap) {
        super(refBaseTableMVPartitionMap);
        this.mvListPartitionMap = mvListPartitionMap;
        this.refBaseTablePartitionMap = refBaseTablePartitionMap;
        this.listPartitionDiff = listPartitionDiff;
        this.refBaseTableRefIdxMap = refBaseTableRefIdxMap;
    }
}
