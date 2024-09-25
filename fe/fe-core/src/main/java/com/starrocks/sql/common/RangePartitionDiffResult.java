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

import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;

import java.util.Map;
import java.util.Set;

/**
 * The result of diffing between materialized view and the base tables.
 */
public class RangePartitionDiffResult extends PartitionDiffResult {
    // The partition range of the materialized view: <mv partition name, mv partition range>
    public final Map<String, Range<PartitionKey>> mvRangePartitionMap;
    // The partition range of the base tables: <base table, <base table partition name, base table partition range>>
    public final Map<Table, Map<String, Range<PartitionKey>>> refBaseTablePartitionMap;
    // The diff result of partition range between materialized view and base tables
    public final RangePartitionDiff rangePartitionDiff;

    public RangePartitionDiffResult(Map<String, Range<PartitionKey>> mvRangePartitionMap,
                                    Map<Table, Map<String, Range<PartitionKey>>> refBaseTablePartitionMap,
                                    Map<Table, Map<String, Set<String>>> refBaseTableMVPartitionMap,
                                    RangePartitionDiff rangePartitionDiff) {
        super(refBaseTableMVPartitionMap);
        this.mvRangePartitionMap = mvRangePartitionMap;
        this.refBaseTablePartitionMap = refBaseTablePartitionMap;
        this.rangePartitionDiff = rangePartitionDiff;
    }
}
