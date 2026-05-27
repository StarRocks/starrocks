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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.mv.pct.BaseToMVPartitionMapping;

import java.util.Collections;
import java.util.Map;

/**
 * {@link PartitionDiffer} is used to compare the difference between two partitions which can be range
 * partition or list partition.
 */
public abstract class PartitionDiffer {
    protected final MaterializedView mv;
    // whether it's used for query rewrite or refresh, the difference is that query rewrite will not
    // consider partition_ttl_number and mv refresh will consider it to avoid creating too much partitions
    protected final MVTimelinessArbiter.QueryRewriteParams queryRewriteParams;

    // Pinned ranges keyed by Table.getTableIdentifier(). Empty means no pinning.
    protected Map<String, TvrVersionRange> pinnedRangeByTableIdentifier = Collections.emptyMap();

    public PartitionDiffer(MaterializedView mv, MVTimelinessArbiter.QueryRewriteParams queryRewriteParams) {
        this.mv = mv;
        this.queryRewriteParams = queryRewriteParams;
    }

    public void setPinnedRanges(Map<String, TvrVersionRange> pinnedRangeByTableIdentifier) {
        this.pinnedRangeByTableIdentifier = pinnedRangeByTableIdentifier == null
                ? Collections.emptyMap() : pinnedRangeByTableIdentifier;
    }

    protected TvrVersionRange pinnedRangeFor(Table table) {
        return pinnedRangeByTableIdentifier.get(table.getTableIdentifier());
    }

    /**
     * Collect the ref base table's partition range map.
     * @return the ref base table's partition range map: <ref base table, <partition name, partition range>>
     */
    public abstract Map<Table, BaseToMVPartitionMapping> syncBaseTablePartitionInfos();

    /**
     * Compute the partition difference between materialized view and all ref base tables.
     * @param rangeToInclude: <partition start, partition end> pair which is use for range differ.

     * @return MvPartitionDiffResult: the result of partition difference
     */
    public abstract PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude);

    public abstract PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude,
                                                             Map<Table, BaseToMVPartitionMapping> refBaseTablePartitionMap);
    /**
     * Generate the reference map between the base table and the mv.
     *
     * NOTE: the result with base table's partition cell is not the normalized cell by mv's partition exprs, but the exact base
     * tables' partition cell.
     *
     * @param basePartitionMaps src partition sorted set of the base table
     * @param mvPartitionMap mv partition sorted set
     * @return base table -> <partition name, mv partition names> mapping
     */
    public abstract Map<Table, PCellSetMapping> generateBaseRefMap(Map<Table, PCellSortedSet> basePartitionMaps,
                                                                   PCellSortedSet mvPartitionMap);

    /**
     * Generate the mapping from materialized view partition to base table partition.
     *
     * NOTE: the result with base table's partition cell is not the normalized cell by mv's partition exprs, but the exact base
     * tables' partition cell.
     *
     * @param mvPCells : materialized view partition sorted set
     * @param baseTablePCells: base table partition sorted set map
     * @return mv partition name -> <base table, base partition names> mapping
     */
    public abstract Map<String, Map<Table, PCellSortedSet>> generateMvRefMap(PCellSortedSet mvPCells,
                                                                             Map<Table, PCellSortedSet> baseTablePCells);
}
