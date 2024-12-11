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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.PartitionUtil;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link PartitionDiffer} is used to compare the difference between two partitions which can be range
 * partition or list partition.
 */
public abstract class PartitionDiffer {
    protected final MaterializedView mv;
    // whether it's used for query rewrite or refresh, the difference is that query rewrite will not
    // consider partition_ttl_number and mv refresh will consider it to avoid creating too much partitions
    protected final boolean isQueryRewrite;

    public PartitionDiffer(MaterializedView mv, boolean isQueryRewrite) {
        this.mv = mv;
        this.isQueryRewrite = isQueryRewrite;
    }

    /**
     * Collect the ref base table's partition range map.
     * @return the ref base table's partition range map: <ref base table, <partition name, partition range>>
     */
    public abstract Map<Table, Map<String, PCell>> syncBaseTablePartitionInfos();

    /**
     * Compute the partition difference between materialized view and all ref base tables.
     * @param rangeToInclude: <partition start, partition end> pair which is use for range differ.

     * @return MvPartitionDiffResult: the result of partition difference
     */
    public abstract PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude);

    public abstract PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude,
                                                             Map<Table, Map<String, PCell>> rBTPartitionMap);
    /**
     * Generate the reference map between the base table and the mv.
     * @param baseRangeMap src partition list map of the base table
     * @param mvRangeMap mv partition name to its list partition cell
     * @return base table -> <partition name, mv partition names> mapping
     */
    public abstract Map<Table, Map<String, Set<String>>> generateBaseRefMap(Map<Table, Map<String, PCell>> baseRangeMap,
                                                                            Map<String, PCell> mvRangeMap);

    /**
     * Generate the mapping from materialized view partition to base table partition.
     * @param mvRangeMap : materialized view partition range map: <partitionName, partitionRange>
     * @param baseRangeMap: base table partition range map, <baseTable, <partitionName, partitionRange>>
     * @return mv partition name -> <base table, base partition names> mapping
     */
    public abstract Map<String, Map<Table, Set<String>>> generateMvRefMap(Map<String, PCell> mvRangeMap,
                                                                          Map<Table, Map<String, PCell>> baseRangeMap);
    /**
     * To solve multi partition columns' problem of external table, record the mv partition name to all the same
     * partition names map here.
     * @param partitionTableAndColumns the partition table and its partition column
     * @param result the result map
     */
    public static void collectExternalPartitionNameMapping(Map<Table, List<Column>> partitionTableAndColumns,
                                                           Map<Table, Map<String, Set<String>>> result) throws AnalysisException {
        for (Map.Entry<Table, List<Column>> e : partitionTableAndColumns.entrySet()) {
            Table refBaseTable = e.getKey();
            List<Column> refPartitionColumns = e.getValue();
            collectExternalBaseTablePartitionMapping(refBaseTable, refPartitionColumns, result);
        }
    }

    /**
     * Collect the external base table's partition name to its intersected materialized view names.
     * @param refBaseTable the base table
     * @param refTablePartitionColumns the partition column of the base table
     * @param result the result map
     * @throws AnalysisException
     */
    private static void collectExternalBaseTablePartitionMapping(
            Table refBaseTable,
            List<Column> refTablePartitionColumns,
            Map<Table, Map<String, Set<String>>> result) throws AnalysisException {
        if (refBaseTable.isNativeTableOrMaterializedView()) {
            return;
        }
        Map<String, Set<String>> mvPartitionNameMap = PartitionUtil.getMVPartitionNameMapOfExternalTable(refBaseTable,
                refTablePartitionColumns, PartitionUtil.getPartitionNames(refBaseTable));
        result.put(refBaseTable, mvPartitionNameMap);
    }
}
