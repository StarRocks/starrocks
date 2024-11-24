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

import com.starrocks.catalog.Column;
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
