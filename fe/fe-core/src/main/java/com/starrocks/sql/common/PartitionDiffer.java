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
    public static void collectExternalPartitionNameMapping(Map<Table, Column> partitionTableAndColumns,
                                                           Map<Table, Map<String, Set<String>>> result) throws AnalysisException {
        for (Map.Entry<Table, Column> e1 : partitionTableAndColumns.entrySet()) {
            Table refBaseTable = e1.getKey();
            Column refPartitionColumn = e1.getValue();
            collectExternalBaseTablePartitionMapping(refBaseTable, refPartitionColumn, result);
        }
    }

    /**
     * Collect the external base table's partition name to its intersected materialized view names.
     * @param refBaseTable the base table
     * @param refTablePartitionColumn the partition column of the base table
     * @param result the result map
     * @throws AnalysisException
     */
    private static void collectExternalBaseTablePartitionMapping(
            Table refBaseTable,
            Column refTablePartitionColumn,
            Map<Table, Map<String, Set<String>>> result) throws AnalysisException {
        if (refBaseTable.isNativeTableOrMaterializedView()) {
            return;
        }
        Map<String, Set<String>> mvPartitionNameMap = PartitionUtil.getMVPartitionNameMapOfExternalTable(refBaseTable,
                refTablePartitionColumn, PartitionUtil.getPartitionNames(refBaseTable));
        result.put(refBaseTable, mvPartitionNameMap);
    }
}
