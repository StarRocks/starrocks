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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

// Finds and deletes the external column statistics of partitions that no longer exist in the table. Shared by
// the cleanup at the end of an external collection (ExternalFullStatisticsCollectJob#cleanupDroppedPartitionStats)
// and the periodic pass (AnalyzeMgr#clearStatisticFromExternalDroppedPartition).
//
// A partition is a candidate only when it is missing from the live partition list AND none of the given columns'
// rows of it was written within `graceSeconds`.
final class ExternalDroppedPartitionStats {
    private ExternalDroppedPartitionStats() {
    }

    // Outcome of deleteInBatches.
    static final class DeleteResult {
        final int batches;
        final int failedBatches;

        DeleteResult(int batches, int failedBatches) {
            this.batches = batches;
            this.failedBatches = failedBatches;
        }
    }

    // The table's complete partition list. Usually served from the connector's metadata cache.
    static List<String> listLivePartitionNames(Table table) {
        return ConnectorPartitionTraits.build(table).getPartitionNames();
    }

    // Partitions of `table` that have statistics of `columnNames` older than the grace but are not live.
    static List<String> findDroppedPartitions(ConnectContext context, StatisticExecutor executor, Table table,
                                              List<String> livePartitionNames, List<String> columnNames,
                                              long graceSeconds) {
        Set<String> live = toStoredPartitionNames(table, livePartitionNames);
        return executor.queryExternalStatPartitionsNotUpdatedSince(context, table.getUUID(), columnNames,
                        graceSeconds).stream()
                .filter(p -> !live.contains(p))
                .collect(Collectors.toList());
    }

    // Partitions and columns are batched separately and every pair of batches is deleted: both lists become an IN
    // predicate, and each IN is bounded by the DELETE's own limit, which applies to the shared-data (UNIQUE_KEYS)
    // statistics table. A periodic pass merges every column the table ever collected, so the column list can
    // exceed the limit too.
    static DeleteResult deleteInBatches(ConnectContext context, StatisticExecutor executor, Table table,
                                        List<String> droppedPartitions, List<String> columnNames) {
        int batchSize = Math.max(1,
                Math.min(Config.expr_children_limit / 2, Config.max_allowed_in_element_num_of_delete));
        int batches = 0;
        int failedBatches = 0;
        for (List<String> partitionBatch : Lists.partition(droppedPartitions, batchSize)) {
            for (List<String> columnBatch : Lists.partition(columnNames, batchSize)) {
                batches++;
                if (!executor.dropExternalStatDroppedPartitions(context, table.getUUID(), partitionBatch,
                        columnBatch)) {
                    failedBatches++;
                }
            }
        }
        return new DeleteResult(batches, failedBatches);
    }

    // Partition names as the collection stores them (see ExternalFullStatisticsCollectJob#buildColumnSelectFromCTE):
    // Iceberg names as listed, other connectors normalized so their NULL placeholders (e.g.
    // __HIVE_DEFAULT_PARTITION__) read as NULL. Comparing the raw listed names against the stored ones would take a
    // live NULL partition for a dropped one.
    static Set<String> toStoredPartitionNames(Table table, List<String> partitionNames) {
        if (table.isIcebergTable()) {
            return new HashSet<>(partitionNames);
        }
        Collection<String> nullValues = ExternalFullStatisticsCollectJob.resolveNullValues(table);
        List<String> partitionColumnNames = table.getPartitionColumnNames();
        return partitionNames.stream()
                .map(p -> PartitionUtil.normalizePartitionName(p, partitionColumnNames, nullValues))
                .collect(Collectors.toSet());
    }
}
