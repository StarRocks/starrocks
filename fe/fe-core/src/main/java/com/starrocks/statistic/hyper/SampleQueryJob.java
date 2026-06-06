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

package com.starrocks.statistic.hyper;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.base.ColumnStats;
import com.starrocks.statistic.base.PartitionSampler;
import com.starrocks.statistic.sample.SampleInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SampleQueryJob extends HyperQueryJob {
    private final PartitionSampler sampler;

    protected SampleQueryJob(ConnectContext context, long analyzeId, Database db,
                             Table table,
                             List<ColumnStats> columnStats,
                             List<Long> partitionIdList, PartitionSampler sampler) {
        super(context, analyzeId, db, table, columnStats, partitionIdList);
        this.sampler = sampler;
    }

    @Override
    protected List<String> buildQuerySQL() {
        int parts = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());

        // Isolate wide string columns to bound per-query Exchange memory; batch the rest.
        long threshold = Config.statistics_large_string_column_merge_threshold;
        Map<Boolean, List<ColumnStats>> partitioned = columnStats.stream()
                .collect(Collectors.partitioningBy(s -> threshold > 0 && isWideStringColumn(s, threshold)));
        List<ColumnStats> wideColumns = partitioned.get(true);
        List<ColumnStats> normalColumns = partitioned.get(false);
        List<List<ColumnStats>> normalPartColumns = Lists.partition(normalColumns, parts);

        List<String> sampleSQLs = Lists.newArrayList();
        for (Long partitionId : partitionIdList) {
            Partition partition = table.getPartition(partitionId);
            SampleInfo sampleInfo = sampler.getSampleInfo(partitionId);
            if (partition == null ||
                    sampleInfo == null ||
                    sampleInfo.getMaxSampleTabletNum() == 0 ||
                    !partition.hasData()) {
                // statistics job doesn't lock DB, partition may be dropped, skip it
                continue;
            }

            // wide columns: one SQL per column
            for (ColumnStats wideCol : wideColumns) {
                sampleSQLs.add(HyperStatisticSQLs.buildSampleSQL(db, table, partition,
                        List.of(wideCol), sampler, HyperStatisticSQLs.BATCH_SAMPLE_STATISTIC_SELECT_TEMPLATE));
            }
            // normal columns: batched by parts
            for (List<ColumnStats> batch : normalPartColumns) {
                sampleSQLs.add(HyperStatisticSQLs.buildSampleSQL(db, table, partition,
                        batch, sampler, HyperStatisticSQLs.BATCH_SAMPLE_STATISTIC_SELECT_TEMPLATE));
            }
        }
        return sampleSQLs;
    }
}
