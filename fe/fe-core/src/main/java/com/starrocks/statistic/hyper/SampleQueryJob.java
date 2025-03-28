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
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.base.ColumnStats;
import com.starrocks.statistic.base.PartitionSampler;
import com.starrocks.statistic.sample.SampleInfo;

import java.util.List;

public class SampleQueryJob extends HyperQueryJob {
    private final PartitionSampler sampler;

    protected SampleQueryJob(ConnectContext context, Database db,
                             Table table,
                             List<ColumnStats> columnStats,
                             List<Long> partitionIdList, PartitionSampler sampler) {
        super(context, db, table, columnStats, partitionIdList);
        this.sampler = sampler;
    }

    @Override
    protected List<String> buildQuerySQL() {
        int parts = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());

        List<List<ColumnStats>> partColumns = Lists.partition(columnStats, parts);
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
            for (List<ColumnStats> stats : partColumns) {
                String sql = HyperStatisticSQLs.buildSampleSQL(db, table, partition, stats, sampler,
                        HyperStatisticSQLs.BATCH_SAMPLE_STATISTIC_SELECT_TEMPLATE);
                sampleSQLs.add(sql);
            }
        }
        return sampleSQLs;
    }
}
