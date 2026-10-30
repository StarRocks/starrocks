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
import org.apache.velocity.VelocityContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FullQueryJob extends HyperQueryJob {
    protected FullQueryJob(ConnectContext context, long analyzeId, Database db,
                           Table table,
                           List<ColumnStats> columnStats,
                           List<Long> partitionIdList) {
        super(context, analyzeId, db, table, columnStats, partitionIdList);
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

        List<String> result = Lists.newArrayList();
        List<String> normalSQL = Lists.newArrayList();

        for (Long partitionId : partitionIdList) {
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                // statistics job doesn't lock DB, partition may be dropped, skip it
                continue;
            }

            // wide columns: one SQL per column, no UNION ALL
            for (ColumnStats wideCol : wideColumns) {
                result.add(buildSingleColumnSQL(partition, wideCol));
            }
            // normal columns: collect for batched UNION ALL below
            for (ColumnStats columnStat : normalColumns) {
                normalSQL.add(buildSingleColumnSQL(partition, columnStat));
            }
        }

        // batch normal columns by parts and join with UNION ALL
        for (List<String> batch : Lists.partition(normalSQL, parts)) {
            result.add(String.join(" UNION ALL ", batch));
        }

        return result;
    }

    private String buildSingleColumnSQL(Partition partition, ColumnStats columnStat) {
        VelocityContext ctx = HyperStatisticSQLs.buildBaseContext(db, table, partition, columnStat);
        ctx.put("dataSize", columnStat.getFullDataSize());
        ctx.put("countNullFunction", columnStat.getFullNullCount());
        ctx.put("hllFunction", columnStat.getNDV());
        ctx.put("maxFunction", columnStat.getMax());
        ctx.put("minFunction", columnStat.getMin());
        ctx.put("collectionSizeFunction", columnStat.getCollectionSize());
        return HyperStatisticSQLs.build(ctx, HyperStatisticSQLs.BATCH_FULL_STATISTIC_TEMPLATE);
    }
}
