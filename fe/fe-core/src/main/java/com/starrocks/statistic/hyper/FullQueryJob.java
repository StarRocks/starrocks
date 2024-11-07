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
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.List;
import java.util.stream.Collectors;

public class FullQueryJob extends HyperQueryJob {
    protected FullQueryJob(ConnectContext context, Database db,
                           Table table,
                           List<ColumnStats> columnStats,
                           List<Long> partitionIdList) {
        super(context, db, table, columnStats, partitionIdList);
    }

    @Override
    public void queryStatistics() {
        queryFullData();
    }

    private void queryFullData() {
        String tableName = StringEscapeUtils.escapeSql(db.getOriginName() + "." + table.getName());

        List<String> metaSQL = buildBatchFullQuerySQL(columnStats);
        for (String sql : metaSQL) {
            // execute sql
            List<TStatisticData> dataList = executeStatisticsQuery(sql, context);

            for (TStatisticData data : dataList) {
                Partition partition = table.getPartition(data.getPartitionId());
                if (partition == null) {
                    continue;
                }
                String partitionName = StringEscapeUtils.escapeSql(partition.getName());
                sqlBuffer.add(createInsertValueSQL(data, tableName, partitionName));
                rowsBuffer.add(createInsertValueExpr(data, tableName, partitionName));
            }
        }
    }

    private List<String> buildBatchFullQuerySQL(List<ColumnStats> queryColumns) {
        List<String> metaSQL = Lists.newArrayList();
        for (Long partitionId : partitionIdList) {
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                // statistics job doesn't lock DB, partition may be dropped, skip it
                continue;
            }

            for (ColumnStats columnStat : queryColumns) {
                String sql = StatisticSQLs.buildFullSQL(db, table, partition, columnStat,
                        StatisticSQLs.BATCH_FULL_STATISTIC_TEMPLATE);
                metaSQL.add(sql);
            }
        }

        int parts = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());
        List<List<String>> l = Lists.partition(metaSQL, parts);
        return l.stream().map(sql -> String.join(" UNION ALL ", sql)).collect(Collectors.toList());
    }

}
