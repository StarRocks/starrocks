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
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.base.ColumnStats;
import com.starrocks.statistic.base.PartitionSampler;
import com.starrocks.statistic.sample.SampleInfo;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.velocity.VelocityContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * Split sample statistics query:
 * 1. max/max/count(1) to meta query
 * 2. length/count null/ndv/collection size to data query
 */
public class MetaQueryJob extends HyperQueryJob {
    // column_partition -> rows index, for find row
    private final Map<String, Integer> rowsIndex = Maps.newHashMap();
    private final List<TStatisticData> tempRowsBuffer = Lists.newArrayList();
    private final PartitionSampler sampler;

    protected MetaQueryJob(ConnectContext context, Database db, Table table, List<ColumnStats> columnStats,
                           List<Long> partitionIdList, PartitionSampler sampler) {
        super(context, db, table, columnStats, partitionIdList);
        this.sampler = sampler;
    }

    @Override
    public void queryStatistics() {
        tempRowsBuffer.clear();

        queryMetaMetric(columnStats);
        queryDataMetric(columnStats);

        tempRowsBuffer.clear();
    }

    private void queryMetaMetric(List<ColumnStats> queryColumns) {
        List<String> metaSQL = buildBatchMetaQuerySQL(queryColumns);
        for (String sql : metaSQL) {
            // execute sql
            List<TStatisticData> dataList = executeStatisticsQuery(sql, context);
            for (TStatisticData data : dataList) {
                Partition partition = table.getPartition(data.getPartitionId());
                if (partition == null) {
                    continue;
                }

                // init
                rowsIndex.put(data.getColumnName() + "_" + data.getPartitionId(), tempRowsBuffer.size());
                tempRowsBuffer.add(data);
            }
        }
    }

    private List<String> buildBatchMetaQuerySQL(List<ColumnStats> queryColumns) {
        List<String> metaSQL = Lists.newArrayList();
        for (Long partitionId : partitionIdList) {
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                // statistics job doesn't lock DB, partition may be dropped, skip it
                continue;
            }

            for (ColumnStats columnStat : queryColumns) {
                VelocityContext context = HyperStatisticSQLs.buildBaseContext(db, table, partition, columnStat);
                context.put("maxFunction", columnStat.getMax());
                context.put("minFunction", columnStat.getMin());
                String sql = HyperStatisticSQLs.build(context, HyperStatisticSQLs.BATCH_META_STATISTIC_TEMPLATE);
                metaSQL.add(sql);
            }
        }

        int parts = Math.max(1, context.getSessionVariable().getStatisticMetaCollectParallelism());
        List<List<String>> l = Lists.partition(metaSQL, parts);
        return l.stream().map(sql -> String.join(" UNION ALL ", sql)).collect(Collectors.toList());
    }

    private void queryDataMetric(List<ColumnStats> queryColumns) {
        String tableName = StringEscapeUtils.escapeSql(db.getOriginName() + "." + table.getName());

        List<String> metaSQL = buildBatchNDVQuerySQL(queryColumns);
        for (String sql : metaSQL) {
            // execute sql
            List<TStatisticData> dataList = executeStatisticsQuery(sql, context);
            for (TStatisticData data : dataList) {
                Partition partition = table.getPartition(data.getPartitionId());
                if (partition == null) {
                    continue;
                }
                String key = data.getColumnName() + "_" + data.getPartitionId();
                if (!rowsIndex.containsKey(key)) {
                    continue;
                }
                String partitionName = StringEscapeUtils.escapeSql(partition.getName());

                int index = rowsIndex.get(key);
                TStatisticData tempData = tempRowsBuffer.get(index);
                tempData.setNullCount(data.getNullCount()); // real null count
                tempData.setDataSize(data.getDataSize()); // real data size
                tempData.setHll(data.getHll()); // real hll
                tempData.setCollectionSize(data.getCollectionSize() <= 0 ? -1 : data.getCollectionSize());
                sqlBuffer.add(createInsertValueSQL(tempData, tableName, partitionName));
                rowsBuffer.add(createInsertValueExpr(tempData, tableName, partitionName));
            }
        }
    }

    private List<String> buildBatchNDVQuerySQL(List<ColumnStats> queryColumns) {
        int parts = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());
        List<List<ColumnStats>> partColumns = Lists.partition(queryColumns, parts);
        pipelineDop = partColumns.size() < parts ? parts / partColumns.size() : 1;

        List<String> metaSQL = Lists.newArrayList();
        for (Long partitionId : partitionIdList) {
            Partition partition = table.getPartition(partitionId);
            SampleInfo sampleInfo = sampler.getSampleInfo(partitionId);
            if (partition == null ||
                    sampleInfo == null || sampleInfo.getMaxSampleTabletNum() == 0 ||
                    !partition.hasData()) {
                // statistics job doesn't lock DB, partition may be dropped, skip it
                continue;
            }
            for (List<ColumnStats> part : partColumns) {
                String sql = HyperStatisticSQLs.buildSampleSQL(db, table, partition, part, sampler,
                        HyperStatisticSQLs.BATCH_DATA_STATISTIC_SELECT_TEMPLATE);
                metaSQL.add(sql);
            }
        }
        return metaSQL;
    }
}
