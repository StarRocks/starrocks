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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.base.ColumnStats;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringEscapeUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class ConstQueryJob extends HyperQueryJob {
    private final Map<Long, Long> partitionRowCounts = Maps.newHashMap();

    protected ConstQueryJob(ConnectContext context, Database db,
                            Table table,
                            List<ColumnStats> columnStats,
                            List<Long> partitionIdList) {
        super(context, db, table, columnStats, partitionIdList);
        initPartitionRowCounts();
    }

    private void initPartitionRowCounts() {
        for (Long pid : partitionIdList) {
            Partition partition = table.getPartition(pid);
            if (partition == null || !partition.hasData()) {
                continue;
            }
            partitionRowCounts.put(pid, partition.getRowCount());
        }
    }

    @Override
    public void queryStatistics() {
        String tableName = StringEscapeUtils.escapeSql(db.getOriginName() + "." + table.getName());

        for (Long pid : partitionIdList) {
            Partition partition = table.getPartition(pid);
            if (partition == null || !partition.hasData() || !partitionRowCounts.containsKey(pid)) {
                // statistics job doesn't lock DB, partition may be dropped, skip it
                continue;
            }

            long rowCount = partitionRowCounts.get(pid);
            String partitionName = StringEscapeUtils.escapeSql(partition.getName());
            for (ColumnStats column : columnStats) {
                TStatisticData data = new TStatisticData();
                data.setPartitionId(pid);
                data.setRowCount(rowCount);
                data.setColumnName(column.getColumnNameStr());
                data.setHll("00".getBytes(StandardCharsets.UTF_8));
                data.setNullCount(0);
                data.setDataSize(column.getTypeSize() * rowCount);
                data.setMax("");
                data.setMin("");
                data.setCollectionSize(-1);
                sqlBuffer.add(createInsertValueSQL(data, tableName, partitionName));
                rowsBuffer.add(createInsertValueExpr(data, tableName, partitionName));
            }
        }
    }
}
