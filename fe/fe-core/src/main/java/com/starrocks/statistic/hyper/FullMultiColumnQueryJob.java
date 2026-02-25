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
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.base.ColumnStats;
import org.apache.velocity.VelocityContext;

import java.util.List;

public class FullMultiColumnQueryJob extends MultiColumnQueryJob {

    public FullMultiColumnQueryJob(ConnectContext context, Database db, Table table, List<ColumnStats> columnStats) {
        super(context, db, table, columnStats);
    }

    @Override
    public String buildStatisticsQuery() {
        List<String> sqlList = Lists.newArrayList();
        for (ColumnStats columnStats : columnStats) {
            VelocityContext context = new VelocityContext();
            context.put("version", StatsConstants.STATISTIC_MULTI_COLUMN_VERSION);
            context.put("dbName", db.getOriginName());
            context.put("tableName", table.getName());
            context.put("columnIdsStr", columnStats.getColumnNameStr());
            context.put("ndvFunction", columnStats.getNDV());
            String sql = HyperStatisticSQLs.build(context, HyperStatisticSQLs.FULL_MULTI_COLUMN_STATISTICS_SELECT_TEMPLATE);
            sqlList.add(sql);
        }

        return String.join(" UNION ALL ", sqlList);
    }
}
