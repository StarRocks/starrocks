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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.base.ColumnStats;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.Arrays;
import java.util.List;

import static com.starrocks.statistic.StatsConstants.COLUMN_ID_SEPARATOR;

public abstract class MultiColumnQueryJob extends HyperQueryJob {

    public MultiColumnQueryJob(ConnectContext context, Database db, Table table, List<ColumnStats> columnStats) {
        super(context, db, table, columnStats, null);
    }

    protected abstract String buildStatisticsQuery();

    @Override
    public void queryStatistics() {
        String sql = buildStatisticsQuery();
        List<TStatisticData> dataList = executeStatisticsQuery(sql, context);
        for (TStatisticData data : dataList) {
            String tableName = StringEscapeUtils.escapeSql(db.getOriginName() + "." + table.getName());
            sqlBuffer.add(createInsertValueSQL(data, tableName));
            rowsBuffer.add(createInsertValueExpr(data, tableName));
        }
    }

    public String createInsertValueSQL(TStatisticData data, String tableName) {
        List<String> params = Lists.newArrayList();
        params.add(String.valueOf(table.getId()));
        params.add("'" + StringEscapeUtils.escapeSql(data.getColumnName()) + "'");
        params.add(String.valueOf(db.getId()));
        params.add("'" + tableName + "'");
        params.add("'" + StringEscapeUtils.escapeSql(getColumnNames(data.getColumnName())) + "'");
        params.add(String.valueOf(data.getCountDistinct()));
        params.add("now()");
        return "(" + String.join(", ", params) + ")";
    }

    public List<Expr> createInsertValueExpr(TStatisticData data, String tableName) {
        List<Expr> row = Lists.newArrayList();
        row.add(new IntLiteral(table.getId(), Type.BIGINT));
        row.add(new StringLiteral(data.getColumnName()));
        row.add(new IntLiteral(db.getId(), Type.BIGINT));
        row.add(new StringLiteral(tableName));
        row.add(new StringLiteral(getColumnNames(data.getColumnName())));
        row.add(new IntLiteral(data.getCountDistinct(), Type.BIGINT));
        row.add(nowFn());
        return row;
    }

    private String getColumnNames(String combinedColumnName) {
        String[] columnIds = combinedColumnName.split(COLUMN_ID_SEPARATOR);

        return String.join(",", Arrays.stream(columnIds)
                .map(x -> {
                    Column column = table.getColumnByUniqueId(Long.parseLong(x));
                    return column != null ? column.getName() : "";
                })
                .toArray(String[]::new));
    }
}
