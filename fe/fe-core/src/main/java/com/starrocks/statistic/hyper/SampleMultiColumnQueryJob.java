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
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.base.ColumnStats;
import com.starrocks.statistic.sample.SampleInfo;
import com.starrocks.statistic.sample.TabletSampleManager;
import com.starrocks.statistic.sample.TabletStats;
import org.apache.velocity.VelocityContext;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.statistic.sample.TabletSampleManager.MAX_ROW_COUNT_BY_BLOCK_SAMPLE;

public class SampleMultiColumnQueryJob extends MultiColumnQueryJob {
    private final SampleInfo sampleInfo;
    private final TabletSampleManager manager;

    public SampleMultiColumnQueryJob(ConnectContext context, Database db, Table table,
                                     List<ColumnStats> columnStats, TabletSampleManager sampleManager) {
        super(context, db, table, columnStats);
        this.manager = sampleManager;
        this.sampleInfo = sampleManager.generateSampleInfo();
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
            context.put("combined_column_key", columnStats.getCombinedMultiColumnKey());
            context.put("ndvFunction", columnStats.getSampleNDV(sampleInfo));
            context.put("base_cte_table", buildDataQuery(columnStats));
            String sql = HyperStatisticSQLs.build(context, HyperStatisticSQLs.SAMPLE_MULTI_COLUMN_STATISTICS_SELECT_TMEPLATE);
            sqlList.add(sql);
        }

        return String.join(" UNION ALL ", sqlList);
    }

    private String buildDataQuery(ColumnStats columnStats) {
        StringBuilder builder = new StringBuilder();
        builder.append("WITH base_cte_table as (");

        String fullQualifiedName = "`" + db.getOriginName() + "`.`" + table.getName() + "`";
        String col = columnStats.getCombinedMultiColumnKey() + " as combined_column_key";

        List<TabletStats> highWeightTablets = sampleInfo.getHighWeightTablets();
        List<TabletStats> mediumHighWeightTablets = sampleInfo.getMediumHighWeightTablets();
        List<TabletStats> mediumLowWeightTablets = sampleInfo.getMediumLowWeightTablets();
        List<TabletStats> lowWeightTablets = sampleInfo.getLowWeightTablets();

        StringBuilder sql = new StringBuilder();

        addTableSelect(sql, highWeightTablets, "t_high", col, fullQualifiedName,
                manager.getHighWeight().getTabletReadRatio());
        addTableSelect(sql, mediumHighWeightTablets, "t_medium_high", col, fullQualifiedName,
                manager.getMediumHighWeight().getTabletReadRatio());
        addTableSelect(sql, mediumLowWeightTablets, "t_medium_low", col, fullQualifiedName,
                manager.getMediumLowWeight().getTabletReadRatio());
        addTableSelect(sql, lowWeightTablets, "t_low", col, fullQualifiedName,
                manager.getLowWeight().getTabletReadRatio());

        if (sql.isEmpty()) {
            sql.append("SELECT ").append(col).append(" FROM ").append(fullQualifiedName)
                    .append(" LIMIT ").append(Config.statistic_sample_collect_rows);
        }

        return builder.append(sql).append(") ").toString();
    }

    private void addTableSelect(StringBuilder sql, List<TabletStats> tablets, String alias,
                                String columnNames, String fullQualifiedName, double ratio) {
        if (!tablets.isEmpty()) {
            if (!sql.isEmpty()) {
                sql.append(" UNION ALL ");
            }
            sql.append("SELECT * FROM (")
                    .append("SELECT ").append(columnNames).append(" FROM ")
                    .append(fullQualifiedName)
                    .append(buildHint(tablets, ratio))
                    .append(") ").append(alias);
        }
    }

    private String buildHint(List<TabletStats> tabletStats, double readRatio) {
        if (tabletStats.isEmpty()) {
            return "";
        }

        StringBuilder hint = new StringBuilder();
        hint.append(" TABLET");
        hint.append(tabletStats.stream()
                .map(e -> String.valueOf(e.getTabletId()))
                .collect(Collectors.joining(", ", "(", ")")));

        int percent = Math.max(1, Math.min(100, (int) (readRatio * 100)));
        String sampleMethod = sampleInfo.getTotalRowCount() > MAX_ROW_COUNT_BY_BLOCK_SAMPLE ? "by_page" : "by_block";

        hint.append(String.format(" SAMPLE('percent'='%d', 'method'='%s')", percent, sampleMethod));
        return hint.toString();
    }
}
