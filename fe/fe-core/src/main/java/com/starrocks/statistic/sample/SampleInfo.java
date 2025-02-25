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

package com.starrocks.statistic.sample;

import com.starrocks.common.Config;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.SAMPLE_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME;

public class SampleInfo {
    private final double tabletSampleRatio;

    private final long sampleRowCount;

    private final long totalRowCount;

    private final double rowSampleRatio;

    private final List<TabletStats> highWeightTablets;

    private final List<TabletStats> mediumHighWeightTablets;

    private final List<TabletStats> mediumLowWeightTablets;

    private final List<TabletStats> lowWeightTablets;

    public SampleInfo() {
        this.tabletSampleRatio = 1;
        this.sampleRowCount = 1;
        this.totalRowCount = 1;
        this.rowSampleRatio = 1;
        this.highWeightTablets = null;
        this.mediumHighWeightTablets = null;
        this.mediumLowWeightTablets = null;
        this.lowWeightTablets = null;
    }

    public SampleInfo(double tabletSampleRatio,
                      long sampleRowCount, long totalRowCount,
                      List<TabletStats> highWeightTablets,
                      List<TabletStats> mediumHighWeightTablets,
                      List<TabletStats> mediumLowWeightTablets,
                      List<TabletStats> lowWeightTablets) {
        this.tabletSampleRatio = tabletSampleRatio;
        this.sampleRowCount = sampleRowCount;
        this.totalRowCount = totalRowCount;
        this.rowSampleRatio = sampleRowCount * 1.0 / totalRowCount;
        this.highWeightTablets = highWeightTablets;
        this.mediumHighWeightTablets = mediumHighWeightTablets;
        this.mediumLowWeightTablets = mediumLowWeightTablets;
        this.lowWeightTablets = lowWeightTablets;
    }

    public long getTotalRowCount() {
        return totalRowCount;
    }

    public double getRowSampleRatio() {
        return rowSampleRatio;
    }

    public List<TabletStats> getHighWeightTablets() {
        return highWeightTablets;
    }

    public List<TabletStats> getMediumHighWeightTablets() {
        return mediumHighWeightTablets;
    }

    public List<TabletStats> getMediumLowWeightTablets() {
        return mediumLowWeightTablets;
    }

    public List<TabletStats> getLowWeightTablets() {
        return lowWeightTablets;
    }

    public double getTabletSampleRatio() {
        return tabletSampleRatio;
    }

    public long getSampleRowCount() {
        return sampleRowCount;
    }

    public int getMaxSampleTabletNum() {
        int max = highWeightTablets.size();
        max = Math.max(max, mediumHighWeightTablets.size());
        max = Math.max(max, mediumLowWeightTablets.size());
        max = Math.max(max, lowWeightTablets.size());
        return max;
    }

    public String generateComplexTypeColumnTask(long tableId, long dbId, String tableName, String dbName,
                                                List<ColumnStats> complexTypeStats) {
        String sep = ", ";
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(SAMPLE_STATISTICS_TABLE_NAME).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());
        String columnNames = "(" + String.join(", ", targetColumnNames) + ")";

        String prefix = "INSERT INTO " + STATISTICS_DB_NAME + "." + SAMPLE_STATISTICS_TABLE_NAME + columnNames + " VALUES ";
        StringJoiner joiner = new StringJoiner(sep, prefix, ";");

        for (ColumnStats columnStats : complexTypeStats) {
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            builder.append(tableId).append(sep);
            builder.append(addSingleQuote(columnStats.getColumnName())).append(sep);
            builder.append(dbId).append(sep);
            builder.append("'").append(StringEscapeUtils.escapeSql(dbName)).append(".")
                    .append(StringEscapeUtils.escapeSql(tableName)).append("'").append(sep);
            builder.append(addSingleQuote(dbName)).append(sep);
            builder.append(columnStats.getRowCount()).append(sep);
            builder.append(columnStats.getDateSize()).append(sep);
            builder.append(columnStats.getDistinctCount(0L)).append(sep);
            builder.append(columnStats.getNullCount()).append(sep);
            builder.append(columnStats.getMax()).append(sep);
            builder.append(columnStats.getMin()).append(sep);
            builder.append("NOW()");
            builder.append(")");
            joiner.add(builder);
        }
        return joiner.toString();
    }

    public String generatePrimitiveTypeColumnTask(long tableId, long dbId, String tableName, String dbName,
                                                  List<ColumnStats> primitiveTypeStats,
                                                  TabletSampleManager manager) {
        String prefix = "INSERT INTO " + STATISTICS_DB_NAME + "." + SAMPLE_STATISTICS_TABLE_NAME;
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(SAMPLE_STATISTICS_TABLE_NAME).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());
        String columnNames = "(" + String.join(", ", targetColumnNames) + ")";
        StringBuilder builder = new StringBuilder();
        builder.append(prefix).append(columnNames).append(" ");
        builder.append("WITH base_cte_table as (");
        String queryDataSql = generateQueryDataSql(tableName, dbName, primitiveTypeStats, manager);
        builder.append(queryDataSql).append(") ");

        int idx = 0;
        int size = primitiveTypeStats.size();
        for (ColumnStats columnStats : primitiveTypeStats) {
            idx++;
            builder.append(generateQueryColumnSql(tableId, dbId, tableName, dbName, columnStats, "col_" + idx));
            if (idx != size) {
                builder.append(" UNION ALL ");
            }
        }
        return builder.toString();
    }

    private String generateQueryDataSql(String tableName, String dbName,
                                       List<ColumnStats> primitiveTypeStats,
                                       TabletSampleManager manager) {
        StringBuilder sql = new StringBuilder();
        String fullQualifiedName = "`" + dbName + "`.`" + tableName + "`";
        StringJoiner joiner = new StringJoiner(", ");
        for (int i = 0; i < primitiveTypeStats.size(); i++) {
            joiner.add(primitiveTypeStats.get(i).getQuotedColumnName() + " as col_" + (i + 1));
        }
        String columnNames = joiner.toString();
        if (!highWeightTablets.isEmpty()) {
            sql.append("SELECT * FROM (")
                    .append("SELECT ").append(columnNames).append(" FROM ")
                    .append(fullQualifiedName)
                    .append(generateTabletHint(highWeightTablets, manager.getHighWeight().getTabletReadRatio(),
                            manager.getSampleRowsLimit()))
                    .append(") t_high");
        }

        if (!mediumHighWeightTablets.isEmpty()) {
            if (sql.length() > 0) {
                sql.append(" UNION ALL ");
            }
            sql.append("SELECT * FROM (")
                    .append("SELECT ").append(columnNames).append(" FROM ")
                    .append(fullQualifiedName)
                    .append(generateTabletHint(mediumHighWeightTablets, manager.getMediumHighWeight().getTabletReadRatio(),
                            manager.getSampleRowsLimit()))
                    .append(") t_medium_high");
        }

        if (!mediumLowWeightTablets.isEmpty()) {
            if (sql.length() > 0) {
                sql.append(" UNION ALL ");
            }
            sql.append("SELECT * FROM (")
                    .append("SELECT ").append(columnNames).append(" FROM ")
                    .append(fullQualifiedName)
                    .append(generateTabletHint(mediumLowWeightTablets, manager.getMediumLowWeight().getTabletReadRatio(),
                            manager.getSampleRowsLimit()))
                    .append(") t_medium_low");
        }

        if (!lowWeightTablets.isEmpty()) {
            if (sql.length() > 0) {
                sql.append(" UNION ALL ");
            }
            sql.append("SELECT * FROM (")
                    .append("SELECT ").append(columnNames).append(" FROM ")
                    .append(fullQualifiedName)
                    .append(generateTabletHint(lowWeightTablets, manager.getLowWeight().getTabletReadRatio(),
                            manager.getSampleRowsLimit()))
                    .append(") t_low");
        }

        if (sql.length() == 0) {
            sql.append("SELECT").append(columnNames).append(" FROM ").append(fullQualifiedName).append(" LIMIT ").append(
                    Config.statistic_sample_collect_rows);
        }
        return sql.toString();
    }

    private String generateTabletHint(List<TabletStats> tabletStats, double readRatio, long sampleRowsLimit) {
        if (tabletStats.isEmpty()) {
            return "";
        }
        StringBuilder hint = new StringBuilder();
        hint.append(" TABLET");
        hint.append(tabletStats.stream()
                .map(e -> String.valueOf(e.getTabletId()))
                .collect(Collectors.joining(", ", "(", ")")));

        if (Config.enable_use_table_sample_collect_statistics) {
            int percent = Math.max(1, Math.min(100, (int) (readRatio * 100)));
            hint.append(String.format(" SAMPLE('percent'='%d') LIMIT %d ", percent, sampleRowsLimit));
        } else {
            hint.append(" WHERE rand() <= ").append(readRatio);
            hint.append(" LIMIT ").append(sampleRowsLimit);
        }
        return hint.toString();
    }

    private String generateQueryColumnSql(long tableId, long dbId, String tableName, String dbName,
                                          ColumnStats columnStats, String alias) {
        String sep = ", ";
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        builder.append(tableId).append(sep);
        builder.append(addSingleQuote(columnStats.getColumnName())).append(sep);
        builder.append(dbId).append(sep);
        builder.append("'").append(StringEscapeUtils.escapeSql(dbName)).append(".")
                .append(StringEscapeUtils.escapeSql(tableName)).append("'").append(sep);
        builder.append(addSingleQuote(dbName)).append(sep);
        builder.append(columnStats.getRowCount()).append(sep);
        builder.append(columnStats.getDateSize()).append(sep);
        builder.append(columnStats.getDistinctCount(rowSampleRatio)).append(sep);
        builder.append(columnStats.getNullCount()).append(sep);
        builder.append(columnStats.getMax()).append(sep);
        builder.append(columnStats.getMin()).append(sep);
        builder.append("NOW() FROM (");
        builder.append("SELECT t0.`column_key`, COUNT(1) as count FROM (SELECT ");
        builder.append(alias).append(" AS column_key FROM `base_cte_table`) as t0 GROUP BY t0.column_key) AS t1");
        return builder.toString();
    }

    private String addSingleQuote(String str) {
        return "'" + str + "'";
    }
}
