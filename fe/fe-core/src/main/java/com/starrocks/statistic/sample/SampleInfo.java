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
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.SAMPLE_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME;

public class SampleInfo {

    protected static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    private static final String CTE_TEMPLATE = "SELECT $columnName FROM $fullyQualifiedName $tabletsHint " +
            "$laterals $whereAndLimit";

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
        int max = getHighWeightTablets().size();
        max = Math.max(max, getMediumHighWeightTablets().size());
        max = Math.max(max, getMediumLowWeightTablets().size());
        max = Math.max(max, getLowWeightTablets().size());
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
            builder.append(columnStats.getDataSize()).append(sep);
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

        // CTEs
        builder.append("WITH ");
        for (int i = 0; i < primitiveTypeStats.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("cte_").append(i).append(" as (");
            String queryDataSql = generateQueryDataSql(tableName, dbName, primitiveTypeStats.get(i), manager);
            builder.append(queryDataSql).append(")");
        }
        builder.append(" ");

        // Generate SELECT statements for each column referencing its own CTE
        for (int i = 0; i < primitiveTypeStats.size(); i++) {
            if (i > 0) {
                builder.append(" UNION ALL ");
            }
            builder.append(generateQueryColumnSql(tableId, dbId, tableName, dbName, primitiveTypeStats.get(i), "cte_" + i));
        }
        return builder.toString();
    }

    private static String getSampleCte(String columnName, String fullyQualifiedName, String tabletsHint, String laterals,
                                       String whereAndLimit) {
        final var velocityContext = new VelocityContext();
        velocityContext.put("columnName", columnName);
        velocityContext.put("fullyQualifiedName", fullyQualifiedName);
        velocityContext.put("tabletsHint", tabletsHint);
        velocityContext.put("laterals", laterals);
        velocityContext.put("whereAndLimit", whereAndLimit);

        final var cteWriter = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(velocityContext, cteWriter, "", CTE_TEMPLATE);

        return cteWriter.toString();
    }

    private String generateQueryDataSql(String tableName, String dbName,
                                        ColumnStats columnStat,
                                        TabletSampleManager manager) {
        StringBuilder sql = new StringBuilder();
        String fullQualifiedName = "`" + dbName + "`.`" + tableName + "`";
        String columnName = columnStat.getQuotedColumnName() + " as col";
        String lateral = columnStat.getLateralJoin();

        if (!highWeightTablets.isEmpty()) {
            final var tabletsHint = generateTabletHint(highWeightTablets, manager.getHighWeight().getTabletReadRatio());
            final var whereAndLimit = getWhereAndLimit(manager.getHighWeight().getTabletReadRatio(),
                    manager.getSampleRowsLimit());
            sql.append("SELECT * FROM (") //
                    .append(getSampleCte(columnName, fullQualifiedName, tabletsHint, lateral, whereAndLimit)) //
                    .append(") t_high");
        }

        if (!mediumHighWeightTablets.isEmpty()) {
            if (sql.length() > 0) {
                sql.append(" UNION ALL ");
            }

            final var tabletsHint =
                    generateTabletHint(mediumHighWeightTablets, manager.getMediumHighWeight().getTabletReadRatio());
            final var whereAndLimit = getWhereAndLimit(manager.getMediumHighWeight().getTabletReadRatio(),
                    manager.getSampleRowsLimit());
            sql.append("SELECT * FROM (") //
                    .append(getSampleCte(columnName, fullQualifiedName, tabletsHint, lateral, whereAndLimit)) //
                    .append(") t_medium_high");
        }

        if (!mediumLowWeightTablets.isEmpty()) {
            if (sql.length() > 0) {
                sql.append(" UNION ALL ");
            }

            final var tabletsHint = generateTabletHint(mediumLowWeightTablets, manager.getMediumLowWeight().getTabletReadRatio());
            final var whereAndLimit = getWhereAndLimit(manager.getMediumLowWeight().getTabletReadRatio(),
                    manager.getSampleRowsLimit());
            sql.append("SELECT * FROM (") //
                    .append(getSampleCte(columnName, fullQualifiedName, tabletsHint, lateral, whereAndLimit)) //
                    .append(") t_medium_low");
        }

        if (!lowWeightTablets.isEmpty()) {
            if (sql.length() > 0) {
                sql.append(" UNION ALL ");
            }
            final var tabletsHint = generateTabletHint(lowWeightTablets, manager.getLowWeight().getTabletReadRatio());
            final var whereAndLimit = getWhereAndLimit(manager.getLowWeight().getTabletReadRatio(),
                    manager.getSampleRowsLimit());
            sql.append("SELECT * FROM (") //
                    .append(getSampleCte(columnName, fullQualifiedName, tabletsHint, lateral, whereAndLimit)) //
                    .append(") t_low");
        }

        if (sql.isEmpty()) {
            sql.append("SELECT * FROM ( ") //
                    .append(getSampleCte(columnName, fullQualifiedName, "", lateral, "")) //
                    .append(") t_no_hint LIMIT ") //
                    .append(Config.statistic_sample_collect_rows);
        }
        return sql.toString();
    }

    private String generateTabletHint(List<TabletStats> tabletStats, double readRatio) {
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
            hint.append(String.format(" SAMPLE('percent'='%d') ", percent));
        }
        return hint.toString();
    }

    private String getWhereAndLimit(double readRatio, long sampleRowsLimit) {
        final var limit = " LIMIT " + sampleRowsLimit;
        if (Config.enable_use_table_sample_collect_statistics) {
            return limit; // Sampling is already covered when generating the tablet hint.
        }

        return " WHERE rand() <= " + readRatio + " LIMIT " + sampleRowsLimit;
    }

    private String generateQueryColumnSql(long tableId, long dbId, String tableName, String dbName,
                                          ColumnStats columnStats, String cteName) {
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
        builder.append(columnStats.getDataSize()).append(sep);
        builder.append(columnStats.getDistinctCount(rowSampleRatio)).append(sep);
        builder.append(columnStats.getNullCount()).append(sep);
        builder.append(columnStats.getMax()).append(sep);
        builder.append(columnStats.getMin()).append(sep);
        builder.append("NOW() FROM (");
        builder.append("SELECT t0.`column_key`, COUNT(1) as count FROM (SELECT ");
        builder.append("col").append(" AS column_key FROM `").append(cteName).append("`) as t0 GROUP BY t0.column_key) AS t1");
        return builder.toString();
    }

    private String addSingleQuote(String str) {
        return "'" + str + "'";
    }
}
