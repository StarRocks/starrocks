// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.statistic;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.FULL_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.SAMPLE_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.STATISTIC_DATA_VERSION;
import static com.starrocks.statistic.StatsConstants.STATISTIC_HISTOGRAM_VERSION;

public class StatisticSQLBuilder {
    private static final String QUERY_SAMPLE_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_DATA_VERSION + " as INT), update_time, db_id, table_id, column_name,"
                    + " row_count, data_size, distinct_count, null_count, max, min"
                    + " FROM " + StatsConstants.SAMPLE_STATISTICS_TABLE_NAME
                    + " WHERE $predicate";

    private static final String QUERY_FULL_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_DATA_VERSION + " as INT), $updateTime, db_id, table_id, column_name,"
                    + " sum(row_count), cast(sum(data_size) as bigint), hll_union_agg(ndv), sum(null_count), "
                    + " cast(max(cast(max as $type)) as string), cast(min(cast(min as $type)) as string)"
                    + " FROM " + StatsConstants.FULL_STATISTICS_TABLE_NAME
                    + " WHERE $predicate"
                    + " GROUP BY db_id, table_id, column_name";

    private static final String QUERY_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_HISTOGRAM_VERSION + " as INT), db_id, table_id, column_name,"
                    + " cast(json_object(\"buckets\", buckets, \"mcv\", mcv) as varchar)"
                    + " FROM " + StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME
                    + " WHERE $predicate";

    private static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_LOGSYSTEM_CLASS,
                "org.apache.velocity.runtime.log.Log4JLogChute");
        DEFAULT_VELOCITY_ENGINE.setProperty("runtime.log.logsystem.log4j.logger", "velocity");
    }

    public static String buildQuerySampleStatisticsSQL(Long dbId, Long tableId, List<String> columnNames) {
        VelocityContext context = new VelocityContext();

        List<String> predicateList = Lists.newArrayList();
        if (dbId != null) {
            predicateList.add("db_id = " + dbId);
        }

        if (tableId != null) {
            predicateList.add("table_id = " + tableId);
        }

        if (!columnNames.isEmpty()) {
            predicateList.add("column_name in (" + Joiner.on(", ")
                    .join(columnNames.stream().map(c -> "'" + c + "'").collect(Collectors.toList())) + ")");
        }

        context.put("predicate", Joiner.on(" and ").join(predicateList));

        return build(context, QUERY_SAMPLE_STATISTIC_TEMPLATE);
    }

    public static String buildQueryFullStatisticsSQL(Long dbId, Long tableId, List<Column> columns) {
        List<String> querySQL = new ArrayList<>();
        for (Column column : columns) {
            VelocityContext context = new VelocityContext();
            context.put("updateTime", "now()");

            if (column.getType().canStatistic()) {
                context.put("type", column.getType().toSql());
            } else {
                context.put("type", "string");
            }
            context.put("predicate", "table_id = " + tableId + " and column_name = \"" + column.getName() + "\"");
            querySQL.add(build(context, QUERY_FULL_STATISTIC_TEMPLATE));
        }

        return Joiner.on(" UNION ALL ").join(querySQL);
    }

    public static String buildDropStatisticsSQL(Long tableId, StatsConstants.AnalyzeType analyzeType) {
        String tableName;
        if (analyzeType.equals(StatsConstants.AnalyzeType.SAMPLE)) {
            tableName = SAMPLE_STATISTICS_TABLE_NAME;
        } else {
            tableName = FULL_STATISTICS_TABLE_NAME;
        }

        return "DELETE FROM " + tableName + " WHERE TABLE_ID = " + tableId;
    }

    public static String buildDropPartitionSQL(List<Long> pids) {
        return "DELETE FROM " + FULL_STATISTICS_TABLE_NAME + " WHERE " +
                " PARTITION_ID IN (" +
                pids.stream().map(String::valueOf).collect(Collectors.joining(", ")) +
                ");";
    }

    public static String buildDropTableInvalidPartitionSQL(List<Long> tables, List<Long> partitions) {
        Preconditions.checkState(!tables.isEmpty() && !partitions.isEmpty());

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM " + FULL_STATISTICS_TABLE_NAME + " WHERE");
        String tids = tables.stream().map(String::valueOf).collect(Collectors.joining(", "));
        String pids = partitions.stream().map(String::valueOf).collect(Collectors.joining(", "));
        sql.append(" TABLE_ID IN (").append(tids).append(")");
        sql.append(" AND PARTITION_ID NOT IN (").append(pids).append(")");
        return sql.toString();
    }

    public static String buildQueryHistogramStatisticsSQL(Long tableId, List<String> columnNames) {
        VelocityContext context = new VelocityContext();

        List<String> predicateList = Lists.newArrayList();
        if (tableId != null) {
            predicateList.add("table_id = " + tableId);
        }

        if (!columnNames.isEmpty()) {
            predicateList.add("column_name in (" + Joiner.on(", ")
                    .join(columnNames.stream().map(c -> "'" + c + "'").collect(Collectors.toList())) + ")");
        }

        context.put("predicate", Joiner.on(" and ").join(predicateList));
        return build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }

    public static String buildDropHistogramSQL(Long tableId, List<String> columnNames) {
        return "delete from " + StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME + " where table_id = "
                + tableId + " and column_name in (" + Joiner.on(", ")
                .join(columnNames.stream().map(c -> "'" + c + "'").collect(Collectors.toList())) + ")";
    }

    private static String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }
}
