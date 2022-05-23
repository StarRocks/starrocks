// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.server.GlobalStateMgr;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.statistic.Constants.STATISTIC_DATA_VERSION;

public class StatisticSQLBuilder {
    private static final String QUERY_SAMPLE_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_DATA_VERSION + " as INT), update_time, db_id, table_id, column_name,"
                    + " row_count, data_size, distinct_count, null_count, max, min"
                    + " FROM " + Constants.StatisticsTableName
                    + " WHERE $predicate";

    private static final String QUERY_FULL_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_DATA_VERSION + " as INT), $updateTime, db_id, table_id, column_name,"
                    + " sum(row_count), cast(avg(data_size) as bigint), hll_union_agg(ndv), sum(null_count), "
                    + " max(max), min(min)"
                    + " FROM " + Constants.FullStatisticsTableName
                    + " WHERE $predicate"
                    + " GROUP BY db_id, table_id, column_name";

    private static final String COLLECT_FULL_STATISTIC_TEMPLATE =
            " SELECT $partitionId, '$columnName'," +
                    " $dbId, '$dbName', $tableId, '$tableName', '$partitionName'," +
                    " COUNT(1), $dataSize, $countDistinctFunction, $countNullFunction, $maxFunction, $minFunction, NOW() "
                    + "FROM $tableName";

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

    public static String buildQueryFullStatisticsSQL(Long dbId, Long tableId, List<String> columnNames) {
        VelocityContext context = new VelocityContext();
        context.put("updateTime", "now()");
        context.put("dbId", dbId);
        context.put("tableId", tableId);

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

        return build(context, QUERY_FULL_STATISTIC_TEMPLATE);
    }

    public static String buildCollectFullStatisticSQL(Long dbId, Long tableId, Long partitionId, List<String> columnNames) {
        StringBuilder builder = new StringBuilder("INSERT INTO column_statistics").append(" ");

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);

        for (String name : columnNames) {
            VelocityContext context = new VelocityContext();
            Column column = table.getColumn(name);

            context.put("dbId", dbId);
            context.put("tableId", tableId);
            context.put("partitionId", partitionId);
            context.put("columnName", name);
            context.put("dbName", db.getFullName());
            context.put("tableName", ClusterNamespace.getNameFromFullName(db.getFullName()) + "." + table.getName());
            context.put("partitionName", partition.getName());
            context.put("dataSize", getDataSize(column, false));

            if (!column.getType().canStatistic()) {
                context.put("countDistinctFunction", "0");
                context.put("countNullFunction", "0");
                context.put("maxFunction", "''");
                context.put("minFunction", "''");
            } else {
                context.put("countDistinctFunction", "hll_union(hll_hash(`" + name + "`))");
                context.put("countNullFunction", "COUNT(1) - COUNT(`" + name + "`)");
                context.put("maxFunction", "IFNULL(MAX(`" + name + "`), '')");
                context.put("minFunction", "IFNULL(MIN(`" + name + "`), '')");
            }

            builder.append(build(context, COLLECT_FULL_STATISTIC_TEMPLATE));
            builder.append(" UNION ALL ");
        }

        return builder.substring(0, builder.length() - "UNION ALL ".length());
    }

    private static String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }

    private static String getDataSize(Column column, boolean isSample) {
        if (column.getPrimitiveType().isCharFamily() || column.getPrimitiveType().isJsonType()) {
            if (isSample) {
                return "IFNULL(SUM(CHAR_LENGTH(`" + column.getName() + "`) * t1.count), 0)";
            }
            return "IFNULL(SUM(CHAR_LENGTH(`" + column.getName() + "`)), 0)";
        }

        long typeSize = column.getType().getTypeSize();

        if (isSample && column.getType().canStatistic()) {
            return "IFNULL(SUM(t1.count), 0) * " + typeSize;
        }
        return "COUNT(1) * " + typeSize;
    }
}
