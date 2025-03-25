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

package com.starrocks.statistic;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Type;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.FULL_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.MULTI_COLUMN_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.SAMPLE_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.STATISTIC_DATA_VERSION;
import static com.starrocks.statistic.StatsConstants.STATISTIC_DATA_VERSION_V2;
import static com.starrocks.statistic.StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION;
import static com.starrocks.statistic.StatsConstants.STATISTIC_EXTERNAL_QUERY_V2_VERSION;
import static com.starrocks.statistic.StatsConstants.STATISTIC_HISTOGRAM_VERSION;
import static com.starrocks.statistic.StatsConstants.STATISTIC_PARTITION_VERSION_V2;
import static com.starrocks.statistic.StatsConstants.STATISTIC_QUERY_MULTI_COLUMN_VERSION;
import static com.starrocks.statistic.StatsConstants.STATISTIC_TABLE_VERSION;

public class StatisticSQLBuilder {
    private static final String QUERY_TABLE_STATISTIC_TEMPLATE =
            "select cast(" + STATISTIC_TABLE_VERSION + " as INT), partition_id, any_value(row_count)"
                    + " FROM " + FULL_STATISTICS_TABLE_NAME
                    + " WHERE $predicate"
                    + " GROUP BY partition_id";

    private static final String QUERY_PARTITION_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_PARTITION_VERSION_V2 + " as INT), " +
                    " `partition_id`, `column_name`, hll_cardinality(hll_union(`ndv`)) as distinct_count,"
                    + " any_value(null_count), any_value(row_count)"
                    + " FROM " + FULL_STATISTICS_TABLE_NAME
                    + " WHERE $predicate"
                    + " GROUP BY `partition_id`, `column_name`";

    private static final String QUERY_SAMPLE_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_DATA_VERSION + " as INT), update_time, db_id, table_id, column_name,"
                    + " row_count, data_size, distinct_count, null_count, max, min"
                    + " FROM " + StatsConstants.SAMPLE_STATISTICS_TABLE_NAME
                    + " WHERE $predicate";

    private static final String QUERY_FULL_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_DATA_VERSION_V2 + " as INT), $updateTime, db_id, table_id, column_name,"
                    + " sum(row_count), cast(sum(data_size) as bigint), hll_union_agg(ndv), sum(null_count), "
                    + " cast(max(cast(max as $type)) as string), cast(min(cast(min as $type)) as string),"
                    + " cast(avg(collection_size) as bigint)"
                    + " FROM " + StatsConstants.FULL_STATISTICS_TABLE_NAME
                    + " WHERE $predicate"
                    + " GROUP BY db_id, table_id, column_name";

    private static final String QUERY_COLLECTION_FULL_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_DATA_VERSION_V2 + " as INT), $updateTime, db_id, table_id, column_name,"
                    + " sum(row_count), cast(sum(data_size) as bigint), hll_union_agg(ndv), sum(null_count), "
                    + " any_value(''), any_value(''), cast(avg(collection_size) as bigint) "
                    + " FROM " + StatsConstants.FULL_STATISTICS_TABLE_NAME
                    + " WHERE $predicate"
                    + " GROUP BY db_id, table_id, column_name";

    private static final String QUERY_EXTERNAL_FULL_STATISTIC_V2_TEMPLATE =
            "SELECT cast(" + STATISTIC_EXTERNAL_QUERY_V2_VERSION + " as INT), column_name,"
                    + " sum(row_count), cast(sum(data_size) as bigint), hll_union_agg(ndv), sum(null_count), "
                    + " cast(max(cast(max as $type)) as string), cast(min(cast(min as $type)) as string),"
                    + " max(update_time)"
                    + " FROM " + StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME
                    + " WHERE $predicate"
                    + " GROUP BY table_uuid, column_name";

    private static final String QUERY_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_HISTOGRAM_VERSION + " as INT), db_id, table_id, column_name,"
                    + " cast(json_object(\"buckets\", buckets, \"mcv\", mcv) as varchar)"
                    + " FROM " + StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME
                    + " WHERE $predicate";

    private static final String QUERY_EXTERNAL_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT), column_name,"
                    + " cast(json_object(\"buckets\", buckets, \"mcv\", mcv) as varchar)"
                    + " FROM " + StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME
                    + " WHERE $predicate";

    private static final String QUERY_MULTI_COLUMNS_COMBINED_STATISTICS_TEMPLATE =
            "SELECT cast(" + STATISTIC_QUERY_MULTI_COLUMN_VERSION + " as INT), db_id, table_id, column_ids, ndv"
                    + " FROM " + MULTI_COLUMN_STATISTICS_TABLE_NAME
                    + " WHERE $predicate";

    private static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    public static String buildQueryTableStatisticsSQL(Long tableId, List<Long> partitionIds) {
        VelocityContext context = new VelocityContext();
        context.put("predicate", "table_id = " + tableId);
        if (!partitionIds.isEmpty()) {
            context.put("predicate", "table_id = " + tableId + " and partition_id in (" +
                    partitionIds.stream().map(String::valueOf).collect(Collectors.joining(", ")) + ")");
        }
        return build(context, QUERY_TABLE_STATISTIC_TEMPLATE);
    }

    public static String buildQueryPartitionStatisticsSQL(Long tableId, List<Long> partitionIds, List<String> columns) {
        VelocityContext context = new VelocityContext();
        String tablePredicate = "table_id=" + tableId;
        String partitionPredicate = CollectionUtils.isEmpty(partitionIds) ? "" :
                " AND `partition_id` in (" +
                        partitionIds.stream().map(String::valueOf).collect(Collectors.joining(", ")) + ")";
        String columnPredicate = CollectionUtils.isEmpty(columns) ? "" :
                " AND `column_name` in (" +
                        columns.stream().map(Strings::quote).collect(Collectors.joining(",")) + ")";
        context.put("predicate", tablePredicate + partitionPredicate + columnPredicate);

        return build(context, QUERY_PARTITION_STATISTIC_TEMPLATE);
    }

    public static String buildQueryTableStatisticsSQL(Long tableId, Long partitionId) {
        VelocityContext context = new VelocityContext();
        context.put("predicate", "table_id = " + tableId + " and partition_id = " + partitionId);
        return build(context, QUERY_TABLE_STATISTIC_TEMPLATE);
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

    public static String buildQueryFullStatisticsSQL(Long tableId, List<String> columnNames, List<Type> columnTypes) {
        Map<String, List<String>> nameGroups = groupByTypes(columnNames, columnTypes, false);

        List<String> querySQL = new ArrayList<>();
        nameGroups.forEach((type, names) -> {
            VelocityContext context = new VelocityContext();
            context.put("updateTime", "now()");
            context.put("type", type);
            context.put("predicate", "table_id = " + tableId + " and column_name in (" +
                    names.stream().map(c -> "\"" + c + "\"").collect(Collectors.joining(", ")) + ")");

            if (type.startsWith("array") || type.startsWith("map")) {
                querySQL.add(build(context, QUERY_COLLECTION_FULL_STATISTIC_TEMPLATE));
            } else {
                querySQL.add(build(context, QUERY_FULL_STATISTIC_TEMPLATE));
            }
        });
        return Joiner.on(" UNION ALL ").join(querySQL);
    }

    public static String buildQueryExternalFullStatisticsSQL(String tableUUID, List<String> columnNames,
                                                             List<Type> columnTypes) {
        Map<String, List<String>> nameGroups = groupByTypes(columnNames, columnTypes, true);

        List<String> querySQL = new ArrayList<>();
        nameGroups.forEach((type, names) -> {
            VelocityContext context = new VelocityContext();
            context.put("type", type);
            context.put("predicate",
                    "table_uuid = \"" + tableUUID + "\"" + " and column_name in (" +
                            names.stream().map(c -> "\"" + c + "\"").collect(Collectors.joining(", ")) + ")");
            querySQL.add(build(context, QUERY_EXTERNAL_FULL_STATISTIC_V2_TEMPLATE));
        });

        return Joiner.on(" UNION ALL ").join(querySQL);
    }

    public static String buildMultiColumnCombinedStatisticsSQL(List<Long> tableIds) {
        VelocityContext context = new VelocityContext();
        context.put("predicate", "table_id in (" +
                tableIds.stream().map(String::valueOf).collect(Collectors.joining(", ")) + ")");
        return build(context, QUERY_MULTI_COLUMNS_COMBINED_STATISTICS_TEMPLATE);
    }

    private static Map<String, List<String>> groupByTypes(List<String> columnNames, List<Type> columnTypes,
                                                          boolean isExternal) {
        Map<String, List<String>> groupByTypeNames = Maps.newHashMap();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);

            if (columnType.isStringType() || !columnType.canStatistic() || (isExternal && columnType.isComplexType())) {
                groupByTypeNames.computeIfAbsent("string", k -> Lists.newArrayList()).add(columnName);
            } else if (columnType.isIntegerType()) {
                groupByTypeNames.computeIfAbsent("bigint", k -> Lists.newArrayList()).add(columnName);
            } else {
                groupByTypeNames.computeIfAbsent(columnType.toSql(), k -> Lists.newArrayList()).add(columnName);
            }
        }
        return groupByTypeNames;
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

    public static String buildDropMultipleStatisticsSQL(Long tableId) {
        return "DELETE FROM " + MULTI_COLUMN_STATISTICS_TABLE_NAME + " WHERE TABLE_ID = " + tableId;
    }

    public static String buildDropExternalStatSQL(String tableUUID) {
        return "DELETE FROM " + EXTERNAL_FULL_STATISTICS_TABLE_NAME + " WHERE TABLE_UUID = '" + tableUUID + "'";
    }

    public static String buildDropExternalStatSQL(String catalogName, String dbName, String tableName) {
        return "DELETE FROM " + EXTERNAL_FULL_STATISTICS_TABLE_NAME + " WHERE CATALOG_NAME = '" + catalogName + "'" +
                " AND DB_NAME = '" + dbName + "' AND TABLE_NAME = '" + tableName + "'";
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

    public static String buildQueryConnectorHistogramStatisticsSQL(String tableUUID, List<String> columnNames) {
        VelocityContext context = new VelocityContext();

        List<String> predicateList = Lists.newArrayList();
        if (tableUUID != null) {
            predicateList.add("table_uuid = '" + tableUUID + "'");
        }

        if (!columnNames.isEmpty()) {
            predicateList.add("column_name in (" + Joiner.on(", ")
                    .join(columnNames.stream().map(c -> "'" + c + "'").collect(Collectors.toList())) + ")");
        }

        context.put("predicate", Joiner.on(" and ").join(predicateList));
        return build(context, QUERY_EXTERNAL_HISTOGRAM_STATISTIC_TEMPLATE);
    }

    public static String buildDropHistogramSQL(Long tableId, List<String> columnNames) {
        return "delete from " + StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME + " where table_id = "
                + tableId + " and column_name in (" + Joiner.on(", ")
                .join(columnNames.stream().map(c -> "'" + c + "'").collect(Collectors.toList())) + ")";
    }

    public static String buildDropExternalHistogramSQL(String tableUUID, List<String> columnNames) {
        return "delete from " + StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME + " where table_uuid = '"
                + tableUUID + "' and column_name in (" + Joiner.on(", ")
                .join(columnNames.stream().map(c -> "'" + c + "'").collect(Collectors.toList())) + ")";
    }

    public static String buildDropExternalHistogramSQL(String catalogName, String dbName, String tableName,
                                                       List<String> columnNames) {
        return "delete from " + StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME + " where catalog_name = '"
                + catalogName + "' and db_name = '" + dbName + "' and table_name = '" + tableName + "' and column_name in ("
                + Joiner.on(", ").join(columnNames.stream().map(c -> "'" + c + "'").collect(Collectors.toList())) + ")";
    }

    private static String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }
}
