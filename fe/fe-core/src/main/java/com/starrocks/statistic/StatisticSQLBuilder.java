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
import com.starrocks.common.util.SqlUtils;
import com.starrocks.type.Type;
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
                    + " WHERE $predicate AND row_count > 0"
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

    // table_uuid isn't part of the projection and the predicate already scopes rows to a single
    // logical table via table_uuid in (hash, raw) (see buildTableUUIDInPredicate), so grouping by
    // column_name alone is enough to merge a table's data regardless of which representation any
    // given partition currently uses.
    //
    // The inner subquery additionally dedups to at most one row per (partition_name, column_name),
    // keeping only the one with the latest update_time. This is what actually prevents double-
    // counting when a partition has been re-collected (fresh row under the hashed key) but its
    // superseded raw-keyed row hasn't been cleaned up yet - correctness does not depend on that
    // best-effort cleanup (see ExternalFullStatisticsCollectJob#cleanupStaleRawKeyedRows) succeeding.
    private static final String QUERY_EXTERNAL_FULL_STATISTIC_V2_TEMPLATE =
            "SELECT cast(" + STATISTIC_EXTERNAL_QUERY_V2_VERSION + " as INT), column_name,"
                    + " sum(row_count), cast(sum(data_size) as bigint), hll_union_agg(ndv), sum(null_count), "
                    + " cast(max(cast(max as $type)) as string), cast(min(cast(min as $type)) as string),"
                    + " max(update_time)"
                    + " FROM (SELECT *, row_number() over ("
                    + " partition by partition_name, column_name order by update_time desc) as rn"
                    + " FROM " + StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME
                    + " WHERE $predicate) dedup_t"
                    + " WHERE rn = 1"
                    + " GROUP BY column_name";

    private static final String QUERY_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_HISTOGRAM_VERSION + " as INT), db_id, table_id, column_name,"
                    + " cast(json_object(\"buckets\", buckets, \"mcv\", mcv) as varchar)"
                    + " FROM " + StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME
                    + " WHERE $predicate";

    // Same dedup rationale as QUERY_EXTERNAL_FULL_STATISTIC_V2_TEMPLATE, keyed by column_name only
    // (external_histogram_statistics has no partition dimension).
    private static final String QUERY_EXTERNAL_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT), column_name,"
                    + " cast(json_object(\"buckets\", buckets, \"mcv\", mcv) as varchar)"
                    + " FROM (SELECT *, row_number() over ("
                    + " partition by column_name order by update_time desc) as rn"
                    + " FROM " + StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME
                    + " WHERE $predicate) dedup_t"
                    + " WHERE rn = 1";

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
        return buildQueryFullStatisticsSQL(tableId, columnNames, columnTypes, null);
    }

    public static String buildQueryFullStatisticsSQL(Long tableId, List<String> columnNames, List<Type> columnTypes,
                                                     List<Long> partitionIds) {
        Map<String, List<String>> nameGroups = groupByTypes(columnNames, columnTypes, false);

        List<String> querySQL = new ArrayList<>();
        nameGroups.forEach((type, names) -> {
            VelocityContext context = new VelocityContext();
            context.put("updateTime", "now()");
            context.put("type", type);
            
            // Build predicate with partition_id filter to exclude temp partitions
            StringBuilder predicateBuilder = new StringBuilder();
            predicateBuilder.append("table_id = ").append(tableId);
            predicateBuilder.append(" and column_name in (")
                    .append(names.stream().map(c -> "\"" + c + "\"").collect(Collectors.joining(", ")))
                    .append(")");
            if (partitionIds != null && !partitionIds.isEmpty()) {
                predicateBuilder.append(" and partition_id in (")
                        .append(partitionIds.stream().map(String::valueOf).collect(Collectors.joining(", ")))
                        .append(")");
            }
            context.put("predicate", predicateBuilder.toString());

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
        String tableUUIDPredicate = buildTableUUIDInPredicate(tableUUID);

        List<String> querySQL = new ArrayList<>();
        nameGroups.forEach((type, names) -> {
            VelocityContext context = new VelocityContext();
            context.put("type", type);
            context.put("predicate",
                    tableUUIDPredicate + " and column_name in (" +
                            names.stream().map(c -> "\"" + c + "\"").collect(Collectors.joining(", ")) + ")");
            querySQL.add(build(context, QUERY_EXTERNAL_FULL_STATISTIC_V2_TEMPLATE));
        });

        return Joiner.on(" UNION ALL ").join(querySQL);
    }

    // external_column_statistics / external_histogram_statistics store table_uuid hashed
    // (StatisticUtils.hashTableUuidForPkStorage) to keep the PRIMARY KEY within BE's
    // primary_key_limit_size. Matching on both the hashed and the raw value keeps historical
    // rows (written before this hashing was introduced) visible until they naturally age out.
    // tableUUID is derived from catalog/db/table names (Table.getUUID()), so it must be escaped
    // like any other untrusted value before being embedded into a string literal.
    private static String buildTableUUIDInPredicate(String tableUUID) {
        String hash = escapeForDoubleQuotedSqlString(StatisticUtils.hashTableUuidForPkStorage(tableUUID));
        String raw = escapeForDoubleQuotedSqlString(tableUUID);
        return "table_uuid in (\"" + hash + "\", \"" + raw + "\")";
    }

    // Same as buildTableUUIDInPredicate but single-quoted, for call sites that build their SQL with '...'.
    private static String buildTableUUIDInPredicateQuoted(String tableUUID) {
        String hash = SqlUtils.escapeSqlString(StatisticUtils.hashTableUuidForPkStorage(tableUUID));
        String raw = SqlUtils.escapeSqlString(tableUUID);
        return "table_uuid in ('" + hash + "', '" + raw + "')";
    }

    // SqlUtils.escapeSqlString only targets single-quoted literals (escapes backslash then single
    // quote); this is the double-quoted equivalent (escapes backslash then double quote), for the
    // handful of templates in this class that use "..." string literals.
    private static String escapeForDoubleQuotedSqlString(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
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

    public static String buildDropStatisticsSQL(List<Long> tableIds, StatsConstants.AnalyzeType analyzeType) {
        Preconditions.checkState(tableIds != null && !tableIds.isEmpty());
        String tableName;
        if (analyzeType.equals(StatsConstants.AnalyzeType.SAMPLE)) {
            tableName = SAMPLE_STATISTICS_TABLE_NAME;
        } else {
            tableName = FULL_STATISTICS_TABLE_NAME;
        }

        String tids = tableIds.stream().map(String::valueOf).collect(Collectors.joining(", "));
        return "DELETE FROM " + tableName + " WHERE TABLE_ID IN (" + tids + ")";
    }

    public static String buildDropMultipleStatisticsSQL(Long tableId) {
        return "DELETE FROM " + MULTI_COLUMN_STATISTICS_TABLE_NAME + " WHERE TABLE_ID = " + tableId;
    }

    public static String buildDropMultipleStatisticsSQL(List<Long> tableIds) {
        Preconditions.checkState(tableIds != null && !tableIds.isEmpty());
        String tids = tableIds.stream().map(String::valueOf).collect(Collectors.joining(", "));
        return "DELETE FROM " + MULTI_COLUMN_STATISTICS_TABLE_NAME + " WHERE TABLE_ID IN (" + tids + ")";
    }

    public static String buildDropExternalStatSQL(String tableUUID) {
        return "DELETE FROM " + EXTERNAL_FULL_STATISTICS_TABLE_NAME + " WHERE " + buildTableUUIDInPredicateQuoted(tableUUID);
    }

    // Cleans up the stale raw-keyed row(s) for exactly the (partition, column) pairs just
    // (re-)written under the hashed table_uuid, so a partition never has both a raw and a hashed
    // row alive at once. Purely storage hygiene - buildQueryExternalFullStatisticsSQL's dedup-by-
    // update_time already makes correctness independent of this cleanup succeeding. Only ever
    // targets the raw uuid - never the hash.
    public static String buildDropExternalStatSQLForPartitions(String rawTableUUID, List<String> partitionNames,
                                                                List<String> columnNames) {
        String partitionsIn = partitionNames.stream()
                .map(p -> "'" + SqlUtils.escapeSqlString(p) + "'").collect(Collectors.joining(", "));
        String columnsIn = columnNames.stream()
                .map(c -> "'" + SqlUtils.escapeSqlString(c) + "'").collect(Collectors.joining(", "));
        return "DELETE FROM " + EXTERNAL_FULL_STATISTICS_TABLE_NAME +
                " WHERE TABLE_UUID = '" + SqlUtils.escapeSqlString(rawTableUUID) + "'" +
                " AND PARTITION_NAME IN (" + partitionsIn + ")" +
                " AND COLUMN_NAME IN (" + columnsIn + ")";
    }

    // Same cleanup purpose as buildDropExternalStatSQLForPartitions, for external_histogram_statistics
    // (PK is table_uuid + column_name, no partition dimension).
    public static String buildDropExternalHistogramSQLForRawUuid(String rawTableUUID, List<String> columnNames) {
        String columnsIn = columnNames.stream()
                .map(c -> "'" + SqlUtils.escapeSqlString(c) + "'").collect(Collectors.joining(", "));
        return "delete from " + StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME +
                " where table_uuid = '" + SqlUtils.escapeSqlString(rawTableUUID) + "' and column_name in (" + columnsIn + ")";
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
            predicateList.add(buildTableUUIDInPredicateQuoted(tableUUID));
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

    public static String buildDropHistogramSQL(List<Long> tableIds) {
        Preconditions.checkState(tableIds != null && !tableIds.isEmpty());
        String tids = tableIds.stream().map(String::valueOf).collect(Collectors.joining(", "));
        return "delete from " + StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME + " where table_id in (" + tids + ")";
    }

    public static String buildDropExternalHistogramSQL(String tableUUID, List<String> columnNames) {
        return "delete from " + StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME + " where "
                + buildTableUUIDInPredicateQuoted(tableUUID) + " and column_name in (" + Joiner.on(", ")
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
