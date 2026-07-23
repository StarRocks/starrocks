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

package com.starrocks.sql.optimizer.dump;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Version;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.ColumnStatisticDump;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.HistogramUtils;
import com.starrocks.system.BackendResourceStat;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QueryDumpSerializer implements JsonSerializer<QueryDumpInfo> {
    private static final Logger LOG = LogManager.getLogger(QueryDumpSerializer.class);

    @Override
    public JsonElement serialize(QueryDumpInfo dumpInfo, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject dumpJson = serializeSensitiveContent(dumpInfo);

        // session variables
        try {
            dumpJson.addProperty("session_variables", dumpInfo.getSessionVariable().getJsonString());
        } catch (IOException e) {
            LOG.warn("serialize session variables failed. ", e);
        }

        // BE number
        ConnectContext ctx = ConnectContext.get();
        long beNum = ctx.getAliveBackendNumber();
        dumpJson.addProperty("be_number", beNum);
        // backend core stat
        BackendResourceStat.getInstance().dump(dumpJson, ctx);
        // exception
        JsonArray exceptions = new JsonArray();
        for (String ex : dumpInfo.getExceptionList()) {
            exceptions.add(ex);
        }
        dumpJson.add("exception", exceptions);
        // version
        if (!FeConstants.runningUnitTest) {
            dumpJson.addProperty("version", Version.STARROCKS_VERSION);
            dumpJson.addProperty("commit_version", Version.STARROCKS_COMMIT_HASH);
        }
        return dumpJson;
    }

    private boolean shouldDesensitizeDump(QueryDumpInfo dumpInfo) {
        return Config.enable_desensitize_query_dump || dumpInfo.isDesensitizedInfo();
    }

    private JsonObject serializeSensitiveContent(QueryDumpInfo dumpInfo) {
        JsonObject dumpJson = new JsonObject();
        if (shouldDesensitizeDump(dumpInfo)) {
            try {
                desensitizeContent(dumpInfo, dumpJson);
                return dumpJson;
            } catch (Exception e) {
                LOG.info("failed to desensitize content, use the original content", e);
                dumpInfo.addException(e.getMessage());
                dumpJson = new JsonObject();
            }
        }
        // statement
        dumpJson.addProperty("statement", dumpInfo.getOriginStmt());
        // resource
        if (!dumpInfo.getResourceSet().isEmpty()) {
            JsonObject resourceMetaData = new JsonObject();
            for (Resource resource : dumpInfo.getResourceSet()) {
                resourceMetaData.addProperty(resource.getName(), resource.toString());
            }
            dumpJson.add("resources", resourceMetaData);
        }
        // table meta
        JsonObject tableMetaData = new JsonObject();
        List<Pair<String, Table>> tableMetaPairs = Lists.newArrayList(dumpInfo.getTableMap().values());
        for (Pair<String, com.starrocks.catalog.Table> entry : tableMetaPairs) {
            String tableName = entry.first + "." + entry.second.getName();
            List<String> createTableStmt = Lists.newArrayList();
            AstToStringBuilder.getDdlStmt(entry.second, createTableStmt, null, null, false, true /* hide password */);
            tableMetaData.addProperty(tableName, createTableStmt.get(0));
        }
        dumpJson.add("table_meta", tableMetaData);
        // hive meta store table info
        if (!dumpInfo.getHmsTableMap().isEmpty()) {
            JsonObject externalTableInfoData = new JsonObject();
            for (Map.Entry<String, Map<String, Map<String, HiveMetaStoreTableDumpInfo>>> resourceEntry :
                    dumpInfo.getHmsTableMap().entrySet()) {
                String resourceName = resourceEntry.getKey();
                for (Map.Entry<String, Map<String, HiveMetaStoreTableDumpInfo>> dbEntry :
                        resourceEntry.getValue().entrySet()) {
                    String dbName = dbEntry.getKey();
                    for (Map.Entry<String, HiveMetaStoreTableDumpInfo> tableEntry : dbEntry.getValue().entrySet()) {
                        String tableName = tableEntry.getKey();
                        String fullName = String.join("%", resourceName, dbName, tableName);
                        JsonObject tableTypeObject = new JsonObject();
                        tableTypeObject.addProperty("type", tableEntry.getValue().getType());
                        JsonArray jsonArray = new JsonArray();
                        jsonArray.add(tableTypeObject);
                        jsonArray.add(GsonUtils.GSON.toJson(tableEntry.getValue()));
                        externalTableInfoData.add(fullName, jsonArray);
                    }
                }
            }
            dumpJson.add("hms_table", externalTableInfoData);
        }

        // table row count
        JsonObject tableRowCount = new JsonObject();
        for (Map.Entry<String, Map<String, Long>> entry : dumpInfo.getPartitionRowCountMap().entrySet()) {
            JsonObject partitionRowCount = new JsonObject();
            for (Map.Entry<String, Long> partitionEntry : entry.getValue().entrySet()) {
                partitionRowCount.addProperty(partitionEntry.getKey(), partitionEntry.getValue());
            }
            tableRowCount.add(entry.getKey(), partitionRowCount);
        }
        dumpJson.add("table_row_count", tableRowCount);
        // automatic/expression partition values: CREATE TABLE (table_meta) omits partition definitions for these
        // tables, so capture one representative value per concrete partition to let replay recreate the
        // partitions and match the per-partition row counts above. Keyed like table_meta (db.table).
        addPartitionValuesSection(dumpJson, tableMetaPairs, entry -> entry.first + "." + entry.second.getName());
        // view meta
        if (!dumpInfo.getViewMap().isEmpty()) {
            JsonObject viewMetaData = new JsonObject();
            for (Pair<String, View> entry : dumpInfo.getViewMap().values()) {
                String viewName = entry.first + "." + entry.second.getName();
                viewMetaData.addProperty(viewName, entry.second.getInlineViewDef());
            }
            dumpJson.add("view_meta", viewMetaData);
        }

        // column statistics
        JsonObject tableColumnStatistics = new JsonObject();
        for (Map.Entry<String, Map<String, ColumnStatistic>> entry : dumpInfo.getTableStatisticsMap().entrySet()) {
            JsonObject columnStatistics = new JsonObject();
            for (Map.Entry<String, ColumnStatistic> columnEntry : entry.getValue().entrySet()) {
                ColumnStatistic toBeSerializedStats = stripHistogram(columnEntry.getValue());
                columnStatistics.add(columnEntry.getKey(),
                        GsonUtils.GSON.toJsonTree(ColumnStatisticDump.from(toBeSerializedStats)));
            }
            tableColumnStatistics.add(entry.getKey(), columnStatistics);
        }
        dumpJson.add("column_statistics", tableColumnStatistics);
        // column_statistics stores base stats through ColumnStatisticDump so QueryDumpDeserializer can replay them.
        // The embedded histogram is stripped above because the complete histogram round-trips separately here,
        // keyed the same way as column_statistics.
        // Only emitted when a column actually carries a histogram, so older/histogram-free dumps are unaffected.
        // Intentionally not emitted on the desensitized path: raw bucket bounds and MCV values would leak data.
        JsonObject tableColumnHistogram = new JsonObject();
        for (Map.Entry<String, Map<String, ColumnStatistic>> entry : dumpInfo.getTableStatisticsMap().entrySet()) {
            JsonObject columnHistograms = new JsonObject();
            for (Map.Entry<String, ColumnStatistic> columnEntry : entry.getValue().entrySet()) {
                Histogram histogram = columnEntry.getValue().getHistogram();
                if (histogram != null) {
                    columnHistograms.addProperty(columnEntry.getKey(), HistogramUtils.serializeHistogram(histogram));
                }
            }
            if (columnHistograms.size() > 0) {
                tableColumnHistogram.add(entry.getKey(), columnHistograms);
            }
        }
        if (tableColumnHistogram.size() > 0) {
            dumpJson.add("column_histogram", tableColumnHistogram);
        }
        if (StringUtils.isNotEmpty(dumpInfo.getExplainInfo())) {
            dumpJson.addProperty("explain_info", dumpInfo.getExplainInfo());
        }
        return dumpJson;
    }

    // Emits the "partition_values" section (shared by the normal and desensitized paths): for every table that
    // needs it, one representative value per concrete partition, under a table key produced by tableKeyFn.
    private static void addPartitionValuesSection(JsonObject dumpJson, List<Pair<String, Table>> tableMetaPairs,
                                                  Function<Pair<String, Table>, String> tableKeyFn) {
        JsonObject tablePartitionValues = new JsonObject();
        for (Pair<String, Table> entry : tableMetaPairs) {
            List<List<String>> partitionValues = collectAutomaticPartitionValues(entry.second);
            if (!partitionValues.isEmpty()) {
                tablePartitionValues.add(tableKeyFn.apply(entry), partitionValuesToJson(partitionValues));
            }
        }
        if (tablePartitionValues.size() > 0) {
            dumpJson.add("partition_values", tablePartitionValues);
        }
    }

    // Returns one representative value tuple per concrete partition, but only for tables whose CREATE TABLE
    // omits partition definitions: automatic (expression) range partitioning and automatic list partitioning.
    // These are exactly the types AnalyzerUtils.getAddPartitionClauseFromPartitionValues can recreate on
    // replay. Returns empty for tables whose partitions the CREATE TABLE already reproduces (explicit
    // range/list, single/unpartitioned) or for non-OLAP tables.
    private static List<List<String>> collectAutomaticPartitionValues(Table table) {
        if (!(table instanceof OlapTable)) {
            return Lists.newArrayList();
        }
        OlapTable olapTable = (OlapTable) table;
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        List<List<String>> partitionValues = Lists.newArrayList();
        // Automatic range partitioning is modeled as ExpressionRangePartitionInfo (EXPR_RANGE). Its V2 subclass
        // is used for non-automatic "partition by range expr" and is not a subclass of this type, so it is
        // correctly excluded here (its partitions are already listed in the CREATE TABLE).
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo rangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            for (Map.Entry<Long, Range<PartitionKey>> entry : rangePartitionInfo.getIdToRange(false).entrySet()) {
                if (isShadowPartition(olapTable, entry.getKey())) {
                    continue;
                }
                List<LiteralExpr> keys = entry.getValue().lowerEndpoint().getKeys();
                // automatic range partitioning is single-column; the lower bound recreates the same partition.
                if (keys.size() == 1) {
                    partitionValues.add(Lists.newArrayList(keys.get(0).getStringValue()));
                }
            }
        } else if (partitionInfo instanceof ListPartitionInfo
                && ((ListPartitionInfo) partitionInfo).isAutomaticPartition()) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            for (Map.Entry<Long, List<String>> entry : listPartitionInfo.getIdToValues().entrySet()) {
                if (isShadowPartition(olapTable, entry.getKey())) {
                    continue;
                }
                for (String value : entry.getValue()) {
                    partitionValues.add(Lists.newArrayList(value));
                }
            }
            for (Map.Entry<Long, List<List<String>>> entry : listPartitionInfo.getIdToMultiValues().entrySet()) {
                if (isShadowPartition(olapTable, entry.getKey())) {
                    continue;
                }
                partitionValues.addAll(entry.getValue());
            }
        }
        return partitionValues;
    }

    // Automatic-partition tables carry a hidden shadow partition (name prefixed with "$") that is not a real
    // data partition and whose bound value does not round-trip through the partition-creation path.
    private static boolean isShadowPartition(OlapTable olapTable, long partitionId) {
        Partition partition = olapTable.getPartition(partitionId);
        return partition == null
                || partition.getName().startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX);
    }

    private static JsonArray partitionValuesToJson(List<List<String>> partitionValues) {
        JsonArray valuesArray = new JsonArray();
        for (List<String> tuple : partitionValues) {
            JsonArray tupleArray = new JsonArray();
            tuple.forEach(tupleArray::add);
            valuesArray.add(tupleArray);
        }
        return valuesArray;
    }

    private void desensitizeContent(QueryDumpInfo dumpInfo, JsonObject dumpJson) {
        DesensitizedInfoCollector collector = new DesensitizedInfoCollector(dumpInfo);
        collector.init();
        Map<String, String> dict = collector.getDesensitizedDict();
        String sql = DesensitizedSQLBuilder.desensitizeSQL(dumpInfo.getStatement(), dict);
        // statement
        dumpJson.addProperty("statement", sql);
        // resource
        if (!dumpInfo.getResourceSet().isEmpty()) {
            JsonObject resourceMetaData = new JsonObject();
            for (Resource resource : dumpInfo.getResourceSet()) {
                resourceMetaData.addProperty(resource.getName(), resource.toString());
            }
            dumpJson.add("resources", resourceMetaData);
        }
        // table meta
        JsonObject tableMetaData = new JsonObject();
        List<Pair<String, Table>> tableMetaPairs = Lists.newArrayList(dumpInfo.getTableMap().values());
        for (Pair<String, com.starrocks.catalog.Table> entry : tableMetaPairs) {
            String tableName = DesensitizedSQLBuilder.desensitizeDbName(entry.first, dict) + "."
                    + DesensitizedSQLBuilder.desensitizeTblName(entry.second.getName(), dict);
            String createTableStmt = DesensitizedSQLBuilder.desensitizeTableDef(entry, dict);
            tableMetaData.addProperty(tableName, createTableStmt);
        }
        dumpJson.add("table_meta", tableMetaData);

        // hive meta store table info
        if (!dumpInfo.getHmsTableMap().isEmpty()) {
            JsonObject externalTableInfoData = new JsonObject();
            for (Map.Entry<String, Map<String, Map<String, HiveMetaStoreTableDumpInfo>>> resourceEntry :
                    dumpInfo.getHmsTableMap().entrySet()) {
                String resourceName = resourceEntry.getKey();
                for (Map.Entry<String, Map<String, HiveMetaStoreTableDumpInfo>> dbEntry :
                        resourceEntry.getValue().entrySet()) {
                    String dbName = DesensitizedSQLBuilder.desensitizeDbName(dbEntry.getKey(), dict);
                    for (Map.Entry<String, HiveMetaStoreTableDumpInfo> tableEntry : dbEntry.getValue().entrySet()) {
                        String tableName = DesensitizedSQLBuilder.desensitizeTblName(tableEntry.getKey(), dict);
                        String fullName = String.join("%", resourceName, dbName, tableName);
                        JsonObject tableTypeObject = new JsonObject();
                        tableTypeObject.addProperty("type", tableEntry.getValue().getType());
                        JsonArray jsonArray = new JsonArray();
                        jsonArray.add(tableTypeObject);
                        HiveMetaStoreTableDumpInfo hiveMeta = tableEntry.getValue();
                        HiveMetaStoreTableDumpInfo desensitizedMeta = desensitizeHiveMeta(hiveMeta, dict);
                        jsonArray.add(GsonUtils.GSON.toJson(desensitizedMeta));
                        externalTableInfoData.add(fullName, jsonArray);
                    }
                }
            }
            dumpJson.add("hms_table", externalTableInfoData);
        }

        // table row count
        JsonObject tableRowCount = new JsonObject();
        for (Map.Entry<String, Map<String, Long>> entry : dumpInfo.getPartitionRowCountMap().entrySet()) {
            JsonObject partitionRowCount = new JsonObject();
            for (Map.Entry<String, Long> partitionEntry : entry.getValue().entrySet()) {
                String partitionName = partitionEntry.getKey();
                if (entry.getValue().size() == 1 && dict.containsKey(partitionEntry.getKey())) {
                    // the partitionName of table without setting partition is the table name
                    partitionName = DesensitizedSQLBuilder.desensitizeTblName(partitionName, dict);
                }
                partitionRowCount.addProperty(partitionName, partitionEntry.getValue());

            }
            String[] splits = entry.getKey().split("\\.");
            String tableName = DesensitizedSQLBuilder.desensitizeDbName(splits[0], dict) + "."
                    + DesensitizedSQLBuilder.desensitizeTblName(splits[1], dict);
            tableRowCount.add(tableName, partitionRowCount);
        }
        dumpJson.add("table_row_count", tableRowCount);
        // automatic/expression partition values, keyed by the desensitized table name. The values are the
        // partition boundary values, which carry the same information as the (undesensitized) partition names
        // already kept in table_row_count above, so replay can rebuild the same partitions and match them.
        addPartitionValuesSection(dumpJson, tableMetaPairs, entry ->
                DesensitizedSQLBuilder.desensitizeDbName(entry.first, dict) + "."
                        + DesensitizedSQLBuilder.desensitizeTblName(entry.second.getName(), dict));
        // view meta
        if (!dumpInfo.getViewMap().isEmpty()) {
            JsonObject viewMetaData = new JsonObject();
            for (Pair<String, View> entry : dumpInfo.getViewMap().values()) {
                String viewName = DesensitizedSQLBuilder.desensitizeDbName(entry.first, dict) + "."
                        + DesensitizedSQLBuilder.desensitizeTblName(entry.second.getName(), dict);
                String viewDef = DesensitizedSQLBuilder.desensitizeViewDef(entry.second,
                        collector.getDesensitizedDict(), ConnectContext.get());
                viewMetaData.addProperty(viewName, viewDef);
            }
            dumpJson.add("view_meta", viewMetaData);
        }

        // column statistics
        JsonObject tableColumnStatistics = new JsonObject();
        for (Map.Entry<String, Map<String, ColumnStatistic>> entry : dumpInfo.getTableStatisticsMap().entrySet()) {
            JsonObject columnStatistics = new JsonObject();
            for (Map.Entry<String, ColumnStatistic> columnEntry : entry.getValue().entrySet()) {
                columnStatistics.add(
                        DesensitizedSQLBuilder.desensitizeColName(columnEntry.getKey(), dict),
                        GsonUtils.GSON.toJsonTree(ColumnStatisticDump.from(
                                stripSensitiveStatisticValues(columnEntry.getValue())))
                );
            }
            String[] splits = entry.getKey().split("\\.");
            String tableName = DesensitizedSQLBuilder.desensitizeDbName(splits[0], dict) + "."
                    + DesensitizedSQLBuilder.desensitizeTblName(splits[1], dict);
            tableColumnStatistics.add(tableName, columnStatistics);
        }
        dumpJson.add("column_statistics", tableColumnStatistics);
        String explainInfo = desensitizeExplainInfo(dumpInfo.getExplainInfo(), dict);
        if (StringUtils.isNotEmpty(explainInfo)) {
            dumpJson.addProperty("explain_info", desensitizeExplainInfo(dumpInfo.getExplainInfo(), dict));
        }

    }

    private static ColumnStatistic stripHistogram(ColumnStatistic columnStatistic) {
        if (columnStatistic.getHistogram() == null) {
            return columnStatistic;
        }
        return ColumnStatistic.buildFrom(columnStatistic).setHistogram(null).build();
    }

    private static ColumnStatistic stripSensitiveStatisticValues(ColumnStatistic columnStatistic) {
        return ColumnStatistic.buildFrom(columnStatistic)
                .setHistogram(null)
                .setMinString(null)
                .setMaxString(null)
                .build();
    }

    private HiveMetaStoreTableDumpInfo desensitizeHiveMeta(HiveMetaStoreTableDumpInfo hiveMeta, Map<String, String> dict) {
        HiveTableDumpInfo hiveTableDumpInfo = new HiveTableDumpInfo();
        if (CollectionUtils.isNotEmpty(hiveMeta.getDataColumnNames())) {
            hiveTableDumpInfo.setDataColumnNames(
                    hiveMeta.getDataColumnNames().stream()
                            .map(e -> DesensitizedSQLBuilder.desensitizeColName(e, dict))
                            .collect(Collectors.toList())
            );
        }

        if (CollectionUtils.isNotEmpty(hiveMeta.getPartColumnNames())) {
            hiveTableDumpInfo.setPartColumnNames(
                    hiveMeta.getPartColumnNames().stream()
                            .map(e -> DesensitizedSQLBuilder.desensitizeColName(e, dict))
                            .collect(Collectors.toList())
            );
        }

        if (CollectionUtils.isNotEmpty(hiveMeta.getPartitionNames())) {
            hiveTableDumpInfo.setPartitionNames(hiveMeta.getPartitionNames());
        }

        return hiveTableDumpInfo;
    }

    private String desensitizeExplainInfo(String explainInfo, Map<String, String> dict) {
        Set<String> keys = Sets.newHashSet();
        Pattern pattern = Pattern.compile("[0-9a-zA-Z_$\\u0080-\\uffff]+");

        for (String key : dict.keySet()) {
            if (pattern.matcher(key).matches()) {
                keys.add(key);
            }
        }
        pattern = Pattern.compile("\\b(" + String.join("|", keys) + ")\\b",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(explainInfo);

        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String matchStr = matcher.group();
            String value = dict.get(matchStr) == null ? dict.get(StringUtils.lowerCase(matchStr)) : dict.get(matchStr);
            if (value == null) {
                // failed desensitize ExplainInfo just return empty str
                return "";
            }
            matcher.appendReplacement(result, value);
        }
        matcher.appendTail(result);
        return result.toString();
    }
}
