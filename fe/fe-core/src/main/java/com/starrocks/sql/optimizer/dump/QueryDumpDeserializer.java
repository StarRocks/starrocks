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

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import com.starrocks.catalog.Resource;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.ColumnStatisticDump;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.HistogramUtils;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.LegacyColumnStatisticParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

public class QueryDumpDeserializer implements JsonDeserializer<QueryDumpInfo> {
    private static final Logger LOG = LogManager.getLogger(QueryDumpDeserializer.class);

    @Override
    public QueryDumpInfo deserialize(JsonElement jsonElement, Type type,
                                     JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        QueryDumpInfo dumpInfo = new QueryDumpInfo();

        JsonObject dumpJsonObject = jsonElement.getAsJsonObject();
        // statement
        String statement = dumpJsonObject.get("statement").getAsString();
        dumpInfo.setOriginStmt(statement);
        // resource
        if (dumpJsonObject.has("resources")) {
            JsonObject resourceMeta = dumpJsonObject.getAsJsonObject("resources");
            for (Map.Entry<String, JsonElement> entry : resourceMeta.entrySet()) {
                String resourceInfo = entry.getValue().getAsString();
                Resource resource = GsonUtils.GSON.fromJson(resourceInfo, Resource.class);
                dumpInfo.addResourceCreateStmt(resource.getDdlStmt());
            }
        }
        // table meta data
        JsonObject tableMeta = dumpJsonObject.getAsJsonObject("table_meta");
        for (Map.Entry<String, JsonElement> entry : tableMeta.entrySet()) {
            dumpInfo.addTableCreateStmt(entry.getKey(), entry.getValue().getAsString());
        }
        // hive meta store table info
        if (dumpJsonObject.has("hms_table")) {
            JsonObject externalTableMeta = dumpJsonObject.getAsJsonObject("hms_table");
            for (Map.Entry<String, JsonElement> entry : externalTableMeta.entrySet()) {
                String fullName = entry.getKey();
                String[] names = fullName.split("%");
                Preconditions.checkState(names.length == 3);
                String resourceName = names[0];
                String dbName = names[1];
                String tableName = names[2];

                JsonArray externalTableInfoArray = entry.getValue().getAsJsonArray();
                JsonObject typeObject = externalTableInfoArray.get(0).getAsJsonObject();
                String externalTableInfo = externalTableInfoArray.get(1).getAsString();
                if (typeObject.get("type").getAsString().equals("hive")) {
                    HiveTableDumpInfo hiveTableDumpInfo =
                            GsonUtils.GSON.fromJson(externalTableInfo, HiveTableDumpInfo.class);
                    dumpInfo.addHMSTable(resourceName, dbName, tableName, hiveTableDumpInfo);
                }
            }
        }
        // table row count
        JsonObject tableRowCount = dumpJsonObject.getAsJsonObject("table_row_count");
        for (String tableKey : tableRowCount.keySet()) {
            JsonObject partitionRowCount = tableRowCount.get(tableKey).getAsJsonObject();
            for (String partitionKey : partitionRowCount.keySet()) {
                long partitionRowCountNum = partitionRowCount.get(partitionKey).getAsLong();
                dumpInfo.addPartitionRowCount(tableKey, partitionKey, partitionRowCountNum);
            }
        }
        // view meta
        if (dumpJsonObject.has("view_meta")) {
            JsonObject viewMeta = dumpJsonObject.getAsJsonObject("view_meta");
            for (Map.Entry<String, JsonElement> entry : viewMeta.entrySet()) {
                dumpInfo.addViewCreateStmt(entry.getKey(), entry.getValue().getAsString());
            }
        }
        // session variables
        if (dumpJsonObject.has("session_variables")) {
            try {
                dumpInfo.getSessionVariable().replayFromJson(dumpJsonObject.get("session_variables").getAsString());
            } catch (IOException e) {
                LOG.warn("deserialize from json failed. ", e);
            }
        }
        // column statistics
        JsonObject tableColumnStatistics = dumpJsonObject.getAsJsonObject("column_statistics");
        for (String tableKey : tableColumnStatistics.keySet()) {
            JsonObject columnStatistics = tableColumnStatistics.get(tableKey).getAsJsonObject();
            for (String columnKey : columnStatistics.keySet()) {
                final var columnStatisticElement = columnStatistics.get(columnKey);
                ColumnStatistic columnStatistic;
                if (columnStatisticElement.isJsonObject()) {
                    columnStatistic = GsonUtils.GSON.fromJson(columnStatisticElement, ColumnStatisticDump.class)
                            .toColumnStatistic();
                } else {
                    // Legacy text format written by older versions
                    columnStatistic = LegacyColumnStatisticParser.parse(columnStatisticElement.getAsString()).build();
                }
                dumpInfo.addTableStatistics(tableKey, columnKey, columnStatistic);
            }
        }
        // Compatibility with dumps written while histograms used a separate side channel. New structured
        // column_statistics objects carry their histogram inline, but this optional legacy section still needs
        // to be merged onto either text or structured base statistics.
        if (dumpJsonObject.has("column_histogram")) {
            JsonObject tableColumnHistogram = dumpJsonObject.getAsJsonObject("column_histogram");
            for (String tableKey : tableColumnHistogram.keySet()) {
                JsonObject columnHistograms = tableColumnHistogram.get(tableKey).getAsJsonObject();
                Map<String, ColumnStatistic> tableStats =
                        dumpInfo.getTableStatisticsMap().getOrDefault(tableKey, Collections.emptyMap());
                for (String columnKey : columnHistograms.keySet()) {
                    ColumnStatistic base = tableStats.get(columnKey);
                    if (base == null) {
                        continue;
                    }
                    String histogramStr = columnHistograms.get(columnKey).getAsString();
                    Histogram histogram = HistogramUtils.deserializeHistogram(histogramStr);
                    dumpInfo.addTableStatistics(tableKey, columnKey,
                            ColumnStatistic.buildFrom(base).setHistogram(histogram).build());
                }
            }
        }
        // low-cardinality global dictionary captured for the query; replay seeds it so the dict-encoding
        // (Decode-node) optimization reproduces offline. Optional section (older dumps lack it), guarded by has().
        if (dumpJsonObject.has("global_dict")) {
            JsonObject tableGlobalDict = dumpJsonObject.getAsJsonObject("global_dict");
            for (String tableKey : tableGlobalDict.keySet()) {
                JsonObject columnDicts = tableGlobalDict.get(tableKey).getAsJsonObject();
                for (String columnKey : columnDicts.keySet()) {
                    dumpInfo.addTableGlobalDict(tableKey, columnKey,
                            ColumnDict.fromJson(columnDicts.get(columnKey).getAsString()));
                }
            }
        }
        // column min/max captured for replay (meta-scan / group-by-compressed-key rewrites). Optional section
        // (older dumps don't have it), guarded by has(); mirror of global_dict keyed db.table -> column.
        if (dumpJsonObject.has("column_min_max")) {
            JsonObject tableColumnMinMax = dumpJsonObject.getAsJsonObject("column_min_max");
            for (String tableKey : tableColumnMinMax.keySet()) {
                JsonObject columnMinMaxes = tableColumnMinMax.get(tableKey).getAsJsonObject();
                for (String columnKey : columnMinMaxes.keySet()) {
                    JsonObject minMax = columnMinMaxes.get(columnKey).getAsJsonObject();
                    String min = minMax.has("min") && !minMax.get("min").isJsonNull()
                            ? minMax.get("min").getAsString() : null;
                    String max = minMax.has("max") && !minMax.get("max").isJsonNull()
                            ? minMax.get("max").getAsString() : null;
                    dumpInfo.addColumnMinMax(tableKey, columnKey, new IMinMaxStatsMgr.ColumnMinMax(min, max));
                }
            }
        }
        // automatic/expression partition values: one representative value tuple per concrete partition, used
        // to recreate partitions on replay for tables whose CREATE TABLE omits partition definitions.
        // Optional section (older dumps and tables with explicit partitions don't have it), guarded by has().
        if (dumpJsonObject.has("partition_values")) {
            JsonObject tablePartitionValues = dumpJsonObject.getAsJsonObject("partition_values");
            for (String tableKey : tablePartitionValues.keySet()) {
                JsonArray valuesArray = tablePartitionValues.get(tableKey).getAsJsonArray();
                List<List<String>> partitionValues = new ArrayList<>();
                for (JsonElement tupleElement : valuesArray) {
                    List<String> tuple = new ArrayList<>();
                    for (JsonElement valueElement : tupleElement.getAsJsonArray()) {
                        tuple.add(valueElement.getAsString());
                    }
                    partitionValues.add(tuple);
                }
                dumpInfo.addAutomaticPartitionValues(tableKey, partitionValues);
            }
        }
        // BE number
        int beNum = dumpJsonObject.get("be_number").getAsInt();
        dumpInfo.setBeNum(beNum);
        // Be core stat
        if (dumpJsonObject.has("be_core_stat")) {
            JsonObject beCoreStat = dumpJsonObject.getAsJsonObject("be_core_stat");
            dumpInfo.setCachedAvgNumOfHardwareCores(beCoreStat.get("cachedAvgNumOfHardwareCores").getAsInt());
            Map<Long, Integer> numOfHardwareCoresPerBe = GsonUtils.GSON.fromJson(
                    beCoreStat.get("numOfHardwareCoresPerBe").getAsString(),
                    new TypeToken<Map<Long, Integer>>() {
                    }.getType());
            dumpInfo.addNumOfHardwareCoresPerBe(numOfHardwareCoresPerBe);
        }

        return dumpInfo;
    }
}
