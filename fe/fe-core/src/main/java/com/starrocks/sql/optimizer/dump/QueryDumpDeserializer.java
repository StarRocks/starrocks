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
                LOG.warn("deserialize from json failed. " + e);
            }
        }
        // column statistics
        JsonObject tableColumnStatistics = dumpJsonObject.getAsJsonObject("column_statistics");
        for (String tableKey : tableColumnStatistics.keySet()) {
            JsonObject columnStatistics = tableColumnStatistics.get(tableKey).getAsJsonObject();
            for (String columnKey : columnStatistics.keySet()) {
                String columnStatistic = columnStatistics.get(columnKey).getAsString();
                dumpInfo.addTableStatistics(tableKey, columnKey, ColumnStatistic.buildFrom(columnStatistic).build());
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
