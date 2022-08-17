// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.dump;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
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
        // 1. statement
        String statement = dumpJsonObject.get("statement").getAsString();
        dumpInfo.setOriginStmt(statement);
        // 2. table meta data
        JsonObject tableMeta = dumpJsonObject.getAsJsonObject("table_meta");
        for (Map.Entry<String, JsonElement> entry : tableMeta.entrySet()) {
            dumpInfo.addTableCreateStmt(entry.getKey(), entry.getValue().getAsString());
        }
        // 3. table row count
        JsonObject tableRowCount = dumpJsonObject.getAsJsonObject("table_row_count");
        for (String tableKey : tableRowCount.keySet()) {
            JsonObject partitionRowCount = tableRowCount.get(tableKey).getAsJsonObject();
            for (String partitionKey : partitionRowCount.keySet()) {
                long partitionRowCountNum = partitionRowCount.get(partitionKey).getAsLong();
                dumpInfo.addPartitionRowCount(tableKey, partitionKey, partitionRowCountNum);
            }
        }
        // 4. view meta
        if (dumpJsonObject.has("view_meta")) {
            JsonObject viewMeta = dumpJsonObject.getAsJsonObject("view_meta");
            for (Map.Entry<String, JsonElement> entry : viewMeta.entrySet()) {
                dumpInfo.addViewCreateStmt(entry.getKey(), entry.getValue().getAsString());
            }
        }
        // 5. session variables
        if (dumpJsonObject.has("session_variables")) {
            try {
                dumpInfo.getSessionVariable().replayFromJson(dumpJsonObject.get("session_variables").getAsString());
            } catch (IOException e) {
                LOG.warn("deserialize from json failed. " + e);
            }
        }
        // 6. column statistics
        JsonObject tableColumnStatistics = dumpJsonObject.getAsJsonObject("column_statistics");
        for (String tableKey : tableColumnStatistics.keySet()) {
            JsonObject columnStatistics = tableColumnStatistics.get(tableKey).getAsJsonObject();
            for (String columnKey : columnStatistics.keySet()) {
                String columnStatistic = columnStatistics.get(columnKey).getAsString();
                dumpInfo.addTableStatistics(tableKey, columnKey, ColumnStatistic.buildFrom(columnStatistic).build());
            }
        }
        // 7. BE number
        int beNum = dumpJsonObject.get("be_number").getAsInt();
        dumpInfo.setBeNum(beNum);

        return dumpInfo;
    }
}
