// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.dump;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Version;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class QueryDumpSerializer implements JsonSerializer<QueryDumpInfo> {
    private static final Logger LOG = LogManager.getLogger(QueryDumpSerializer.class);

    @Override
    public JsonElement serialize(QueryDumpInfo dumpInfo, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject dumpJson = new JsonObject();
        // 1. statement
        dumpJson.addProperty("statement", dumpInfo.getOriginStmt());
        // 2. table meta
        JsonObject tableMetaData = new JsonObject();
        List<Pair<String, Table>> tableMetaPairs = Lists.newArrayList(dumpInfo.getTableMap().values());
        tableMetaPairs.sort(Comparator.comparing(pair -> pair.first + ":" + pair.second.getName()));
        for (Pair<String, com.starrocks.catalog.Table> entry : tableMetaPairs) {
            String tableName = entry.first + "." + entry.second.getName();
            List<String> createTableStmt = Lists.newArrayList();
            GlobalStateMgr.getDdlStmt(entry.second, createTableStmt, null, null, false, true /* hide password */);
            tableMetaData.addProperty(tableName, createTableStmt.get(0));
        }
        dumpJson.add("table_meta", tableMetaData);
        // 3. table row count
        JsonObject tableRowCount = new JsonObject();
        for (Map.Entry<String, Map<String, Long>> entry : dumpInfo.getPartitionRowCountMap().entrySet()) {
            JsonObject partitionRowCount = new JsonObject();
            for (Map.Entry<String, Long> partitionEntry : entry.getValue().entrySet()) {
                partitionRowCount.addProperty(partitionEntry.getKey(), partitionEntry.getValue());
            }
            tableRowCount.add(entry.getKey(), partitionRowCount);
        }
        dumpJson.add("table_row_count", tableRowCount);
        // 4. view meta
        if (!dumpInfo.getViewMap().isEmpty()) {
            JsonObject viewMetaData = new JsonObject();
            for (Pair<String, View> entry : dumpInfo.getViewMap().values()) {
                String viewName = entry.first + "." + entry.second.getName();
                viewMetaData.addProperty(viewName, entry.second.getInlineViewDef());
            }
            dumpJson.add("view_meta", viewMetaData);
        }
        // 5. session variables
        try {
            dumpJson.addProperty("session_variables", dumpInfo.getSessionVariable().getJsonString());
        } catch (IOException e) {
            LOG.warn("serialize session variables failed. " + e);
        }
        // 6. column statistics
        JsonObject tableColumnStatistics = new JsonObject();
        for (Map.Entry<String, Map<String, ColumnStatistic>> entry : dumpInfo.getTableStatisticsMap().entrySet()) {
            JsonObject columnStatistics = new JsonObject();
            for (Map.Entry<String, ColumnStatistic> columnEntry : entry.getValue().entrySet()) {
                columnStatistics.addProperty(columnEntry.getKey(), columnEntry.getValue().toString());
            }
            tableColumnStatistics.add(entry.getKey(), columnStatistics);
        }
        dumpJson.add("column_statistics", tableColumnStatistics);
        // 7. BE number
        long beNum = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).size();
        dumpJson.addProperty("be_number", beNum);
        // 8. exception
        JsonArray exceptions = new JsonArray();
        for (String ex : dumpInfo.getExceptionList()) {
            exceptions.add(ex);
        }
        dumpJson.add("exception", exceptions);
        // 9. version
        if (!FeConstants.runningUnitTest) {
            dumpJson.addProperty("version", Version.STARROCKS_VERSION);
            dumpJson.addProperty("commit_version", Version.STARROCKS_COMMIT_HASH);
        }
        return dumpJson;
    }
}
