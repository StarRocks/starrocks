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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Version;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.system.BackendCoreStat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class QueryDumpSerializer implements JsonSerializer<QueryDumpInfo> {
    private static final Logger LOG = LogManager.getLogger(QueryDumpSerializer.class);

    @Override
    public JsonElement serialize(QueryDumpInfo dumpInfo, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject dumpJson = serializeSensitiveContent(dumpInfo);

        // session variables
        try {
            dumpJson.addProperty("session_variables", dumpInfo.getSessionVariable().getJsonString());
        } catch (IOException e) {
            LOG.warn("serialize session variables failed. " + e);
        }

        // BE number
        ConnectContext ctx = ConnectContext.get();
        long beNum = ctx.getAliveBackendNumber();
        dumpJson.addProperty("be_number", beNum);
        // backend core stat
        JsonObject backendCoreStat = new JsonObject();
        backendCoreStat.addProperty("numOfHardwareCoresPerBe",
                GsonUtils.GSON.toJson(BackendCoreStat.getNumOfHardwareCoresPerBe()));
        backendCoreStat.addProperty("cachedAvgNumOfHardwareCores",
                BackendCoreStat.getCachedAvgNumOfHardwareCores());
        dumpJson.add("be_core_stat", backendCoreStat);
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

    private JsonObject serializeSensitiveContent(QueryDumpInfo dumpInfo) {
        JsonObject dumpJson = new JsonObject();
        if (dumpInfo.isDesensitizedInfo()) {
            try {
                desensitizeContent(dumpInfo, dumpJson);
                return dumpJson;
            } catch (Exception e) {
                LOG.info("failed to desensitize content, use the original content", e);
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
            GlobalStateMgr.getDdlStmt(entry.second, createTableStmt, null, null, false, true /* hide password */);
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
                columnStatistics.addProperty(columnEntry.getKey(), columnEntry.getValue().toString());
            }
            tableColumnStatistics.add(entry.getKey(), columnStatistics);
        }
        dumpJson.add("column_statistics", tableColumnStatistics);
        return dumpJson;
    }

    private void desensitizeContent(QueryDumpInfo dumpInfo, JsonObject dumpJson) {
        DesensitizedInfoCollector collector = new DesensitizedInfoCollector(dumpInfo);
        collector.init();
        Map<String, String> dict = collector.getDesensitizedDict();
        String sql = DesensitizedSQLBuilder.desensitizeSQL(dumpInfo.getStatement(), dict);
        // statement
        dumpJson.addProperty("statement", sql);
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

        // table row count
        JsonObject tableRowCount = new JsonObject();
        for (Map.Entry<String, Map<String, Long>> entry : dumpInfo.getPartitionRowCountMap().entrySet()) {
            JsonObject partitionRowCount = new JsonObject();
            for (Map.Entry<String, Long> partitionEntry : entry.getValue().entrySet()) {
                partitionRowCount.addProperty(partitionEntry.getKey(), partitionEntry.getValue());
            }
            String[] splits = entry.getKey().split("\\.");
            String tableName = DesensitizedSQLBuilder.desensitizeDbName(splits[0], dict) + "."
                    + DesensitizedSQLBuilder.desensitizeTblName(splits[1], dict);
            tableRowCount.add(tableName, partitionRowCount);
        }
        dumpJson.add("table_row_count", tableRowCount);
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
                columnStatistics.addProperty(
                        DesensitizedSQLBuilder.desensitizeColName(columnEntry.getKey(), dict),
                        columnEntry.getValue().toString()
                );
            }
            String[] splits = entry.getKey().split("\\.");
            String tableName = DesensitizedSQLBuilder.desensitizeDbName(splits[0], dict) + "."
                    + DesensitizedSQLBuilder.desensitizeTblName(splits[1], dict);
            tableColumnStatistics.add(tableName, columnStatistics);
        }
        dumpJson.add("column_statistics", tableColumnStatistics);
    }
}
