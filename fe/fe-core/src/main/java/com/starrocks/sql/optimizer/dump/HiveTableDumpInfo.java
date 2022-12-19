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

import com.clearspring.analytics.util.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.List;

public class HiveTableDumpInfo implements HiveMetaStoreTableDumpInfo {
    private List<String> partitionNames;
    private List<String> partColumnNames;
    private List<String> dataColumnNames;
    private double rowCount;
    private static final String TYPE = "hive";

    @Override
    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    @Override
    public List<String> getPartitionNames() {
        return partitionNames;
    }

    @Override
    public void setPartColumnNames(List<String> partColumnNames) {
        this.partColumnNames = partColumnNames;
    }

    @Override
    public List<String> getPartColumnNames() {
        return this.partColumnNames;
    }

    @Override
    public void setDataColumnNames(List<String> dataColumnNames) {
        this.dataColumnNames = dataColumnNames;
    }

    @Override
    public List<String> getDataColumnNames() {
        return this.dataColumnNames;
    }

    @Override
    public double getScanRowCount() {
        return rowCount;
    }

    @Override
    public void setScanRowCount(double rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class HiveTableDumpInfoSerializer implements JsonSerializer<HiveTableDumpInfo> {
        @Override
        public JsonElement serialize(HiveTableDumpInfo hiveTableDumpInfo, Type type,
                                     JsonSerializationContext jsonSerializationContext) {
            List<String> partitionNames = hiveTableDumpInfo.partitionNames;
            List<String> partColumnNames = hiveTableDumpInfo.partColumnNames;
            List<String> dataColumnNames = hiveTableDumpInfo.dataColumnNames;

            JsonObject result = new JsonObject();

            result.addProperty("OutputRowCount", String.valueOf(hiveTableDumpInfo.rowCount));

            // serialize partition columns
            if (partColumnNames != null) {
                JsonArray partColumnNamesJson = new JsonArray();
                for (String partColumnName : partColumnNames) {
                    partColumnNamesJson.add(partColumnName);
                }
                result.add("PartitionColumns", partColumnNamesJson);
            }

            // serialize data columns
            if (dataColumnNames != null) {
                JsonArray dataColumnNamesJson = new JsonArray();
                for (String dataColumnName : dataColumnNames) {
                    dataColumnNamesJson.add(dataColumnName);
                }
                result.add("DataColumns", dataColumnNamesJson);
            }

            // serialize partition names
            if (partitionNames != null) {
                JsonArray partitionNamesJson = new JsonArray();
                for (String partitionName : partitionNames) {
                    partitionNamesJson.add(partitionName);
                }
                result.add("PartitionNames", partitionNamesJson);
            }

            return result;
        }
    }

    public static class HiveTableDumpInfoDeserializer implements JsonDeserializer<HiveTableDumpInfo> {
        @Override
        public HiveTableDumpInfo deserialize(JsonElement jsonElement, Type type,
                                             JsonDeserializationContext jsonDeserializationContext)
                throws JsonParseException {
            HiveTableDumpInfo hiveTableDumpInfo = new HiveTableDumpInfo();
            JsonObject dumpJsonObject = jsonElement.getAsJsonObject();

            double rowCount = dumpJsonObject.getAsJsonPrimitive("OutputRowCount").getAsDouble();
            hiveTableDumpInfo.setScanRowCount(rowCount);

            // deserialize partition columns
            JsonArray partitionColumnsJson = dumpJsonObject.getAsJsonArray("PartitionColumns");
            List<String> partitionColumns = Lists.newArrayList();
            for (JsonElement partitionColumn : partitionColumnsJson) {
                partitionColumns.add(partitionColumn.getAsString());
            }
            hiveTableDumpInfo.setPartColumnNames(partitionColumns);

            // deserialize data columns
            JsonArray dateColumnsJson = dumpJsonObject.getAsJsonArray("DataColumns");
            List<String> dataColumns = Lists.newArrayList();
            for (JsonElement dataColumn : dateColumnsJson) {
                dataColumns.add(dataColumn.getAsString());
            }
            hiveTableDumpInfo.setDataColumnNames(dataColumns);

            // deserialize partition names
            JsonArray partitionNamesJson = dumpJsonObject.getAsJsonArray("PartitionNames");
            List<String> partitionNames = Lists.newArrayList();
            for (JsonElement partitionName : partitionNamesJson) {
                partitionNames.add(partitionName.getAsString());
            }
            hiveTableDumpInfo.setPartitionNames(partitionNames);

            return hiveTableDumpInfo;
        }
    }
}
