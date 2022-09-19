// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.dump;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.external.hive.HdfsFileDesc;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HivePartitionStats;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.external.hive.RemoteFileInputFormat;
import com.starrocks.persist.gson.GsonUtils;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class HiveTableDumpInfo implements HiveMetaStoreTableDumpInfo {
    private Map<PartitionKey, Long> partitionKeys;
    private HiveTableStats hiveTableStats;
    private Map<PartitionKey, HivePartitionStats> hivePartitionStats = Maps.newHashMap();
    private Map<PartitionKey, HivePartition> partitions = Maps.newHashMap();
    private List<String> partColumnNames;
    private List<String> dataColumnNames;
    private Map<String, HiveColumnStats> tableLevelColumnStats = Maps.newHashMap();
    private static final String TYPE = "hive";

    @Override
    public void setPartitionKeys(Map<PartitionKey, Long> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    @Override
    public Map<PartitionKey, Long> getPartitionKeys() {
        return partitionKeys;
    }

    @Override
    public void setHiveTableStats(HiveTableStats hiveTableStats) {
        this.hiveTableStats = hiveTableStats;
    }

    @Override
    public HiveTableStats getHiveTableStats() {
        return hiveTableStats;
    }

    @Override
    public void addPartitions(Map<PartitionKey, HivePartition> partitions) {
        this.partitions.putAll(partitions);
    }

    @Override
    public Map<PartitionKey, HivePartition> getPartitions() {
        return partitions;
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
    public void addPartitionsStats(Map<PartitionKey, HivePartitionStats> partitionsStats) {
        this.hivePartitionStats.putAll(partitionsStats);
    }

    @Override
    public Map<PartitionKey, HivePartitionStats> getPartitionsStats() {
        return this.hivePartitionStats;
    }

    @Override
    public void addTableLevelColumnStats(Map<String, HiveColumnStats> tableLevelColumnStats) {
        this.tableLevelColumnStats.putAll(tableLevelColumnStats);
    }

    @Override
    public Map<String, HiveColumnStats> getTableLevelColumnStats() {
        return this.tableLevelColumnStats;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class HiveTableDumpInfoSerializer implements JsonSerializer<HiveTableDumpInfo> {
        @Override
        public JsonElement serialize(HiveTableDumpInfo hiveTableDumpInfo, Type type,
                                     JsonSerializationContext jsonSerializationContext) {
            Map<PartitionKey, Long> partitionKeys = hiveTableDumpInfo.partitionKeys;
            Map<PartitionKey, HivePartition> partitions = hiveTableDumpInfo.partitions;
            HiveTableStats hiveTableStats = hiveTableDumpInfo.hiveTableStats;
            Map<PartitionKey, HivePartitionStats> hivePartitionStats = hiveTableDumpInfo.hivePartitionStats;
            List<String> partColumnNames = hiveTableDumpInfo.partColumnNames;
            List<String> dataColumnNames = hiveTableDumpInfo.dataColumnNames;
            Map<String, HiveColumnStats> tableLevelColumnStats = hiveTableDumpInfo.tableLevelColumnStats;

            JsonObject result = new JsonObject();
            // serialize partitionKeys
            if (partitionKeys != null) {
                JsonObject partitionKeysJson = new JsonObject();
                for (Map.Entry<PartitionKey, Long> entry : partitionKeys.entrySet()) {
                    partitionKeysJson.addProperty(entry.getKey().toString(), entry.getValue());
                }
                result.add("PartitionKeys", partitionKeysJson);
            }
            // serialize partitions
            if (!partitions.isEmpty()) {
                JsonObject partitionsJson = new JsonObject();
                for (Map.Entry<PartitionKey, HivePartition> entry : partitions.entrySet()) {
                    HivePartition partition = entry.getValue();
                    JsonObject partitionJson = new JsonObject();
                    partitionJson.addProperty("format", partition.getFormat().toString());
                    partitionJson.addProperty("fullPath", partition.getFullPath());
                    JsonArray fileArray = new JsonArray();
                    for (HdfsFileDesc fileDesc : partition.getFiles()) {
                        JsonObject fileObject = new JsonObject();
                        fileObject.addProperty("fileName", fileDesc.getFileName());
                        fileObject.addProperty("compression", fileDesc.getCompression());
                        fileObject.addProperty("length", fileDesc.getLength());
                        fileObject.addProperty("splittable", fileDesc.isSplittable());
                        fileArray.add(fileObject);
                    }
                    partitionJson.add("files", fileArray);
                    partitionsJson.add(entry.getKey().toString(), partitionJson);
                }
                result.add("Partitions", partitionsJson);
            }
            // serialize hive table stats
            if (hiveTableStats != null) {
                result.addProperty("HiveTableStats", GsonUtils.GSON.toJson(hiveTableStats));
            }
            // serialize hive partition stats
            if (!hivePartitionStats.isEmpty()) {
                JsonObject partitionStatsObject = new JsonObject();
                for (Map.Entry<PartitionKey, HivePartitionStats> entry : hivePartitionStats.entrySet()) {
                    partitionStatsObject.addProperty(entry.getKey().toString(), GsonUtils.GSON.toJson(entry.getValue()));
                }
                result.add("HivePartitionStats", partitionStatsObject);
            }
            // serialize hive table level column statistics
            if (!tableLevelColumnStats.isEmpty()) {
                JsonObject tableColumnStatisticsObject = new JsonObject();
                for (Map.Entry<String, HiveColumnStats> entry : tableLevelColumnStats.entrySet()) {
                    tableColumnStatisticsObject.addProperty(entry.getKey(), entry.getValue().toString());
                }
                result.add("tableLevelColumnStats", tableColumnStatisticsObject);
            }

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
            // deserialize partitionKeys
            JsonObject partitionKeysObject = dumpJsonObject.getAsJsonObject("PartitionKeys");
            Map<PartitionKey, Long> partitionKeyMap = Maps.newHashMap();
            for (Map.Entry<String, JsonElement> entry : partitionKeysObject.entrySet()) {
                String partitionKeyStr = entry.getKey();
                PartitionKey partitionKey = PartitionKey.fromString(partitionKeyStr);
                long partitionId = entry.getValue().getAsLong();
                partitionKeyMap.put(partitionKey, partitionId);
            }
            hiveTableDumpInfo.setPartitionKeys(partitionKeyMap);

            // deserialize partitions
            JsonObject partitionsObject = dumpJsonObject.getAsJsonObject("Partitions");
            Map<PartitionKey, HivePartition> hivePartitions = Maps.newHashMap();
            for (Map.Entry<String, JsonElement> entry : partitionsObject.entrySet()) {
                JsonObject partition = entry.getValue().getAsJsonObject();
                RemoteFileInputFormat format = RemoteFileInputFormat.valueOf(partition.get("format").getAsString());
                String fullPath = partition.get("fullPath").getAsString();
                List<HdfsFileDesc> fileDescs = Lists.newArrayList();
                for (JsonElement file : partition.get("files").getAsJsonArray()) {
                    JsonObject fileObject = file.getAsJsonObject();
                    fileDescs.add(new HdfsFileDesc(fileObject.get("fileName").getAsString(),
                            fileObject.get("compression").getAsString(), fileObject.get("length").getAsLong(),
                            ImmutableList.of(), ImmutableList.of(), fileObject.get("splittable").getAsBoolean(),
                            null));
                }

                hivePartitions.put(PartitionKey.fromString(entry.getKey()),
                        new HivePartition(format, ImmutableList.copyOf(fileDescs), fullPath));
            }
            hiveTableDumpInfo.addPartitions(hivePartitions);

            // deserialize hive table stats
            JsonPrimitive hiveTableStatsPrimitive = dumpJsonObject.getAsJsonPrimitive("HiveTableStats");
            HiveTableStats hiveTableStats = GsonUtils.GSON.fromJson(hiveTableStatsPrimitive.getAsString(), HiveTableStats.class);
            hiveTableDumpInfo.setHiveTableStats(hiveTableStats);

            // deserialize hive partition stats
            if (dumpJsonObject.has("HivePartitionStats")) {
                JsonObject hivePartitionStatsObject = dumpJsonObject.getAsJsonObject("HivePartitionStats");
                Map<PartitionKey, HivePartitionStats> hivePartitionStatsMap = Maps.newHashMap();
                for (Map.Entry<String, JsonElement> entry : hivePartitionStatsObject.entrySet()) {
                    hivePartitionStatsMap.put(PartitionKey.fromString(entry.getKey()),
                            GsonUtils.GSON.fromJson(entry.getValue().getAsString(), HivePartitionStats.class));
                }
                hiveTableDumpInfo.addPartitionsStats(hivePartitionStatsMap);
            }

            // deserialize hive table level column statistics
            if (dumpJsonObject.has("tableLevelColumnStats")) {
                JsonObject tableLevelColumnStats = dumpJsonObject.getAsJsonObject("tableLevelColumnStats");
                Map<String, HiveColumnStats> tableLevelColumnStatsMap = Maps.newHashMap();
                for (Map.Entry<String, JsonElement> columnStats : tableLevelColumnStats.entrySet()) {
                    tableLevelColumnStatsMap.put(columnStats.getKey(), HiveColumnStats.fromString(
                            columnStats.getValue().getAsString()));
                }
                hiveTableDumpInfo.addTableLevelColumnStats(tableLevelColumnStatsMap);
            }

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

            return hiveTableDumpInfo;
        }
    }
}
