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


package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.io.Text;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.THudiTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;


/**
 * Currently, we depend on Hive metastore to obtain table/partition path and statistics.
 * This logic should be decoupled from metastore when the related interfaces are ready.
 */
public class HudiTable extends Table implements HiveMetaStoreTable {
    private static final Logger LOG = LogManager.getLogger(HudiTable.class);

    private static final String JSON_KEY_HUDI_DB = "database";
    private static final String JSON_KEY_HUDI_TABLE = "table";
    private static final String JSON_KEY_RESOURCE_NAME = "resource";
    private static final String JSON_KEY_PART_COLUMN_NAMES = "partColumnNames";
    private static final String JSON_KEY_DATA_COLUMN_NAMES = "dataColumnNames";
    private static final String JSON_KEY_HUDI_PROPERTIES = "hudiProperties";

    public static final String HUDI_TABLE_TYPE = "hudi.table.type";
    public static final String HUDI_BASE_PATH = "hudi.table.base.path";
    public static final String HUDI_TABLE_SERDE_LIB = "hudi.table.serde.lib";
    public static final String HUDI_TABLE_INPUT_FOAMT = "hudi.table.input.format";
    public static final String HUDI_TABLE_COLUMN_NAMES = "hudi.table.column.names";
    public static final String HUDI_TABLE_COLUMN_TYPES = "hudi.table.column.types";

    public static final String COW_INPUT_FORMAT = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    public static final String COW_INPUT_FORMAT_LEGACY = "com.uber.hoodie.hadoop.HoodieInputFormat";
    public static final String MOR_RO_INPUT_FORMAT = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    public static final String MOR_RO_INPUT_FORMAT_LEGACY = "com.uber.hoodie.hadoop.HoodieInputFormat";
    public static final String MOR_RT_INPUT_FORMAT = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    public static final String MOR_RT_INPUT_FORMAT_LEGACY = "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat";

    public enum HudiTableType {
        COW, MOR, UNKNOWN
    }

    private String catalogName;
    @SerializedName(value = "dn")
    private String hiveDbName;
    @SerializedName(value = "tn")
    private String hiveTableName;
    @SerializedName(value = "rn")
    private String resourceName;
    @SerializedName(value = "pcn")
    private List<String> partColumnNames = Lists.newArrayList();
    // dataColumnNames stores all the non-partition columns of the hudi table,
    // consistent with the order defined in the hudi table
    @SerializedName(value = "dcn")
    private List<String> dataColumnNames = Lists.newArrayList();
    @SerializedName(value = "prop")
    private Map<String, String> hudiProperties = Maps.newHashMap();

    public HudiTable() {
        super(TableType.HUDI);
    }

    public HudiTable(long id, String name, String catalogName, String hiveDbName, String hiveTableName,
                     String resourceName, List<Column> schema, List<String> dataColumnNames,
                     List<String> partColumnNames, long createTime, Map<String, String> properties) {
        super(id, name, TableType.HUDI, schema);
        this.catalogName = catalogName;
        this.hiveDbName = hiveDbName;
        this.hiveTableName = hiveTableName;
        this.resourceName = resourceName;
        this.dataColumnNames = dataColumnNames;
        this.partColumnNames = partColumnNames;
        this.createTime = createTime;
        this.hudiProperties = properties;
    }

    public String getDbName() {
        return hiveDbName;
    }

    public String getResourceName() {
        return resourceName;
    }

    @Override
    public String getCatalogName() {
        return catalogName == null ? getResourceMappingCatalogName(resourceName, "hudi") : catalogName;
    }

    public HoodieTableType getTableType() {
        return HoodieTableType.valueOf(hudiProperties.get(HUDI_TABLE_TYPE));
    }

    public String getTableLocation() {
        return hudiProperties.get(HUDI_BASE_PATH);
    }

    public String getHudiInputFormat() {
        return hudiProperties.get(HUDI_TABLE_INPUT_FOAMT);
    }

    @Override
    public String getTableName() {
        return hiveTableName;
    }

    @Override
    public String getUUID() {
        if (CatalogMgr.isExternalCatalog(catalogName)) {
            return String.join(".", catalogName, hiveDbName, hiveTableName, Long.toString(createTime));
        } else {
            return Long.toString(id);
        }
    }

    @Override
    public List<Column> getPartitionColumns() {
        return partColumnNames.stream()
                .map(name -> nameToColumn.get(name))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    public List<String> getDataColumnNames() {
        return dataColumnNames;
    }

    @Override
    public boolean isUnPartitioned() {
        return partColumnNames.size() == 0;
    }

    @Override
    public String getTableIdentifier() {
        return Joiner.on(":").join(name, createTime);
    }

    public static HudiTableType fromInputFormat(String inputFormat) {
        switch (inputFormat) {
            case COW_INPUT_FORMAT:
            case COW_INPUT_FORMAT_LEGACY:
                return HudiTableType.COW;
            case MOR_RT_INPUT_FORMAT:
            case MOR_RT_INPUT_FORMAT_LEGACY:
                return HudiTableType.MOR;
            default:
                return HudiTableType.UNKNOWN;
        }
    }

    public Map<String, String> getProperties() {
        return hudiProperties;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        THudiTable tHudiTable = new THudiTable();
        tHudiTable.setLocation(getTableLocation());

        // columns and partition columns
        Set<String> partitionColumnNames = Sets.newHashSet();
        List<TColumn> tPartitionColumns = Lists.newArrayList();
        List<TColumn> tColumns = Lists.newArrayList();

        for (Column column : getPartitionColumns()) {
            tPartitionColumns.add(column.toThrift());
            partitionColumnNames.add(column.getName());
        }
        for (Column column : getBaseSchema()) {
            if (partitionColumnNames.contains(column.getName())) {
                continue;
            }
            tColumns.add(column.toThrift());
        }
        tHudiTable.setColumns(tColumns);
        if (!tPartitionColumns.isEmpty()) {
            tHudiTable.setPartition_columns(tPartitionColumns);
        }

        // partitions
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        for (DescriptorTable.ReferencedPartitionInfo partition : partitions) {
            partitionKeys.add(partition.getKey());
        }
        List<RemoteFileInfo> hudiPartitions;
        try {
            hudiPartitions = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getRemoteFileInfos(getCatalogName(), this, partitionKeys);
        } catch (StarRocksConnectorException e) {
            LOG.warn("Table {} gets partition info failed.", name, e);
            return null;
        }

        for (int i = 0; i < hudiPartitions.size(); i++) {
            DescriptorTable.ReferencedPartitionInfo info = partitions.get(i);
            PartitionKey key = info.getKey();
            long partitionId = info.getId();

            THdfsPartition tPartition = new THdfsPartition();
            tPartition.setFile_format(hudiPartitions.get(i).getFormat().toThrift());

            List<LiteralExpr> keys = key.getKeys();
            tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));

            THdfsPartitionLocation tPartitionLocation = new THdfsPartitionLocation();
            tPartitionLocation.setPrefix_index(-1);
            tPartitionLocation.setSuffix(hudiPartitions.get(i).getFullPath());
            tPartition.setLocation(tPartitionLocation);
            tHudiTable.putToPartitions(partitionId, tPartition);
        }

        Configuration conf = new Configuration();
        HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder().setConf(conf).setBasePath(getTableLocation()).build();
        HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        Option<HoodieInstant> latestInstant = timeline.lastInstant();
        String queryInstant = "";
        if (latestInstant.isPresent()) {
            queryInstant = latestInstant.get().getTimestamp();
        }
        tHudiTable.setInstant_time(queryInstant);
        tHudiTable.setHive_column_names(hudiProperties.get(HUDI_TABLE_COLUMN_NAMES));
        tHudiTable.setHive_column_types(hudiProperties.get(HUDI_TABLE_COLUMN_TYPES));
        tHudiTable.setInput_format(hudiProperties.get(HUDI_TABLE_INPUT_FOAMT));
        tHudiTable.setSerde_lib(hudiProperties.get(HUDI_TABLE_SERDE_LIB));
        tHudiTable.setIs_mor_table(getTableType() == HoodieTableType.MERGE_ON_READ);

        TTableDescriptor tTableDescriptor =
                new TTableDescriptor(id, TTableType.HUDI_TABLE, fullSchema.size(), 0, hiveTableName, hiveDbName);
        tTableDescriptor.setHudiTable(tHudiTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(JSON_KEY_HUDI_DB, hiveDbName);
        jsonObject.addProperty(JSON_KEY_HUDI_TABLE, hiveTableName);
        if (!Strings.isNullOrEmpty(resourceName)) {
            jsonObject.addProperty(JSON_KEY_RESOURCE_NAME, resourceName);
        }
        if (!partColumnNames.isEmpty()) {
            JsonArray jPartColumnNames = new JsonArray();
            for (String partColName : partColumnNames) {
                jPartColumnNames.add(partColName);
            }
            jsonObject.add(JSON_KEY_PART_COLUMN_NAMES, jPartColumnNames);
        }
        if (!dataColumnNames.isEmpty()) {
            JsonArray jDataColumnNames = new JsonArray();
            for (String dataColumnName : dataColumnNames) {
                jDataColumnNames.add(dataColumnName);
            }
            jsonObject.add(JSON_KEY_DATA_COLUMN_NAMES, jDataColumnNames);
        }
        if (!hudiProperties.isEmpty()) {
            JsonObject jHudiProperties = new JsonObject();
            for (Map.Entry<String, String> entry : hudiProperties.entrySet()) {
                jHudiProperties.addProperty(entry.getKey(), entry.getValue());
            }
            jsonObject.add(JSON_KEY_HUDI_PROPERTIES, jHudiProperties);
        }
        Text.writeString(out, jsonObject.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        String json = Text.readString(in);
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        hiveDbName = jsonObject.getAsJsonPrimitive(JSON_KEY_HUDI_DB).getAsString();
        hiveTableName = jsonObject.getAsJsonPrimitive(JSON_KEY_HUDI_TABLE).getAsString();
        if (jsonObject.has(JSON_KEY_RESOURCE_NAME)) {
            resourceName = jsonObject.getAsJsonPrimitive(JSON_KEY_RESOURCE_NAME).getAsString();
        }
        if (jsonObject.has(JSON_KEY_PART_COLUMN_NAMES)) {
            JsonArray jPartColumnNames = jsonObject.getAsJsonArray(JSON_KEY_PART_COLUMN_NAMES);
            for (int i = 0; i < jPartColumnNames.size(); i++) {
                partColumnNames.add(jPartColumnNames.get(i).getAsString());
            }
        }
        if (jsonObject.has(JSON_KEY_HUDI_PROPERTIES)) {
            JsonObject jHudiProperties = jsonObject.getAsJsonObject(JSON_KEY_HUDI_PROPERTIES);
            for (Map.Entry<String, JsonElement> entry : jHudiProperties.entrySet()) {
                hudiProperties.put(entry.getKey(), entry.getValue().getAsString());
            }
        }
        if (jsonObject.has(JSON_KEY_DATA_COLUMN_NAMES)) {
            JsonArray jDataColumnNames = jsonObject.getAsJsonArray(JSON_KEY_DATA_COLUMN_NAMES);
            for (int i = 0; i < jDataColumnNames.size(); i++) {
                dataColumnNames.add(jDataColumnNames.get(i).getAsString());
            }
        } else {
            // In order to be compatible with the case where JSON_KEY_DATA_COLUMN_NAMES does not exist.
            // Just put (full schema - partition columns) to dataColumnNames.
            // But there may be errors, because fullSchema may not store all the non-partition columns of the hive table
            // and the order may be inconsistent with that in hive

            // full schema - partition columns = data columns
            HashSet<String> partColumnSet = new HashSet<>(partColumnNames);
            for (Column col : fullSchema) {
                if (!partColumnSet.contains(col.getName())) {
                    dataColumnNames.add(col.getName());
                }
            }
        }
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        if (isResourceMappingCatalog(getCatalogName())) {
            GlobalStateMgr.getCurrentState().getMetadataMgr().dropTable(getCatalogName(), db.getFullName(), name);
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HudiTable{");
        sb.append("resourceName='").append(resourceName).append('\'');
        sb.append(", catalogName='").append(catalogName).append('\'');
        sb.append(", hiveDbName='").append(hiveDbName).append('\'');
        sb.append(", hiveTableName='").append(hiveTableName).append('\'');
        sb.append(", id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", createTime=").append(createTime);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getCatalogName(), hiveDbName, getTableIdentifier());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HudiTable)) {
            return false;
        }

        HudiTable otherTable = (HudiTable) other;
        String catalogName = getCatalogName();
        String tableIdentifier = getTableIdentifier();
        return Objects.equal(catalogName, otherTable.getCatalogName()) &&
                Objects.equal(hiveDbName, otherTable.hiveDbName) &&
                Objects.equal(tableIdentifier, otherTable.getTableIdentifier());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long id;
        private String tableName;
        private String catalogName;
        private String hiveDbName;
        private String hiveTableName;
        private String resourceName;
        private long createTime;
        private List<Column> fullSchema;
        private List<String> partitionColNames = Lists.newArrayList();
        private List<String> dataColNames = Lists.newArrayList();
        private Map<String, String> hudiProperties = Maps.newHashMap();

        public Builder() {
        }

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setCatalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public Builder setHiveDbName(String hiveDbName) {
            this.hiveDbName = hiveDbName;
            return this;
        }

        public Builder setHiveTableName(String hiveTableName) {
            this.hiveTableName = hiveTableName;
            return this;
        }

        public Builder setResourceName(String resourceName) {
            this.resourceName = resourceName;
            return this;
        }

        public Builder setCreateTime(long createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder setFullSchema(List<Column> fullSchema) {
            this.fullSchema = fullSchema;
            return this;
        }

        public Builder setPartitionColNames(List<String> partitionColNames) {
            this.partitionColNames = partitionColNames;
            return this;
        }

        public Builder setDataColNames(List<String> dataColNames) {
            this.dataColNames = dataColNames;
            return this;
        }

        public Builder setHudiProperties(Map<String, String> hudiProperties) {
            this.hudiProperties = hudiProperties;
            return this;
        }

        public HudiTable build() {
            return new HudiTable(id, tableName, catalogName, hiveDbName, hiveTableName, resourceName, fullSchema,
                    dataColNames, partitionColNames, createTime, hudiProperties);
        }
    }
}
