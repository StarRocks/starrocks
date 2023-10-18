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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/HiveTable.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class HiveTable extends Table implements HiveMetaStoreTable {
    private static final Logger LOG = LogManager.getLogger(HiveTable.class);

    private static final String JSON_KEY_HIVE_DB = "hiveDb";
    private static final String JSON_KEY_HIVE_TABLE = "hiveTable";
    private static final String JSON_KEY_RESOURCE_NAME = "resourceName";
    private static final String JSON_KEY_HDFS_PATH = "hdfsPath";
    private static final String JSON_KEY_PART_COLUMN_NAMES = "partColumnNames";
    private static final String JSON_KEY_DATA_COLUMN_NAMES = "dataColumnNames";
    private static final String JSON_KEY_HIVE_PROPERTIES = "hiveProperties";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    private String catalogName;
    @SerializedName(value = "dn")
    private String hiveDbName;
    @SerializedName(value = "tn")
    private String hiveTableName;
    @SerializedName(value = "rn")
    private String resourceName;
    @SerializedName(value = "tl")
    private String tableLocation;
    @SerializedName(value = "pcn")
    private List<String> partColumnNames = Lists.newArrayList();
    // dataColumnNames stores all the non-partition columns of the hive table,
    // consistent with the order defined in the hive table
    @SerializedName(value = "dcn")
    private List<String> dataColumnNames = Lists.newArrayList();
    @SerializedName(value = "prop")
    private Map<String, String> hiveProperties = Maps.newHashMap();

    // For `insert into target_table select from hive_table, we set it to false when executing this kind of insert query.
    // 1. `useMetadataCache` is false means that this query need to list all selected partitions files from hdfs/s3.
    // 2. Insert into statement could ignore the additional overhead caused by list partitions.
    // 3. The most import point is that query result may be wrong with cached and expired partition files, causing insert data is wrong.
    // This error will happen when appending files to an existed partition on user side.
    private boolean useMetadataCache = true;

    private HiveStorageFormat storageFormat;

    public HiveTable() {
        super(TableType.HIVE);
    }

    public HiveTable(long id, String name, List<Column> fullSchema, String resourceName, String catalog,
                     String hiveDbName, String hiveTableName, String tableLocation, long createTime,
                     List<String> partColumnNames, List<String> dataColumnNames, Map<String, String> properties,
                     HiveStorageFormat storageFormat) {
        super(id, name, TableType.HIVE, fullSchema);
        this.resourceName = resourceName;
        this.catalogName = catalog;
        this.hiveDbName = hiveDbName;
        this.hiveTableName = hiveTableName;
        this.tableLocation = tableLocation;
        this.createTime = createTime;
        this.partColumnNames = partColumnNames;
        this.dataColumnNames = dataColumnNames;
        this.hiveProperties = properties;
        this.storageFormat = storageFormat;
    }

    public String getHiveDbTable() {
        return String.format("%s.%s", hiveDbName, hiveTableName);
    }

    @Override
    public String getResourceName() {
        return resourceName;
    }

    @Override
    public String getCatalogName() {
        return catalogName == null ? getResourceMappingCatalogName(resourceName, "hive") : catalogName;
    }

    public String getDbName() {
        return hiveDbName;
    }

    @Override
    public String getTableName() {
        return hiveTableName;
    }

    public HiveStorageFormat getStorageFormat() {
        return storageFormat;
    }

    public boolean isUseMetadataCache() {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isEnableHiveMetadataCacheWithInsert()) {
            return true;
        } else {
            return useMetadataCache;
        }
    }

    public void useMetadataCache(boolean useMetadataCache) {
        if (!isResourceMappingCatalog(getCatalogName())) {
            this.useMetadataCache = useMetadataCache;
        }
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

    public String getTableLocation() {
        return this.tableLocation;
    }

    @Override
    public String getTableIdentifier() {
        return Joiner.on(":").join(name, createTime);
    }

    @Override
    public Map<String, String> getProperties() {
        // The user may alter the resource properties
        // So we do this to get the fresh properties
        Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
        if (resource != null) {
            HiveResource hiveResource = (HiveResource) resource;
            hiveProperties.put(HIVE_METASTORE_URIS, hiveResource.getHiveMetastoreURIs());
        }
        return hiveProperties == null ? new HashMap<>() : hiveProperties;
    }

    public boolean hasBooleanTypePartitionColumn() {
        return getPartitionColumns().stream().anyMatch(column -> column.getType().isBoolean());
    }

    public void modifyTableSchema(String dbName, String tableName, HiveTable updatedTable) {
        ImmutableList.Builder<Column> fullSchemaTemp = ImmutableList.builder();
        ImmutableMap.Builder<String, Column> nameToColumnTemp = ImmutableMap.builder();
        ImmutableList.Builder<String> dataColumnNamesTemp = ImmutableList.builder();


        updatedTable.nameToColumn.forEach((colName, column) -> {
            Column baseColumn = nameToColumn.get(colName);
            if (baseColumn != null) {
                column.setComment(baseColumn.getComment());
            }
        });

        fullSchemaTemp.addAll(updatedTable.fullSchema);
        nameToColumnTemp.putAll(updatedTable.nameToColumn);
        dataColumnNamesTemp.addAll(updatedTable.dataColumnNames);

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new StarRocksConnectorException("Not found database " + dbName);
        }

        db.writeLock();
        try {
            this.fullSchema.clear();
            this.nameToColumn.clear();
            this.dataColumnNames.clear();

            this.fullSchema.addAll(fullSchemaTemp.build());
            this.nameToColumn.putAll(nameToColumnTemp.build());
            this.dataColumnNames.addAll(dataColumnNamesTemp.build());

            if (GlobalStateMgr.getCurrentState().isLeader()) {
                ModifyTableColumnOperationLog log = new ModifyTableColumnOperationLog(dbName, tableName, fullSchema);
                GlobalStateMgr.getCurrentState().getEditLog().logModifyTableColumn(log);
            }
        } finally {
            db.writeUnlock();
        }
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        THdfsTable tHdfsTable = new THdfsTable();
        tHdfsTable.setHdfs_base_dir(tableLocation);

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
        tHdfsTable.setColumns(tColumns);
        if (!tPartitionColumns.isEmpty()) {
            tHdfsTable.setPartition_columns(tPartitionColumns);
        }

        // partitions
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        for (ReferencedPartitionInfo partition : partitions) {
            partitionKeys.add(partition.getKey());
        }
        List<RemoteFileInfo> hivePartitions;
        try {
            useMetadataCache = true;
            hivePartitions = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getRemoteFileInfos(getCatalogName(), this, partitionKeys);
        } catch (StarRocksConnectorException e) {
            LOG.warn("table {} gets partition info failed.", name, e);
            return null;
        }

        for (int i = 0; i < hivePartitions.size(); i++) {
            ReferencedPartitionInfo info = partitions.get(i);
            PartitionKey key = info.getKey();
            long partitionId = info.getId();

            THdfsPartition tPartition = new THdfsPartition();
            tPartition.setFile_format(hivePartitions.get(i).getFormat().toThrift());

            List<LiteralExpr> keys = key.getKeys();
            tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));

            THdfsPartitionLocation tPartitionLocation = new THdfsPartitionLocation();
            tPartitionLocation.setPrefix_index(-1);
            tPartitionLocation.setSuffix(hivePartitions.get(i).getFullPath());
            tPartition.setLocation(tPartitionLocation);
            tHdfsTable.putToPartitions(partitionId, tPartition);
        }

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.HDFS_TABLE, fullSchema.size(),
                0, hiveTableName, hiveDbName);
        tTableDescriptor.setHdfsTable(tHdfsTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(JSON_KEY_HIVE_DB, hiveDbName);
        jsonObject.addProperty(JSON_KEY_HIVE_TABLE, hiveTableName);
        if (!Strings.isNullOrEmpty(resourceName)) {
            jsonObject.addProperty(JSON_KEY_RESOURCE_NAME, resourceName);
        }
        if (!Strings.isNullOrEmpty(tableLocation)) {
            jsonObject.addProperty(JSON_KEY_HDFS_PATH, tableLocation);
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
        if (!hiveProperties.isEmpty()) {
            JsonObject jHiveProperties = new JsonObject();
            for (Map.Entry<String, String> entry : hiveProperties.entrySet()) {
                jHiveProperties.addProperty(entry.getKey(), entry.getValue());
            }
            jsonObject.add(JSON_KEY_HIVE_PROPERTIES, jHiveProperties);
        }
        Text.writeString(out, jsonObject.toString());
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        if (GlobalStateMgr.getCurrentStateStarRocksMetaVersion() >= StarRocksFEMetaVersion.VERSION_3) {
            String json = Text.readString(in);
            JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
            hiveDbName = jsonObject.getAsJsonPrimitive(JSON_KEY_HIVE_DB).getAsString();
            hiveTableName = jsonObject.getAsJsonPrimitive(JSON_KEY_HIVE_TABLE).getAsString();
            if (jsonObject.has(JSON_KEY_RESOURCE_NAME)) {
                resourceName = jsonObject.getAsJsonPrimitive(JSON_KEY_RESOURCE_NAME).getAsString();
            }
            if (jsonObject.has(JSON_KEY_HDFS_PATH)) {
                tableLocation = jsonObject.getAsJsonPrimitive(JSON_KEY_HDFS_PATH).getAsString();
            }
            if (jsonObject.has(JSON_KEY_PART_COLUMN_NAMES)) {
                JsonArray jPartColumnNames = jsonObject.getAsJsonArray(JSON_KEY_PART_COLUMN_NAMES);
                for (int i = 0; i < jPartColumnNames.size(); i++) {
                    partColumnNames.add(jPartColumnNames.get(i).getAsString());
                }
            }
            if (jsonObject.has(JSON_KEY_HIVE_PROPERTIES)) {
                JsonObject jHiveProperties = jsonObject.getAsJsonObject(JSON_KEY_HIVE_PROPERTIES);
                for (Map.Entry<String, JsonElement> entry : jHiveProperties.entrySet()) {
                    hiveProperties.put(entry.getKey(), entry.getValue().getAsString());
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
        } else {
            hiveDbName = Text.readString(in);
            hiveTableName = Text.readString(in);
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String val = Text.readString(in);
                hiveProperties.put(key, val);
            }
        }
    }

    @Override
    public void onReload() {
        if (Config.enable_hms_events_incremental_sync && isResourceMappingCatalog(getCatalogName())) {
            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().registerTableFromResource(
                    String.join(".", getCatalogName(), hiveDbName, hiveTableName));
        }
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        if (Config.enable_hms_events_incremental_sync && isResourceMappingCatalog(getCatalogName())) {
            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().unRegisterTableFromResource(
                    String.join(".", getCatalogName(), hiveDbName, hiveTableName));
        }

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
        final StringBuilder sb = new StringBuilder("HiveTable{");
        sb.append("catalogName='").append(catalogName).append('\'');
        sb.append(", hiveDbName='").append(hiveDbName).append('\'');
        sb.append(", hiveTableName='").append(hiveTableName).append('\'');
        sb.append(", resourceName='").append(resourceName).append('\'');
        sb.append(", id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", createTime=").append(createTime);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public List<UniqueConstraint> getUniqueConstraints() {
        return uniqueConstraints;
    }

    @Override
    public List<ForeignKeyConstraint> getForeignKeyConstraints() {
        return foreignKeyConstraints;
    }

    @Override
    public boolean supportInsert() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getCatalogName(), hiveDbName, getTableIdentifier());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HiveTable)) {
            return false;
        }

        HiveTable otherTable = (HiveTable) other;
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
        private String tableLocation;
        private long createTime;
        private List<Column> fullSchema;
        private List<String> partitionColNames = Lists.newArrayList();
        private List<String> dataColNames = Lists.newArrayList();
        private Map<String, String> properties = Maps.newHashMap();
        private HiveStorageFormat storageFormat;

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

        public Builder setTableLocation(String tableLocation) {
            this.tableLocation = tableLocation;
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

        public Builder setDataColumnNames(List<String> dataColumnNames) {
            this.dataColNames = dataColumnNames;
            return this;
        }

        public Builder setPartitionColumnNames(List<String> partitionColumnNames) {
            this.partitionColNames = partitionColumnNames;
            return this;
        }

        public Builder setProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder setStorageFormat(HiveStorageFormat storageFormat) {
            this.storageFormat = storageFormat;
            return this;
        }

        public HiveTable build() {
            return new HiveTable(id, tableName, fullSchema, resourceName, catalogName, hiveDbName, hiveTableName,
                    tableLocation, createTime, partitionColNames, dataColNames, properties, storageFormat);
        }
    }
}