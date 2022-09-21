// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Resource.ResourceType;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.external.ColumnTypeConverter;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HiveRepository;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.common.util.Util.validateMetastoreUris;
import static com.starrocks.external.HiveMetaStoreTableUtils.isInternalCatalog;

/**
 * External hive table
 * At the very beginning, hive table is only designed for spark load, and property hive.metastore.uris is used to
 * record hive metastore uris.
 * But when hive table supports query and there is a lot of hive tables,
 * using hive.resource property is more convenient to change hive config.
 * So we still remains the hive.metastore.uris property for compatible, but hive table only set hive.metastore.uris
 * dose not support query.
 */
public class HiveTable extends Table implements HiveMetaStoreTable {
    private static final Logger LOG = LogManager.getLogger(HiveTable.class);

    private static final String PROPERTY_MISSING_MSG =
            "Hive %s is null. Please add properties('%s'='xxx') when create table";
    private static final String JSON_KEY_HIVE_DB = "hiveDb";
    private static final String JSON_KEY_HIVE_TABLE = "hiveTable";
    private static final String JSON_KEY_RESOURCE_NAME = "resourceName";
    private static final String JSON_KEY_HDFS_PATH = "hdfsPath";
    private static final String JSON_KEY_PART_COLUMN_NAMES = "partColumnNames";
    private static final String JSON_KEY_DATA_COLUMN_NAMES = "dataColumnNames";
    private static final String JSON_KEY_HIVE_PROPERTIES = "hiveProperties";
    private static final String JSON_KEY_SR_DB_NAME = "srDbName";

    public static final String HIVE_DB = "database";
    public static final String HIVE_TABLE = "table";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_RESOURCE = "resource";

    private String catalogName;
    private String hiveDbName;
    private String hiveTableName;
    private String resourceName;
    private String hdfsPath;
    private List<String> partColumnNames = Lists.newArrayList();
    // dataColumnNames stores all the non-partition columns of the hive table,
    // consistent with the order defined in the hive table
    private List<String> dataColumnNames = Lists.newArrayList();
    private Map<String, String> hiveProperties = Maps.newHashMap();

    private HiveMetaStoreTableInfo hmsTableInfo;

    private HiveRepository hiveRepository;

    public HiveTable() {
        super(TableType.HIVE);
        this.hiveRepository = GlobalStateMgr.getServingState().getHiveRepository();
    }

    public HiveTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        this(id, name, schema, properties, null);
    }

    public HiveTable(long id, String name, List<Column> schema, Map<String, String> properties,
                     org.apache.hadoop.hive.metastore.api.Table hiveTable) throws DdlException {
        super(id, name, TableType.HIVE, schema);
        this.hiveRepository = GlobalStateMgr.getCurrentState().getHiveRepository();
        validate(properties, hiveTable);
        initHmsTableInfo();
    }

    public HiveTable(long id, String name, List<Column> fullSchema, String resourceName, String catalog,
                     String hiveDbName, String hiveTableName, String hdfsPath, List<String> partColumnNames,
                     List<String> dataColumnNames, Map<String, String> properties) {
        super(id, name, TableType.HIVE, fullSchema);
        this.resourceName = resourceName;
        this.catalogName = catalog;
        this.hiveDbName = hiveDbName;
        this.hiveTableName = hiveTableName;
        this.hdfsPath = hdfsPath;
        this.partColumnNames = partColumnNames;
        this.dataColumnNames = dataColumnNames;
        this.hiveProperties = properties;
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
        return catalogName == null ? CatalogMgr.RESOURCE_MAPPING_CATALOG_PREFIX + resourceName : catalogName;
    }

    public String getDbName() {
        return hiveDbName;
    }

    @Override
    public String getTableName() {
        return hiveTableName;
    }

    public HiveMetaStoreTableInfo getHmsTableInfo() {
        return hmsTableInfo;
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

    public String getHdfsPath() {
        return this.hdfsPath;
    }

    @Override
    public String getTableIdentifier() {
        return Joiner.on(":").join(name, createTime);
    }

    public Map<String, String> getHiveProperties() {
        // The user may alter the resource properties
        // So we do this to get the fresh properties
        Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
        if (resource != null) {
            HiveResource hiveResource = (HiveResource) resource;
            hiveProperties.put(HIVE_METASTORE_URIS, hiveResource.getHiveMetastoreURIs());
        }
        return hiveProperties;
    }

    public void initHmsTableInfo() {
        if (hmsTableInfo == null) {
            hmsTableInfo = new HiveMetaStoreTableInfo(resourceName, hiveDbName, hiveTableName,
                    partColumnNames, dataColumnNames, nameToColumn, type);
        }
    }

    public Map<PartitionKey, Long> getPartitionKeys() throws DdlException {
        return HiveMetaStoreTableUtils.getPartitionKeys(hmsTableInfo);
    }

    @Override
    public List<HivePartition> getPartitions(List<PartitionKey> partitionKeys) throws DdlException {
        return HiveMetaStoreTableUtils.getPartitions(hmsTableInfo, partitionKeys);
    }

    @Override
    public HiveTableStats getTableStats() throws DdlException {
        return HiveMetaStoreTableUtils.getTableStats(hmsTableInfo);
    }

    @Override
    public Map<String, HiveColumnStats> getTableLevelColumnStats(List<String> columnNames) throws DdlException {
        return HiveMetaStoreTableUtils.getTableLevelColumnStats(hmsTableInfo, columnNames);
    }

    @Override
    public void refreshTableCache(String dbName, String tableName) throws DdlException {
        org.apache.hadoop.hive.metastore.api.Table table;
        try {
            table = hiveRepository.getTable(resourceName, this.hiveDbName, this.hiveTableName);
            List<FieldSchema> updatedTableSchemas = HiveMetaStoreTableUtils.getAllHiveColumns(table);
            boolean needRefreshColumn = isRefreshColumn(updatedTableSchemas);

            if (!needRefreshColumn) {
                refreshTableCache(nameToColumn);
            } else {
                if (HiveMetaStoreTableUtils.isInternalCatalog(resourceName)) {
                    modifyTableSchema(dbName, tableName, table.getSd().getCols(), updatedTableSchemas);
                    refreshTableCache(nameToColumn);
                } else {
                    hiveRepository.refreshConnectorTable(resourceName, hiveDbName, hiveTableName);
                }
                this.hmsTableInfo = new HiveMetaStoreTableInfo(resourceName, hiveDbName, hiveTableName,
                        partColumnNames, dataColumnNames, nameToColumn, type);
            }
        } catch (Exception e) {
            hiveRepository.clearCache(hmsTableInfo);
            LOG.warn("Failed to refresh [{}.{}.{}]. Invalidate all cache on it",
                    resourceName, hiveDbName, hiveTableName);
            throw new DdlException(e.getMessage());
        }
    }

    private void refreshTableCache(Map<String, Column> nameToColumn) throws DdlException {
        HiveMetaStoreTableInfo hmsTableInfo = new HiveMetaStoreTableInfo(resourceName, hiveDbName, hiveTableName,
                partColumnNames, new ArrayList<>(nameToColumn.keySet()), nameToColumn, TableType.HIVE);
        hiveRepository.refreshTableCache(hmsTableInfo);
    }

    public boolean isRefreshColumn(List<FieldSchema> tableSchemas) throws DdlException {
        Map<String, FieldSchema> updatedTableSchemas = tableSchemas.stream()
                .collect(Collectors.toMap(FieldSchema::getName, fieldSchema -> fieldSchema));
        boolean needRefreshColumn = updatedTableSchemas.size() != nameToColumn.size();
        if (!needRefreshColumn) {
            for (Column column : nameToColumn.values()) {
                FieldSchema fieldSchema = updatedTableSchemas.get(column.getName());
                if (fieldSchema == null) {
                    needRefreshColumn = true;
                    break;
                }
                Type type = ColumnTypeConverter.fromHiveType(fieldSchema.getType());
                if (!type.equals(column.getType())) {
                    needRefreshColumn = true;
                    break;
                }
            }
        }
        return needRefreshColumn;
    }

    private void modifyTableSchema(String dbName, String tableName,
                                    List<FieldSchema> unpartHiveCols, List<FieldSchema> allHiveColumns)
            throws DdlException {
        ImmutableList.Builder<Column> fullSchemaTemp = ImmutableList.builder();
        ImmutableMap.Builder<String, Column> nameToColumnTemp = ImmutableMap.builder();
        ImmutableList.Builder<String> dataColumnNamesTemp = ImmutableList.builder();

        // TODO: Column type conversion should not throw an exception, use invalidate type instead.
        for (FieldSchema fieldSchema : allHiveColumns) {
            Type srType = ColumnTypeConverter.fromHiveType(fieldSchema.getType());
            Column column = new Column(fieldSchema.getName(), srType, true);
            fullSchemaTemp.add(column);
            nameToColumnTemp.put(column.getName(), column);
        }

        unpartHiveCols.forEach(fieldSchema -> dataColumnNamesTemp.add(fieldSchema.getName()));
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("Not found database " + dbName);
        }

        db.writeLock();
        try {
            fullSchema.clear();
            nameToColumn.clear();
            dataColumnNames.clear();

            fullSchema.addAll(fullSchemaTemp.build());
            nameToColumn.putAll(nameToColumnTemp.build());
            dataColumnNames.addAll(dataColumnNamesTemp.build());

            if (GlobalStateMgr.getCurrentState().isLeader()) {
                ModifyTableColumnOperationLog log = new ModifyTableColumnOperationLog(dbName, tableName, fullSchema);
                GlobalStateMgr.getCurrentState().getEditLog().logModifyTableColumn(log);
            }
        } finally {
            db.writeUnlock();
        }
    }

    @Override
    public void refreshPartCache(List<String> partNames) throws DdlException {
        hiveRepository.refreshPartitionCache(hmsTableInfo, partNames);
    }

    @Override
    public void refreshTableColumnStats() throws DdlException {
        hiveRepository.refreshTableColumnStats(hmsTableInfo);
    }

    /**
     * Computes and returns the number of rows scanned based on the per-partition row count stats
     * TODO: consider missing or corrupted partition stats
     */
    @Override
    public long getPartitionStatsRowCount(List<PartitionKey> partitions) {
        return HiveMetaStoreTableUtils.getPartitionStatsRowCount(hmsTableInfo, partitions);
    }

    private void validate(Map<String, String> properties, org.apache.hadoop.hive.metastore.api.Table hiveTable)
            throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of hive table, "
                    + "they are: database, table and resource");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        hiveDbName = copiedProps.get(HIVE_DB);
        if (Strings.isNullOrEmpty(hiveDbName)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_DB, HIVE_DB));
        }
        copiedProps.remove(HIVE_DB);

        hiveTableName = copiedProps.get(HIVE_TABLE);
        if (Strings.isNullOrEmpty(hiveTableName)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_TABLE, HIVE_TABLE));
        }
        copiedProps.remove(HIVE_TABLE);

        // check hive properties
        // resource must be set and hive.metastore.uris will be ignored if specified.
        String hiveMetastoreUris = copiedProps.get(HIVE_METASTORE_URIS);
        String resourceName = copiedProps.get(HIVE_RESOURCE);
        if (Strings.isNullOrEmpty(resourceName)) {
            throw new DdlException("property " + HIVE_RESOURCE + " must be set");
        }

        if (!Strings.isNullOrEmpty(hiveMetastoreUris)) {
            validateMetastoreUris(hiveMetastoreUris);
            copiedProps.remove(HIVE_METASTORE_URIS);
            LOG.warn("property " + HIVE_METASTORE_URIS + " will be ignored " +
                    "and hive table will be created by using property " + HIVE_RESOURCE + " only.");
        }
        copiedProps.remove(HIVE_RESOURCE);

        if (isInternalCatalog(resourceName)) {
            checkResource(resourceName);
        }

        this.resourceName = resourceName;

        // check column
        // 1. check column exists in hive table
        // 2. check column type mapping
        // 3. check hive partition column exists in table column list
        if (hiveTable == null) {
            hiveTable = hiveRepository.getTable(resourceName, this.hiveDbName, this.hiveTableName);
        }
        this.createTime = hiveTable.getCreateTime();

        String hiveTableType = hiveTable.getTableType();
        if (hiveTableType == null) {
            throw new DdlException("Unknown hive table type.");
        }
        switch (hiveTableType) {
            case "VIRTUAL_VIEW": // hive view table not supported
                throw new DdlException("Hive view table is not supported.");
            case "EXTERNAL_TABLE": // hive external table supported
            case "MANAGED_TABLE": // basic hive table supported
            case "MATERIALIZED_VIEW": // hive materialized view table supported
                break;
            default:
                throw new DdlException("unsupported hive table type [" + hiveTableType + "].");
        }
        List<FieldSchema> unPartHiveColumns = hiveTable.getSd().getCols();
        List<FieldSchema> partHiveColumns = hiveTable.getPartitionKeys();
        Map<String, FieldSchema> allHiveColumns = HiveMetaStoreTableUtils.getAllHiveColumns(hiveTable).stream()
                .collect(Collectors.toMap(FieldSchema::getName, fieldSchema -> fieldSchema));
        for (Column column : this.fullSchema) {
            FieldSchema hiveColumn = allHiveColumns.get(column.getName());
            if (hiveColumn == null) {
                throw new DdlException("column [" + column.getName() + "] not exists in hive");
            }
            // Only internal catalog like hive external table need to validate column type
            if (HiveMetaStoreTableUtils.isInternalCatalog(resourceName) &&
                    !HiveMetaStoreTableUtils.validateColumnType(hiveColumn.getType(), column.getType()) &&
                    !FeConstants.runningUnitTest) {
                throw new DdlException("can not convert hive column type [" + hiveColumn.getType() + "] to " +
                        "starrocks type [" + column.getPrimitiveType() + "]");
            }
            if (!column.isAllowNull() && !isTypeRead) {
                throw new DdlException(
                        "hive extern table not support no-nullable column: [" + hiveColumn.getName() + "]");
            }
        }
        for (FieldSchema partHiveColumn : partHiveColumns) {
            String columnName = partHiveColumn.getName();
            Column partColumn = this.nameToColumn.get(columnName);
            if (partColumn == null) {
                throw new DdlException("partition column [" + columnName + "] must exist in column list");
            } else {
                this.partColumnNames.add(columnName);
            }
        }

        for (FieldSchema s : unPartHiveColumns) {
            this.dataColumnNames.add(s.getName());
        }

        // set hdfs path
        // todo hdfs ip may change, store it in cache?
        this.hdfsPath = hiveTable.getSd().getLocation();

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
        }
    }

    private void checkResource(String resourceName) throws DdlException {
        Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new DdlException("hive resource [" + resourceName + "] not exists");
        }
        if (resource.getType() != ResourceType.HIVE) {
            throw new DdlException("resource [" + resourceName + "] is not hive resource");
        }
        HiveResource hiveResource = (HiveResource) resource;
        hiveProperties.put(HIVE_METASTORE_URIS, hiveResource.getHiveMetastoreURIs());
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        THdfsTable tHdfsTable = new THdfsTable();
        tHdfsTable.setHdfs_base_dir(hdfsPath);

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
        List<HivePartition> hivePartitions;
        try {
            hivePartitions = getPartitions(partitionKeys);
        } catch (DdlException e) {
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
        if (!Strings.isNullOrEmpty(hdfsPath)) {
            jsonObject.addProperty(JSON_KEY_HDFS_PATH, hdfsPath);
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

        if (GlobalStateMgr.getCurrentStateStarRocksJournalVersion() >= StarRocksFEMetaVersion.VERSION_3) {
            String json = Text.readString(in);
            JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
            hiveDbName = jsonObject.getAsJsonPrimitive(JSON_KEY_HIVE_DB).getAsString();
            hiveTableName = jsonObject.getAsJsonPrimitive(JSON_KEY_HIVE_TABLE).getAsString();
            if (jsonObject.has(JSON_KEY_RESOURCE_NAME)) {
                resourceName = jsonObject.getAsJsonPrimitive(JSON_KEY_RESOURCE_NAME).getAsString();
            }
            if (jsonObject.has(JSON_KEY_HDFS_PATH)) {
                hdfsPath = jsonObject.getAsJsonPrimitive(JSON_KEY_HDFS_PATH).getAsString();
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
        initHmsTableInfo();
    }

    @Override
    public void onCreate() {
        hiveRepository.getCounter().add(resourceName, hiveDbName, hiveTableName);
        GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().registerTable(this);
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        if (hiveRepository.getCounter().reduce(resourceName, hiveDbName, hiveTableName) == 0) {
            hiveRepository.clearCache(hmsTableInfo);
            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().unregisterTable(this);
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long id;
        private String catalogName;
        private String hiveDbName;
        private String hiveTableName;
        private String resourceName;
        private String hdfsPath;
        private List<Column> fullSchema;
        private List<String> partitionColNames = Lists.newArrayList();
        private List<String> dataColNames = Lists.newArrayList();
        private Map<String, String> properties = Maps.newHashMap();

        public Builder() {
        }

        public Builder setId(long id) {
            this.id = id;
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

        public Builder setHdfsPath(String hdfsPath) {
            this.hdfsPath = hdfsPath;
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

        public HiveTable build() {
            return new HiveTable(id, hiveTableName, fullSchema, resourceName, catalogName, hiveDbName, hiveTableName,
                    hdfsPath, partitionColNames, dataColNames, properties);
        }
    }
}