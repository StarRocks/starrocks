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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HivePartitionStats;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.external.hive.Utils;
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

/**
 * External hive table
 * At the very beginning, hive table is only designed for spark load, and property hive.metastore.uris is used to
 * record hive metastore uris.
 * But when hive table supports query and there is a lot of hive tables,
 * using hive.resource property is more convenient to change hive config.
 * So we still remains the hive.metastore.uris property for compatible, but hive table only set hive.metastore.uris
 * dose not support query.
 */
public class HiveTable extends Table {
    private static final Logger LOG = LogManager.getLogger(HiveTable.class);

    private static final String PROPERTY_MISSING_MSG =
            "Hive %s is null. Please add properties('%s'='xxx') when create table";
    private static final String JSON_KEY_HIVE_DB = "hiveDb";
    private static final String JSON_KEY_HIVE_TABLE = "hiveTable";
    private static final String JSON_KEY_RESOURCE_NAME = "resourceName";
    private static final String JSON_KEY_HDFS_PATH = "hdfsPath";
    private static final String JSON_KEY_PART_COLUMN_NAMES = "partColumnNames";
    private static final String JSON_KEY_HIVE_PROPERTIES = "hiveProperties";

    private static final String HIVE_DB = "database";
    private static final String HIVE_TABLE = "table";
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static final String HIVE_RESOURCE = "resource";

    private String hiveDb;
    private String hiveTable;
    private String resourceName;
    private String hdfsPath;
    private List<String> partColumnNames = Lists.newArrayList();
    private List<String> dataColumnNames = Lists.newArrayList();
    private Map<String, String> hiveProperties = Maps.newHashMap();

    public HiveTable() {
        super(TableType.HIVE);
    }

    public HiveTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.HIVE, schema);
        validate(properties);
    }

    public String getHiveDbTable() {
        return String.format("%s.%s", hiveDb, hiveTable);
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getHiveDb() {
        return hiveDb;
    }

    public String getHiveTable() {
        return hiveTable;
    }

    public List<Column> getPartitionColumns() {
        List<Column> partColumns = Lists.newArrayList();
        for (String columnName : partColumnNames) {
            partColumns.add(nameToColumn.get(columnName));
        }
        return partColumns;
    }

    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    public List<String> getDataColumnNames() {
        return dataColumnNames;
    }

    public String getHdfsPath() {
        return this.hdfsPath;
    }

    public Map<String, String> getHiveProperties() {
        return hiveProperties;
    }

    public Map<PartitionKey, Long> getPartitionKeys() throws DdlException {
        List<Column> partColumns = getPartitionColumns();
        return Catalog.getCurrentCatalog().getHiveRepository()
                .getPartitionKeys(resourceName, hiveDb, hiveTable, partColumns);
    }

    public HivePartition getPartition(PartitionKey partitionKey) throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository()
                .getPartition(resourceName, hiveDb, hiveTable, partitionKey);
    }

    public List<HivePartition> getPartitions(List<PartitionKey> partitionKeys)
            throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository()
                .getPartitions(resourceName, hiveDb, hiveTable, partitionKeys);
    }

    public HiveTableStats getTableStats() throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository().getTableStats(resourceName, hiveDb, hiveTable);
    }

    public List<HivePartitionStats> getPartitionsStats(List<PartitionKey> partitionKeys) throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository()
                .getPartitionsStats(resourceName, hiveDb, hiveTable, partitionKeys);
    }

    public Map<String, HiveColumnStats> getTableLevelColumnStats(List<String> columnNames) throws DdlException {
        // NOTE: Using allColumns as param to get column stats, we will get the best cache effect.
        List<String> allColumnNames = new ArrayList<>(this.nameToColumn.keySet());
        Map<String, HiveColumnStats> allColumnStats = Catalog.getCurrentCatalog().getHiveRepository()
                .getTableLevelColumnStats(resourceName, hiveDb, hiveTable, getPartitionColumns(), allColumnNames);
        Map<String, HiveColumnStats> result = Maps.newHashMapWithExpectedSize(columnNames.size());
        for (String columnName : columnNames) {
            result.put(columnName, allColumnStats.get(columnName));
        }
        return result;
    }

    public void refreshTableCache() throws DdlException {
        Catalog.getCurrentCatalog().getHiveRepository()
                .refreshTableCache(resourceName, hiveDb, hiveTable, getPartitionColumns(),
                        new ArrayList<>(nameToColumn.keySet()));
    }

    public void refreshPartCache(List<String> partNames) throws DdlException {
        Catalog.getCurrentCatalog().getHiveRepository()
                .refreshPartitionCache(resourceName, hiveDb, hiveTable, partNames);
    }

    /**
     * 1. get from table stats
     * 2. get from partition stats if table stats is missing
     */
    public long getRowCount() {
        // from table stats
        HiveTableStats tableStats = null;
        try {
            tableStats = getTableStats();
        } catch (DdlException e) {
            LOG.warn("table {} gets stats failed", name, e);
            return 0;
        }
        long numRows = tableStats.getNumRows();
        long tableTotalFileBytes = tableStats.getTotalFileBytes();
        if (numRows < 0 || tableTotalFileBytes <= 0 || (numRows == 0 && tableTotalFileBytes != 0)) {
            numRows = -1;
        }
        if (numRows != -1) {
            return numRows;
        }

        // from partition stats
        numRows = getPartitionStatsRowCount(null);
        if (numRows != -1) {
            return numRows;
        }

        return 0;
    }

    /**
     * Returns an estimated row count for the given number of file bytes. The row count is
     * extrapolated using the table-level row count and file bytes statistics.
     */
    public long getExtrapolatedRowCount(long totalPartitionFileBytes) {
        if (totalPartitionFileBytes == 0) {
            return 0;
        }
        if (totalPartitionFileBytes < 0) {
            return -1;
        }

        HiveTableStats tableStats = null;
        try {
            tableStats = getTableStats();
        } catch (DdlException e) {
            LOG.warn("table {} gets stats failed", name, e);
            return -1;
        }
        long numRows = tableStats.getNumRows();
        long totalFileBytes = tableStats.getTotalFileBytes();
        if (numRows < 0 || totalFileBytes <= 0 || (numRows == 0 && totalFileBytes != 0)) {
            return -1;
        }

        double bytesPerRow = totalFileBytes / (double) numRows;
        double extrapolatedNumRows = totalPartitionFileBytes / bytesPerRow;
        return Math.max(1, Math.round(extrapolatedNumRows));
    }

    /**
     * Computes and returns the number of rows scanned based on the per-partition row count stats
     * TODO: consider missing or corrupted partition stats
     */
    public long getPartitionStatsRowCount(List<PartitionKey> partitions) {
        if (partitions == null) {
            try {
                partitions = Lists.newArrayList(getPartitionKeys().keySet());
            } catch (DdlException e) {
                LOG.warn("table {} gets partitions failed.", name, e);
                return -1;
            }
        }
        if (partitions.isEmpty()) {
            return 0;
        }

        long numRows = -1;

        List<HivePartitionStats> partitionsStats = Lists.newArrayList();
        try {
            partitionsStats = getPartitionsStats(partitions);
        } catch (DdlException e) {
            LOG.warn("table {} gets partitions stats failed.", name, e);
        }


        for (int i = 0; i < partitionsStats.size(); i++) {
            long partNumRows = partitionsStats.get(i).getNumRows();
            long partTotalFileBytes = partitionsStats.get(i).getTotalFileBytes();
            // -1: missing stats
            if (partNumRows > -1) {
                if (numRows == -1) {
                    numRows = 0;
                }
                numRows += partNumRows;
            } else {
                LOG.debug("table {} partition {} stats abnormal. num rows: {}, total file bytes: {}",
                        name, partitions.get(i), partNumRows, partTotalFileBytes);
            }
        }
        return numRows;
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of hive table, "
                    + "they are: database, table and 'hive.metastore.uris'");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        hiveDb = copiedProps.get(HIVE_DB);
        if (Strings.isNullOrEmpty(hiveDb)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_DB, HIVE_DB));
        }
        copiedProps.remove(HIVE_DB);

        hiveTable = copiedProps.get(HIVE_TABLE);
        if (Strings.isNullOrEmpty(hiveTable)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_TABLE, HIVE_TABLE));
        }
        copiedProps.remove(HIVE_TABLE);

        // check hive properties
        // hive.metastore.uris or hive.resource must be set
        String hiveMetastoreUris = copiedProps.get(HIVE_METASTORE_URIS);
        String resourceName = copiedProps.get(HIVE_RESOURCE);
        if (Strings.isNullOrEmpty(hiveMetastoreUris) && Strings.isNullOrEmpty(resourceName)) {
            throw new DdlException("property " + HIVE_METASTORE_URIS + " or " + HIVE_RESOURCE + " must be set");
        }

        if (!Strings.isNullOrEmpty(hiveMetastoreUris)) {
            copiedProps.remove(HIVE_METASTORE_URIS);
            hiveProperties.put(HIVE_METASTORE_URIS, hiveMetastoreUris);
        }

        if (!Strings.isNullOrEmpty(resourceName)) {
            copiedProps.remove(HIVE_RESOURCE);
            Resource resource = Catalog.getCurrentCatalog().getResourceMgr().getResource(resourceName);
            if (resource == null) {
                throw new DdlException("hive resource [" + resourceName + "] not exists");
            }
            if (resource.getType() != ResourceType.HIVE) {
                throw new DdlException("resource [" + resourceName + "] is not hive resource");
            }
            HiveResource hiveResource = (HiveResource) resource;
            hiveProperties.put(HIVE_METASTORE_URIS, hiveResource.getHiveMetastoreURIs());
            this.resourceName = resourceName;

            // check column
            // 1. check column exists in hive table
            // 2. check column type mapping
            // 3. check hive partition column exists in table column list
            org.apache.hadoop.hive.metastore.api.Table hiveTable = Catalog.getCurrentCatalog().getHiveRepository()
                    .getTable(resourceName, this.hiveDb, this.hiveTable);
            List<FieldSchema> unPartHiveColumns = hiveTable.getSd().getCols();
            List<FieldSchema> partHiveColumns = hiveTable.getPartitionKeys();
            Map<String, FieldSchema> allHiveColumns = unPartHiveColumns.stream()
                    .collect(Collectors.toMap(FieldSchema::getName, fieldSchema -> fieldSchema));
            for (FieldSchema hiveColumn : partHiveColumns) {
                allHiveColumns.put(hiveColumn.getName(), hiveColumn);
            }
            for (Column column : this.fullSchema) {
                FieldSchema hiveColumn = allHiveColumns.get(column.getName());
                if (hiveColumn == null) {
                    throw new DdlException("column [" + column.getName() + "] not exists in hive");
                }
                Set<PrimitiveType> validColumnTypes = getValidColumnType(hiveColumn.getType());
                if (!validColumnTypes.contains(column.getPrimitiveType())) {
                    throw new DdlException("can not convert hive column type [" + hiveColumn.getType() + "] to " +
                            "starrocks type [" + column.getPrimitiveType() + "]");
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
        }

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
        }
    }

    private Set<PrimitiveType> getValidColumnType(String hiveType) {
        if (hiveType == null) {
            return Sets.newHashSet();
        }

        // for type with length, like char(10), we only check the type and ignore the length
        hiveType = Utils.getTypeKeyword(hiveType);
        String typeUpperCase = hiveType.toUpperCase();
        switch (typeUpperCase) {
            case "TINYINT":
                return Sets.newHashSet(PrimitiveType.TINYINT);
            case "SMALLINT":
                return Sets.newHashSet(PrimitiveType.SMALLINT);
            case "INT":
            case "INTEGER":
                return Sets.newHashSet(PrimitiveType.INT);
            case "BIGINT":
                return Sets.newHashSet(PrimitiveType.BIGINT);
            case "FLOAT":
                return Sets.newHashSet(PrimitiveType.FLOAT);
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return Sets.newHashSet(PrimitiveType.DOUBLE);
            case "DECIMAL":
            case "NUMERIC":
                return Sets.newHashSet(PrimitiveType.DECIMALV2, PrimitiveType.DECIMAL32, PrimitiveType.DECIMAL64,
                        PrimitiveType.DECIMAL128);
            case "TIMESTAMP":
                return Sets.newHashSet(PrimitiveType.DATETIME);
            case "DATE":
                return Sets.newHashSet(PrimitiveType.DATE);
            case "STRING":
            case "VARCHAR":
            case "BINARY":
                return Sets.newHashSet(PrimitiveType.VARCHAR);
            case "CHAR":
                return Sets.newHashSet(PrimitiveType.CHAR);
            case "BOOLEAN":
                return Sets.newHashSet(PrimitiveType.BOOLEAN);
            default:
                return Sets.newHashSet();
        }
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
            keys.forEach(v -> v.setUseVectorized(true));
            tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));

            THdfsPartitionLocation tPartitionLocation = new THdfsPartitionLocation();
            tPartitionLocation.setPrefix_index(-1);
            tPartitionLocation.setSuffix(hivePartitions.get(i).getFullPath());
            tPartition.setLocation(tPartitionLocation);
            tHdfsTable.putToPartitions(partitionId, tPartition);
        }

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.HDFS_TABLE, fullSchema.size(),
                0, hiveTable, hiveDb);
        tTableDescriptor.setHdfsTable(tHdfsTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(JSON_KEY_HIVE_DB, hiveDb);
        jsonObject.addProperty(JSON_KEY_HIVE_TABLE, hiveTable);
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

        if (Catalog.getCurrentCatalogStarRocksJournalVersion() >= StarRocksFEMetaVersion.VERSION_3) {
            String json = Text.readString(in);
            JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
            hiveDb = jsonObject.getAsJsonPrimitive(JSON_KEY_HIVE_DB).getAsString();
            hiveTable = jsonObject.getAsJsonPrimitive(JSON_KEY_HIVE_TABLE).getAsString();
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
            {
                // full schema - partition columns = data columns
                HashSet<String> partColumnSet = new HashSet(partColumnNames);
                for (Column col : fullSchema) {
                    if (!partColumnSet.contains(col.getName())) {
                        dataColumnNames.add(col.getName());
                    }
                }
            }
        } else {
            hiveDb = Text.readString(in);
            hiveTable = Text.readString(in);
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String val = Text.readString(in);
                hiveProperties.put(key, val);
            }
        }
    }

    @Override
    public void onDrop() {
        if (this.resourceName != null) {
            Catalog.getCurrentCatalog().getHiveRepository().
                    clearCache(this.resourceName, this.hiveDb, this.hiveTable);
        }
    }
}
