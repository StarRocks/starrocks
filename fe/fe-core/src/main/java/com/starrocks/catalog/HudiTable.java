// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HivePartitionStats;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.THudiTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
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

public class HudiTable extends Table {
    private static final Logger LOG = LogManager.getLogger(HudiTable.class);

    private static final String PROPERTY_MISSING_MSG =
            "Hudi %s is null. Please add properties('%s'='xxx') when create table";
    private static final String JSON_KEY_HUDI_DB = "database";
    private static final String JSON_KEY_HUDI_TABLE = "table";
    private static final String JSON_KEY_RESOURCE_NAME = "resource";
    private static final String JSON_KEY_PART_COLUMN_NAMES = "partColumnNames";
    private static final String JSON_KEY_DATA_COLUMN_NAMES = "dataColumnNames";
    private static final String JSON_KEY_HUDI_PROPERTIES = "hudiProperties";

    private static final String HUDI_TABLE_TYPE = "hudi.table.type";
    private static final String HUDI_TABLE_PRIMARY_KEY = "hudi.table.primaryKey";
    private static final String HUDI_TABLE_PRE_COMBINE_FIELD = "hudi.table.preCombineField";
    private static final String HUDI_MATADATA_ENABLE = "hudi.metadata.enable";
    private static final String HUDI_METASTORE_URIS = "hive.metastore.uris";
    private static final String HUDI_BASE_PATH = "hudi.table.base.path";
    private static final String HUDI_TABLE_INPUT_FORMAT = "hudi.table.input.format";
    private static final String HUDI_DB = "database";
    private static final String HUDI_TABLE = "table";
    private static final String HUDI_RESOURCE = "resource";

    private String db;
    private String table;
    private String resourceName;

    private List<String> partColumnNames = Lists.newArrayList();
    // dataColumnNames stores all the non-partition columns of the hive table,
    // consistent with the order defined in the hive table
    private List<String> dataColumnNames = Lists.newArrayList();

    private Map<String, String> hudiProperties = Maps.newHashMap();

    public HudiTable() {
        super(TableType.HUDI);
    }

    public HudiTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.HUDI, schema);
        validate(properties);
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
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

    public Map<PartitionKey, Long> getPartitionKeys() throws DdlException {
        List<Column> partColumns = getPartitionColumns();
        return Catalog.getCurrentCatalog().getHiveRepository()
                .getPartitionKeys(resourceName, db, table, partColumns);
    }

    public List<HivePartition> getPartitions(List<PartitionKey> partitionKeys)
            throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository()
                .getHudiPartitions(resourceName, db, table, partitionKeys);
    }

    public String getResourceName() {
        return resourceName;
    }

    public HoodieTableType getTableType() {
        return HoodieTableType.valueOf(hudiProperties.get(HUDI_TABLE_TYPE));
    }

    public String getHudiBasePath() {
        return hudiProperties.get(HUDI_BASE_PATH);
    }

    public Map<String, HiveColumnStats> getTableLevelColumnStats(List<String> columnNames) throws DdlException {
        // NOTE: Using allColumns as param to get column stats, we will get the best cache effect.
        List<String> allColumnNames = new ArrayList<>(this.nameToColumn.keySet());
        Map<String, HiveColumnStats> allColumnStats = Catalog.getCurrentCatalog().getHiveRepository()
                .getTableLevelColumnStats(resourceName, db, table, getPartitionColumns(), allColumnNames);
        Map<String, HiveColumnStats> result = Maps.newHashMapWithExpectedSize(columnNames.size());
        for (String columnName : columnNames) {
            result.put(columnName, allColumnStats.get(columnName));
        }
        return result;
    }

    public HiveTableStats getTableStats() throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository().getTableStats(resourceName, db, table);
    }

    public List<HivePartitionStats> getPartitionsStats(List<PartitionKey> partitionKeys) throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository()
                .getPartitionsStats(resourceName, db, table, partitionKeys);
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
                LOG.warn("Failed to get table {} partitions.", name, e);
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
            LOG.warn("Failed to get table {} partitions stats.", name, e);
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
                LOG.debug("Table {} partition {} stats is invalid. num rows: {}, total file bytes: {}",
                        name, partitions.get(i), partNumRows, partTotalFileBytes);
            }
        }
        return numRows;
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of hudi table.");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        this.db = copiedProps.remove(HUDI_DB);
        if (Strings.isNullOrEmpty(db)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HUDI_DB, HUDI_DB));
        }

        this.table = copiedProps.remove(HUDI_TABLE);
        if (Strings.isNullOrEmpty(table)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HUDI_TABLE, HUDI_TABLE));
        }

        String hudiMetaDataEnable = copiedProps.get(HUDI_MATADATA_ENABLE);
        if (!Strings.isNullOrEmpty(hudiMetaDataEnable)) {
            copiedProps.remove(HUDI_MATADATA_ENABLE);
            hudiProperties.put(HUDI_MATADATA_ENABLE, hudiMetaDataEnable);
        } else {
            hudiProperties.put(HUDI_MATADATA_ENABLE, "false");
        }

        String resourceName = copiedProps.remove(HUDI_RESOURCE);
        Resource resource = Catalog.getCurrentCatalog().getResourceMgr().getResource(resourceName);
        HudiResource hudiResource = (HudiResource) resource;
        if (hudiResource == null) {
            throw new DdlException("Hudi resource [" + resourceName + "] does NOT exists");
        }
        if (hudiResource.getType() != Resource.ResourceType.HUDI) {
            throw new DdlException("Resource [" + resourceName + "] is not hudi resource");
        }
        hudiProperties.put(HUDI_METASTORE_URIS, hudiResource.getHiveMetastoreURIs());

        org.apache.hadoop.hive.metastore.api.Table metastoreTable = Catalog.getCurrentCatalog().getHiveRepository()
                .getTable(resourceName, this.db, this.table);
        String metastoreTableType = metastoreTable.getTableType();
        if (metastoreTableType == null) {
            throw new DdlException("Unknown metastore table type.");
        }
        switch (metastoreTableType) {
            case "VIRTUAL_VIEW":
                throw new DdlException("VIRTUAL_VIEW table is not supported.");
            case "EXTERNAL_TABLE":
            case "MANAGED_TABLE":
                break;
            default:
                throw new DdlException("unsupported hudi table type [" + metastoreTableType + "].");
        }

        this.resourceName = resourceName;

        String hudiBasePath = metastoreTable.getSd().getLocation();
        if (!Strings.isNullOrEmpty(hudiBasePath)) {
            hudiProperties.put(HUDI_BASE_PATH, hudiBasePath);
        }

        String hudiTableType = metastoreTable.getParameters().get("type");
        if (!Strings.isNullOrEmpty(hudiTableType)) {
            hudiProperties.put(HUDI_TABLE_TYPE, hudiTableType);
        }

        String hudiTablePrimaryKey = metastoreTable.getParameters().get("primaryKey");
        if (!Strings.isNullOrEmpty(hudiTablePrimaryKey)) {
            hudiProperties.put(HUDI_TABLE_PRIMARY_KEY, hudiTablePrimaryKey);
        }

        String hudiTablePreCombineField = metastoreTable.getParameters().get("preCombineField");
        if (!Strings.isNullOrEmpty(hudiTablePreCombineField)) {
            hudiProperties.put(HUDI_TABLE_PRE_COMBINE_FIELD, hudiTablePreCombineField);
        }

        String hudiInputFormat = metastoreTable.getSd().getInputFormat();
        if (!Strings.isNullOrEmpty(hudiInputFormat)) {
            hudiProperties.put(HUDI_TABLE_INPUT_FORMAT, hudiInputFormat);
        }

        List<FieldSchema> unPartHiveColumns = metastoreTable.getSd().getCols();
        List<FieldSchema> partHiveColumns = metastoreTable.getPartitionKeys();
        for (FieldSchema partHiveColumn : partHiveColumns) {
            String columnName = partHiveColumn.getName();
            Column partColumn = this.nameToColumn.get(columnName);
            if (partColumn == null) {
                throw new DdlException("Partition column [" + columnName + "] must exist in column list");
            } else {
                this.partColumnNames.add(columnName);
            }
        }

        for (FieldSchema s : unPartHiveColumns) {
            this.dataColumnNames.add(s.getName());
        }

        Configuration conf = new Configuration();
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(conf).setBasePath(hudiBasePath).build();
        TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
        Schema tableSchema;
        try {
            tableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchemaWithoutMetadataFields());
        } catch (Exception e) {
            throw new DdlException("Cannot get hudi table schema.");
        }
        for (Column column : this.fullSchema) {
            if (!column.isAllowNull()) {
                throw new DdlException(
                        "Hudi extern table does not support no-nullable column: [" + column.getName() + "]");
            }
            Schema.Field hudiColumn = tableSchema.getField(column.getName());
            if (hudiColumn == null) {
                throw new DdlException("Column [" + column.getName() + "] not exists in hudi.");
            }
            Schema columnSchema = hudiColumn.schema().getTypes().get(1);
            if (!validColumnType(columnSchema, column.getType())) {
                Schema.Type hudiType = columnSchema.getType();
                throw new DdlException("Can not convert hudi column type [" + hudiType + "] to " +
                        "starrocks type [" + column.getPrimitiveType() + "], column name: " + column.getName());
            }
        }

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
        }
    }

    private boolean validColumnType(Schema columnSchema, Type type) {
        Schema.Type columnType = columnSchema.getType();
        if (columnType == null) {
            return false;
        }

        PrimitiveType primitiveType = type.getPrimitiveType();
        switch (columnType) {
            case BOOLEAN:
                return primitiveType == PrimitiveType.BOOLEAN;
            case INT:
                return primitiveType == PrimitiveType.INT ||
                        primitiveType == PrimitiveType.TINYINT ||
                        primitiveType == PrimitiveType.SMALLINT;
            case LONG:
                return primitiveType == PrimitiveType.BIGINT;
            case FLOAT:
                return primitiveType == PrimitiveType.FLOAT;
            case DOUBLE:
                return primitiveType == PrimitiveType.DOUBLE;
            case STRING:
                return primitiveType == PrimitiveType.VARCHAR ||
                        primitiveType == PrimitiveType.CHAR;
            case ARRAY:
                return validColumnType(columnSchema.getElementType(), ((ArrayType) type).getItemType());
            case FIXED:
            case ENUM:
            case UNION:
            case MAP:
            case BYTES:
            default:
                return false;
        }
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        THudiTable tHudiTable = new THudiTable();
        tHudiTable.setLocation(getHudiBasePath());

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
        List<HivePartition> hudiPartitions;
        try {
            hudiPartitions = getPartitions(partitionKeys);
        } catch (DdlException e) {
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

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.HUDI_TABLE,
                fullSchema.size(), 0, table, db);
        tTableDescriptor.setHudiTable(tHudiTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(JSON_KEY_HUDI_DB, db);
        jsonObject.addProperty(JSON_KEY_HUDI_TABLE, table);
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

        if (Catalog.getCurrentCatalogStarRocksJournalVersion() >= StarRocksFEMetaVersion.VERSION_CURRENT) {
            String json = Text.readString(in);
            JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
            db = jsonObject.getAsJsonPrimitive(JSON_KEY_HUDI_DB).getAsString();
            table = jsonObject.getAsJsonPrimitive(JSON_KEY_HUDI_TABLE).getAsString();
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
    }
}
