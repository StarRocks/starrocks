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
import com.starrocks.common.io.Text;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.THudiTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
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

/**
 * Currently, we depend on Hive metastore to obtain table/partition path and statistics.
 * This logic should be decoupled from metastore when the related interfaces are ready.
 */
public class HudiTable extends Table implements HiveMetaStoreTable {
    private static final Logger LOG = LogManager.getLogger(HudiTable.class);

    public enum HoodieTableType {
        COW, MOR, UNKNOWN
    }

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
    private static final String HUDI_BASE_PATH = "hudi.table.base.path";
    private static final String HUDI_TABLE_BASE_FILE_FORMAT = "hudi.table.base.file.format";
    private static final String HUDI_DB = "database";
    private static final String HUDI_TABLE = "table";
    private static final String HUDI_RESOURCE = "resource";

    private String db;
    private String table;
    private String resourceName;

    private List<String> partColumnNames = Lists.newArrayList();
    // dataColumnNames stores all the non-partition columns of the hudi table,
    // consistent with the order defined in the hudi table
    private List<String> dataColumnNames = Lists.newArrayList();

    private Map<String, String> hudiProperties = Maps.newHashMap();

    private HiveMetaStoreTableInfo hmsTableInfo;

    public HudiTable() {
        super(TableType.HUDI);
    }

    public HudiTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.HUDI, schema);
        validate(properties);
        initHmsTableInfo();
    }

    public String getCatalogName() {
        return "catalogName";
    }

    public String getHiveDb() {
        return db;
    }

    public String getTable() {
        return table;
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

    @Override
    public String getTableName() {
        return table;
    }

    public static boolean isHudiTable(String inputFormat) {
        return HudiTable.fromInputFormat(inputFormat) != HudiTable.HoodieTableType.UNKNOWN;
    }

    public static HoodieTableType fromInputFormat(String inputFormat) {
        switch (inputFormat) {
            case "org.apache.hudi.hadoop.HoodieParquetInputFormat":
            case "com.uber.hoodie.hadoop.HoodieInputFormat":
                return HoodieTableType.COW;
            case "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat":
            case "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat":
                return HoodieTableType.MOR;
            default:
                return HoodieTableType.UNKNOWN;
        }
    }

    @Override
    public List<Column> getPartitionColumns() {
        return HiveMetaStoreTableUtils.getPartitionColumns(hmsTableInfo);
    }

    @Override
    public boolean isUnpartitioned() {
        return partColumnNames.size() == 0;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    public List<String> getDataColumnNames() {
        return dataColumnNames;
    }

    public void initHmsTableInfo() {
        if (hmsTableInfo == null) {
            hmsTableInfo = new HiveMetaStoreTableInfo(resourceName, db, table,
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
        GlobalStateMgr.getCurrentState().getHiveRepository().refreshTableCache(hmsTableInfo);
    }

    @Override
    public void refreshPartCache(List<String> partNames) throws DdlException {
        GlobalStateMgr.getCurrentState().getHiveRepository()
                .refreshPartitionCache(hmsTableInfo, partNames);
    }

    @Override
    public void refreshTableColumnStats() throws DdlException {
        GlobalStateMgr.getCurrentState().getHiveRepository()
                .refreshTableColumnStats(hmsTableInfo);
    }

    /**
     * Computes and returns the number of rows scanned based on the per-partition row count stats
     * TODO: consider missing or corrupted partition stats
     */
    @Override
    public long getPartitionStatsRowCount(List<PartitionKey> partitions) {
        return HiveMetaStoreTableUtils.getPartitionStatsRowCount(hmsTableInfo, partitions);
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

        String resourceName = copiedProps.remove(HUDI_RESOURCE);
        Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
        HudiResource hudiResource = (HudiResource) resource;
        if (hudiResource == null) {
            throw new DdlException("Hudi resource [" + resourceName + "] does NOT exists");
        }
        if (hudiResource.getType() != Resource.ResourceType.HUDI) {
            throw new DdlException("Resource [" + resourceName + "] is not hudi resource");
        }

        org.apache.hadoop.hive.metastore.api.Table metastoreTable = GlobalStateMgr.getCurrentState().getHiveRepository()
                .getTable(resourceName, this.db, this.table);

        this.resourceName = resourceName;

        String hudiBasePath = metastoreTable.getSd().getLocation();
        if (!Strings.isNullOrEmpty(hudiBasePath)) {
            hudiProperties.put(HUDI_BASE_PATH, hudiBasePath);
        }

        Configuration conf = new Configuration();
        HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder().setConf(conf).setBasePath(hudiBasePath).build();
        HoodieTableConfig hudiTableConfig = metaClient.getTableConfig();

//        HoodieTableType hudiTableType = hudiTableConfig.getTableType();
//        if (hudiTableType == HoodieTableType.MERGE_ON_READ) {
//            throw new DdlException("MERGE_ON_READ type of hudi table is NOT supported.");
//        }
//        hudiProperties.put(HUDI_TABLE_TYPE, hudiTableType.name());

        Option<String[]> hudiTablePrimaryKey = hudiTableConfig.getRecordKeyFields();
        if (hudiTablePrimaryKey.isPresent()) {
            hudiProperties.put(HUDI_TABLE_PRIMARY_KEY, hudiTableConfig.getRecordKeyFieldProp());
        }

        String hudiTablePreCombineField = hudiTableConfig.getPreCombineField();
        if (!Strings.isNullOrEmpty(hudiTablePreCombineField)) {
            hudiProperties.put(HUDI_TABLE_PRE_COMBINE_FIELD, hudiTablePreCombineField);
        }

        HoodieFileFormat hudiBaseFileFormat = hudiTableConfig.getBaseFileFormat();
        hudiProperties.put(HUDI_TABLE_BASE_FILE_FORMAT, hudiBaseFileFormat.name());

        TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
        Schema tableSchema;
        try {
            tableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchema());
        } catch (Exception e) {
            throw new DdlException("Cannot get hudi table schema.");
        }

        Option<String[]> hudiPartitionFields = hudiTableConfig.getPartitionFields();
        if (hudiPartitionFields.isPresent()) {
            for (String partField : hudiPartitionFields.get()) {
                Column partColumn = this.nameToColumn.get(partField);
                if (partColumn == null) {
                    throw new DdlException("Partition column [" + partField + "] must exist in column list");
                } else {
                    this.partColumnNames.add(partField);
                }
            }
        } else if (!metastoreTable.getPartitionKeys().isEmpty()) {
            for (FieldSchema fieldSchema : metastoreTable.getPartitionKeys()) {
                String partField = fieldSchema.getName();
                Column partColumn = this.nameToColumn.get(partField);
                if (partColumn == null) {
                    throw new DdlException("Partition column [" + partField + "] must exist in column list");
                } else {
                    this.partColumnNames.add(partField);
                }
            }
        }
        for (Schema.Field hudiField : tableSchema.getFields()) {
            if (this.partColumnNames.stream().noneMatch(p -> p.equals(hudiField.name()))) {
                this.dataColumnNames.add(hudiField.name());
            }
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
            if (!validColumnType(hudiColumn.schema(), column.getType())) {
                throw new DdlException("Can not convert hudi column type [" + hudiColumn.schema().toString() + "] " +
                        "to starrocks type [" + column.getPrimitiveType() + "], column name: " + column.getName());
            }
        }

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps);
        }
    }

    private boolean validColumnType(Schema avroSchema, Type srType) {
        Schema.Type columnType = avroSchema.getType();
        if (columnType == null) {
            return false;
        }

        PrimitiveType primitiveType = srType.getPrimitiveType();
        LogicalType logicalType = avroSchema.getLogicalType();

        switch (columnType) {
            case BOOLEAN:
                return primitiveType == PrimitiveType.BOOLEAN;
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    return primitiveType == PrimitiveType.DATE;
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    return primitiveType == PrimitiveType.TIME;
                } else {
                    return primitiveType == PrimitiveType.INT;
                }
            case LONG:
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    return primitiveType == PrimitiveType.TIME;
                } else if (logicalType instanceof LogicalTypes.TimestampMillis
                        || logicalType instanceof LogicalTypes.TimestampMicros) {
                    return primitiveType == PrimitiveType.DATETIME;
                } else {
                    return primitiveType == PrimitiveType.BIGINT;
                }
            case FLOAT:
                return primitiveType == PrimitiveType.FLOAT;
            case DOUBLE:
                return primitiveType == PrimitiveType.DOUBLE;
            case STRING:
                return primitiveType == PrimitiveType.VARCHAR ||
                        primitiveType == PrimitiveType.CHAR;
            case ARRAY:
                return validColumnType(avroSchema.getElementType(), ((ArrayType) srType).getItemType());
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    return primitiveType == PrimitiveType.DECIMALV2 || primitiveType == PrimitiveType.DECIMAL32 ||
                            primitiveType == PrimitiveType.DECIMAL64 || primitiveType == PrimitiveType.DECIMAL128;
                } else {
                    return primitiveType == PrimitiveType.VARCHAR;
                }
            case UNION:
                List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                        .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                        .collect(Collectors.toList());

                if (nonNullMembers.size() == 1) {
                    return validColumnType(nonNullMembers.get(0), srType);
                } else {
                    // UNION type is not supported in Starrocks
                    return false;
                }
            case ENUM:
            case MAP:
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
        initHmsTableInfo();
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        if (this.resourceName != null) {
            GlobalStateMgr.getCurrentState().getHiveRepository().clearCache(hmsTableInfo);
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
